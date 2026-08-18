//! Reading a sealed footer a block of rows at a time rather than whole
//!
//! The rows are sorted and mostly fixed stride, so a row's place is arithmetic: the
//! directory in the last few bytes of the file says where each column's rows begin, and a
//! key is then a binary search over blocks of rows read on demand. This path does not
//! verify the footer's checksum, which covers the whole footer and cannot be checked
//! without reading all of it; that is safe only because a row from here names a record,
//! and the record carries its own checksum, key and sequence number for the read.

use std::sync::Arc;

use crate::config::FenceResidency;
use crate::error::{ReelError, Result};
use crate::format::column::ColumnId;
use crate::format::fence::{fence_bytes, top_leads, Fence, FenceCut, FENCE_LEAD};
use crate::format::filter::Filter;
use crate::format::footer::{
    carry_region, directory_span, partition_spans, verify_row, DirectorySpan, FooterFind,
    FooterRow, DIRECTORY_ROW_LEN, ENTRY_TAIL_LEN, FIXED_TAIL_LEN, VARYING_WIDTH,
};
use crate::format::prefix::{unpack_block, RESTART_INTERVAL, TRAILER_LEN};
use crate::format::record::read_u32_le;
use crate::index::counters::FilterProbes;
use crate::io::op::FileId;
use crate::reel::segment::IoDriver;

/// Bytes read from the end of a segment to reach its directory
///
/// The fixed tail plus room for the directory rows of every column a volume is likely to
/// serve. A directory that does not fit is read again at its real size.
const TAIL_PROBE: u64 = 512;

/// Rows one block holds at most
///
/// Large enough that a run of neighbouring keys is answered from one read, small enough
/// that a point lookup does not buy a megabyte to use fifty bytes.
pub const BLOCK_ROWS: usize = 64;

/// Bytes one block holds at most, whatever the row count would allow
///
/// A cap rather than a target: reading more rows a block means fewer probes that each
/// move more bytes, and for a cold lookup that runs the wrong way.
pub const BLOCK_BYTES: usize = 8 * 1024;

/// Rows one block of a partition holds, the row count under the byte cap
///
/// A varying partition's rows are prefix compressed, so no row's place is arithmetic and
/// its unit of read is the restart block, whose first row carries its key whole. Free of
/// the span because the seal cuts its fence at the same boundaries, and two copies of
/// this arithmetic would be a fence naming the wrong block past the first cut.
pub fn block_rows_of(key_width: u16, inline_width: u16) -> usize {
    if key_width == VARYING_WIDTH {
        return RESTART_INTERVAL;
    }
    let stride = key_width as usize + ENTRY_TAIL_LEN + carry_region(inline_width);
    BLOCK_ROWS.min((BLOCK_BYTES / stride.max(1)).max(1))
}

/// Where one column's rows sit inside a sealed segment
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionSpan {
    /// Column the rows belong to
    pub column: ColumnId,

    /// Width every row strides by, or the varying sentinel
    pub key_width: u16,

    /// Bytes each row reserves for a value the column carries
    pub inline_width: u16,

    /// Rows this column contributed to the footer
    pub rows: usize,

    /// Byte offset of the first row within the segment file
    pub at: u64,

    /// Bytes the partition's encoded rows occupy on disk, which only a varying one needs
    pub encoded: u64,
}

impl PartitionSpan {
    /// Whether the rows carry their own starts rather than striding
    pub fn is_varying(&self) -> bool {
        self.key_width == VARYING_WIDTH
    }

    /// Bytes one row takes, where every row takes the same
    pub fn stride(&self) -> usize {
        debug_assert!(!self.is_varying(), "a varying partition has no stride");
        self.key_width as usize + ENTRY_TAIL_LEN + carry_region(self.inline_width)
    }

    /// Rows one of this partition's blocks holds, the row count under the byte cap
    pub fn block_rows(&self) -> usize {
        block_rows_of(self.key_width, self.inline_width)
    }

    /// Blocks the rows divide into
    pub fn blocks(&self) -> usize {
        self.rows.div_ceil(self.block_rows())
    }

    /// The first row of one block and how many rows it holds
    fn block(&self, at: usize) -> (usize, usize) {
        let rows = self.block_rows();
        let first = at * rows;
        (first, rows.min(self.rows.saturating_sub(first)))
    }
}

/// Where a packed partition's restart blocks begin, read once with the map
///
/// The offsets are the prefix encoding's own, relative to the partition's rows. Held
/// beside the spans because reading them per probe would spend the seek they save.
#[derive(Clone, Debug)]
pub struct RestartTable {
    /// Where each restart block begins within the partition's rows
    offsets: Vec<u32>,

    /// One past the last row byte, which closes the final block
    rows_end: u32,
}

impl RestartTable {
    /// Where one restart block's bytes sit within the partition's rows
    fn cut(&self, block: usize) -> Option<(u32, u32)> {
        let start = *self.offsets.get(block)?;
        let end = self
            .offsets
            .get(block + 1)
            .copied()
            .unwrap_or(self.rows_end);
        (end > start).then_some((start, end))
    }
}

/// A sealed segment's directory, which is all that has to be resident to read it
///
/// Bytes rather than rows: a volume holding a thousand sealed segments holds a thousand
/// of these, and each is a handful of spans whatever the segment holds.
#[derive(Clone, Debug, Default)]
pub struct FooterMap {
    /// Where each column's rows sit
    spans: Vec<PartitionSpan>,

    /// Each span's filter, parallel to the spans and read with the directory
    filters: Vec<Option<Filter>>,

    /// Each span's restart table, parallel again, present only where rows are packed
    restarts: Vec<Option<RestartTable>>,

    /// Each span's fence, parallel again, present only where the seal wrote one
    fences: Vec<Option<Fence>>,
}

impl FooterMap {
    /// Read a sealed segment's directory without reading its rows
    ///
    /// Nothing comes back for a file with no footer, which is a tail rather than a sealed
    /// segment and is answered from the map instead.
    pub fn read(
        driver: &IoDriver,
        file: FileId,
        file_len: u64,
        fence: FenceResidency,
        probes: &FilterProbes,
    ) -> Result<Option<FooterMap>> {
        if file_len < FIXED_TAIL_LEN as u64 {
            return Ok(None);
        }
        let probe = TAIL_PROBE.min(file_len);
        probes.note_map_read();
        let tail = driver.pread(file, file_len - probe, probe)?;
        if tail.len() < FIXED_TAIL_LEN {
            return Ok(None);
        }

        let Some(span) = directory_span(&tail, file_len)? else {
            return Ok(None);
        };
        // The directory sat above the probe, so it is read again at the size the trailer
        // just gave. The filters sit immediately below it and are taken in the same read,
        // since paying a second read for them would spend what they came to save.
        let wanted = span.footer_len - span.directory_at + span.filter_len;
        let tailed = match wanted as u64 <= probe {
            true => tail[tail.len() - wanted..].to_vec(),
            false => {
                probes.note_map_read();
                driver.pread(file, file_len - wanted as u64, wanted as u64)?
            }
        };
        if tailed.len() < span.filter_len + span.partitions * DIRECTORY_ROW_LEN {
            return Ok(None);
        }
        let (region, directory) = tailed.split_at(span.filter_len);

        // Rows begin where the footer does, which is that far back from the end.
        let rows_at = file_len - span.footer_len as u64;
        let spans = partition_spans(directory, span.partitions, rows_at)?;
        let restarts = spans
            .iter()
            .map(|span| read_restarts(driver, file, span, probes))
            .collect::<Result<Vec<_>>>()?;
        let fences = read_fences(driver, file, file_len, &span, &spans, fence, probes)?;
        Ok(Some(FooterMap {
            filters: Filter::parse_region(region, spans.len()),
            restarts,
            fences,
            spans,
        }))
    }

    /// Where one column's rows sit and what its filter says, in one lookup
    ///
    /// Nothing comes back for a column this segment holds no rows for. A column with no
    /// filter, or one this reader could not follow, comes back with none, which means
    /// search: only a filter that parsed ever stops one.
    pub fn locate(&self, column: ColumnId) -> Option<(PartitionSpan, Option<&Filter>)> {
        let at = self.spans.iter().position(|span| span.column == column)?;
        Some((
            self.spans[at],
            self.filters.get(at).and_then(Option::as_ref),
        ))
    }

    /// Where one column's rows sit, if this segment holds any of them
    pub fn span_of(&self, column: ColumnId) -> Option<PartitionSpan> {
        self.locate(column).map(|(span, _)| span)
    }

    /// One column's restart table, which only a packed partition has
    pub fn restarts_of(&self, column: ColumnId) -> Option<&RestartTable> {
        let at = self.spans.iter().position(|span| span.column == column)?;
        self.restarts.get(at).and_then(Option::as_ref)
    }

    /// One column's fence, which only a segment sealed on a fenced volume has
    pub fn fence_of(&self, column: ColumnId) -> Option<&Fence> {
        let at = self.spans.iter().position(|span| span.column == column)?;
        self.fences.get(at).and_then(Option::as_ref)
    }

    /// The columns this segment holds rows for
    pub fn columns(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.spans.iter().map(|span| span.column)
    }

    /// Bytes this map weighs, for a cache that bounds what it holds
    pub fn weight(&self) -> usize {
        let filters: usize = self
            .filters
            .iter()
            .flatten()
            .map(|filter| filter.encoded_len())
            .sum();
        let restarts: usize = self
            .restarts
            .iter()
            .flatten()
            .map(|table| table.offsets.len() * std::mem::size_of::<u32>())
            .sum();
        let fences: usize = self.fences.iter().flatten().map(Fence::weight).sum();
        self.spans.len() * std::mem::size_of::<PartitionSpan>() + filters + restarts + fences
    }
}

/// Read the fences of every partition, or the sampled level over them
///
/// One read for the region rather than one per partition: the leads are packed in
/// directory order and the sampled level after all of them. Nothing in the footer says
/// the region is there, so the gap the rows leave below the filters is believed only when
/// it is exactly the size these partitions' blocks imply.
fn read_fences(
    driver: &IoDriver,
    file: FileId,
    file_len: u64,
    span: &DirectorySpan,
    spans: &[PartitionSpan],
    residency: FenceResidency,
    probes: &FilterProbes,
) -> Result<Vec<Option<Fence>>> {
    let blocks: Vec<usize> = spans.iter().map(PartitionSpan::blocks).collect();
    let leads_len: usize = blocks.iter().map(|blocks| blocks * FENCE_LEAD).sum();
    let region_len: usize = blocks.iter().copied().map(fence_bytes).sum();
    let rows_len: usize = spans.iter().map(|span| span.encoded as usize).sum();
    let gap = span.directory_at.saturating_sub(span.filter_len + rows_len);
    if residency == FenceResidency::Off || region_len == 0 || gap != region_len {
        return Ok(spans.iter().map(|_| None).collect());
    }

    // The region sits immediately below the filters, which sit below the directory.
    let region_at = file_len - span.footer_len as u64
        + (span.directory_at - span.filter_len - region_len) as u64;
    // A resident fence takes the leads and a paged one the sampled level over them, which
    // is why the region is written leads first: either half is one run of bytes. The off
    // arm is unreachable and rides with the resident one rather than a catch-all.
    let (at, wanted) = match residency {
        FenceResidency::Off | FenceResidency::Resident => (region_at, leads_len),
        FenceResidency::Paged => (region_at + leads_len as u64, region_len - leads_len),
    };
    probes.note_map_read();
    let bytes = driver.pread(file, at, wanted as u64)?;
    if bytes.len() < wanted {
        return Err(ReelError::Corruption(
            "a footer's fence is truncated".to_string(),
        ));
    }

    let mut fences = Vec::with_capacity(spans.len());
    let mut leads_at = 0usize;
    let mut tops_at = 0usize;
    for held in blocks {
        if held == 0 {
            fences.push(None);
            continue;
        }
        let fence = match residency {
            FenceResidency::Off | FenceResidency::Resident => {
                let cut = &bytes[leads_at..leads_at + held * FENCE_LEAD];
                Fence::Held(FenceCut::try_new(Arc::from(cut), 0)?)
            }
            FenceResidency::Paged => Fence::Sampled {
                tops: FenceCut::try_new(
                    Arc::from(&bytes[tops_at..tops_at + top_leads(held) * FENCE_LEAD]),
                    0,
                )?,
                at: region_at + leads_at as u64,
                blocks: held,
            },
        };
        fences.push(Some(fence));
        leads_at += held * FENCE_LEAD;
        tops_at += top_leads(held) * FENCE_LEAD;
    }
    Ok(fences)
}

/// Read one packed partition's restart table, or nothing for a strided one
///
/// The table's place needs no read to find: the restart count follows from the directory's
/// row count, and the table and trailer close the partition's encoded span. The trailer
/// has to agree with the directory, or the offsets describe other rows than these.
fn read_restarts(
    driver: &IoDriver,
    file: FileId,
    span: &PartitionSpan,
    probes: &FilterProbes,
) -> Result<Option<RestartTable>> {
    if !span.is_varying() || span.rows == 0 {
        return Ok(None);
    }
    let count = span.rows.div_ceil(RESTART_INTERVAL);
    let wanted = (count * std::mem::size_of::<u32>() + TRAILER_LEN) as u64;
    if span.encoded < wanted {
        return Err(ReelError::Corruption(
            "a packed partition is shorter than its restart table".to_string(),
        ));
    }
    probes.note_map_read();
    let bytes = driver.pread(file, span.at + span.encoded - wanted, wanted)?;
    if bytes.len() < wanted as usize {
        return Err(ReelError::Corruption(
            "a packed partition's restart table is truncated".to_string(),
        ));
    }
    let table_len = count * std::mem::size_of::<u32>();
    let stored_count = read_u32_le(&bytes[table_len..table_len + 4]) as usize;
    let stored_rows = read_u32_le(&bytes[table_len + 4..table_len + 8]) as usize;
    if stored_count != count || stored_rows != span.rows {
        return Err(ReelError::Corruption(
            "a packed partition's trailer disagrees with its directory row".to_string(),
        ));
    }

    let rows_end = (span.encoded - wanted) as u32;
    let mut offsets = Vec::with_capacity(count);
    for at in 0..count {
        let offset = read_u32_le(&bytes[at * 4..at * 4 + 4]);
        if offset >= rows_end || offsets.last().is_some_and(|last| offset <= *last) {
            return Err(ReelError::Corruption(
                "a packed partition's restarts do not ascend".to_string(),
            ));
        }
        offsets.push(offset);
    }
    if offsets.first().copied() != Some(0) {
        return Err(ReelError::Corruption(
            "a packed partition's rows do not open on a restart".to_string(),
        ));
    }
    Ok(Some(RestartTable { offsets, rows_end }))
}

/// One block of a column's rows, as read off the volume
#[derive(Debug)]
pub struct RowBlock {
    /// The rows themselves, whole rather than prefix compressed
    packed: Vec<u8>,

    /// Bytes one row takes, zero when the rows carry their own starts
    stride: usize,

    /// Width every row's key was written at, zero when they vary
    key_width: usize,

    /// Bytes each row reserves for a value the column carries
    inline_width: u16,

    /// Where each of this block's rows begins, rebased on the block, empty when strided
    starts: Vec<u32>,

    /// Row index the block's first row holds, so a hit can be reported absolutely
    first: usize,
}

impl RowBlock {
    /// Read one block of a column's rows
    ///
    /// A strided column reads the rows alone, since where they sit is arithmetic. A
    /// varying one reads the cut of the packed rows its restart table names.
    pub fn read(
        driver: &IoDriver,
        file: FileId,
        span: &PartitionSpan,
        at: usize,
        restarts: Option<&RestartTable>,
    ) -> Result<RowBlock> {
        let (first, rows) = span.block(at);
        if !span.is_varying() {
            let stride = span.stride();
            let offset = span.at + (first * stride) as u64;
            let packed = driver.pread(file, offset, (rows * stride) as u64)?;
            return Ok(RowBlock {
                packed,
                stride,
                key_width: span.key_width as usize,
                inline_width: span.inline_width,
                starts: Vec::new(),
                first,
            });
        }

        // A varying partition's rows are prefix compressed, so the unit of read is the
        // cut between two restart offsets, rebuilt into the row form searches speak.
        let Some(table) = restarts else {
            return Err(ReelError::Corruption(
                "a packed partition was read without its restart table".to_string(),
            ));
        };
        let Some((start, end)) = table.cut(at) else {
            return Err(ReelError::Corruption(
                "a packed partition's restarts leave a block empty".to_string(),
            ));
        };
        let bytes = driver.pread(file, span.at + u64::from(start), u64::from(end - start))?;
        if bytes.len() < (end - start) as usize {
            return Err(ReelError::Corruption(
                "footer partition is truncated".to_string(),
            ));
        }
        let tail_len = ENTRY_TAIL_LEN + carry_region(span.inline_width);
        let (packed, starts) = unpack_block(&bytes, tail_len)?;
        if starts.len() - 1 != rows {
            return Err(ReelError::Corruption(
                "a restart block holds a row count its table denies".to_string(),
            ));
        }
        Ok(RowBlock {
            packed,
            stride: 0,
            key_width: 0,
            inline_width: span.inline_width,
            starts,
            first,
        })
    }

    /// Rows the block actually read back
    pub fn len(&self) -> usize {
        if !self.starts.is_empty() {
            return self.starts.len() - 1;
        }
        match self.stride {
            0 => 0,
            stride => self.packed.len() / stride,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes this block weighs, for a cache that bounds what it holds
    pub fn weight(&self) -> usize {
        self.packed.len()
    }

    /// Where one of the block's rows begins and ends
    fn row_span(&self, at: usize) -> Option<(usize, usize)> {
        if !self.starts.is_empty() {
            let start = *self.starts.get(at)? as usize;
            let end = *self.starts.get(at + 1)? as usize;
            return (end >= start && end <= self.packed.len()).then_some((start, end));
        }
        let start = at.checked_mul(self.stride)?;
        let end = start.checked_add(self.stride)?;
        (end <= self.packed.len()).then_some((start, end))
    }

    /// The key width one of the block's rows was written at
    fn key_len(&self, at: usize) -> Option<usize> {
        if self.starts.is_empty() {
            return Some(self.key_width);
        }
        let (start, end) = self.row_span(at)?;
        (end - start).checked_sub(ENTRY_TAIL_LEN + carry_region(self.inline_width))
    }

    /// The key one of the block's rows carries
    pub fn key_at(&self, at: usize) -> Option<&[u8]> {
        let (start, _) = self.row_span(at)?;
        let width = self.key_len(at)?;
        self.packed.get(start..start + width)
    }

    /// The first key the block holds, which is what orders it against a search
    pub fn first_key(&self) -> Option<&[u8]> {
        self.key_at(0)
    }

    /// The newest row for a key inside this block, if the block holds it
    ///
    /// A segment that overwrote its own record holds both versions under one key in the
    /// order they were written, so the last of an equal run is the live one. A caller
    /// wanting the value passes a buffer, since the block goes out of scope behind the row.
    pub fn find(&self, key: &[u8], carry: Option<&mut Vec<u8>>) -> Option<Result<FooterRow>> {
        let at = self.upper_bound(key).checked_sub(1)?;
        if self.key_at(at)? != key {
            return None;
        }
        let row = self.row_at(at);
        if let (Some(into), Ok(found)) = (carry, row.as_ref()) {
            into.clear();
            if found.carries() {
                match self.carry_at(at, found.len as usize) {
                    Ok(Some(held)) => into.extend_from_slice(held),
                    Ok(None) => {}
                    Err(error) => return Some(Err(error)),
                }
            }
        }
        Some(row)
    }

    /// The value bytes one row carries, once the row's own checksum has answered
    ///
    /// A block read is not checksummed, which is safe while a row only names a record. A
    /// row that answers with its own bytes carries a checksum, and this is where it is
    /// spent.
    fn carry_at(&self, at: usize, len: usize) -> Result<Option<&[u8]>> {
        let Some((start, end)) = self.row_span(at) else {
            return Ok(None);
        };
        let Some(width) = self.key_len(at) else {
            return Ok(None);
        };
        match verify_row(&self.packed[start..end], width, self.inline_width)? {
            Some(held) => Ok(held.get(..len)),
            None => Ok(None),
        }
    }

    /// The first row past a key, or the row count if none is
    pub fn upper_bound(&self, key: &[u8]) -> usize {
        let mut low = 0usize;
        let mut high = self.len();
        while low < high {
            let mid = low + (high - low) / 2;
            match self.key_at(mid) {
                Some(found) if found <= key => low = mid + 1,
                Some(_) => high = mid,
                None => return low,
            }
        }
        low
    }

    /// Decode one of the block's rows, value and all
    pub fn row_at(&self, at: usize) -> Result<FooterRow> {
        let (start, end) = self
            .row_span(at)
            .ok_or_else(|| ReelError::Corruption("footer row is out of range".to_string()))?;
        let width = self.key_len(at).ok_or_else(|| {
            ReelError::Corruption("footer row is shorter than its tail".to_string())
        })?;
        let bytes = self
            .packed
            .get(start..end)
            .ok_or_else(|| ReelError::Corruption("footer row is out of range".to_string()))?;
        FooterRow::from_packed(bytes, width, self.inline_width)
    }

    /// Where this block's rows sit among the partition's
    pub fn first(&self) -> usize {
        self.first
    }
}

/// One key against a column's blocked rows, the filter asked before any search
///
/// The loader hands back the block asked for, or nothing when the segment has gone from
/// under the search, which ends the lookup as missing. The fence arrives as a thunk for
/// the same reason the loader is one: a segment the filter rules out pays no io at all.
pub fn lookup_in_span(
    span: &PartitionSpan,
    filter: Option<&Filter>,
    key: &[u8],
    fence: impl FnOnce() -> Result<Option<FenceCut>>,
    load: impl FnMut(usize) -> Result<Option<Arc<RowBlock>>>,
    carry: Option<&mut Vec<u8>>,
) -> Result<FooterFind> {
    if filter.is_some_and(|filter| !filter.may_hold(key)) {
        return Ok(FooterFind::RuledOut);
    }
    match find_in_span(span, key, fence()?, load, carry)? {
        None => Ok(FooterFind::Missing),
        Some(row) => Ok(FooterFind::Found(row)),
    }
}

/// Find a key in one column's rows, reading only the blocks the search touches
///
/// A binary search over blocks by their first key, then a search inside the one block
/// that could hold it, with the blocks read through the caller's own loader. A fence
/// narrows the search to the blocks whose leads tie with the key, which moves the
/// halvings off the volume and changes nothing about which row answers.
fn find_in_span(
    span: &PartitionSpan,
    key: &[u8],
    fence: Option<FenceCut>,
    mut load: impl FnMut(usize) -> Result<Option<Arc<RowBlock>>>,
    carry: Option<&mut Vec<u8>>,
) -> Result<Option<FooterRow>> {
    let blocks = span.blocks();
    if blocks == 0 {
        return Ok(None);
    }

    // The block whose first key is the last one at or below the search key: any earlier
    // block ends below it and any later one begins above it.
    let (low, high) = match &fence {
        // Clamped, so a fence naming more blocks than the directory does costs a search
        // of the blocks that exist rather than a read past the partition.
        Some(cut) => {
            let (low, high) = cut.bracket(key);
            (low.min(blocks), high.min(blocks))
        }
        None => (0, blocks),
    };
    let Some(at) = landing(key, low, high, fence.is_some(), &mut load)? else {
        return Ok(None);
    };

    let Some(block) = load(at)? else {
        return Ok(None);
    };
    match block.find(key, carry) {
        Some(found) => found.map(Some),
        // A key equal to a later block's first key would have placed the search there, so
        // a miss here is a miss outright.
        None => Ok(None),
    }
}

/// The block a key would be in, out of the blocks the bracket left standing
///
/// A bracket of the whole partition is halved. A fence's bracket is asked at its top
/// first, since the top is the answer for every key whose lead is its own, so the
/// halvings run only for a key whose lead ties with several blocks. Nothing comes back
/// for a key below every block, or for a segment that went out from under the search.
fn landing(
    key: &[u8],
    mut low: usize,
    mut high: usize,
    is_fenced: bool,
    load: &mut impl FnMut(usize) -> Result<Option<Arc<RowBlock>>>,
) -> Result<Option<usize>> {
    if is_fenced && low < high {
        let Some(block) = load(high - 1)? else {
            return Ok(None);
        };
        match block.first_key() {
            Some(first) if first <= key => return Ok(Some(high - 1)),
            _ => high -= 1,
        }
    }
    while low < high {
        let mid = low + (high - low) / 2;
        let Some(block) = load(mid)? else {
            return Ok(None);
        };
        match block.first_key() {
            Some(first) if first <= key => low = mid + 1,
            _ => high = mid,
        }
    }
    Ok(low.checked_sub(1))
}
