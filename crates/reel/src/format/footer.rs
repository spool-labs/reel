//! Sealed segment footer: a packed sorted index of the segment's live records
//!
//! One segment holds records from every column, so its footer is partitioned: each
//! column's rows sit together, sorted by key and strided at that column's own key width,
//! behind a directory naming the partitions. Striding at the natural width keeps a footer
//! the size of the keys it describes rather than of the widest key declared anywhere.

use std::borrow::Cow;

use crate::error::{ReelError, Result};
use crate::format::block::block_rows_of;
use crate::format::column::{
    carry_bytes, inline_bytes, CarryBytes, ColumnId, RecordKey, INLINE_MAX, ROW_CARRY_MAX,
};
use crate::format::fence::{fence_bytes, lead_of, top_leads, FENCE_LEAD, FENCE_PAGE_LEADS};
use crate::format::filter::{Filter, HEADER_LEN as FILTER_HEADER_LEN};
use crate::format::lsn::Lsn;
use crate::format::prefix::PrefixRows;
use crate::format::record::{checksum, read_u32_le, read_u64_le, Flags, RecordHeader};

/// Marker in the final bytes of a sealed segment
const FOOTER_MAGIC: u32 = u32::from_le_bytes(*b"REEL");

/// Bytes a little endian word takes
const U32_BYTES: usize = std::mem::size_of::<u32>();

/// Bytes a wide little endian word takes
const U64_BYTES: usize = std::mem::size_of::<u64>();

/// Bytes one packed entry takes past its key
pub const ENTRY_TAIL_LEN: usize = U64_BYTES + U32_BYTES + U32_BYTES + 1;

/// Bytes a carrying row spends on its own checksum, ahead of the value
///
/// A row that names a record is checked by that record, and the block path does not
/// verify the whole-footer checksum, so a row that answers with its own bytes carries a
/// checksum of its own and nothing else does.
pub const ROW_CRC_LEN: usize = U32_BYTES;

/// Bytes a row spends on a carry of this width, checksum included
pub const fn carry_region(carry: u16) -> usize {
    match carry {
        0 => 0,
        carry => ROW_CRC_LEN + carry as usize,
    }
}

/// Bytes one directory row takes: the column, its widths, its rows, and its span
///
/// The span is there because a varying partition's length is not its row count times
/// anything, and a reader holding only the directory has to step from one to the next.
pub const DIRECTORY_ROW_LEN: usize = 1 + 2 + 2 + U32_BYTES + U32_BYTES;

/// Where a directory row's span field begins
const DIRECTORY_SPAN_AT: usize = 1 + 2 + 2 + U32_BYTES;

/// The width a partition declares when its rows are not all one width
///
/// Past the longest key by a wide margin, so it can never collide with a width a column
/// really keyed its rows at. Such a partition carries a start per row ahead of the rows.
pub const VARYING_WIDTH: u16 = u16::MAX;

/// Bytes one row start takes in a varying partition's table
pub const START_BYTES: usize = U32_BYTES;

const MAGIC_FROM_END: usize = U32_BYTES;
const FOOTER_LEN_FROM_END: usize = MAGIC_FROM_END + U32_BYTES;
const CRC_FROM_END: usize = FOOTER_LEN_FROM_END + U32_BYTES;
const MAX_LSN_FROM_END: usize = CRC_FROM_END + U64_BYTES;
const MIN_LSN_FROM_END: usize = MAX_LSN_FROM_END + U64_BYTES;
const LIVE_BYTES_FROM_END: usize = MIN_LSN_FROM_END + U64_BYTES;
const DEAD_BYTES_FROM_END: usize = LIVE_BYTES_FROM_END + U64_BYTES;
const ENTRY_COUNT_FROM_END: usize = DEAD_BYTES_FROM_END + U32_BYTES;
const PARTITION_COUNT_FROM_END: usize = ENTRY_COUNT_FROM_END + U32_BYTES;
const BLOOM_LEN_FROM_END: usize = PARTITION_COUNT_FROM_END + U32_BYTES;
const SEALED_AT_FROM_END: usize = BLOOM_LEN_FROM_END + U64_BYTES;

/// Bytes the footer holds after its partitions, with the filter region empty
pub const FIXED_TAIL_LEN: usize = SEALED_AT_FROM_END;

/// Live and dead record bytes a segment held when it sealed, frozen into the file
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FooterTally {
    pub live: u64,
    pub dead: u64,
}

/// The offset a row carrying its own value records, since it names no record
///
/// A carried row answers from itself, so it points at nothing, and a zero would read as
/// the segment header record.
pub const NO_RECORD: u32 = u32::MAX;

/// The carried bytes of a packed row, once its checksum has answered for them
///
/// The row's own four checksum bytes are zeroed and the rest recomputed, so a flipped
/// bit anywhere in the key, the tail or the value is caught here rather than served.
pub fn verify_row(row: &[u8], key_width: usize, carry: u16) -> Result<Option<&[u8]>> {
    if carry == 0 {
        return Ok(None);
    }
    let crc_at = key_width + ENTRY_TAIL_LEN;
    let held_at = crc_at + ROW_CRC_LEN;
    let held = row
        .get(held_at..held_at + carry as usize)
        .ok_or_else(|| ReelError::Corruption("footer row is short of its value".to_string()))?;
    let stored = read_u32_le(&row[crc_at..held_at]);
    let mut scratch = Vec::with_capacity(row.len());
    scratch.extend_from_slice(&row[..crc_at]);
    scratch.extend_from_slice(&0u32.to_le_bytes());
    scratch.extend_from_slice(held);
    if checksum(&scratch) != stored {
        return Err(ReelError::Corruption(
            "a footer row carrying its value fails its own checksum".to_string(),
        ));
    }
    Ok(Some(held))
}

/// One sealed record's index entry, as packed in a segment footer
///
/// A point tombstone carries a zero length, a data entry its payload length, and a range
/// tombstone the length of the end key it names. Each also says which kind it is, since a
/// zero length fits an empty data record too.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FooterEntry {
    /// Column and key this entry resolves
    pub key: RecordKey,

    /// Append sequence number of the record
    pub lsn: Lsn,

    /// Byte offset of the record header within the segment
    pub offset: u32,

    /// Payload length in bytes, zero for a point tombstone
    pub len: u32,

    /// The record's own flags, which say which kind of record it is
    pub flags: Flags,

    /// Bytes the column reserves per row for a value it carries here
    pub inline_width: u16,

    /// The value itself, meaningful up to the length above
    pub inline: CarryBytes,
}

/// What one sealed segment says about a key, the filter consulted first
#[derive(Debug)]
pub enum FooterFind {
    /// The filter rules the key out, so nothing was searched
    RuledOut,

    /// Searched, and the segment holds nothing under the key
    Missing,

    /// The newest row the segment holds for the key
    Found(FooterRow),
}

/// What one row says about its record, without the key it is filed under
///
/// A merge already holds the key it is asking about, so decoding the row's copy of it is
/// a memcpy per candidate row that is then thrown away.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FooterRow {
    /// Append sequence number of the record, which is what orders two rows
    pub lsn: Lsn,

    /// Byte offset of the record header within the segment
    pub offset: u32,

    /// Payload length in bytes, zero for a point tombstone
    pub len: u32,

    /// The record's own flags, which say which kind of record it is
    pub flags: Flags,

    /// Bytes the column reserves per row for a value it carries here
    inline_width: u16,

    /// The value itself, meaningful up to the length above
    inline: [u8; INLINE_MAX],
}

impl FooterRow {
    /// Decode one packed row, value and all, for a reader holding only that row
    pub fn from_packed(row: &[u8], width: usize, inline_width: u16) -> Result<FooterRow> {
        let found = FooterRow::read(row, width)?;
        let inline_at = width + ENTRY_TAIL_LEN;
        let inline = row
            .get(inline_at..inline_at + inline_width as usize)
            .ok_or_else(|| ReelError::Corruption("footer row is short of its value".to_string()))?;
        Ok(found.with_inline(inline_width, inline))
    }

    fn read(row: &[u8], width: usize) -> Result<FooterRow> {
        let offset_at = width + U64_BYTES;
        let len_at = offset_at + U32_BYTES;
        let flags_at = len_at + U32_BYTES;
        Ok(FooterRow {
            lsn: Lsn(read_u64_le(&row[width..offset_at])),
            offset: read_u32_le(&row[offset_at..len_at]),
            len: read_u32_le(&row[len_at..flags_at]),
            flags: Flags::from_bits(row[flags_at])?,
            inline_width: 0,
            inline: [0u8; INLINE_MAX],
        })
    }

    /// The same row carrying the value its column keeps beside it
    fn with_inline(self, inline_width: u16, inline: &[u8]) -> FooterRow {
        FooterRow {
            inline_width,
            inline: inline_bytes(inline),
            ..self
        }
    }

    /// Whether this row is the only place its value lives
    ///
    /// There is no record to fall back to, so the read path answers such a row from its
    /// carry or reports corruption.
    pub fn stands_alone(&self) -> bool {
        self.offset == NO_RECORD
    }

    /// The value this row carries, when the column asked for one and it fits
    ///
    /// A row may carry more than this struct holds, so a wider value reads as none and
    /// the caller goes to the record.
    pub fn inlined(&self) -> Option<&[u8]> {
        let len = self.len as usize;
        if !self.flags.is_data()
            || self.inline_width == 0
            || len > self.inline_width as usize
            || len > INLINE_MAX
        {
            return None;
        }
        Some(&self.inline[..len])
    }

    /// Whether this row holds the value itself, whatever its length
    ///
    /// A carried value can be zero bytes long, so an empty answer cannot be the way a
    /// caller learns there was none.
    pub fn carries(&self) -> bool {
        self.flags.is_data() && self.inline_width > 0 && self.len <= u32::from(self.inline_width)
    }

    /// The same row filed under its key, for a caller that wants one
    fn into_entry(self, key: RecordKey) -> FooterEntry {
        FooterEntry::new(key, self.lsn, self.offset, self.len, self.flags)
    }

    /// Whether the row says its key was deleted rather than written
    pub fn is_tombstone(&self) -> bool {
        self.flags.is_tombstone()
    }

    /// Whether the row is a range delete, which names a span rather than a key
    pub fn is_range_tombstone(&self) -> bool {
        self.flags.is_range_tombstone()
    }
}

impl FooterEntry {
    /// An entry pointing at a record header within its segment
    pub fn new(key: RecordKey, lsn: Lsn, offset: u32, len: u32, flags: Flags) -> FooterEntry {
        FooterEntry {
            key,
            lsn,
            offset,
            len,
            flags,
            inline_width: 0,
            inline: [0u8; ROW_CARRY_MAX],
        }
    }

    /// An entry for a record if the footer lists its kind, else nothing
    ///
    /// Data records and both kinds of tombstone are listed; pads and segment headers are
    /// not. A payload short enough for the column's ceiling is copied into the row, and
    /// everything else is only pointed at.
    pub fn from_record(
        header: &RecordHeader,
        offset: u32,
        payload: &[u8],
        inline_width: u16,
    ) -> Option<FooterEntry> {
        if !is_listed(header.flags) {
            return None;
        }
        let mut entry = FooterEntry::new(
            header.key.clone(),
            header.lsn,
            offset,
            header.length,
            header.flags,
        );
        entry.inline_width = inline_width.min(ROW_CARRY_MAX as u16);
        if header.flags.is_data() && header.length <= u32::from(entry.inline_width) {
            entry.inline = carry_bytes(payload);
        }
        Some(entry)
    }

    /// The value this row carries, when the column asked for one and it fits
    pub fn inlined(&self) -> Option<&[u8]> {
        let len = self.len as usize;
        if !self.flags.is_data() || self.inline_width == 0 || len > self.inline_width as usize {
            return None;
        }
        Some(&self.inline[..len])
    }

    /// An entry for a value the caller is keeping in the row and nowhere else
    ///
    /// No record is written for it, so the offset is the sentinel and the carry holds the
    /// value. Refused for a value the row cannot hold, which would lose it.
    pub fn standing_alone(
        key: RecordKey,
        lsn: Lsn,
        flags: Flags,
        carry: u16,
        value: &[u8],
    ) -> Option<FooterEntry> {
        if !flags.is_data() || carry == 0 || value.len() > carry as usize {
            return None;
        }
        let mut entry = FooterEntry::new(key, lsn, NO_RECORD, value.len() as u32, flags);
        entry.inline_width = carry;
        entry.inline = carry_bytes(value);
        Some(entry)
    }

    /// Whether this entry marks a delete of one key rather than a payload
    pub fn is_tombstone(&self) -> bool {
        self.flags.is_tombstone()
    }

    /// Whether this entry marks a delete of a whole key range
    pub fn is_range_tombstone(&self) -> bool {
        self.flags.is_range_tombstone()
    }
}

/// Whether a record of this kind takes a row in a footer
fn is_listed(flags: Flags) -> bool {
    flags.is_data() || flags.is_tombstone() || flags.is_range_tombstone()
}

/// One column's rows within a footer, packed at that column's stride
///
/// The rows are held in the shape they take on disk rather than as structs, since a tail
/// accumulates one per record. A column whose keys are all one width strides by it and
/// stores no row offsets; one whose keys vary carries a start per row instead.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FooterPartition {
    /// Column every row in the partition belongs to
    pub column: ColumnId,

    /// Key width every row in the partition strides by, or the varying sentinel
    pub key_width: u16,

    /// Bytes each row reserves for the value the column carries here, zero for none
    pub inline_width: u16,

    /// The rows themselves, in the shape they are written in
    packed: Vec<u8>,

    /// Where each row begins, and a closing sentinel, empty when the rows stride
    starts: Vec<u32>,

    /// What the segment says about keys it does not hold, when it says anything
    filter: Option<Filter>,
}

impl FooterPartition {
    /// An empty partition for one column at its key width
    pub fn new(column: ColumnId, key_width: u16, inline_width: u16) -> FooterPartition {
        FooterPartition {
            column,
            key_width,
            inline_width,
            packed: Vec::new(),
            starts: Vec::new(),
            filter: None,
        }
    }

    /// Bytes the partition holds in memory, which prices its on-disk form
    ///
    /// Exact for a strided partition, which is written as it stands. A varying one is
    /// prefix compressed on the way out, so this is only an estimate.
    pub fn encoded_len(&self) -> usize {
        self.starts.len() * START_BYTES + self.packed.len()
    }

    /// Whether the partition records a start per row rather than striding
    pub fn is_varying(&self) -> bool {
        self.key_width == VARYING_WIDTH
    }

    /// Stop striding, keeping the rows already packed
    ///
    /// The rows in hand were all written at the old width, so their starts are the
    /// strides they were packed at, and everything appended after this records its own.
    fn stop_striding(&mut self) {
        if self.is_varying() {
            return;
        }
        let stride = self.stride();
        let rows = self.len();
        self.starts = (0..=rows).map(|row| (row * stride) as u32).collect();
        self.key_width = VARYING_WIDTH;
    }

    /// Whether this partition may hold the key, which only its filter can deny
    ///
    /// A partition with no filter answers yes to everything, so the search happens and
    /// the answer is right either way.
    pub fn may_hold(&self, key: &[u8]) -> bool {
        match &self.filter {
            Some(filter) => filter.may_hold(key),
            None => true,
        }
    }

    /// One key against this partition, the filter asked before any search
    ///
    /// A caller wanting the value the row carries passes a buffer for it, which is filled
    /// from the packed rows while they are in hand; nothing is copied for a caller that
    /// passes none.
    pub fn lookup(&self, key: &[u8], carry: Option<&mut Vec<u8>>) -> Result<FooterFind> {
        if !self.may_hold(key) {
            return Ok(FooterFind::RuledOut);
        }
        match self.find_row(key, carry) {
            None => Ok(FooterFind::Missing),
            Some(row) => Ok(FooterFind::Found(row?)),
        }
    }

    /// Whether the partition carries a filter at all, which is a property of the segment
    pub fn is_filtered(&self) -> bool {
        self.filter.is_some()
    }

    /// Build the filter over every key the partition holds
    ///
    /// Every row, tombstones included: a delete missing from the filter lets a probe skip
    /// the segment and find an older version still standing behind it.
    fn build_filter(&mut self, bits_per_key: u8) {
        let rows = self.len();
        let keys = (0..rows).filter_map(|at| self.key_at(at));
        self.filter = Filter::build(keys, rows, bits_per_key);
    }

    /// Bytes one row of this partition occupies, where every row occupies the same
    ///
    /// Only a strided partition has an answer; a varying one is asked for a row span,
    /// which is the door every reader goes through whatever the shape.
    pub fn stride(&self) -> usize {
        debug_assert!(!self.is_varying(), "a varying partition has no stride");
        self.key_width as usize + ENTRY_TAIL_LEN + carry_region(self.inline_width)
    }

    /// The widest key any row in this partition opens with
    ///
    /// A record's prefix is the header plus its key, so this bounds how far past a footer
    /// offset the payload can begin.
    pub fn widest_key(&self) -> usize {
        if !self.is_varying() {
            return self.key_width as usize;
        }
        (0..self.len())
            .filter_map(|row| self.key_len(row))
            .max()
            .unwrap_or(0)
    }

    /// Rows one of this partition's blocks holds on disk
    ///
    /// The read side cuts its blocks by the same arithmetic off the directory row, both
    /// through one function: a fence built at a different cut names the wrong block.
    pub fn block_rows(&self) -> usize {
        block_rows_of(self.key_width, self.inline_width)
    }

    /// Blocks this partition's rows divide into on disk
    pub fn blocks(&self) -> usize {
        self.len().div_ceil(self.block_rows())
    }

    /// Bytes this partition's leads take in a fenced footer
    ///
    /// The gap left between the rows and the filters is the fence, and this is how long
    /// that gap has to be, so a fenced footer keeps the trailer an unfenced one has.
    pub fn fence_len(&self) -> usize {
        fence_bytes(self.blocks())
    }

    /// Whether the records these rows name sit in the order the rows are in
    ///
    /// Which is what makes a segment a sorted run. Rows that stand alone name no record
    /// and are passed over, since the sentinel among real offsets reads as unsorted.
    pub fn is_sorted_run(&self) -> bool {
        let mut behind = 0u32;
        for at in 0..self.len() {
            let offset = self.offset_at(at);
            if offset == NO_RECORD {
                continue;
            }
            if offset < behind {
                return false;
            }
            behind = offset;
        }
        true
    }

    /// Bytes past the key that every row carries, whatever its key width
    fn row_tail(&self) -> usize {
        ENTRY_TAIL_LEN + carry_region(self.inline_width)
    }

    /// Where one row begins and ends within the packed rows
    pub fn row_span(&self, index: usize) -> Option<(usize, usize)> {
        if self.is_varying() {
            let start = *self.starts.get(index)? as usize;
            let end = *self.starts.get(index + 1)? as usize;
            return (end >= start && end <= self.packed.len()).then_some((start, end));
        }
        let stride = self.stride();
        let start = index.checked_mul(stride)?;
        let end = start.checked_add(stride)?;
        (end <= self.packed.len()).then_some((start, end))
    }

    /// The key width one row was written at
    ///
    /// Derived from the row's own length rather than stored: the tail and the inline
    /// value are one size on every row, so whatever the row holds past them is its key.
    fn key_len(&self, index: usize) -> Option<usize> {
        if !self.is_varying() {
            return Some(self.key_width as usize);
        }
        let (start, end) = self.row_span(index)?;
        (end - start).checked_sub(self.row_tail())
    }

    /// Rows the partition holds
    pub fn len(&self) -> usize {
        match self.is_varying() {
            true => self.starts.len().saturating_sub(1),
            false => self.packed.len() / self.stride(),
        }
    }

    /// Whether the partition holds no rows
    pub fn is_empty(&self) -> bool {
        self.packed.is_empty()
    }

    /// Append one row
    ///
    /// A strided partition takes rows keyed at its width. A varying one records where the
    /// row started, and that start is the only thing separating this row from the next.
    pub fn push(&mut self, entry: &FooterEntry) {
        let began = self.packed.len();
        self.packed.extend_from_slice(entry.key.as_slice());
        self.packed.extend_from_slice(&entry.lsn.pack());
        self.packed.extend_from_slice(&entry.offset.to_le_bytes());
        self.packed.extend_from_slice(&entry.len.to_le_bytes());
        self.packed.push(entry.flags.bits());
        if self.inline_width > 0 {
            // Over the whole row with its own four bytes left zero, the same rule the
            // record header follows, so a reader of either can zero the field and redo it.
            let crc_at = self.packed.len();
            self.packed.extend_from_slice(&0u32.to_le_bytes());
            self.packed
                .extend_from_slice(&entry.inline[..self.inline_width as usize]);
            let crc = checksum(&self.packed[began..]);
            self.packed[crc_at..crc_at + ROW_CRC_LEN].copy_from_slice(&crc.to_le_bytes());
        }
        if self.is_varying() {
            self.starts.push(self.packed.len() as u32);
        }
    }

    /// The value a row carries, checked against the checksum the row carries with it
    ///
    /// The one door to a carried value, so no reader can take the bytes without the
    /// check. A failed checksum is reported rather than served, since a carried value
    /// has no record to fall back to.
    pub fn carried_at(&self, index: usize) -> Result<Option<&[u8]>> {
        if self.inline_width == 0 {
            return Ok(None);
        }
        let (start, end) = self
            .row_span(index)
            .ok_or_else(|| ReelError::Corruption("footer row is out of range".to_string()))?;
        let row = &self.packed[start..end];
        let width = self.key_len(index).unwrap_or(0);
        let Some(held) = verify_row(row, width, self.inline_width)? else {
            return Ok(None);
        };
        // The region is the column's declared width and the value is the record's own
        // length, so the cut happens after the checksum, which covers the whole region.
        let found = FooterRow::read(row, width)?;
        match found.flags.is_data() && found.len <= u32::from(self.inline_width) {
            true => Ok(held.get(..found.len as usize)),
            false => Ok(None),
        }
    }

    /// Read one row back out
    pub fn entry_at(&self, index: usize) -> Result<FooterEntry> {
        let width = self
            .key_len(index)
            .ok_or_else(|| ReelError::Corruption("footer row is out of range".to_string()))?;
        let row = self.row_bytes(index)?;
        let key = RecordKey::from_bytes(self.column, &row[..width])
            .map_err(|error| ReelError::Corruption(error.to_string()))?;
        let mut entry = FooterRow::read(row, width)?.into_entry(key);
        entry.inline_width = self.inline_width;
        if let Some(held) = verify_row(row, width, self.inline_width)? {
            entry.inline = carry_bytes(held);
        }
        Ok(entry)
    }

    /// Every row in the order the partition holds them
    pub fn entries(&self) -> impl Iterator<Item = Result<FooterEntry>> + '_ {
        (0..self.len()).map(|index| self.entry_at(index))
    }

    /// What one row says about its record, without decoding the key it repeats
    pub fn row_at(&self, index: usize) -> Result<FooterRow> {
        let width = self
            .key_len(index)
            .ok_or_else(|| ReelError::Corruption("footer row is out of range".to_string()))?;
        FooterRow::read(self.row_bytes(index)?, width)
    }

    /// The bytes of one row, which is where every reading of one starts
    fn row_bytes(&self, index: usize) -> Result<&[u8]> {
        let (start, end) = self
            .row_span(index)
            .ok_or_else(|| ReelError::Corruption("footer row is out of range".to_string()))?;
        self.packed
            .get(start..end)
            .ok_or_else(|| ReelError::Corruption("footer row is out of range".to_string()))
    }

    /// The sequence number one row carries, for ordering a key's versions
    ///
    /// Read straight out of the packed row like the key, since the sort asks for it per
    /// comparison and decoding a whole row to answer would be the sort's cost.
    fn lsn_at(&self, index: usize) -> u64 {
        let Some(((start, _), width)) = self.row_span(index).zip(self.key_len(index)) else {
            return 0;
        };
        let at = start + width;
        match self.packed.get(at..at + U64_BYTES) {
            Some(bytes) => read_u64_le(bytes),
            None => 0,
        }
    }

    /// The offset one row names, read out of the packed row like the key
    ///
    /// Zero for a row this cannot reach, which the sortedness test treats as out of
    /// order rather than as absent.
    fn offset_at(&self, index: usize) -> u32 {
        let Some(((start, _), width)) = self.row_span(index).zip(self.key_len(index)) else {
            return 0;
        };
        let at = start + width + U64_BYTES;
        match self.packed.get(at..at + U32_BYTES) {
            Some(bytes) => read_u32_le(bytes),
            None => 0,
        }
    }

    /// The key one row carries, without decoding the rest of it
    ///
    /// A search compares keys and nothing else, so it reads the key alone and decodes a
    /// row only once it has found the one it wants.
    pub fn key_at(&self, index: usize) -> Option<&[u8]> {
        let (start, _) = self.row_span(index)?;
        let width = self.key_len(index)?;
        self.packed.get(start..start + width)
    }

    /// The newest row this partition holds for a key, if it holds one at all
    ///
    /// A segment that overwrote its own record holds both versions under one key, in the
    /// order they were written, so the search walks to the last of an equal run.
    pub fn find(&self, key: &[u8]) -> Option<Result<FooterEntry>> {
        let at = self.locate(key)?;
        Some(self.entry_at(at))
    }

    /// What the newest row for a key says, without rebuilding the key to say it
    pub fn find_row(&self, key: &[u8], carry: Option<&mut Vec<u8>>) -> Option<Result<FooterRow>> {
        let at = self.locate(key)?;
        Some(self.row_at(at).and_then(|row| {
            let width = self
                .key_len(at)
                .ok_or_else(|| ReelError::Corruption("footer row is out of range".to_string()))?;
            let bytes = self.row_bytes(at)?;
            let held = match verify_row(bytes, width, self.inline_width)? {
                Some(held) => held,
                None => &[][..],
            };
            let row = row.with_inline(self.inline_width, held);
            if let Some(into) = carry {
                into.clear();
                if row.carries() {
                    into.extend_from_slice(&held[..row.len as usize]);
                }
            }
            Ok(row)
        }))
    }

    /// The first and last row sharing the key at a position
    ///
    /// Two rows share a key when a segment overwrote its own record, and the sort keeps
    /// versions in sequence order, so the last of a run is the live one. Nearly every key
    /// is written once, so each end is a compare against its neighbour before a search.
    pub fn run(&self, index: usize) -> Option<(usize, usize)> {
        let key = self.key_at(index)?;
        let first = match index.checked_sub(1).and_then(|before| self.key_at(before)) {
            Some(before) if before == key => self.lower_bound(key),
            Some(_) | None => index,
        };
        let last = match self.key_at(index + 1) == Some(key) {
            true => self.upper_bound(key).checked_sub(1)?,
            false => index,
        };
        Some((first, last))
    }

    /// Where the newest row for a key sits, if the partition holds one
    fn locate(&self, key: &[u8]) -> Option<usize> {
        let at = self.upper_bound(key).checked_sub(1)?;
        (self.key_at(at)? == key).then_some(at)
    }

    /// The first row at or past a key, or the row count if every row is below it
    ///
    /// A bound shorter than the partition's keys is a prefix, and a prefix sorts below
    /// every key that begins with it, so the answer is the first row the prefix reaches.
    pub fn lower_bound(&self, key: &[u8]) -> usize {
        self.bound(key, false)
    }

    /// The first row past a key, or the row count if none is
    pub fn upper_bound(&self, key: &[u8]) -> usize {
        self.bound(key, true)
    }

    /// Binary search for the first row past a key, counting equal rows or not
    fn bound(&self, key: &[u8], past_equal: bool) -> usize {
        let (mut low, mut high) = (0usize, self.len());
        while low < high {
            let middle = low + (high - low) / 2;
            let Some(found) = self.key_at(middle) else {
                return self.len();
            };
            let is_below = match past_equal {
                true => found <= key,
                false => found < key,
            };
            match is_below {
                true => low = middle + 1,
                false => high = middle,
            }
        }
        low
    }

    /// The lowest and highest key the partition holds
    ///
    /// What a paged index keeps resident per sealed segment, so a lookup can rule a
    /// segment out without reading its footer at all.
    pub fn key_range(&self) -> Option<(&[u8], &[u8])> {
        let count = self.len();
        if count == 0 {
            return None;
        }
        Some((self.key_at(0)?, self.key_at(count - 1)?))
    }

    /// Rows that still resolved to a record when the segment sealed
    ///
    /// A key overwritten inside its own segment leaves both versions in the partition, so
    /// once the rows are in key order the live ones are the distinct keys. A record
    /// shadowed by a rewrite in another tail leaves one row here and is counted live.
    pub fn live_rows(&self) -> u32 {
        let mut live = 0u32;
        let mut last: Option<&[u8]> = None;
        for at in 0..self.len() {
            let key = self.key_at(at);
            if key != last {
                live += 1;
                last = key;
            }
        }
        live
    }

    /// Whether the rows already sit in the order the sort would put them in
    ///
    /// One pass of n compares against the n log n the sort costs, and arrival order is no
    /// shuffle: a column whose producer advances a counter is already in key order. The
    /// compare is the whole key, since adjacent rows of these columns tie on a lead.
    fn in_key_order(&self) -> bool {
        let count = self.len();
        let Some(mut before) = self.key_at(0) else {
            return true;
        };
        for at in 1..count {
            // A row this cannot read is one the sort has to handle rather than skip.
            let Some(here) = self.key_at(at) else {
                return false;
            };
            match before.cmp(here) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Greater => return false,
                // Equal keys are the sort's tie break: the rows a rewrite left behind are
                // in order when their sequence numbers ascend with them.
                std::cmp::Ordering::Equal => {
                    if self.lsn_at(at - 1) >= self.lsn_at(at) {
                        return false;
                    }
                }
            }
            before = here;
        }
        true
    }

    /// Put the rows in key order, ordering a key's versions by sequence number
    ///
    /// Two rows can share a key when a segment holds an overwrite of its own record, and
    /// the one written later has the higher sequence number.
    pub fn sort(&mut self) {
        // Rows that arrived in order are left where they are, which skips the permute
        // below as well as the sort.
        if self.in_key_order() {
            return;
        }

        let count = self.len();
        let mut order: Vec<u32> = (0..count as u32).collect();
        // Unstable, with the sequence number as the tie break. Writers finish in whatever
        // order they finish, so a key rewritten within one segment can arrive newest
        // first, and ordering that run by arrival leaves the older row where a lookup
        // takes it.
        order.sort_unstable_by(|left, right| {
            let left = *left as usize;
            let right = *right as usize;
            // In range by construction, since the order came from the row count.
            let one = self.key_at(left).unwrap_or_default();
            let two = self.key_at(right).unwrap_or_default();
            one.cmp(two)
                .then_with(|| self.lsn_at(left).cmp(&self.lsn_at(right)))
        });

        let mut sorted = Vec::with_capacity(self.packed.len());
        let mut starts = Vec::with_capacity(self.starts.len());
        if self.is_varying() {
            starts.push(0u32);
        }
        for index in order {
            // In range by construction, as above.
            let (start, end) = self.row_span(index as usize).unwrap_or((0, 0));
            sorted.extend_from_slice(&self.packed[start..end]);
            if self.is_varying() {
                starts.push(sorted.len() as u32);
            }
        }
        self.packed = sorted;
        if self.is_varying() {
            self.starts = starts;
        }
    }
}

/// The parsed contents of a sealed segment footer
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentFooter {
    /// One partition per column present, in column order
    pub partitions: Vec<FooterPartition>,

    /// Lowest append sequence number among the entries
    pub min_lsn: Lsn,

    /// Highest append sequence number among the entries
    pub max_lsn: Lsn,

    /// The sequence number frontier when the segment sealed, which bounds the tally
    pub sealed_at: Lsn,

    /// What the segment's records weighed, live and dead, at the moment it sealed
    pub tally: FooterTally,
}

impl SegmentFooter {
    /// An empty footer, which a tail fills a row at a time as it writes
    pub fn empty() -> SegmentFooter {
        SegmentFooter {
            partitions: Vec::new(),
            min_lsn: Lsn::NONE,
            max_lsn: Lsn::NONE,
            sealed_at: Lsn::NONE,
            tally: FooterTally::default(),
        }
    }

    /// Add one row, opening the partition for its column if this is the first
    ///
    /// A partition opens at the width of the first key its column offers and strides by
    /// it. A key of a second width stops the striding rather than being padded into it.
    pub fn push(&mut self, entry: &FooterEntry) {
        let column = entry.key.column;
        let width = entry.key.width();
        let at = match self
            .partitions
            .iter()
            .position(|found| found.column == column)
        {
            Some(at) => at,
            None => {
                self.partitions
                    .push(FooterPartition::new(column, width, entry.inline_width));
                self.partitions.len() - 1
            }
        };
        if self.partitions[at].key_width != width {
            self.partitions[at].stop_striding();
        }
        self.partitions[at].push(entry);

        if self.min_lsn == Lsn::NONE || entry.lsn < self.min_lsn {
            self.min_lsn = entry.lsn;
        }
        if entry.lsn > self.max_lsn {
            self.max_lsn = entry.lsn;
        }
    }

    /// Assemble a footer from loose entries, grouping and bounding them
    pub fn build(entries: Vec<FooterEntry>) -> SegmentFooter {
        let mut footer = SegmentFooter::empty();
        for entry in &entries {
            footer.push(entry);
        }
        footer
    }

    /// Rows across every partition
    pub fn entry_count(&self) -> usize {
        self.partitions
            .iter()
            .map(|partition| partition.len())
            .sum()
    }

    /// Whether the footer lists nothing at all
    pub fn is_empty(&self) -> bool {
        self.entry_count() == 0
    }

    /// Every row across every partition, columns in order
    pub fn entries(&self) -> impl Iterator<Item = Result<FooterEntry>> + '_ {
        self.partitions
            .iter()
            .flat_map(|partition| partition.entries())
    }

    /// On-disk length this footer packs to
    pub fn encoded_len(&self) -> usize {
        let rows: usize = self
            .partitions
            .iter()
            .map(|partition| partition.encoded_len())
            .sum();
        rows + self.filter_region_len() + self.partitions.len() * DIRECTORY_ROW_LEN + FIXED_TAIL_LEN
    }

    /// Every partition's leads, then the sampled level over each of them
    ///
    /// Both halves in directory order, and neither is written down twice: a partition's
    /// block count follows from the rows and widths its directory row already carries.
    /// The sampled level sits at the end so a volume holding only it takes one run.
    fn fence_region(&self) -> Vec<u8> {
        let blocks: usize = self.partitions.iter().map(FooterPartition::blocks).sum();
        let mut region = Vec::with_capacity(fence_bytes(blocks));
        let mut tops = Vec::with_capacity(top_leads(blocks) * FENCE_LEAD);
        for partition in &self.partitions {
            let rows = partition.block_rows();
            for block in 0..partition.blocks() {
                let lead = lead_of(partition.key_at(block * rows).unwrap_or_default());
                if block % FENCE_PAGE_LEADS == 0 {
                    tops.extend_from_slice(&lead);
                }
                region.extend_from_slice(&lead);
            }
        }
        region.extend_from_slice(&tops);
        region
    }

    /// Bytes the filter region takes, which is a header per partition either way
    ///
    /// A partition with no filter still writes its header, so the region is walked in the
    /// same order as the directory with nothing else to say where each one begins.
    fn filter_region_len(&self) -> usize {
        match self
            .partitions
            .iter()
            .any(|partition| partition.filter.is_some())
        {
            true => self
                .partitions
                .iter()
                .map(|partition| match &partition.filter {
                    Some(filter) => filter.encoded_len(),
                    None => FILTER_HEADER_LEN,
                })
                .sum(),
            false => 0,
        }
    }

    /// Serialize the footer to its on-disk bytes with checksum and magic
    ///
    /// The partitions are put in column order and their rows in key order first, since a
    /// footer is written once and read as a sorted index from then on. Varying rows are
    /// prefix compressed on the way out, so every encoded form is in hand before a length
    /// is written anywhere.
    pub fn pack(&mut self, filter_bits: u8) -> Result<Vec<u8>> {
        self.pack_fenced(filter_bits, false)
    }

    /// The same footer with a fence over each partition's blocks
    ///
    /// The fence is what a blocked search descends instead of reading a block per
    /// halving, so it is written for the volumes that search that way. It sits below the
    /// filters, and nothing in the trailer says it is there: its length is what is left
    /// between the rows and the filters, which follows from the row counts and widths the
    /// directory already carries.
    pub fn pack_fenced(&mut self, filter_bits: u8, is_fenced: bool) -> Result<Vec<u8>> {
        self.partitions.sort_by_key(|partition| partition.column);
        for partition in self.partitions.iter_mut() {
            partition.sort();
            partition.build_filter(filter_bits);
        }

        let encoded = self
            .partitions
            .iter()
            .map(encoded_partition_rows)
            .collect::<Result<Vec<_>>>()?;
        let rows_len: usize = encoded.iter().map(|rows| rows.len()).sum();
        let fences = match is_fenced {
            true => self.fence_region(),
            false => Vec::new(),
        };
        let region_len = self.filter_region_len();
        let footer_len = rows_len
            + fences.len()
            + region_len
            + self.partitions.len() * DIRECTORY_ROW_LEN
            + FIXED_TAIL_LEN;
        let mut buf = Vec::with_capacity(footer_len);
        for rows in &encoded {
            buf.extend_from_slice(rows);
        }
        buf.extend_from_slice(&fences);
        if region_len > 0 {
            for partition in &self.partitions {
                match &partition.filter {
                    Some(filter) => filter.encode(&mut buf),
                    None => Filter::absent().encode(&mut buf),
                }
            }
        }
        for (partition, rows) in self.partitions.iter().zip(&encoded) {
            write_directory_row(&mut buf, partition, rows.len());
        }

        buf.extend_from_slice(&self.sealed_at.pack());
        buf.extend_from_slice(&(region_len as u32).to_le_bytes());
        buf.extend_from_slice(&(self.partitions.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(self.entry_count() as u32).to_le_bytes());
        buf.extend_from_slice(&self.tally.dead.to_le_bytes());
        buf.extend_from_slice(&self.tally.live.to_le_bytes());
        buf.extend_from_slice(&self.min_lsn.pack());
        buf.extend_from_slice(&self.max_lsn.pack());

        let crc_at = buf.len();
        buf.extend_from_slice(&[0u8; U32_BYTES]);
        buf.extend_from_slice(&(footer_len as u32).to_le_bytes());
        buf.extend_from_slice(&FOOTER_MAGIC.to_le_bytes());

        let crc = checksum(&buf);
        buf[crc_at..crc_at + U32_BYTES].copy_from_slice(&crc.to_le_bytes());
        Ok(buf)
    }

    /// Parse a footer from the trailing bytes of a segment
    ///
    /// The slice must end at the segment end. An absent magic, a length out of range, or
    /// a checksum mismatch is reported so the caller falls back to a record scan.
    pub fn parse(segment_tail: &[u8]) -> Result<SegmentFooter> {
        let total = segment_tail.len();
        if total < FIXED_TAIL_LEN {
            return Err(ReelError::Corruption(
                "segment is shorter than a footer trailer".to_string(),
            ));
        }

        let magic = read_u32_le(&segment_tail[total - MAGIC_FROM_END..total]);
        if magic != FOOTER_MAGIC {
            return Err(ReelError::Corruption(
                "segment has no footer magic".to_string(),
            ));
        }

        let footer_len =
            read_u32_le(&segment_tail[total - FOOTER_LEN_FROM_END..total - MAGIC_FROM_END])
                as usize;
        if footer_len < FIXED_TAIL_LEN || footer_len > total {
            return Err(ReelError::Corruption(
                "footer length is out of range".to_string(),
            ));
        }

        let footer = &segment_tail[total - footer_len..total];
        verify_footer_crc(footer, footer_len)?;

        let entry_count = read_at_u32(footer, footer_len, ENTRY_COUNT_FROM_END) as usize;
        let tally = FooterTally {
            live: read_u64_le(
                &footer[footer_len - LIVE_BYTES_FROM_END..footer_len - MIN_LSN_FROM_END],
            ),
            dead: read_u64_le(
                &footer[footer_len - DEAD_BYTES_FROM_END..footer_len - LIVE_BYTES_FROM_END],
            ),
        };
        let partition_count = read_at_u32(footer, footer_len, PARTITION_COUNT_FROM_END) as usize;
        let bloom_len = read_at_u32(footer, footer_len, BLOOM_LEN_FROM_END) as usize;
        let min_lsn = Lsn(read_u64_le(
            &footer[footer_len - MIN_LSN_FROM_END..footer_len - MAX_LSN_FROM_END],
        ));
        let max_lsn = Lsn(read_u64_le(
            &footer[footer_len - MAX_LSN_FROM_END..footer_len - CRC_FROM_END],
        ));
        let sealed_at = Lsn(read_u64_le(
            &footer[footer_len - SEALED_AT_FROM_END..footer_len - BLOOM_LEN_FROM_END],
        ));

        let body = footer_len - FIXED_TAIL_LEN;
        let directory_len = partition_count * DIRECTORY_ROW_LEN;
        if bloom_len > body || directory_len > body - bloom_len {
            return Err(ReelError::Corruption(
                "footer directory does not fit its own length".to_string(),
            ));
        }

        let directory_at = body - directory_len;
        let (mut partitions, consumed) = read_partitions(footer, directory_at, partition_count)?;
        let region = &footer[directory_at - bloom_len..directory_at];
        for (partition, filter) in partitions
            .iter_mut()
            .zip(Filter::parse_region(region, partition_count))
        {
            partition.filter = filter;
        }

        // What is left between the rows and the filters is the fence, and nothing here
        // reads it. It is still checked against the size these partitions imply, since
        // the only other thing a gap can be is a footer that does not describe itself.
        let listed: usize = partitions.iter().map(|partition| partition.len()).sum();
        let rows_end = directory_at - bloom_len;
        let fenced: usize = partitions.iter().map(FooterPartition::fence_len).sum();
        let gap = rows_end.checked_sub(consumed);
        let is_tiled = gap.is_some_and(|gap| gap == 0 || gap == fenced);
        if !is_tiled || listed != entry_count {
            return Err(ReelError::Corruption(
                "footer entry count is inconsistent with its length".to_string(),
            ));
        }

        Ok(SegmentFooter {
            partitions,
            min_lsn,
            max_lsn,
            sealed_at,
            tally,
        })
    }
}

/// One partition's rows in their on-disk form
///
/// A strided partition writes its rows as they stand. A varying one writes the
/// prefix-compressed block, which shares each key's front with the row before it and so
/// needs the rows already sorted.
fn encoded_partition_rows(partition: &FooterPartition) -> Result<Cow<'_, [u8]>> {
    if !partition.is_varying() {
        return Ok(Cow::Borrowed(partition.packed.as_slice()));
    }
    let mut rows = PrefixRows::new();
    for index in 0..partition.len() {
        let width = partition.key_len(index).ok_or_else(|| {
            ReelError::Corruption(
                "footer row is shorter than the tail every row carries".to_string(),
            )
        })?;
        let row = partition.row_bytes(index)?;
        rows.push(&row[..width], &row[width..])?;
    }
    let mut out = Vec::with_capacity(rows.encoded_len());
    rows.encode(&mut out);
    Ok(Cow::Owned(out))
}

/// Write one partition's directory row
///
/// The span is the encoded rows' length, handed in rather than derived, because a varying
/// partition's on-disk form is built at write time.
fn write_directory_row(buf: &mut Vec<u8>, partition: &FooterPartition, span: usize) {
    buf.push(partition.column.as_u8());
    buf.extend_from_slice(&partition.key_width.to_le_bytes());
    buf.extend_from_slice(&partition.inline_width.to_le_bytes());
    buf.extend_from_slice(&(partition.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(span as u32).to_le_bytes());
}

/// Decode the directory and take each partition's packed rows behind it
///
/// Also answers how many bytes of the rows region the partitions consumed, since the
/// in-memory form's length is not the on-disk one for prefix packed rows.
fn read_partitions(
    footer: &[u8],
    directory_at: usize,
    partition_count: usize,
) -> Result<(Vec<FooterPartition>, usize)> {
    let mut partitions = Vec::with_capacity(partition_count);
    let mut rows_at = 0usize;
    for index in 0..partition_count {
        let at = directory_at + index * DIRECTORY_ROW_LEN;
        let row = footer
            .get(at..at + DIRECTORY_ROW_LEN)
            .ok_or_else(|| ReelError::Corruption("footer directory is truncated".to_string()))?;
        let column = ColumnId(row[0]);
        let key_width = u16::from_le_bytes([row[1], row[2]]);
        let inline_width = u16::from_le_bytes([row[3], row[4]]);
        if inline_width as usize > ROW_CARRY_MAX {
            return Err(ReelError::Corruption(
                "footer partition claims more carried bytes than a row holds".to_string(),
            ));
        }
        let count = read_u32_le(&row[5..DIRECTORY_SPAN_AT]) as usize;
        let listed_span = read_u32_le(&row[DIRECTORY_SPAN_AT..DIRECTORY_ROW_LEN]) as usize;

        let mut partition = FooterPartition::new(column, key_width, inline_width);
        if partition.is_varying() {
            // The varying rows land prefix compressed, so the parse rebuilds the
            // whole-row form every in-memory reader searches.
            let blob = footer.get(rows_at..rows_at + listed_span).ok_or_else(|| {
                ReelError::Corruption("footer partition is truncated".to_string())
            })?;
            let rows = PrefixRows::decode(blob)?;
            if rows.len() != count {
                return Err(ReelError::Corruption(
                    "footer partition holds a row count its directory row denies".to_string(),
                ));
            }
            let (packed, starts) = rows.unpacked(partition.row_tail())?;
            partition.packed = packed;
            partition.starts = starts;
            rows_at += listed_span;
            partitions.push(partition);
            continue;
        }
        let span = count * partition.stride();
        let packed = footer
            .get(rows_at..rows_at + span)
            .ok_or_else(|| ReelError::Corruption("footer partition is truncated".to_string()))?;
        partition.packed = packed.to_vec();
        if partition.encoded_len() != listed_span {
            return Err(ReelError::Corruption(
                "footer partition is not the length its directory row claims".to_string(),
            ));
        }
        rows_at += span;
        partitions.push(partition);
    }
    Ok((partitions, rows_at))
}

/// Read a sealed segment's trailer without reading the footer it describes
///
/// The caller supplies the trailing bytes of the file and its full length, since every
/// offset in a footer is measured from the end. Nothing comes back for a file carrying no
/// footer; a footer whose length does not fit the file is reported as corruption.
pub fn directory_span(tail: &[u8], file_len: u64) -> Result<Option<DirectorySpan>> {
    let total = tail.len();
    if total < FIXED_TAIL_LEN {
        return Ok(None);
    }
    if read_u32_le(&tail[total - MAGIC_FROM_END..total]) != FOOTER_MAGIC {
        return Ok(None);
    }

    let footer_len =
        read_u32_le(&tail[total - FOOTER_LEN_FROM_END..total - MAGIC_FROM_END]) as usize;
    if footer_len < FIXED_TAIL_LEN || footer_len as u64 > file_len {
        return Err(ReelError::Corruption(
            "footer length is out of range".to_string(),
        ));
    }

    let partitions = read_u32_le(
        &tail[total - PARTITION_COUNT_FROM_END..total - PARTITION_COUNT_FROM_END + U32_BYTES],
    ) as usize;
    let bloom_len =
        read_u32_le(&tail[total - BLOOM_LEN_FROM_END..total - BLOOM_LEN_FROM_END + U32_BYTES])
            as usize;

    let body = footer_len - FIXED_TAIL_LEN;
    let directory_len = partitions * DIRECTORY_ROW_LEN;
    if bloom_len > body || directory_len > body - bloom_len {
        return Err(ReelError::Corruption(
            "footer directory does not fit its own length".to_string(),
        ));
    }
    Ok(Some(DirectorySpan {
        footer_len,
        directory_at: body - directory_len,
        partitions,
        filter_len: bloom_len,
    }))
}

/// Where a sealed segment's directory and filters sit, without reading its rows
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirectorySpan {
    /// Bytes the whole footer occupies at the end of the file
    pub footer_len: usize,

    /// Where the directory begins, counted from the start of the footer
    pub directory_at: usize,

    /// Partitions the directory names
    pub partitions: usize,

    /// Bytes of filters, sitting immediately below the directory
    pub filter_len: usize,
}

/// Decode a directory into where each column's rows sit in the segment file
///
/// The offsets are absolute, taken from where the footer begins, so a reader can go
/// straight to a row without holding anything between it and the file.
pub fn partition_spans(
    directory: &[u8],
    partitions: usize,
    rows_at: u64,
) -> Result<Vec<crate::format::block::PartitionSpan>> {
    let mut spans = Vec::with_capacity(partitions);
    let mut at = rows_at;
    for index in 0..partitions {
        let start = index * DIRECTORY_ROW_LEN;
        let row = directory
            .get(start..start + DIRECTORY_ROW_LEN)
            .ok_or_else(|| ReelError::Corruption("footer directory is truncated".to_string()))?;
        let inline_width = u16::from_le_bytes([row[3], row[4]]);
        if inline_width as usize > ROW_CARRY_MAX {
            return Err(ReelError::Corruption(
                "footer partition claims more carried bytes than a row holds".to_string(),
            ));
        }
        let key_width = u16::from_le_bytes([row[1], row[2]]);
        let rows = read_u32_le(&row[5..DIRECTORY_SPAN_AT]) as usize;
        let encoded = read_u32_le(&row[DIRECTORY_SPAN_AT..DIRECTORY_ROW_LEN]) as u64;
        let span = crate::format::block::PartitionSpan {
            column: ColumnId(row[0]),
            key_width,
            inline_width,
            rows,
            at,
            encoded,
        };
        at += encoded;
        spans.push(span);
    }
    Ok(spans)
}

fn read_at_u32(footer: &[u8], footer_len: usize, from_end: usize) -> u32 {
    read_u32_le(&footer[footer_len - from_end..footer_len - from_end + U32_BYTES])
}

fn verify_footer_crc(footer: &[u8], footer_len: usize) -> Result<()> {
    let crc_at = footer_len - CRC_FROM_END;
    let stored = read_u32_le(&footer[crc_at..footer_len - FOOTER_LEN_FROM_END]);

    let mut check = footer.to_vec();
    check[crc_at..crc_at + U32_BYTES].copy_from_slice(&[0u8; U32_BYTES]);

    if checksum(&check) != stored {
        return Err(ReelError::Corruption(
            "footer checksum mismatch".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const RECORD: ColumnId = ColumnId(1);
    const BLOB: ColumnId = ColumnId(2);

    fn key(column: ColumnId, byte: u8, width: usize) -> RecordKey {
        RecordKey::from_bytes(column, &vec![byte; width]).expect("key")
    }

    fn entry(
        column: ColumnId,
        byte: u8,
        width: usize,
        lsn: u64,
        offset: u32,
        len: u32,
    ) -> FooterEntry {
        let flags = if len == 0 {
            Flags::TOMBSTONE
        } else {
            Flags::DATA
        };
        FooterEntry::new(key(column, byte, width), Lsn(lsn), offset, len, flags)
    }

    fn collect(footer: &SegmentFooter) -> Vec<FooterEntry> {
        footer
            .entries()
            .map(|entry| entry.expect("entry"))
            .collect()
    }

    // shaped names spend most of their bytes on shared fronts, which packing drops
    #[test]
    fn packed_object_names_shrink_the_partition() {
        let mut rows = Vec::new();
        for at in 0..1000u32 {
            let name = format!(
                "tenants/{:032x}/exports/2026/08/02/part-{:05}.parquet",
                at / 64,
                at % 64
            );
            let named = RecordKey::from_bytes(RECORD, name.as_bytes()).expect("key");
            rows.push(FooterEntry::new(
                named,
                Lsn(at as u64 + 1),
                at * 8,
                64,
                Flags::DATA,
            ));
        }
        // A second width, so the partition varies and takes the packed form.
        rows.push(entry(RECORD, 1, 8, 2000, 0, 64));

        let mut footer = SegmentFooter::build(rows);
        let whole: usize = footer
            .partitions
            .iter()
            .map(|partition| partition.encoded_len())
            .sum();
        let bytes = footer.pack(0).expect("pack");

        assert!(
            bytes.len() < whole / 2,
            "packing saved less than half: {} of {whole}",
            bytes.len()
        );
        assert_eq!(
            SegmentFooter::parse(&bytes).expect("parse").partitions[0].len(),
            1001
        );
    }

    /// One column's partition, packed and sorted the way a seal leaves it
    fn packed(rows: Vec<FooterEntry>) -> FooterPartition {
        let mut footer = SegmentFooter::build(rows);
        let _ = footer.pack(0);
        footer.partitions.remove(0)
    }

    // a row carries a value past what an index entry holds, and reads it back
    #[test]
    fn a_row_carries_more_than_an_entry() {
        const CARRY: u16 = 200;
        let value = vec![0x5Au8; 165];
        let key = RecordKey::from_bytes(RECORD, &[7u8; 34]).expect("key");
        let mut partition = FooterPartition::new(RECORD, 34, CARRY);

        let mut entry =
            FooterEntry::new(key.clone(), Lsn(9), 4096, value.len() as u32, Flags::DATA);
        entry.inline_width = CARRY;
        entry.inline = carry_bytes(&value);
        partition.push(&entry);

        // The stride is the key, the fixed tail, and a carry region that is the
        // row's own checksum ahead of the value.
        assert_eq!(
            partition.stride(),
            34 + ENTRY_TAIL_LEN + ROW_CRC_LEN + CARRY as usize
        );

        let read = partition.entry_at(0).expect("row");
        assert_eq!(read.key, key);
        assert_eq!(read.lsn, Lsn(9));
        assert_eq!(
            read.inlined(),
            Some(value.as_slice()),
            "the row holds the whole value"
        );

        let row = partition.row_at(0).expect("row");
        assert_eq!(
            row.inlined(),
            None,
            "a value wider than an entry reads as none"
        );
    }

    // a row can be the only place a value lives, and says so
    #[test]
    fn a_row_can_stand_alone() {
        const CARRY: u16 = 200;
        let value = vec![0x5Au8; 165];
        let key = RecordKey::from_bytes(RECORD, &[7u8; 34]).expect("key");
        let entry = FooterEntry::standing_alone(key.clone(), Lsn(9), Flags::DATA, CARRY, &value)
            .expect("a value the row can hold");

        let mut partition = FooterPartition::new(RECORD, 34, CARRY);
        partition.push(&entry);

        let row = partition
            .find_row(&[7u8; 34], None)
            .expect("found")
            .expect("row");
        assert!(row.stands_alone(), "the row says it is the only copy");
        assert!(row.carries(), "and it carries the value");
        assert_eq!(
            partition.carried_at(0).expect("checked"),
            Some(value.as_slice())
        );

        // The whole footer round trips, so a sealed segment can hold rows like this.
        let mut footer = SegmentFooter::empty();
        footer.push(&entry);
        let packed = footer.pack(0).expect("pack");
        let parsed = SegmentFooter::parse(&packed).expect("parse");
        let back = parsed.partitions[0]
            .find_row(&[7u8; 34], None)
            .expect("found")
            .expect("row");
        assert!(back.stands_alone());
        assert_eq!(
            parsed.partitions[0].carried_at(0).expect("checked"),
            Some(value.as_slice())
        );
    }

    // a value the row cannot hold is refused rather than losing its record
    #[test]
    fn a_value_past_the_carry_cannot_stand_alone() {
        let key = RecordKey::from_bytes(RECORD, &[7u8; 34]).expect("key");
        assert!(
            FooterEntry::standing_alone(key.clone(), Lsn(1), Flags::DATA, 200, &[0u8; 201])
                .is_none(),
            "dropping the record for a value the row cannot hold would lose it"
        );
        assert!(
            FooterEntry::standing_alone(key, Lsn(1), Flags::DATA, 0, &[0u8; 4]).is_none(),
            "a column that carries nothing has no row to stand on"
        );
    }

    // a carried value whose bytes rotted is refused, since no record stands behind it
    #[test]
    fn a_rotted_carry_is_refused() {
        const CARRY: u16 = 200;
        let value = vec![0x5Au8; 165];
        let key = RecordKey::from_bytes(RECORD, &[7u8; 34]).expect("key");
        let mut partition = FooterPartition::new(RECORD, 34, CARRY);
        let mut entry = FooterEntry::new(key, Lsn(9), 4096, value.len() as u32, Flags::DATA);
        entry.inline_width = CARRY;
        entry.inline = carry_bytes(&value);
        partition.push(&entry);

        assert!(
            partition.carried_at(0).expect("checked").is_some(),
            "whole to begin with"
        );

        let at = 34 + ENTRY_TAIL_LEN + ROW_CRC_LEN + 3;
        partition.packed[at] ^= 0xff;

        assert!(
            partition.carried_at(0).is_err(),
            "a rotted carry is corruption"
        );
        assert!(partition
            .find_row(&[7u8; 34], None)
            .expect("found")
            .is_err());
        assert!(partition.entry_at(0).is_err());
    }

    /// Rows enough to fill several blocks, in the order a fresh segment writes them
    fn many_rows(count: u32) -> Vec<FooterEntry> {
        (0..count)
            .map(|at| {
                let mut bytes = [0u8; 34];
                bytes[..4].copy_from_slice(&at.to_be_bytes());
                let key = RecordKey::from_bytes(RECORD, &bytes).expect("key");
                FooterEntry::new(key, Lsn(u64::from(at) + 1), at * 100, 64, Flags::DATA)
            })
            .collect()
    }

    // a fenced footer costs a lead a block, and the rows come back unchanged
    #[test]
    fn a_fence_costs_a_lead_a_block() {
        let mut plain = SegmentFooter::build(many_rows(200));
        let mut fenced = SegmentFooter::build(many_rows(200));

        let bare = plain.pack(0).expect("pack");
        let leaded = fenced.pack_fenced(0, true).expect("pack");

        let blocks = fenced.partitions[0].blocks();
        assert!(blocks > 1, "one block would not exercise a fence");
        assert_eq!(
            leaded.len() - bare.len(),
            fence_bytes(blocks),
            "a fence is a lead a block and one more per page of them",
        );

        // The fence is on the blocked path alone, so parsing the footer whole reads
        // exactly what an unfenced one holds.
        assert_eq!(
            SegmentFooter::parse(&leaded).expect("parse"),
            SegmentFooter::parse(&bare).expect("parse"),
        );
    }

    // the leads ascend and each one leads the block it names
    #[test]
    fn leads_name_their_blocks() {
        let mut footer = SegmentFooter::build(many_rows(200));
        let _ = footer.pack_fenced(0, true).expect("pack");
        let partition = &footer.partitions[0];
        let region = footer.fence_region();

        let rows = partition.block_rows();
        for block in 0..partition.blocks() {
            let at = block * FENCE_LEAD;
            let first = partition.key_at(block * rows).expect("a block's first row");
            assert_eq!(
                &region[at..at + FENCE_LEAD],
                &lead_of(first)[..],
                "block {block} is led by another block's key",
            );
        }
    }

    // a fenced sorted run keeps its filter, and only zero bits take one away
    #[test]
    fn a_sorted_run_keeps_its_filter() {
        let mut sorted = SegmentFooter::build(many_rows(200));
        // The same rows with the records where a fresh segment would have put them,
        // which is arrival order rather than key order.
        let mut scattered = SegmentFooter::build(
            many_rows(200)
                .into_iter()
                .map(|mut entry| {
                    entry.offset = u32::MAX - 1 - entry.offset;
                    entry
                })
                .collect(),
        );
        let mut merged = SegmentFooter::build(many_rows(200));

        let _ = sorted.pack_fenced(10, true).expect("pack");
        let _ = scattered.pack_fenced(10, true).expect("pack");
        let _ = merged.pack_fenced(0, true).expect("pack");

        assert!(sorted.partitions[0].is_sorted_run());
        assert!(
            sorted.partitions[0].filter.is_some(),
            "a fence brackets a key without answering membership",
        );
        assert!(!scattered.partitions[0].is_sorted_run());
        assert!(scattered.partitions[0].filter.is_some());
        assert!(
            merged.partitions[0].filter.is_none(),
            "merge output is the run whose bits buy nothing",
        );
    }

    // a column offering one width strides, and offering a second one stops
    #[test]
    fn a_second_width_stops_the_striding() {
        let one = packed(vec![
            entry(RECORD, 1, 34, 1, 0, 100),
            entry(RECORD, 2, 34, 2, 100, 100),
        ]);
        assert!(!one.is_varying(), "one width strides");
        assert_eq!(one.key_width, 34);

        let two = packed(vec![
            entry(RECORD, 1, 34, 1, 0, 100),
            entry(RECORD, 2, 200, 2, 100, 100),
        ]);
        assert!(two.is_varying(), "two widths cannot stride");
        assert_eq!(two.len(), 2);
        assert_eq!(two.key_at(0).map(<[u8]>::len), Some(34));
        assert_eq!(two.key_at(1).map(<[u8]>::len), Some(200));
    }

    // rows of differing widths survive a pack and a parse with their keys intact
    #[test]
    fn varying_rows_round_trip() {
        let widths = [200usize, 8, 34, 1056, 12];
        let rows: Vec<FooterEntry> = widths
            .iter()
            .enumerate()
            .map(|(at, &width)| {
                entry(
                    RECORD,
                    at as u8 + 1,
                    width,
                    at as u64 + 1,
                    at as u32 * 8,
                    64,
                )
            })
            .collect();

        let mut footer = SegmentFooter::build(rows.clone());
        let bytes = footer.pack(10).expect("pack");
        let parsed = SegmentFooter::parse(&bytes).expect("parse");
        let partition = &parsed.partitions[0];

        assert!(partition.is_varying());
        assert_eq!(partition.len(), widths.len());

        // Every key comes back at its own width, and a search finds each one where
        // the sort left it rather than where a stride would have put it.
        for row in &rows {
            let found = partition
                .find(row.key.as_slice())
                .unwrap_or_else(|| panic!("a {} byte key is missing", row.key.width()))
                .expect("entry");
            assert_eq!(found.key, row.key);
            assert_eq!(found.lsn, row.lsn);
            assert_eq!(found.offset, row.offset);
            assert!(
                partition.may_hold(row.key.as_slice()),
                "the filter denies a key the partition holds",
            );
        }

        // The rows are in key order, which is what every search above relies on.
        for at in 1..partition.len() {
            assert!(
                partition.key_at(at - 1) <= partition.key_at(at),
                "row {at} sorts below its predecessor",
            );
        }
    }

    // a strided column beside a varying one keeps its own shape
    #[test]
    fn one_varying_column_leaves_the_others_strided() {
        let mut footer = SegmentFooter::build(vec![
            entry(RECORD, 1, 34, 1, 0, 100),
            entry(RECORD, 2, 34, 2, 100, 100),
            entry(BLOB, 3, 8, 3, 200, 100),
            entry(BLOB, 4, 300, 4, 300, 100),
        ]);
        let bytes = footer.pack(0).expect("pack");
        let parsed = SegmentFooter::parse(&bytes).expect("parse");

        let by_column = |column| {
            parsed
                .partitions
                .iter()
                .find(|found| found.column == column)
                .expect("partition")
        };
        let record = by_column(RECORD);
        let blob = by_column(BLOB);
        assert!(!record.is_varying(), "a one width column still strides");
        assert!(blob.is_varying(), "the column with two widths does not");
        assert_eq!(record.len(), 2);
        assert_eq!(blob.len(), 2);
        assert_eq!(blob.key_at(0).map(<[u8]>::len), Some(8));
        assert_eq!(blob.key_at(1).map(<[u8]>::len), Some(300));
    }

    // a key written once is a run of one, at either end of the partition
    #[test]
    fn a_key_written_once_stands_alone() {
        let rows = packed(vec![
            entry(RECORD, 1, 34, 1, 0, 100),
            entry(RECORD, 2, 34, 2, 100, 100),
            entry(RECORD, 3, 34, 3, 200, 100),
        ]);

        assert_eq!(rows.run(0), Some((0, 0)));
        assert_eq!(rows.run(1), Some((1, 1)));
        assert_eq!(
            rows.run(2),
            Some((2, 2)),
            "the last row has no neighbour above"
        );
        assert_eq!(rows.run(3), None, "past the rows there is no run");
    }

    // every rewritten key keeps the order its versions arrived in
    #[test]
    fn every_rewritten_key_keeps_its_version_order() {
        const KEYS: u64 = 512;
        const VERSIONS: u64 = 8;

        // Round robin, so a key's versions are KEYS apart rather than adjacent.
        let mut rows = Vec::new();
        let mut lsn = 1u64;
        for version in 0..VERSIONS {
            for key in 0..KEYS {
                rows.push(entry(RECORD, key as u8, 34, lsn, lsn as u32 * 100, 100));
                let _ = version;
                lsn += 1;
            }
        }
        let rows = packed(rows);

        // Keys are one byte wide here, so KEYS above 256 collapses into 256 runs of
        // more versions each, which is the same property under a denser shape.
        let mut checked = 0usize;
        let mut at = 0usize;
        while at < rows.len() {
            let (first, last) = rows.run(at).expect("a run");
            let found: Vec<u64> = (first..=last)
                .map(|row| rows.row_at(row).expect("row").lsn.as_u64())
                .collect();
            let mut wanted = found.clone();
            wanted.sort_unstable();
            assert_eq!(found, wanted, "a key's versions came back out of order");
            checked += 1;
            at = last + 1;
        }
        assert!(
            checked > 1,
            "the shape collapsed to one run and proved nothing"
        );
    }

    // a key whose versions arrived newest first still ends its run on the newest
    #[test]
    fn a_run_ends_on_the_newest_however_it_arrived() {
        // One key, versions appended newest first, which is what an out of order
        // completion looks like once the footer is packed.
        let mut rows = Vec::new();
        for lsn in [596u64, 590, 584] {
            rows.push(entry(RECORD, 7, 34, lsn, lsn as u32 * 10, 100));
        }
        let rows = packed(rows);

        let (first, last) = rows.run(0).expect("a run");
        assert_eq!(last - first, 2, "the three versions are one run");
        assert_eq!(
            rows.row_at(last).expect("row").lsn.as_u64(),
            596,
            "the run ended on a version the store had already replaced"
        );
    }

    // a key a segment rewrote is one run however far into it the caller stands
    #[test]
    fn a_rewritten_key_is_one_run_from_either_end() {
        let rows = packed(vec![
            entry(RECORD, 1, 34, 1, 0, 100),
            entry(RECORD, 5, 34, 2, 100, 100),
            entry(RECORD, 5, 34, 3, 200, 100),
            entry(RECORD, 5, 34, 4, 300, 100),
            entry(RECORD, 9, 34, 5, 400, 100),
        ]);

        assert_eq!(rows.run(1), Some((1, 3)), "from the first of the run");
        assert_eq!(rows.run(2), Some((1, 3)), "from the middle of it");
        assert_eq!(rows.run(3), Some((1, 3)), "from the last of it");
        assert_eq!(rows.run(0), Some((0, 0)), "the run below is its own");
        assert_eq!(rows.run(4), Some((4, 4)), "and so is the one above");

        // The live row is the last of the run, which is what the search agrees on.
        let live = rows.run(1).expect("run").1;
        assert_eq!(rows.row_at(live).expect("row").lsn, Lsn(4));
        assert_eq!(
            rows.find(&[5u8; 34]).expect("found").expect("row").lsn,
            Lsn(4),
            "and what a point lookup answers with"
        );
    }

    // a footer round trips its rows, sorted within a column, with its bounds
    #[test]
    fn roundtrip() {
        let mut footer = SegmentFooter::build(vec![
            entry(RECORD, 0x22, 34, 30, 200, 1600),
            entry(RECORD, 0x11, 34, 10, 0, 900),
            entry(RECORD, 0x99, 34, 20, 56, 0),
        ]);

        let packed = footer.pack(0).expect("pack");
        let parsed = SegmentFooter::parse(&packed).expect("parse");

        assert_eq!(parsed, footer);
        assert_eq!(parsed.min_lsn, Lsn(10));
        assert_eq!(parsed.max_lsn, Lsn(30));
        let rows = collect(&parsed);
        assert!(rows[0].key < rows[1].key);
        assert!(rows[1].key < rows[2].key);
    }

    // columns keep their own partition, each striding at its own key width
    #[test]
    fn partitions_by_column() {
        let mut footer = SegmentFooter::build(vec![
            entry(BLOB, 0x44, 32, 12, 700, 4096),
            entry(RECORD, 0x33, 34, 11, 400, 1600),
            entry(BLOB, 0x11, 32, 13, 900, 512),
        ]);

        let packed = footer.pack(0).expect("pack");
        let parsed = SegmentFooter::parse(&packed).expect("parse");

        assert_eq!(parsed.partitions.len(), 2);
        assert_eq!(parsed.partitions[0].column, RECORD);
        assert_eq!(parsed.partitions[0].key_width, 34);
        assert_eq!(parsed.partitions[0].stride(), 34 + ENTRY_TAIL_LEN);
        assert_eq!(parsed.partitions[1].column, BLOB);
        assert_eq!(parsed.partitions[1].len(), 2);
        assert_eq!(parsed.entry_count(), 3);
    }

    // a narrow column costs its own width per row, not the widest column's
    #[test]
    fn width_sets_the_cost() {
        let mut narrow = SegmentFooter::build(vec![entry(BLOB, 0x01, 24, 1, 0, 10)]);
        let mut wide = SegmentFooter::build(vec![entry(RECORD, 0x01, 108, 1, 0, 10)]);

        assert_eq!(
            wide.pack(0).expect("pack").len() - narrow.pack(0).expect("pack").len(),
            108 - 24,
        );
    }

    // a tombstone entry survives with a zero length beside a data entry
    #[test]
    fn tombstone_entry() {
        let mut footer = SegmentFooter::build(vec![
            entry(RECORD, 0x01, 34, 5, 0, 1600),
            entry(RECORD, 0x02, 34, 6, 1656, 0),
        ]);

        let packed = footer.pack(0).expect("pack");
        let parsed = SegmentFooter::parse(&packed).expect("parse");
        let rows = collect(&parsed);

        assert_eq!(rows[1].len, 0);
        assert!(rows[1].is_tombstone());
        assert_eq!(rows.len(), 2);
    }

    // a range tombstone is listed with the length of the end key it names
    #[test]
    fn range_tombstone_entry() {
        let row = FooterEntry::new(
            key(RECORD, 0x10, 34),
            Lsn(4),
            96,
            34,
            Flags::RANGE_TOMBSTONE,
        );
        let mut footer = SegmentFooter::build(vec![row]);

        let packed = footer.pack(0).expect("pack");
        let parsed = SegmentFooter::parse(&packed).expect("parse");
        let rows = collect(&parsed);

        assert!(rows[0].is_range_tombstone());
        assert_eq!(rows[0].len, 34);
    }

    // an empty footer round trips
    #[test]
    fn empty_footer() {
        let mut footer = SegmentFooter::empty();
        let packed = footer.pack(0).expect("pack");
        let parsed = SegmentFooter::parse(&packed).expect("parse");

        assert!(parsed.is_empty());
        assert!(parsed.partitions.is_empty());
        assert_eq!(parsed.min_lsn, Lsn::NONE);
        assert_eq!(parsed.max_lsn, Lsn::NONE);
        assert_eq!(packed.len(), FIXED_TAIL_LEN);
    }

    // a footer parses when it trails other segment bytes
    #[test]
    fn parses_with_prefix() {
        let mut footer = SegmentFooter::build(vec![entry(RECORD, 0x40, 34, 7, 88, 1600)]);
        let mut segment = vec![0xcd; 5000];
        segment.extend_from_slice(&footer.pack(0).expect("pack"));

        let parsed = SegmentFooter::parse(&segment).expect("parse");

        assert_eq!(parsed, footer);
    }

    // a flipped magic byte is not a sealed footer
    #[test]
    fn corrupt_magic() {
        let mut footer = SegmentFooter::build(vec![entry(RECORD, 0x01, 34, 1, 0, 10)]);
        let mut packed = footer.pack(0).expect("pack");
        let last = packed.len() - 1;

        packed[last] ^= 0x01;

        assert!(SegmentFooter::parse(&packed).is_err());
    }

    // a truncated footer fails before it can be trusted
    #[test]
    fn truncated_footer() {
        let mut footer = SegmentFooter::build(vec![entry(RECORD, 0x01, 34, 1, 0, 10)]);
        let packed = footer.pack(0).expect("pack");

        assert!(SegmentFooter::parse(&packed[..packed.len() - 1]).is_err());
        assert!(SegmentFooter::parse(&packed[..4]).is_err());
    }

    // a flipped entry byte fails the footer checksum
    #[test]
    fn corrupt_entry() {
        let mut footer = SegmentFooter::build(vec![entry(RECORD, 0x01, 34, 1, 0, 10)]);
        let mut packed = footer.pack(0).expect("pack");

        packed[0] ^= 0x01;

        assert!(SegmentFooter::parse(&packed).is_err());
    }

    // a mixed record stream keeps only the listed kinds in the footer
    #[test]
    fn excludes_pads_and_headers() {
        let key_a = key(RECORD, 0x11, 34);
        let key_b = key(RECORD, 0x22, 34);

        let records = vec![
            (RecordHeader::segment_header(&[0u8; 8]), 0u32),
            (
                RecordHeader::data(key_a.clone(), Lsn(5), &[0xab; 100]),
                4096,
            ),
            (RecordHeader::pad(4252), 4252),
            (RecordHeader::tombstone(key_b.clone(), Lsn(7)), 8248),
        ];

        let mut footer = SegmentFooter::empty();
        for (header, offset) in records {
            if let Some(entry) = FooterEntry::from_record(&header, offset, &[], 0) {
                footer.push(&entry);
            }
        }

        assert_eq!(footer.entry_count(), 2);
        assert_eq!(footer.min_lsn, Lsn(5));
        assert_eq!(footer.max_lsn, Lsn(7));

        let rows = collect(&footer);
        let data_entry = rows
            .iter()
            .find(|found| found.key == key_a)
            .expect("data entry");
        let tomb_entry = rows
            .iter()
            .find(|found| found.key == key_b)
            .expect("tombstone entry");

        assert_eq!(data_entry.len, 100);
        assert_eq!(data_entry.offset, 4096);
        assert_eq!(tomb_entry.len, 0);
        assert_eq!(tomb_entry.offset, 8248);

        let packed = footer.pack(0).expect("pack");
        assert_eq!(SegmentFooter::parse(&packed).expect("parse"), footer);
    }

    // a footer whose row count disagrees with its body is rejected past the checksum
    #[test]
    fn inconsistent_entry_count() {
        let mut footer = SegmentFooter::build(vec![entry(RECORD, 0x01, 34, 1, 0, 10)]);
        let mut packed = footer.pack(0).expect("pack");
        let total = packed.len();

        let count_at = total - ENTRY_COUNT_FROM_END;
        packed[count_at..count_at + U32_BYTES].copy_from_slice(&2u32.to_le_bytes());
        reseal(&mut packed);

        assert!(SegmentFooter::parse(&packed).is_err());
    }

    // a directory claiming more partitions than the body holds is rejected
    #[test]
    fn inconsistent_partition_count() {
        let mut footer = SegmentFooter::build(vec![entry(RECORD, 0x01, 34, 1, 0, 10)]);
        let mut packed = footer.pack(0).expect("pack");
        let total = packed.len();

        let count_at = total - PARTITION_COUNT_FROM_END;
        packed[count_at..count_at + U32_BYTES].copy_from_slice(&64u32.to_le_bytes());
        reseal(&mut packed);

        assert!(SegmentFooter::parse(&packed).is_err());
    }

    // a corrupt length field is caught before any large read
    #[test]
    fn corrupt_length() {
        let mut footer = SegmentFooter::build(vec![entry(RECORD, 0x01, 34, 1, 0, 10)]);
        let mut packed = footer.pack(0).expect("pack");
        let total = packed.len();

        let footer_len_at = total - FOOTER_LEN_FROM_END;
        packed[footer_len_at..footer_len_at + U32_BYTES].copy_from_slice(&u32::MAX.to_le_bytes());

        assert!(SegmentFooter::parse(&packed).is_err());
    }

    fn reseal(packed: &mut [u8]) {
        let total = packed.len();
        let crc_at = total - CRC_FROM_END;
        packed[crc_at..crc_at + U32_BYTES].copy_from_slice(&[0u8; U32_BYTES]);
        let crc = checksum(packed);
        packed[crc_at..crc_at + U32_BYTES].copy_from_slice(&crc.to_le_bytes());
    }
}
