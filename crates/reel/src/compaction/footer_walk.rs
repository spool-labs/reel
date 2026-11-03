//! A compaction pass's walk over a sealed footer's rows, read in place where the segment maps

use std::sync::Arc;

use crate::compaction::compactor::TRAILER_LEN;
use crate::error::{ReelError, Result};
use crate::format::block::PartitionSpan;
use crate::format::column::{ColumnId, RecordKey};
use crate::format::footer::{check_in_place, FooterRow, SegmentFooter, VARYING_WIDTH};
use crate::format::loc::{Loc, SegmentId};
use crate::format::prefix::{PackedCursor, RESTART_INTERVAL, TRAILER_LEN as PACKED_TRAILER_LEN};
use crate::format::record::{read_u32_le, HEADER_LEN};
use crate::index::map::ReelIndex;
use crate::index::paged::FooterSource;
use crate::io::mapping::Mapping;
use crate::reel::segment::SegmentHandle;
use crate::reel::ReelShared;

/// One rewrite stripe holds at most this many payload bytes between fetch and apply
pub const STRIPE_STAGE_BYTES: u64 = 256 * 1024 * 1024;

/// One rewrite stripe holds at most this many records, so small records stage a bounded list
pub const STRIPE_ROWS: usize = 64 * 1024;

/// A sealed segment's footer rows as a compaction pass reads them
pub enum FooterRows {
    /// A footer parsed whole, cached already or read for a segment that does not map
    Whole(Arc<SegmentFooter>),

    /// Each column's rows read where they lie in the segment's mapping
    Mapped {
        map: Arc<Mapping>,
        columns: Vec<MappedColumn>,
    },
}

/// Where one column's rows lie in a mapped footer
pub struct MappedColumn {
    column: ColumnId,
    key_width: u16,
    at: u64,
    rows: usize,
    shape: Shape,
}

/// How a mapped column's rows lie
enum Shape {
    /// At a fixed stride, each key `key_width` wide
    Strided { stride: usize, key_width: usize },

    /// Prefix packed, its restart table at `rows_end` past the rows
    Packed { rows_end: u32 },
}

impl FooterRows {
    /// A sealed segment's rows and their key prefix bound, or nothing without a footer
    ///
    /// The bound is missing when a row does not decode
    pub fn open(
        shared: &Arc<ReelShared>,
        segment: SegmentId,
        source: &SegmentHandle,
        file_len: u64,
    ) -> Result<Option<(FooterRows, Option<u64>)>> {
        if shared.footers.get(segment).is_none() {
            if let Some(mapped) = FooterRows::mapped(shared, source, file_len) {
                // A row that does not decode in place is left to the whole parse to settle
                if let Some(bound) = mapped.prefix_bound() {
                    return Ok(Some((mapped, Some(bound))));
                }
            }
        }
        Ok(shared.footer_once(segment)?.map(|footer| {
            let rows = FooterRows::Whole(footer);
            let bound = rows.prefix_bound();
            (rows, bound)
        }))
    }

    /// The footer read in place through the segment's mapping, or nothing when it fails a check
    fn mapped(shared: &ReelShared, source: &SegmentHandle, file_len: u64) -> Option<FooterRows> {
        let map = source.shared_mapping(shared.config.segment_bytes.to_bytes())?;
        let trailer = map.slice(file_len.checked_sub(TRAILER_LEN)?, TRAILER_LEN as usize)?;
        let footer_len = u64::from(read_u32_le(&trailer[0..4]));
        let opens = file_len.checked_sub(footer_len)?;
        let footer = map.slice(opens, footer_len as usize)?;
        let spans = check_in_place(footer, opens).ok()?;
        let columns = spans
            .iter()
            .map(|span| MappedColumn::new(&map, span))
            .collect::<Option<Vec<_>>>()?;
        Some(FooterRows::Mapped { map, columns })
    }

    /// How many columns the footer holds
    fn columns(&self) -> usize {
        match self {
            FooterRows::Whole(footer) => footer.partitions.len(),
            FooterRows::Mapped { columns, .. } => columns.len(),
        }
    }

    /// One column's id, its row count, and the width its keys are declared at
    fn column(&self, part: usize) -> (ColumnId, usize, u16) {
        match self {
            FooterRows::Whole(footer) => {
                let partition = &footer.partitions[part];
                (partition.column, partition.len(), partition.key_width)
            }
            FooterRows::Mapped { columns, .. } => {
                let held = &columns[part];
                (held.column, held.rows, held.key_width)
            }
        }
    }

    /// The key and row at a place in one column, a packed one decoded by `cursor`
    fn read<'a>(
        &'a self,
        cursor: &'a mut PackedCursor,
        part: usize,
        row: usize,
    ) -> Result<(&'a [u8], FooterRow)> {
        match self {
            FooterRows::Whole(footer) => {
                let partition = &footer.partitions[part];
                Ok((partition.key_at(row).unwrap_or(&[]), partition.row_at(row)?))
            }
            FooterRows::Mapped { map, columns } => columns[part].read(map, cursor, row),
        }
    }

    /// The header and the widest key, which bound a record's prefix, or nothing on a bad row
    pub fn prefix_bound(&self) -> Option<u64> {
        let mut widest = 0usize;
        for part in 0..self.columns() {
            let (_, rows, declared) = self.column(part);
            let mut cursor = PackedCursor::default();
            for row in 0..rows {
                let (key, _) = self.read(&mut cursor, part, row).ok()?;
                if declared == VARYING_WIDTH {
                    widest = widest.max(key.len());
                }
            }
            if declared != VARYING_WIDTH {
                widest = widest.max(declared as usize);
            }
        }
        Some(HEADER_LEN as u64 + widest as u64)
    }
}

impl MappedColumn {
    /// One column's rows in a mapping, a packed one checked against its trailer
    fn new(map: &Mapping, span: &PartitionSpan) -> Option<MappedColumn> {
        let shape = match span.is_packed {
            false => Shape::Strided {
                stride: span.stride(),
                key_width: span.key_width as usize,
            },
            true => {
                let restarts = span.rows.div_ceil(RESTART_INTERVAL);
                let wanted = (restarts * 4 + PACKED_TRAILER_LEN) as u64;
                let trailer_at = span.at + span.encoded.checked_sub(PACKED_TRAILER_LEN as u64)?;
                let trailer = map.slice(trailer_at, PACKED_TRAILER_LEN)?;
                if read_u32_le(&trailer[0..4]) as usize != restarts
                    || read_u32_le(&trailer[4..8]) as usize != span.rows
                {
                    return None;
                }
                Shape::Packed {
                    rows_end: u32::try_from(span.encoded.checked_sub(wanted)?).ok()?,
                }
            }
        };
        Some(MappedColumn {
            column: span.column,
            key_width: span.key_width,
            at: span.at,
            rows: span.rows,
            shape,
        })
    }

    /// The key and row at a place in the column
    fn read<'a>(
        &self,
        map: &'a Mapping,
        cursor: &'a mut PackedCursor,
        row: usize,
    ) -> Result<(&'a [u8], FooterRow)> {
        let missing = || ReelError::Corruption(format!("a footer read past its rows at {row}"));
        if row >= self.rows {
            return Err(missing());
        }
        match self.shape {
            Shape::Strided { stride, key_width } => {
                let bytes = map
                    .slice(self.at + (row * stride) as u64, stride)
                    .ok_or_else(missing)?;
                Ok((&bytes[..key_width], FooterRow::read(bytes, key_width)?))
            }
            Shape::Packed { rows_end } => {
                let bytes = map.slice(self.at, rows_end as usize).ok_or_else(missing)?;
                let table = self.at + u64::from(rows_end);
                let restart = |block: usize| {
                    map.slice(table + (block * 4) as u64, 4)
                        .map(read_u32_le)
                        .filter(|start| *start < rows_end)
                };
                cursor.read(bytes, restart, row)
            }
        }
    }
}

/// A row a rewrite reads: its offset and length, and for a keyless segment its key and row
pub type KeptRow = ((u32, u32), Option<(RecordKey, FooterRow)>);

/// Walks a footer's rows in order, a stripe at a time, keeping the rows the index still points at
pub struct LiveWalk<'a> {
    rows: &'a FooterRows,
    part: usize,
    row: usize,
    cursor: PackedCursor,
    held: Option<KeptRow>,
}

impl<'a> LiveWalk<'a> {
    pub fn new(rows: &'a FooterRows) -> LiveWalk<'a> {
        LiveWalk {
            rows,
            part: 0,
            row: 0,
            cursor: PackedCursor::default(),
            held: None,
        }
    }

    /// Fill the next stripe's offsets and lengths, and the keyless rows beside them
    ///
    /// The stripe takes at least one row, then stops before `STRIPE_STAGE_BYTES` or `STRIPE_ROWS`
    pub fn fill(
        &mut self,
        index: &ReelIndex,
        segment: SegmentId,
        is_keyless: bool,
        order: &mut Vec<(u32, u32)>,
        kept: &mut Vec<(RecordKey, FooterRow)>,
    ) -> Result<()> {
        order.clear();
        kept.clear();
        let mut staged = 0u64;
        loop {
            let next = match self.held.take() {
                Some(next) => next,
                None => match self.next_kept(index, segment, is_keyless)? {
                    Some(next) => next,
                    None => return Ok(()),
                },
            };
            let len = u64::from(next.0 .1);
            let is_full = staged + len > STRIPE_STAGE_BYTES || order.len() >= STRIPE_ROWS;
            if !order.is_empty() && is_full {
                self.held = Some(next);
                return Ok(());
            }
            staged += len;
            order.push(next.0);
            if let Some(row) = next.1 {
                kept.push(row);
            }
        }
    }

    /// The next row the index still points at, or any tombstone
    fn next_kept(
        &mut self,
        index: &ReelIndex,
        segment: SegmentId,
        is_keyless: bool,
    ) -> Result<Option<KeptRow>> {
        let rows = self.rows;
        while self.part < rows.columns() {
            let (column, count, _) = rows.column(self.part);
            if self.row >= count {
                self.part += 1;
                self.row = 0;
                self.cursor = PackedCursor::default();
                continue;
            }
            let at = self.row;
            self.row += 1;
            let (key, row) = rows.read(&mut self.cursor, self.part, at)?;
            let key = RecordKey::from_bytes(column, key)?;
            let loc = Loc::new(segment, row.offset, row.len);
            if row.flags.is_data() && !index.is_live_at(&key, loc, row.lsn)? {
                continue;
            }
            return Ok(Some((
                (row.offset, row.len),
                is_keyless.then_some((key, row)),
            )));
        }
        Ok(None)
    }
}
