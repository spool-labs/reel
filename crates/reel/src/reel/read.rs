//! Planning and framing for the reads a reel serves off its segments

use crate::error::{ReelError, Result};

use crate::format::column::{ColumnId, KeyRef};
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::Lsn;
use crate::format::record::{RecordHeader, HEADER_LEN};
use crate::reel::segment::{SegmentHandle, SplitAnswer, SplitRead};

use super::{is_missing, recycle_header, RecordRead};
use reel_core::Value;

/// A whole framed record, or nothing when the segment no longer holds one there
///
/// A short read hands both buffers back where they came from, and a segment that is
/// gone reads as nothing rather than as an error.
pub(super) fn framed_or_nothing(
    read: SplitAnswer,
    prefix: usize,
    len: usize,
) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    match read {
        Ok((head, body)) if head.len() == prefix && body.len() == len => Ok(Some((head, body))),
        Ok((head, body)) => {
            recycle_header(head);
            crate::reel::payload::give(body);
            Ok(None)
        }
        Err((error, spare)) => {
            recycle_header(spare);
            match is_missing(&error) {
                true => Ok(None),
                false => Err(error),
            }
        }
    }
}

/// Where a payload window begins on the volume
pub(super) fn window_start(loc: Loc, key_width: u16, at: u64) -> u64 {
    u64::from(loc.offset) + HEADER_LEN as u64 + u64::from(key_width) + at
}

/// A whole window, or nothing when the volume cannot answer it
///
/// A short read and a missing segment both read as nothing rather than as an error,
/// since the caller has a header-checked read to fall back to.
pub(super) fn window_or_nothing(read: Result<Vec<u8>>, len: usize) -> Result<Option<Value>> {
    match read {
        Ok(bytes) if bytes.len() == len => {
            Ok(Some(Value::pooled(bytes, crate::reel::payload::give)))
        }
        Ok(bytes) => {
            crate::reel::payload::give(bytes);
            Ok(None)
        }
        Err(error) if is_missing(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Whether an on-disk record is the one an index pointer claimed to name
///
/// The sequence number is part of the comparison because the other fields are equal
/// across two versions of the same key at the same length. Compaction copies a
/// record under the sequence number it copied, so this holds across a relocation.
pub(super) fn header_matches(
    header: &RecordHeader,
    expected: KeyRef<'_>,
    lsn: Lsn,
    loc: Loc,
) -> bool {
    header.flags.is_data()
        && header.key.column == expected.column
        && header.key.as_slice() == expected.bytes
        && header.lsn == lsn
        && header.length == loc.len
}

/// Bytes of gap a merged read spans rather than breaking the run
///
/// Reading a small gap costs the bytes and saves the round trip.
pub(super) const MERGE_GAP: u64 = 4 * 1024;

/// Bytes one merged read reaches before the run is broken
///
/// A bound on what a single answer can hold, since every window cut from a block
/// keeps the whole block alive until it drops.
const MERGE_SPAN: u64 = 1024 * 1024;

/// One record's place in a batch, resolved before anything is submitted
pub(super) struct Planned {
    pub(super) at: usize,
    pub(super) segment: SegmentId,
    pub(super) handle: SegmentHandle,
    pub(super) offset: u64,
    pub(super) prefix: usize,
    pub(super) len: usize,
}

impl Planned {
    /// Where this record ends on the volume
    pub(super) fn end(&self) -> u64 {
        self.offset + (self.prefix + self.len) as u64
    }
}

/// A stretch of the plan answered by one read
#[derive(Clone, Copy)]
pub(super) struct Run {
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) span: u64,
}

impl Run {
    /// Whether this run is one record, which is read framed rather than cut
    pub(super) fn is_single(&self) -> bool {
        self.end - self.start == 1
    }
}

/// Group records that sit next to each other into the reads that will serve them
///
/// Only forward, only within one segment, and only across a small gap: a run that
/// went backwards or jumped would read the bytes between for nothing.
pub(super) fn merge_runs_into(plan: &[Planned], runs: &mut Vec<Run>) {
    runs.clear();
    let mut start = 0usize;
    while start < plan.len() {
        let mut end = start + 1;
        while end < plan.len() && joins(&plan[end - 1], &plan[end], plan[start].offset) {
            end += 1;
        }
        runs.push(Run {
            start,
            end,
            span: plan[end - 1].end() - plan[start].offset,
        });
        start = end;
    }
}

/// Whether the next record can ride the same read as the one before it
pub(super) fn joins(last: &Planned, next: &Planned, from: u64) -> bool {
    last.segment == next.segment
        && next.offset >= last.end()
        && next.offset - last.end() <= MERGE_GAP
        && next.end() - from <= MERGE_SPAN
}

/// Check one record framed inside a merged read, yielding why it was rejected
///
/// Nothing rather than a verdict means the record is good and its window stands.
pub(super) fn check_in_block(
    block: &[u8],
    at: usize,
    held: &Planned,
    expected: KeyRef<'_>,
    lsn: Lsn,
    loc: Loc,
    is_verified: bool,
) -> std::result::Result<u8, RecordRead> {
    let Some(body_at) = at.checked_add(held.prefix) else {
        return Err(RecordRead::Stale);
    };
    let Some(body_end) = body_at.checked_add(held.len) else {
        return Err(RecordRead::Stale);
    };
    if body_end > block.len() {
        return Err(RecordRead::Stale);
    }
    let header = match RecordHeader::unpack(&block[at..body_at]) {
        Ok(header) => header,
        Err(_) => return Err(RecordRead::Stale),
    };
    if !header_matches(&header, expected, lsn, loc) {
        return Err(RecordRead::Stale);
    }
    if is_verified && !header.verify(&block[body_at..body_end]) {
        return Err(RecordRead::Corrupt);
    }
    Ok(header.codec)
}

/// Decide what a framed record read means, once the bytes are in hand
pub(super) fn frame_to_read(
    head: Vec<u8>,
    body: Vec<u8>,
    expected: KeyRef<'_>,
    lsn: Lsn,
    loc: Loc,
    is_verified: bool,
) -> RecordRead {
    // Wrapped before anything can return, so a record the checks reject still
    // hands its buffer back to the pool rather than to the allocator.
    let body = Value::pooled(body, crate::reel::payload::give);
    let unpacked = RecordHeader::unpack(&head);
    recycle_header(head);
    let header = match unpacked {
        Ok(header) => header,
        Err(_) => return RecordRead::Stale,
    };
    if !header_matches(&header, expected, lsn, loc) {
        return RecordRead::Stale;
    }
    if is_verified && !header.verify(&body) {
        return RecordRead::Corrupt;
    }
    if header.codec != 0 {
        // A decode that fails is corruption wearing a valid checksum, answered
        // exactly as a failed checksum is.
        return match crate::append::codec::decode(header.codec, &body) {
            Some(decoded) => RecordRead::Found(Value::pooled(decoded, crate::reel::payload::give)),
            None => RecordRead::Corrupt,
        };
    }
    RecordRead::Found(body)
}

/// Frame a range that came back on the record header's own read
pub(super) fn near_range(
    read: SplitAnswer,
    prefix: usize,
    at: u64,
    len: usize,
    expected: KeyRef<'_>,
    lsn: Lsn,
    loc: Loc,
) -> Result<RecordRead> {
    let span = at as usize + len;
    let (head, block) = match framed_or_nothing(read, prefix, span)? {
        Some(framed) => framed,
        None => return Ok(RecordRead::Stale),
    };
    let verdict = match cut_range(block, at as usize, len) {
        Some(body) => frame_to_range(&head, body, expected, lsn, loc),
        None => Ok(RecordRead::Stale),
    };
    recycle_header(head);
    verdict
}

/// The window of a block a range asked for, or nothing when the block is short
///
/// One window and no neighbours, so the value owns the block outright rather than
/// sharing it by refcount.
pub(super) fn cut_range(block: Vec<u8>, at: usize, len: usize) -> Option<Value> {
    Value::cut(block, crate::reel::payload::give, at, len)
}

/// Turn a deep range's two reads into one answer
///
/// Either read coming back short says the same thing a short framed read does: the
/// segment no longer holds what the pointer described.
pub(super) fn deep_range(
    filled: Vec<SplitRead>,
    prefix: usize,
    len: usize,
    expected: KeyRef<'_>,
    lsn: Lsn,
    loc: Loc,
) -> Result<RecordRead> {
    // Taken apart in place: a ranged read is always exactly its header and its
    // window, so there is nothing to stage them through.
    let Ok([first, second]) = <[SplitRead; 2]>::try_from(filled) else {
        return Err(ReelError::Backend(
            "a ranged read came back with something other than its two reads".to_string(),
        ));
    };
    let head = match ranged_bytes(first)? {
        Some(bytes) => bytes,
        // A segment retired under the read, which a reader can lose without
        // anything being wrong with the record.
        None => return Ok(RecordRead::Stale),
    };
    let range = match ranged_bytes(second) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            crate::reel::payload::give(head);
            return Ok(RecordRead::Stale);
        }
        Err(error) => {
            crate::reel::payload::give(head);
            return Err(error);
        }
    };
    if head.len() != prefix || range.len() != len {
        crate::reel::payload::give(head);
        crate::reel::payload::give(range);
        return Ok(RecordRead::Stale);
    }

    let body = Value::pooled(range, crate::reel::payload::give);
    let verdict = frame_to_range(&head, body, expected, lsn, loc);
    crate::reel::payload::give(head);
    verdict
}

/// What one read of a ranged pair filled, or nothing when its segment is gone
///
/// The header buffer a split read carries is empty here, since both reads of a deep
/// range are whole-body reads, so it goes straight back to the pool.
fn ranged_bytes(read: SplitRead) -> Result<Option<Vec<u8>>> {
    match read {
        Ok((empty, body)) => {
            crate::reel::payload::give(empty);
            Ok(Some(body))
        }
        Err(error) if is_missing(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Decide what a ranged read means, once its bytes and its record's header are in hand
///
/// What is missing is the checksum, which covers bytes this read does not hold.
pub(super) fn frame_to_range(
    head: &[u8],
    body: Value,
    expected: KeyRef<'_>,
    lsn: Lsn,
    loc: Loc,
) -> Result<RecordRead> {
    let header = match RecordHeader::unpack(head) {
        Ok(header) => header,
        Err(_) => return Ok(RecordRead::Stale),
    };
    if !header_matches(&header, expected, lsn, loc) {
        return Ok(RecordRead::Stale);
    }
    // A record carrying the byte here was written when the column declared a codec
    // and read back after it stopped. Its stored bytes are not the ones the caller
    // is addressing and are not corrupt either, so it is reported rather than cut.
    if header.codec != 0 {
        return Err(coded_range(expected.column));
    }
    Ok(RecordRead::Found(body))
}

/// A range asked of bytes a codec produced
pub fn coded_range(column: ColumnId) -> ReelError {
    ReelError::CodedRange(format!(
        "column {} stores what a codec produced, and a codec frame decodes whole",
        column.as_u8()
    ))
}
