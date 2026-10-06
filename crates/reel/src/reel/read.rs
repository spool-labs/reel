//! Planning and framing for the reads a reel serves off its segments

use crate::error::{ReelError, Result};

use crate::format::column::KeyRef;
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::Lsn;
use crate::format::record::{check_keyless, data_codec, CheckKey, Flags, KeylessRead, RecordHeader, RecordLayout};
use crate::io::direct::{DIRECT_ALIGN, DIRECT_REQUEST_BYTES};
use crate::io::op::FileId;
use crate::io::ServingBackend;
use crate::reel::segment::{SplitAnswer, SplitRead};

use super::{is_missing, recycle_header, Ask, ReadScratch, RecordRead, Spot};
use reel_core::{ReadBlock, Value};

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

/// Where a payload window begins on the volume, behind a record prefix this long
pub(super) fn window_start(loc: Loc, prefix: usize, at: u64) -> u64 {
    u64::from(loc.offset) + prefix as u64 + at
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

/// Whether a record's header and key hash to the checksum it was written with
fn is_intact(prefix: &[u8], payload: &[u8]) -> bool {
    RecordHeader::unpack(prefix).is_ok_and(|header| header.verify(payload))
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

/// The same bound on a volume whose reads bypass the page cache
///
/// A read the registered buffer cannot serve leaves the ring for the posix path a record
/// at a time. The cap is the request width less the block a covering read rounds out by,
/// so every run that merges is one the ring can still carry whole.
const DIRECT_MERGE_SPAN: u64 = (DIRECT_REQUEST_BYTES - DIRECT_ALIGN) as u64;

/// Bytes a merged read on this backend reaches before the run is broken
pub(super) fn merge_span(serving: ServingBackend) -> u64 {
    match serving.is_direct() {
        true => DIRECT_MERGE_SPAN,
        false => MERGE_SPAN,
    }
}

/// One record's place in a batch, resolved before anything is submitted
pub(super) struct Planned {
    pub(super) at: usize,
    pub(super) segment: SegmentId,
    pub(super) file: FileId,
    pub(super) layout: RecordLayout,
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

/// Group records that sit next to each other into the reads that will serve them
///
/// Only forward, only within one segment, and only across a small gap: a run that
/// went backwards or jumped would read the bytes between for nothing.
pub(super) fn merge_runs_into(plan: &[Planned], span: u64, runs: &mut Vec<Run>) {
    runs.clear();
    let mut start = 0usize;
    while start < plan.len() {
        let mut end = start + 1;
        while end < plan.len() && joins(&plan[end - 1], &plan[end], plan[start].offset, span) {
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
pub(super) fn joins(last: &Planned, next: &Planned, from: u64, span: u64) -> bool {
    last.segment == next.segment
        && next.offset >= last.end()
        && next.offset - last.end() <= MERGE_GAP
        && next.end() - from <= span
}

/// Check one record framed inside a merged read, yielding why it was rejected
///
/// Nothing rather than a verdict means the record is good and its window stands.
#[allow(clippy::too_many_arguments)]
pub(super) fn check_in_block(
    block: &[u8],
    at: usize,
    prefix: usize,
    expected: KeyRef<'_>,
    lsn: Lsn,
    loc: Loc,
    layout: RecordLayout,
    is_verified: bool,
) -> std::result::Result<u8, RecordRead> {
    let Some(body_at) = at.checked_add(prefix) else {
        return Err(RecordRead::Stale);
    };
    let Some(body_end) = body_at.checked_add(loc.len as usize) else {
        return Err(RecordRead::Stale);
    };
    if body_end > block.len() {
        return Err(RecordRead::Stale);
    }
    if let Some(check) = layout.keyless_key(loc.len) {
        return match check_keyless(&block[at..body_at], &block[body_at..body_end], expected, Flags::DATA, &check) {
            KeylessRead::Intact(codec) => Ok(codec),
            KeylessRead::Unwritten => Err(RecordRead::Stale),
            KeylessRead::Corrupt => Err(RecordRead::Corrupt),
        };
    }
    let prefix = &block[at..body_at];
    let Some(codec) = data_codec(prefix, expected, lsn, loc.len) else {
        return Err(RecordRead::Stale);
    };
    if is_verified && !is_intact(prefix, &block[body_at..body_end]) {
        return Err(RecordRead::Corrupt);
    }
    Ok(codec)
}

/// Frame every record each run read, leaving its spot in the run's block
///
/// A run that came back short, or a segment gone under it, leaves its records as
/// misses for the caller to resolve again.
pub(super) fn place_runs(
    scratch: &mut ReadScratch,
    asks: &[Ask],
    keys: &[KeyRef<'_>],
    is_verified: bool,
    blocks: &mut Vec<ReadBlock>,
    spots: &mut [Spot],
) -> Result<()> {
    for (run, filled) in scratch.runs.iter().zip(scratch.filled.drain(..)) {
        let block = match filled {
            Ok((head, body)) => {
                recycle_header(head);
                body
            }
            Err(error) if is_missing(&error) => continue,
            Err(error) => return Err(error),
        };
        let base = scratch.plan[run.start].offset;
        let index = blocks.len() as u32;
        for held in &scratch.plan[run.start..run.end] {
            let at = (held.offset - base) as usize;
            let ask = &asks[held.at];
            let key = keys[ask.at as usize];
            if let Ok(codec) = check_in_block(
                &block,
                at,
                held.prefix,
                key,
                ask.lsn,
                ask.loc,
                held.layout,
                is_verified,
            ) {
                spots[ask.at as usize] = Spot {
                    block: index,
                    at: (at + held.prefix) as u32,
                    len: held.len as u32,
                    codec,
                };
            }
        }
        blocks.push(ReadBlock::new(block, crate::reel::payload::give));
    }
    Ok(())
}

/// Decide what a framed record read means, once the bytes are in hand
///
/// A keyless record is always verified, since its check is the only thing that says
/// it is the record the entry or row points at.
pub(super) fn frame_to_read(
    head: Vec<u8>,
    body: Vec<u8>,
    expected: KeyRef<'_>,
    lsn: Lsn,
    loc: Loc,
    layout: RecordLayout,
    is_verified: bool,
) -> RecordRead {
    // Wrapped before anything can return, so a record the checks reject still
    // hands its buffer back to the pool rather than to the allocator.
    let body = Value::pooled(body, crate::reel::payload::give);
    if let Some(check) = layout.keyless_key(loc.len) {
        let read = check_keyless(&head, &body, expected, Flags::DATA, &check);
        recycle_header(head);
        return match read {
            KeylessRead::Intact(codec) => decoded(codec, body),
            KeylessRead::Unwritten => RecordRead::Stale,
            KeylessRead::Corrupt => RecordRead::Corrupt,
        };
    }
    let codec = data_codec(&head, expected, lsn, loc.len);
    let is_corrupt = codec.is_some() && is_verified && !is_intact(&head, &body);
    recycle_header(head);
    let Some(codec) = codec else {
        return RecordRead::Stale;
    };
    if is_corrupt {
        return RecordRead::Corrupt;
    }
    decoded(codec, body)
}

/// A checked record's payload, decoded where a codec produced it
pub(super) fn decoded(codec: u8, body: Value) -> RecordRead {
    if codec != 0 {
        // A decode that fails is corruption wearing a valid checksum, answered
        // exactly as a failed checksum is.
        return match crate::append::codec::decode(codec, &body) {
            Some(decoded) => RecordRead::Found(Value::pooled(decoded, crate::reel::payload::give)),
            None => RecordRead::Corrupt,
        };
    }
    RecordRead::Found(body)
}

/// A window of a keyless record read whole, since only the whole record checks
///
/// A coded record says so, and the caller reads it again to decode and cut.
pub(super) fn keyless_range(
    head: Vec<u8>,
    body: Vec<u8>,
    expected: KeyRef<'_>,
    check: &CheckKey,
    at: u64,
    len: usize,
) -> RecordRead {
    let read = check_keyless(&head, &body, expected, Flags::DATA, check);
    recycle_header(head);
    if read != KeylessRead::Intact(0) {
        crate::reel::payload::give(body);
        return match read {
            KeylessRead::Intact(_) => RecordRead::Coded,
            KeylessRead::Unwritten => RecordRead::Stale,
            KeylessRead::Corrupt => RecordRead::Corrupt,
        };
    }
    match cut_range(body, at as usize, len) {
        Some(window) => RecordRead::Found(window),
        None => RecordRead::Stale,
    }
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
    let Some(codec) = data_codec(head, expected, lsn, loc.len) else {
        return Ok(RecordRead::Stale);
    };
    // The offsets the caller asked at address the payload this record decodes to,
    // and none of that payload is on the volume, so the window is left to a whole
    // read that decodes and cuts.
    if codec != 0 {
        return Ok(RecordRead::Coded);
    }
    Ok(RecordRead::Found(body))
}
