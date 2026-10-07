//! The volume's directory of append tails
//!
//! A reel owns every segment file on the volume, the append sequence counter that
//! orders its records, and the monotonic segment numbering its tails draw from.
//! Writes route to the least-loaded tail. Every column shares the one log, so a
//! batch spanning columns is one durability point and one recovery domain.

pub mod bias;
pub mod checkpoint;
pub mod cue;
pub mod payload;
mod read;
pub mod segment;
pub mod tail;
pub mod volumes;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::append::admission::InflightBudget;
use crate::append::{Appender, BatchRecord, Commit, Committed};
use crate::config::{PointReads, ReelConfig};
use crate::error::{ReelError, Result};
use std::sync::OnceLock;

use crate::format::block::{lookup_in_span, FooterMap, RowBlock};
use crate::format::column::{ColumnId, ColumnSet, KeyRef, RecordKey};
use crate::format::footer::{FooterFind, FooterPartition, FooterRow, FooterTally, SegmentFooter};
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::{Lsn, LsnCounter};
use crate::format::record::{
    check_keyless, fits_keyless, keyless_len, CheckKey, Flags, KeylessRead, RecordLayout,
    HEADER_LEN, KEYLESS_PREFIX,
};
use crate::index::counters::{FilterProbes, SegmentTable};
use crate::index::paged::{FooterCache, FooterSource};
use crate::index::recovery::{read_footer, ResumableTail};
use crate::index::spot::{Head, HeadRead, RecordSource, SpotRead};
use crate::index::tbtreemap::{TBTreeMap, NODE_WIDTH};
use crate::io::op::{Advice, Completion, Op, WarmFirst};
use crate::reel::segment::{FdCache, IoDriver, SegmentHandle, SplitAnswer, SplitRead};
use crate::sync::{lock, read, write};

use reel_core::{ReadBlock, Value};

use read::{
    check_in_block, cut_range, decoded, deep_range, frame_to_range, frame_to_read,
    framed_or_nothing, keyless_range, merge_runs_into, merge_span, near_range, place_runs,
    window_or_nothing, window_start, Planned, Proof, Run, MERGE_GAP,
};

/// The first segment number a fresh reel numbers from
const FIRST_SEGMENT: u32 = 1;

/// The floor of a volume that has purged nothing, which no key sits below
pub const NOTHING_PURGED: u64 = 0;

/// Width of the zero-padded segment number in a file name
const SEGMENT_DIGITS: usize = 6;

/// Suffix every segment file carries
pub const SEGMENT_SUFFIX: &str = ".reel";

/// The spot index reads its candidates' records through this
impl RecordSource for ReelShared {
    fn head(&self, key: KeyRef<'_>, segment: SegmentId, offset: u32) -> Result<HeadRead> {
        let Some(handle) = self.handle_for(segment)? else {
            return Ok(HeadRead::Missing);
        };
        if handle.layout().is_keyless_layout() {
            return self.keyless_head(key, segment, offset);
        }
        let prefix = (HEADER_LEN + key.bytes.len()) as u64;
        let bytes = self
            .driver
            .pread(handle.file(), u64::from(offset), prefix)?;
        Ok(head_read(&bytes, key))
    }

    fn cached_head(&self, key: KeyRef<'_>, segment: SegmentId, offset: u32) -> Result<HeadRead> {
        let Some(handle) = self.handle_for(segment)? else {
            return Ok(HeadRead::Missing);
        };
        if handle.layout().is_keyless_layout() {
            // only a footer already held counts as memory
            let Some(footer) = self.footers.get(segment) else {
                return Ok(HeadRead::Cold);
            };
            let found = match footer.partition(key.column) {
                Some(partition) => row_at_offset(partition, key.bytes, offset)?,
                None => None,
            };
            return Ok(keyless_head_of(found, offset));
        }
        let prefix = HEADER_LEN + key.bytes.len();
        Ok(
            match self
                .driver
                .warm_only(handle.file(), u64::from(offset), prefix)
            {
                Some(bytes) => head_read(&bytes, key),
                None => HeadRead::Cold,
            },
        )
    }

    fn record(
        &self,
        key: &RecordKey,
        segment: SegmentId,
        offset: u32,
        bound: u32,
        alone: bool,
    ) -> Result<SpotRead> {
        let Some(handle) = self.handle_for(segment)? else {
            return Ok(SpotRead::Gone);
        };
        if let RecordLayout::Keyless(check) = handle.layout() {
            if alone && fits_keyless(bound) {
                let answer = self.driver.pread_split_reusing(
                    handle.file(),
                    u64::from(offset),
                    KEYLESS_PREFIX,
                    bound as usize,
                    take_header(),
                    self.warm_first(),
                );
                if let Some(read) = lone_keyless(answer, key, &check)? {
                    return Ok(read);
                }
            }
            return self.keyless_record(&handle, key, segment, offset);
        }
        let prefix = HEADER_LEN + key.as_slice().len();
        let answer = self.driver.pread_split_reusing(
            handle.file(),
            u64::from(offset),
            prefix,
            bound as usize,
            take_header(),
            self.warm_first(),
        );
        match self.spot_verdict(answer, key, segment, offset)? {
            Verdict::Read(read) => Ok(read),
            Verdict::Whole(head) => {
                self.whole_record(&handle, key, head, Loc::new(segment, offset, head.len))
            }
        }
    }
}

/// Settle a lone candidate's keyless record off its own check, or leave it to the footer row
fn lone_keyless(
    answer: SplitAnswer,
    key: &RecordKey,
    check: &CheckKey,
) -> Result<Option<SpotRead>> {
    let (head, mut body) = match answer {
        Ok(read) => read,
        Err((error, spare)) => {
            recycle_header(spare);
            return match is_missing(&error) {
                true => Ok(Some(SpotRead::Gone)),
                false => Err(error),
            };
        }
    };
    let Some(len) = keyless_len(&head).filter(|len| *len as usize <= body.len()) else {
        recycle_header(head);
        crate::reel::payload::give(body);
        return Ok(None);
    };
    body.truncate(len as usize);
    let read = check_keyless(&head, &body, key.as_ref(), Flags::DATA, check);
    recycle_header(head);
    // Another key's record and rot look alike here, so the row decides
    let KeylessRead::Intact(codec) = read else {
        crate::reel::payload::give(body);
        return Ok(None);
    };
    Ok(Some(
        match decoded(codec, Value::pooled(body, crate::reel::payload::give)) {
            RecordRead::Found(value) => SpotRead::Newest(len, value),
            // checked out whole and still would not decode, which the checked path settles
            RecordRead::Corrupt | RecordRead::Stale | RecordRead::Gone | RecordRead::Coded => {
                SpotRead::Unsure
            }
        },
    ))
}

/// Copy a window of a payload already in hand into a buffer of its own
fn window_of_value(value: &Value, at: u64, len: usize) -> Option<Value> {
    let wanted = (value.len() as u64).saturating_sub(at).min(len as u64) as usize;
    let window = value.get(at as usize..at as usize + wanted)?;
    let mut cut = crate::reel::payload::take(wanted);
    cut.extend_from_slice(window);
    Some(Value::pooled(cut, crate::reel::payload::give))
}

/// A window cut from a whole record read, settled the way the read settled
fn range_of(read: SpotRead, at: u64, len: usize) -> SpotRange {
    match read {
        SpotRead::Found(head, value) => match window_of_value(&value, at, len) {
            Some(window) => SpotRange::Found(head, window),
            None => SpotRange::Unsure,
        },
        SpotRead::Newest(_, value) => match window_of_value(&value, at, len) {
            Some(window) => SpotRange::Newest(window),
            None => SpotRange::Unsure,
        },
        SpotRead::Tombstone(head) => SpotRange::Tombstone(head),
        SpotRead::Other => SpotRange::Other,
        SpotRead::Gone => SpotRange::Gone,
        SpotRead::Unsure => SpotRange::Unsure,
    }
}

/// What a keyless segment's row for a key says about the candidate at an offset
fn keyless_head_of(found: Option<FooterRow>, offset: u32) -> HeadRead {
    // A range delete covers a span, so its row answers for no single key
    match found {
        Some(row) if row.offset == offset && !row.is_range_tombstone() => HeadRead::Same(Head {
            lsn: row.lsn,
            len: row.len,
            is_tombstone: row.is_tombstone(),
        }),
        Some(_) | None => HeadRead::Other,
    }
}

/// The row of a key's run that sits at an offset, nothing where none of them does
fn row_at_offset(
    partition: &FooterPartition,
    key: &[u8],
    offset: u32,
) -> Result<Option<FooterRow>> {
    for at in partition.lower_bound(key)..partition.upper_bound(key) {
        let row = partition.row_at(at)?;
        if row.offset == offset {
            return Ok(Some(row));
        }
    }
    Ok(None)
}

/// The outcome of one spot index range read
pub enum SpotRange {
    /// The key's record at this candidate, and the window of its payload asked for
    Found(Head, Value),

    /// The window of a lone candidate's record, confirmed by its own check with no version read
    Newest(Value),

    /// The key's tombstone at this candidate
    Tombstone(Head),

    /// The candidate is not the key's record
    Other,

    /// The candidate's segment is gone
    Gone,

    /// A coded record, or one the read came up short on, which the checked path reads
    Unsure,
}

/// One spot index candidate in a batch
pub struct SpotAsk<'a> {
    /// The key being looked up
    pub key: &'a RecordKey,

    /// The segment holding the candidate
    pub segment: SegmentId,

    /// The candidate's offset in its segment
    pub offset: u32,

    /// The read takes this many payload bytes, enough for any length in the slot's class
    pub bound: u32,

    /// Whether this is the key's only slot, so a keyless record may confirm itself
    pub alone: bool,
}

/// The outcome of one bounded read of a spot index candidate
enum Verdict {
    /// The bounded read settled the candidate
    Read(SpotRead),

    /// The record runs past the bounded read, so it reads again at its own length
    Whole(Head),
}

/// How one candidate of a batch is read, settled before anything is submitted
enum Asked {
    /// One bounded read of a keyed record's header and payload
    Bounded(SegmentHandle),

    /// One read of a record at the length its keyless segment's row gave
    Exact(SegmentHandle, Head),

    /// One bounded read of a lone keyless record, which its own check confirms
    Lone(SegmentHandle, CheckKey),

    /// Settled with no read: the segment is gone, or its row says another key or a tombstone
    Settled(SpotRead),
}

impl ReelShared {
    /// Confirm a candidate in a keyless segment against the footer row filed under its key at its offset
    fn keyless_head(&self, key: KeyRef<'_>, segment: SegmentId, offset: u32) -> Result<HeadRead> {
        let newest = self.find(segment, key.column, key.bytes)?;
        // The newest row nearly always is the candidate, so only a miss walks the key's run
        if newest.as_ref().is_none_or(|row| row.offset == offset) {
            return Ok(keyless_head_of(newest, offset));
        }
        let Some(footer) = self.footer_of(segment)? else {
            return Ok(HeadRead::Missing);
        };
        let row = match footer.partition(key.column) {
            Some(partition) => row_at_offset(partition, key.bytes, offset)?,
            None => None,
        };
        Ok(keyless_head_of(row, offset))
    }

    /// One candidate in a keyless segment, settled off its row and one read of its record
    fn keyless_record(
        &self,
        handle: &SegmentHandle,
        key: &RecordKey,
        segment: SegmentId,
        offset: u32,
    ) -> Result<SpotRead> {
        let head = match self.keyless_head(key.as_ref(), segment, offset)? {
            HeadRead::Same(head) => head,
            HeadRead::Missing => return Ok(SpotRead::Gone),
            HeadRead::Other | HeadRead::Cold => return Ok(SpotRead::Other),
        };
        if head.is_tombstone {
            return Ok(SpotRead::Tombstone(head));
        }
        self.whole_record(handle, key, head, Loc::new(segment, offset, head.len))
    }

    /// A window of a keyless candidate's record read whole, or nothing for a record past the keyless ceiling
    fn keyless_range(
        &self,
        handle: &SegmentHandle,
        key: &RecordKey,
        segment: SegmentId,
        offset: u32,
        at: u64,
        len: usize,
    ) -> Result<Option<SpotRange>> {
        let head = match self.keyless_head(key.as_ref(), segment, offset)? {
            HeadRead::Same(head) => head,
            HeadRead::Missing => return Ok(Some(SpotRange::Gone)),
            HeadRead::Other | HeadRead::Cold => return Ok(Some(SpotRange::Other)),
        };
        if head.is_tombstone {
            return Ok(Some(SpotRange::Tombstone(head)));
        }
        if !fits_keyless(head.len) {
            return Ok(None);
        }
        let loc = Loc::new(segment, offset, head.len);
        Ok(Some(range_of(
            self.whole_record(handle, key, head, loc)?,
            at,
            len,
        )))
    }

    /// The record at a place, read as a future in one read of its header and up to `bound` payload bytes
    pub async fn spot_record_wait(
        &self,
        key: &RecordKey,
        segment: SegmentId,
        offset: u32,
        bound: u32,
        alone: bool,
    ) -> Result<SpotRead> {
        let Some(handle) = self.handle_for(segment)? else {
            return Ok(SpotRead::Gone);
        };
        if let RecordLayout::Keyless(check) = handle.layout() {
            if alone && fits_keyless(bound) {
                let answer = self
                    .driver
                    .wait_split_reusing(
                        handle.file(),
                        u64::from(offset),
                        KEYLESS_PREFIX,
                        bound as usize,
                        take_header(),
                        self.warm_first(),
                    )
                    .await;
                if let Some(read) = lone_keyless(answer, key, &check)? {
                    return Ok(read);
                }
            }
            let head = match self.keyless_head(key.as_ref(), segment, offset)? {
                HeadRead::Same(head) => head,
                HeadRead::Missing => return Ok(SpotRead::Gone),
                HeadRead::Other | HeadRead::Cold => return Ok(SpotRead::Other),
            };
            if head.is_tombstone {
                return Ok(SpotRead::Tombstone(head));
            }
            let loc = Loc::new(segment, offset, head.len);
            return self.whole_record_wait(&handle, key, head, loc).await;
        }
        let prefix = HEADER_LEN + key.as_slice().len();
        let answer = self
            .driver
            .wait_split_reusing(
                handle.file(),
                u64::from(offset),
                prefix,
                bound as usize,
                take_header(),
                self.warm_first(),
            )
            .await;
        match self.spot_verdict(answer, key, segment, offset)? {
            Verdict::Read(read) => Ok(read),
            Verdict::Whole(head) => {
                let loc = Loc::new(segment, offset, head.len);
                self.whole_record_wait(&handle, key, head, loc).await
            }
        }
    }

    /// One record read as a future at the length its header gave
    async fn whole_record_wait(
        &self,
        handle: &SegmentHandle,
        key: &RecordKey,
        head: Head,
        loc: Loc,
    ) -> Result<SpotRead> {
        let layout = handle.layout();
        let prefix = layout.prefix_len(key.as_slice().len(), loc.len);
        let len = loc.len as usize;
        let read = self
            .driver
            .wait_split_reusing(
                handle.file(),
                u64::from(loc.offset),
                prefix,
                len,
                take_header(),
                self.warm_first(),
            )
            .await;
        Ok(match framed_or_nothing(read, prefix, len)? {
            Some((bytes, body)) => spot_read_of(
                frame_to_read(
                    bytes,
                    body,
                    key.as_ref(),
                    head.lsn,
                    loc,
                    layout,
                    Proof::of(self.config.verify_reads, false),
                ),
                head,
            ),
            None => SpotRead::Other,
        })
    }

    /// A window of the record at a place, its header and the window in one round trip
    #[allow(clippy::too_many_arguments)]
    pub fn spot_range(
        &self,
        key: &RecordKey,
        segment: SegmentId,
        offset: u32,
        bound: u32,
        alone: bool,
        at: u64,
        len: usize,
    ) -> Result<SpotRange> {
        let Some(handle) = self.handle_for(segment)? else {
            return Ok(SpotRange::Gone);
        };
        if let RecordLayout::Keyless(check) = handle.layout() {
            if alone && fits_keyless(bound) {
                let answer = self.driver.pread_split_reusing(
                    handle.file(),
                    u64::from(offset),
                    KEYLESS_PREFIX,
                    bound as usize,
                    take_header(),
                    self.warm_first(),
                );
                if let Some(read) = lone_keyless(answer, key, &check)? {
                    return Ok(range_of(read, at, len));
                }
            }
            if let Some(settled) = self.keyless_range(&handle, key, segment, offset, at, len)? {
                return Ok(settled);
            }
        }
        let prefix = HEADER_LEN + key.as_slice().len();
        let base = u64::from(offset);
        if at <= MERGE_GAP {
            let span = at as usize + len;
            let answer = self.driver.pread_split_reusing(
                handle.file(),
                base,
                prefix,
                span,
                take_header(),
                self.warm_first(),
            );
            return near_spot_range(answer, key, at, len);
        }
        let ops = vec![
            self.driver.split_read(handle.file(), base, 0, prefix),
            self.driver
                .split_read(handle.file(), base + prefix as u64 + at, 0, len),
        ];
        deep_spot_range(self.driver.run_split_reads(ops)?, key, at, len)
    }

    /// The same window as a future, through the driver
    #[allow(clippy::too_many_arguments)]
    pub async fn spot_range_wait(
        &self,
        key: &RecordKey,
        segment: SegmentId,
        offset: u32,
        bound: u32,
        alone: bool,
        at: u64,
        len: usize,
    ) -> Result<SpotRange> {
        let Some(handle) = self.handle_for(segment)? else {
            return Ok(SpotRange::Gone);
        };
        if let RecordLayout::Keyless(check) = handle.layout() {
            if alone && fits_keyless(bound) {
                let answer = self
                    .driver
                    .wait_split_reusing(
                        handle.file(),
                        u64::from(offset),
                        KEYLESS_PREFIX,
                        bound as usize,
                        take_header(),
                        WarmFirst::Skip,
                    )
                    .await;
                if let Some(read) = lone_keyless(answer, key, &check)? {
                    return Ok(range_of(read, at, len));
                }
            }
            if let Some(settled) = self.keyless_range(&handle, key, segment, offset, at, len)? {
                return Ok(settled);
            }
        }
        let prefix = HEADER_LEN + key.as_slice().len();
        let base = u64::from(offset);
        if at <= MERGE_GAP {
            let span = at as usize + len;
            let answer = self
                .driver
                .wait_split_reusing(
                    handle.file(),
                    base,
                    prefix,
                    span,
                    take_header(),
                    WarmFirst::Skip,
                )
                .await;
            return near_spot_range(answer, key, at, len);
        }
        let ops = vec![
            self.driver.split_read(handle.file(), base, 0, prefix),
            self.driver
                .split_read(handle.file(), base + prefix as u64 + at, 0, len),
        ];
        deep_spot_range(self.driver.wait_split_reads(ops).await?, key, at, len)
    }

    /// One bounded read of each spot index candidate, submitted together
    pub fn spot_records(&self, asks: &[SpotAsk<'_>]) -> Result<Vec<SpotRead>> {
        let (ops, asked) = self.spot_ops(asks)?;
        let filled = self.driver.run_split_reads(ops)?;
        let mut filled = filled.into_iter();
        let mut reads = Vec::with_capacity(asks.len());
        for (ask, asked) in asks.iter().zip(asked) {
            reads.push(match asked {
                Asked::Settled(read) => read,
                Asked::Exact(handle, head) => {
                    let loc = Loc::new(ask.segment, ask.offset, head.len);
                    self.exact_read(next_split(&mut filled)?, &handle, ask.key, head, loc)?
                }
                Asked::Lone(handle, check) => {
                    match lone_keyless(next_split(&mut filled)?, ask.key, &check)? {
                        Some(read) => read,
                        None => self.keyless_record(&handle, ask.key, ask.segment, ask.offset)?,
                    }
                }
                Asked::Bounded(handle) => {
                    let answer = next_split(&mut filled)?;
                    match self.spot_verdict(answer, ask.key, ask.segment, ask.offset)? {
                        Verdict::Read(read) => read,
                        Verdict::Whole(head) => {
                            let loc = Loc::new(ask.segment, ask.offset, head.len);
                            self.whole_record(&handle, ask.key, head, loc)?
                        }
                    }
                }
            });
        }
        Ok(reads)
    }

    /// The same batch as a future, one submission and one wait
    pub async fn spot_records_wait(&self, asks: &[SpotAsk<'_>]) -> Result<Vec<SpotRead>> {
        let (ops, asked) = self.spot_ops(asks)?;
        let filled = self.driver.wait_split_reads(ops).await?;
        let mut filled = filled.into_iter();
        let mut reads = Vec::with_capacity(asks.len());
        for (ask, asked) in asks.iter().zip(asked) {
            reads.push(match asked {
                Asked::Settled(read) => read,
                Asked::Exact(handle, head) => {
                    let loc = Loc::new(ask.segment, ask.offset, head.len);
                    self.exact_read(next_split(&mut filled)?, &handle, ask.key, head, loc)?
                }
                Asked::Lone(handle, check) => {
                    match lone_keyless(next_split(&mut filled)?, ask.key, &check)? {
                        Some(read) => read,
                        None => self.keyless_record(&handle, ask.key, ask.segment, ask.offset)?,
                    }
                }
                Asked::Bounded(handle) => {
                    let answer = next_split(&mut filled)?;
                    match self.spot_verdict(answer, ask.key, ask.segment, ask.offset)? {
                        Verdict::Read(read) => read,
                        Verdict::Whole(head) => {
                            let loc = Loc::new(ask.segment, ask.offset, head.len);
                            self.whole_record_wait(&handle, ask.key, head, loc).await?
                        }
                    }
                }
            });
        }
        Ok(reads)
    }

    /// Plan one read per candidate, with the handle holding its segment open
    fn spot_ops(&self, asks: &[SpotAsk<'_>]) -> Result<(Vec<Op>, Vec<Asked>)> {
        let mut ops = Vec::with_capacity(asks.len());
        let mut asked = Vec::with_capacity(asks.len());
        for ask in asks {
            let Some(handle) = self.handle_for(ask.segment)? else {
                asked.push(Asked::Settled(SpotRead::Gone));
                continue;
            };
            let layout = handle.layout();
            if let RecordLayout::Keyless(check) = layout {
                if ask.alone && fits_keyless(ask.bound) {
                    ops.push(self.driver.split_read(
                        handle.file(),
                        u64::from(ask.offset),
                        KEYLESS_PREFIX,
                        ask.bound as usize,
                    ));
                    asked.push(Asked::Lone(handle, check));
                    continue;
                }
                let head = match self.keyless_head(ask.key.as_ref(), ask.segment, ask.offset)? {
                    HeadRead::Same(head) => head,
                    HeadRead::Missing => {
                        asked.push(Asked::Settled(SpotRead::Gone));
                        continue;
                    }
                    HeadRead::Other | HeadRead::Cold => {
                        asked.push(Asked::Settled(SpotRead::Other));
                        continue;
                    }
                };
                if head.is_tombstone {
                    asked.push(Asked::Settled(SpotRead::Tombstone(head)));
                    continue;
                }
                let prefix = layout.prefix_len(ask.key.as_slice().len(), head.len);
                ops.push(self.driver.split_read(
                    handle.file(),
                    u64::from(ask.offset),
                    prefix,
                    head.len as usize,
                ));
                asked.push(Asked::Exact(handle, head));
                continue;
            }
            let prefix = HEADER_LEN + ask.key.as_slice().len();
            ops.push(self.driver.split_read(
                handle.file(),
                u64::from(ask.offset),
                prefix,
                ask.bound as usize,
            ));
            asked.push(Asked::Bounded(handle));
        }
        Ok((ops, asked))
    }

    /// Settle a record read at the exact length its row gave
    fn exact_read(
        &self,
        answer: SplitAnswer,
        handle: &SegmentHandle,
        key: &RecordKey,
        head: Head,
        loc: Loc,
    ) -> Result<SpotRead> {
        let layout = handle.layout();
        let prefix = layout.prefix_len(key.as_slice().len(), loc.len);
        Ok(match framed_or_nothing(answer, prefix, loc.len as usize)? {
            Some((bytes, body)) => spot_read_of(
                frame_to_read(
                    bytes,
                    body,
                    key.as_ref(),
                    head.lsn,
                    loc,
                    layout,
                    Proof::of(self.config.verify_reads, false),
                ),
                head,
            ),
            None => SpotRead::Other,
        })
    }

    /// Settle one bounded read of a candidate, or say it needs a read at the record's own length
    fn spot_verdict(
        &self,
        answer: SplitAnswer,
        key: &RecordKey,
        segment: SegmentId,
        offset: u32,
    ) -> Result<Verdict> {
        let (bytes, mut body) = match answer {
            Ok(read) => read,
            Err((error, spare)) => {
                recycle_header(spare);
                return match is_missing(&error) {
                    true => Ok(Verdict::Read(SpotRead::Gone)),
                    false => Err(error),
                };
            }
        };
        let verdict = match head_read(&bytes, key.as_ref()) {
            HeadRead::Same(head) if head.is_tombstone => Verdict::Read(SpotRead::Tombstone(head)),
            HeadRead::Same(head) if body.len() >= head.len as usize => {
                body.truncate(head.len as usize);
                let loc = Loc::new(segment, offset, head.len);
                let read = frame_to_read(
                    bytes,
                    body,
                    key.as_ref(),
                    head.lsn,
                    loc,
                    RecordLayout::Keyed,
                    Proof::of(self.config.verify_reads, false),
                );
                return Ok(Verdict::Read(spot_read_of(read, head)));
            }
            HeadRead::Same(head) => Verdict::Whole(head),
            HeadRead::Other | HeadRead::Missing | HeadRead::Cold => Verdict::Read(SpotRead::Other),
        };
        recycle_header(bytes);
        crate::reel::payload::give(body);
        Ok(verdict)
    }

    /// One record read at the length its header or its row gave
    fn whole_record(
        &self,
        handle: &SegmentHandle,
        key: &RecordKey,
        head: Head,
        loc: Loc,
    ) -> Result<SpotRead> {
        let layout = handle.layout();
        let prefix = layout.prefix_len(key.as_slice().len(), loc.len);
        let len = loc.len as usize;
        let read = self.driver.pread_split_reusing(
            handle.file(),
            u64::from(loc.offset),
            prefix,
            len,
            take_header(),
            self.warm_first(),
        );
        Ok(match framed_or_nothing(read, prefix, len)? {
            Some((bytes, body)) => spot_read_of(
                frame_to_read(
                    bytes,
                    body,
                    key.as_ref(),
                    head.lsn,
                    loc,
                    layout,
                    Proof::of(self.config.verify_reads, false),
                ),
                head,
            ),
            None => SpotRead::Other,
        })
    }
}

/// The payload bytes of a window that a record of this length holds
fn window_of(head: &Head, at: u64, len: usize) -> usize {
    (u64::from(head.len).saturating_sub(at)).min(len as u64) as usize
}

/// What a header says about a ranged read: the record, or why the window cannot come from it
fn range_head(prefix: &[u8], key: &RecordKey) -> std::result::Result<Head, SpotRange> {
    match head_read(prefix, key.as_ref()) {
        HeadRead::Same(head) if head.is_tombstone => Err(SpotRange::Tombstone(head)),
        // A coded record's window is of the payload it decodes to, so only a whole read cuts it.
        HeadRead::Same(head) => {
            match crate::format::record::data_codec(prefix, key.as_ref(), head.lsn, head.len) {
                Some(0) => Ok(head),
                Some(_) | None => Err(SpotRange::Unsure),
            }
        }
        HeadRead::Other | HeadRead::Missing | HeadRead::Cold => Err(SpotRange::Other),
    }
}

/// A window that came in one span with its record's header
fn near_spot_range(answer: SplitAnswer, key: &RecordKey, at: u64, len: usize) -> Result<SpotRange> {
    let (bytes, body) = match answer {
        Ok(read) => read,
        Err((error, spare)) => {
            recycle_header(spare);
            return match is_missing(&error) {
                true => Ok(SpotRange::Gone),
                false => Err(error),
            };
        }
    };
    let verdict = match range_head(&bytes, key) {
        Ok(head) => {
            let wanted = window_of(&head, at, len);
            match body.len() >= at as usize + wanted {
                true => match cut_range(body, at as usize, wanted) {
                    Some(window) => SpotRange::Found(head, window),
                    None => SpotRange::Unsure,
                },
                false => {
                    crate::reel::payload::give(body);
                    SpotRange::Unsure
                }
            }
        }
        Err(verdict) => {
            crate::reel::payload::give(body);
            verdict
        }
    };
    recycle_header(bytes);
    Ok(verdict)
}

/// A window that came as its own read beside its record's header
fn deep_spot_range(
    filled: Vec<SplitRead>,
    key: &RecordKey,
    at: u64,
    len: usize,
) -> Result<SpotRange> {
    let mut filled = filled.into_iter();
    let header = next_split(&mut filled)?;
    let window = next_split(&mut filled)?;
    let (head_bytes, window_bytes) = match (header, window) {
        (Ok((empty, head)), Ok((spare, window))) => {
            crate::reel::payload::give(empty);
            crate::reel::payload::give(spare);
            (head, window)
        }
        (Err((error, _)), _) | (_, Err((error, _))) => {
            return match is_missing(&error) {
                true => Ok(SpotRange::Gone),
                false => Err(error),
            };
        }
    };
    let verdict = match range_head(&head_bytes, key) {
        Ok(head) => {
            let wanted = window_of(&head, at, len);
            match window_bytes.len() >= wanted {
                true => match cut_range(window_bytes, 0, wanted) {
                    Some(window) => SpotRange::Found(head, window),
                    None => SpotRange::Unsure,
                },
                false => {
                    crate::reel::payload::give(window_bytes);
                    SpotRange::Unsure
                }
            }
        }
        Err(verdict) => {
            crate::reel::payload::give(window_bytes);
            verdict
        }
    };
    crate::reel::payload::give(head_bytes);
    Ok(verdict)
}

/// The next answer of a batch, as a single read would have it
fn next_split(filled: &mut impl Iterator<Item = SplitRead>) -> Result<SplitAnswer> {
    match filled.next() {
        Some(read) => Ok(read.map_err(|error| (error, Vec::new()))),
        None => Err(ReelError::Backend(
            "a spot index batch came back with fewer answers than it asked".to_string(),
        )),
    }
}

/// What a record's header says about one key, any other record reading as other
fn head_read(prefix: &[u8], key: KeyRef<'_>) -> HeadRead {
    match crate::format::record::head_for(prefix, key) {
        Some((lsn, len, flags)) if flags.is_data() || flags.is_tombstone() => {
            HeadRead::Same(Head {
                lsn,
                len,
                is_tombstone: flags.is_tombstone(),
            })
        }
        Some(_) | None => HeadRead::Other,
    }
}

/// What a checked read of one spot index candidate settles
fn spot_read_of(read: RecordRead, head: Head) -> SpotRead {
    match read {
        RecordRead::Found(value) => SpotRead::Found(head, value),
        RecordRead::Stale => SpotRead::Other,
        RecordRead::Gone | RecordRead::Corrupt | RecordRead::Coded => SpotRead::Unsure,
    }
}

/// The footers a paged index resolves its sealed keys through
impl FooterSource for ReelShared {
    fn footer(&self, segment: SegmentId) -> Result<Option<Arc<SegmentFooter>>> {
        self.footer_of(segment)
    }

    fn footer_once(&self, segment: SegmentId) -> Result<Option<Arc<SegmentFooter>>> {
        if let Some(footer) = self.footers.get(segment) {
            return Ok(Some(footer));
        }
        Ok(self.footer_from_disk(segment)?.map(Arc::new))
    }

    /// One key, answered by reading the blocks the search touches and no more
    fn find(&self, segment: SegmentId, column: ColumnId, key: &[u8]) -> Result<Option<FooterRow>> {
        self.probes.note_probe();
        if let Some(footer) = self.footers.get(segment) {
            let Some(partition) = footer.partition(column) else {
                return Ok(None);
            };
            return Ok(self.answer_of(partition.lookup(key)?));
        }

        let Some(map) = self.footer_map_of(segment)? else {
            return Ok(None);
        };
        let Some((span, filter)) = map.locate(column) else {
            return Ok(None);
        };
        // The descriptor is resolved inside the loader, so a segment the filter
        // rules out costs no io. A segment gone by then ends the search as missing.
        let mut handle = None;
        let outcome = lookup_in_span(&span, filter, key, |at| {
            self.probes.note_block();
            if let Some(block) = self.footers.block_of(segment, column, at) {
                return Ok(Some(block));
            }
            let opened = match handle.as_ref() {
                Some(opened) => opened,
                None => match self.handle_for(segment)? {
                    Some(opened) => handle.insert(opened),
                    None => return Ok(None),
                },
            };
            self.probes.note_block_read();
            let block = Arc::new(RowBlock::read(
                &self.driver,
                opened.file(),
                &span,
                at,
                map.restarts_of(span.column),
            )?);
            self.footers
                .insert_block(segment, column, at, Arc::clone(&block));
            Ok(Some(block))
        })?;
        Ok(self.answer_of(outcome))
    }
}

/// State every tail of the reel shares
pub struct ReelShared {
    /// The volume roots the reel's segment files live across
    pub volumes: crate::reel::volumes::Volumes,

    /// Append sequence counter that orders the reel's records
    pub lsn: LsnCounter,

    /// Ring-shaped backend every tail submits through
    pub driver: Arc<IoDriver>,

    /// Per-volume admission budget shared across every tail
    pub budget: Arc<InflightBudget>,

    /// Descriptor cache of sealed segments shared across the volume
    pub fd_cache: Arc<FdCache>,

    /// What the footer filters were asked and how often they answered
    pub probes: FilterProbes,

    /// The per-segment counters, so a seal can write down what its segment weighs
    pub segments: OnceLock<Arc<SegmentTable>>,

    /// Footers of sealed segments, which a paged column resolves its keys through
    pub footers: FooterCache,

    /// Load-time settings for the volume
    pub config: ReelConfig,

    /// The columns this reel serves, for the widths a record's column declares
    pub columns: ColumnSet,

    /// Timeline position everything below which the volume is finished with
    purge_floor: AtomicU64,

    /// Monotonic segment number every tail draws from
    next_segment: AtomicU32,

    /// Segments sealed since the last pass, waiting to be given to their footers
    sealed_pending: Mutex<Vec<Pending>>,

    /// Whether anything is waiting above, read before the lock rather than under it
    sealed_waiting: AtomicBool,

    /// Segments held from the draw of their number until every record is published
    holds: RwLock<TBTreeMap<SegmentId, NODE_WIDTH, Option<Arc<SegmentHolds>>>>,

    /// Lowest segment number anything still holds, or all ones when nothing does
    held_floor: AtomicU32,

    /// Segments released without a footer, until one lands or their file goes
    unsealed: Mutex<std::collections::HashSet<SegmentId>>,

    /// Segments retired holding acknowledged bytes no sync ever covered
    past_saving: AtomicU64,

    /// Rolled segments whose seals failed, parked for the maintenance tick
    pub(crate) broken_seals: Mutex<Vec<crate::append::BrokenSeal>>,

    /// Sequence numbers drawn for records that have not yet taken a segment hold
    drawn: AtomicU64,
}

/// Drawn sequence numbers' place in the gauge, given back when their records land
///
/// A guard rather than a pair of calls, so a placement that fails between the draw
/// and the claim cannot wedge the prune floor closed for the life of the volume.
pub struct DrawnRecords<'a> {
    shared: &'a ReelShared,
    count: u64,
}

impl Drop for DrawnRecords<'_> {
    fn drop(&mut self) {
        self.shared.drawn.fetch_sub(self.count, Ordering::AcqRel);
    }
}

/// What is keeping one segment from being retired
#[derive(Default)]
pub struct SegmentHolds {
    /// Whether a tail can still append to it
    is_tail: AtomicBool,

    /// Records on the device that nobody has published to the index yet
    unpublished: AtomicU64,
}

impl SegmentHolds {
    fn is_free(&self) -> bool {
        !self.is_tail.load(Ordering::Acquire) && self.unpublished.load(Ordering::Acquire) == 0
    }

    /// Take a hold for a record that has landed but has not been published
    pub fn hold_record(&self) {
        self.unpublished.fetch_add(1, Ordering::AcqRel);
    }

    /// Whether giving up a record's hold leaves the segment ready to be forgotten
    pub fn release_record(&self) -> bool {
        self.unpublished.fetch_sub(1, Ordering::AcqRel) == 1
            && !self.is_tail.load(Ordering::Acquire)
    }
}

/// A sealed segment the index has not been told about yet
///
/// The entry stays on the queue while anybody holds its footer, since the queue is
/// what holds compaction off the segment, and is_taken keeps two callers from each
/// settling it off while the other still owes its spans.
struct Pending {
    /// The segment sealed
    segment: SegmentId,

    /// Whether a caller is already noting this one's spans
    is_taken: bool,

    /// The seal's footer, kept so noting and handing over never read it back
    footer: Arc<SegmentFooter>,
}

impl ReelShared {
    /// Turn a footer lookup into the trait's answer, counting a ruled-out skip
    fn answer_of(&self, outcome: FooterFind) -> Option<FooterRow> {
        match outcome {
            FooterFind::RuledOut => {
                self.probes.note_skip();
                None
            }
            FooterFind::Missing => None,
            FooterFind::Found(row) => Some(row),
        }
    }

    /// Shared reel state numbering segments from a starting point
    pub fn new(
        dir: PathBuf,
        driver: Arc<IoDriver>,
        budget: Arc<InflightBudget>,
        fd_cache: Arc<FdCache>,
        config: ReelConfig,
        columns: ColumnSet,
        next_segment: u32,
    ) -> ReelShared {
        // The primary root stays first: the lock and the manifest live on it, and
        // it is always the fast tier.
        let mut roots = vec![dir];
        let mut classes = vec![crate::config::VolumeClass::Fast];
        let mut dead = vec![false];
        for volume in &config.volumes {
            roots.push(volume.path.clone());
            classes.push(volume.class);
            dead.push(volume.dead);
        }
        let watermark = config.segment_bytes.to_bytes() * crate::reel::volumes::WATERMARK_SEGMENTS;
        ReelShared {
            volumes: crate::reel::volumes::Volumes::new(roots, classes, dead, watermark),
            lsn: LsnCounter::new(),
            driver,
            budget,
            fd_cache,
            probes: FilterProbes::default(),
            segments: OnceLock::new(),
            footers: FooterCache::new(config.footer_cache.to_bytes() as usize),
            config,
            columns,
            purge_floor: AtomicU64::new(NOTHING_PURGED),
            next_segment: AtomicU32::new(next_segment.max(FIRST_SEGMENT)),
            sealed_pending: Mutex::new(Vec::new()),
            sealed_waiting: AtomicBool::new(false),
            holds: RwLock::new(TBTreeMap::new()),
            held_floor: AtomicU32::new(u32::MAX),
            unsealed: Mutex::new(std::collections::HashSet::new()),
            past_saving: AtomicU64::new(0),
            broken_seals: Mutex::new(Vec::new()),
            drawn: AtomicU64::new(0),
        }
    }

    /// Move the floor everything below which the volume is finished with, upwards only
    pub fn purge_below(&self, floor: u64) {
        self.purge_floor.fetch_max(floor, Ordering::AcqRel);
    }

    /// The floor compaction drops records below
    pub fn purge_floor(&self) -> u64 {
        self.purge_floor.load(Ordering::Acquire)
    }

    /// A handle on a segment file, from the descriptor cache or from a fresh open
    ///
    /// Every reader here asks for a range it already knows, so readahead serves
    /// nothing and costs the pages it faulted.
    pub fn handle_for(&self, segment: SegmentId) -> Result<Option<SegmentHandle>> {
        if let Some(handle) = self.fd_cache.get(segment) {
            return Ok(Some(handle));
        }
        let path = self.segment_path(segment);
        let file = match self.driver.open(&path, false) {
            Ok(file) => file,
            Err(error) if error.is_missing() => return Ok(None),
            Err(error) => return Err(error),
        };
        // A refused hint leaves the reads correct and only the readahead wrong,
        // which is not worth failing an open over.
        let _ = self.driver.advise(file, 0, 0, Advice::Random);
        let handle = SegmentHandle::opened(segment, path, file, Arc::clone(&self.driver))?;
        self.fd_cache.insert(handle.clone());
        Ok(Some(handle))
    }

    /// The parsed footer of a sealed segment, from the cache or from the file
    pub fn footer_of(&self, segment: SegmentId) -> Result<Option<Arc<SegmentFooter>>> {
        if let Some(footer) = self.footers.get(segment) {
            return Ok(Some(footer));
        }
        let Some(footer) = self.footer_from_disk(segment)? else {
            return Ok(None);
        };
        let footer = Arc::new(footer);
        self.footers.insert(segment, Arc::clone(&footer));
        Ok(Some(footer))
    }

    /// One sealed segment's footer read from its file, with no cache asked or filled
    fn footer_from_disk(&self, segment: SegmentId) -> Result<Option<SegmentFooter>> {
        let Some(handle) = self.handle_for(segment)? else {
            return Ok(None);
        };
        let file_len = match self.driver.length(handle.file()) {
            Ok(len) => len,
            Err(error) if error.is_missing() => return Ok(None),
            Err(error) => return Err(error),
        };
        read_footer(&self.driver, handle.file(), file_len)
    }

    /// What a segment weighs, live against dead, for the seal that writes it down
    ///
    /// Zero before the counters are wired, which is a reader that never seals.
    pub fn tally_of(&self, segment: SegmentId) -> FooterTally {
        let Some(segments) = self.segments.get() else {
            return FooterTally::default();
        };
        let bytes = segments.bytes_of(segment);
        FooterTally {
            live: bytes.live,
            dead: bytes.dead,
        }
    }

    /// Hand the seal the counters it writes a segment's tally from
    pub fn set_segments(&self, segments: Arc<SegmentTable>) {
        let _ = self.segments.set(segments);
    }

    /// Note the oldest number a segment can surface, as soon as its bytes are down
    ///
    /// Booked at landing rather than at the publish, since a caller that goes away
    /// between the two leaves a record a rebuild still finds and no entry names. The
    /// publish books the same number again, which a minimum takes twice for free.
    pub fn note_landed(&self, segment: SegmentId, lsn: Lsn) {
        if let Some(segments) = self.segments.get() {
            segments.note_min(segment, lsn);
        }
    }

    /// A sealed segment's directory, read once and held for the life of the segment
    pub fn footer_map_of(&self, segment: SegmentId) -> Result<Option<Arc<FooterMap>>> {
        if let Some(map) = self.footers.map_of(segment) {
            return Ok(Some(map));
        }
        let Some(handle) = self.handle_for(segment)? else {
            return Ok(None);
        };
        let file_len = match self.driver.length(handle.file()) {
            Ok(len) => len,
            Err(error) if error.is_missing() => return Ok(None),
            Err(error) => return Err(error),
        };
        let read = FooterMap::read(&self.driver, handle.file(), file_len, &self.probes)?;
        let Some(map) = read else {
            return Ok(None);
        };
        let map = Arc::new(map);
        self.footers.insert_map(segment, Arc::clone(&map));
        Ok(Some(map))
    }

    /// Note a segment whose footer is now on disk, for the index to page out
    pub fn note_sealed(&self, segment: SegmentId, footer: Arc<SegmentFooter>) {
        // The window where a seal is durable but the index has not been told.
        crate::sync::rendezvous::at("seal/queued");
        lock(&self.sealed_pending).push(Pending {
            segment,
            is_taken: false,
            footer,
        });
        self.sealed_waiting.store(true, Ordering::Release);
    }

    /// Whether a segment has sealed that the index has not been told about
    pub fn has_sealed_waiting(&self) -> bool {
        self.sealed_waiting.load(Ordering::Acquire)
    }

    /// The segments sealed since the last call, each with its seal's footer
    pub fn peek_sealed(&self) -> Vec<(SegmentId, Arc<SegmentFooter>)> {
        let mut pending = lock(&self.sealed_pending);
        // Entries stay queued for compaction to see, and the cleared flag only sends other callers past them
        self.sealed_waiting.store(false, Ordering::Release);
        let mut taken = Vec::with_capacity(pending.len());
        for entry in pending.iter_mut().filter(|entry| !entry.is_taken) {
            entry.is_taken = true;
            taken.push((entry.segment, Arc::clone(&entry.footer)));
        }
        taken
    }

    /// Take off the queue the segments a pass has told the index about
    ///
    /// What is left stays owed, but the flag is not raised again for it: that would
    /// put every following read into the same failing footer read. The retry rides
    /// the maintenance tick, which asks whether or not the flag is up.
    pub fn settle_sealed(&self, named: &[SegmentId]) {
        lock(&self.sealed_pending).retain(|entry| !named.contains(&entry.segment));
    }

    /// Segments whose spans the index has not been told about yet
    pub fn pending_seals(&self) -> Vec<SegmentId> {
        lock(&self.sealed_pending)
            .iter()
            .map(|entry| entry.segment)
            .collect()
    }

    /// Register the hold for a segment an appender resumes, marking it a tail
    ///
    /// The number was drawn by a previous process, so nothing advances here;
    /// the segment only comes back under a tail's hold.
    pub fn adopt_segment(&self, id: SegmentId) -> Arc<SegmentHolds> {
        let mut map = write(&self.holds);
        let held = match map.get(&id) {
            Some(Some(held)) => Arc::clone(held),
            _ => {
                let held: Arc<SegmentHolds> = Arc::default();
                map.insert(id, Some(Arc::clone(&held)));
                held
            }
        };
        self.refresh_held_floor(&map);
        held.is_tail.store(true, Ordering::Release);
        held
    }

    /// Draw the next monotonic segment number, holding it for the tail that drew it
    ///
    /// Numbers are never given back, and a wrap would name a fresh file after a
    /// segment the index still points at, so the draw refuses at the end of the
    /// range instead of rolling over.
    pub fn next_segment(&self) -> Result<(SegmentId, Arc<SegmentHolds>)> {
        let drawn = self
            .next_segment
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|current| {
                ReelError::Rejected(format!("reel segment numbers exhausted at {current}"))
            })?;
        let id = SegmentId(drawn);
        let holds = {
            let mut map = write(&self.holds);
            let held = match map.get(&id) {
                Some(Some(held)) => Arc::clone(held),
                // A node's unfilled slots are None, so the hold is made here
                // rather than defaulted into every one of them.
                _ => {
                    let held: Arc<SegmentHolds> = Arc::default();
                    map.insert(id, Some(Arc::clone(&held)));
                    held
                }
            };
            self.refresh_held_floor(&map);
            held
        };
        holds.is_tail.store(true, Ordering::Release);
        Ok((id, holds))
    }

    /// Give up a segment no tail will append to again, sealed or given up on
    pub fn release_segment(&self, id: SegmentId) {
        {
            let holds = read(&self.holds);
            match holds.get(&id).and_then(|held| held.as_ref()) {
                Some(held) => held.is_tail.store(false, Ordering::Release),
                None => return,
            }
        }
        self.forget_if_free(id);
    }

    /// Drop a segment's entry once nothing stands between it and retirement
    ///
    /// The check is made again under the exclusive lock, since a record can take a
    /// fresh hold between the release that emptied the count and this.
    pub fn forget_if_free(&self, id: SegmentId) {
        let mut holds = write(&self.holds);
        if holds
            .get(&id)
            .and_then(|held| held.as_ref())
            .map(|held| held.is_free())
            .unwrap_or(false)
        {
            holds.remove(&id);
            self.refresh_held_floor(&holds);
        }
    }

    /// Recompute the lowest held number, under the write lock the caller holds
    fn refresh_held_floor(
        &self,
        holds: &TBTreeMap<SegmentId, NODE_WIDTH, Option<Arc<SegmentHolds>>>,
    ) {
        let floor = holds
            .first_key_value()
            .map(|(id, _)| id.as_u32())
            .unwrap_or(u32::MAX);
        self.held_floor.store(floor, Ordering::Relaxed);
    }

    /// Whether anything still stands between this segment and its retirement
    pub fn is_held(&self, id: SegmentId) -> bool {
        read(&self.holds).contains_key(&id)
    }

    /// Whether this segment is sealed, synced, and past every hold
    ///
    /// The window that matters is between a tail rolling off a segment and the
    /// sealer's fsync returning: no tail owns it and its pages are still dirty. A
    /// segment given up on without a footer never gets that far, so the unsealed
    /// mark answers before the holds do. The mark names that segment and no other:
    /// one dooming says nothing about the segments already sealed under it.
    pub fn is_settled(&self, id: SegmentId) -> bool {
        if lock(&self.unsealed).contains(&id) {
            return false;
        }
        id.as_u32() < self.held_floor.load(Ordering::Relaxed) || !self.is_held(id)
    }

    /// Note a segment released without its footer, which never reads as settled
    pub fn note_unsealed(&self, id: SegmentId) {
        lock(&self.unsealed).insert(id);
    }

    /// Take the mark off a segment that has landed a footer, or whose file has gone
    pub fn forget_unsealed(&self, id: SegmentId) {
        lock(&self.unsealed).remove(&id);
    }

    /// Count a segment retired holding acknowledged bytes no sync covered
    pub fn note_past_saving(&self) {
        self.past_saving.fetch_add(1, Ordering::Relaxed);
    }

    /// Give one past-saving count back, for a parked seal that landed after all
    pub fn un_note_past_saving(&self) {
        let _ = self
            .past_saving
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |held| {
                held.checked_sub(1)
            });
    }

    /// Segments holding acknowledged records a failed sync left past saving
    pub fn past_saving_count(&self) -> u64 {
        self.past_saving.load(Ordering::Relaxed)
    }

    /// Count drawn sequence numbers in until their records take segment holds
    ///
    /// Taken before the numbers are drawn, so there is no instant where a drawn
    /// number is in flight and neither counter says so.
    pub fn draw_gauge(&self, count: u64) -> DrawnRecords<'_> {
        self.drawn.fetch_add(count, Ordering::AcqRel);
        DrawnRecords {
            shared: self,
            count,
        }
    }

    /// Whether every number drawn so far has been published to the index
    ///
    /// A record leaves the drawn gauge only after its segment hold is taken, so the
    /// gauge has to be read first or both reads could miss it.
    pub fn nothing_unpublished(&self) -> bool {
        if self.drawn.load(Ordering::Acquire) > 0 {
            return false;
        }
        let holds = read(&self.holds);
        // Bound rather than left as the tail expression: the walk borrows the guard.
        let quiet = holds
            .iter()
            .filter_map(|(_, held)| held.as_ref())
            .all(|held| held.unpublished.load(Ordering::Acquire) == 0);
        quiet
    }

    /// The number below which no record can still land, the floor a delete is done at
    ///
    /// The frontier is read first, so a draw between the two reads is one the check
    /// sees rather than one the floor lets past. A volume with something in flight
    /// falls back to the same window a grave holds a key against.
    pub fn settled_below(&self) -> Lsn {
        let peek = self.lsn.peek().as_u64();
        match self.nothing_unpublished() {
            true => Lsn(peek),
            false => Lsn(peek.saturating_sub(crate::engine::GRAVE_WINDOW)),
        }
    }

    /// Whether an awaited whole-record read asks the page cache before it queues
    pub fn warm_first(&self) -> WarmFirst {
        match self.config.point_reads {
            PointReads::Probed => WarmFirst::Ask,
            PointReads::Queued => WarmFirst::Skip,
        }
    }

    /// The number the next segment will take, without taking it
    ///
    /// Numbers climb, so once a cue has sealed every tail, every segment below this
    /// is sealed and immutable and every later roll lands at or above it.
    pub fn peek_segment(&self) -> SegmentId {
        SegmentId(self.next_segment.load(Ordering::Relaxed))
    }

    /// Raise the segment counter above a number found on disk during rebuild
    pub fn recover_next_segment(&self, highest_seen: SegmentId) {
        let floor = highest_seen.as_u32().saturating_add(1).max(FIRST_SEGMENT);
        self.next_segment.fetch_max(floor, Ordering::Relaxed);
    }

    /// The next segment number to be drawn, which is the write head's age zero
    pub fn segment_head(&self) -> u32 {
        self.next_segment.load(Ordering::Relaxed)
    }

    /// Path of one segment file, on whichever volume holds it
    pub fn segment_path(&self, id: SegmentId) -> PathBuf {
        self.volumes.path_of(id)
    }

    /// The directory a segment's file sits in, for the sync its create owes
    pub fn segment_dir(&self, id: SegmentId) -> &Path {
        self.volumes.root_dir_of(id)
    }

    /// Whether every write this volume issues has to cover whole blocks
    ///
    /// A direct backend hands its writes straight to the device, which takes whole
    /// blocks or nothing, so a record closes by writing its alignment fill.
    pub fn writes_whole_blocks(&self) -> bool {
        self.config.io_backend.is_direct()
    }
}

/// What resolving one index pointer against the files produced
#[derive(Debug, Eq, PartialEq)]
pub enum RecordRead {
    /// The payload, checked against its checksum when the volume asked
    Found(Value),

    /// The pointer no longer names this key, so resolving it again may find it
    Stale,

    /// The segment the pointer names is not on disk any more
    Gone,

    /// The record is where the pointer said, but its bytes failed the checksum
    Corrupt,

    /// A codec produced the record's stored bytes, so no window of it is on disk
    Coded,
}

/// One record a resolved batch asks the device for
///
/// The key is named by position rather than carried, so the list of asks holds no
/// borrow and a thread can keep it between submissions.
/// Where a placed read left one record: a window of one of its blocks
///
/// A codec other than zero says the window holds the record's stored bytes, which
/// decode to the payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Spot {
    pub block: u32,
    pub at: u32,
    pub len: u32,
    pub codec: u8,
}

impl Spot {
    /// A record the read did not find where its ask said
    pub const MISS: Spot = Spot {
        block: u32::MAX,
        at: 0,
        len: 0,
        codec: 0,
    };
}

#[derive(Clone, Copy)]
pub struct Ask {
    /// Where the index says the record sits
    pub loc: Loc,

    /// The version the index resolved, which the record's header has to match
    pub lsn: Lsn,

    /// Which of the caller's keys this ask is for
    pub at: u32,

    /// Whether the index's stamp still vouches for the place, so a keyless record answers to its shape
    pub certain: bool,
}

/// The lists one thread's batched reads work through, kept between submissions
///
/// A batch resolves, plans, merges, submits and cuts through a stack of vectors as
/// wide as the batch and nothing wider, so they stay with the thread and only the
/// answers leave. Every one is cleared on the way back, since a plan holds segment
/// handles open and a filled read holds pooled payload buffers.
#[derive(Default)]
struct ReadScratch {
    /// Each ask's place on the volume beside its position, sorted into volume order
    order: Vec<(u64, u32)>,

    /// Every ask resolved to a place on the volume
    plan: Vec<Planned>,

    /// One handle per segment the plan reads, so none is unlinked under a read in flight
    handles: Vec<SegmentHandle>,

    /// The reads the plan was grouped into
    runs: Vec<Run>,

    /// One op per run, built before anything is submitted
    ops: Vec<Op>,

    /// Completions the backend answered a batch with, in submit order
    completions: Vec<Completion>,

    /// What each run's read filled
    filled: Vec<SplitRead>,
}

impl ReadScratch {
    const fn empty() -> ReadScratch {
        ReadScratch {
            order: Vec::new(),
            plan: Vec::new(),
            handles: Vec::new(),
            runs: Vec::new(),
            ops: Vec::new(),
            completions: Vec::new(),
            filled: Vec::new(),
        }
    }

    /// Drop everything the last batch left, keeping the room it grew into
    fn release(&mut self) {
        self.order.clear();
        self.plan.clear();
        self.handles.clear();
        self.runs.clear();
        self.ops.clear();
        self.completions.clear();
        self.filled.clear();
    }
}

thread_local! {
    /// One set of read lists per reading thread, handed back after every batch
    static READ_SCRATCH: std::cell::Cell<ReadScratch> =
        const { std::cell::Cell::new(ReadScratch::empty()) };
}

/// This thread's read lists, given back however the batch that borrowed them ends
struct HeldScratch(ReadScratch);

impl HeldScratch {
    /// Borrow this thread's lists, leaving it empty ones until they come back
    fn take() -> HeldScratch {
        HeldScratch(READ_SCRATCH.with(std::cell::Cell::take))
    }
}

impl Drop for HeldScratch {
    fn drop(&mut self) {
        let mut held = std::mem::take(&mut self.0);
        held.release();
        // A read that nested inside another gives its own back first and this
        // overwrites it, which keeps the lists the outer batch grew.
        READ_SCRATCH.with(|spare| spare.set(held));
    }
}

/// The volume's reel: shared state plus its active append tails
pub struct Reel {
    shared: Arc<ReelShared>,
    tails: Vec<Appender>,
    reserved: usize,
    leased: AtomicU64,
}

/// One reserved tail, held by one compaction pass and given back however it leaves
pub struct ReservedLease<'reel> {
    reel: &'reel Reel,
    index: usize,
}

impl ReservedLease<'_> {
    /// Position of the leased tail among the reel's tails
    pub fn index(&self) -> usize {
        self.index
    }
}

impl Drop for ReservedLease<'_> {
    fn drop(&mut self) {
        let bit = 1u64 << (self.index - self.reel.foreground().len());
        self.reel.leased.fetch_and(!bit, Ordering::AcqRel);
    }
}

/// Only a reserved tail answers to a chosen tier, so a volume with a capacity tier keeps one per compaction pass
fn reserved_count(shared: &ReelShared) -> usize {
    match shared.volumes.has_capacity() {
        true => shared.config.compact_passes(),
        false => 0,
    }
}

/// The mapped check looks this many records ahead, so that many memory stalls overlap
const PREFETCH_AHEAD: usize = 16;

/// The prefetch asks for this many lines of a record: its header, its key and a small payload
const PREFETCH_LINES: usize = 4;

const CACHE_LINE: usize = 64;

/// Ask the machine for a record's first lines without waiting on them
fn prefetch_record(record: &[u8]) {
    let lines = record.len().div_ceil(CACHE_LINE).min(PREFETCH_LINES);
    for line in 0..lines {
        crate::io::mapping::prefetch(record[line * CACHE_LINE..].as_ptr());
    }
}

impl Reel {
    /// Open a reel with the configured number of active tails, resuming unsealed ones in number order
    pub fn open(shared: Arc<ReelShared>, resumable: Vec<ResumableTail>) -> Result<Reel> {
        let count = shared.config.tail_count();
        let reserved = reserved_count(&shared);
        let mut candidates = resumable.into_iter();
        let mut tails = Vec::with_capacity(count + reserved);
        for index in 0..count + reserved {
            // Reserved tails come last and never resume, so each pass starts on a segment of its own
            let adopted = match index < count {
                true => candidates.next(),
                false => None,
            };
            tails.push(Appender::open(Arc::clone(&shared), index as u64, adopted)?);
        }
        Ok(Reel {
            shared,
            tails,
            reserved,
            leased: AtomicU64::new(0),
        })
    }

    /// Hold a reserved tail for one pass, or nothing when the volume keeps none free
    pub fn lease_reserved(&self) -> Option<ReservedLease<'_>> {
        let first = self.foreground().len();
        let mut leased = self.leased.load(Ordering::Acquire);
        loop {
            let free = (0..self.reserved).find(|at| leased & (1u64 << at) == 0)?;
            match self.leased.compare_exchange_weak(
                leased,
                leased | (1u64 << free),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(ReservedLease {
                        reel: self,
                        index: first + free,
                    })
                }
                Err(found) => leased = found,
            }
        }
    }

    /// Whether this volume keeps tails back for compaction
    pub fn keeps_reserved(&self) -> bool {
        self.reserved > 0
    }

    /// The tails a foreground write may be routed to
    fn foreground(&self) -> &[Appender] {
        &self.tails[..self.tails.len() - self.reserved]
    }

    /// Open a reel with no append tails, for a read-only open that never writes
    pub fn open_read_only(shared: Arc<ReelShared>) -> Reel {
        Reel {
            shared,
            tails: Vec::new(),
            reserved: 0,
            leased: AtomicU64::new(0),
        }
    }

    /// Shared reel state, for wiring an index and counters over it
    pub fn shared(&self) -> &Arc<ReelShared> {
        &self.shared
    }

    /// The reel's active append tails
    pub fn tails(&self) -> &[Appender] {
        &self.tails
    }

    /// Append or overwrite a payload, routed to the least-loaded tail
    pub fn put(
        &self,
        key: RecordKey,
        payload: Vec<u8>,
        codec: u8,
        commit: Commit,
    ) -> Result<Committed> {
        self.route().append_data(key, payload, codec, commit)
    }

    /// The same append awaited, taking its durability point on the async door
    ///
    /// Admission is awaited, and the sync a per-record commit owes is forwarded to
    /// the tail's sealer rather than run on the caller.
    pub async fn put_wait(
        &self,
        key: RecordKey,
        payload: Vec<u8>,
        codec: u8,
        commit: Commit,
    ) -> Result<Committed> {
        self.route()
            .append_data_wait(key, payload, codec, commit)
            .await
    }

    /// Append a tombstone for one key, routed to the least-loaded tail
    pub fn delete(&self, key: RecordKey, commit: Commit) -> Result<Committed> {
        self.route().append_tombstone(key, commit)
    }

    /// Append a tombstone covering a half-open key range within one column
    pub fn delete_range(
        &self,
        start: RecordKey,
        end: Option<&[u8]>,
        commit: Commit,
    ) -> Result<Committed> {
        self.route().append_range_tombstone(start, end, commit)
    }

    /// Append a whole batch to one tail as one reservation and one write
    ///
    /// Everything the batch carries lands together or not at all: the records go down
    /// back to back behind a frame declaring their count and their span, and a rebuild
    /// keeps the run only when it reads exactly what the frame declared.
    pub fn write_batch(&self, records: Vec<BatchRecord>) -> Result<Vec<Committed>> {
        self.route().append_batch(records)
    }

    /// The same batch awaited, admitted without holding a thread for the budget
    pub async fn write_batch_wait(&self, records: Vec<BatchRecord>) -> Result<Vec<Committed>> {
        self.route().append_batch_wait(records).await
    }

    /// The least loaded foreground tail, where a write or a compaction pass lands
    pub fn least_loaded(&self) -> usize {
        let mut chosen = 0;
        let mut lowest = u64::MAX;
        for (at, tail) in self.foreground().iter().enumerate() {
            let load = tail.load();
            if load < lowest {
                lowest = load;
                chosen = at;
            }
        }
        chosen
    }

    /// Sync every active tail
    pub fn flush(&self) -> Result<()> {
        for tail in &self.tails {
            tail.flush()?;
        }
        self.check_past_saving()
    }

    /// Sync every active tail, awaited, with no fsync on the calling thread
    pub async fn flush_wait(&self) -> Result<()> {
        for tail in &self.tails {
            tail.flush_wait().await?;
        }
        self.check_past_saving()
    }

    /// Refuse to answer clean while any segment holds records past saving
    ///
    /// A segment a failed sync ended holds acknowledged records that read from cache
    /// and are gone at the next open, so a flush cannot answer Ok over them.
    fn check_past_saving(&self) -> Result<()> {
        let stranded = self.shared.past_saving_count();
        if stranded == 0 {
            return Ok(());
        }
        Err(ReelError::Io(std::io::Error::other(format!(
            "{stranded} segments hold acknowledged records a failed sync left past saving"
        ))))
    }

    /// Take the sync a batch left owed on every tail it touched
    pub fn sync_if_owed(&self) -> Result<()> {
        for tail in &self.tails {
            tail.sync_if_owed()?;
        }
        Ok(())
    }

    /// The same durability point awaited, with owed turns forwarded to the sealers
    pub async fn sync_if_owed_wait(&self) -> Result<()> {
        for tail in &self.tails {
            tail.sync_if_owed_wait().await?;
        }
        Ok(())
    }

    /// Seal every tail for a clean shutdown, so a reopen reads footers only
    ///
    /// Every tail is closed even when one of them fails, and the first failure is
    /// what the caller hears about.
    pub fn close(&self) -> Result<()> {
        let mut outcome = Ok(());
        for tail in &self.tails {
            if let Err(error) = tail.close() {
                outcome = outcome.and(Err(error));
            }
        }
        outcome
    }

    /// Read one record's payload, checking it still resolves the expected key
    ///
    /// The segment is resolved to a refcounted handle first, so the file cannot be
    /// unlinked while the read is in flight. A pointer the index has since moved
    /// reads as stale rather than as a wrong payload.
    pub fn read_record(
        &self,
        loc: Loc,
        expected: KeyRef<'_>,
        lsn: Lsn,
        is_verified: bool,
        is_placed: bool,
    ) -> Result<RecordRead> {
        let handle = match self.handle_for(loc.segment)? {
            Some(handle) => handle,
            None => return Ok(RecordRead::Gone),
        };
        let layout = handle.layout();
        let prefix = layout.prefix_len(expected.width(), loc.len);
        let framed = self.read_framed(&handle, u64::from(loc.offset), prefix, loc.len as usize)?;
        let (head, body) = match framed {
            Some(framed) => framed,
            None => return Ok(RecordRead::Stale),
        };
        Ok(frame_to_read(
            head,
            body,
            expected,
            lsn,
            loc,
            layout,
            Proof::of(is_verified, is_placed),
        ))
    }

    /// Read one record as a future, always through the driver
    ///
    /// The mapping is left to the blocking door: a page fault cannot be awaited and
    /// a device error inside one arrives as SIGBUS on whichever worker was polling.
    pub async fn read_record_wait(
        &self,
        loc: Loc,
        expected: KeyRef<'_>,
        lsn: Lsn,
        is_verified: bool,
        is_placed: bool,
    ) -> Result<RecordRead> {
        let handle = match self.handle_for(loc.segment)? {
            Some(handle) => handle,
            None => return Ok(RecordRead::Gone),
        };
        let layout = handle.layout();
        let prefix = layout.prefix_len(expected.width(), loc.len);
        let len = loc.len as usize;
        let spare = take_header();
        let read = self
            .shared
            .driver
            .wait_split_reusing(
                handle.file(),
                u64::from(loc.offset),
                prefix,
                len,
                spare,
                self.shared.warm_first(),
            )
            .await;
        let (head, body) = match framed_or_nothing(read, prefix, len)? {
            Some(framed) => framed,
            None => return Ok(RecordRead::Stale),
        };
        Ok(frame_to_read(
            head,
            body,
            expected,
            lsn,
            loc,
            layout,
            Proof::of(is_verified, is_placed),
        ))
    }

    /// Read one window of a record's payload with no echo, one device read
    ///
    /// For the caller whose index already vouched for the offset: a live segment's
    /// record bytes never change and its number is never reused. Nothing comes back
    /// for a window the volume cannot answer whole, an absent segment included.
    pub fn read_window(
        &self,
        loc: Loc,
        key_width: u16,
        at: u64,
        len: usize,
    ) -> Result<Option<Value>> {
        let Some(handle) = self.handle_for(loc.segment)? else {
            return Ok(None);
        };
        let start = window_start(
            loc,
            handle.layout().prefix_len(key_width as usize, loc.len),
            at,
        );

        if self.maps(loc.segment, len) {
            if let Some(map) = handle.mapping(self.shared.config.segment_bytes.to_bytes()) {
                if let Some(bytes) = map.slice(start, len) {
                    let mut body = crate::reel::payload::take(len);
                    body.extend_from_slice(bytes);
                    return Ok(Some(Value::pooled(body, crate::reel::payload::give)));
                }
            }
        }

        let read = self.shared.driver.pread_reusing(
            handle.file(),
            start,
            len as u64,
            crate::reel::payload::take(len),
        );
        window_or_nothing(read, len)
    }

    /// Read one window as a future, always through the driver
    pub async fn read_window_wait(
        &self,
        loc: Loc,
        key_width: u16,
        at: u64,
        len: usize,
    ) -> Result<Option<Value>> {
        let Some(handle) = self.handle_for(loc.segment)? else {
            return Ok(None);
        };
        let start = window_start(
            loc,
            handle.layout().prefix_len(key_width as usize, loc.len),
            at,
        );
        let read = self
            .shared
            .driver
            .wait_pread_reusing(
                handle.file(),
                start,
                len as u64,
                crate::reel::payload::take(len),
            )
            .await;
        window_or_nothing(read, len)
    }

    /// Read part of one record's payload, without reading the rest of it
    ///
    /// The header still rides along for the echo, and a range beginning within
    /// MERGE_GAP of the payload's start comes back in that same read; a deeper one
    /// takes a read of its own in the same submission.
    ///
    /// It cannot verify: the checksum covers the whole payload and this read holds a
    /// piece of it.
    pub fn read_range(
        &self,
        loc: Loc,
        expected: KeyRef<'_>,
        lsn: Lsn,
        at: u64,
        len: usize,
        is_placed: bool,
    ) -> Result<RecordRead> {
        let handle = match self.handle_for(loc.segment)? {
            Some(handle) => handle,
            None => return Ok(RecordRead::Gone),
        };
        let offset = u64::from(loc.offset);
        // A keyless record checks only whole, so its window is cut from a whole read.
        if let Some(check) = handle.layout().keyless_key(loc.len) {
            let framed = self.read_framed(&handle, offset, KEYLESS_PREFIX, loc.len as usize)?;
            return Ok(match framed {
                Some((head, body)) => keyless_range(
                    head,
                    body,
                    expected,
                    &check,
                    at,
                    len,
                    Proof::of(false, is_placed),
                ),
                None => RecordRead::Stale,
            });
        }
        let prefix = HEADER_LEN + expected.width();

        if let Some((head, body)) = self.map_range(&handle, offset, prefix, at, len) {
            let verdict = frame_to_range(&head, body, expected, lsn, loc);
            recycle_header(head);
            return verdict;
        }

        if at <= MERGE_GAP {
            let span = at as usize + len;
            let read = self.shared.driver.pread_split_reusing(
                handle.file(),
                offset,
                prefix,
                span,
                take_header(),
                self.shared.warm_first(),
            );
            return near_range(read, prefix, at, len, expected, lsn, loc);
        }

        let ops = self.range_ops(&handle, offset, prefix, at, len);
        let filled = self.shared.driver.run_split_reads(ops)?;
        deep_range(filled, prefix, len, expected, lsn, loc)
    }

    /// Read part of one record's payload as a future, always through the driver
    pub async fn read_range_wait(
        &self,
        loc: Loc,
        expected: KeyRef<'_>,
        lsn: Lsn,
        at: u64,
        len: usize,
        is_placed: bool,
    ) -> Result<RecordRead> {
        let handle = match self.handle_for(loc.segment)? {
            Some(handle) => handle,
            None => return Ok(RecordRead::Gone),
        };
        let offset = u64::from(loc.offset);
        if let Some(check) = handle.layout().keyless_key(loc.len) {
            let whole = loc.len as usize;
            let read = self
                .shared
                .driver
                .wait_split_reusing(
                    handle.file(),
                    offset,
                    KEYLESS_PREFIX,
                    whole,
                    take_header(),
                    WarmFirst::Skip,
                )
                .await;
            return Ok(match framed_or_nothing(read, KEYLESS_PREFIX, whole)? {
                Some((head, body)) => keyless_range(
                    head,
                    body,
                    expected,
                    &check,
                    at,
                    len,
                    Proof::of(false, is_placed),
                ),
                None => RecordRead::Stale,
            });
        }
        let prefix = HEADER_LEN + expected.width();

        if at <= MERGE_GAP {
            let span = at as usize + len;
            let read = self
                .shared
                .driver
                .wait_split_reusing(
                    handle.file(),
                    offset,
                    prefix,
                    span,
                    take_header(),
                    WarmFirst::Skip,
                )
                .await;
            return near_range(read, prefix, at, len, expected, lsn, loc);
        }

        let ops = self.range_ops(&handle, offset, prefix, at, len);
        let filled = self.shared.driver.wait_split_reads(ops).await?;
        deep_range(filled, prefix, len, expected, lsn, loc)
    }

    /// The two reads a deep range takes: the record's header, and the range itself
    ///
    /// Both are built here and submitted together, so the second is not a round trip
    /// behind the first.
    fn range_ops(
        &self,
        handle: &SegmentHandle,
        offset: u64,
        prefix: usize,
        at: u64,
        len: usize,
    ) -> Vec<Op> {
        let driver = &self.shared.driver;
        vec![
            driver.split_read(handle.file(), offset, 0, prefix),
            driver.split_read(handle.file(), offset + prefix as u64 + at, 0, len),
        ]
    }

    /// Copy a range and the record's header out of a segment mapping
    ///
    /// A mapping that does not cover both of them leaves the read to the driver.
    fn map_range(
        &self,
        handle: &SegmentHandle,
        offset: u64,
        prefix: usize,
        at: u64,
        len: usize,
    ) -> Option<(Vec<u8>, Value)> {
        if !self.maps(handle.id(), len) {
            return None;
        }
        let map = handle.mapping(self.shared.config.segment_bytes.to_bytes())?;
        let head_bytes = map.slice(offset, prefix)?;
        let body_bytes = map.slice(offset + prefix as u64 + at, len)?;

        let mut head = take_header();
        head.clear();
        head.extend_from_slice(head_bytes);
        let mut body = crate::reel::payload::take(len);
        body.extend_from_slice(body_bytes);
        Some((head, Value::pooled(body, crate::reel::payload::give)))
    }

    /// Read several records with one submission, each left in place in its run's block
    ///
    /// One spot per key, in the caller's order, into vectors the caller keeps. Records
    /// written in one batch are one contiguous byte range, so neighbours are read
    /// together and every record is framed inside its run's block. A walk lends
    /// straight from the blocks, and a caller keeping a record takes a window of one.
    /// A spot of `Spot::MISS` is a record the caller resolves again: its segment is
    /// gone, the pointer is stale, or the bytes failed their checks.
    pub fn read_placed(
        &self,
        asks: &[Ask],
        keys: &[KeyRef<'_>],
        is_verified: bool,
        blocks: &mut Vec<ReadBlock>,
        spots: &mut Vec<Spot>,
    ) -> Result<()> {
        let mut held = HeldScratch::take();
        let scratch = &mut held.0;
        if self.plan_reads(asks, keys, is_verified, scratch, blocks, spots)? {
            self.shared.driver.run_split_reads_into(
                &mut scratch.ops,
                &mut scratch.completions,
                &mut scratch.filled,
            )?;
            place_runs(scratch, asks, keys, is_verified, blocks, spots)?;
        }
        Ok(())
    }

    /// Read several records as one future, each left in place in its run's block
    pub async fn read_placed_wait(
        &self,
        asks: &[Ask],
        keys: &[KeyRef<'_>],
        is_verified: bool,
        blocks: &mut Vec<ReadBlock>,
        spots: &mut Vec<Spot>,
    ) -> Result<()> {
        let mut held = HeldScratch::take();
        let scratch = &mut held.0;
        if self.plan_reads(asks, keys, is_verified, scratch, blocks, spots)? {
            self.shared
                .driver
                .wait_split_reads_into(&mut scratch.ops, &mut scratch.filled)
                .await?;
            place_runs(scratch, asks, keys, is_verified, blocks, spots)?;
        }
        Ok(())
    }

    /// Place every record a mapping covers and build one read per run of the rest
    ///
    /// A mapped record is checked where it lies and copied out once, into one block
    /// for the batch. Key order is not offset order, so the asks left for the driver
    /// are sorted by place first: runs only form once neighbours are neighbours. An
    /// ask whose segment is gone stays a miss. False when nothing is left to read.
    fn plan_reads(
        &self,
        asks: &[Ask],
        keys: &[KeyRef<'_>],
        is_verified: bool,
        scratch: &mut ReadScratch,
        blocks: &mut Vec<ReadBlock>,
        spots: &mut Vec<Spot>,
    ) -> Result<bool> {
        blocks.clear();
        spots.clear();
        spots.resize(keys.len(), Spot::MISS);
        scratch.order.clear();
        scratch.plan.clear();
        scratch.handles.clear();
        // Every mapped segment is held before any read, so the handle list never moves under a borrowed record
        for ask in asks {
            if self.maps(ask.loc.segment, ask.loc.len as usize) {
                self.hold_segment(ask.loc.segment, &mut scratch.handles)?;
            }
        }
        // Each record is prefetched a window ahead of its check, so the memory stalls overlap
        let handles = &scratch.handles;
        let mut ahead: [Option<(&[u8], RecordLayout)>; PREFETCH_AHEAD] = [None; PREFETCH_AHEAD];
        let mut mapped = Vec::new();
        for step in 0..asks.len() + PREFETCH_AHEAD {
            let slot = step % PREFETCH_AHEAD;
            let due = ahead[slot].take();
            if let Some(ask) = asks.get(step) {
                let record = self.in_place(handles, keys, ask);
                if let Some((record, _)) = record {
                    prefetch_record(record);
                }
                ahead[slot] = record;
            }
            let Some(at) = step.checked_sub(PREFETCH_AHEAD) else {
                continue;
            };
            let ask = &asks[at];
            let key = keys[ask.at as usize];
            let len = ask.loc.len as usize;
            let Some((record, layout)) = due else {
                let place = (u64::from(ask.loc.segment.0) << 32) | u64::from(ask.loc.offset);
                scratch.order.push((place, at as u32));
                continue;
            };
            let prefix = layout.prefix_len(key.width(), ask.loc.len);
            if let Ok(codec) = check_in_block(
                record,
                0,
                prefix,
                key,
                ask.lsn,
                ask.loc,
                layout,
                Proof::of(is_verified, ask.certain),
            ) {
                if mapped.capacity() == 0 {
                    let wanted = asks.iter().map(|ask| ask.loc.len as usize).sum();
                    mapped = crate::reel::payload::take(wanted);
                }
                spots[ask.at as usize] = Spot {
                    block: 0,
                    at: mapped.len() as u32,
                    len: len as u32,
                    codec,
                };
                mapped.extend_from_slice(&record[prefix..]);
            }
        }
        if mapped.capacity() != 0 {
            blocks.push(ReadBlock::new(mapped, crate::reel::payload::give));
        }
        scratch.order.sort_unstable_by_key(|&(place, _)| place);

        scratch.plan.reserve(scratch.order.len());
        for slot in 0..scratch.order.len() {
            let at = scratch.order[slot].1 as usize;
            let ask = &asks[at];
            // Sorted by segment, so a segment's handle is asked for once, at its first record.
            let (file, layout) = match scratch.plan.last() {
                Some(last) if last.segment == ask.loc.segment => (last.file, last.layout),
                _ => match self.hold_segment(ask.loc.segment, &mut scratch.handles)? {
                    Some(handle) => (handle.file(), handle.layout()),
                    None => continue,
                },
            };
            scratch.plan.push(Planned {
                at,
                segment: ask.loc.segment,
                file,
                layout,
                offset: u64::from(ask.loc.offset),
                prefix: layout.prefix_len(keys[ask.at as usize].width(), ask.loc.len),
                len: ask.loc.len as usize,
            });
        }
        if scratch.plan.is_empty() {
            return Ok(false);
        }

        merge_runs_into(
            &scratch.plan,
            merge_span(self.shared.driver.serving()),
            &mut scratch.runs,
        );
        // Every record's header sits inside its run's span, so each run is one read
        // of the whole span, with no header of its own.
        scratch.ops.clear();
        scratch.ops.reserve(scratch.runs.len());
        for run in &scratch.runs {
            let first = &scratch.plan[run.start];
            let op = self
                .shared
                .driver
                .split_read(first.file, first.offset, 0, run.span as usize);
            scratch.ops.push(op);
        }
        Ok(true)
    }

    /// A record's bytes in its segment's mapping, when the read maps and the batch holds the segment
    fn in_place<'held>(
        &self,
        handles: &'held [SegmentHandle],
        keys: &[KeyRef<'_>],
        ask: &Ask,
    ) -> Option<(&'held [u8], RecordLayout)> {
        let len = ask.loc.len as usize;
        if !self.maps(ask.loc.segment, len) {
            return None;
        }
        let at = handles
            .binary_search_by_key(&ask.loc.segment, SegmentHandle::id)
            .ok()?;
        let layout = handles[at].layout();
        let map = handles[at].mapping(self.shared.config.segment_bytes.to_bytes())?;
        let prefix = layout.prefix_len(keys[ask.at as usize].width(), ask.loc.len);
        map.slice(u64::from(ask.loc.offset), prefix + len)
            .map(|record| (record, layout))
    }

    /// Take a segment's handle once per batch and lend it for every record after
    ///
    /// Held until the batch is done, so no segment is unlinked under a read in flight.
    /// The handles stay in segment order, so finding one is a search. Nothing when the
    /// segment is gone.
    fn hold_segment<'held>(
        &self,
        segment: SegmentId,
        handles: &'held mut Vec<SegmentHandle>,
    ) -> Result<Option<&'held SegmentHandle>> {
        let at = match handles.binary_search_by_key(&segment, SegmentHandle::id) {
            Ok(at) => at,
            Err(at) => match self.handle_for(segment)? {
                Some(handle) => {
                    handles.insert(at, handle);
                    at
                }
                None => return Ok(None),
            },
        };
        Ok(Some(&handles[at]))
    }

    /// Whether a read of `len` bytes in `segment` goes through the segment's mapping
    fn maps(&self, segment: SegmentId, len: usize) -> bool {
        let config = &self.shared.config;
        // A tail's pages were just written, so a mapping reads them from the page cache with no syscall
        config.maps(len)
            || (config.maps_tails()
                && self
                    .tails()
                    .iter()
                    .any(|tail| tail.tail().active_segment() == segment))
    }

    /// Resolve a segment number to a handle, opening and caching it on a miss
    pub fn handle_for(&self, segment: SegmentId) -> Result<Option<SegmentHandle>> {
        self.shared.handle_for(segment)
    }

    /// Read a framed record into its header and key and its payload, or nothing
    /// when the segment no longer holds a whole record at that offset
    fn read_framed(
        &self,
        handle: &SegmentHandle,
        offset: u64,
        prefix: usize,
        len: usize,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        // A mapped volume serves a record the mapping covers straight out of the
        // page cache; anything it does not cover takes the driver below.
        if self.maps(handle.id(), len) {
            if let Some(map) = handle.mapping(self.shared.config.segment_bytes.to_bytes()) {
                let head_at = map.slice(offset, prefix);
                let body_at = map.slice(offset + prefix as u64, len);
                if let (Some(head_bytes), Some(body_bytes)) = (head_at, body_at) {
                    let mut head = take_header();
                    head.clear();
                    head.extend_from_slice(head_bytes);
                    let mut body = crate::reel::payload::take(len);
                    body.extend_from_slice(body_bytes);
                    return Ok(Some((head, body)));
                }
            }
        }

        let spare = take_header();
        let read = self.shared.driver.pread_split_reusing(
            handle.file(),
            offset,
            prefix,
            len,
            spare,
            self.shared.warm_first(),
        );
        framed_or_nothing(read, prefix, len)
    }

    /// The tail a foreground write goes to, which is the least loaded of them
    fn route(&self) -> &Appender {
        &self.foreground()[self.least_loaded()]
    }
}

thread_local! {
    /// One header buffer per reading thread, handed back after every framed read
    static HEADER_SPARE: std::cell::Cell<Vec<u8>> = const { std::cell::Cell::new(Vec::new()) };
}

/// This thread's header buffer, or a fresh one when it has none to lend
fn take_header() -> Vec<u8> {
    HEADER_SPARE.with(|held| held.take())
}

/// Hand a header buffer back for the next read on this thread to fill
fn recycle_header(head: Vec<u8>) {
    // The roomier of the two is kept, since a merged read hands back a header
    // buffer it never filled.
    HEADER_SPARE.with(|held| {
        let spare = held.take();
        held.set(match spare.capacity() > head.capacity() {
            true => spare,
            false => head,
        });
    });
}

fn is_missing(error: &ReelError) -> bool {
    if let ReelError::Io(source) = error {
        return source.kind() == std::io::ErrorKind::NotFound;
    }
    false
}

/// File name a segment number resolves to within a reel directory
pub fn segment_file_name(id: SegmentId) -> String {
    format!(
        "{number:0width$}{suffix}",
        number = id.as_u32(),
        width = SEGMENT_DIGITS,
        suffix = SEGMENT_SUFFIX,
    )
}

/// Segment number parsed back from a segment file name
pub fn segment_number(name: &str) -> Option<u32> {
    name.strip_suffix(SEGMENT_SUFFIX)?.parse().ok()
}
