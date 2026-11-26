//! The scrub: a pass over every sealed segment that checks each record's sum and record check, so a record that rotted on the
//! disk shows as damage before a read needs it. It reads past the record cache and leaves no pages in the kernel's cache, and its
//! pace climbs from a floor while its own reads stay quick and falls back when they slow or callers burst, so foreground reads
//! keep their share of the device

use std::mem;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, FileOffset, Nanoseconds, WritePlace};

use crate::engine::engine_state::EngineState;
use crate::engine::walk_job::FREE_IN_PLACE_BYTES;
use crate::guard::hold;
use crate::io::job::{Job, JobStep};
use crate::engine::scrub_pace::ScrubPace;
use crate::engine::open_segment::data_path;
use crate::pool::{TOUCH_BYTE, touched_filled};
use crate::io::{DIRECT_ALIGN, DeviceReads, Direct, InPlace, Io, IoFile, in_place};
use crate::record::record_check::RecordIdentity;
use crate::record::record_reader::{RecordLocation, RecordRead};
use crate::row_kind::RowKind;
use crate::segment::row_block::RowBlockWalk;
use crate::segment::segment_tail::SegmentNumber;

/// The floor a store's scrub rate never falls under when its config says nothing else, in bytes a second
pub const DEFAULT_SCRUB_BYTES_PER_SECOND: u64 = 4 * 1024 * 1024;
/// How long a scrub rests between passes
pub const SCRUB_PASS_REST: Nanoseconds = Nanoseconds(3_600_000_000_000);
/// The most record bytes one step checks before it stops inside its block, so the device never sees a long burst
pub const STEP_BYTES: u64 = 1024 * 1024;
/// Records this long or longer are read past the page cache. Shorter ones read plainly, so the records sharing a page come
/// from one device read and their pages are a small share of the cache
pub const LEAST_UNCACHED_RECORD: u64 = 64 * 1024;
/// The most a scrub that stood idle may check in one go, in seconds of its rate
const MOST_BANKED_SECONDS: u64 = 1;
const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// One volume's scrub: where its pass stands, and what its rate has earned
pub struct ScrubJob<Backend: Io> {
    state: Arc<EngineState<Backend>>,
    volume: usize,
    /// The floor of bytes a second the pass checks, the store's rate unless a caller sets its own. The pace climbs from it while
    /// the scrub's reads stay quick
    pub rate: u64,
    pace: ScrubPace,
    /// The segment the pass stands in, and its next row block
    segment: Option<SegmentNumber>,
    block: u32,
    /// Bytes the rate lets the pass check, which a block's records can take below zero, and when it last earned
    allowance: i64,
    earned_at: Nanoseconds,
    /// A pass ended, and the next waits until then
    rest_until: Nanoseconds,
    /// The row block in `block_bytes`, and the rows of it the pass has checked, so a step can stop inside a block
    loaded: Option<(SegmentNumber, u32)>,
    rows_done: usize,
    block_bytes: Vec<u8>,
    key: Vec<u8>,
    value: Vec<u8>,
    /// The segment a direct handle is open on, for the pace's probe: one aligned read a second that skips the page cache both
    /// ways, so its time is the device's whatever the cache holds
    probe: Option<(SegmentNumber, Backend::File)>,
    probe_bytes: Vec<u8>,
    probed_at: Nanoseconds,
    probe_seed: u64,
    /// The first record the last step left unchecked: its segment, offset and length, for the probe to read a page of
    probe_target: Option<(SegmentNumber, FileOffset, ByteCount)>,
    /// The aligned buffer a long record's direct read goes through, grown to the longest record, so a pass at its cap
    /// allocates nothing a record
    direct_scratch: Vec<u8>,
    /// A direct read failed where a plain one of the same record passed, so the device or mount refuses direct reads, and the job
    /// reads plainly from then on with no probe
    is_direct_refused: bool,
}

impl<Backend: Io> ScrubJob<Backend> {
    /// Start a volume's scrub at its first segment
    pub fn new(state: Arc<EngineState<Backend>>, volume: usize) -> Self {
        Self {
            rate: state.scrub_rate,
            pace: ScrubPace::new(state.scrub_rate),
            state,
            volume,
            segment: None,
            block: 0,
            allowance: 0,
            earned_at: Nanoseconds(0),
            rest_until: Nanoseconds(0),
            loaded: None,
            rows_done: 0,
            block_bytes: Vec::new(),
            key: Vec::new(),
            value: Vec::new(),
            probe: None,
            // twice the alignment, so an aligned read fits wherever the allocation starts, and written at open as every buffer is
            probe_bytes: touched_filled(2 * DIRECT_ALIGN, TOUCH_BYTE),
            probed_at: Nanoseconds(0),
            probe_seed: 0x9E37_79B9_7F4A_7C15,
            probe_target: None,
            direct_scratch: Vec::new(),
            is_direct_refused: false,
        }
    }

    /// Time one aligned read of the segment past the page cache, at most once a second, and give the pace its time. A system with
    /// no direct read gives none, and the pace holds at its floor with the burst rule alone
    fn probe_device(&mut self, segment: SegmentNumber, now: Nanoseconds) {
        if now.0.saturating_sub(self.probed_at.0) < NANOS_PER_SECOND {
            return;
        }
        self.probed_at = now;
        let state = Arc::clone(&self.state);
        if !self.open_direct(segment) {
            return;
        }
        // the probe reads a page of the next record the pass has yet to check: bytes a write put there, so never a hole or room
        // that reads as zeros with no device read, and bytes nothing read lately, so never the drive's own cache
        let Some((target, start, length)) = self.probe_target.take() else {
            return;
        };
        let Some((_segment, file)) = self.probe.as_ref().filter(|(number, _file)| *number == target) else {
            return;
        };
        let first = start.0 / DIRECT_ALIGN as u64;
        let pages = (start.0 + length.0.max(1)).div_ceil(DIRECT_ALIGN as u64) - first;
        // a step of a 64-bit mixer picks the page inside a long record
        self.probe_seed = self.probe_seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        let offset = FileOffset((first + (self.probe_seed >> 11) % pages) * DIRECT_ALIGN as u64);
        let skip = self.probe_bytes.as_ptr().align_offset(DIRECT_ALIGN);
        let buf = &mut self.probe_bytes[skip..skip + DIRECT_ALIGN];
        let started = state.io.now();
        if file.read_exact_at(offset, buf).is_ok() {
            self.pace.note_read(Nanoseconds(state.io.now().0.saturating_sub(started.0)), DIRECT_ALIGN as u64);
        }
    }

    /// Once a second, pick a record of the loaded block the pass has yet to check and time a page of it past the page cache. It runs
    /// whether or not the rate lets the pass check this step, so the pace has its samples at any rate
    fn probe_ahead(&mut self, now: Nanoseconds) {
        if now.0.saturating_sub(self.probed_at.0) < NANOS_PER_SECOND {
            return;
        }
        let Some((segment, block)) = self.loaded else {
            return;
        };
        let Some(column) = self.state.volumes[self.volume].table.walk_block(segment, block).map(|line| line.column) else {
            return;
        };
        let Some(place_base) = self.state.volumes[self.volume].table.pin(segment).map(|pin| pin.head.place_base) else {
            return;
        };
        let Some(mut ahead) = RowBlockWalk::open_checked(&self.block_bytes, place_base) else {
            return;
        };
        if ahead.rows() <= self.rows_done {
            return;
        }
        self.probe_seed = self.probe_seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        let pick = self.rows_done + 1 + (self.probe_seed >> 11) as usize % (ahead.rows() - self.rows_done);
        let mut at = 0;
        while let Some(row) = ahead.next_row(&mut self.key) {
            at += 1;
            // a large record a free punched keeps its row but reads as zeros with no device read, so the probe takes a long
            // record only while it is its key's newest
            if at >= pick && row.kind == RowKind::Value && (row.length < FREE_IN_PLACE_BYTES || self.is_newest(column, row.place)) {
                self.probe_target = Some((segment, row.offset, row.length));
                break;
            }
        }
        self.probe_device(segment, now);
    }

    /// Hold a direct handle on a segment, opening it when the one held is another's, and say whether one is held
    fn open_direct(&mut self, segment: SegmentNumber) -> bool {
        if self.is_direct_refused {
            self.close_probe();
            return false;
        }
        if self.probe.as_ref().is_some_and(|(number, _file)| *number == segment) {
            return true;
        }
        self.close_probe();
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let Some(root) = volume.table.root_of(segment) else {
            return false;
        };
        let Ok(Some(file)) = state.io.open_direct(&data_path(&volume.roots[root.0 as usize], segment)) else {
            return false;
        };
        self.probe = Some((segment, file));
        true
    }

    /// Close the probe's direct handle, once
    fn close_probe(&mut self) {
        if let Some((_segment, file)) = self.probe.take() {
            self.state.io.close_file(file);
        }
    }

    /// Read the rate the pass checks at now, which the pace moves between the floor and many times it
    #[cfg(feature = "test-hooks")]
    pub fn current_rate(&self) -> u64 {
        self.pace.rate()
    }

    /// Say the job stopped reading direct, after a direct read failed where a plain one passed
    #[cfg(feature = "test-hooks")]
    pub fn is_direct_refused(&self) -> bool {
        self.is_direct_refused
    }

    /// Read the pace's baseline read time a KiB, or nothing before it took a sample
    #[cfg(feature = "test-hooks")]
    pub fn baseline(&self) -> Option<u64> {
        self.pace.baseline()
    }

    /// Check one row block's records while the rate allows, or say when it allows the next
    pub fn step_unit(&mut self, now: Nanoseconds) -> JobStep {
        let rate = self.rate;
        if rate == 0 || self.state.is_read_only {
            return JobStep::Idle;
        }
        if now < self.rest_until {
            return JobStep::IdleUntil { deadline: self.rest_until };
        }
        // a caller that set its own floor starts the pace over from it
        if self.pace.floor() != rate {
            self.pace = ScrubPace::new(rate);
        }
        self.pace.tick(now, self.state.caller_ops.total());
        let rate = self.pace.rate();
        self.state.volumes[self.volume].scrub_rate.store(rate, Ordering::Relaxed);
        self.probe_ahead(now);
        self.earn(now, rate);
        // a rate that fell takes effect at once: the bank holds at most a second of it
        self.allowance = self.allowance.min(rate as i64);
        if self.allowance <= 0 {
            // the job wakes at least once a second while it waits, so the probe keeps the pace fed at any rate
            let wait = ((self.allowance.unsigned_abs() + 1).saturating_mul(NANOS_PER_SECOND) / rate).min(NANOS_PER_SECOND);
            return JobStep::IdleUntil { deadline: Nanoseconds(now.0.saturating_add(wait.max(1))) };
        }
        let Some(segment) = self.segment.or_else(|| self.next_segment(None)) else {
            // a store with nothing sealed yet has nothing to check, so the scrub looks again soon instead of resting a whole pass, and
            // its rate reads 0 since it reads nothing
            self.state.volumes[self.volume].scrub_rate.store(0, Ordering::Relaxed);
            self.rest_until = Nanoseconds(now.0.saturating_add(SCRUB_LOOK_PERIOD.0));
            return JobStep::IdleUntil { deadline: self.rest_until };
        };
        self.segment = Some(segment);
        if !self.check_block(segment, now) {
            self.block = 0;
            self.loaded = None;
            self.segment = self.next_segment(Some(segment));
            if self.segment.is_none() {
                self.end_pass(now);
            }
        }
        JobStep::Worked
    }

    /// Earn what the rate gives for the time since the last step, up to what a short idle banks
    fn earn(&mut self, now: Nanoseconds, rate: u64) {
        if self.earned_at.0 == 0 {
            self.earned_at = now;
            self.allowance = rate as i64;
            return;
        }
        let elapsed = now.0.saturating_sub(self.earned_at.0);
        let earned = (u128::from(elapsed) * u128::from(rate) / u128::from(NANOS_PER_SECOND)) as i64;
        let most = rate.saturating_mul(MOST_BANKED_SECONDS) as i64;
        self.allowance = self.allowance.saturating_add(earned).min(most);
        self.earned_at = now;
    }

    /// Count a finished pass and rest until the next
    fn end_pass(&mut self, now: Nanoseconds) {
        self.close_probe();
        self.segment = None;
        self.block = 0;
        self.state.scrub_passes.fetch_add(1, Ordering::Relaxed);
        let volume = &self.state.volumes[self.volume];
        volume.scrub_passes.fetch_add(1, Ordering::Relaxed);
        // a scrub resting until its next pass reads nothing, so its rate reads 0 until then
        volume.scrub_rate.store(0, Ordering::Relaxed);
        self.rest_until = Nanoseconds(now.0.saturating_add(SCRUB_PASS_REST.0));
    }

    /// Find the lowest sealed segment past one, or the lowest of all
    fn next_segment(&self, after: Option<SegmentNumber>) -> Option<SegmentNumber> {
        let summary = hold(&self.state.volumes[self.volume].summary);
        let numbers = summary.rows().map(|row| row.facts.segment);
        numbers.filter(|number| after.is_none_or(|after| *number > after)).min()
    }

    /// Check the records of a segment's current row block up to a step's bytes, and say whether the segment has more. A segment
    /// that left or failed its check has none. A step stops inside a block once its records pass STEP_BYTES, so the device never
    /// sees more than that at once whatever the rate
    fn check_block(&mut self, segment: SegmentNumber, now: Nanoseconds) -> bool {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let Some(pin) = volume.table.pin(segment) else {
            return false;
        };
        let Some(line) = volume.table.walk_block(segment, self.block) else {
            return false;
        };
        let Ok(info) = state.column(line.column) else {
            self.block += 1;
            return true;
        };
        if self.loaded != Some((segment, self.block)) {
            // a block reads plainly: the open read every block, so the page cache holds them already. The pace's time comes from
            // its own probe, never from a block read
            let length = line.entry.block_length().0 as usize;
            self.block_bytes.resize(length, 0);
            if in_place(InPlace.read_exact(&pin.file, line.entry.block_at(), &mut self.block_bytes)).is_err() {
                return false;
            }
            self.loaded = Some((segment, self.block));
            self.rows_done = 0;
        }
        self.probe_ahead(now);
        // the segment's direct handle opens before the walk, which holds the block, for the long records the walk meets
        let has_direct = self.open_direct(segment);
        let Some(mut walk) = RowBlockWalk::open_checked(&self.block_bytes, pin.head.place_base) else {
            return false;
        };
        let counts = state.cues.counts();
        let mut checked = 0;
        let mut row_at = 0;
        while let Some(row) = walk.next_row(&mut self.key) {
            row_at += 1;
            if row_at <= self.rows_done || row.kind != RowKind::Value {
                continue;
            }
            if checked >= STEP_BYTES {
                self.rows_done = row_at - 1;
                self.note_checked(checked);
                return true;
            }
            let identity = RecordIdentity { record_key: state.record_key, column: line.column, key: &self.key, kind: row.kind, place: row.place };
            let location = RecordLocation { offset: row.offset, length: row.length };
            // a long record is read from the device through the segment's direct handle, so the scrub checks the disk's copy, leaves
            // no page behind and touches no page of a caller's. A short one reads plainly, so the records sharing its page come from
            // one device read. Where no direct read opens, a long one reads plainly too
            let direct = if row.length.0 >= LEAST_UNCACHED_RECORD && has_direct && !self.is_direct_refused {
                let device = Direct::new(mem::take(&mut self.direct_scratch));
                let read = self.probe.as_ref().map(|(_segment, file)| {
                    let mut reader = state.record_reader_for(&device);
                    in_place(reader.read_value(&device, file, location, &identity, info.codec, &mut self.value))
                });
                self.direct_scratch = device.into_scratch();
                read
            } else {
                None
            };
            // a direct read whose bytes fail their check stands, and so does one the device failed: the disk's copy went bad under a
            // good one in the page cache, which is what reading direct is for. Any other error may be the read refused, as on a device
            // whose blocks pass DIRECT_ALIGN or a mount that takes the flag and not the read, so the record reads again plainly and
            // that decides. A plain read that passes then stops direct reads for the rest of the job
            let read = match direct {
                Some(Ok(read)) => Ok(read),
                Some(Err(error)) if is_media_error(&error) => Ok(RecordRead::Failed),
                other => {
                    let mut reader = state.record_reader_for(&InPlace);
                    let plain = in_place(reader.read_value(&InPlace, &pin.file, location, &identity, info.codec, &mut self.value));
                    if matches!(other, Some(Err(_))) && matches!(plain, Ok(RecordRead::Valid)) {
                        self.is_direct_refused = true;
                    }
                    plain
                },
            };
            checked += row.length.0;
            if matches!(read, Ok(RecordRead::Valid)) {
                continue;
            }
            // a free punches only large dead records, and moves the counts first, so a failure there is no damage
            let is_large = row.length >= FREE_IN_PLACE_BYTES;
            if is_large && (!state.has_outlived(counts, true) || !self.is_newest(line.column, row.place)) {
                continue;
            }
            state.scrub_failures.fetch_add(1, Ordering::Relaxed);
            // the scrub holds the failed record's key, so the damage is that key alone and every other key reads on
            state.damage_record(self.volume, segment, line.column, &self.key, row.place);
            self.note_checked(checked);
            return false;
        }
        self.note_checked(checked);
        self.block += 1;
        true
    }

    /// Charge the bytes a block's records took
    fn note_checked(&mut self, bytes: u64) {
        self.state.scrub_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.state.volumes[self.volume].scrub_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.allowance = self.allowance.saturating_sub(bytes as i64);
    }

    /// Check a row is still its key's newest, since a record a free may punch is a dead one
    fn is_newest(&self, column: ColumnId, place: WritePlace) -> bool {
        self.state.is_newest_row(self.volume, column, &self.key, place)
    }
}

/// Say an error is the device failing the bytes, as a bad sector does. A short read or a refused one is not
fn is_media_error(error: &std::io::Error) -> bool {
    matches!(error.raw_os_error(), Some(libc::EIO | libc::ENXIO | libc::ENODATA | libc::EBADMSG))
}

/// How often the scrub's thread looks for work when no scrub named a time, since nothing wakes it the way seals woke the planner
pub const SCRUB_LOOK_PERIOD: Nanoseconds = Nanoseconds(1_000_000_000);

/// The store's scrubs on a thread of their own, in the idle I/O class. A spinning disk busy with callers holds an idle read back
/// for seconds, and here that holds back nothing else: the planner keeps the default class and never issues an idle read
pub struct ScrubRunner<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub scrubs: Vec<ScrubJob<Backend>>,
    is_idle: bool,
}

impl<Backend: Io> ScrubRunner<Backend> {
    /// Run a scrub for every volume
    pub fn new(state: Arc<EngineState<Backend>>) -> Self {
        let scrubs = (0..state.volumes.len()).map(|volume| ScrubJob::new(Arc::clone(&state), volume)).collect();
        Self { state, scrubs, is_idle: false }
    }
}

impl<Backend: Io> Job for ScrubRunner<Backend> {
    fn step(&mut self) -> JobStep {
        if !self.is_idle {
            self.state.io.set_io_idle(true);
            self.is_idle = true;
        }
        let now = self.state.io.now();
        let mut soonest: Option<Nanoseconds> = None;
        for scrub in &mut self.scrubs {
            match scrub.step_unit(now) {
                JobStep::Worked => return JobStep::Worked,
                JobStep::Idle | JobStep::Finished => {},
                JobStep::IdleUntil { deadline } => soonest = Some(soonest.map_or(deadline, |other| Nanoseconds(other.0.min(deadline.0)))),
            }
        }
        match soonest {
            // a scrub at rest ends its rest early when new segments come, which it sees only when stepped
            Some(deadline) => JobStep::IdleUntil { deadline: Nanoseconds(deadline.0.min(now.0 + SCRUB_LOOK_PERIOD.0)) },
            None => JobStep::IdleUntil { deadline: Nanoseconds(now.0 + SCRUB_LOOK_PERIOD.0) },
        }
    }
}
