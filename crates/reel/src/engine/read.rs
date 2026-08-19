//! Point reads, ranges and batches, and the resolve loop they share

use std::ops::Bound;
use std::sync::Arc;

use crate::units::ByteCount;

use crate::config::RepairPath;
use crate::error::{ReelError, Result};
use crate::format::column::{Codec, ColumnId, KeyRef, RecordKey};
use crate::format::loc::Loc;
use crate::format::lsn::Lsn;
use crate::index::page::KeyPage;
use crate::index::playback::PlaybackCursor;
use crate::reel::cue::CuePoint;

use super::{read_only, FoundPlan, ReelStore, GRAVE_WINDOW, SWEEP_RUN};
use crate::index::entry::Entry;
use crate::index::recovery::rebuild_reel;
use crate::index::tailer::{catch_up, CaughtUp, LogCursor};
use crate::reel::{Ask, RecordRead};
use crate::sync::lock;
use reel_core::{range_of, Value};

/// How many times a read re-resolves a stale index pointer before giving up
const RESOLVE_RETRIES: u32 = 4;

/// The lists one thread's resolved batches work through, kept between reads
///
/// Both are as wide as the batch and neither leaves, so they stay with the thread:
/// only the answers a batch hands back are bought per read.
#[derive(Default)]
struct FoundAsks {
    /// What the device is asked for, one per key the index placed
    asks: Vec<Ask>,

    /// What it answered, in the order asked
    read: Vec<RecordRead>,
}

impl FoundAsks {
    const fn empty() -> FoundAsks {
        FoundAsks {
            asks: Vec::new(),
            read: Vec::new(),
        }
    }
}

thread_local! {
    /// One set of batch lists per reading thread, handed back after every read
    static FOUND_ASKS: std::cell::Cell<FoundAsks> =
        const { std::cell::Cell::new(FoundAsks::empty()) };
}

/// This thread's batch lists, given back however the read that borrowed them ends
struct HeldAsks(FoundAsks);

impl HeldAsks {
    fn take() -> HeldAsks {
        HeldAsks(FOUND_ASKS.with(std::cell::Cell::take))
    }
}

impl Drop for HeldAsks {
    fn drop(&mut self) {
        let mut held = std::mem::take(&mut self.0);
        held.asks.clear();
        // Cleared rather than dropped, so a value a failed batch left goes back to
        // the payload pool here rather than being held until this thread reads again.
        held.read.clear();
        FOUND_ASKS.with(|spare| spare.set(held));
    }
}

impl ReelStore {
    /// Read one payload, verified against its header key and checksum
    ///
    /// A pointer the index has since moved is re-resolved, and a record that fails
    /// its checksum has its key evicted so the miss becomes a repair enqueue. A
    /// read-only open rebuilds once before it calls a pointer unresolvable.
    pub fn get(&self, key: &RecordKey) -> Result<Option<Value>> {
        self.settle_sealed()?;
        match self.resolve_read(key)? {
            Resolved::Payload(payload) => return Ok(Some(payload)),
            Resolved::Missing => return Ok(None),
            Resolved::Unresolved => {}
        }

        self.rebuild()?;
        match self.resolve_read(key)? {
            Resolved::Payload(payload) => Ok(Some(payload)),
            Resolved::Missing => Ok(None),
            Resolved::Unresolved => Err(unresolved(key)),
        }
    }

    /// Read one payload as a future, for a caller with no thread to park
    pub async fn get_wait(&self, key: &RecordKey) -> Result<Option<Value>> {
        self.settle_sealed()?;
        match self.resolve_read_wait(key).await? {
            Resolved::Payload(payload) => return Ok(Some(payload)),
            Resolved::Missing => return Ok(None),
            Resolved::Unresolved => {}
        }

        self.rebuild()?;
        match self.resolve_read_wait(key).await? {
            Resolved::Payload(payload) => Ok(Some(payload)),
            Resolved::Missing => Ok(None),
            Resolved::Unresolved => Err(unresolved(key)),
        }
    }

    /// Read part of one payload, clamped to it, at the caller's logical offsets
    ///
    /// A raw record's window skips the whole-payload checksum but keeps identity.
    /// A coded record decodes whole, checksum verified, and the window is cut
    /// from the decoded payload.
    pub fn get_range(&self, key: &RecordKey, offset: u64, len: usize) -> Result<Option<Value>> {
        if self.is_coded(key.column) {
            return Ok(self.get(key)?.map(|payload| range_of(payload, offset, len)));
        }
        self.settle_sealed()?;
        match self.resolve_range(key, offset, len)? {
            Resolved::Payload(payload) => return Ok(Some(payload)),
            Resolved::Missing => return Ok(None),
            Resolved::Unresolved => {}
        }

        self.rebuild()?;
        match self.resolve_range(key, offset, len)? {
            Resolved::Payload(payload) => Ok(Some(payload)),
            Resolved::Missing => Ok(None),
            Resolved::Unresolved => Err(unresolved(key)),
        }
    }

    /// Read part of one payload as a future, for a caller with no thread to park
    pub async fn get_range_wait(
        &self,
        key: &RecordKey,
        offset: u64,
        len: usize,
    ) -> Result<Option<Value>> {
        if self.is_coded(key.column) {
            return Ok(self
                .get_wait(key)
                .await?
                .map(|payload| range_of(payload, offset, len)));
        }
        self.settle_sealed()?;
        match self.resolve_range_wait(key, offset, len).await? {
            Resolved::Payload(payload) => return Ok(Some(payload)),
            Resolved::Missing => return Ok(None),
            Resolved::Unresolved => {}
        }

        self.rebuild()?;
        match self.resolve_range_wait(key, offset, len).await? {
            Resolved::Payload(payload) => Ok(Some(payload)),
            Resolved::Missing => Ok(None),
            Resolved::Unresolved => Err(unresolved(key)),
        }
    }

    /// Read several keys with one submission, answered in the order asked
    ///
    /// The index is asked for every key first, so what reaches the device is one
    /// batch of reads. Only a clean answer is batched: a moved pointer, a retired
    /// segment or a bad checksum falls back to the single-key path, which is the one
    /// that retries, rebuilds and evicts.
    pub fn get_many(&self, keys: &[RecordKey]) -> Result<Vec<Option<Value>>> {
        self.settle_sealed()?;
        let found = self.locate_many(keys)?;
        // Borrowed for the read: the batch holds the caller's keys the whole way
        // through and the read path only ever reads them.
        let borrowed: Vec<KeyRef<'_>> = keys.iter().map(RecordKey::as_ref).collect();
        self.read_found(&borrowed, &found, &mut [])
    }

    /// Read several keys as one future, answered in the order asked
    ///
    /// One submission and one wait, with no thread held per read.
    pub async fn get_many_wait(&self, keys: &[RecordKey]) -> Result<Vec<Option<Value>>> {
        self.settle_sealed()?;
        let found = self.locate_many(keys)?;
        let borrowed: Vec<KeyRef<'_>> = keys.iter().map(RecordKey::as_ref).collect();
        self.read_found_wait(&borrowed, &found, &mut []).await
    }

    /// Ask the index where every key sits, all of them against one state of it
    ///
    /// The index takes its own barrier, so a batch publishing beside this cannot
    /// answer some keys from before it and the rest from after.
    fn locate_many(&self, keys: &[RecordKey]) -> Result<Vec<Option<Entry>>> {
        self.index.get_many(keys)
    }

    /// Read a batch whose entries the caller already resolved
    ///
    /// The index already answered where every key sits, so nothing is resolved
    /// twice. An entry of nothing is a key the caller found no record for.
    pub fn read_found(
        &self,
        keys: &[KeyRef<'_>],
        found: &[Option<Entry>],
        carried: &mut [Option<Arc<[u8]>>],
    ) -> Result<Vec<Option<Value>>> {
        let mut held = HeldAsks::take();
        let asked = &mut held.0;
        let mut plan = self.plan_found(keys, found, carried, &mut asked.asks);
        if asked.asks.is_empty() {
            return Ok(plan.answers);
        }

        self.reel
            .read_records(&asked.asks, keys, self.config.verify_reads, &mut asked.read)?;
        // Stepped by position rather than by a draining iterator, since the retry
        // below re-enters the engine and must not be holding these lists when it does.
        for slot in 0..asked.asks.len().min(asked.read.len()) {
            let ask = asked.asks[slot];
            let outcome = std::mem::replace(&mut asked.read[slot], RecordRead::Stale);
            let at = ask.at as usize;
            match self.keep_found(keys[at], ask.lsn, outcome) {
                Some(payload) => plan.answers[at] = Some(payload),
                // The record moved or went bad since the caller resolved it, which is
                // what compaction does under a playback.
                None => plan.answers[at] = self.get(&keys[at].to_owned_key()?)?,
            }
        }
        Ok(plan.answers)
    }

    /// Read a resolved batch as a future, answered in the order asked
    ///
    /// The retry a moved record needs stays on the async door.
    pub async fn read_found_wait(
        &self,
        keys: &[KeyRef<'_>],
        found: &[Option<Entry>],
        carried: &mut [Option<Arc<[u8]>>],
    ) -> Result<Vec<Option<Value>>> {
        let mut held = HeldAsks::take();
        let asked = &mut held.0;
        let mut plan = self.plan_found(keys, found, carried, &mut asked.asks);
        if asked.asks.is_empty() {
            return Ok(plan.answers);
        }

        self.reel
            .read_records_wait(&asked.asks, keys, self.config.verify_reads, &mut asked.read)
            .await?;
        for slot in 0..asked.asks.len().min(asked.read.len()) {
            let ask = asked.asks[slot];
            let outcome = std::mem::replace(&mut asked.read[slot], RecordRead::Stale);
            let at = ask.at as usize;
            match self.keep_found(keys[at], ask.lsn, outcome) {
                Some(payload) => plan.answers[at] = Some(payload),
                None => plan.answers[at] = self.get_wait(&keys[at].to_owned_key()?).await?,
            }
        }
        Ok(plan.answers)
    }

    /// Split a resolved batch into what the index answers and what the device must
    ///
    /// A value carried out of the index is answered without taking a place in the
    /// submission, so the batch is only the keys that need the device.
    fn plan_found(
        &self,
        keys: &[KeyRef<'_>],
        found: &[Option<Entry>],
        carried: &mut [Option<Arc<[u8]>>],
        asks: &mut Vec<Ask>,
    ) -> FoundPlan {
        let mut answers: Vec<Option<Value>> = (0..keys.len()).map(|_| None).collect();
        asks.clear();
        asks.reserve(keys.len());

        for (at, entry) in found.iter().enumerate().take(keys.len()) {
            if let Some(entry) = entry {
                if let Some(value) = carried.get_mut(at).and_then(Option::take) {
                    answers[at] = Some(Value::shared(value));
                    continue;
                }
                asks.push(Ask {
                    loc: entry.loc,
                    lsn: entry.lsn,
                    at: at as u32,
                });
            }
        }
        FoundPlan { answers }
    }

    /// What one batched read answered, or nothing when the key has to be re-resolved
    fn keep_found(&self, key: KeyRef<'_>, lsn: Lsn, outcome: RecordRead) -> Option<Value> {
        match outcome {
            RecordRead::Found(payload) => {
                // A scan must not displace the point-get working set, so an armed
                // carried tier admits nothing the bulk path read.
                if self.config.carried_budget.to_bytes() == 0 {
                    self.index.warm_carried(key, lsn, &payload, false);
                }
                Some(payload)
            }
            RecordRead::Stale | RecordRead::Gone | RecordRead::Corrupt => None,
        }
    }

    /// Advance the resident index to what the volume holds now
    ///
    /// How a read-only open picks up appends and sees compaction move records. The
    /// pass reads from where the last one stopped, so one append costs that append.
    pub fn refresh(&self) -> Result<CaughtUp> {
        if !self.is_read_only {
            return Err(ReelError::Rejected(
                "a writable reel already holds the current index".to_string(),
            ));
        }
        let caught_up = {
            let mut cursor = lock(&self.cursor);
            catch_up(&self.driver, &self.root, &self.index, &mut cursor)?
        };

        // Only a retired segment leaves a stale descriptor behind, so the rest of the
        // cache keeps its descriptors and the advice set on each one.
        for segment in &caught_up.retired {
            self.fd_cache.remove(*segment);
        }

        // A reader leaves the same graves and covers a writer does and never reaches
        // the maintenance tick that settles them, so it sweeps and prunes here.
        self.index.sweep_covers(SWEEP_RUN)?;
        let floor = caught_up.highest_lsn.as_u64().saturating_sub(GRAVE_WINDOW);
        if floor > 0 {
            self.index.prune_tombstones(Lsn(floor));
        }

        // A reader holding more covers than it will test per record can no longer
        // follow cheaply, and a rebuild needs no covers at all.
        if caught_up.is_saturated {
            self.rebuild()?;
        }
        Ok(caught_up)
    }

    /// Rebuild the whole index from the volume, discarding where the reader was
    ///
    /// For the case following the log cannot answer: a reader so far behind that
    /// segments it never read have been compacted away.
    pub fn rebuild(&self) -> Result<()> {
        if !self.is_read_only {
            return Err(ReelError::Rejected(
                "a writable reel already holds the current index".to_string(),
            ));
        }
        self.fd_cache.clear();
        let mut roots = vec![self.root.clone()];
        roots.extend(self.config.volumes.iter().map(|volume| volume.path.clone()));
        let dead: Vec<bool> = std::iter::once(false)
            .chain(self.config.volumes.iter().map(|volume| volume.dead))
            .collect();
        let rebuilt = rebuild_reel(&self.driver, &roots, &dead, self.config.index.pages())?;
        self.index.install(
            rebuilt.entries,
            rebuilt.covers,
            rebuilt.segments,
            rebuilt.segment_min_lsn,
            rebuilt.segment_max_lsn,
            rebuilt.sealed,
            rebuilt.sealed_keys,
        );
        let mut cursor = lock(&self.cursor);
        *cursor = LogCursor::new();
        cursor.start_from(&rebuilt.consumed);
        Ok(())
    }

    /// Recorded length of one record, served from the index with no read
    pub fn size_of(&self, key: &RecordKey) -> Result<Option<ByteCount>> {
        self.settle_sealed()?;
        self.index.size_of(key)
    }

    /// Whether a record exists, index only, no read
    pub fn contains(&self, key: &RecordKey) -> Result<bool> {
        self.settle_sealed()?;
        self.index.contains(key)
    }

    /// Cue up a view of the volume as it stands now
    ///
    /// The tails are sealed first, so every version at or below the number returned
    /// sits in a segment with a footer and can be found again.
    pub fn cue(&self) -> Result<CuePoint> {
        if self.is_read_only {
            return Err(read_only());
        }
        // A cue seals so everything it can see has a footer, so it settles what those
        // seals leave behind before anything reads through it.
        self.settle_sealed()?;
        for tail in self.reel.tails() {
            tail.seal()?;
        }
        // The seal alone leaves the segments unrecorded, and a read here has to know
        // which of them could hold a key.
        self.hold_sealed()?;
        // peek names the number the next write will take, so cueing at peek would
        // include a write that has not happened yet.
        let at = Lsn(self.reel.shared().lsn.peek().as_u64().saturating_sub(1));
        Ok(CuePoint::hold(at, Arc::clone(&self.cues)))
    }

    /// Read one key as the volume stood at a cue point
    pub fn get_at(&self, key: &RecordKey, cue: &CuePoint) -> Result<Option<Value>> {
        self.settle_sealed()?;
        self.read_as_of(key, cue.at())
    }

    /// Read one key as of a sequence number nothing is holding open
    ///
    /// Nothing holds the version open, so a miss here means gone rather than absent
    /// at that number.
    pub fn read_as_of(&self, key: &RecordKey, at: Lsn) -> Result<Option<Value>> {
        self.check_column(key)?;
        let Some(entry) = self.index.get_at(key, at)? else {
            return Ok(None);
        };
        match self.reel.read_record(entry.loc, key.as_ref(), entry.lsn, self.config.verify_reads)? {
            RecordRead::Found(payload) => Ok(Some(payload)),
            RecordRead::Corrupt if self.config.repair == RepairPath::None => {
                Err(ReelError::Corruption(format!(
                    "the record for a key in segment {} fails its checksum, and this volume is its only copy",
                    entry.loc.segment.as_u32()
                )))
            }
            // Never retried: a newer version is not this reader's to see.
            RecordRead::Stale | RecordRead::Gone | RecordRead::Corrupt => Ok(None),
        }
    }

    /// Fill a buffer with one bounded page of a column's keys, ascending
    ///
    /// A paged column reads footers to fill a page, so a page can fail where a
    /// resident one never could, and the failure is reported rather than swallowed.
    pub fn page(
        &self,
        column: ColumnId,
        start: Bound<&[u8]>,
        limit: usize,
        out: &mut KeyPage,
    ) -> Result<()> {
        self.index.page(column, start, limit, out)
    }

    /// Fill a buffer with one bounded page of a column's keys, descending
    pub fn page_back(
        &self,
        column: ColumnId,
        end: Bound<&[u8]>,
        limit: usize,
        out: &mut KeyPage,
    ) -> Result<()> {
        self.index.page_back(column, end, limit, out)
    }

    /// Fill a buffer with the next page a playback has reached, and carry it past it
    ///
    /// The cursor holds the playback's place and, on a paged column, the footers it
    /// is reading, so a playback opens those once rather than once per page. One
    /// page is filled against one state of the index, so no page holds part of a
    /// batch, but a walk of many pages is not a view of the volume at one moment.
    pub fn page_from(
        &self,
        playback: &mut PlaybackCursor,
        limit: usize,
        out: &mut KeyPage,
    ) -> Result<()> {
        self.index.page_from(playback, limit, out)
    }

    /// Whether a column's records are stored as a codec produced them
    ///
    /// A coded record is stored at a length of its own, so an offset the caller has
    /// in mind addresses nothing on the volume: the range is cut from the decoded
    /// payload instead of read off it. A column the volume does not serve is left
    /// to the read.
    fn is_coded(&self, column: ColumnId) -> bool {
        self.index
            .spec(column)
            .is_some_and(|spec| !matches!(spec.codec, Codec::None))
    }

    /// Resolve one key, re-resolving a moved pointer and evicting a rotted record
    ///
    /// A writable index moves under its own writes, so a pointer that will not
    /// resolve is retried and finally evicted. A read-only index reports the same
    /// pointer unresolved, since evicting would delete a key from a volume the
    /// reader does not own.
    fn resolve_read(&self, key: &RecordKey) -> Result<Resolved> {
        // Resolved once, so a column that carries nothing pays no lookup per retry.
        let mut resolving = Resolving::new(self, key);
        for _ in 0..RESOLVE_RETRIES {
            let entry = match resolving.step(self, key)? {
                Step::Done(resolved) => return Ok(resolved),
                Step::Read(entry, _) => entry,
            };
            let read = self.reel.read_record(
                entry.loc,
                key.as_ref(),
                entry.lsn,
                self.config.verify_reads,
            )?;
            if let Some(resolved) = resolving.fold(self, key, entry, read)? {
                return Ok(resolved);
            }
        }
        resolving.give_up(self, key)
    }

    /// Resolve one key as a future, always through the driver
    ///
    /// Nothing here maps, since an async caller has a runtime worker to protect and
    /// a fault cannot be woken.
    async fn resolve_read_wait(&self, key: &RecordKey) -> Result<Resolved> {
        let mut resolving = Resolving::new(self, key);
        for _ in 0..RESOLVE_RETRIES {
            let entry = match resolving.step(self, key)? {
                Step::Done(resolved) => return Ok(resolved),
                Step::Read(entry, _) => entry,
            };
            let read = self
                .reel
                .read_record_wait(entry.loc, key.as_ref(), entry.lsn, self.config.verify_reads)
                .await?;
            if let Some(resolved) = resolving.fold(self, key, entry, read)? {
                return Ok(resolved);
            }
        }
        resolving.give_up(self, key)
    }

    /// Resolve one key and read the range its entry places, retrying as a read does
    ///
    /// The carried tier is read from and never written to here, since a range holds
    /// a piece of a record and the tier holds records. An entry whose incarnation
    /// stamp is still current takes one device read and no header echo; everything
    /// else takes the header-checked read.
    fn resolve_range(&self, key: &RecordKey, offset: u64, len: usize) -> Result<Resolved> {
        let mut resolving = Resolving::new(self, key);
        for _ in 0..RESOLVE_RETRIES {
            let (entry, wanted) = match resolving.step_range(self, key, offset, len)? {
                Step::Done(resolved) => return Ok(resolved),
                Step::Read(entry, wanted) => (entry, wanted),
            };
            if self.window_certain(&entry) {
                if let Some(found) =
                    self.reel
                        .read_window(entry.loc, key.width(), offset, wanted)?
                {
                    return Ok(Resolved::Payload(found));
                }
            }
            let read = self
                .reel
                .read_range(entry.loc, key.as_ref(), entry.lsn, offset, wanted)?;
            if let Some(resolved) = resolving.fold_range(self, key, entry, read)? {
                return Ok(resolved);
            }
        }
        resolving.give_up(self, key)
    }

    /// Resolve one key and await the range its entry places, always through the driver
    async fn resolve_range_wait(
        &self,
        key: &RecordKey,
        offset: u64,
        len: usize,
    ) -> Result<Resolved> {
        let mut resolving = Resolving::new(self, key);
        for _ in 0..RESOLVE_RETRIES {
            let (entry, wanted) = match resolving.step_range(self, key, offset, len)? {
                Step::Done(resolved) => return Ok(resolved),
                Step::Read(entry, wanted) => (entry, wanted),
            };
            if self.window_certain(&entry) {
                let found = self
                    .reel
                    .read_window_wait(entry.loc, key.width(), offset, wanted)
                    .await?;
                if let Some(found) = found {
                    return Ok(Resolved::Payload(found));
                }
            }
            let read = self
                .reel
                .read_range_wait(entry.loc, key.as_ref(), entry.lsn, offset, wanted)
                .await?;
            if let Some(resolved) = resolving.fold_range(self, key, entry, read)? {
                return Ok(resolved);
            }
        }
        resolving.give_up(self, key)
    }

    /// Whether the index can vouch for this entry without the on-disk echo
    ///
    /// A live segment's record bytes never change, and anything that retires the
    /// segment or reuses its space drops the incarnation first, so an uncertain
    /// entry loses the fast path and never its correctness.
    pub(super) fn window_certain(&self, entry: &Entry) -> bool {
        let current = self.index.segments().incarnation_of(entry.loc.segment);
        !current.is_none() && current == entry.incarnation
    }

    /// What the index alone answers for one key, before the volume is asked
    fn resolve_index(&self, key: &RecordKey, carries: bool) -> Result<Ready> {
        // A column whose rows carry their values asks the search to bring the value
        // back with the row, since the block holding it was read either way.
        let row_carry = self.reel.shared().row_carry(key.column);
        let mut from_row = match row_carry {
            0 => Vec::new(),
            carry => crate::reel::payload::take(carry as usize),
        };
        let asking = (row_carry > 0).then_some(&mut from_row);
        let entry = match self.index.get_carried(key, asking)? {
            Some(entry) => entry,
            None => {
                crate::reel::payload::give(from_row);
                return Ok(Ready::Missing);
            }
        };
        // A value the row did not carry leaves the buffer cleared, and its length can
        // only match by being zero, which the record answers with the same bytes.
        if row_carry > 0 && from_row.len() == entry.loc.len as usize {
            return Ok(Ready::Payload(Value::pooled(
                from_row,
                crate::reel::payload::give,
            )));
        }
        crate::reel::payload::give(from_row);
        if carries {
            if let Some(payload) = self.index.carried_value(key, entry.lsn) {
                return Ok(Ready::Payload(Value::shared(payload)));
            }
        }
        Ok(Ready::Read(entry))
    }

    /// What one attempt's read settled, or nothing when the loop is to try again
    fn after_read(
        &self,
        key: &RecordKey,
        entry: Entry,
        read: RecordRead,
        carries: bool,
        framed_nothing: &mut Option<Loc>,
    ) -> Result<Option<Resolved>> {
        match read {
            RecordRead::Found(payload) => {
                // A carrying column remembers what the device answered, guarded on
                // the sequence number so a racing overwrite is never captured.
                if carries {
                    let two_touch = self.config.carried_budget.to_bytes() > 0;
                    self.index.warm_carried(key.as_ref(), entry.lsn, &payload, two_touch);
                }
                Ok(Some(Resolved::Payload(payload)))
            }
            // A sole copy has nobody to repair from, so evicting would hide the loss:
            // the key keeps its place and the read fails and says so, every time.
            RecordRead::Corrupt if self.config.repair == RepairPath::None => {
                Err(ReelError::Corruption(format!(
                    "the record for a key in segment {} fails its checksum, and this volume is its only copy",
                    entry.loc.segment.as_u32()
                )))
            }
            RecordRead::Corrupt => {
                self.evict(key, entry.loc, "failed its checksum")?;
                Ok(Some(Resolved::Missing))
            }
            RecordRead::Gone if self.is_read_only => Ok(Some(Resolved::Unresolved)),
            // The segment is not on the volume, which on one that compacts means a
            // retire ran under this read. Retried, never evicted.
            RecordRead::Gone => Ok(None),
            RecordRead::Stale => {
                *framed_nothing = Some(entry.loc);
                Ok(None)
            }
        }
    }

    /// What a key the retries never resolved leaves behind
    ///
    /// A location whose bytes do not frame this record is one no further retry will
    /// fix. A missing file is not that: it is a race a reader can lose for longer
    /// than the loop waits, so evicting on it would bury a live key.
    fn give_up(&self, key: &RecordKey, framed_nothing: Option<Loc>) -> Result<Resolved> {
        if self.is_read_only {
            return Ok(Resolved::Unresolved);
        }
        if let Some(loc) = framed_nothing {
            self.evict(key, loc, "never framed its record")?;
        }
        Ok(Resolved::Missing)
    }

    fn evict(&self, key: &RecordKey, loc: Loc, why: &str) -> Result<()> {
        if self.index.evict_at(key, loc)? {
            tracing::warn!(
                "evicted a record that {why} in segment {}",
                loc.segment.as_u32()
            );
        }
        Ok(())
    }
}

/// What one resolve attempt has decided, before any record is read
///
/// The deciding lives here so each door is left holding only its own wait.
enum Step {
    /// The attempt answered without asking the volume
    Done(Resolved),

    /// The record to read, and for a ranged read how much of it is wanted
    Read(Entry, usize),
}

/// What a resolve carries across its retries
///
/// Both doors hold one of these and hand it back the read they made, so the lookup,
/// the clamp, the stale-pointer fold and the giving up happen here once.
struct Resolving {
    /// Whether the key's column carries values in the index
    carries: bool,

    /// A location that framed nothing, kept so giving up can evict it
    framed_nothing: Option<Loc>,
}

impl Resolving {
    fn new(store: &ReelStore, key: &RecordKey) -> Resolving {
        Resolving {
            carries: store.index.carry_max(key.column) != 0,
            framed_nothing: None,
        }
    }

    /// What the index says about a whole-record read
    fn step(&self, store: &ReelStore, key: &RecordKey) -> Result<Step> {
        Ok(match store.resolve_index(key, self.carries)? {
            Ready::Payload(payload) => Step::Done(Resolved::Payload(payload)),
            Ready::Missing => Step::Done(Resolved::Missing),
            Ready::Read(entry) => Step::Read(entry, 0),
        })
    }

    /// The same for a window of one record, which also clamps what is wanted
    ///
    /// A window past the end of the payload wants nothing, and a caller asking for
    /// nothing is answered rather than sent to the volume for zero bytes.
    fn step_range(
        &self,
        store: &ReelStore,
        key: &RecordKey,
        offset: u64,
        len: usize,
    ) -> Result<Step> {
        let entry = match store.resolve_index(key, self.carries)? {
            Ready::Payload(payload) => {
                return Ok(Step::Done(Resolved::Payload(range_of(
                    payload, offset, len,
                ))))
            }
            Ready::Missing => return Ok(Step::Done(Resolved::Missing)),
            Ready::Read(entry) => entry,
        };
        let wanted = clamped(entry.loc.len, offset, len);
        match wanted {
            0 => Ok(Step::Done(Resolved::Payload(Value::default()))),
            wanted => Ok(Step::Read(entry, wanted)),
        }
    }

    /// Fold a whole-record read back in: an answer, or nothing and go round again
    fn fold(
        &mut self,
        store: &ReelStore,
        key: &RecordKey,
        entry: Entry,
        read: RecordRead,
    ) -> Result<Option<Resolved>> {
        store.after_read(key, entry, read, self.carries, &mut self.framed_nothing)
    }

    /// The same for a window, which is never answered from the carried tier
    ///
    /// A carried value is a whole payload, and one was already answered from before
    /// a window reached a read.
    fn fold_range(
        &mut self,
        store: &ReelStore,
        key: &RecordKey,
        entry: Entry,
        read: RecordRead,
    ) -> Result<Option<Resolved>> {
        store.after_read(key, entry, read, false, &mut self.framed_nothing)
    }

    /// Out of retries, which is where a pointer that never framed is evicted
    fn give_up(self, store: &ReelStore, key: &RecordKey) -> Result<Resolved> {
        store.give_up(key, self.framed_nothing)
    }
}

/// What the index alone can say about one key, before the volume is asked
enum Ready {
    /// The value, from the entry itself or from the carried tier
    Payload(Value),

    /// The entry whose record has to be read off the volume
    Read(Entry),

    /// The reel does not hold this key
    Missing,
}

/// What resolving one key against the reel produced
enum Resolved {
    /// The payload the pointer named
    Payload(Value),

    /// The reel does not hold this key
    Missing,

    /// The index names a record the files cannot answer, so it has to be rebuilt
    Unresolved,
}

/// A pointer a rebuilt read-only index still cannot answer
fn unresolved(key: &RecordKey) -> ReelError {
    ReelError::Corruption(format!(
        "column {} points a record into a segment that is not on the volume",
        key.column.as_u8()
    ))
}

/// Bytes of a payload a range actually covers, once it is clamped to the record
///
/// A range past the end of the record is the bytes that are there, and one starting
/// at or past the end is none of them.
fn clamped(payload_len: u32, offset: u64, len: usize) -> usize {
    let left = u64::from(payload_len).saturating_sub(offset);
    left.min(len as u64) as usize
}
