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

use super::{read_only, ReelStore, GRAVE_WINDOW, SWEEP_RUN};
use crate::index::entry::Entry;
use crate::index::recovery::rebuild_reel;
use crate::index::tailer::{catch_up, CaughtUp, LogCursor};
use crate::reel::{Ask, RecordRead, Spot};
use crate::sync::lock;
use reel_core::{range_of, ReadBlock, Value};

/// How many times a read re-resolves a stale index pointer before giving up
const RESOLVE_RETRIES: u32 = 4;

/// One batch read in place: the blocks it filled, a spot per key, and the records
/// it had to own
///
/// A walk lends from it until its next batch, and a caller keeping records takes
/// them out as values.
#[derive(Default)]
pub(crate) struct Placed {
    /// What the device is asked for, one per key the index placed
    asks: Vec<Ask>,

    /// One block per merged read
    blocks: Vec<ReadBlock>,

    /// Where each key's record sits, by the caller's position
    spots: Vec<Spot>,

    /// Records a lookup or a decode answered, which no block holds
    owned: Vec<Value>,
}

/// A spot's block when the record is held owned, its index into `owned`
const OWNED: u32 = u32::MAX - 1;

impl Placed {
    /// The record at a key's position, lent until the next batch
    pub(crate) fn lend(&self, at: usize) -> Option<&[u8]> {
        let spot = self.spots.get(at)?;
        match spot.block {
            OWNED => Some(&self.owned[spot.at as usize]),
            block => {
                let start = spot.at as usize;
                let block = self.blocks.get(block as usize)?;
                block.get(start..start + spot.len as usize)
            }
        }
    }

    /// The record at a key's position, as a value of its own
    pub(crate) fn take(&mut self, at: usize) -> Option<Value> {
        let spot = *self.spots.get(at)?;
        match spot.block {
            OWNED => Some(std::mem::take(&mut self.owned[spot.at as usize])),
            block => self
                .blocks
                .get(block as usize)?
                .window(spot.at as usize, spot.len as usize),
        }
    }

    /// Whether the batch holds a record for the key at this position
    pub(crate) fn holds(&self, at: usize) -> bool {
        self.spots.get(at).is_some_and(|spot| *spot != Spot::MISS)
    }

    /// Hold a record the blocks cannot answer, at a key's position
    pub(crate) fn hold(&mut self, at: usize, value: Value) {
        self.spots[at] = Spot {
            block: OWNED,
            at: self.owned.len() as u32,
            len: 0,
            codec: 0,
        };
        self.owned.push(value);
    }

    /// Start over on a batch of this many keys, with every one missing
    fn reset(&mut self, keys: usize) {
        self.asks.clear();
        self.blocks.clear();
        self.owned.clear();
        self.spots.clear();
        self.spots.resize(keys, Spot::MISS);
    }

    /// Keys the index placed that the read did not find, which have to be resolved again
    pub(crate) fn missed(&self) -> impl Iterator<Item = usize> + '_ {
        self.asks
            .iter()
            .map(|ask| ask.at as usize)
            .filter(|&at| self.spots[at] == Spot::MISS)
    }

    /// Decode every record the read left as stored bytes
    fn decode_coded(&mut self) {
        for at in 0..self.spots.len() {
            let spot = self.spots[at];
            if spot.codec == 0 || spot.block == OWNED {
                continue;
            }
            let stored = &self.blocks[spot.block as usize][spot.at as usize..][..spot.len as usize];
            match crate::append::codec::decode(spot.codec, stored) {
                Some(decoded) => self.hold(at, Value::pooled(decoded, crate::reel::payload::give)),
                // A decode that fails is corruption wearing a valid checksum, left for
                // the single-key path to answer.
                None => self.spots[at] = Spot::MISS,
            }
        }
    }
}

thread_local! {
    /// One placed batch per reading thread, handed back after every read
    static PLACED: std::cell::Cell<Placed> = std::cell::Cell::new(Placed::default());
}

/// This thread's placed batch, given back however the read that borrowed it ends
struct HeldPlaced(Placed);

impl HeldPlaced {
    fn take() -> HeldPlaced {
        HeldPlaced(PLACED.with(std::cell::Cell::take))
    }
}

impl Drop for HeldPlaced {
    fn drop(&mut self) {
        let mut held = std::mem::take(&mut self.0);
        // Cleared rather than dropped, so a block a failed batch left goes back to
        // the payload pool here rather than being held until this thread reads again.
        held.reset(0);
        PLACED.with(|spare| spare.set(held));
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
    /// The record's own header decides which way it is served. A raw record's
    /// window skips the whole-payload checksum but keeps identity. A coded record
    /// decodes whole, checksum verified, and the window is cut from the payload it
    /// decodes to.
    pub fn get_range(&self, key: &RecordKey, offset: u64, len: usize) -> Result<Option<Value>> {
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
        self.read_found(&borrowed, &found)
    }

    /// Read several keys as one future, answered in the order asked
    ///
    /// One submission and one wait, with no thread held per read.
    pub async fn get_many_wait(&self, keys: &[RecordKey]) -> Result<Vec<Option<Value>>> {
        self.settle_sealed()?;
        let found = self.locate_many(keys)?;
        let borrowed: Vec<KeyRef<'_>> = keys.iter().map(RecordKey::as_ref).collect();
        self.read_found_wait(&borrowed, &found).await
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
    ) -> Result<Vec<Option<Value>>> {
        let mut held = HeldPlaced::take();
        let placed = &mut held.0;
        self.read_placed(keys, found, placed)?;
        // The record moved or went bad since the caller resolved it, which is what
        // compaction does under a playback.
        let missed: Vec<usize> = placed.missed().collect();
        for at in missed {
            if let Some(value) = self.get(&keys[at].to_owned_key()?)? {
                placed.hold(at, value);
            }
        }
        Ok((0..keys.len()).map(|at| placed.take(at)).collect())
    }

    /// Read a resolved batch as a future, answered in the order asked
    ///
    /// The retry a moved record needs stays on the async door.
    pub async fn read_found_wait(
        &self,
        keys: &[KeyRef<'_>],
        found: &[Option<Entry>],
    ) -> Result<Vec<Option<Value>>> {
        let mut held = HeldPlaced::take();
        let placed = &mut held.0;
        self.plan_placed(keys.len(), found, placed);
        if !placed.asks.is_empty() {
            self.reel
                .read_placed_wait(
                    &placed.asks,
                    keys,
                    self.config.verify_reads,
                    &mut placed.blocks,
                    &mut placed.spots,
                )
                .await?;
            placed.decode_coded();
        }
        let missed: Vec<usize> = placed.missed().collect();
        for at in missed {
            if let Some(value) = self.get_wait(&keys[at].to_owned_key()?).await? {
                placed.hold(at, value);
            }
        }
        Ok((0..keys.len()).map(|at| placed.take(at)).collect())
    }

    /// Read a resolved batch into place, leaving every record it could not find a miss
    pub(crate) fn read_placed(
        &self,
        keys: &[KeyRef<'_>],
        found: &[Option<Entry>],
        placed: &mut Placed,
    ) -> Result<()> {
        self.plan_placed(keys.len(), found, placed);
        if placed.asks.is_empty() {
            return Ok(());
        }
        self.reel.read_placed(
            &placed.asks,
            keys,
            self.config.verify_reads,
            &mut placed.blocks,
            &mut placed.spots,
        )?;
        placed.decode_coded();
        Ok(())
    }

    /// An ask for every key the index placed, with every key missing until read
    fn plan_placed(&self, keys: usize, found: &[Option<Entry>], placed: &mut Placed) {
        placed.reset(keys);
        for (at, entry) in found.iter().enumerate().take(keys) {
            if let Some(entry) = entry {
                placed.asks.push(Ask {
                    loc: entry.loc,
                    lsn: entry.lsn,
                    at: at as u32,
                });
            }
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
        let rebuilt = rebuild_reel(
            &self.driver,
            &roots,
            &dead,
            self.config.index.pages(),
            &self.index,
        )?;
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
            RecordRead::Stale | RecordRead::Gone | RecordRead::Corrupt | RecordRead::Coded => {
                Ok(None)
            }
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

    /// Whether a column's records can hold what a codec produced
    ///
    /// The declaration says what may have happened and the record header says what
    /// did, so this only decides whether the header has to be read. A column the
    /// volume does not serve is left to the read.
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
        let mut resolving = Resolving::new(self, key);
        for _ in 0..RESOLVE_RETRIES {
            let entry = match resolving.step(self, key)? {
                Step::Done(resolved) => return Ok(resolved),
                Step::Read(entry) => entry,
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
                Step::Read(entry) => entry,
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
    /// An entry whose incarnation stamp is still current takes one device read and no
    /// header echo. Everything else takes the header-checked read, and a header saying a
    /// codec produced the record sends the window to the whole read.
    fn resolve_range(&self, key: &RecordKey, offset: u64, len: usize) -> Result<Resolved> {
        let mut resolving = Resolving::new(self, key);
        for _ in 0..RESOLVE_RETRIES {
            let (entry, wanted) = match resolving.step_range(self, key, offset, len)? {
                RangeStep::Done(resolved) => return Ok(resolved),
                RangeStep::Whole => return self.whole_range(key, offset, len),
                RangeStep::Read(entry, wanted) => (entry, wanted),
            };
            if resolving.window_bare(self, &entry) {
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
            if matches!(read, RecordRead::Coded) {
                return self.whole_range(key, offset, len);
            }
            if let Some(resolved) = resolving.fold(self, key, entry, read)? {
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
                RangeStep::Done(resolved) => return Ok(resolved),
                RangeStep::Whole => return self.whole_range_wait(key, offset, len).await,
                RangeStep::Read(entry, wanted) => (entry, wanted),
            };
            if resolving.window_bare(self, &entry) {
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
            if matches!(read, RecordRead::Coded) {
                return self.whole_range_wait(key, offset, len).await;
            }
            if let Some(resolved) = resolving.fold(self, key, entry, read)? {
                return Ok(resolved);
            }
        }
        resolving.give_up(self, key)
    }

    /// Read the record whole and cut the window out of what it decodes to
    ///
    /// Where a record whose header says a codec produced its bytes is served: the
    /// offsets the caller asked at address the decoded payload, which no read off
    /// the volume reaches.
    fn whole_range(&self, key: &RecordKey, offset: u64, len: usize) -> Result<Resolved> {
        Ok(match self.get(key)? {
            Some(payload) => Resolved::Payload(range_of(payload, offset, len)),
            None => Resolved::Missing,
        })
    }

    /// The same as a future, for a caller with no thread to park
    async fn whole_range_wait(&self, key: &RecordKey, offset: u64, len: usize) -> Result<Resolved> {
        Ok(match self.get_wait(key).await? {
            Some(payload) => Resolved::Payload(range_of(payload, offset, len)),
            None => Resolved::Missing,
        })
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

    /// What one attempt's read settled, or nothing when the loop is to try again
    fn after_read(
        &self,
        key: &RecordKey,
        entry: Entry,
        read: RecordRead,
        framed_nothing: &mut Option<Loc>,
    ) -> Result<Option<Resolved>> {
        match read {
            RecordRead::Found(payload) => Ok(Some(Resolved::Payload(payload))),
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
            // A whole read decodes what a codec produced, and a window is answered
            // by the range door before it folds, so nothing arrives here coded.
            RecordRead::Coded => Ok(None),
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

    /// The record to read off the volume
    Read(Entry),
}

/// The same for a window, which the record itself can send to the whole read
enum RangeStep {
    /// The attempt answered without asking the volume
    Done(Resolved),

    /// The record to read, and how much of it is wanted
    Read(Entry, usize),

    /// The record has to be read whole, whatever window the caller asked for
    Whole,
}

/// What a resolve carries across its retries
///
/// Both doors hold one of these and hand it back the read they made, so the lookup,
/// the clamp, the stale-pointer fold and the giving up happen here once.
struct Resolving {
    /// Whether the column's records can hold what a codec produced
    coded: bool,

    /// A location that framed nothing, kept so giving up can evict it
    framed_nothing: Option<Loc>,
}

impl Resolving {
    fn new(store: &ReelStore, key: &RecordKey) -> Resolving {
        Resolving {
            coded: store.is_coded(key.column),
            framed_nothing: None,
        }
    }

    /// Whether this record's window can be read off the volume with no header
    ///
    /// A column that declares a codec holds raw and coded records side by side and
    /// only the header tells them apart, so a window of one is never read bare.
    fn window_bare(&self, store: &ReelStore, entry: &Entry) -> bool {
        !self.coded && store.window_certain(entry)
    }

    /// What the index says about a whole-record read
    fn step(&self, store: &ReelStore, key: &RecordKey) -> Result<Step> {
        Ok(match store.index.get(key)? {
            Some(entry) => Step::Read(entry),
            None => Step::Done(Resolved::Missing),
        })
    }

    /// The same for a window of one record, which also clamps what is wanted
    ///
    /// A window past the end of the payload wants nothing, and a caller asking for
    /// nothing is answered without a trip to the volume. The
    /// clamp is against the stored length, which is the payload's only on a record
    /// no codec produced.
    fn step_range(
        &self,
        store: &ReelStore,
        key: &RecordKey,
        offset: u64,
        len: usize,
    ) -> Result<RangeStep> {
        let Some(entry) = store.index.get(key)? else {
            return Ok(RangeStep::Done(Resolved::Missing));
        };
        let wanted = clamped(entry.loc.len, offset, len);
        match wanted {
            // A coded record is stored at a length of its own, so nothing left to
            // clamp against it says nothing about where the payload ends.
            0 if self.coded => Ok(RangeStep::Whole),
            0 => Ok(RangeStep::Done(Resolved::Payload(Value::default()))),
            wanted => Ok(RangeStep::Read(entry, wanted)),
        }
    }

    /// Fold a read back in: an answer, or nothing and go round again
    fn fold(
        &mut self,
        store: &ReelStore,
        key: &RecordKey,
        entry: Entry,
        read: RecordRead,
    ) -> Result<Option<Resolved>> {
        store.after_read(key, entry, read, &mut self.framed_nothing)
    }

    /// Out of retries, which is where a pointer that never framed is evicted
    fn give_up(self, store: &ReelStore, key: &RecordKey) -> Result<Resolved> {
        store.give_up(key, self.framed_nothing)
    }
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
