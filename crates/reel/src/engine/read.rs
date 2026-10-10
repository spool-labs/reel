//! Point reads, ranges and batches, and the resolve loop they share

use std::future::Future;
use std::ops::Bound;
use std::sync::Arc;

use crate::units::ByteCount;

use crate::config::RepairPath;
use crate::error::{ReelError, Result};
use crate::format::column::{Codec, ColumnId, KeyRef, RecordKey};
use crate::format::loc::Loc;
use crate::format::lsn::Lsn;
use crate::index::map::{Located, SpotPick, SpotRoute};
use crate::index::page::KeyPage;
use crate::index::paged::FooterSource;
use crate::index::playback::PlaybackCursor;
use crate::index::spot::{Candidate, Lookup, Offered, RecordSource, Since, SpotRead, LOOKUP_TRIES};
use crate::reel::cue::CuePoint;
use crate::reel::{SpotAsk, SpotRange};

use super::{read_only, ReelStore, GRAVE_WINDOW, SWEEP_RUN};
use crate::index::counters::Stamps;
use crate::index::entry::Entry;
use crate::index::recovery::rebuild_reel;
use crate::index::tailer::{catch_up, CaughtUp, LogCursor};
use crate::reel::{Ask, RecordRead, Spot};
use crate::sync::lock;
use reel_core::{range_of, ReadBlock, Value};

/// How many times a read re-resolves a stale index pointer before giving up
const RESOLVE_RETRIES: u32 = 4;

/// One batch read in place, with its blocks, a spot per key and any owned records
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

/// A spot's block value for an owned record, whose `at` indexes `owned`
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

    /// Empty the batch, keeping its vectors for the next one
    pub(crate) fn clear(&mut self) {
        self.reset(0);
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
                // A failed decode is corruption behind a good checksum, for the single-key path
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
        // Reset on drop so blocks a failed batch left go back to the payload pool now
        held.reset(0);
        PLACED.with(|spare| spare.set(held));
    }
}

impl ReelStore {
    /// Run a read against one whole index, again if a rebuilt one swapped in under it
    fn steady<Answer>(&self, mut read: impl FnMut() -> Result<Answer>) -> Result<Answer> {
        // Only a read-only open ever installs, so a writable one reads straight through
        if !self.is_read_only {
            return read();
        }
        loop {
            let seen = self.index.settled_installs();
            let answer = read();
            if self.index.installs_held(seen) {
                return answer;
            }
        }
    }

    /// The same check around a read as a future
    async fn steady_wait<Answer, Read: Future<Output = Result<Answer>>>(
        &self,
        read: impl Fn() -> Read,
    ) -> Result<Answer> {
        if !self.is_read_only {
            return read().await;
        }
        loop {
            let seen = self.index.settled_installs();
            let answer = read().await;
            if self.index.installs_held(seen) {
                return answer;
            }
        }
    }

    /// Read one payload, verified against its header key and checksum
    pub fn get(&self, key: &RecordKey) -> Result<Option<Value>> {
        self.settle_sealed()?;
        let (behind, first) =
            self.steady(|| Ok((self.index.spot_behind(), self.resolve_read(key)?)))?;
        match first {
            _ if self.fell_behind(behind) => {}
            Resolved::Payload(payload) => return Ok(Some(payload)),
            Resolved::Missing => return Ok(None),
            Resolved::Unresolved => {}
        }

        // The resync may rebuild, so it runs outside the check that would see its own install
        self.resync(behind)?;
        match self.steady(|| self.resolve_read(key))? {
            Resolved::Payload(payload) => Ok(Some(payload)),
            Resolved::Missing => Ok(None),
            Resolved::Unresolved => Err(unresolved(key)),
        }
    }

    /// Read one payload as a future, for a caller with no thread to park
    pub async fn get_wait(&self, key: &RecordKey) -> Result<Option<Value>> {
        self.settle_sealed()?;
        let (behind, first) = self
            .steady_wait(|| async {
                let behind = self.index.spot_behind();
                Ok((behind, self.resolve_read_wait(key).await?))
            })
            .await?;
        match first {
            _ if self.fell_behind(behind) => {}
            Resolved::Payload(payload) => return Ok(Some(payload)),
            Resolved::Missing => return Ok(None),
            Resolved::Unresolved => {}
        }

        self.resync(behind)?;
        match self.steady_wait(|| self.resolve_read_wait(key)).await? {
            Resolved::Payload(payload) => Ok(Some(payload)),
            Resolved::Missing => Ok(None),
            Resolved::Unresolved => Err(unresolved(key)),
        }
    }

    /// Read part of one payload, clamped to it, at the caller's logical offsets
    pub fn get_range(&self, key: &RecordKey, offset: u64, len: usize) -> Result<Option<Value>> {
        self.settle_sealed()?;
        let (behind, first) = self.steady(|| {
            let behind = self.index.spot_behind();
            Ok((behind, self.resolve_range(key, offset, len)?))
        })?;
        match first {
            _ if self.fell_behind(behind) => {}
            Some(Resolved::Payload(payload)) => return Ok(Some(payload)),
            Some(Resolved::Missing) => return Ok(None),
            None => return self.whole_range(key, offset, len),
            Some(Resolved::Unresolved) => {}
        }

        self.resync(behind)?;
        match self.steady(|| self.resolve_range(key, offset, len))? {
            Some(Resolved::Payload(payload)) => Ok(Some(payload)),
            Some(Resolved::Missing) => Ok(None),
            None => self.whole_range(key, offset, len),
            Some(Resolved::Unresolved) => Err(unresolved(key)),
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
        let (behind, first) = self
            .steady_wait(|| async {
                let behind = self.index.spot_behind();
                Ok((behind, self.resolve_range_wait(key, offset, len).await?))
            })
            .await?;
        match first {
            _ if self.fell_behind(behind) => {}
            Some(Resolved::Payload(payload)) => return Ok(Some(payload)),
            Some(Resolved::Missing) => return Ok(None),
            None => return self.whole_range_wait(key, offset, len).await,
            Some(Resolved::Unresolved) => {}
        }

        self.resync(behind)?;
        let again = self
            .steady_wait(|| self.resolve_range_wait(key, offset, len))
            .await?;
        match again {
            Some(Resolved::Payload(payload)) => Ok(Some(payload)),
            Some(Resolved::Missing) => Ok(None),
            None => self.whole_range_wait(key, offset, len).await,
            Some(Resolved::Unresolved) => Err(unresolved(key)),
        }
    }

    /// Read several keys with one submission, answered in the order asked
    pub fn get_many(&self, keys: &[RecordKey]) -> Result<Vec<Option<Value>>> {
        self.settle_sealed()?;
        let (mut values, unsettled) = self.steady(|| self.get_many_once(keys))?;
        // A key the batch could not settle reads alone, since that read may rebuild
        for at in unsettled {
            values[at] = self.get(&keys[at])?;
        }
        Ok(values)
    }

    /// One try at a batch read, with the keys it left for a lone read
    fn get_many_once(&self, keys: &[RecordKey]) -> Result<(Vec<Option<Value>>, Vec<usize>)> {
        let located = self.locate_many(keys)?;
        let borrowed: Vec<KeyRef<'_>> = keys.iter().map(RecordKey::as_ref).collect();
        let (mut values, mut unsettled) = self.read_found_once(&borrowed, &located.found)?;
        if located.picks.is_empty() {
            return Ok((values, unsettled));
        }
        let mut picks = located.picks;
        let asks = self.first_asks(keys, &mut picks);
        let reads = self.reel.shared().spot_records(&asks.asks)?;
        let offered = self.offer_firsts(&mut picks, asks.taken, reads);
        for (spot, offered) in picks.into_iter().zip(offered) {
            let key = &keys[spot.at];
            let column = self.index.spot_column(spot.column);
            let lookup = match offered {
                Offered::Next => column.read_on(key, spot.pick)?,
                Offered::Again | Offered::Unsettled => Lookup::Unsettled,
            };
            match self.index.spot_finish(spot.column, key, spot.since, lookup) {
                Lookup::Found(_, value) | Lookup::Newest(value) => values[spot.at] = Some(value),
                Lookup::Missing => values[spot.at] = None,
                Lookup::Unsettled => unsettled.push(spot.at),
            }
        }
        Ok((values, unsettled))
    }

    /// Each pick's first candidate, as one batch of asks, and which picks have one
    fn first_asks<'a>(&self, keys: &'a [RecordKey], picks: &mut [SpotPick]) -> FirstAsks<'a> {
        let mut asks = Vec::with_capacity(picks.len());
        let mut taken = Vec::with_capacity(picks.len());
        for spot in picks.iter_mut() {
            let candidate = self.index.spot_column(spot.column).next(&mut spot.pick);
            if let Some(candidate) = candidate {
                asks.push(SpotAsk {
                    key: &keys[spot.at],
                    segment: candidate.segment,
                    offset: candidate.offset,
                    bound: candidate.bound,
                    alone: candidate.alone,
                });
            }
            taken.push(candidate);
        }
        FirstAsks { asks, taken }
    }

    /// Fold each first candidate's read into its pick
    fn offer_firsts(
        &self,
        picks: &mut [SpotPick],
        taken: Vec<Option<Candidate>>,
        reads: Vec<SpotRead>,
    ) -> Vec<Offered> {
        let mut reads = reads.into_iter();
        let mut offered = Vec::with_capacity(picks.len());
        for (spot, candidate) in picks.iter_mut().zip(taken) {
            let column = self.index.spot_column(spot.column);
            offered.push(match (candidate, candidate.and_then(|_| reads.next())) {
                (Some(candidate), Some(read)) => column.offer(&mut spot.pick, candidate, read),
                (Some(_), None) | (None, Some(_)) | (None, None) => Offered::Next,
            });
        }
        offered
    }

    /// Read several keys as one future, answered in the order asked
    pub async fn get_many_wait(&self, keys: &[RecordKey]) -> Result<Vec<Option<Value>>> {
        self.settle_sealed()?;
        let (mut values, unsettled) = self.steady_wait(|| self.get_many_wait_once(keys)).await?;
        for at in unsettled {
            values[at] = self.get_wait(&keys[at]).await?;
        }
        Ok(values)
    }

    /// One try at an awaited batch read, with the keys it left for a lone read
    async fn get_many_wait_once(
        &self,
        keys: &[RecordKey],
    ) -> Result<(Vec<Option<Value>>, Vec<usize>)> {
        let located = self.locate_many(keys)?;
        let borrowed: Vec<KeyRef<'_>> = keys.iter().map(RecordKey::as_ref).collect();
        let (mut values, mut unsettled) =
            self.read_found_wait_once(&borrowed, &located.found).await?;
        if located.picks.is_empty() {
            return Ok((values, unsettled));
        }
        let mut picks = located.picks;
        let asks = self.first_asks(keys, &mut picks);
        let reads = self.reel.shared().spot_records_wait(&asks.asks).await?;
        let offered = self.offer_firsts(&mut picks, asks.taken, reads);
        let shared = self.reel.shared();
        for (mut spot, offered) in picks.into_iter().zip(offered) {
            let key = &keys[spot.at];
            let column = self.index.spot_column(spot.column);
            let mut flow = offered;
            while flow == Offered::Next {
                let Some(candidate) = column.next(&mut spot.pick) else {
                    break;
                };
                let read = shared
                    .spot_record_wait(
                        key,
                        candidate.segment,
                        candidate.offset,
                        candidate.bound,
                        candidate.alone,
                    )
                    .await?;
                flow = column.offer(&mut spot.pick, candidate, read);
            }
            let lookup = match flow {
                Offered::Next => column.settle(key, spot.pick),
                Offered::Again | Offered::Unsettled => Lookup::Unsettled,
            };
            match self.index.spot_finish(spot.column, key, spot.since, lookup) {
                Lookup::Found(_, value) | Lookup::Newest(value) => values[spot.at] = Some(value),
                Lookup::Missing => values[spot.at] = None,
                Lookup::Unsettled => unsettled.push(spot.at),
            }
        }
        Ok((values, unsettled))
    }

    /// Ask the index where every key sits, all against one state of the index
    fn locate_many(&self, keys: &[RecordKey]) -> Result<Located> {
        self.index.get_many(keys)
    }

    /// Read a batch whose entries the caller already resolved
    pub fn read_found(
        &self,
        keys: &[KeyRef<'_>],
        found: &[Option<Entry>],
    ) -> Result<Vec<Option<Value>>> {
        let (mut values, missed) = self.steady(|| self.read_found_once(keys, found))?;
        // The record moved or went bad since the caller resolved it, as compaction does
        for at in missed {
            values[at] = self.get(&keys[at].to_owned_key()?)?;
        }
        Ok(values)
    }

    /// One try at a resolved batch, with the keys it missed for a lone read
    fn read_found_once(
        &self,
        keys: &[KeyRef<'_>],
        found: &[Option<Entry>],
    ) -> Result<(Vec<Option<Value>>, Vec<usize>)> {
        let mut held = HeldPlaced::take();
        let placed = &mut held.0;
        self.read_placed(keys, found, placed)?;
        let missed: Vec<usize> = placed.missed().collect();
        Ok(((0..keys.len()).map(|at| placed.take(at)).collect(), missed))
    }

    /// Read a resolved batch as a future, answered in the order asked
    pub async fn read_found_wait(
        &self,
        keys: &[KeyRef<'_>],
        found: &[Option<Entry>],
    ) -> Result<Vec<Option<Value>>> {
        let (mut values, missed) = self
            .steady_wait(|| self.read_found_wait_once(keys, found))
            .await?;
        for at in missed {
            values[at] = self.get_wait(&keys[at].to_owned_key()?).await?;
        }
        Ok(values)
    }

    /// One try at an awaited resolved batch, with the keys it missed for a lone read
    async fn read_found_wait_once(
        &self,
        keys: &[KeyRef<'_>],
        found: &[Option<Entry>],
    ) -> Result<(Vec<Option<Value>>, Vec<usize>)> {
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
        Ok(((0..keys.len()).map(|at| placed.take(at)).collect(), missed))
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
        // Take the segment stamps once, since a lookup per record contends on the table's lock
        let stamps = self.index.segments().stamps();
        for (at, entry) in found.iter().enumerate().take(keys) {
            if let Some(entry) = entry {
                placed.asks.push(Ask {
                    loc: entry.loc,
                    lsn: entry.lsn,
                    at: at as u32,
                    certain: is_certain(&stamps, entry),
                });
            }
        }
    }

    /// Advance the index to what the volume holds now
    pub fn refresh(&self) -> Result<CaughtUp> {
        if !self.is_read_only {
            return Err(ReelError::Rejected(
                "a writable reel already holds the current index".to_string(),
            ));
        }
        // The cursor's lock keeps a rebuild out until this pass and its sweep are done
        let mut cursor = lock(&self.cursor);
        let caught_up = catch_up(&self.driver, &self.root, &self.index, &mut cursor)?;

        // Only retired segments leave stale descriptors, so the rest of the cache stays
        for segment in &caught_up.retired {
            self.fd_cache.remove(*segment);
        }

        // A reader never runs the tick, so it sweeps covers and prunes old graves here
        self.index.sweep_covers(SWEEP_RUN)?;
        let floor = caught_up.highest_lsn.as_u64().saturating_sub(GRAVE_WINDOW);
        if floor > 0 {
            self.index.prune_tombstones(Lsn(floor));
        }

        // A reader with too many covers or lost segments rebuilds, which needs no covers
        if caught_up.is_saturated || caught_up.lost > 0 {
            self.rebuild_held(&mut cursor)?;
        }
        Ok(caught_up)
    }

    /// Rebuild the whole index from the volume, discarding where the reader was
    pub fn rebuild(&self) -> Result<()> {
        if !self.is_read_only {
            return Err(ReelError::Rejected(
                "a writable reel already holds the current index".to_string(),
            ));
        }
        let seen = self.index.settled_installs();
        // The cursor's lock keeps catch-ups and other rebuilds out until the new index is in
        let mut cursor = lock(&self.cursor);
        // A rebuild another reader finished while this one waited serves both
        if self.index.settled_installs() != seen {
            return Ok(());
        }
        self.rebuild_held(&mut cursor)
    }

    /// Build a new index beside the live one and swap it in, leaving both alone on a failure
    fn rebuild_held(&self, cursor: &mut LogCursor) -> Result<()> {
        self.fd_cache.clear();
        let mut roots = vec![self.root.clone()];
        roots.extend(self.config.volumes.iter().map(|volume| volume.path.clone()));
        let dead: Vec<bool> = std::iter::once(false)
            .chain(self.config.volumes.iter().map(|volume| volume.dead))
            .collect();
        // Set up the way an open sets up its index, so reads keep the old one until the swap
        let fresh = self.index.new_beside()?;
        let rebuilt = rebuild_reel(&self.driver, &roots, &dead, &fresh)?;
        let shared = self.reel.shared();
        fresh.set_footers(Arc::clone(shared) as Arc<dyn FooterSource>);
        fresh.set_records(Arc::clone(shared) as Arc<dyn RecordSource>);
        fresh.follow();
        fresh.finish_open()?;
        self.index.install(fresh);
        *cursor = LogCursor::new();
        cursor.start_from(&rebuilt.consumed);
        Ok(())
    }

    /// The recorded length of one record, from the index with no read
    pub fn size_of(&self, key: &RecordKey) -> Result<Option<ByteCount>> {
        self.settle_sealed()?;
        self.followed(|| self.index.size_of(key))
    }

    /// Whether a record exists, index only, no read
    pub fn contains(&self, key: &RecordKey) -> Result<bool> {
        self.settle_sealed()?;
        self.followed(|| self.index.contains(key))
    }

    /// Whether a read-only open met a segment its writer retired since `behind` was read
    fn fell_behind(&self, behind: u64) -> bool {
        self.is_read_only && self.index.spot_behind() != behind
    }

    /// Bring a read-only open up to date, catching up if it fell behind, else rebuilding
    fn resync(&self, behind: u64) -> Result<()> {
        if self.fell_behind(behind) {
            self.refresh()?;
            return Ok(());
        }
        self.rebuild()
    }

    /// Answer from the index, asking again after a catch-up if a writer retired a segment
    fn followed<Answer>(&self, answer: impl Fn() -> Result<Answer>) -> Result<Answer> {
        let (behind, found) = self.steady(|| Ok((self.index.spot_behind(), answer()?)))?;
        if !self.fell_behind(behind) {
            return Ok(found);
        }
        // The catch-up may rebuild, so it runs outside the check that would see its own install
        self.refresh()?;
        self.steady(&answer)
    }

    /// Cue up a view of the volume as it stands now, sealing the tails first
    pub fn cue(&self) -> Result<CuePoint> {
        if self.is_read_only {
            return Err(read_only());
        }
        for tail in self.reel.tails() {
            tail.seal()?;
        }
        // peek is the next write's number, so the cue sits one below it
        let at = Lsn(self.reel.shared().lsn.peek().as_u64().saturating_sub(1));
        Ok(CuePoint::hold(at, Arc::clone(&self.cues)))
    }

    /// Read one key as the volume stood at a cue point
    pub fn get_at(&self, key: &RecordKey, cue: &CuePoint) -> Result<Option<Value>> {
        self.read_as_of(key, cue.at())
    }

    /// Read one key as of a sequence number nothing is holding open
    pub fn read_as_of(&self, key: &RecordKey, at: Lsn) -> Result<Option<Value>> {
        self.steady(|| self.read_as_of_once(key, at))
    }

    /// One try at a versioned read
    fn read_as_of_once(&self, key: &RecordKey, at: Lsn) -> Result<Option<Value>> {
        self.check_column(key)?;
        // The segments a cue sealed are searchable only once their spans are noted
        self.settle_sealed()?;
        // The spot index holds a key's newest sealed version, one read away when the cue can see it
        if let Some((column, since)) = self.index.spot_route_at(key) {
            let lookup = self.index.spot_column(column).read_versioned(key)?;
            match self.index.spot_finish_at(column, key, since, at, lookup) {
                Lookup::Found(_, value) => return Ok(Some(value)),
                Lookup::Missing => return Ok(None),
                Lookup::Newest(_) | Lookup::Unsettled => {}
            }
        }
        let Some(entry) = self.index.get_at(key, at)? else {
            return Ok(None);
        };
        let certain = self.window_certain(&entry);
        match self.reel.read_record(entry.loc, key.as_ref(), entry.lsn, self.config.verify_reads, certain)? {
            RecordRead::Found(payload) => Ok(Some(payload)),
            RecordRead::Corrupt if self.config.repair == RepairPath::None => {
                Err(ReelError::Corruption(format!(
                    "the record for a key in segment {} fails its checksum, and this volume is its only copy",
                    entry.loc.segment.as_u32()
                )))
            }
            // Never retried, since a newer version is outside this reader's view
            RecordRead::Stale | RecordRead::Gone | RecordRead::Corrupt | RecordRead::Coded => {
                Ok(None)
            }
        }
    }

    /// Fill a buffer with one bounded page of a column's keys, ascending
    pub fn page(
        &self,
        column: ColumnId,
        start: Bound<&[u8]>,
        limit: usize,
        out: &mut KeyPage,
    ) -> Result<()> {
        self.steady(|| self.index.page(column, start, limit, out))
    }

    /// Fill a buffer with one bounded page of a column's keys, descending
    pub fn page_back(
        &self,
        column: ColumnId,
        end: Bound<&[u8]>,
        limit: usize,
        out: &mut KeyPage,
    ) -> Result<()> {
        self.steady(|| self.index.page_back(column, end, limit, out))
    }

    /// Fill a buffer with the playback's next page and move the playback past it
    pub fn page_from(
        &self,
        playback: &mut PlaybackCursor,
        limit: usize,
        out: &mut KeyPage,
    ) -> Result<()> {
        if !self.is_read_only {
            return self.index.page_from(playback, limit, out);
        }
        // A page thrown away has moved the playback, so the next try starts from the mark
        let mark = playback.mark();
        let mut again = false;
        self.steady(|| {
            if again {
                playback.rewind(&mark);
            }
            again = true;
            self.index.page_from(playback, limit, out)
        })
    }

    /// Whether a column's records can hold what a codec produced
    fn is_coded(&self, column: ColumnId) -> bool {
        self.index
            .spec(column)
            .is_some_and(|spec| !matches!(spec.codec, Codec::None))
    }

    /// Resolve one key, re-resolving a moved pointer and evicting a rotted record
    fn resolve_read(&self, key: &RecordKey) -> Result<Resolved> {
        match self.index.spot_read(key)? {
            Lookup::Found(_, payload) | Lookup::Newest(payload) => {
                return Ok(Resolved::Payload(payload))
            }
            Lookup::Missing => return Ok(Resolved::Missing),
            Lookup::Unsettled => {}
        }
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
                self.window_certain(&entry),
            )?;
            if let Some(resolved) = resolving.fold(self, key, entry, read)? {
                return Ok(resolved);
            }
        }
        resolving.give_up(self, key)
    }

    /// Resolve one key as a future, always through the driver
    async fn resolve_read_wait(&self, key: &RecordKey) -> Result<Resolved> {
        match self.spot_read_wait(key).await? {
            Lookup::Found(_, payload) | Lookup::Newest(payload) => {
                return Ok(Resolved::Payload(payload))
            }
            Lookup::Missing => return Ok(Resolved::Missing),
            Lookup::Unsettled => {}
        }
        let mut resolving = Resolving::new(self, key);
        for _ in 0..RESOLVE_RETRIES {
            let entry = match resolving.step(self, key)? {
                Step::Done(resolved) => return Ok(resolved),
                Step::Read(entry) => entry,
            };
            let read = self
                .reel
                .read_record_wait(
                    entry.loc,
                    key.as_ref(),
                    entry.lsn,
                    self.config.verify_reads,
                    self.window_certain(&entry),
                )
                .await?;
            if let Some(resolved) = resolving.fold(self, key, entry, read)? {
                return Ok(resolved);
            }
        }
        resolving.give_up(self, key)
    }

    /// A key's newest payload as a future, reading each needed spot index candidate once
    async fn spot_read_wait(&self, key: &RecordKey) -> Result<Lookup> {
        let (at, since) = match self.index.spot_route(key) {
            SpotRoute::Settled(lookup) => return Ok(lookup),
            SpotRoute::Column(at, since) => (at, since),
        };
        let column = self.index.spot_column(at);
        let shared = self.reel.shared();
        'tries: for _ in 0..LOOKUP_TRIES {
            let Some(mut pick) = column.pick(key) else {
                return Ok(Lookup::Unsettled);
            };
            while let Some(candidate) = column.next(&mut pick) {
                let read = shared
                    .spot_record_wait(
                        key,
                        candidate.segment,
                        candidate.offset,
                        candidate.bound,
                        candidate.alone,
                    )
                    .await?;
                match column.offer(&mut pick, candidate, read) {
                    Offered::Next => {}
                    Offered::Again => continue 'tries,
                    Offered::Unsettled => return Ok(Lookup::Unsettled),
                }
            }
            return Ok(self
                .index
                .spot_finish(at, key, since, column.settle(key, pick)));
        }
        Ok(Lookup::Unsettled)
    }

    /// Look up the key's sole spot index candidate so a range read can go straight to it
    fn spot_range_candidate(&self, key: &RecordKey) -> Option<(usize, Since, Candidate)> {
        let SpotRoute::Column(at, since) = self.index.spot_route(key) else {
            return None;
        };
        Some((at, since, self.index.spot_column(at).sole(key)?))
    }

    /// Turn a spot index range read into an answer, or nothing for the checked path to settle
    fn spot_range_answer(
        &self,
        at: usize,
        key: &RecordKey,
        since: Since,
        read: SpotRange,
    ) -> Option<Resolved> {
        let lookup = match read {
            SpotRange::Found(head, window) => Lookup::Found(head.lsn, window),
            SpotRange::Newest(window) => Lookup::Newest(window),
            SpotRange::Tombstone(_) => Lookup::Missing,
            SpotRange::Other | SpotRange::Gone | SpotRange::Unsure => return None,
        };
        match self.index.spot_finish(at, key, since, lookup) {
            Lookup::Found(_, window) | Lookup::Newest(window) => Some(Resolved::Payload(window)),
            Lookup::Missing => Some(Resolved::Missing),
            Lookup::Unsettled => None,
        }
    }

    /// Resolve one key and read the range its entry places, or nothing when it has to read whole
    fn resolve_range(&self, key: &RecordKey, offset: u64, len: usize) -> Result<Option<Resolved>> {
        if let Some((at, since, candidate)) = self.spot_range_candidate(key) {
            let read = self.reel.shared().spot_range(
                key,
                candidate.segment,
                candidate.offset,
                candidate.bound,
                candidate.alone,
                offset,
                len,
            )?;
            if let Some(resolved) = self.spot_range_answer(at, key, since, read) {
                return Ok(Some(resolved));
            }
        }
        let mut resolving = Resolving::new(self, key);
        for _ in 0..RESOLVE_RETRIES {
            let (entry, wanted) = match resolving.step_range(self, key, offset, len)? {
                RangeStep::Done(resolved) => return Ok(Some(resolved)),
                RangeStep::Whole => return Ok(None),
                RangeStep::Read(entry, wanted) => (entry, wanted),
            };
            if resolving.window_bare(self, &entry) {
                if let Some(found) =
                    self.reel
                        .read_window(entry.loc, key.width(), offset, wanted)?
                {
                    return Ok(Some(Resolved::Payload(found)));
                }
            }
            let read = self.reel.read_range(
                entry.loc,
                key.as_ref(),
                entry.lsn,
                offset,
                wanted,
                self.window_certain(&entry),
            )?;
            if matches!(read, RecordRead::Coded) {
                return Ok(None);
            }
            if let Some(resolved) = resolving.fold(self, key, entry, read)? {
                return Ok(Some(resolved));
            }
        }
        resolving.give_up(self, key).map(Some)
    }

    /// Resolve one key and await the range its entry places, or nothing when it has to read whole
    async fn resolve_range_wait(
        &self,
        key: &RecordKey,
        offset: u64,
        len: usize,
    ) -> Result<Option<Resolved>> {
        if let Some((at, since, candidate)) = self.spot_range_candidate(key) {
            let read = self
                .reel
                .shared()
                .spot_range_wait(
                    key,
                    candidate.segment,
                    candidate.offset,
                    candidate.bound,
                    candidate.alone,
                    offset,
                    len,
                )
                .await?;
            if let Some(resolved) = self.spot_range_answer(at, key, since, read) {
                return Ok(Some(resolved));
            }
        }
        let mut resolving = Resolving::new(self, key);
        for _ in 0..RESOLVE_RETRIES {
            let (entry, wanted) = match resolving.step_range(self, key, offset, len)? {
                RangeStep::Done(resolved) => return Ok(Some(resolved)),
                RangeStep::Whole => return Ok(None),
                RangeStep::Read(entry, wanted) => (entry, wanted),
            };
            if resolving.window_bare(self, &entry) {
                let found = self
                    .reel
                    .read_window_wait(entry.loc, key.width(), offset, wanted)
                    .await?;
                if let Some(found) = found {
                    return Ok(Some(Resolved::Payload(found)));
                }
            }
            let read = self
                .reel
                .read_range_wait(
                    entry.loc,
                    key.as_ref(),
                    entry.lsn,
                    offset,
                    wanted,
                    self.window_certain(&entry),
                )
                .await?;
            if matches!(read, RecordRead::Coded) {
                return Ok(None);
            }
            if let Some(resolved) = resolving.fold(self, key, entry, read)? {
                return Ok(Some(resolved));
            }
        }
        resolving.give_up(self, key).map(Some)
    }

    /// Read the record whole and cut the window out of what it decodes to
    fn whole_range(&self, key: &RecordKey, offset: u64, len: usize) -> Result<Option<Value>> {
        Ok(self.get(key)?.map(|payload| range_of(payload, offset, len)))
    }

    /// The same as a future, for a caller with no thread to park
    async fn whole_range_wait(
        &self,
        key: &RecordKey,
        offset: u64,
        len: usize,
    ) -> Result<Option<Value>> {
        Ok(self
            .get_wait(key)
            .await?
            .map(|payload| range_of(payload, offset, len)))
    }

    /// Whether the index can vouch for this entry without the on-disk echo
    pub(super) fn window_certain(&self, entry: &Entry) -> bool {
        is_certain(&self.index.segments().stamps(), entry)
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
            // A sole copy can't be repaired, so the key stays and every read fails loudly
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
            // A retire ran under this read, so retry without evicting
            RecordRead::Gone => Ok(None),
            RecordRead::Stale => {
                *framed_nothing = Some(entry.loc);
                Ok(None)
            }
            // Whole reads decode and the range door takes coded windows, so nothing here is coded
            RecordRead::Coded => Ok(None),
        }
    }

    /// After the retries, evict a location that never framed the record and answer missing
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

/// Whether an entry's segment still wears the incarnation the index stamped it with
fn is_certain(stamps: &Stamps<'_>, entry: &Entry) -> bool {
    let current = stamps.of(entry.loc.segment);
    !current.is_none() && current == entry.incarnation
}

/// What one resolve attempt has decided, before any record is read
enum Step {
    /// The attempt answered without asking the volume
    Done(Resolved),

    /// The record to read off the volume
    Read(Entry),
}

/// What one range resolve attempt has decided, before any record is read
enum RangeStep {
    /// The attempt answered without asking the volume
    Done(Resolved),

    /// The record to read, and how much of it is wanted
    Read(Entry, usize),

    /// The record has to be read whole, whatever window the caller asked for
    Whole,
}

/// The state one resolve keeps across its retries
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
            // A coded record's stored length isn't its payload length, so read it whole
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

/// The first candidate of each pick in a batch, and which picks had one
struct FirstAsks<'a> {
    asks: Vec<SpotAsk<'a>>,
    taken: Vec<Option<Candidate>>,
}

/// What resolving one key against the reel produced
enum Resolved {
    /// The payload the pointer led to
    Payload(Value),

    /// The reel does not hold this key
    Missing,

    /// The index points at a record the files can't answer, so it has to be rebuilt
    Unresolved,
}

/// A pointer a rebuilt read-only index still cannot answer
fn unresolved(key: &RecordKey) -> ReelError {
    ReelError::Corruption(format!(
        "column {} points a record into a segment that is not on the volume",
        key.column.as_u8()
    ))
}

/// How many payload bytes a range covers once it is clamped to the record
fn clamped(payload_len: u32, offset: u64, len: usize) -> usize {
    let left = u64::from(payload_len).saturating_sub(offset);
    left.min(len as u64) as usize
}
