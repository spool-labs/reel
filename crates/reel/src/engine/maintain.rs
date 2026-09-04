//! The maintenance tick: seal handover, footprint, compaction, the merge and the scrub

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use std::sync::atomic::Ordering;

use crate::compaction::compactor::EraseReport;
use crate::compaction::merge::{merge_once, sorted_run_dead_ratio, MergeReport};
use crate::config::IndexResidency;
use crate::error::{ReelError, Result};
use crate::format::column::ColumnId;
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::Lsn;
use crate::index::counters::ProbeCounts;
use crate::reel::checkpoint::{
    checkpoint_name, link_into, sealed_below, staging_of, write_copy_manifest, write_copy_marker,
    Checkpoint,
};

use super::{
    read_only, CompactPass, Held, ReelStore, Totals, GRAVE_WINDOW, INGEST_HOT_BYTES, SWEEP_RUN,
};
use crate::sync::lock;

impl ReelStore {
    /// Tell the index about every segment that has sealed since it was last told
    ///
    /// A tail seals without holding the index, so until the number is picked up a
    /// paged read searches a set of footers missing the one holding its key.
    pub(super) fn settle_sealed(&self) -> Result<()> {
        if self.reel.shared().has_sealed_waiting() {
            self.hold_sealed()?;
        }
        Ok(())
    }

    /// What the sealed segments were asked about keys, and what asking them cost
    ///
    /// Counts rather than times, so they read the same on any machine. All zero on a
    /// resident volume, which never searches a footer.
    pub fn filter_probes(&self) -> ProbeCounts {
        self.reel.shared().probes.counts()
    }

    /// Live record count and byte total store-wide, from counters, no I/O
    pub fn totals(&self) -> Totals {
        self.index.totals()
    }

    /// Take a durable copy of the volume as it stands at a cue
    ///
    /// The copy is a directory of sealed segments that opens as a volume of its own,
    /// standing at the returned sequence number. The cue's floor stops compaction
    /// retiring what it still needs, and the cost is a hard link apiece and two
    /// syncs. Refused where the target already exists, and on a read-only volume.
    ///
    /// Hard links cannot cross devices, so each living volume stages under its own
    /// root and the home rename is the atomic point: anything a crash leaves short
    /// of it is debris to sweep, and a volume declared dead contributes nothing.
    pub fn checkpoint(&self, target: &Path) -> Result<Checkpoint> {
        if self.is_read_only {
            return Err(read_only());
        }
        let shared = self.reel.shared();
        let name = checkpoint_name(target)?;
        let mut pieces: Vec<(usize, PathBuf)> = vec![(0, target.to_path_buf())];
        for at in 1..shared.volumes.len() {
            if !shared.volumes.is_dead(at) {
                pieces.push((at, shared.volumes.roots()[at].join(name)));
            }
        }
        for at in 1..pieces.len() {
            if pieces[..at]
                .iter()
                .any(|(_, before)| before == &pieces[at].1)
            {
                return Err(ReelError::Rejected(format!(
                    "{} is where two pieces of this checkpoint would land, so the \
                     target is standing inside one of the volume roots",
                    pieces[at].1.display(),
                )));
            }
        }
        for (_, piece) in &pieces {
            if piece.exists() {
                return Err(ReelError::Rejected(format!(
                    "{} already exists, and a checkpoint will not publish over one",
                    piece.display(),
                )));
            }
            let staging = staging_of(piece);
            if staging.exists() {
                return Err(ReelError::Rejected(format!(
                    "{} is left over from a checkpoint that did not finish, so this one \
                     would build on top of it",
                    staging.display(),
                )));
            }
        }

        // The cue is held across everything below: it seals the tails, so the set is
        // immutable before it is read, and it pins the floor, so compaction cannot
        // retire a segment the copy needs while the links are taken.
        let cue = self.cue()?;
        // Read after the seal, so it is the line between what the cue can see and
        // what the volume does next.
        let boundary = shared.peek_segment();

        // Whatever this attempt made short of the home rename is debris. Once that
        // rename lands the copy exists and nothing may be swept again.
        let mut sweep: Vec<PathBuf> = Vec::new();
        match self.stage_and_publish(&pieces, boundary, cue.at(), &mut sweep) {
            Ok(segments) => {
                let at = cue.at();
                drop(cue);
                Ok(Checkpoint { at, segments })
            }
            Err(error) => {
                for debris in sweep {
                    let _ = std::fs::remove_dir_all(&debris);
                }
                Err(error)
            }
        }
    }

    /// Write the copy an index of its own, or leave it to open by sweeping
    ///
    /// Best effort: the copy is a whole volume without it and opens the slow way, so
    /// a refusal here is a slower restore rather than a failed checkpoint. Taken
    /// under the same cue and boundary as the links beside it, so its rows name the
    /// segments the copy is about to be given.
    fn index_for_copy(&self, staging: &Path, at: Lsn, boundary: SegmentId) {
        if !self.keeps_index() {
            return;
        }
        if let Err(error) = self.write_index(staging, at, boundary) {
            tracing::warn!("the copy takes no index of its own and will open by sweeping: {error}");
        }
    }

    /// Stage every piece of a checkpoint, then publish with home last
    fn stage_and_publish(
        &self,
        pieces: &[(usize, PathBuf)],
        boundary: SegmentId,
        at: Lsn,
        sweep: &mut Vec<PathBuf>,
    ) -> Result<usize> {
        let shared = self.reel.shared();
        let published: Vec<PathBuf> = pieces.iter().map(|(_, piece)| piece.clone()).collect();
        let mut segments = 0usize;
        for (volume, piece) in pieces {
            let root = &shared.volumes.roots()[*volume];
            let mut sealed = sealed_below(root, boundary)?;
            // The boundary alone is not enough: a tail draws its next segment when it
            // seals, so a link to that one shares an inode that then grows. `is_held`
            // covers the other case too, a segment holding an unpublished record.
            sealed.retain(|segment| !shared.is_held(*segment));
            let staging = staging_of(piece);
            std::fs::create_dir_all(&staging)?;
            sweep.push(staging.clone());
            link_into(root, &staging, &sealed)?;
            // A copy over one volume needs no manifest; one over several proves
            // itself the way the live store does.
            if pieces.len() > 1 {
                match *volume == 0 {
                    true => write_copy_manifest(&staging, &published)?,
                    false => write_copy_marker(&staging, piece)?,
                }
            }
            // The index goes beside the manifest, on home, since its rows name
            // segments across every piece and one file describes the whole copy.
            if *volume == 0 {
                self.index_for_copy(&staging, at, boundary);
            }
            // The links are directory entries and nothing else, so this is the only
            // durability point a checkpoint adds per piece.
            self.driver.sync_dir(&staging)?;
            segments += sealed.len();
        }
        // Every link is down and durable and no piece exists yet. A crash here
        // has to leave staging directories to sweep rather than a partial copy.
        crate::sync::rendezvous::at("checkpoint/staged");
        for (volume, piece) in pieces.iter().skip(1) {
            std::fs::rename(staging_of(piece), piece)?;
            sweep.push(piece.clone());
            // Durable before home publishes, so the atomic point below never
            // stands ahead of a piece its manifest names.
            self.driver.sync_dir(&shared.volumes.roots()[*volume])?;
        }
        std::fs::rename(staging_of(&pieces[0].1), &pieces[0].1)?;
        // The copy is whole and every staging name is gone, so a crash here has to
        // leave a copy that opens.
        sweep.clear();
        crate::sync::rendezvous::at("checkpoint/published");
        if let Some(parent) = pieces[0].1.parent() {
            self.driver.sync_dir(parent)?;
        }
        Ok(segments)
    }

    /// Live record count and byte total for one column, from counters
    ///
    /// Nothing comes back from a column resolving keys through sealed footers,
    /// whose counters cover the resident half alone.
    pub fn column_totals(&self, column: ColumnId) -> Option<Totals> {
        self.index.column_totals(column)
    }

    /// Whether a column's counters say what a walk of it would find
    ///
    /// They do not while a paged column holds sealed keys no shard counted, nor
    /// while a range cover is owed its sweep and the counters still carry what it
    /// deleted.
    pub fn counters_agree(&self, column: ColumnId) -> bool {
        !self.index.answers_from_footers(column)
            && !self
                .index
                .column(column)
                .is_some_and(|index| index.has_pending_covers())
    }

    /// Live count and byte total under a key prefix, when the counters answer it
    ///
    /// Nothing comes back where the shards cannot answer the prefix on their own,
    /// which leaves the caller to walk the keys instead.
    pub fn prefix_totals(&self, column: ColumnId, prefix: &[u8]) -> Option<Totals> {
        self.index.prefix_totals(column, prefix)
    }

    /// Hand the keys of newly sealed segments over to their footers
    ///
    /// The footer is read once, each column's row range is recorded so a later
    /// lookup knows whether to search this segment at all, and every key it still
    /// answers for leaves the map. Paging is guarded by location, so a key
    /// overwritten since the seal keeps its resident entry.
    pub fn page_out_sealed(&self) -> Result<usize> {
        self.hold_sealed()?;
        if !self.config.index.pages() {
            return Ok(0);
        }

        let mut paged = 0usize;
        while let Some(segment) = self.next_to_hand_over() {
            paged += self.hand_over(segment)?;
        }
        Ok(paged)
    }

    /// Record the key range each column occupies in one sealed segment
    ///
    /// The half of the handover worth doing on every volume: it names the segments a
    /// search must consider and rules out the rest, without handing a key over.
    /// False where there is no footer to read at all, which no retry changes.
    fn note_spans(&self, segment: SegmentId) -> Result<bool> {
        let Some(footer) = self.reel.shared().footer_of(segment)? else {
            return Ok(false);
        };
        // The footer is in hand and the spans are not down yet. A retire that lands
        // here takes the file and clears the registry this is about to write into.
        crate::sync::rendezvous::at("seal/spans");
        self.index.note_spans(segment, &footer)?;
        Ok(true)
    }

    /// The next segment whose keys are neither recent enough nor cheap enough to keep
    ///
    /// A paged volume keeps nothing, so this is the queue in order. A hot one hands
    /// over what has waited out its age, then keeps going while the maps are over
    /// budget.
    fn next_to_hand_over(&self) -> Option<SegmentId> {
        let now = Instant::now();
        let over_budget = match self.config.index {
            IndexResidency::Hot(hot) => self.index.resident_bytes() > hot.budget,
            _ => false,
        };
        let mut held = lock(&self.held);
        let front = held.front()?;
        (over_budget || front.ready_at <= now)
            .then(|| held.pop_front().expect("a front that answered").segment)
    }

    /// Give one sealed segment's keys to its footer
    ///
    /// The spans are not recorded here: recording them off this queue would put a
    /// segment compaction has since retired back into the search. Paging a key out
    /// cannot, since it moves only an entry that still points into this segment.
    fn hand_over(&self, segment: SegmentId) -> Result<usize> {
        // The keys are given up one at a time, with reads served throughout.
        crate::sync::rendezvous::at("paged/handover");
        let Some(footer) = self.reel.shared().footer_of(segment)? else {
            return Ok(0);
        };
        let mut paged = 0usize;
        for partition in &footer.partitions {
            // The key is borrowed out of the packed bytes: decoding whole entries
            // would build a `KeyBytes` per row, including the rows this skips.
            for at in 0..partition.len() {
                let row = partition.row_at(at)?;
                if !row.flags.is_data() {
                    continue;
                }
                let Some(key) = partition.key_at(at) else {
                    continue;
                };
                let loc = Loc::new(segment, row.offset, row.len);
                if self.index.page_out(partition.column, key, loc) {
                    paged += 1;
                }
            }
        }
        Ok(paged)
    }

    /// Take the segments sealed since the last tick, name them, and start their wait
    ///
    /// The wait runs from the seal rather than from this tick, so two segments
    /// sealed either side of a tick are the same age at the next one. Spans go down
    /// first and for every residency, and a segment stays on the sealed queue until
    /// it has been named, so nothing retires a segment the index cannot search yet.
    pub(super) fn hold_sealed(&self) -> Result<()> {
        let sealed = self.reel.shared().peek_sealed();
        if sealed.is_empty() {
            return Ok(());
        }
        let mut named = Vec::with_capacity(sealed.len());
        // Taken by the peek above and not named here, so the claim has to come off
        // or nothing will ever take them again.
        let mut kept = Vec::new();
        for (segment, sealed_at) in sealed {
            match self.note_spans(segment) {
                Ok(_) => named.push((segment, sealed_at)),
                // A device that would not answer this time may answer next time.
                Err(ReelError::Io(error)) => {
                    kept.push(segment);
                    tracing::warn!(
                        "could not read reel segment {}'s footer to name what it holds, trying again: {error}",
                        segment.as_u32()
                    );
                }
                // Anything else is about the bytes rather than about the device,
                // so the segment is given up to compaction.
                Err(error) => {
                    tracing::warn!(
                        "reel segment {} sealed with a footer that names no key range: {error}",
                        segment.as_u32()
                    );
                    named.push((segment, sealed_at));
                }
            }
        }
        let settled: Vec<SegmentId> = named.iter().map(|(segment, _)| *segment).collect();
        // The mark comes off here rather than beside the paged queue below, or a
        // resident volume would hold every merge's note for the life of the volume.
        let mut merged = Vec::new();
        for segment in &settled {
            if self.reel.shared().forget_merge_output(*segment) {
                merged.push(*segment);
            }
        }
        self.reel.shared().settle_sealed(&settled);
        self.reel.shared().release_sealed(&kept);

        if !self.config.index.pages() {
            return Ok(());
        }
        let wait = match self.config.index {
            IndexResidency::Hot(hot) => hot.after(),
            _ => Duration::ZERO,
        };
        let mut held = lock(&self.held);
        for (segment, sealed_at) in named {
            // Merge output is never recent: a row written recently lives in a newer
            // run and shadows the merged copy. Left in seal order a base merge would
            // seal last and take the residency budget from the segments that earned it.
            let is_promotable = !merged.contains(&segment);
            match is_promotable {
                true => held.push_back(Held {
                    segment,
                    ready_at: sealed_at + wait,
                }),
                false => held.push_front(Held {
                    segment,
                    ready_at: sealed_at,
                }),
            }
        }
        Ok(())
    }

    /// Move the floor everything below which the volume is finished with
    ///
    /// A column that marks its keys has its records dropped by compaction once their
    /// mark falls below this, and its writes banded against it where it asked for that.
    /// A floor only ever moves up.
    pub fn purge_below(&self, floor: u64) {
        self.reel.shared().purge_below(floor);
    }

    /// The floor compaction drops records below, and bands are measured from
    pub fn purge_floor(&self) -> u64 {
        self.reel.shared().purge_floor()
    }

    /// Erase the dead runs out of sealed segments, and report what came back
    ///
    /// Reclamation through the filesystem's own extent map, without moving any
    /// survivor. Linux gives the blocks back, elsewhere the report prices the layout
    /// without acting. Nothing schedules this; a caller that wants it runs it.
    pub fn erase_dead_runs(&self) -> Result<EraseReport> {
        self.compactor.erase_dead_runs(&self.reel, &self.index)
    }

    /// Collapse the volume's sorted runs into one, and report what the pass did
    ///
    /// The pass a caller drives, which runs whatever the stack's debt is. The floor an
    /// open cue point holds is read here, so a run holding a version an older reader can
    /// still see is left alone.
    pub fn merge_once(&self) -> Result<MergeReport> {
        if self.is_read_only {
            return Err(read_only());
        }
        // A pass asks the index which of a run's rows are still live, so it works
        // from a settled view: a segment the index has not been told about answers
        // that with nothing at all.
        self.settle_sealed()?;
        merge_once(&self.compactor, &self.reel, &self.index, self.cues.floor())
    }

    /// The dead share of the standing sorted runs, which is what a tick decides on
    ///
    /// Nothing where fewer runs stand than a merge could collapse.
    pub fn sorted_run_dead_ratio(&self) -> Result<Option<f64>> {
        sorted_run_dead_ratio(&self.compactor, &self.reel, &self.index, self.cues.floor())
    }

    /// Collapse the sorted runs where the stack has gone as dead as the volume allows
    ///
    /// What the tick drives, and nothing on a volume that did not arm the merge. The
    /// trigger is the stack's own dead share rather than a cadence, so the volume merges
    /// as often as its traffic shadows rows and never on a quiet one. Nothing comes back
    /// where the stack is under the threshold or a seat or the rate held the pass off.
    pub fn merge_when_due(&self) -> Result<Option<MergeReport>> {
        if self.is_read_only || !self.config.merge_sorted_runs {
            return Ok(None);
        }
        // The seat is what keeps a merge and a rewrite off one another's segments, and
        // it is taken before the rate is read for the reason a rewrite takes it first.
        let Some(_pass) = self.compaction_plane.enter() else {
            return Ok(None);
        };
        if !self.compactor.is_compaction_due() {
            return Ok(None);
        }
        // A run the index has not been told about answers every liveness question with
        // nothing, so the stack is priced from a settled view and merged from one.
        self.settle_sealed()?;
        let debt = self.sorted_run_dead_ratio()?;
        if !debt.is_some_and(|ratio| ratio >= self.config.merge_dead_ratio) {
            return Ok(None);
        }
        merge_once(&self.compactor, &self.reel, &self.index, self.cues.floor()).map(Some)
    }

    /// Run one bounded pass of the whole maintenance plane
    ///
    /// Every pass is paced by the rates the volume was configured with, so the caller
    /// drives this on a timer and never has to bound it. Bounded in bytes, not in
    /// wall clock: a volume that named a compaction cap takes as long as the bytes it
    /// moves owe at that cap, and an unpaced one returns at device speed.
    pub fn maintain_once(&self) -> Result<()> {
        if self.is_read_only {
            return Ok(());
        }
        self.retry_broken_seals();
        self.publish_footprint();
        self.page_out_sealed()?;
        self.sweep_covers()?;
        self.prune_tombstones();
        self.shed_carried();
        self.compact_once()?;
        // After the rewrite, so a run the pass above unlinked whole is never bytes a
        // merge reads. A pass that refuses or fails is one tier of one tick, and the
        // scrub below is still owed.
        if let Err(error) = self.merge_when_due() {
            tracing::warn!("a maintenance tick left the sorted runs standing: {error}");
        }
        self.scrub_once()?;
        Ok(())
    }

    /// Retry the seals of segments a device failure left footerless
    ///
    /// Run by the tick ahead of the handover, so a segment the retry seals can give
    /// its keys to the paged tier in the same pass.
    pub fn retry_broken_seals(&self) -> usize {
        crate::append::retry_broken_seals(self.reel.shared())
    }

    /// Republish what the volume occupies, for the write path's ceiling check
    ///
    /// Taken from the filesystem rather than summed over the index, since
    /// preallocation puts whole segments on disk before a record lands in one and
    /// what the guard needs is what ENOSPC counts.
    fn publish_footprint(&self) {
        if self.bias.is_none() {
            return;
        }
        // Summed over every volume: the guard slows the door when the store as a
        // whole is running out, while a single full drive is placement's problem. A
        // reading with no answers at all leaves the last one standing.
        let mut used = 0u64;
        let mut answered = false;
        for root in self.reel.shared().volumes.roots() {
            let capacity = crate::reel::bias::capacity_bytes(root);
            let available = crate::reel::bias::available_bytes(root);
            if let (Some(capacity), Some(available)) = (capacity, available) {
                used = used.saturating_add(capacity.saturating_sub(available));
                answered = true;
            }
        }
        if answered {
            self.apply_footprint(used);
        }
    }

    /// Set both halves of the write door from one footprint reading
    ///
    /// The refusal half reads the stored number per put, and the slow half is pushed
    /// into the admission budget here. Apart from `publish_footprint` so both doors
    /// can be driven from a reading a caller chooses.
    pub(super) fn apply_footprint(&self, used: u64) {
        self.footprint.store(used, Ordering::Relaxed);
        self.reel
            .shared()
            .budget
            .throttle(self.compactor.pressure().foreground_throttle(used));
    }

    /// Settle one bounded pass of what standing range deletes still cover
    ///
    /// A range delete costs its caller a record and a cover, so the records it took
    /// are settled here. What comes back is whether anything is still owed.
    pub fn sweep_covers(&self) -> Result<bool> {
        self.index.sweep_covers(SWEEP_RUN)
    }

    /// Give back the places held by tombstones nothing older can still reach
    ///
    /// A delete holds the key's place so a put drawn before it cannot be published
    /// after it and come back fresh. A tick that finds every drawn number published
    /// prunes to the counter itself, and one that does not falls back to the window.
    pub fn prune_tombstones(&self) -> u64 {
        let shared = self.reel.shared();
        let peek = shared.lsn.peek().as_u64();
        // A paging volume keeps the window: the exact floor lets a grave go the
        // moment its origin seals, and a footer search that cannot offer that segment
        // yet then answers an older version.
        let is_exact = shared.nothing_unpublished() && !self.config.index.pages();
        let floor = if is_exact {
            peek
        } else {
            peek.saturating_sub(GRAVE_WINDOW)
        };
        if floor == 0 {
            return 0;
        }
        self.index.prune_tombstones(Lsn(floor))
    }

    /// Shed carried values down to the configured budget, coldest first
    ///
    /// Two loads and nothing else while the budget is unset, and one sum while
    /// it is unmet, so an unarmed volume never pays for the tier's policy.
    pub fn shed_carried(&self) -> u64 {
        let budget = self.config.carried_budget.to_bytes();
        if budget == 0 {
            return 0;
        }
        self.index.shed_carried(budget)
    }

    /// Run one bounded compaction pass over the fullest sealed segment
    ///
    /// The pass is deferred while ingest is hot and debt is low, otherwise it picks
    /// the segment with the highest dead fraction past the effective threshold,
    /// rewrites its live records, and retires it. A named rate is held inside the
    /// pass, so a paced volume returns when the bytes it moved have been paid for.
    pub fn compact_once(&self) -> Result<CompactPass> {
        if self.is_read_only {
            return Ok(CompactPass::Held);
        }
        // A segment the index has not been told about answers with no live records,
        // and a pass that took one would retire it having copied none of them.
        self.settle_sealed()?;
        // The guard is tried, not queued for, and it is taken before the gate is
        // read: the other order lets a second caller check the gate while a pass
        // still runs and start its own the moment that pass's charge shuts it.
        let Some(_pass) = self.compaction_plane.enter() else {
            return Ok(CompactPass::Held);
        };
        if !self.compactor.is_compaction_due() {
            return Ok(CompactPass::Held);
        }
        let dead = self.index.dead_bytes();
        let live = self.totals().bytes.to_bytes();
        let dead_fraction = dead_fraction(dead, live);
        let is_hot = self.is_ingest_hot();
        if !self
            .compactor
            .pressure()
            .should_compact(dead_fraction, is_hot)
        {
            return Ok(CompactPass::Held);
        }
        let ratio = self
            .compactor
            .pressure()
            .effective_dead_ratio(dead_fraction, is_hot);

        // A wholly dead segment retires by unlink, so every one the horizon has
        // finished drains in this pass: the gate bounds copying, and this copies
        // nothing. The scans it reads are still charged to the passes after it.
        let cue_floor = self.cues.floor();
        let mut did_work = false;
        while let Some((segment, _)) =
            self.compactor
                .select_whole_dead(&self.reel, &self.index, cue_floor)
        {
            self.compactor
                .compact_segment(&self.reel, &self.index, segment)?;
            did_work = true;
        }

        match self
            .compactor
            .select_target(&self.reel, &self.index, ratio, cue_floor)
        {
            Some((segment, _)) => {
                self.compactor
                    .compact_segment(&self.reel, &self.index, segment)?;
                Ok(CompactPass::Copied)
            }
            None if did_work => Ok(CompactPass::Copied),
            None => Ok(CompactPass::Idle),
        }
    }

    /// Run one bounded scrub pass over sealed segments, verifying record checksums
    ///
    /// A record that fails its checksum has its key evicted through the same path a
    /// read-time failure uses, so repair re-lands it. The pass resumes where the last
    /// one stopped, so the sweep costs the scrub rate rather than a read per tick.
    pub fn scrub_once(&self) -> Result<usize> {
        if self.is_read_only {
            return Ok(0);
        }
        self.compactor.scrub_pass(&self.reel, &self.index)
    }

    /// Whether ingest since the last ask is heavy enough to defer compaction for
    ///
    /// Bytes admitted between asks rather than a point sample of the queue, so the
    /// window this measures is whatever cadence the caller asks at.
    pub(super) fn is_ingest_hot(&self) -> bool {
        let admitted = self.budget.admitted_total();
        let marker = self.ingest_marker.swap(admitted, Ordering::Relaxed);
        admitted.saturating_sub(marker) >= INGEST_HOT_BYTES
    }
}

/// Dead bytes as a fraction of what the volume is holding altogether
fn dead_fraction(dead: u64, live: u64) -> f64 {
    let total = dead + live;
    if total == 0 {
        return 0.0;
    }
    dead as f64 / total as f64
}
