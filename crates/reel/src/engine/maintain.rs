//! The maintenance tick: seal handover, footprint, compaction, the merge and the scrub

use std::path::{Path, PathBuf};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::compaction::compactor::EraseReport;
use crate::compaction::keymerge::{merge_into_key_run, MergeReport};
use crate::error::{ReelError, Result};
use crate::format::column::ColumnId;
use crate::format::footer::SegmentFooter;
use crate::format::loc::SegmentId;
use crate::format::lsn::Lsn;
use crate::index::counters::ProbeCounts;
use crate::reel::checkpoint::{
    checkpoint_name, link_into, sealed_below, staging_of, write_copy_manifest, write_copy_marker,
    Checkpoint,
};

use super::{read_only, CompactPass, ReelStore, Totals, GRAVE_WINDOW, INGEST_HOT_BYTES, SWEEP_RUN};
use crate::sync::lock;

/// Older versions the spot index cleaner takes out on one maintenance tick, with no reads
const SPOT_SCRUB_BUDGET: usize = 65_536;

/// Runs one walk merges, the uncovered segments and the key runs, before a key merge
///
/// A walk seeks once in every run over its start, so this bounds what a short scan pays.
const MERGE_DEPTH: usize = 8;

/// How often the hand-over runs while compaction passes hold the tick, the tick's own second
const HANDOVER_TICK: Duration = Duration::from_secs(1);

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
        while let Some((segment, footer)) = self.next_to_hand_over() {
            paged += self.hand_over(segment, &footer)?;
        }
        Ok(paged)
    }

    /// Record the key range each column occupies in one sealed segment
    ///
    /// The half of the handover worth doing on every volume: it names the segments a
    /// search must consider and rules out the rest, without handing a key over.
    fn note_spans(&self, segment: SegmentId, footer: &SegmentFooter) -> Result<()> {
        // The footer is in hand and the spans are not down yet. A retire that lands
        // here takes the file and clears the registry this is about to write into.
        crate::sync::rendezvous::at("seal/spans");
        self.index.note_spans(segment, footer)
    }

    /// The next sealed segment owed a handover, oldest first, with the footer its seal wrote
    fn next_to_hand_over(&self) -> Option<(SegmentId, Arc<SegmentFooter>)> {
        lock(&self.held).pop_front()
    }

    /// Give one sealed segment's keys to its footer
    ///
    /// The spans are not recorded here: recording them off this queue would put a
    /// segment compaction has since retired back into the search. Paging a key out
    /// cannot, since it moves only an entry that still points into this segment.
    fn hand_over(&self, segment: SegmentId, footer: &SegmentFooter) -> Result<usize> {
        // The keys are given up one at a time, with reads served throughout.
        crate::sync::rendezvous::at("paged/handover");
        let mut paged = 0usize;
        for partition in &footer.partitions {
            paged += self.index.page_out_partition(segment, partition)?;
        }
        Ok(paged)
    }

    /// Take the segments sealed since the last tick, record their spans, and queue the handover
    ///
    /// Spans go down first and for every residency, and only a paged volume queues the
    /// handover. A segment stays on the sealed queue until it has been noted, so nothing
    /// retires a segment the index cannot search yet.
    pub(super) fn hold_sealed(&self) -> Result<()> {
        let sealed = self.reel.shared().peek_sealed();
        if sealed.is_empty() {
            return Ok(());
        }
        for (segment, footer) in &sealed {
            // A footer with no key range is given up to compaction.
            if let Err(error) = self.note_spans(*segment, footer) {
                tracing::warn!(
                    "reel segment {} sealed with a footer that names no key range: {error}",
                    segment.as_u32()
                );
            }
        }
        let named: Vec<SegmentId> = sealed.iter().map(|(segment, _)| *segment).collect();
        self.reel.shared().settle_sealed(&named);

        if !self.config.index.pages() {
            return Ok(());
        }
        lock(&self.held).extend(sealed);
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

    /// Merge the walk's runs into one key run once too many stand over one key
    ///
    /// What the tick drives. The young pile is every sealed segment no key run covers
    /// yet. Key runs join it smallest first while each is no bigger than twice what is
    /// taken, so the young runs merge often and cheaply and a large run is merged again
    /// only once the pile has grown to its size. Nothing comes back where the walk is
    /// shallow enough, or another pass holds the seat.
    pub fn merge_when_due(&self) -> Result<Option<MergeReport>> {
        if self.is_read_only {
            return Ok(None);
        }
        let Some(_pass) = self.compaction_plane.enter() else {
            return Ok(None);
        };
        let Some(_merging) = self.index.key_runs().try_merge() else {
            return Ok(None);
        };
        self.settle_sealed()?;
        if self.index.overlap_depth() <= MERGE_DEPTH || self.index.has_pending_covers() {
            return Ok(None);
        }
        let shared = self.reel.shared();
        let covered = self.index.key_runs().covered();
        let owed = shared.pending_seals();
        let mut segments = Vec::new();
        let mut taken = 0u64;
        for (segment, bytes) in self.index.segments_snapshot() {
            if covered.contains(&segment) || shared.is_held(segment) || owed.contains(&segment) || bytes.total() == 0 {
                continue;
            }
            let Some(footer) = shared.footer_of(segment)? else {
                continue;
            };
            taken += footer.entry_count() as u64;
            segments.push(segment);
        }
        let mut runs = self.index.key_runs().runs();
        runs.sort_by_key(|run| run.columns().iter().map(|column| column.rows()).sum::<u64>());
        let mut joining = Vec::new();
        for run in runs {
            let rows: u64 = run.columns().iter().map(|column| column.rows()).sum();
            if taken > 0 && rows > taken.saturating_mul(2) {
                break;
            }
            taken += rows;
            joining.push(run);
        }
        let merged = merge_into_key_run(&self.compactor, &self.reel, &self.index, &segments, &joining)?;
        if merged.runs_merged == 0 {
            return Ok(None);
        }
        self.compactor.note_merged_runs(merged.runs_merged);
        Ok(Some(merged))
    }

    /// Run one bounded pass of the whole maintenance plane
    ///
    /// Every pass is paced by the rates the volume was configured with, so the caller
    /// drives this on a timer and never has to bound it. A compaction pass moves the
    /// bytes its cap allows and takes as long as those bytes owe at that cap, and an
    /// unpaced one returns at device speed. The scrub also stops at the stretch it
    /// earned since its last pass, so a slow scrub holds the tick no longer than it
    /// waited.
    pub fn maintain_once(&self) -> Result<()> {
        if self.is_read_only {
            return Ok(());
        }
        self.retry_broken_seals();
        self.publish_footprint();
        self.page_out_sealed()?;
        self.sweep_covers()?;
        self.prune_tombstones();
        self.compact_and_merge()?;
        // The handover goes again ahead of the scrub, since a rewrite or a merge above
        // can hold the tick long enough for a backlog of sealed keys to build.
        self.page_out_sealed()?;
        self.scrub_once()?;
        self.index.scrub_spot(SPOT_SCRUB_BUDGET);
        self.index.sweep_walk_runs();
        Ok(())
    }

    /// Run the tick's compaction passes, with the hand-over going on beside them
    fn compact_and_merge(&self) -> Result<()> {
        let running = AtomicBool::new(true);
        std::thread::scope(|scope| {
            // The hand-over keeps its own second while the passes run. A merge holds the
            // tick for as long as it takes, and the keys sealed meanwhile would wait in the
            // map for all of it, holding memory the page cache then goes without.
            let keeper = scope.spawn(|| -> Result<()> {
                loop {
                    std::thread::park_timeout(HANDOVER_TICK);
                    if !running.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    self.page_out_sealed()?;
                }
            });
            let passed = self.run_passes();
            running.store(false, Ordering::Release);
            keeper.thread().unpark();
            let kept = keeper.join().unwrap_or_else(|panic| std::panic::resume_unwind(panic));
            passed.and(kept)
        })
    }

    /// Rewrite and merge with every compaction pass the volume runs at once
    ///
    /// A one-pass volume rewrites and then merges, so a run the rewrite unlinked whole
    /// is never bytes the merge reads. A wider one merges on one worker while the rest
    /// rewrite, and they keep rewriting for as long as the merge holds the tick, since
    /// the tick waits for it anyway.
    fn run_passes(&self) -> Result<()> {
        let passes = self.config.compact_passes();
        if passes == 1 {
            self.compact_once()?;
            self.merge_owed();
            return Ok(());
        }
        let merging = AtomicBool::new(true);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                self.merge_owed();
                merging.store(false, Ordering::Release);
            });
            let workers: Vec<_> = (1..passes)
                .map(|_| {
                    scope.spawn(|| -> Result<()> {
                        while self.compact_once()? == CompactPass::Copied && merging.load(Ordering::Acquire) {}
                        Ok(())
                    })
                })
                .collect();
            workers
                .into_iter()
                .try_for_each(|worker| worker.join().unwrap_or_else(|panic| std::panic::resume_unwind(panic)))
        })
    }

    /// Merge what is due, where a pass that refuses or fails is one tier of one tick
    fn merge_owed(&self) {
        if let Err(error) = self.merge_when_due() {
            tracing::warn!("a maintenance tick left the walk's runs unmerged: {error}");
        }
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
            None => {
                self.compactor.release_spare();
                Ok(CompactPass::Idle)
            }
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
