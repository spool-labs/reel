//! The maintenance tick: seal handover, footprint, compaction, the merge and the scrub

use std::path::{Path, PathBuf};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::compaction::compactor::{EraseReport, ERASE_QUIET};
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

/// The spot index cleaner takes out at most this many older versions a tick, with no reads
const SPOT_SCRUB_BUDGET: usize = 65_536;

/// A key merge runs once a walk merges more runs than this, since a walk seeks once in each
const MERGE_DEPTH: usize = 8;

/// The hand-over runs this often while compaction passes hold the tick
const HANDOVER_TICK: Duration = Duration::from_secs(1);

/// A close waits this long between looks for a free place on the compaction plane
const DRAIN_WAIT: Duration = Duration::from_millis(1);

/// A tail that takes no write for this long is sealed by the tick
const IDLE_SEAL: Duration = Duration::from_secs(5);

impl ReelStore {
    /// Tell the index about every segment that has sealed since it was last told
    pub(super) fn settle_sealed(&self) -> Result<()> {
        if self.reel.shared().has_sealed_waiting() {
            self.hold_sealed()?;
        }
        Ok(())
    }

    /// Counts of key asks to sealed segments and the reads they cost
    pub fn filter_probes(&self) -> ProbeCounts {
        self.reel.shared().probes.counts()
    }

    /// Live record count and byte total store-wide, from counters, no I/O
    pub fn totals(&self) -> Totals {
        self.index.totals()
    }

    /// Take a durable copy of the volume as it stands at a cue
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

        // Holding the cue seals the tails and stops compaction retiring what the copy needs
        let cue = self.cue()?;
        // Read after the seal, so it splits what the cue sees from what comes next
        let boundary = shared.peek_segment();

        // Everything made before the home rename is debris to sweep on failure
        let mut sweep: Vec<PathBuf> = Vec::new();
        match self.stage_and_publish(&pieces, boundary, &mut sweep) {
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

    /// Stage every piece of a checkpoint, then publish with home last
    fn stage_and_publish(
        &self,
        pieces: &[(usize, PathBuf)],
        boundary: SegmentId,
        sweep: &mut Vec<PathBuf>,
    ) -> Result<usize> {
        let shared = self.reel.shared();
        let published: Vec<PathBuf> = pieces.iter().map(|(_, piece)| piece.clone()).collect();
        let mut segments = 0usize;
        for (volume, piece) in pieces {
            let root = &shared.volumes.roots()[*volume];
            let mut sealed = sealed_below(root, boundary)?;
            // Skip held segments, which can still grow or hold an unpublished record
            sealed.retain(|segment| !shared.is_held(*segment));
            let staging = staging_of(piece);
            std::fs::create_dir_all(&staging)?;
            sweep.push(staging.clone());
            link_into(root, &staging, &sealed)?;
            // Only a copy over several volumes needs a manifest and markers
            if pieces.len() > 1 {
                match *volume == 0 {
                    true => write_copy_manifest(&staging, &published)?,
                    false => write_copy_marker(&staging, piece)?,
                }
            }
            // Links are only directory entries, so one directory sync makes a piece durable
            self.driver.sync_dir(&staging)?;
            segments += sealed.len();
        }
        // Every link is durable and no piece is published, so a crash here leaves only staging
        crate::sync::rendezvous::at("checkpoint/staged");
        for (volume, piece) in pieces.iter().skip(1) {
            std::fs::rename(staging_of(piece), piece)?;
            sweep.push(piece.clone());
            // Make each piece durable before home publishes its manifest
            self.driver.sync_dir(&shared.volumes.roots()[*volume])?;
        }
        std::fs::rename(staging_of(&pieces[0].1), &pieces[0].1)?;
        // The copy is whole and staging is gone, so a crash here leaves a copy that opens
        sweep.clear();
        crate::sync::rendezvous::at("checkpoint/published");
        if let Some(parent) = pieces[0].1.parent() {
            self.driver.sync_dir(parent)?;
        }
        Ok(segments)
    }

    /// Live record count and byte total for one column, from counters
    pub fn column_totals(&self, column: ColumnId) -> Totals {
        self.index.column_totals(column)
    }

    /// Whether a column's counters match a walk, false while a cover still owes its sweep
    pub fn counters_agree(&self, column: ColumnId) -> bool {
        !self
            .index
            .column(column)
            .is_some_and(|index| index.has_pending_covers())
    }

    /// Live count and byte total under a key prefix, when the counters answer it
    pub fn prefix_totals(&self, column: ColumnId, prefix: &[u8]) -> Option<Totals> {
        self.index.prefix_totals(column, prefix)
    }

    /// Hand the keys of newly sealed segments over to their footers
    pub fn page_out_sealed(&self) -> Result<usize> {
        self.hold_sealed()?;
        let mut paged = 0usize;
        while let Some((segment, footer)) = self.next_to_hand_over() {
            paged += self.hand_over(segment, &footer)?;
        }
        Ok(paged)
    }

    /// Record the key range each column occupies in one sealed segment
    fn note_spans(&self, segment: SegmentId, footer: &SegmentFooter) -> Result<()> {
        // A retire landing here clears the registry before the spans are written
        crate::sync::rendezvous::at("seal/spans");
        self.index.note_spans(segment, footer)
    }

    /// The next sealed segment owed a handover, oldest first, with the footer from its seal
    fn next_to_hand_over(&self) -> Option<(SegmentId, Arc<SegmentFooter>)> {
        lock(&self.held).pop_front()
    }

    /// Give one sealed segment's keys to its footer
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
    pub(super) fn hold_sealed(&self) -> Result<()> {
        let sealed = self.reel.shared().peek_sealed();
        if sealed.is_empty() {
            return Ok(());
        }
        for (segment, footer) in &sealed {
            // A footer with no key range is given up to compaction
            if let Err(error) = self.note_spans(*segment, footer) {
                tracing::warn!(
                    "reel segment {} sealed with a footer that holds no key range: {error}",
                    segment.as_u32()
                );
            }
        }
        // Only noted segments leave the sealed queue, so none is retired before it is searchable
        let named: Vec<SegmentId> = sealed.iter().map(|(segment, _)| *segment).collect();
        self.reel.shared().settle_sealed(&named);
        lock(&self.held).extend(sealed);
        Ok(())
    }

    /// Raise the purge floor, below which compaction drops marked records
    pub fn purge_below(&self, floor: u64) {
        self.reel.shared().purge_below(floor);
    }

    /// The purge floor, below which compaction drops marked records
    pub fn purge_floor(&self) -> u64 {
        self.reel.shared().purge_floor()
    }

    /// Erase the dead runs out of sealed segments, and report what came back
    pub fn erase_dead_runs(&self) -> Result<EraseReport> {
        self.compactor.erase_dead_runs(&self.reel, &self.index)
    }

    /// Merge the walk's runs into one key run once too many stand over one key
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
            if covered.contains(&segment)
                || shared.is_held(segment)
                || owed.contains(&segment)
                || bytes.total() == 0
            {
                continue;
            }
            let Some(footer) = shared.footer_of(segment)? else {
                continue;
            };
            taken += footer.entry_count() as u64;
            segments.push(segment);
        }
        let mut runs = self.index.key_runs().runs();
        runs.sort_by_key(|run| {
            run.columns()
                .iter()
                .map(|column| column.rows())
                .sum::<u64>()
        });
        // Runs join smallest first while each is at most twice the pile, so young runs merge often
        let mut joining = Vec::new();
        for run in runs {
            let rows: u64 = run.columns().iter().map(|column| column.rows()).sum();
            if taken > 0 && rows > taken.saturating_mul(2) {
                break;
            }
            taken += rows;
            joining.push(run);
        }
        let merged = merge_into_key_run(
            &self.compactor,
            &self.reel,
            &self.index,
            &segments,
            &joining,
        )?;
        if merged.runs_merged == 0 {
            return Ok(None);
        }
        self.compactor.note_merged_runs(merged.runs_merged);
        Ok(Some(merged))
    }

    /// Run one bounded pass of the whole maintenance plane
    pub fn maintain_once(&self) -> Result<()> {
        if self.is_read_only {
            return Ok(());
        }
        self.retry_broken_seals();
        self.seal_idle_tails(IDLE_SEAL)?;
        self.publish_footprint();
        self.page_out_sealed()?;
        self.sweep_covers()?;
        self.prune_tombstones();
        self.compact_and_merge()?;
        self.erase_when_due()?;
        // Hand over again before the scrub, since a long rewrite or merge lets sealed keys pile up
        self.page_out_sealed()?;
        self.scrub_once()?;
        self.index.scrub_spot(SPOT_SCRUB_BUDGET);
        self.index.sweep_walk_runs();
        Ok(())
    }

    /// Give back the blocks under one sealed segment's replaced records, once its dead bytes stop growing
    fn erase_when_due(&self) -> Result<Option<EraseReport>> {
        let ratio = self.config.compact_dead_ratio;
        self.compactor.erase_when_due(
            &self.reel,
            &self.index,
            ratio,
            self.cues.floor(),
            ERASE_QUIET,
        )
    }

    /// Seal each tail that took no write for `idle`, so compaction can reach what it holds
    pub(crate) fn seal_idle_tails(&self, idle: Duration) -> Result<()> {
        let now = Instant::now();
        let mut seen = lock(&self.idle);
        let tails = self.reel.tails();
        seen.resize(tails.len(), None);
        for (tail, watch) in tails.iter().zip(seen.iter_mut()) {
            let head = (tail.tail().active_segment(), tail.tail().committed_len());
            match *watch {
                Some((segment, len, since)) if (segment, len) == head => {
                    if !tail.holds_records() || now.duration_since(since) < idle {
                        continue;
                    }
                    tail.seal()?;
                    *watch = None;
                }
                Some(_) | None => *watch = Some((head.0, head.1, now)),
            }
        }
        Ok(())
    }

    /// Run the tick's compaction passes, with the hand-over going on beside them
    fn compact_and_merge(&self) -> Result<()> {
        let running = AtomicBool::new(true);
        std::thread::scope(|scope| {
            // A merge can hold the tick a long time, so keep handing sealed keys over meanwhile
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
            let kept = keeper
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
            passed.and(kept)
        })
    }

    /// Rewrite and merge with every compaction pass the volume runs at once
    fn run_passes(&self) -> Result<()> {
        let passes = self.config.compact_passes();
        // A single pass rewrites before it merges, so the merge never reads an unlinked run
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
            // The other passes keep rewriting while the merge holds the tick
            let workers: Vec<_> = (1..passes)
                .map(|_| {
                    scope.spawn(|| -> Result<()> {
                        loop {
                            let copied = self.compact_once()? == CompactPass::Copied;
                            if !copied || !merging.load(Ordering::Acquire) {
                                return Ok(());
                            }
                        }
                    })
                })
                .collect();
            workers.into_iter().try_for_each(|worker| {
                worker
                    .join()
                    .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
            })
        })
    }

    /// Merge what is due, logging a failure without failing the tick
    fn merge_owed(&self) {
        if let Err(error) = self.merge_when_due() {
            tracing::warn!("a maintenance tick left the walk's runs unmerged: {error}");
        }
    }

    /// Retry the seals of segments a device failure left footerless
    pub fn retry_broken_seals(&self) -> usize {
        crate::append::retry_broken_seals(self.reel.shared())
    }

    /// Republish the filesystem's used bytes for the write path's ceiling check
    fn publish_footprint(&self) {
        if self.bias.is_none() {
            return;
        }
        // Sum used bytes over every volume, and keep the last reading if none answers
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
    pub(super) fn apply_footprint(&self, used: u64) {
        self.footprint.store(used, Ordering::Relaxed);
        self.reel
            .shared()
            .budget
            .throttle(self.compactor.pressure().foreground_throttle(used));
    }

    /// Settle one bounded pass of standing range deletes, true while any are still owed
    pub fn sweep_covers(&self) -> Result<bool> {
        self.index.sweep_covers(SWEEP_RUN)
    }

    /// Give back the places held by tombstones nothing older can still reach
    pub fn prune_tombstones(&self) -> u64 {
        let shared = self.reel.shared();
        let peek = shared.lsn.peek().as_u64();
        // A write still out holds the floor wherever the window has moved to
        let settled = shared.settled_below().as_u64();
        // A cue read past a later delete finds its version through that delete's grave
        let cue = self.cues.floor().map_or(u64::MAX, |cue| cue.as_u64());
        let floor = peek.saturating_sub(GRAVE_WINDOW).min(settled).min(cue);
        if floor == 0 {
            return 0;
        }
        self.index.prune_tombstones(Lsn(floor))
    }

    /// Compact every sealed segment past the dead ratio, ignoring the rate gate, before a close
    pub fn drain(&self) -> Result<()> {
        if self.is_read_only {
            return Ok(());
        }
        loop {
            self.settle_sealed()?;
            // A tick's pass may hold every place on the plane, so wait for one
            let _pass = loop {
                match self.compaction_plane.enter() {
                    Some(seat) => break seat,
                    None => std::thread::sleep(DRAIN_WAIT),
                }
            };
            let cue_floor = self.cues.floor();
            while let Some((segment, _)) =
                self.compactor
                    .select_whole_dead(&self.reel, &self.index, cue_floor)
            {
                if !self
                    .compactor
                    .compact_segment(&self.reel, &self.index, segment)?
                {
                    break;
                }
            }
            let ratio = self.config.compact_dead_ratio;
            let Some((segment, _)) =
                self.compactor
                    .select_target(&self.reel, &self.index, ratio, cue_floor)
            else {
                return Ok(());
            };
            // A target left standing would be picked again forever
            if !self
                .compactor
                .compact_segment(&self.reel, &self.index, segment)?
            {
                return Ok(());
            }
        }
    }

    /// Run one bounded compaction pass on the sealed segment with the most dead space
    pub fn compact_once(&self) -> Result<CompactPass> {
        if self.is_read_only {
            return Ok(CompactPass::Held);
        }
        // An untold segment looks empty, so a pass would retire it without copying its records
        self.settle_sealed()?;
        // Try the guard before reading the gate, so no second pass starts behind a running one
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

        // Wholly dead segments retire by unlink and copy nothing, so all of them drain this pass
        let cue_floor = self.cues.floor();
        let mut did_work = false;
        while let Some((segment, _)) =
            self.compactor
                .select_whole_dead(&self.reel, &self.index, cue_floor)
        {
            // Stop if the segment is still there, or the next pick would be the same one forever
            if !self
                .compactor
                .compact_segment(&self.reel, &self.index, segment)?
            {
                break;
            }
            did_work = true;
        }

        match self
            .compactor
            .select_target(&self.reel, &self.index, ratio, cue_floor)
        {
            Some((segment, _)) => {
                // If the target is still there, report Held so a caller looping on progress stops
                let retired = self
                    .compactor
                    .compact_segment(&self.reel, &self.index, segment)?;
                match retired || did_work {
                    true => Ok(CompactPass::Copied),
                    false => Ok(CompactPass::Held),
                }
            }
            None if did_work => Ok(CompactPass::Copied),
            None => {
                self.compactor.release_spare();
                Ok(CompactPass::Idle)
            }
        }
    }

    /// Run one bounded scrub pass over sealed segments, verifying record checksums
    pub fn scrub_once(&self) -> Result<usize> {
        if self.is_read_only {
            return Ok(0);
        }
        self.compactor.scrub_pass(&self.reel, &self.index)
    }

    /// Whether enough bytes were admitted since the last ask to defer compaction
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
