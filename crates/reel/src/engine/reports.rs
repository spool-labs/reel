//! What a store says about itself: sizes from the summaries, and each job's state and counts, none of it a scan

use std::io::Error;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::maintenance_report::{CompleteReading, JOB_KINDS, JobKind, JobState, MaintenanceReport, VolumeReading};
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, RootId, RowCount, SegmentCount, VolumeId};
use reel_api::usage::{ColumnRootUsage, ColumnUsage, RootUsage, UsageReport, VolumeUsage};

use crate::engine::engine_state::EngineState;
use crate::engine::job_tally::STALL_AFTER;
use crate::engine::planner::is_sealing;
use crate::engine::volume::{NO_SEAL, Volume};
use crate::index::complete_table::CompleteTable;
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::io::{Io, IoFile};
use crate::segment::segment_tail::SegmentNumber;
use crate::segment::summary::{ColumnFacts, SummaryRows};

impl<Backend: Io> EngineState<Backend> {
    /// Count bytes a job moved
    pub fn note_moved(&self, kind: JobKind, bytes: ByteCount) {
        self.tallies[kind.index()].note_moved(bytes, self.io.now());
    }

    /// Count a corruption error on its way to a caller
    pub fn counted<Answer>(&self, answer: Result<Answer, StoreError>) -> Result<Answer, StoreError> {
        if matches!(answer, Err(StoreError::Corruption { .. })) {
            self.corruption_errors.fetch_add(1, Ordering::Relaxed);
        }
        answer
    }

    /// Estimate the live bytes of the whole store
    pub fn live_data_size(&self) -> ByteCount {
        let mut total = 0;
        for volume in &self.volumes {
            for_each_column(&hold(&volume.summary), |_row, column| total += column.live_bytes.0);
        }
        ByteCount(total)
    }

    /// Estimate the keys of the whole store: rows that aren't graves or replaced, sealed and open
    pub fn key_count(&self) -> RowCount {
        let mut total = 0;
        for volume in &self.volumes {
            for_each_column(&hold(&volume.summary), |_row, column| {
                let dead = column.dead_rows.0 - column.dead_graves.0.min(column.dead_rows.0);
                total += column.row_count.0.saturating_sub(column.grave_count.0).saturating_sub(dead);
            });
            total += hold(&volume.claims).open.iter().map(|segment| segment.row_count.0).sum::<u64>();
        }
        RowCount(total)
    }

    /// Fill a report with one column's live and on-disk bytes, on each root and in all
    pub fn column_usage(&self, column: ColumnId, report: &mut ColumnUsage) -> Result<(), StoreError> {
        let info = self.column(column)?;
        let volume = &self.volumes[info.volume.0 as usize];
        report.clear();
        for_each_column(&hold(&volume.summary), |segment, facts| {
            if facts.column != column {
                return;
            }
            let on_disk = facts.live_bytes.0 + facts.dead_bytes.0;
            report.live = ByteCount(report.live.0 + facts.live_bytes.0);
            report.on_disk = ByteCount(report.on_disk.0 + on_disk);
            let root = volume.table.root_of(segment).unwrap_or_default();
            let at = report.roots.iter().position(|usage| usage.root == root).unwrap_or_else(|| {
                report.roots.push(ColumnRootUsage { root, ..ColumnRootUsage::default() });
                report.roots.len() - 1
            });
            report.roots[at].live = ByteCount(report.roots[at].live.0 + facts.live_bytes.0);
            report.roots[at].on_disk = ByteCount(report.roots[at].on_disk.0 + on_disk);
        });
        Ok(())
    }

    /// Fill a report with used and free bytes of every volume and root
    pub fn volume_usage(&self, report: &mut UsageReport) -> Result<(), StoreError> {
        report.clear();
        for (index, volume) in self.volumes.iter().enumerate() {
            let id = VolumeId(index as u8);
            let first = report.roots.len();
            for (root, path) in volume.roots.iter().enumerate() {
                let free = self.io.free_space(path).map_err(usage_error)?;
                report.roots.push(RootUsage {
                    volume: id,
                    root: RootId(root as u8),
                    used: ByteCount(0),
                    free,
                });
            }
            let roots = &mut report.roots[first..];
            let mut failed = None;
            volume.table.for_each_segment(|_segment, root, file| match file.held_bytes() {
                Ok(held) => add_used(roots, root, held),
                Err(error) => failed = Some(error),
            });
            for segment in hold(&volume.claims).open.iter() {
                match segment.data.held_bytes().and_then(|data| Ok(ByteCount(data.0 + segment.rows.held_bytes()?.0))) {
                    Ok(held) => add_used(roots, segment.root, held),
                    Err(error) => failed = Some(error),
                }
            }
            if let Some(error) = failed {
                return Err(usage_error(error));
            }
            report.volumes.push(VolumeUsage {
                volume: id,
                used: ByteCount(roots.iter().map(|root| root.used.0).sum()),
                free: ByteCount(roots.iter().map(|root| root.free.0).sum()),
            });
        }
        Ok(())
    }

    /// Fill a report with each job's state and the store's counts
    pub fn maintenance(&self, report: &mut MaintenanceReport) {
        report.clear();
        let now = self.io.now();
        for (entry, kind) in report.jobs.iter_mut().zip(JOB_KINDS) {
            let tally = &self.tallies[kind.index()];
            entry.bytes_moved = tally.bytes_moved();
            entry.last_run = tally.last_run();
            let since = entry.last_run.unwrap_or(self.opened_at);
            entry.state = if self.is_stopping.load(Ordering::Acquire) {
                JobState::Stopped
            } else if !self.has_work(kind) {
                JobState::Idle
            } else if now.0.saturating_sub(since.0) >= STALL_AFTER.0 {
                JobState::Stalled
            } else {
                JobState::Running
            };
        }

        let (mut rewritten, mut unlinked, mut parked, mut empties, mut strays, mut retries) = (0, 0, 0, 0, 0, 0);
        for (index, volume) in self.volumes.iter().enumerate() {
            rewritten += volume.segments_rewritten.load(Ordering::Acquire);
            unlinked += volume.segments_unlinked.load(Ordering::Acquire);
            parked += volume.segments_parked.load(Ordering::Acquire);
            empties += volume.empties_left.load(Ordering::Acquire);
            strays += volume.stray_outputs.load(Ordering::Acquire);
            retries += volume.reclaim_retries.load(Ordering::Acquire);
            let rows = hold(&volume.summary);
            report.volumes.push(VolumeReading {
                volume: VolumeId(index as u8),
                segment_count: SegmentCount(rows.rows().count() as u64),
                used_bytes: ByteCount(rows.rows().map(|row| row.facts.file_length.0).sum()),
            });
        }
        for (id, info) in self.columns.iter().enumerate() {
            if info.is_some_and(|info| info.is_complete) {
                let table = self.complete_table(ColumnId(id as u8));
                report.complete_columns.push(CompleteReading {
                    column: ColumnId(id as u8),
                    has_table: table.is_some(),
                    used_slots: table.map_or(RowCount(0), CompleteTable::used_slots),
                    rows_past_capacity: table.map_or(RowCount(0), CompleteTable::past_capacity),
                });
            }
        }
        report.segments_rewritten = SegmentCount(rewritten);
        report.segments_unlinked = SegmentCount(unlinked);
        report.segments_parked = SegmentCount(parked);
        report.empties_left = SegmentCount(empties);
        report.stray_outputs = SegmentCount(strays);
        report.reclaim_retries = RowCount(retries);
        report.graves_at_open = RowCount(self.graves_at_open.load(Ordering::Acquire));
        report.deletes_since_open = RowCount(self.deletes.load(Ordering::Relaxed));
        report.graves_dropped = RowCount(self.graves_dropped.load(Ordering::Relaxed));
        report.graves_carried = RowCount(self.graves_carried.load(Ordering::Relaxed));
        report.rewrites_reclaiming_nothing = SegmentCount(self.empty_rewrites.load(Ordering::Relaxed));
        report.corruption_errors = RowCount(self.corruption_errors.load(Ordering::Relaxed));
        report.damaged_segments = SegmentCount(self.damaged_segments.load(Ordering::Acquire));
        report.summary_rows_dropped = RowCount(self.dropped_summary_rows.load(Ordering::Acquire));
        report.refused_reads = RowCount(self.cues.refused_reads());
    }

    /// Check a kind of job has work standing, which is what makes moving nothing a stall
    pub fn has_work(&self, kind: JobKind) -> bool {
        match kind {
            JobKind::RowWriteOut => (0..self.queues.len()).any(|slot| self.has_waiting_rows(OpenSlot(slot as u8))),
            JobKind::Seal => self.volumes.iter().any(has_seal_work),
            JobKind::DeadByteWalk => self.volumes.iter().any(has_walk_work),
            JobKind::RangeDeleteApply => self.volumes.iter().any(|volume| volume.is_applying.load(Ordering::Acquire)),
            JobKind::Compaction => self.volumes.iter().any(has_reclaim_work),
            JobKind::KeyRunMerge | JobKind::SpotCacheUpkeep => false,
        }
    }

    /// Count what a store holds at open that a roll compares carried graves against
    pub fn note_graves_at_open(&self) {
        let mut total = 0;
        for volume in &self.volumes {
            for_each_column(&hold(&volume.summary), |_row, column| {
                total += column.grave_count.0.saturating_sub(column.dead_graves.0);
            });
        }
        self.graves_at_open.store(total, Ordering::Release);
    }
}

/// Visit every column of every summary row
pub fn for_each_column(rows: &SummaryRows, mut visit: impl FnMut(SegmentNumber, &ColumnFacts<'_>)) {
    for row in rows.rows() {
        let mut cursor = row.cursor();
        for _column in 0..row.column_count {
            let Some(facts) = cursor.column() else {
                break;
            };
            visit(row.facts.segment, &facts);
        }
    }
}

/// Add bytes to a root's used total
pub fn add_used(roots: &mut [RootUsage], root: RootId, bytes: ByteCount) {
    if let Some(usage) = roots.iter_mut().find(|usage| usage.root == root) {
        usage.used = ByteCount(usage.used.0 + bytes.0);
    }
}

/// Check a volume has a seal to run
pub fn has_seal_work<Backend: Io>(volume: &Volume<Backend>) -> bool {
    volume.sealing.load(Ordering::Acquire) != NO_SEAL || hold(&volume.claims).open.iter().any(is_sealing)
}

/// Check a volume has a walk to run
pub fn has_walk_work<Backend: Io>(volume: &Volume<Backend>) -> bool {
    volume.walks_waiting.load(Ordering::Acquire) > 0 || volume.has_walk_target.load(Ordering::Acquire)
}

/// Check a volume has a merge under way or a file it can't yet give up
pub fn has_reclaim_work<Backend: Io>(volume: &Volume<Backend>) -> bool {
    volume.is_rewriting()
        || volume.segments_parked.load(Ordering::Acquire) > 0
        || volume.stray_outputs.load(Ordering::Acquire) > 0
}

/// Report a failed space reading
pub fn usage_error(source: Error) -> StoreError {
    StoreError::Io {
        action: "reading space used",
        source,
    }
}
