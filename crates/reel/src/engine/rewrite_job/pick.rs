//! Picking a rewrite's inputs: the overlap runs, the ready segments, and the bars a segment's dead share is held to

use std::cell::Cell;
use std::cmp::Reverse;
use std::iter;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::maintenance_report::{
    PickKind, PickOutcome,
};
use reel_api::units::{ByteCount, DeathMark, FileOffset, WritePlace};

use crate::engine::reserve_job::RESERVE_PERIOD;
use crate::engine::engine_state::EngineState;
use crate::engine::seal_job::MOST_SEGMENT_BLOCKS;
use crate::engine::volume::{MOST_MERGE_OUTPUTS, Volume};
use crate::guard::hold;
use crate::lifetime::MarkRange;
use crate::io::Io;
use crate::fixed_list::FixedList;
use crate::index::key_hash::hash_key;
use crate::segment::block_entry::KeyBound;
use crate::segment::column_directory::ColumnDirectory;
use crate::segment::segment_tail::{MOST_MERGE_SEGMENTS, SegmentNumber};
use crate::segment::summary::{SummaryRow, SummaryRows};
use crate::segment::summary_orders::{COVERS_LIST, ROW_FOOTER_BYTES, SHARE_BUCKETS, SHARE_SCALE, WalkList, is_wholly_dead, reclaim_share, share_bucket};

use crate::engine::compact_job::can_unlink;
use crate::engine::load_state::{Load, OverlapLanes, Owed, is_capped, is_deferring, is_paced, overlap_lanes, record_bar};
use crate::engine::rewrite_job::{BAR_SCALE, BUCKET_LOOKS, MOST_PASSED_LOOKS, BarsFired, CROWD_PAGES, MOST_COUNTED_CROWD, MOST_LOOK_TIME, OVERLAP_BASE_RECORD_BYTES, EARLY_BAR, GATHER_LOOKS, MOST_OVERLAP, MergeInput, MergeTarget, OVERLAP_LOOKS, OVERLAP_PROBES, PLAIN_BAR, PLANNER_LANE, RewriteJob, RewritePhase, rows_of};
use crate::engine::rewrite_job::install::freed_in_place;

impl<Backend: Io> RewriteJob<Backend> {
    /// Start on a run of segments over the overlap bound, or else on the next ready segments past the last one tried. A merge
    /// of walked segments needs no walk target in progress, since it would count rows this merge has decided on, and the walks
    /// wait from here to the install. A grave-heavy segment is decided first, and copies nothing if no row would drop
    pub fn start(&mut self) -> bool {
        self.claim() && self.begin()
    }

    /// Pick, take admission's word on the pick's need and claim the inputs, so no other lane picks them. The job maps its region
    /// and opens its output at its first step, on whichever thread steps it, which keeps a populate off the planner
    pub fn claim(&mut self) -> bool {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let Some((target, is_grave_heavy)) = self.pick_admitted(volume) else {
            return false;
        };
        // an output takes its disk before its inputs give theirs back, so a merge starts only with room beside the writers'
        self.clear(target);
        volume.lanes[self.lane].is_rewriting.store(!self.is_unwalked, Ordering::Release);
        let mut inputs = [SegmentNumber(0); MOST_MERGE_SEGMENTS];
        inputs[..self.input_count].copy_from_slice(&self.numbers[..self.input_count]);
        *hold(&volume.lanes[self.lane].inputs) = inputs;
        self.claimed = Some((target, is_grave_heavy));
        true
    }

    /// Check a claimed pick waits for its first step to map its region
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn is_claimed(&self) -> bool {
        self.claimed.is_some()
    }

    /// Map the claimed pick's region, carve its lists and set the job going, or give the merge up and say so
    pub fn begin(&mut self) -> bool {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let (Some((target, is_grave_heavy)), Some((admit, bound))) = (self.claimed.take(), self.admitted.take()) else {
            return false;
        };
        let Some(region) = self.state.map_admitted(&admit) else {
            self.abandon();
            return false;
        };
        self.region_need = admit.need;
        if self.attach(region, &bound).is_err() {
            // the bound came out short of its own carving, a bug the full count shows
            self.abandon();
            return false;
        }
        self.clear(target);
        self.begun_at = state.io.now();
        if bound.is_holding {
            self.hold_inputs(volume);
        }
        // the inputs copy in offset order when their lists fit, and a deciding pass says which rows go across and to which output
        let rows: u64 = self.inputs[..self.input_count].iter().map(|input| input.block_rows).sum();
        self.is_ordered = self.is_ordered_bound && rows as usize <= self.placed.len();
        if self.is_ordered {
            volume.offset_order_copies.fetch_add(1, Ordering::Relaxed);
            self.cut_bytes = self.merge_cut(target);
        } else {
            volume.key_order_copies.fetch_add(1, Ordering::Relaxed);
        }
        self.marks.fill(0);
        self.cuts = 0;
        self.decide_bytes = 0;
        self.decide_rows = 0;
        if is_grave_heavy || self.is_ordered {
            self.phase = RewritePhase::Deciding;
            return true;
        }
        match self.open_output(volume, target) {
            Ok(()) => true,
            Err(_error) => {
                self.abandon();
                false
            },
        }
    }

    /// Pick the inputs: a run of segments over the overlap bound first, since that comes ahead of the walk, then ready segments
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn pick(&mut self, volume: &Volume<Backend>) -> Option<(MergeTarget, bool)> {
        self.pick_within(volume, MOST_MERGE_SEGMENTS)
    }

    /// Pick up to this many inputs
    pub fn pick_within(&mut self, volume: &Volume<Backend>, most: usize) -> Option<(MergeTarget, bool)> {
        let room = volume.park_room().min(most);
        let refused_at = volume.room_refused_at.load(Ordering::Acquire);
        let is_backing_off = refused_at != 0 && self.state.io.now().0.saturating_sub(refused_at) < RESERVE_PERIOD.0;
        if !volume.can_start_rewrite() || volume.stray_outputs.load(Ordering::Acquire) > 0 || room == 0 || is_backing_off {
            volume.park_refusals.fetch_add(u64::from(room == 0), Ordering::Relaxed);
            volume.note_pick(PickOutcome::Refused);
            return None;
        }
        let state = Arc::clone(&self.state);
        let kind = if self.lane == PLANNER_LANE { PickKind::Overlap } else { PickKind::Rewrite };
        let now = self.state.io.now();
        let load = state.load_of(volume, now);
        let mut rows = state.hold_for_pick(self.volume, kind);
        self.is_unwalked = false;
        self.bars_fired = BarsFired::default();
        // its rows counted for nothing a walk can trust, so the output goes through the forward walk they owe
        // no input is the walk's started target, whose rows the walk is counting
        let walking = volume.walk_segment.load(Ordering::Acquire);
        if let Some(row) = self.rebuild.and_then(|segment| rows.get(segment)).filter(|row| row.facts.segment.0 != walking) {
            self.is_unwalked = true;
            let target = self.gather(volume, &rows, &row, 1, |_row| false);
            // the row counts every block the segment had, and only the ones that read are in the table
            self.inputs[0].block_count = self.rebuild_blocks;
            self.picked_as = Some(PickOutcome::Rebuild);
            volume.note_pick(PickOutcome::Rebuild);
            return Some((target, false));
        }
        // busy callers keep their cores: a few merges run while they call, and every worker may merge once they go quiet
        let in_flight = volume.lanes.iter().filter(|lane| lane.is_rewriting.load(Ordering::Acquire)).count();
        let is_over_cap = is_capped(load) && in_flight >= volume.merge_cap();
        // a forced segment frees a stuck seal or the cover pool, so writers wait on it and it goes past the cap, as it goes past
        // hot ingest
        if is_over_cap && self.forced.is_none() {
            volume.capped_picks.fetch_add(1, Ordering::Relaxed);
            volume.note_pick(PickOutcome::Deferred);
            return None;
        }
        // an overlap merge copies records, so one at a time on the first worker keeps callers at their line while they call, and
        // once they stop every idle worker takes one over segments no other merge holds. The planner runs one only with no workers
        let busy_lane = if volume.walk_slots.is_empty() { PLANNER_LANE } else { PLANNER_LANE + 1 };
        let may_overlap = !is_over_cap
            && match overlap_lanes(load) {
                OverlapLanes::None => false,
                OverlapLanes::One => self.lane == busy_lane,
                OverlapLanes::All => true,
            };
        if may_overlap && let Some(target) = self.pick_overlap(volume, &rows, room) {
            self.picked_as = Some(PickOutcome::Overlap);
            volume.note_pick(PickOutcome::Overlap);
            return Some((target, false));
        }
        // a full pool refuses range deletes, so the segment holding the oldest cover back goes whatever the trigger says
        if let Some(row) = self.forced.take().and_then(|forced| rows.get(forced)).filter(|row| is_mergeable(volume, row)) {
            self.forced_pick = Some(row.facts.segment);
            let target = self.gather(volume, &rows, &row, 1, |_row| false);
            self.picked_as = Some(PickOutcome::Forced);
            volume.note_pick(PickOutcome::Forced);
            return Some((target, false));
        }
        // a forced segment that can't merge leaves the cap as it was
        if is_over_cap {
            volume.capped_picks.fetch_add(1, Ordering::Relaxed);
            volume.note_pick(PickOutcome::Deferred);
            return None;
        }
        // record rewrites run under hot ingest at the pace, which the dead bytes the walk counts pay for. A spinning disk keeps
        // today's rule, leaving them for quiet time, until a run on one measures the pace
        if is_deferring(load) && volume.is_spinning {
            volume.note_pick(PickOutcome::Deferred);
            return None;
        }
        // a volume merges back to the bound before it reclaims, so record rewrites wait while an overlap merge runs or can start
        if volume.is_over_bound.load(Ordering::Acquire) && !load.is_disk_short {
            volume.note_pick(PickOutcome::Behind);
            return None;
        }
        let current = DeathMark(self.state.current_mark.load(Ordering::Relaxed));
        let owed = Owed { is_reclaim_due: is_reclaim_due(&rows), is_reclaim_held: volume.is_reclaim_held() };
        // a row's counts are trusted with every segment walked, or with every replacement charged below the unwalked ones. A
        // trusted row goes at the floor, under ingest too, and one that isn't waits for the half bar. Under ingest a short disk
        // holds trusted rows to the floor, since past it a copy is of mostly live rows while the disk fills
        let (floor, half) = match owed.is_reclaim_due {
            true => (record_bar(load, owed), PLAIN_BAR),
            false => (record_bar(Load { is_disk_short: false, ..load }, Owed { is_reclaim_due: true, ..owed }), PLAIN_BAR),
        };
        // a spinning disk keeps the half bar while callers are busy, as it did before the pace, until a run on one measures it
        let floor = if volume.is_spinning && load.is_busy { half } else { floor };
        let walked_below = walked_below(&rows);
        let gave_up = self.gave_up_list(volume);
        let index = self.volume;
        // a wholly dead segment the unlink can take now goes by the unlink, and a rewrite of it would throw one of the two away
        let is_for_unlink = |row: &SummaryRow<'_>| is_wholly_dead(row) && can_unlink(&state, index, volume, row.facts.segment);
        let is_ready_at = |row: &SummaryRow<'_>, plain: u64| {
            let bar = (current, plain);
            let is_free = row.facts.is_walked() && row.facts.segment.0 != walking && !gave_up.holds(row.facts.segment);
            is_free && !is_for_unlink(row) && is_rewrite_ready(volume, row, 0, bar) && is_rewrite_ready(volume, row, freed_in_place(volume, row), bar)
        };
        // an untrusted row waits for the half bar, and a pick counts one fallback when such a row would have passed the floor
        let has_fallen_back = Cell::new(false);
        let bar_of = |row: &SummaryRow<'_>| match is_count_trusted(volume, row.facts.segment, walked_below) {
            true => floor,
            false => {
                if !has_fallen_back.get() && is_ready_at(row, floor) && !is_ready_at(row, half) {
                    has_fallen_back.set(true);
                }
                half.max(floor)
            },
        };
        let is_ready = |row: &SummaryRow<'_>| is_ready_at(row, bar_of(row));
        // the most won back for the bytes read goes first, and a picked row moves to its bucket's end, so a merge that gave way
        // lets the next one up
        let first = first_ready(&rows, floor.min(half), is_ready, is_for_unlink);
        volume.trust_fallbacks.fetch_add(u64::from(has_fallen_back.get()), Ordering::Relaxed);
        if let Some(first) = first {
            let is_grave_heavy = is_grave_heavy(&first);
            let (segment, lifetime) = (first.facts.segment, first.facts.marks);
            let is_more = |row: &SummaryRow<'_>| is_ready(row) && row.facts.segment > segment && row.facts.marks.shares_lifetime(lifetime);
            let target = self.gather(volume, &rows, &first, room, is_more);
            // a pick goes with no pace only when every input is past half dead, so a first input past half gathers no paced rows
            // for free
            let mut inputs = self.numbers[..self.input_count].iter().filter_map(|number| rows.get(*number));
            let is_past_half = inputs.all(|row| is_ready_at(&row, PLAIN_BAR));
            self.is_paced = is_paced(load, is_past_half);
            self.note_bars(&rows, current);
            rows.rotate(segment);
            self.picked_as = Some(PickOutcome::Record);
            volume.note_pick(PickOutcome::Record);
            return Some((target, is_grave_heavy));
        }
        volume.note_pick(PickOutcome::Nothing);
        None
    }

    /// End a look that reached the newest segment. A pass that began past the oldest looks again from the start, since a segment
    /// it passed while a walk or a seal held it can merge now. A pass that saw every segment and found nothing clears the flag,
    /// since overlap only rises at a seal
    pub fn end_pass(&mut self, volume: &Volume<Backend>) {
        if volume.looks.has_left() {
            return;
        }
        volume.looks.set_up_to(SegmentNumber(0));
        if volume.looks.is_whole() {
            volume.is_over_bound.store(false, Ordering::Release);
        } else {
            volume.looks.set_left(true);
        }
        volume.looks.set_whole(true);
    }

    /// Pick a run of segments over the overlap bound, all walked or all unwalked: a walked input's rows are counted and an
    /// unwalked one's aren't, so a mix leaves no rule for the output's counts
    pub fn pick_overlap(&mut self, volume: &Volume<Backend>, rows: &SummaryRows, room: usize) -> Option<MergeTarget> {
        let sealing = volume.sealing.load(Ordering::Acquire);
        let walking = volume.walk_segment.load(Ordering::Acquire);
        // a merge that found nothing to win leaves its inputs out until something changes, or it would run again for nothing
        let gave_up = self.gave_up_list(volume);
        let can_merge = |row: &SummaryRow<'_>| {
            let segment = row.facts.segment.0;
            let is_free = segment != sealing && segment != walking && !volume.is_taken(row.facts.segment) && !gave_up.holds(row.facts.segment);
            is_overlap_input(row) && is_free
        };
        let state = Arc::clone(&self.state);
        // a column with a complete table has no overlap bound
        let directory_of = |column: ColumnId| state.complete_table(column).is_none().then(|| state.directories[column.0 as usize].as_ref()).flatten();
        let lists = (&mut self.listed, &mut self.crowd);
        let started = state.io.now();
        let is_late = || state.io.now().0.saturating_sub(started.0) > MOST_LOOK_TIME.0;
        let arena = &self.state.filter_arena;
        let is_resident = |segment: SegmentNumber| !volume.are_filters_held_out() && arena.has_filters(segment);
        // a crowd answer holds while no filter came or went, and holding the filters out changes every answer
        let filters = 2 * arena.changes() + u64::from(volume.are_filters_held_out());
        let answers = (&mut self.crowd_answers, filters);
        let mut looked_up_to = volume.looks.up_to();
        let found = overlap_start(rows, directory_of, &mut looked_up_to, can_merge, &mut self.crowded_key, lists, !volume.is_bound_pinned(), &is_resident, is_late, answers);
        volume.looks.set_up_to(looked_up_to);
        volume.looks.set_left(!rows.orders().after(looked_up_to).is_empty());
        let Some((crowded, column)) = found else {
            // a look stopped for time has more to see, so it ends no pass and finds no miss
            if is_late() {
                volume.looks.set_left(true);
                return None;
            }
            self.end_pass(volume);
            volume.note_pick(PickOutcome::OverlapMiss);
            return None;
        };
        let point = &self.crowded_key;
        let lifetime = crowded.facts.marks;
        let is_near = |row: &SummaryRow<'_>| can_merge(row) && covers_point(row, column, point) && row.facts.marks.shares_lifetime(lifetime);
        // the segments over the point are the merge's candidates, and the directory lists them
        if let Some(directory) = directory_of(column) {
            directory.overlapping(&KeyBound::of_key(point), &KeyBound::past_prefix(point), 0, &mut self.listed);
        }
        let near = self.listed.iter().filter_map(|segment| rows.get(*segment)).filter(is_near);
        // walked segments go first, since their dead rows leave in the same copy that brings overlap down, and unwalked ones only
        // when fewer than two walked ones cover the point
        let is_unwalked = near.filter(|row| row.facts.is_walked()).take(2).count() < 2;
        let is_more = |row: &SummaryRow<'_>| is_near(row) && row.facts.is_walked() != is_unwalked;
        // the widest ranges over the point go first: segments that span the same keys merge overlap down across all of them,
        // and pieces of different merges meet only near the point
        let near = self.listed.iter().filter_map(|segment| rows.get(*segment)).filter(is_near);
        let shared = near.filter_map(|row| column_range(&row, column)).map(|range| shared_with(range, point)).min().unwrap_or(0);
        let width = |row: &SummaryRow<'_>| column_range(row, column).map_or(0, |range| range_width(range, shared));
        let picks = (is_more, width);
        let target = gather_widest(volume, (rows, &self.listed), room, picks, &mut self.inputs, &mut self.numbers, &mut self.input_count);
        // a point whose segments running rewrites hold merges once they end, so record rewrites start no more and the look stays
        if self.input_count < 2 && self.listed.iter().any(|segment| volume.is_taken(*segment)) {
            volume.looks.set_left(false);
            volume.is_over_bound.store(true, Ordering::Release);
            volume.note_pick(PickOutcome::OverlapLone);
            return None;
        }
        // one segment alone wins back no overlap, and the next look starts past it. A point no merge can start holds no record
        // rewrite back
        if self.input_count < 2 {
            volume.looks.set_up_to(crowded.facts.segment);
            volume.looks.set_left(!rows.orders().after(volume.looks.up_to()).is_empty());
            self.end_pass(volume);
            volume.note_pick(PickOutcome::OverlapLone);
            return None;
        }
        // the look resumes past this point after the merge, so the pass it's part of no longer saw every segment
        volume.looks.set_whole(false);
        self.is_unwalked = is_unwalked;
        if is_unwalked {
            let mut merging = hold(&volume.lanes[self.lane].merging);
            merging[..self.input_count].copy_from_slice(&self.numbers[..self.input_count]);
        }
        // the merge runs, and record rewrites wait for it
        volume.is_over_bound.store(true, Ordering::Release);
        Some(target)
    }

    /// Count the marked segments picked, by the bar each one passed
    pub fn note_bars(&mut self, rows: &SummaryRows, current: DeathMark) {
        for number in &self.numbers[..self.input_count] {
            let Some(marks) = rows.get(*number).map(|row| row.facts.marks).filter(|marks| !marks.is_empty()) else {
                continue;
            };
            if bar_of(marks, current) == EARLY_BAR {
                self.bars_fired.early += 1;
            } else {
                self.bars_fired.plain += 1;
            }
        }
    }

    /// Take a first segment and the next ones a test passes, while they fit one output
    pub fn gather(
        &mut self,
        volume: &Volume<Backend>,
        rows: &SummaryRows,
        first: &SummaryRow<'_>,
        most: usize,
        is_more: impl Fn(&SummaryRow<'_>) -> bool,
    ) -> MergeTarget {
        self.input_count = 0;
        let mut data = 0;
        let mut length = 0;
        let mut blocks = 0;
        let mut marks = MarkRange::NONE;
        let mut next = Some(*first);
        while let Some(row) = next {
            let facts = row.facts;
            let input = &mut self.inputs[self.input_count];
            input.segment = facts.segment;
            input.block_count = facts.block_count;
            input.block_rows = rows_of(&row);
            input.most_block_rows = facts.most_block_rows;
            input.data_end = facts.data_end.0;
            self.numbers[self.input_count] = facts.segment;
            self.input_count += 1;
            data += facts.data_end.0;
            length += facts.file_length.0;
            blocks += facts.block_count;
            marks = marks.join(facts.marks);
            if self.input_count == most {
                break;
            }
            // the next row up that passes, within a few looks along the segment order
            let after = rows.orders().after(facts.segment).iter().take(GATHER_LOOKS);
            next = after
                .filter_map(|segment| rows.get(*segment))
                .find(|row| is_more(row))
                .filter(|row| {
                    data + row.facts.data_end.0 <= volume.segment_bytes.0
                        && blocks + row.facts.block_count <= MOST_SEGMENT_BLOCKS.0
                });
        }
        MergeTarget {
            blocks_at: FileOffset(data.min(2 * volume.segment_bytes.0)),
            data: ByteCount(data),
            length: ByteCount(length),
            marks,
        }
    }
}

/// Check a segment's covers have all left the pool, with every cover still there in a sealed segment: a cover dropped inside a
/// newer one rests on that one, which a crash could lose until its segment seals
pub fn has_settled_covers<Backend: Io>(volume: &Volume<Backend>, row: &SummaryRow<'_>) -> bool {
    row.facts.has_covers && volume.covers.are_all_applied() && !volume.covers.holds_segment(row.facts.segment)
}

/// Check a segment can go into a merge at all. Damage and covers answer for more than their rows
pub fn is_mergeable<Backend: Io>(volume: &Volume<Backend>, row: &SummaryRow<'_>) -> bool {
    let is_clear = !row.facts.has_covers || has_settled_covers(volume, row);
    row.facts.is_walked() && !row.facts.is_damaged && is_clear && row.facts.block_count > 0 && !volume.is_taken(row.facts.segment)
}

/// Check a segment can go into an overlap merge: walked, or with no walk begun and a forward one owed. A half-walked segment's
/// rows are half counted
pub fn is_overlap_input(row: &SummaryRow<'_>) -> bool {
    let is_unwalked = row.facts.walked_blocks == 0 && !row.facts.is_backward;
    (row.facts.is_walked() || is_unwalked) && !row.facts.is_damaged && !row.facts.has_covers && row.facts.block_count > 0
}

/// Check a row's counts say a merge would win back its share of the rows or the bytes, past the bytes already freed in place
pub fn is_rewrite_ready<Backend: Io>(volume: &Volume<Backend>, row: &SummaryRow<'_>, freed_bytes: u64, (current, plain): (DeathMark, u64)) -> bool {
    if !is_mergeable(volume, row) {
        return false;
    }
    // a settled cover's row only leaves at a rewrite
    if row.facts.has_covers {
        return true;
    }
    let (mut rows, mut dead_rows, mut live_graves, mut bytes, mut dead_bytes) = (0, 0, 0, 0, 0);
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let Some(column) = cursor.column() else {
            return false;
        };
        rows += column.row_count.0;
        dead_rows += column.dead_rows.0;
        live_graves += column.grave_count.0.saturating_sub(column.dead_graves.0);
        bytes += column.live_bytes.0 + column.dead_bytes.0;
        dead_bytes += column.dead_bytes.0;
    }
    let won_back = dead_bytes.saturating_sub(freed_bytes) + (dead_rows + live_graves) * ROW_FOOTER_BYTES;
    won_back > 0 && BAR_SCALE * won_back >= held_bar(row.facts.marks, current, plain) * (bytes + rows * ROW_FOOTER_BYTES)
}

/// Check a row's graves make up the share that would trigger a merge, so deciding first can save the copy
pub fn is_grave_heavy(row: &SummaryRow<'_>) -> bool {
    let (mut rows, mut graves) = (0, 0);
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let Some(column) = cursor.column() else {
            return false;
        };
        rows += column.row_count.0;
        graves += column.grave_count.0;
    }
    BAR_SCALE * graves >= PLAIN_BAR * rows
}

/// Take the widest segments a test passes, the smallest first among equals, whole segments included, since the output is cut to
/// size. Wide ones first, so the inputs share the most of their ranges, and small ones among them, so a merge rewrites as little
/// as it takes to bring a crowded point down
pub fn gather_widest<Backend: Io>(
    volume: &Volume<Backend>,
    (rows, candidates): (&SummaryRows, &[SegmentNumber]),
    most: usize,
    (is_more, width): (impl Fn(&SummaryRow<'_>) -> bool, impl Fn(&SummaryRow<'_>) -> u64),
    inputs: &mut [MergeInput],
    numbers: &mut [SegmentNumber; MOST_MERGE_SEGMENTS],
    input_count: &mut usize,
) -> MergeTarget {
    // segments of one size merge first, and a point with no two of a size takes any
    let target = gather_tier(volume, (rows, candidates), most, (&is_more, &width, true), inputs, numbers, input_count);
    if *input_count >= 2 {
        return target;
    }
    gather_tier(volume, (rows, candidates), most, (&is_more, &width, false), inputs, numbers, input_count)
}

/// Gather the widest segments over a point, smallest first, keeping to the smallest one's size when told to
pub fn gather_tier<Backend: Io>(
    volume: &Volume<Backend>,
    (rows, candidates): (&SummaryRows, &[SegmentNumber]),
    most: usize,
    (is_more, width, is_tiered): (&impl Fn(&SummaryRow<'_>) -> bool, &impl Fn(&SummaryRow<'_>) -> u64, bool),
    inputs: &mut [MergeInput],
    numbers: &mut [SegmentNumber; MOST_MERGE_SEGMENTS],
    input_count: &mut usize,
) -> MergeTarget {
    *input_count = 0;
    let (mut data, mut length, mut blocks) = (0, 0, 0);
    let mut marks = MarkRange::NONE;
    while *input_count < most {
        let taken = &numbers[..*input_count];
        let fits = |row: &SummaryRow<'_>| {
            // an input joins while it's no bigger than twice the merge's smallest, so a big output waits for others of its size
            // and isn't copied again with every handful of small ones
            let is_tier = !is_tiered || *input_count == 0 || row.facts.data_end.0 <= 2 * inputs[0].data_end;
            let is_inside = is_tier && data + row.facts.data_end.0 <= MOST_MERGE_OUTPUTS as u64 * volume.segment_bytes.0;
            is_inside && blocks + row.facts.block_count <= MOST_MERGE_OUTPUTS as u32 * MOST_SEGMENT_BLOCKS.0 && !taken.contains(&row.facts.segment)
        };
        let listed = candidates.iter().filter_map(|segment| rows.get(*segment));
        // the deadest go first, so the copy that brings overlap down also wins the most back, then the widest, then the smallest
        let Some(row) = listed.filter(|row| is_more(row) && fits(row)).min_by_key(|row| (Reverse(dead_permille(row)), Reverse(width(row)), row.facts.data_end)) else {
            break;
        };
        let facts = row.facts;
        inputs[*input_count].segment = facts.segment;
        inputs[*input_count].block_count = facts.block_count;
        inputs[*input_count].block_rows = rows_of(&row);
        inputs[*input_count].most_block_rows = facts.most_block_rows;
        inputs[*input_count].data_end = facts.data_end.0;
        numbers[*input_count] = facts.segment;
        *input_count += 1;
        data += facts.data_end.0;
        length += facts.file_length.0;
        blocks += facts.block_count;
        marks = marks.join(facts.marks);
    }
    MergeTarget {
        blocks_at: FileOffset(data.min(2 * volume.segment_bytes.0)),
        data: ByteCount(data),
        length: ByteCount(length),
        marks,
    }
}

/// Read a segment's lowest key and bound for a column. An empty bound runs past the highest key
pub fn column_range<'row>(row: &SummaryRow<'row>, column: ColumnId) -> Option<(&'row [u8], &'row [u8])> {
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let facts = cursor.column()?;
        if facts.column == column {
            return Some((facts.lowest, facts.bound));
        }
    }
    None
}

/// Count the bytes a range's ends share with a key inside it
pub fn shared_with((lowest, bound): (&[u8], &[u8]), key: &[u8]) -> usize {
    let shared = |end: &[u8]| end.iter().zip(key).take_while(|(left, right)| left == right).count();
    if bound.is_empty() { shared(lowest) } else { shared(lowest).min(shared(bound)) }
}

/// Read eight bytes of a key past a prefix as a number, so ranges past a prefix they all share compare by the keys they span
pub fn key_number(key: &[u8], prefix: usize) -> u64 {
    let mut bytes = [0; 8];
    let tail = key.get(prefix..).unwrap_or_default();
    let length = tail.len().min(bytes.len());
    bytes[..length].copy_from_slice(&tail[..length]);
    u64::from_be_bytes(bytes)
}

/// Read the keys a range spans past a prefix, an empty bound running to the top
pub fn range_width((lowest, bound): (&[u8], &[u8]), prefix: usize) -> u64 {
    let high = if bound.is_empty() { u64::MAX } else { key_number(bound, prefix) };
    high.saturating_sub(key_number(lowest, prefix))
}

/// Check a segment's range for a column holds a key. A summary keeps only a key's first bytes, so this can only be wider than the
/// truth
pub fn covers_point(row: &SummaryRow<'_>, column: ColumnId, point: &[u8]) -> bool {
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let Some(facts) = cursor.column() else {
            return false;
        };
        if facts.column == column {
            return facts.lowest <= point && (facts.bound.is_empty() || point < facts.bound);
        }
    }
    false
}

/// Check two segments share a column and their ranges meet. A summary keeps only a key's first bytes, so a range here can only
/// be wider than the truth
#[cfg(any(test, feature = "test-hooks"))]
pub fn overlaps(left: &SummaryRow<'_>, right: &SummaryRow<'_>) -> bool {
    let (mut left_columns, mut right_columns) = (left.cursor(), right.cursor());
    let (mut left_left, mut right_left) = (left.column_count, right.column_count);
    let (mut ours, mut theirs) = (left_columns.column(), right_columns.column());
    // a row keeps its columns in order, so the two lists meet in one pass
    while let (Some(our), Some(their)) = (&ours, &theirs) {
        if our.column == their.column {
            let is_before = !their.bound.is_empty() && their.bound <= our.lowest;
            let is_after = !our.bound.is_empty() && our.bound <= their.lowest;
            if !is_before && !is_after {
                return true;
            }
        }
        if our.column <= their.column {
            left_left -= 1;
            ours = if left_left > 0 { left_columns.column() } else { None };
        } else {
            right_left -= 1;
            theirs = if right_left > 0 { right_columns.column() } else { None };
        }
    }
    false
}

/// Find a point more segments cover than the bound allows, in a segment no look has passed, and fill in the key. Overlap only
/// grows when a segment arrives, so each one takes one look, and with none new a look costs nothing. It rises at a segment's
/// lowest key, so the points worth trying are the new segment's own and those of a few that begin inside it
#[allow(clippy::too_many_arguments, reason = "the summary rows, the directories, the look-up mark and the crowd answers")]
pub fn overlap_start<'rows, 'state>(
    rows: &'rows SummaryRows,
    directory_of: impl Fn(ColumnId) -> Option<&'state ColumnDirectory>,
    looked_up_to: &mut SegmentNumber,
    can_merge: impl Fn(&SummaryRow<'_>) -> bool,
    point: &mut FixedList<u8>,
    lists: (&mut FixedList<SegmentNumber>, &mut FixedList<SegmentNumber>),
    is_sized: bool,
    is_resident: &dyn Fn(SegmentNumber) -> bool,
    is_late: impl Fn() -> bool,
    (answers, filters): (&mut CrowdAnswers, u64),
) -> Option<(SummaryRow<'rows>, ColumnId)> {
    let (listed, crowd) = lists;
    for _look in 0..OVERLAP_LOOKS {
        // a look holds the summary, so one that runs long stops and the next pick goes on from here
        if is_late() {
            return None;
        }
        let segment = *rows.orders().after(*looked_up_to).first()?;
        let row = rows.get(segment)?;
        let mut columns = row.cursor();
        for _column in 0..row.column_count {
            let facts = columns.column()?;
            let Some(directory) = directory_of(facts.column) else {
                continue;
            };
            let high = if facts.bound.is_empty() { KeyBound::HIGHEST } else { KeyBound::past_prefix(facts.bound) };
            directory.starting_inside(&KeyBound::of_key(facts.lowest), &high, listed);
            let others = listed.iter().filter_map(|other| rows.get(*other));
            let inside = others.filter_map(|other| lowest_inside(&other, facts.column, facts.lowest, facts.bound));
            let mut probes = iter::once(facts.lowest).chain(inside.take(OVERLAP_PROBES));
            let stamp = CrowdStamp { directory: directory.generation(), filters, is_sized };
            let found = probes.find(|key| answers.answer(facts.column, key, stamp, || is_crowded(rows, directory, (facts.column, key), crowd, is_sized, is_resident)));
            if let Some(key) = found.filter(|_key| can_merge(&row)) {
                point.set_from(key).ok()?;
                return Some((row, facts.column));
            }
        }
        *looked_up_to = segment;
    }
    None
}

/// Crowd answers a pick keeps, so a point one pass asks again costs nothing while the lists and the filters stay as they were
pub const CROWD_ANSWERS: usize = 64;

/// What the lists and the filters were when a crowd answer was worked out
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CrowdStamp {
    pub directory: u64,
    pub filters: u64,
    pub is_sized: bool,
}

/// One point's crowd answer
#[derive(Clone, Copy, Debug, Default)]
pub struct CrowdAnswer {
    pub column: ColumnId,
    pub hash: u64,
    pub length: usize,
    pub stamp: CrowdStamp,
    pub is_crowded: bool,
    pub is_set: bool,
}

/// A ring of crowd answers, the oldest going first. A hash that matches another point's only changes which merge a pick starts
#[derive(Clone, Debug)]
pub struct CrowdAnswers {
    pub answers: Vec<CrowdAnswer>,
    pub next: usize,
    pub worked: u64,
    pub reused: u64,
}

impl CrowdAnswers {
    /// Allocate the ring
    pub fn new() -> Self {
        Self {
            answers: vec![CrowdAnswer::default(); CROWD_ANSWERS],
            next: 0,
            worked: 0,
            reused: 0,
        }
    }

    /// Answer a point from the ring when the stamp still holds, or work it out and keep it
    pub fn answer(&mut self, column: ColumnId, key: &[u8], stamp: CrowdStamp, work: impl FnOnce() -> bool) -> bool {
        let hash = hash_key(column, key).0;
        let is_same = |answer: &CrowdAnswer| answer.is_set && answer.column == column && answer.hash == hash && answer.length == key.len();
        if let Some(answer) = self.answers.iter_mut().find(|answer| is_same(answer)) {
            if answer.stamp == stamp {
                self.reused += 1;
                return answer.is_crowded;
            }
            answer.is_set = false;
        }
        self.worked += 1;
        let is_crowded = work();
        self.answers[self.next] = CrowdAnswer { column, hash, length: key.len(), stamp, is_crowded, is_set: true };
        self.next = (self.next + 1) % CROWD_ANSWERS;
        is_crowded
    }
}

impl Default for CrowdAnswers {
    fn default() -> Self {
        Self::new()
    }
}

/// Check more than the bound's segments hold a key, counting the directory's segments over it against their rows. A segment a
/// running rewrite, the walk or a seal holds now counts, since the merge takes it once that ends, and one no merge can ever take
/// is left out, since no merge could bring the crowd down
pub fn is_crowded(
    rows: &SummaryRows,
    directory: &ColumnDirectory,
    (column, key): (ColumnId, &[u8]),
    crowd: &mut FixedList<SegmentNumber>,
    is_sized: bool,
    is_resident: &dyn Fn(SegmentNumber) -> bool,
) -> bool {
    // a summary row keeps a key cut short, so the directory is asked for every key that starts with it
    let (low, high) = (KeyBound::of_key(key), KeyBound::past_prefix(key));
    let (mut covering, mut skip, mut bytes, mut rows_seen) = (0, 0, 0, 0);
    // a bound above the least holds only while every crowd segment's filters are resident, since a get probes each of them
    let mut is_all_resident = true;
    for _page in 0..CROWD_PAGES {
        let page = directory.overlapping(&low, &high, skip, crowd);
        skip += crowd.len();
        let over = crowd.iter().filter_map(|segment| rows.get(*segment));
        for row in over.filter(|row| can_ever_merge(row) && covers_point(row, column, key)) {
            covering += 1;
            is_all_resident &= is_resident(row.facts.segment);
            let (row_bytes, row_rows) = record_totals(&row);
            bytes += row_bytes;
            rows_seen += row_rows;
        }
        let bound = if is_sized && is_all_resident { overlap_bound(bytes, rows_seen) } else { MOST_OVERLAP };
        if covering > bound {
            return true;
        }
        if !page.is_cut {
            return false;
        }
    }
    false
}

/// Read a segment's lowest key for a column when it falls inside a range
pub fn lowest_inside<'row>(row: &SummaryRow<'row>, column: ColumnId, lowest: &[u8], bound: &[u8]) -> Option<&'row [u8]> {
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let facts = cursor.column()?;
        if facts.column == column {
            let is_inside = facts.lowest > lowest && (bound.is_empty() || facts.lowest < bound);
            return is_inside.then_some(facts.lowest);
        }
    }
    None
}

/// Read a segment's bar in hundredths: the early bar while its highest mark stands, and the plain one after that or with no marks
pub fn bar_of(marks: MarkRange, current: DeathMark) -> u64 {
    if marks.highest().is_some_and(|highest| current <= highest) {
        EARLY_BAR
    } else {
        PLAIN_BAR
    }
}

/// Read the bar a pick holds a segment to, given the plain bar it holds unmarked ones to: a marked segment keeps the early bar,
/// or the plain one when that is higher
pub fn held_bar(marks: MarkRange, current: DeathMark, plain: u64) -> u64 {
    if bar_of(marks, current) == EARLY_BAR { EARLY_BAR.max(plain) } else { plain }
}

/// Find the ready row that wins most back in the highest share bucket that has one, down to the least share the plain bar lets
/// in, and then among the rows with covers. A pick looks at no more than the first few rows of a bucket, and of equal shares
/// takes the last, which leaves the rows before it to other lanes. Rows it passes over for the unlink count for none of those
/// looks, up to MOST_PASSED_LOOKS a bucket, so a queue of dead segments ahead of a proof hides no ready row behind it
pub fn first_ready<'rows>(
    rows: &'rows SummaryRows,
    plain: u64,
    is_ready: impl Fn(&SummaryRow<'_>) -> bool,
    is_passed: impl Fn(&SummaryRow<'_>) -> bool,
) -> Option<SummaryRow<'rows>> {
    let least = share_bucket(SHARE_SCALE * plain / BAR_SCALE);
    let mut lists = (least..SHARE_BUCKETS).rev().chain(iter::once(COVERS_LIST));
    lists.find_map(|list| {
        let (mut looks, mut passed) = (0, 0);
        let mut best: Option<SummaryRow<'rows>> = None;
        for segment in rows.orders().share_list(list) {
            if looks == BUCKET_LOOKS {
                break;
            }
            let Some(row) = rows.get(segment) else {
                looks += 1;
                continue;
            };
            if passed < MOST_PASSED_LOOKS && is_passed(&row) {
                passed += 1;
                continue;
            }
            looks += 1;
            if is_ready(&row) && best.as_ref().is_none_or(|kept| reclaim_share(&row) >= reclaim_share(kept)) {
                best = Some(row);
            }
        }
        best
    })
}

/// Check a volume holds dead bytes and the walk has counted every segment, so its idle time goes to reclaiming them all with each
/// segment copied once
pub fn is_reclaim_due(rows: &SummaryRows) -> bool {
    let orders = rows.orders();
    orders.dead_bytes() > 0 && orders.walk_count(WalkList::Unwalked) + orders.walk_count(WalkList::Backward) == 0
}

/// Check a segment could join an overlap merge once whatever holds it now lets go: damage, pending covers and an empty row block
/// list keep it out for good
pub fn can_ever_merge(row: &SummaryRow<'_>) -> bool {
    !row.facts.is_damaged && !row.facts.has_covers && row.facts.block_count > 0
}

/// Check every root has room for a merge's output and the outputs other merges claimed, and beside the writers' reserve unless the merge frees space
pub fn has_disk_room<Backend: Io>(state: &EngineState<Backend>, volume: &Volume<Backend>, output: ByteCount, is_freeing: bool) -> bool {
    let kept = if is_freeing { 0 } else { volume.reserve.0 + volume.segment_bytes.0 };
    let need = kept + volume.outputs_promised.load(Ordering::Acquire) + output.0;
    volume.roots.iter().all(|root| state.io.free_space(root).is_ok_and(|free| free.0 >= need))
}

/// Estimate what a merge writes for a segment: its live bytes and its live rows' footer share
pub fn output_estimate(row: &SummaryRow<'_>) -> u64 {
    let mut estimate = 0;
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let Some(column) = cursor.column() else {
            return row.facts.file_length.0;
        };
        estimate += column.live_bytes.0 + column.row_count.0.saturating_sub(column.dead_rows.0) * ROW_FOOTER_BYTES;
    }
    estimate
}

impl<Backend: Io> RewriteJob<Backend> {
    /// Find the output a pick would write, when every root has room for it. A merge that gives back more than it writes may use
    /// the writers' reserve, or a full disk could never free itself
    pub fn disk_room_for(&self, volume: &Volume<Backend>, length: ByteCount) -> Option<ByteCount> {
        let summary = hold(&volume.summary);
        // every segment below this place has finished its walk, so a replacement under it is one the merge's ask can find
        let walked_below = walked_below(&summary);
        let (mut inputs, mut output, mut lowest) = (0, 0, WritePlace(u64::MAX));
        for row in self.numbers[..self.input_count].iter().filter_map(|number| summary.get(*number)) {
            inputs += row.facts.file_length.0;
            output += match is_count_trusted(volume, row.facts.segment, walked_below) {
                true => output_estimate(&row),
                false => row.facts.file_length.0,
            };
            lowest = lowest.min(row.facts.places.lowest);
        }
        drop(summary);
        // a live cue at or above an input row keeps the dead ones it can read, so the merge may write all it reads
        if self.state.cues.has_cue_between(self.volume, lowest, WritePlace(u64::MAX)) {
            output = length.0;
        }
        if volume.is_estimate_low() {
            output /= 4;
        }
        let output = ByteCount(output.min(length.0));
        has_disk_room(&self.state, volume, output, output.0 < inputs).then_some(output)
    }
}

/// Most rows of the walk lists a pick reads for the lowest place not yet walked, past which it trusts no count
pub const MOST_UNWALKED_LOOKS: usize = 1_024;

/// Find the lowest place of a segment the walk hasn't finished, from the walk lists, which hold only unwalked rows. Lists past
/// their looks give the lowest place there is, so no count is trusted
pub fn walked_below(rows: &SummaryRows) -> Option<WritePlace> {
    let orders = rows.orders();
    let listed = orders.walk_list(WalkList::Unwalked).chain(orders.walk_list(WalkList::Backward));
    let mut lowest: Option<WritePlace> = None;
    for (looks, segment) in listed.enumerate() {
        if looks == MOST_UNWALKED_LOOKS {
            return Some(WritePlace(0));
        }
        if let Some(row) = rows.get(segment) {
            lowest = Some(lowest.map_or(row.facts.places.lowest, |held| held.min(row.facts.places.lowest)));
        }
    }
    lowest
}

/// Check a segment's dead counts stand for rows a merge drops: with no segment unwalked, or with every replacement charged since
/// open below the unwalked ones. A count from before a reopen may rest on a segment that walks again, which keeps its rows
pub fn is_count_trusted<Backend: Io>(volume: &Volume<Backend>, segment: SegmentNumber, walked_below: Option<WritePlace>) -> bool {
    let Some(unwalked) = walked_below else {
        return true;
    };
    volume.table.highest_replacement(segment).is_some_and(|place| place.0 > 0 && place < unwalked)
}

/// Add up a segment's record bytes and rows over every column, since a merge copies the whole segment
pub fn record_totals(row: &SummaryRow<'_>) -> (u64, u64) {
    let (mut bytes, mut rows) = (0, 0);
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let Some(column) = cursor.column() else {
            break;
        };
        bytes += column.live_bytes.0 + column.dead_bytes.0;
        rows += column.row_count.0;
    }
    (bytes, rows)
}

/// Work out how many segments over one key a crowd of this mean record size may hold before a merge is due, short of the most a
/// crowd count reaches
pub fn overlap_bound(bytes: u64, rows: u64) -> usize {
    let mean = bytes / rows.max(1);
    let scaled = MOST_OVERLAP as u64 * mean / OVERLAP_BASE_RECORD_BYTES;
    MOST_OVERLAP.max(scaled.min(MOST_COUNTED_CROWD as u64 - 1) as usize)
}

/// Read the share of a segment's record bytes its walk counted dead, in thousandths
pub fn dead_permille(row: &SummaryRow<'_>) -> u64 {
    let (mut bytes, mut dead) = (0, 0);
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let Some(column) = cursor.column() else {
            break;
        };
        bytes += column.live_bytes.0 + column.dead_bytes.0;
        dead += column.dead_bytes.0;
    }
    dead * 1_000 / bytes.max(1)
}
