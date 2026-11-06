//! A sealing segment's rows in order, merged from every shard with fixed buffers

use std::mem;

use reel_api::column_id::ColumnId;
use reel_api::units::ByteCount;

use crate::config::MAX_KEY_BYTES;
use crate::index::entry_slab::NO_ENTRY;
use crate::index::open_slots::OpenSlot;
use crate::index::seek_target::{SeekTarget, compare_rows};
use crate::index::write_index::{SHARD_COUNT, WriteIndex};
use crate::index::write_row::WriteRow;
use crate::index::write_tree::{TreePosition, entry_at, step_forward};
use crate::pool::touched_capacity;

/// A merge copies this many of a shard's rows under one hold, so their cold reads overlap
pub const WALK_RUN_ROWS: usize = 16;

/// One shard's next row
#[derive(Clone, Copy, Debug)]
pub struct WalkHead<'walk> {
    pub column: ColumnId,
    pub key: &'walk [u8],
    pub row: Option<WriteRow>,
    pub is_newest: bool,
}

/// One copied row, with its key in the run's key buffer
#[derive(Clone, Copy, Debug, Default)]
pub struct RunRow {
    pub column: ColumnId,
    pub key_start: usize,
    pub key_end: usize,
    pub row: Option<WriteRow>,
    pub is_newest: bool,
}

/// A shard's next rows, copied out so the merge holds no lock
#[derive(Debug)]
pub struct WalkRun {
    pub next: Option<TreePosition>,
    pub rows: [RunRow; WALK_RUN_ROWS],
    pub keys: Vec<u8>,
    pub count: usize,
    pub at: usize,
}

/// A merge over a slot's sixteen trees. A loser tree holds the order, so a row costs four compares, one a level
#[derive(Debug)]
pub struct SealWalk {
    pub slot: OpenSlot,
    pub runs: Vec<WalkRun>,
    losers: [usize; SHARD_COUNT],
    winner: usize,
}

impl SealWalk {
    /// Read the heap the merge's buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let keys: usize = self.runs.iter().map(|run| run.keys.capacity()).sum();
        ByteCount((self.runs.capacity() * size_of::<WalkRun>() + keys) as u64)
    }

    /// Allocate the merge's buffers
    pub fn new() -> Self {
        let mut runs = Vec::with_capacity(SHARD_COUNT);
        for _shard in 0..SHARD_COUNT {
            runs.push(WalkRun {
                next: None,
                rows: [RunRow::default(); WALK_RUN_ROWS],
                keys: touched_capacity(WALK_RUN_ROWS * MAX_KEY_BYTES as usize, 0),
                count: 0,
                at: 0,
            });
        }
        Self {
            slot: OpenSlot::default(),
            runs,
            losers: [0; SHARD_COUNT],
            winner: 0,
        }
    }

    /// Point the merge at the start of a slot, whose trees no longer change
    pub fn start(&mut self, index: &WriteIndex, slot: OpenSlot) {
        self.slot = slot;
        for shard in 0..SHARD_COUNT {
            self.runs[shard].next = {
                let guard = index.read_shard(shard);
                guard.trees[slot.0 as usize].first_position(&guard.nodes)
            };
            self.load(index, shard);
        }
        self.build_tree();
    }

    /// Copy a shard's next rows into its run, under one hold
    pub fn load(&mut self, index: &WriteIndex, shard: usize) {
        let run = &mut self.runs[shard];
        run.at = 0;
        run.count = 0;
        run.rows[0] = RunRow::default();
        run.keys.clear();
        if run.next.is_none() {
            return;
        }

        let guard = index.read_shard(shard);
        // the addresses sit side by side in the leaves, and with them in hand no slab read waits on the one before
        let mut entries = [NO_ENTRY; WALK_RUN_ROWS];
        while run.count < WALK_RUN_ROWS
            && let Some(position) = run.next
        {
            entries[run.count] = entry_at(&guard.nodes, position);
            run.next = step_forward(&guard.nodes, position);
            run.count += 1;
        }
        for (row, entry) in run.rows.iter_mut().zip(entries.into_iter().take(run.count)) {
            let key_start = run.keys.len();
            run.keys.extend_from_slice(guard.slab.key(entry));
            *row = RunRow {
                column: guard.slab.column(entry),
                key_start,
                key_end: run.keys.len(),
                row: guard.slab.row(entry),
                is_newest: guard.slab.newer(entry) == NO_ENTRY && !guard.slab.has_newer_sealed(entry),
            };
        }
    }

    /// Read a shard's next row
    pub fn head(&self, shard: usize) -> WalkHead<'_> {
        let run = &self.runs[shard];
        let row = run.rows[run.at];
        WalkHead {
            column: row.column,
            key: &run.keys[row.key_start..row.key_end],
            row: row.row,
            is_newest: row.is_newest,
        }
    }

    /// Check one shard's row comes before another's, a shard with no row coming last
    pub fn is_before(&self, shard: usize, other: usize) -> bool {
        let (head, against) = (self.head(shard), self.head(other));
        let (Some(row), Some(against_row)) = (head.row, against.row) else {
            return head.row.is_some();
        };
        let target = SeekTarget::new(against.column, against.key, against_row.place);
        compare_rows(head.column, head.key, row.place, &target).is_lt()
    }

    /// Play every match, from the shards up
    pub fn build_tree(&mut self) {
        let mut winners = [0; 2 * SHARD_COUNT];
        for shard in 0..SHARD_COUNT {
            winners[SHARD_COUNT + shard] = shard;
        }
        for node in (1..SHARD_COUNT).rev() {
            let (left, right) = (winners[2 * node], winners[2 * node + 1]);
            let (winner, loser) = if self.is_before(right, left) { (right, left) } else { (left, right) };
            winners[node] = winner;
            self.losers[node] = loser;
        }
        self.winner = winners[1];
    }

    /// Play the matches of a shard again after its row changed, from its place in the tree to the top
    pub fn replay(&mut self, shard: usize) {
        let mut winner = shard;
        let mut node = (SHARD_COUNT + shard) / 2;
        while node > 0 {
            if self.is_before(self.losers[node], winner) {
                mem::swap(&mut self.losers[node], &mut winner);
            }
            node /= 2;
        }
        self.winner = winner;
    }

    /// Find the shard holding the next row in order
    pub fn next_shard(&self) -> Option<usize> {
        self.head(self.winner).row.map(|_row| self.winner)
    }

    /// Move past a shard's current row
    pub fn advance(&mut self, index: &WriteIndex, shard: usize) {
        let run = &mut self.runs[shard];
        run.at += 1;
        if run.at >= run.count {
            self.load(index, shard);
        }
        self.replay(shard);
    }
}

impl Default for SealWalk {
    fn default() -> Self {
        Self::new()
    }
}
