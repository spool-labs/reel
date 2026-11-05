//! A sealing segment's rows in order, merged from every shard with fixed buffers

use std::mem;

use reel_api::column_id::ColumnId;
use reel_api::units::ByteCount;

use crate::pool::touched_capacity;
use crate::config::MAX_KEY_BYTES;
use crate::index::open_slots::OpenSlot;
use crate::index::seek_target::{SeekTarget, compare_rows};
use crate::index::write_index::{SHARD_COUNT, WriteIndex};
use crate::index::write_row::WriteRow;
use crate::index::write_tree::{TreePosition, entry_at, step_forward};

/// One shard's next row, copied out so the merge holds no lock
#[derive(Clone, Debug)]
pub struct WalkHead {
    pub position: Option<TreePosition>,
    pub column: ColumnId,
    pub key: Vec<u8>,
    pub row: Option<WriteRow>,
}

/// A merge over a slot's sixteen trees. A loser tree holds the order, so a row costs four compares, one a level
#[derive(Debug)]
pub struct SealWalk {
    pub slot: OpenSlot,
    pub heads: Vec<WalkHead>,
    losers: [usize; SHARD_COUNT],
    winner: usize,
}

impl SealWalk {
    /// Read the heap the merge's buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let keys: usize = self.heads.iter().map(|head| head.key.capacity()).sum();
        ByteCount((self.heads.capacity() * size_of::<WalkHead>() + keys) as u64)
    }

    /// Allocate the merge's buffers
    pub fn new() -> Self {
        let mut heads = Vec::with_capacity(SHARD_COUNT);
        for _shard in 0..SHARD_COUNT {
            heads.push(WalkHead {
                position: None,
                column: ColumnId::default(),
                key: touched_capacity(MAX_KEY_BYTES as usize, 0),
                row: None,
            });
        }
        Self {
            slot: OpenSlot::default(),
            heads,
            losers: [0; SHARD_COUNT],
            winner: 0,
        }
    }

    /// Point the merge at the start of a slot, whose trees no longer change
    pub fn start(&mut self, index: &WriteIndex, slot: OpenSlot) {
        self.slot = slot;
        for shard in 0..SHARD_COUNT {
            let position = {
                let guard = index.read_shard(shard);
                guard.trees[slot.0 as usize].first_position(&guard.nodes)
            };
            self.heads[shard].position = position;
            self.load(index, shard);
        }
        self.build_tree();
    }

    /// Copy a shard's current row into its head
    pub fn load(&mut self, index: &WriteIndex, shard: usize) {
        let head = &mut self.heads[shard];
        let Some(position) = head.position else {
            head.row = None;
            return;
        };

        let guard = index.read_shard(shard);
        let entry = entry_at(&guard.nodes, position);
        head.column = guard.slab.column(entry);
        head.key.clear();
        head.key.extend_from_slice(guard.slab.key(entry));
        head.row = guard.slab.row(entry);
    }

    /// Check one shard's row comes before another's, a shard with no row coming last
    pub fn is_before(&self, shard: usize, other: usize) -> bool {
        let (head, against) = (&self.heads[shard], &self.heads[other]);
        let (Some(row), Some(against_row)) = (head.row, against.row) else {
            return head.row.is_some();
        };
        let target = SeekTarget::new(against.column, &against.key, against_row.place);
        compare_rows(head.column, &head.key, row.place, &target).is_lt()
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
        self.heads[self.winner].row.map(|_row| self.winner)
    }

    /// Move past a shard's current row
    pub fn advance(&mut self, index: &WriteIndex, shard: usize) {
        let position = self.heads[shard].position;
        let next = match position {
            Some(position) => {
                let guard = index.read_shard(shard);
                step_forward(&guard.nodes, position)
            },
            None => None,
        };
        self.heads[shard].position = next;
        self.load(index, shard);
        self.replay(shard);
    }
}

impl Default for SealWalk {
    fn default() -> Self {
        Self::new()
    }
}
