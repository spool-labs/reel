//! A sealing segment's rows in order, merged from every shard with fixed buffers

use reel_api::column_id::ColumnId;

use crate::index::open_slots::OpenSlot;
use crate::index::seek_target::{SeekTarget, compare_rows};
use crate::index::write_index::{SHARD_COUNT, WriteIndex};
use crate::index::write_row::WriteRow;
use crate::index::write_tree::{TreePosition, entry_at, step_forward};

/// The longest key a column can declare
pub const MAX_KEY_BYTES: usize = 1_056;

/// One shard's next row, copied out so the merge holds no lock
#[derive(Clone, Debug)]
pub struct WalkHead {
    pub position: Option<TreePosition>,
    pub column: ColumnId,
    pub key: Vec<u8>,
    pub row: Option<WriteRow>,
}

/// A merge over a slot's sixteen trees
#[derive(Debug)]
pub struct SealWalk {
    pub slot: OpenSlot,
    pub heads: Vec<WalkHead>,
}

impl SealWalk {
    /// Allocate the merge's buffers
    pub fn new() -> Self {
        let mut heads = Vec::with_capacity(SHARD_COUNT);
        for _shard in 0..SHARD_COUNT {
            heads.push(WalkHead {
                position: None,
                column: ColumnId::default(),
                key: Vec::with_capacity(MAX_KEY_BYTES),
                row: None,
            });
        }
        Self {
            slot: OpenSlot::default(),
            heads,
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

    /// Find the shard holding the next row in order
    pub fn next_shard(&self) -> Option<usize> {
        let mut best: Option<usize> = None;
        for (shard, head) in self.heads.iter().enumerate() {
            let Some(row) = head.row else {
                continue;
            };
            let Some(current) = best else {
                best = Some(shard);
                continue;
            };
            let leader = &self.heads[current];
            let Some(leader_row) = leader.row else {
                continue;
            };
            let target = SeekTarget::new(leader.column, &leader.key, leader_row.place);
            if compare_rows(head.column, &head.key, row.place, &target).is_lt() {
                best = Some(shard);
            }
        }
        best
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
    }
}

impl Default for SealWalk {
    fn default() -> Self {
        Self::new()
    }
}
