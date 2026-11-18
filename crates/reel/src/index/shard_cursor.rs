//! One write index shard's place in a walk, kept between steps

use crate::index::write_tree::TreePosition;

/// Most rows a cursor steps past before a seek is the cheaper way
pub const MAX_CURSOR_STEPS: u32 = 8;

/// A walk's place in one tree, good while its leaf's version stands
#[derive(Clone, Copy, Debug, Default)]
pub struct TreeCursor {
    pub position: Option<TreePosition>,
    pub version: u32,
    pub is_placed: bool,
}

/// A walk's place in every open segment of one shard
#[derive(Clone, Debug)]
pub struct ShardCursor {
    pub trees: Vec<TreeCursor>,
}

impl ShardCursor {
    /// Allocate a cursor for a number of open slots
    pub fn new(open_slots: usize) -> Self {
        Self {
            trees: vec![TreeCursor::default(); open_slots],
        }
    }

    /// Start a new walk in the same buffer
    pub fn reset(&mut self) {
        self.trees.fill(TreeCursor::default());
    }

    /// Check no tree has a place yet, so the next placement descends every tree
    pub fn is_reset(&self) -> bool {
        self.trees.iter().all(|tree| !tree.is_placed)
    }
}
