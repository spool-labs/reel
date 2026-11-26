//! One write index shard's place in a walk, kept between steps

use crate::index::write_tree::TreePosition;

/// Most rows a cursor steps past before a seek is the cheaper way
pub const MAX_CURSOR_STEPS: u32 = 8;

/// A walk's place in one tree, good while its leaf's version stands
#[derive(Clone, Copy, Debug, Default)]
pub struct TreeCursor {
    pub position: Option<TreePosition>,
    pub version: u32,
    /// The node pool's reset count when the cursor stood, so a cursor from before the pool gave its pages back never reads them
    pub pool_resets: u32,
    pub is_placed: bool,
}

/// A walk's place in every open segment of one shard
#[derive(Clone, Debug)]
pub struct ShardCursor {
    trees: Vec<TreeCursor>,
    /// A bit for each tree a walk took for placing since the last reset, so a reset touches only those
    touched: u64,
}

impl ShardCursor {
    /// Allocate a cursor for a number of open slots
    pub fn new(open_slots: usize) -> Self {
        Self {
            trees: vec![TreeCursor::default(); open_slots],
            touched: 0,
        }
    }

    /// Take one tree's cursor to place, which the next reset clears
    pub fn tree(&mut self, index: usize) -> &mut TreeCursor {
        self.touched |= 1 << index;
        &mut self.trees[index]
    }

    /// Start a new walk in the same buffer
    pub fn reset(&mut self) {
        let mut touched = self.touched;
        while touched != 0 {
            self.trees[touched.trailing_zeros() as usize] = TreeCursor::default();
            touched &= touched - 1;
        }
        self.touched = 0;
    }

    /// Check no tree has a place yet, so the next placement descends every tree
    pub fn is_reset(&self) -> bool {
        self.touched == 0
    }

    /// Read the heap the cursor holds
    pub fn heap_bytes(&self) -> usize {
        self.trees.capacity() * size_of::<TreeCursor>()
    }
}
