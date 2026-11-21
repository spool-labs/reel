//! One open segment's rows in one shard, in row order

use std::cmp::Ordering;

use reel_core::units::RowCount;

use crate::index::entry_slab::{EntryAddress, EntrySlab, PageChain};
use crate::index::node_pool::{NO_NODE, NODE_FANOUT, NodeCount, NodeIndex, NodePool};
use crate::index::seek_target::{RowPrefix, SeekTarget, compare_rows};
use crate::segment::segment_tail::SegmentNumber;

/// Deepest a tree can get: half-full nodes reach 2^32 rows in this many levels
pub const MAX_DEPTH: usize = 10;
/// Nodes one insert can need: one per level and a new root
pub const MAX_NEW_NODES: NodeCount = NodeCount(MAX_DEPTH as u32 + 1);

/// A position in a tree
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TreePosition {
    pub leaf: NodeIndex,
    pub index: u32,
}

/// The inner nodes a descent passed through
#[derive(Clone, Copy, Debug)]
pub struct DescentPath {
    pub nodes: [NodeIndex; MAX_DEPTH],
    pub indexes: [u32; MAX_DEPTH],
    pub depth: usize,
}

/// What one slot of a node holds
#[derive(Clone, Copy, Debug)]
pub struct NodeSlot {
    pub prefix: RowPrefix,
    pub address: EntryAddress,
    pub child: NodeIndex,
}

/// A B+ tree whose nodes and rows live in the shard's pools
#[derive(Clone, Copy, Debug)]
pub struct WriteTree {
    pub root: NodeIndex,
    pub first_leaf: NodeIndex,
    pub last_leaf: NodeIndex,
    pub row_count: RowCount,
    pub chain_head: NodeIndex,
    pub chain_tail: NodeIndex,
    pub node_count: NodeCount,
    pub pages: PageChain,
    pub batch_pages: PageChain,
    pub cleanup: Option<TreePosition>,
    pub segment: SegmentNumber,
    /// Slab bytes this tree's rows take for the values they keep, past their plain size
    pub kept_bytes: u64,
}

impl DescentPath {
    /// Start an empty path
    pub fn new() -> Self {
        Self {
            nodes: [NO_NODE; MAX_DEPTH],
            indexes: [0; MAX_DEPTH],
            depth: 0,
        }
    }

    /// Note one step of a descent
    pub fn push(&mut self, node: NodeIndex, index: u32) {
        self.nodes[self.depth] = node;
        self.indexes[self.depth] = index;
        self.depth += 1;
    }
}

impl Default for DescentPath {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteTree {
    /// Start an empty tree
    pub fn empty() -> Self {
        Self {
            root: NO_NODE,
            first_leaf: NO_NODE,
            last_leaf: NO_NODE,
            row_count: RowCount::default(),
            chain_head: NO_NODE,
            chain_tail: NO_NODE,
            node_count: NodeCount::default(),
            pages: PageChain::empty(),
            batch_pages: PageChain::empty(),
            cleanup: None,
            segment: SegmentNumber(0),
            kept_bytes: 0,
        }
    }

    /// Give every node and page back to the pools
    pub fn clear(&mut self, nodes: &mut NodePool, slab: &mut EntrySlab) {
        nodes.release(self.chain_head, self.chain_tail, self.node_count);
        slab.release(&mut self.pages);
        slab.release(&mut self.batch_pages);
        *self = Self::empty();
    }

    /// Take a node and remember it in this tree's chain
    pub fn take_node(&mut self, nodes: &mut NodePool, is_leaf: bool) -> Option<NodeIndex> {
        let index = nodes.take(self.chain_head, is_leaf)?;
        if self.chain_tail == NO_NODE {
            self.chain_tail = index;
        }
        self.chain_head = index;
        self.node_count = NodeCount(self.node_count.0 + 1);
        Some(index)
    }

    /// Link a stored row into the tree
    pub fn link(&mut self, nodes: &mut NodePool, slab: &EntrySlab, target: &SeekTarget, address: EntryAddress) {
        self.row_count = RowCount(self.row_count.0 + 1);
        let slot = NodeSlot {
            prefix: target.prefix,
            address,
            child: NO_NODE,
        };

        if self.root == NO_NODE {
            self.start_root(nodes, &slot);
            return;
        }
        if self.append_at_tail(nodes, slab, target, &slot) {
            return;
        }

        let mut path = DescentPath::new();
        let mut node = self.root;
        while !nodes.node(node).is_leaf {
            let index = child_index(nodes, slab, node, target);
            path.push(node, index);
            node = nodes.node(node).children[index as usize];
        }

        let position = lower_bound(nodes, slab, node, target);
        self.insert_into(nodes, &path, node, position, &slot);
    }

    /// Make the first leaf and root
    pub fn start_root(&mut self, nodes: &mut NodePool, slot: &NodeSlot) {
        let Some(leaf) = self.take_node(nodes, true) else {
            return;
        };
        insert_slot(nodes, leaf, 0, slot);
        self.root = leaf;
        self.first_leaf = leaf;
        self.last_leaf = leaf;
    }

    /// Add a row past the last one with no descent, which is the path ordered keys take
    pub fn append_at_tail(
        &mut self,
        nodes: &mut NodePool,
        slab: &EntrySlab,
        target: &SeekTarget,
        slot: &NodeSlot,
    ) -> bool {
        let leaf = nodes.node(self.last_leaf);
        let count = leaf.count as usize;
        if count == NODE_FANOUT || count == 0 {
            return false;
        }
        if compare_entry(slab, leaf.entries[count - 1], leaf.prefixes[count - 1], target) != Ordering::Less {
            return false;
        }

        insert_slot(nodes, self.last_leaf, count as u32, slot);
        true
    }

    /// Put a row in a leaf, and split upward as far as needed
    pub fn insert_into(
        &mut self,
        nodes: &mut NodePool,
        path: &DescentPath,
        leaf: NodeIndex,
        position: u32,
        slot: &NodeSlot,
    ) {
        if nodes.node(leaf).count as usize != NODE_FANOUT {
            insert_slot(nodes, leaf, position, slot);
            refresh_separators(nodes, path, leaf);
            return;
        }

        let Some(right) = self.split_leaf(nodes, leaf, position) else {
            return;
        };
        let left_count = nodes.node(leaf).count;
        if position < left_count {
            insert_slot(nodes, leaf, position, slot);
        } else {
            insert_slot(nodes, right, position - left_count, slot);
        }
        refresh_separators(nodes, path, leaf);
        self.add_to_parent(nodes, path, leaf, right);
    }

    /// Split a full leaf, leaving the left one full when rows arrive at the tail
    pub fn split_leaf(&mut self, nodes: &mut NodePool, leaf: NodeIndex, position: u32) -> Option<NodeIndex> {
        let right = self.take_node(nodes, true)?;
        let is_tail_append = leaf == self.last_leaf && position as usize == NODE_FANOUT;
        let keep = if is_tail_append { NODE_FANOUT } else { NODE_FANOUT / 2 };

        move_upper(nodes, leaf, right, keep);
        let next = nodes.node(leaf).next;
        nodes.node_mut(right).next = next;
        nodes.node_mut(right).previous = leaf;
        nodes.node_mut(leaf).next = right;
        if next == NO_NODE {
            self.last_leaf = right;
        } else {
            nodes.node_mut(next).previous = right;
        }
        Some(right)
    }

    /// Link a new right sibling into the parent, splitting parents as needed
    pub fn add_to_parent(&mut self, nodes: &mut NodePool, path: &DescentPath, left: NodeIndex, right: NodeIndex) {
        let mut left = left;
        let mut right = right;
        let mut level = path.depth;

        loop {
            if level == 0 {
                self.grow_root(nodes, left, right);
                return;
            }
            level -= 1;
            let parent = path.nodes[level];
            let position = path.indexes[level] + 1;
            let slot = NodeSlot {
                prefix: nodes.node(right).prefixes[0],
                address: nodes.node(right).entries[0],
                child: right,
            };

            if nodes.node(parent).count as usize != NODE_FANOUT {
                insert_slot(nodes, parent, position, &slot);
                return;
            }

            let Some(sibling) = self.take_node(nodes, false) else {
                return;
            };
            move_upper(nodes, parent, sibling, NODE_FANOUT / 2);
            let kept = nodes.node(parent).count;
            if position <= kept {
                insert_slot(nodes, parent, position, &slot);
            } else {
                insert_slot(nodes, sibling, position - kept, &slot);
            }
            left = parent;
            right = sibling;
        }
    }

    /// Put a new root over two nodes
    pub fn grow_root(&mut self, nodes: &mut NodePool, left: NodeIndex, right: NodeIndex) {
        let Some(root) = self.take_node(nodes, false) else {
            return;
        };
        for (position, child) in [left, right].into_iter().enumerate() {
            let slot = NodeSlot {
                prefix: nodes.node(child).prefixes[0],
                address: nodes.node(child).entries[0],
                child,
            };
            insert_slot(nodes, root, position as u32, &slot);
        }
        self.root = root;
    }

    /// Find the first row at or after the target
    pub fn seek(&self, nodes: &NodePool, slab: &EntrySlab, target: &SeekTarget) -> Option<TreePosition> {
        if self.root == NO_NODE {
            return None;
        }

        let mut node = self.root;
        while !nodes.node(node).is_leaf {
            let index = child_index(nodes, slab, node, target);
            node = nodes.node(node).children[index as usize];
        }

        let index = lower_bound(nodes, slab, node, target);
        normalize(nodes, TreePosition { leaf: node, index })
    }

    /// Find the last row before the target
    pub fn seek_before(&self, nodes: &NodePool, slab: &EntrySlab, target: &SeekTarget) -> Option<TreePosition> {
        match self.seek(nodes, slab, target) {
            Some(position) => step_back(nodes, position),
            None => self.last_position(nodes),
        }
    }

    /// Find the first row of the tree
    pub fn first_position(&self, nodes: &NodePool) -> Option<TreePosition> {
        if self.first_leaf == NO_NODE {
            return None;
        }
        let position = TreePosition {
            leaf: self.first_leaf,
            index: 0,
        };
        normalize(nodes, position)
    }

    /// Find the last row of the tree
    pub fn last_position(&self, nodes: &NodePool) -> Option<TreePosition> {
        if self.last_leaf == NO_NODE {
            return None;
        }
        let count = nodes.node(self.last_leaf).count;
        if count == 0 {
            return None;
        }
        Some(TreePosition {
            leaf: self.last_leaf,
            index: count - 1,
        })
    }
}

/// Copy a leaf's first row up the path, since an insert at its front moves it
pub fn refresh_separators(nodes: &mut NodePool, path: &DescentPath, leaf: NodeIndex) {
    let prefix = nodes.node(leaf).prefixes[0];
    let address = nodes.node(leaf).entries[0];
    for level in (0..path.depth).rev() {
        let index = path.indexes[level] as usize;
        let parent = nodes.node_mut(path.nodes[level]);
        parent.prefixes[index] = prefix;
        parent.entries[index] = address;
        if index != 0 {
            return;
        }
    }
}

/// Read the row at a position
pub fn entry_at(nodes: &NodePool, position: TreePosition) -> EntryAddress {
    nodes.node(position.leaf).entries[position.index as usize]
}

/// Move to the next row
pub fn step_forward(nodes: &NodePool, position: TreePosition) -> Option<TreePosition> {
    let next = TreePosition {
        leaf: position.leaf,
        index: position.index + 1,
    };
    normalize(nodes, next)
}

/// Move to the previous row
pub fn step_back(nodes: &NodePool, position: TreePosition) -> Option<TreePosition> {
    if position.index > 0 {
        return Some(TreePosition {
            leaf: position.leaf,
            index: position.index - 1,
        });
    }

    let previous = nodes.node(position.leaf).previous;
    if previous == NO_NODE {
        return None;
    }
    Some(TreePosition {
        leaf: previous,
        index: nodes.node(previous).count.saturating_sub(1),
    })
}

/// Carry a position past a leaf's end into the next leaf
pub fn normalize(nodes: &NodePool, position: TreePosition) -> Option<TreePosition> {
    let leaf = nodes.node(position.leaf);
    if position.index < leaf.count {
        return Some(position);
    }
    if leaf.next == NO_NODE {
        return None;
    }
    Some(TreePosition {
        leaf: leaf.next,
        index: 0,
    })
}

/// Compare a stored row with a target, going to the key bytes only on a prefix tie
pub fn compare_entry(slab: &EntrySlab, address: EntryAddress, prefix: RowPrefix, target: &SeekTarget) -> Ordering {
    match prefix.cmp(&target.prefix) {
        Ordering::Equal => compare_rows(slab.column(address), slab.key(address), slab.place(address), target),
        other => other,
    }
}

/// Find the first slot of a node at or after the target
pub fn lower_bound(nodes: &NodePool, slab: &EntrySlab, node: NodeIndex, target: &SeekTarget) -> u32 {
    let tree_node = nodes.node(node);
    let mut low = 0;
    let mut high = tree_node.count as usize;
    while low < high {
        let middle = (low + high) / 2;
        let entry = tree_node.entries[middle];
        if compare_entry(slab, entry, tree_node.prefixes[middle], target) == Ordering::Less {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    low as u32
}

/// Pick the child of an inner node that could hold the target
pub fn child_index(nodes: &NodePool, slab: &EntrySlab, node: NodeIndex, target: &SeekTarget) -> u32 {
    let position = lower_bound(nodes, slab, node, target);
    let tree_node = nodes.node(node);
    if position < tree_node.count {
        let index = position as usize;
        let entry = tree_node.entries[index];
        if compare_entry(slab, entry, tree_node.prefixes[index], target) == Ordering::Equal {
            return position;
        }
    }
    position.saturating_sub(1)
}

/// Open a slot in a node and fill it
pub fn insert_slot(nodes: &mut NodePool, node: NodeIndex, position: u32, slot: &NodeSlot) {
    let tree_node = nodes.node_mut(node);
    let index = position as usize;
    let count = tree_node.count as usize;
    tree_node.prefixes.copy_within(index..count, index + 1);
    tree_node.entries.copy_within(index..count, index + 1);
    tree_node.children.copy_within(index..count, index + 1);
    tree_node.prefixes[index] = slot.prefix;
    tree_node.entries[index] = slot.address;
    tree_node.children[index] = slot.child;
    tree_node.count += 1;
    tree_node.version = tree_node.version.wrapping_add(1);
}

/// Move a node's slots past a count into an empty sibling
pub fn move_upper(nodes: &mut NodePool, from: NodeIndex, to: NodeIndex, keep: usize) {
    let source = *nodes.node(from);
    let moved = source.count as usize - keep;

    let target = nodes.node_mut(to);
    target.prefixes[..moved].copy_from_slice(&source.prefixes[keep..keep + moved]);
    target.entries[..moved].copy_from_slice(&source.entries[keep..keep + moved]);
    target.children[..moved].copy_from_slice(&source.children[keep..keep + moved]);
    target.count = moved as u32;

    let source = nodes.node_mut(from);
    source.count = keep as u32;
    source.version = source.version.wrapping_add(1);
}
