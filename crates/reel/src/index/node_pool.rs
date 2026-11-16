//! Fixed tree nodes, all allocated and touched at open

use reel_core::units::ByteCount;

use crate::pool::touched_filled;
use crate::index::entry_slab::{EntryAddress, NO_ENTRY};
use crate::index::seek_target::RowPrefix;

/// Most slots in one node
pub const NODE_FANOUT: usize = 32;

/// The id of one node
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct NodeIndex(pub u32);

/// Marks a missing node
pub const NO_NODE: NodeIndex = NodeIndex(u32::MAX);

/// A count of nodes
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeCount(pub u32);

/// One tree node: a leaf holds rows, and an inner node holds children with their first rows
#[derive(Clone, Copy, Debug)]
pub struct TreeNode {
    pub count: u32,
    pub version: u32,
    pub is_leaf: bool,
    pub next: NodeIndex,
    pub previous: NodeIndex,
    pub chain: NodeIndex,
    pub prefixes: [RowPrefix; NODE_FANOUT],
    pub entries: [EntryAddress; NODE_FANOUT],
    pub children: [NodeIndex; NODE_FANOUT],
}

/// A shard's nodes and its list of unused ones
#[derive(Debug)]
pub struct NodePool {
    pub nodes: Vec<TreeNode>,
    pub free_head: NodeIndex,
    pub free_count: NodeCount,
}

impl TreeNode {
    /// Build a node with nothing in it
    pub fn empty() -> Self {
        Self {
            count: 0,
            version: 0,
            is_leaf: true,
            next: NO_NODE,
            previous: NO_NODE,
            chain: NO_NODE,
            prefixes: [RowPrefix(u64::MAX); NODE_FANOUT],
            entries: [NO_ENTRY; NODE_FANOUT],
            children: [NO_NODE; NODE_FANOUT],
        }
    }
}

impl NodePool {
    /// Allocate and touch every node
    pub fn new(capacity: NodeCount) -> Self {
        let mut nodes = touched_filled(capacity.0 as usize, TreeNode::empty());
        for (index, node) in nodes.iter_mut().enumerate() {
            let next = index as u32 + 1;
            node.chain = if next == capacity.0 { NO_NODE } else { NodeIndex(next) };
        }

        Self {
            nodes,
            free_head: if capacity.0 == 0 { NO_NODE } else { NodeIndex(0) },
            free_count: capacity,
        }
    }

    /// Read a node
    pub fn node(&self, index: NodeIndex) -> &TreeNode {
        &self.nodes[index.0 as usize]
    }

    /// Change a node
    pub fn node_mut(&mut self, index: NodeIndex) -> &mut TreeNode {
        &mut self.nodes[index.0 as usize]
    }

    /// Read the heap this pool holds
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount((self.nodes.capacity() * size_of::<TreeNode>()) as u64)
    }

    /// Take an unused node and link it into a tree's chain
    pub fn take(&mut self, chain_head: NodeIndex, is_leaf: bool) -> Option<NodeIndex> {
        let index = self.free_head;
        if index == NO_NODE {
            return None;
        }

        self.free_head = self.node(index).chain;
        self.free_count = NodeCount(self.free_count.0 - 1);
        let node = self.node_mut(index);
        node.count = 0;
        node.version = node.version.wrapping_add(1);
        node.is_leaf = is_leaf;
        node.next = NO_NODE;
        node.previous = NO_NODE;
        node.chain = chain_head;
        Some(index)
    }

    /// Give a tree's whole chain back in one splice
    pub fn release(&mut self, chain_head: NodeIndex, chain_tail: NodeIndex, count: NodeCount) {
        if chain_head == NO_NODE {
            return;
        }
        self.node_mut(chain_tail).chain = self.free_head;
        self.free_head = chain_head;
        self.free_count = NodeCount(self.free_count.0 + count.0);
    }
}
