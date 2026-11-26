//! Tree nodes, in a room that comes into memory as the write index fills

use reel_core::units::ByteCount;

use std::io::Result;
use std::sync::Arc;

use crate::pool::touched_room;
use crate::pool_room::PoolRoom;
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

/// The bytes of one page of nodes, a whole number of which fills any extent a room cuts
pub const NODE_PAGE_BYTES: usize = 4096;

/// The reserve job's steps a pool stays wholly free before it gives its pages back, so a busy shard that empties at each seal keeps
/// them and its cursors
pub const IDLE_STEPS: u32 = 50;

/// Nodes on one page
pub const NODES_PER_PAGE: u32 = (NODE_PAGE_BYTES / size_of::<TreeNode>()) as u32;

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

/// A shard's nodes, in pages of a room the reserve job grows, several nodes a page. The pool keeps its own list of unused nodes as
/// before, takes a page from the room when the list runs dry, and gives every page back once no node is in use, which a quiet
/// shard reaches after its seals
#[derive(Debug)]
pub struct NodePool {
    room: Arc<PoolRoom>,
    free_head: NodeIndex,
    /// Unused nodes on the pages the pool holds
    free_count: NodeCount,
    /// The pages the pool took from the room, to give back whole
    pages: Vec<u32>,
    /// Past every version a node of the pool ever had, taken when the pool last gave its pages back: a page that comes back
    /// zeroed starts its nodes past it, so a cursor that noted a version there never sees it again
    version_floor: u32,
    /// Times the pool gave every page back, which a cursor compares before it reads a leaf
    resets: u32,
    /// The reserve job's steps in a row that found every node free
    idle_steps: u32,
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
    /// Reserve room for this many nodes, with the pages for the first ones in memory and the rest for the reserve job to bring in
    pub fn growing(capacity: NodeCount, first: NodeCount) -> Result<Self> {
        let pages = |count: NodeCount| ByteCount(u64::from(count.0.div_ceil(NODES_PER_PAGE)) * NODE_PAGE_BYTES as u64);
        let room = PoolRoom::reserve_paged(pages(capacity), pages(NodeCount(first.0.min(capacity.0))), NODE_PAGE_BYTES)?;
        let most = room.most_pages() as usize;
        Ok(Self { room: Arc::new(room), free_head: NO_NODE, free_count: NodeCount(0), pages: touched_room(most), version_floor: 0, resets: 0, idle_steps: 0 })
    }

    /// Read the room, which the growing job raises
    pub fn room(&self) -> &Arc<PoolRoom> {
        &self.room
    }

    fn address(&self, index: NodeIndex) -> *mut TreeNode {
        let (page, slot) = (index.0 / NODES_PER_PAGE, index.0 % NODES_PER_PAGE);
        // SAFETY: a node index the pool made names a slot of a page inside the room's reservation
        unsafe { self.room.base().add(page as usize * NODE_PAGE_BYTES + slot as usize * size_of::<TreeNode>()).cast::<TreeNode>() }
    }

    /// Read a node
    pub fn node(&self, index: NodeIndex) -> &TreeNode {
        // SAFETY: a node handed out sits on a page the pool holds, in memory until every node is free again
        unsafe { &*self.address(index) }
    }

    /// Change a node
    pub fn node_mut(&mut self, index: NodeIndex) -> &mut TreeNode {
        // SAFETY: as for node, and the pool is borrowed whole
        unsafe { &mut *self.address(index) }
    }

    /// Count one of the reserve job's steps that found every node free, and give every page back once the pool has stayed free
    /// for IDLE_STEPS of them, or at once when pressed. The job runs this under the shard's write lock, on the thread that grows the
    /// room, so no take, no growth and no cursor's read is under way. Say whether the pages went back
    pub fn step_idle(&mut self, is_pressed: bool) -> bool {
        let is_free = !self.pages.is_empty() && self.free_count.0 as usize == self.pages.len() * NODES_PER_PAGE as usize;
        if !is_free {
            self.idle_steps = 0;
            return false;
        }
        self.idle_steps += 1;
        if !is_pressed && self.idle_steps < IDLE_STEPS {
            return false;
        }
        // past every version a node here had, so a page that comes back zeroed starts its nodes past any a cursor noted
        let mut floor = self.version_floor;
        for at in 0..self.pages.len() {
            let page = self.pages[at];
            for slot in 0..NODES_PER_PAGE {
                floor = floor.max(self.node(NodeIndex(page * NODES_PER_PAGE + slot)).version);
            }
        }
        self.version_floor = floor;
        self.resets = self.resets.wrapping_add(1);
        for page in self.pages.drain(..) {
            self.room.give_page(page);
        }
        self.free_head = NO_NODE;
        self.free_count = NodeCount(0);
        self.idle_steps = 0;
        true
    }

    /// Read how many times the pool gave every page back
    pub fn resets(&self) -> u32 {
        self.resets
    }

    /// Read the heap this pool holds: the pages it took and the ones in memory free for it, and the list of its pages
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount(u64::from(self.room.ready_pages()) * NODE_PAGE_BYTES as u64 + (self.pages.capacity() * size_of::<u32>()) as u64)
    }

    /// Count the nodes it can take with no growth: those unused on its pages and those of the pages in memory it hasn't taken
    pub fn free_count(&self) -> NodeCount {
        NodeCount(self.free_count.0 + self.room.free_pages() * NODES_PER_PAGE)
    }

    /// Count the nodes in memory: those on the pages it took and those of the pages in memory it hasn't taken
    pub fn in_memory(&self) -> NodeCount {
        NodeCount((self.pages.len() as u32 + self.room.free_pages()) * NODES_PER_PAGE)
    }

    /// Count the nodes the pool can grow to
    pub fn capacity(&self) -> NodeCount {
        NodeCount(self.room.most_pages() * NODES_PER_PAGE)
    }

    /// Check the job can still bring nodes in
    pub fn can_grow(&self) -> bool {
        self.room.ready_pages() < self.room.most_pages() && self.room.can_grow()
    }

    /// Ask the job for the next step, for a writer that found too few nodes in memory
    pub fn note_short(&self) {
        self.room.note_wanted();
    }

    /// Take an unused node and link it into a tree's chain, or none when no page is in memory, which asks the job for more
    pub fn take(&mut self, chain_head: NodeIndex, is_leaf: bool) -> Option<NodeIndex> {
        if self.free_head == NO_NODE {
            let Some(page) = self.room.take_page() else {
                self.room.note_wanted();
                return None;
            };
            self.pages.push(page);
            for slot in (0..NODES_PER_PAGE).rev() {
                let index = NodeIndex(page * NODES_PER_PAGE + slot);
                let next = self.free_head;
                self.node_mut(index).chain = next;
                self.free_head = index;
            }
            self.free_count = NodeCount(self.free_count.0 + NODES_PER_PAGE);
        }
        let index = self.free_head;
        self.free_head = self.node(index).chain;
        self.free_count = NodeCount(self.free_count.0 - 1);
        // a node goes on from its own last version, as it did when every node stayed in memory, and one on a page that came back
        // zeroed goes on from past every version the pool gave
        let version = self.node(index).version.max(self.version_floor).wrapping_add(1);
        let node = self.node_mut(index);
        *node = TreeNode::empty();
        node.version = version;
        node.is_leaf = is_leaf;
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
