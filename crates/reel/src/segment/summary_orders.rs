//! Orders over a volume's summary rows, kept as each row changes, so a pick asks them and walks no other row

use reel_api::units::ByteCount;

use crate::segment::segment_table::NumberMap;
use crate::segment::segment_tail::SegmentNumber;
use crate::segment::summary::SummaryRow;

/// Bytes a footer row takes, about, so a rewrite weighs rows it drops beside records. A segment of graves holds rows and no records
pub const ROW_FOOTER_BYTES: u64 = 41;
/// The whole of a share, for comparing what segments win back
pub const SHARE_SCALE: u64 = 1_000_000;
/// Buckets a walked row's reclaim share falls in, each a sixty-fourth of the scale
pub const SHARE_BUCKETS: usize = 64;
/// The list of walked rows with covers, which a rewrite takes for the covers whatever their share
pub const COVERS_LIST: usize = SHARE_BUCKETS;
/// Bytes the orders take for each row they have room for: its entry, its places in the order and the free list, and its place
/// in the slot map at that map's worst rounding
pub const ORDER_BYTES: u64 = (size_of::<OrderEntry>() + size_of::<SegmentNumber>() + size_of::<u32>() + 4 * (size_of::<SegmentNumber>() + size_of::<u32>())) as u64;
/// No entry, at a list's ends
pub const NO_ENTRY: u32 = u32::MAX;
/// No list
pub const NO_LIST: u8 = u8::MAX;
/// The share buckets and the covers list
const SHARE_LISTS: usize = SHARE_BUCKETS + 1;
/// The lists a row's walk puts it on
const WALK_LISTS: usize = 3;
/// The link toward a list's first entry, and a list's first entry among its ends
const PREVIOUS: usize = 0;
/// The link toward a list's last entry, and a list's last entry among its ends
const NEXT: usize = 1;

/// The list a row's walk puts it on, beside its share bucket
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalkList {
    /// A row a forward walk hasn't finished
    Unwalked,
    /// A row a backward walk rebuilds
    Backward,
    /// A walked row whose every row is dead, for a whole unlink
    Dead,
}

/// Which pair of links a list goes through
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Links {
    Share,
    Walk,
}

/// One row's place in the lists
#[derive(Clone, Copy, Debug)]
struct OrderEntry {
    /// The row's segment
    segment: SegmentNumber,
    /// The share bucket or covers list the row is on, or none
    share_list: u8,
    /// The walk list the row is on, or none
    walk_list: u8,
    /// The entries before and after it on its share list
    share_links: [u32; 2],
    /// The entries before and after it on its walk list
    walk_links: [u32; 2],
    /// The dead bytes its row counts
    dead_bytes: u64,
}

/// The lists and the segment order, with room fixed when the summary sizes its lookup
#[derive(Debug)]
pub struct SummaryOrders {
    /// Each row's entry, by slot
    entries: Vec<OrderEntry>,
    /// Slots no row holds
    free: Vec<u32>,
    /// Each row's slot, by segment
    slots: NumberMap,
    /// Every row's segment, in order
    present: Vec<SegmentNumber>,
    /// Each share list's first and last entry
    share_ends: [[u32; 2]; SHARE_LISTS],
    /// Each walk list's first and last entry
    walk_ends: [[u32; 2]; WALK_LISTS],
    /// How many rows each walk list holds
    walk_counts: [usize; WALK_LISTS],
    /// Rows that found no slot, which no pick sees
    left_out: u64,
    /// The dead bytes every row counts, for the quiet volume's line
    dead_total: u64,
}

/// The segments on one list, first to last
#[derive(Clone, Copy, Debug)]
pub struct ListWalk<'orders> {
    orders: &'orders SummaryOrders,
    links: Links,
    next: u32,
}

impl Iterator for ListWalk<'_> {
    type Item = SegmentNumber;

    fn next(&mut self) -> Option<SegmentNumber> {
        let entry = self.orders.entries.get(self.next as usize)?;
        self.next = self.orders.link(self.links, self.next)[NEXT];
        Some(entry.segment)
    }
}

impl WalkList {
    /// Read the list's place
    fn index(self) -> usize {
        match self {
            Self::Unwalked => 0,
            Self::Backward => 1,
            Self::Dead => 2,
        }
    }
}

impl SummaryOrders {
    /// Allocate room for a number of rows
    pub fn new(room: usize) -> Self {
        let empty = OrderEntry { segment: SegmentNumber(0), share_list: NO_LIST, walk_list: NO_LIST, share_links: [NO_ENTRY; 2], walk_links: [NO_ENTRY; 2], dead_bytes: 0 };
        Self {
            entries: vec![empty; room],
            free: (0..room as u32).rev().collect(),
            slots: NumberMap::new(room),
            present: Vec::with_capacity(room),
            share_ends: [[NO_ENTRY; 2]; SHARE_LISTS],
            walk_ends: [[NO_ENTRY; 2]; WALK_LISTS],
            walk_counts: [0; WALK_LISTS],
            left_out: 0,
            dead_total: 0,
        }
    }

    /// Forget every row, keeping the room
    pub fn clear(&mut self) {
        self.slots.clear();
        self.present.clear();
        self.free.clear();
        self.free.extend((0..self.entries.len() as u32).rev());
        self.share_ends = [[NO_ENTRY; 2]; SHARE_LISTS];
        self.walk_ends = [[NO_ENTRY; 2]; WALK_LISTS];
        self.walk_counts = [0; WALK_LISTS];
        self.left_out = 0;
        self.dead_total = 0;
    }

    /// Read the heap the orders hold
    pub fn heap_bytes(&self) -> ByteCount {
        let entries = self.entries.capacity() * size_of::<OrderEntry>();
        let present = self.present.capacity() * size_of::<SegmentNumber>() + self.free.capacity() * size_of::<u32>();
        ByteCount((entries + present + self.slots.heap_bytes()) as u64)
    }

    /// Count the rows that found no slot. The room is the table's segments, the open slots and the damaged spans, and a full
    /// table stops a seal before its row comes, so a store keeps this at none. A row left out still serves reads, and no pick
    /// sees it: no walk, rewrite or unlink takes its segment
    pub fn left_out(&self) -> u64 {
        self.left_out
    }

    /// Read the dead bytes every row counts
    pub fn dead_bytes(&self) -> u64 {
        self.dead_total
    }

    /// Count the rows
    pub fn len(&self) -> usize {
        self.present.len()
    }

    /// Check no row is held
    pub fn is_empty(&self) -> bool {
        self.present.is_empty()
    }

    /// Give a row its place, or move it to the lists its counts now call for
    pub fn place(&mut self, row: &SummaryRow<'_>) {
        // orders with no room are a summary's before it sizes its lookup, which builds them then
        if self.entries.is_empty() {
            return;
        }
        let segment = row.facts.segment;
        let (share_list, walk_list) = lists_of(row);
        let slot = match self.slots.get(segment) {
            Some(slot) => slot,
            None => {
                let Some(slot) = self.free.pop() else {
                    self.left_out += 1;
                    return;
                };
                self.slots.insert(segment, slot);
                let at = self.present.partition_point(|present| *present < segment);
                self.present.insert(at, segment);
                self.entries[slot as usize] =
                    OrderEntry { segment, share_list: NO_LIST, walk_list: NO_LIST, share_links: [NO_ENTRY; 2], walk_links: [NO_ENTRY; 2], dead_bytes: 0 };
                slot
            },
        };
        let dead = row_dead_bytes(row);
        self.dead_total = self.dead_total - self.entries[slot as usize].dead_bytes + dead;
        self.entries[slot as usize].dead_bytes = dead;
        let entry = self.entries[slot as usize];
        if entry.share_list != share_list {
            self.unlink(Links::Share, slot);
            if share_list != NO_LIST {
                self.link_last(Links::Share, usize::from(share_list), slot);
            }
        }
        if entry.walk_list != walk_list {
            self.unlink(Links::Walk, slot);
            if walk_list != NO_LIST {
                self.link_in_order(usize::from(walk_list), slot);
            }
        }
    }

    /// Take a row out of every list and the order
    pub fn forget(&mut self, segment: SegmentNumber) {
        let Some(slot) = self.slots.get(segment) else {
            return;
        };
        self.dead_total -= self.entries[slot as usize].dead_bytes;
        self.entries[slot as usize].dead_bytes = 0;
        self.unlink(Links::Share, slot);
        self.unlink(Links::Walk, slot);
        self.slots.remove(segment);
        if let Ok(at) = self.present.binary_search(&segment) {
            self.present.remove(at);
        }
        self.free.push(slot);
    }

    /// Walk a share bucket or the covers list, first to last
    pub fn share_list(&self, list: usize) -> ListWalk<'_> {
        ListWalk { orders: self, links: Links::Share, next: self.share_ends[list][PREVIOUS] }
    }

    /// Move a row to the end of its share list, so the next pick there starts with another
    pub fn rotate(&mut self, segment: SegmentNumber) {
        let Some(slot) = self.slots.get(segment) else {
            return;
        };
        let list = self.entries[slot as usize].share_list;
        if list != NO_LIST {
            self.unlink(Links::Share, slot);
            self.link_last(Links::Share, usize::from(list), slot);
        }
    }

    /// Walk a walk list in segment order
    pub fn walk_list(&self, list: WalkList) -> ListWalk<'_> {
        ListWalk { orders: self, links: Links::Walk, next: self.walk_ends[list.index()][PREVIOUS] }
    }

    /// Count a walk list's rows
    pub fn walk_count(&self, list: WalkList) -> usize {
        self.walk_counts[list.index()]
    }

    /// Read every row's segment, in order
    pub fn present(&self) -> &[SegmentNumber] {
        &self.present
    }

    /// Read the segments past one, in order
    pub fn after(&self, segment: SegmentNumber) -> &[SegmentNumber] {
        &self.present[self.present.partition_point(|present| *present <= segment)..]
    }

    /// Read the segments below one, in order
    pub fn below(&self, segment: SegmentNumber) -> &[SegmentNumber] {
        &self.present[..self.present.partition_point(|present| *present < segment)]
    }

    /// Read an entry's links of one kind
    fn link(&self, links: Links, slot: u32) -> [u32; 2] {
        let entry = &self.entries[slot as usize];
        match links {
            Links::Share => entry.share_links,
            Links::Walk => entry.walk_links,
        }
    }

    /// Change an entry's links of one kind, and the list it is on
    fn set_link(&mut self, links: Links, slot: u32, pair: [u32; 2], list: u8) {
        let entry = &mut self.entries[slot as usize];
        match links {
            Links::Share => {
                entry.share_links = pair;
                entry.share_list = list;
            },
            Links::Walk => {
                entry.walk_links = pair;
                entry.walk_list = list;
            },
        }
    }

    /// Point a neighbour's link of one side at another entry, or a list's end when there is no neighbour
    fn point(&mut self, links: Links, list: usize, neighbour: u32, side: usize, to: u32) {
        if neighbour == NO_ENTRY {
            let ends = match links {
                Links::Share => &mut self.share_ends[list],
                Links::Walk => &mut self.walk_ends[list],
            };
            // a missing neighbour after an entry means the entry is the list's last, and before it the list's first
            ends[1 - side] = to;
            return;
        }
        let entry = &mut self.entries[neighbour as usize];
        match links {
            Links::Share => entry.share_links[side] = to,
            Links::Walk => entry.walk_links[side] = to,
        }
    }

    /// Take an entry off the list it is on
    fn unlink(&mut self, links: Links, slot: u32) {
        let entry = self.entries[slot as usize];
        let list = match links {
            Links::Share => entry.share_list,
            Links::Walk => entry.walk_list,
        };
        if list == NO_LIST {
            return;
        }
        let list = usize::from(list);
        let [previous, next] = self.link(links, slot);
        self.point(links, list, previous, NEXT, next);
        self.point(links, list, next, PREVIOUS, previous);
        self.set_link(links, slot, [NO_ENTRY; 2], NO_LIST);
        if links == Links::Walk {
            self.walk_counts[list] -= 1;
        }
    }

    /// Put an entry at the end of a list
    fn link_last(&mut self, links: Links, list: usize, slot: u32) {
        let last = match links {
            Links::Share => self.share_ends[list][NEXT],
            Links::Walk => self.walk_ends[list][NEXT],
        };
        self.link_between(links, list, slot, last, NO_ENTRY);
    }

    /// Put an entry on a walk list in segment order, looking back from the end, where a new segment lands
    fn link_in_order(&mut self, list: usize, slot: u32) {
        let segment = self.entries[slot as usize].segment;
        let mut after = self.walk_ends[list][NEXT];
        while after != NO_ENTRY && self.entries[after as usize].segment > segment {
            after = self.entries[after as usize].walk_links[PREVIOUS];
        }
        let before = if after == NO_ENTRY { self.walk_ends[list][PREVIOUS] } else { self.entries[after as usize].walk_links[NEXT] };
        self.link_between(Links::Walk, list, slot, after, before);
        self.walk_counts[list] += 1;
    }

    /// Put an entry on a list between two neighbours, either of which may be a list's end
    fn link_between(&mut self, links: Links, list: usize, slot: u32, previous: u32, next: u32) {
        self.set_link(links, slot, [previous, next], list as u8);
        self.point(links, list, previous, NEXT, slot);
        self.point(links, list, next, PREVIOUS, slot);
    }
}

/// Work out the share list and walk list a row belongs on
pub fn lists_of(row: &SummaryRow<'_>) -> (u8, u8) {
    let facts = &row.facts;
    if !facts.is_walked() {
        let walk = if facts.is_backward { WalkList::Backward } else { WalkList::Unwalked };
        return (NO_LIST, walk.index() as u8);
    }
    let walk = if is_wholly_dead(row) { WalkList::Dead.index() as u8 } else { NO_LIST };
    if facts.is_damaged || facts.block_count == 0 {
        return (NO_LIST, walk);
    }
    if facts.has_covers {
        return (COVERS_LIST as u8, walk);
    }
    (share_bucket(reclaim_share(row)) as u8, walk)
}

/// Read the bucket a share falls in
pub fn share_bucket(share: u64) -> usize {
    (share.min(SHARE_SCALE) * SHARE_BUCKETS as u64 / (SHARE_SCALE + 1)) as usize
}

/// Read what a rewrite of a segment wins back, as a share of its record and footer bytes
pub fn reclaim_share(row: &SummaryRow<'_>) -> u64 {
    let (mut rows, mut dead_rows, mut bytes, mut dead_bytes) = (0, 0, 0, 0);
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let Some(column) = cursor.column() else {
            break;
        };
        rows += column.row_count.0;
        dead_rows += column.dead_rows.0 + column.grave_count.0.saturating_sub(column.dead_graves.0);
        bytes += column.live_bytes.0 + column.dead_bytes.0;
        dead_bytes += column.dead_bytes.0;
    }
    SHARE_SCALE * (dead_bytes + dead_rows * ROW_FOOTER_BYTES) / (bytes + rows * ROW_FOOTER_BYTES).max(1)
}

/// Check a row's counts say nothing in its segment is live. Rows with damage or covers answer for more than their rows
pub fn is_wholly_dead(row: &SummaryRow<'_>) -> bool {
    if !row.facts.is_walked() || row.facts.is_damaged || row.facts.has_covers || row.facts.block_count == 0 {
        return false;
    }
    let mut cursor = row.cursor();
    (0..row.column_count).all(|_column| cursor.column().is_some_and(|column| column.dead_rows == column.row_count))
}

/// Add up the dead bytes a row's columns count
pub fn row_dead_bytes(row: &SummaryRow<'_>) -> u64 {
    let mut cursor = row.cursor();
    (0..row.column_count).filter_map(|_column| cursor.column()).map(|column| column.dead_bytes.0).sum()
}
