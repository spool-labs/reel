//! Fixed pages that hold rows with their keys inline

use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, FileOffset, WritePlace};

use crate::index::open_slots::OpenSlot;
use crate::index::write_row::WriteRow;
use crate::pool::{TOUCH_BYTE, touched_filled};
use crate::row_kind::RowKind;
use crate::segment::segment_tail::SegmentNumber;

/// Bytes in one page
pub const PAGE_BYTES: usize = 8 * 1024;
/// Every row starts on a multiple of this
pub const ENTRY_ALIGNMENT: usize = 8;
/// Where the place sits in a row
pub const PLACE_AT: usize = 0;
/// Where the file offset sits in a row
pub const OFFSET_AT: usize = 8;
/// Where the record length sits in a row
pub const LENGTH_AT: usize = 12;
/// Where the link to the next older version sits in a row
pub const OLDER_AT: usize = 16;
/// Where the link to the next newer version sits in a row
pub const NEWER_AT: usize = 20;
/// Where the column sits in a row
pub const COLUMN_AT: usize = 24;
/// Where the kind and flags sit in a row
pub const KIND_AT: usize = 25;
/// Where the key's length sits in a row
pub const KEY_LENGTH_AT: usize = 26;
/// Where the open slot sits in a row
pub const SLOT_AT: usize = 28;
/// Where the key sits in a row
pub const KEY_AT: usize = 29;
/// Set beside the kind when a newer version of this key is in a sealed segment
pub const NEWER_SEALED_FLAG: u8 = 0x80;
/// The bit of a row's kind byte that says the row is in its column's sorted index and out of the open counts
pub const ENTERED_FLAG: u8 = 0x40;
/// The bit of a row's kind byte that says the row keeps its value after its key, a two-byte length and then the bytes
pub const INLINE_FLAG: u8 = 0x20;
/// The longest value a row keeps after its key, so a get of an open row reads it with the row
pub const MOST_INLINE_VALUE: usize = 256;

/// The id of one page
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PageIndex(pub u32);

/// Marks the end of a page chain
pub const NO_PAGE: PageIndex = PageIndex(u32::MAX);

/// A row's position in the slab, in alignment units
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct EntryAddress(pub u32);

/// Marks a missing row
pub const NO_ENTRY: EntryAddress = EntryAddress(u32::MAX);

/// A count of pages
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PageCount(pub u32);

/// The pages one tree holds, as a chain
#[derive(Clone, Copy, Debug)]
pub struct PageChain {
    pub newest: PageIndex,
    pub oldest: PageIndex,
    pub used: usize,
    pub pages: PageCount,
}

/// A shard's pages, all allocated and touched at open
#[derive(Debug)]
pub struct EntrySlab {
    bytes: Vec<u8>,
    next_page: Vec<PageIndex>,
    free_head: PageIndex,
    free_pages: PageCount,
}

impl PageChain {
    /// Start a chain with no pages
    pub fn empty() -> Self {
        Self {
            newest: NO_PAGE,
            oldest: NO_PAGE,
            used: PAGE_BYTES,
            pages: PageCount::default(),
        }
    }
}

impl EntrySlab {
    /// Allocate and touch a whole number of pages
    pub fn new(pages: PageCount) -> Self {
        let mut next_page = Vec::with_capacity(pages.0 as usize);
        for page in 0..pages.0 {
            let next = page.saturating_add(1);
            next_page.push(if next == pages.0 { NO_PAGE } else { PageIndex(next) });
        }

        Self {
            bytes: touched_filled(pages.0 as usize * PAGE_BYTES, TOUCH_BYTE),
            next_page,
            free_head: if pages.0 == 0 { NO_PAGE } else { PageIndex(0) },
            free_pages: pages,
        }
    }

    /// Read the heap this slab holds
    pub fn heap_bytes(&self) -> ByteCount {
        let links = self.next_page.capacity() * size_of::<PageIndex>();
        ByteCount((self.bytes.capacity() + links) as u64)
    }

    /// Check this many pages are free
    pub fn has_free_pages(&self, pages: PageCount) -> bool {
        self.free_pages >= pages
    }

    /// Count every page of this slab
    pub fn total_pages(&self) -> PageCount {
        PageCount(self.next_page.len() as u32)
    }

    /// Check a row of this key length could be stored
    pub fn has_room(&self, chain: &PageChain, key_length: usize, held: PageCount) -> bool {
        self.has_room_for(chain, entry_size(key_length), held)
    }

    /// Check a row of this many bytes could be stored
    pub fn has_room_for(&self, chain: &PageChain, size: usize, held: PageCount) -> bool {
        chain.used + size <= PAGE_BYTES || self.free_pages > held
    }

    /// Store a row and return where it went, or nothing when the pages ran out
    pub fn store(
        &mut self,
        chain: &mut PageChain,
        column: ColumnId,
        key: &[u8],
        row: &WriteRow,
        inline: Option<&[u8]>,
    ) -> Option<EntryAddress> {
        let size = inline.map_or(entry_size(key.len()), |value| inline_entry_size(key.len(), value.len()));
        let start = self.claim(chain, size)?;
        let value_at = start + KEY_AT + key.len();
        if let Some(value) = inline {
            self.bytes[value_at..value_at + INLINE_LENGTH_BYTES].copy_from_slice(&(value.len() as u16).to_le_bytes());
            self.bytes[value_at + INLINE_LENGTH_BYTES..value_at + INLINE_LENGTH_BYTES + value.len()].copy_from_slice(value);
        }
        let bytes = &mut self.bytes[start..value_at];
        bytes[PLACE_AT..OFFSET_AT].copy_from_slice(&row.place.0.to_le_bytes());
        bytes[OFFSET_AT..LENGTH_AT].copy_from_slice(&(row.offset.0 as u32).to_le_bytes());
        bytes[LENGTH_AT..OLDER_AT].copy_from_slice(&(row.length.0 as u32).to_le_bytes());
        bytes[OLDER_AT..NEWER_AT].copy_from_slice(&NO_ENTRY.0.to_le_bytes());
        bytes[NEWER_AT..COLUMN_AT].copy_from_slice(&NO_ENTRY.0.to_le_bytes());
        bytes[COLUMN_AT] = column.0;
        let flag = if row.has_newer_sealed { NEWER_SEALED_FLAG } else { 0 } | if inline.is_some() { INLINE_FLAG } else { 0 };
        bytes[KIND_AT] = row.kind.as_byte() | flag;
        bytes[KEY_LENGTH_AT..SLOT_AT].copy_from_slice(&(key.len() as u16).to_le_bytes());
        bytes[SLOT_AT] = row.slot.0;
        bytes[KEY_AT..].copy_from_slice(key);
        Some(EntryAddress((start / ENTRY_ALIGNMENT) as u32))
    }

    /// Claim bytes in the chain's newest page, taking a free page when it's full
    pub fn claim(&mut self, chain: &mut PageChain, size: usize) -> Option<usize> {
        if chain.used + size > PAGE_BYTES {
            let page = self.free_head;
            if page == NO_PAGE {
                return None;
            }
            self.free_head = self.next_page[page.0 as usize];
            self.free_pages = PageCount(self.free_pages.0 - 1);
            self.next_page[page.0 as usize] = chain.newest;
            if chain.oldest == NO_PAGE {
                chain.oldest = page;
            }
            chain.newest = page;
            chain.used = 0;
            chain.pages = PageCount(chain.pages.0 + 1);
        }

        let start = chain.newest.0 as usize * PAGE_BYTES + chain.used;
        chain.used += size;
        Some(start)
    }

    /// Give a whole chain back in one splice
    pub fn release(&mut self, chain: &mut PageChain) {
        if chain.oldest != NO_PAGE {
            self.next_page[chain.oldest.0 as usize] = self.free_head;
            self.free_head = chain.newest;
            self.free_pages = PageCount(self.free_pages.0 + chain.pages.0);
        }
        *chain = PageChain::empty();
    }

    /// Read the key of a row
    pub fn key(&self, address: EntryAddress) -> &[u8] {
        let start = byte_of(address);
        let length = u16::from_le_bytes([self.bytes[start + KEY_LENGTH_AT], self.bytes[start + KEY_LENGTH_AT + 1]]);
        &self.bytes[start + KEY_AT..start + KEY_AT + length as usize]
    }

    /// Read the column of a row
    pub fn column(&self, address: EntryAddress) -> ColumnId {
        ColumnId(self.bytes[byte_of(address) + COLUMN_AT])
    }

    /// Read the place of a row
    pub fn place(&self, address: EntryAddress) -> WritePlace {
        WritePlace(self.word(byte_of(address) + PLACE_AT))
    }

    /// Read the open slot of a row
    pub fn slot(&self, address: EntryAddress) -> OpenSlot {
        OpenSlot(self.bytes[byte_of(address) + SLOT_AT])
    }

    /// Read a whole row, or nothing when its kind byte is unknown
    pub fn row(&self, address: EntryAddress) -> Option<WriteRow> {
        let start = byte_of(address);
        Some(WriteRow {
            place: WritePlace(self.word(start + PLACE_AT)),
            offset: FileOffset(u64::from(self.half(start + OFFSET_AT))),
            length: ByteCount(u64::from(self.half(start + LENGTH_AT))),
            kind: RowKind::from_byte(self.bytes[start + KIND_AT] & !(NEWER_SEALED_FLAG | ENTERED_FLAG | INLINE_FLAG))?,
            slot: self.slot(address),
            segment: SegmentNumber(0),
            has_newer_sealed: self.has_newer_sealed(address),
        })
    }

    /// Read the value a row keeps after its key, when it keeps one
    pub fn inline_value(&self, address: EntryAddress) -> Option<&[u8]> {
        let start = byte_of(address);
        if self.bytes[start + KIND_AT] & INLINE_FLAG == 0 {
            return None;
        }
        let at = start + KEY_AT + usize::from(u16::from_le_bytes([self.bytes[start + KEY_LENGTH_AT], self.bytes[start + KEY_LENGTH_AT + 1]]));
        let length = usize::from(u16::from_le_bytes([self.bytes[at], self.bytes[at + 1]]));
        Some(&self.bytes[at + INLINE_LENGTH_BYTES..at + INLINE_LENGTH_BYTES + length])
    }

    /// Read the next older version of a row's key
    pub fn older(&self, address: EntryAddress) -> EntryAddress {
        EntryAddress(self.half(byte_of(address) + OLDER_AT))
    }

    /// Link a row to the next older version of its key
    pub fn set_older(&mut self, address: EntryAddress, older: EntryAddress) {
        let at = byte_of(address) + OLDER_AT;
        self.bytes[at..at + size_of::<u32>()].copy_from_slice(&older.0.to_le_bytes());
    }

    /// Read the next newer version of a row's key
    pub fn newer(&self, address: EntryAddress) -> EntryAddress {
        EntryAddress(self.half(byte_of(address) + NEWER_AT))
    }

    /// Link a row to the next newer version of its key
    pub fn set_newer(&mut self, address: EntryAddress, newer: EntryAddress) {
        let at = byte_of(address) + NEWER_AT;
        self.bytes[at..at + size_of::<u32>()].copy_from_slice(&newer.0.to_le_bytes());
    }

    /// Check whether a newer version of this row's key sits in a sealed segment
    pub fn has_newer_sealed(&self, address: EntryAddress) -> bool {
        self.bytes[byte_of(address) + KIND_AT] & NEWER_SEALED_FLAG != 0
    }

    /// Note that a newer version of this row's key sits in a sealed segment
    pub fn mark_newer_sealed(&mut self, address: EntryAddress) {
        self.bytes[byte_of(address) + KIND_AT] |= NEWER_SEALED_FLAG;
    }

    /// Check a row went into its column's sorted index before its seal
    pub fn is_entered(&self, address: EntryAddress) -> bool {
        self.bytes[byte_of(address) + KIND_AT] & ENTERED_FLAG != 0
    }

    /// Note a row went into its column's sorted index before its seal
    pub fn mark_entered(&mut self, address: EntryAddress) {
        self.bytes[byte_of(address) + KIND_AT] |= ENTERED_FLAG;
    }

    /// Read one little-endian word
    pub fn word(&self, at: usize) -> u64 {
        let mut word = [0; size_of::<u64>()];
        word.copy_from_slice(&self.bytes[at..at + size_of::<u64>()]);
        u64::from_le_bytes(word)
    }

    /// Read one little-endian half word
    pub fn half(&self, at: usize) -> u32 {
        let mut half = [0; size_of::<u32>()];
        half.copy_from_slice(&self.bytes[at..at + size_of::<u32>()]);
        u32::from_le_bytes(half)
    }
}

/// Find a row's first byte
pub fn byte_of(address: EntryAddress) -> usize {
    address.0 as usize * ENTRY_ALIGNMENT
}

/// Size a row with its key, rounded up to the alignment
pub fn entry_size(key_length: usize) -> usize {
    (KEY_AT + key_length).next_multiple_of(ENTRY_ALIGNMENT)
}

/// Bytes the length of a row's kept value takes
pub const INLINE_LENGTH_BYTES: usize = 2;

/// Size a row with its key and the value it keeps, rounded up to the alignment
pub fn inline_entry_size(key_length: usize, value_length: usize) -> usize {
    (KEY_AT + key_length + INLINE_LENGTH_BYTES + value_length).next_multiple_of(ENTRY_ALIGNMENT)
}
