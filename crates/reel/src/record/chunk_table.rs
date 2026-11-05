//! The table in front of a chunked value's chunks, cut into checksummed pages

use reel_api::units::{ByteCount, FileOffset};

use crate::record::record_check::Checksum;

/// Bytes of one table page on disk
pub const TABLE_PAGE_BYTES: usize = 64 * 1024;
/// Bytes of one chunk's entry
pub const TABLE_ENTRY_BYTES: usize = 20;
/// Chunks one page can list, leaving room for the end offset
pub const ENTRIES_PER_PAGE: usize = (TABLE_PAGE_BYTES - size_of::<u64>()) / TABLE_ENTRY_BYTES;
/// Where a full page keeps the stored end of its last chunk
pub const PAGE_END_AT: usize = TABLE_PAGE_BYTES - size_of::<u64>();
/// Where an entry's decompressed length sits
pub const ENTRY_LENGTH_AT: usize = 8;
/// Where an entry's checksum sits
pub const ENTRY_CHECKSUM_AT: usize = 12;

/// A count of chunks in a value
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChunkCount(pub u64);

/// One chunk of a value, counted from the front
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChunkIndex(pub u64);

/// A count of table pages
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TablePageCount(pub u64);

/// One page of a table, counted from the front
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TablePageIndex(pub u64);

/// One chunk's place, size and checksum
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ChunkEntry {
    pub stored_offset: FileOffset,
    pub value_length: ByteCount,
    pub checksum: Checksum,
}

/// A chunk's stored span within its record
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ChunkSpan {
    pub entry: ChunkEntry,
    pub stored_length: ByteCount,
}

/// Count the table pages a number of chunks needs
pub fn page_count(chunks: ChunkCount) -> TablePageCount {
    TablePageCount(chunks.0.div_ceil(ENTRIES_PER_PAGE as u64))
}

/// Find the page that lists a chunk
pub fn page_of(chunk: ChunkIndex) -> TablePageIndex {
    TablePageIndex(chunk.0 / ENTRIES_PER_PAGE as u64)
}

/// Count the stored bytes of a table
pub fn table_bytes(chunks: ChunkCount) -> ByteCount {
    let full_pages = chunks.0 / ENTRIES_PER_PAGE as u64;
    let remainder = chunks.0 % ENTRIES_PER_PAGE as u64;
    ByteCount(full_pages * TABLE_PAGE_BYTES as u64 + remainder * TABLE_ENTRY_BYTES as u64)
}

/// Count the stored bytes of one page of a table
pub fn page_bytes(chunks: ChunkCount, page: TablePageIndex) -> usize {
    let start = page.0 * ENTRIES_PER_PAGE as u64;
    let remaining = chunks.0.saturating_sub(start);
    if remaining >= ENTRIES_PER_PAGE as u64 {
        return TABLE_PAGE_BYTES;
    }
    remaining as usize * TABLE_ENTRY_BYTES
}

/// Write an entry at its place in a page
pub fn write_entry(page: &mut [u8], index: usize, entry: &ChunkEntry) {
    let start = index * TABLE_ENTRY_BYTES;
    let bytes = &mut page[start..start + TABLE_ENTRY_BYTES];
    bytes[..ENTRY_LENGTH_AT].copy_from_slice(&entry.stored_offset.0.to_le_bytes());
    let length = entry.value_length.0 as u32;
    bytes[ENTRY_LENGTH_AT..ENTRY_CHECKSUM_AT].copy_from_slice(&length.to_le_bytes());
    bytes[ENTRY_CHECKSUM_AT..].copy_from_slice(&entry.checksum.0.to_le_bytes());
}

/// Read an entry back from a page
pub fn read_entry(page: &[u8], index: usize) -> Option<ChunkEntry> {
    let start = index * TABLE_ENTRY_BYTES;
    let bytes = page.get(start..start + TABLE_ENTRY_BYTES)?;

    let mut offset = [0; size_of::<u64>()];
    offset.copy_from_slice(&bytes[..ENTRY_LENGTH_AT]);
    let mut length = [0; size_of::<u32>()];
    length.copy_from_slice(&bytes[ENTRY_LENGTH_AT..ENTRY_CHECKSUM_AT]);
    let mut sum = [0; size_of::<u64>()];
    sum.copy_from_slice(&bytes[ENTRY_CHECKSUM_AT..]);

    Some(ChunkEntry {
        stored_offset: FileOffset(u64::from_le_bytes(offset)),
        value_length: ByteCount(u64::from(u32::from_le_bytes(length))),
        checksum: Checksum(u64::from_le_bytes(sum)),
    })
}

/// Read the stored end a full page keeps for its last chunk
pub fn read_page_end(page: &[u8]) -> Option<FileOffset> {
    let bytes = page.get(PAGE_END_AT..TABLE_PAGE_BYTES)?;
    let mut end = [0; size_of::<u64>()];
    end.copy_from_slice(bytes);
    Some(FileOffset(u64::from_le_bytes(end)))
}

/// Find a chunk's entry and stored length within its page
pub fn chunk_span(page: &[u8], index: usize, record_end: FileOffset) -> Option<ChunkSpan> {
    let entry = read_entry(page, index)?;
    let end = if index + 1 < page.len() / TABLE_ENTRY_BYTES && index + 1 < ENTRIES_PER_PAGE {
        read_entry(page, index + 1)?.stored_offset
    } else if page.len() == TABLE_PAGE_BYTES {
        read_page_end(page)?
    } else {
        record_end
    };

    Some(ChunkSpan {
        entry,
        stored_length: ByteCount(end.0.checked_sub(entry.stored_offset.0)?),
    })
}
