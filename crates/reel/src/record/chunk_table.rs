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
pub fn page_count(chunks: u64) -> u64 {
    chunks.div_ceil(ENTRIES_PER_PAGE as u64)
}

/// Count the stored bytes of a table
pub fn table_bytes(chunks: u64) -> u64 {
    let full_pages = chunks / ENTRIES_PER_PAGE as u64;
    let remainder = chunks % ENTRIES_PER_PAGE as u64;
    full_pages * TABLE_PAGE_BYTES as u64 + remainder * TABLE_ENTRY_BYTES as u64
}

/// Count the stored bytes of one page of a table
pub fn page_bytes(chunks: u64, page: u64) -> usize {
    let start = page * ENTRIES_PER_PAGE as u64;
    let remaining = chunks.saturating_sub(start);
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

#[cfg(test)]
mod tests {
    use super::*;

    const ENTRY: ChunkEntry = ChunkEntry {
        stored_offset: FileOffset(1_000),
        value_length: ByteCount(65_536),
        checksum: Checksum(77),
    };

    // an entry reads back as written
    #[test]
    fn entry_round() {
        let mut page = vec![0; TABLE_PAGE_BYTES];

        write_entry(&mut page, 5, &ENTRY);

        assert_eq!(read_entry(&page, 5), Some(ENTRY));
    }

    // one page covers about 200 MiB of chunks, and the next chunk needs a second page
    #[test]
    fn page_math() {
        assert_eq!(ENTRIES_PER_PAGE, 3_276);
        assert_eq!(page_count(1), 1);
        assert_eq!(page_count(ENTRIES_PER_PAGE as u64), 1);
        assert_eq!(page_count(ENTRIES_PER_PAGE as u64 + 1), 2);
        assert_eq!(table_bytes(2), 2 * TABLE_ENTRY_BYTES as u64);
        assert_eq!(table_bytes(ENTRIES_PER_PAGE as u64 + 1), (TABLE_PAGE_BYTES + TABLE_ENTRY_BYTES) as u64);
    }

    // a stored length comes from the next entry, the page end or the record end
    #[test]
    fn stored_lengths() {
        let mut page = vec![0; 2 * TABLE_ENTRY_BYTES];
        let second = ChunkEntry {
            stored_offset: FileOffset(1_500),
            ..ENTRY
        };
        write_entry(&mut page, 0, &ENTRY);
        write_entry(&mut page, 1, &second);

        let first_span = chunk_span(&page, 0, FileOffset(1_800)).expect("span");
        let last_span = chunk_span(&page, 1, FileOffset(1_800)).expect("span");

        assert_eq!(first_span.stored_length, ByteCount(500));
        assert_eq!(last_span.stored_length, ByteCount(300));
    }
}
