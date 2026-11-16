use reel::record::chunk_table::*;
use reel::record::record_check::Checksum;
use reel_core::units::{ByteCount, FileOffset};

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
    assert_eq!(page_count(ChunkCount(1)), TablePageCount(1));
    assert_eq!(page_count(ChunkCount(ENTRIES_PER_PAGE as u64)), TablePageCount(1));
    assert_eq!(page_count(ChunkCount(ENTRIES_PER_PAGE as u64 + 1)), TablePageCount(2));
    assert_eq!(table_bytes(ChunkCount(2)), ByteCount(2 * TABLE_ENTRY_BYTES as u64));
    assert_eq!(
        table_bytes(ChunkCount(ENTRIES_PER_PAGE as u64 + 1)),
        ByteCount((TABLE_PAGE_BYTES + TABLE_ENTRY_BYTES) as u64)
    );
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
