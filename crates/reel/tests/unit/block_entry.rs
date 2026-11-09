use reel::segment::block_entry::*;
use reel_api::units::ByteCount;

// a line reads back as written and is the size the budget allows
#[test]
fn entry_round() {
    let entry = BlockEntry {
        first_key: KeyPrefix::of_key(b"shared-rest-of-the-key-is-long", 7),
        block_offset: 1_000,
        filter_offset: 2_000,
        live_rows: 30,
    };
    let mut bytes = [0; BLOCK_ENTRY_BYTES];

    entry.write(&mut bytes);

    assert_eq!(BlockEntry::read(&bytes), Some(entry));
    assert_eq!(&entry.first_key.0, b"rest-of-the-key-");
    assert_eq!(entry.block_length(), ByteCount(1_000));
}

// prefixes keep key order, a shorter key first
#[test]
fn prefix_order() {
    assert!(KeyPrefix::of_key(b"ab", 0) < KeyPrefix::of_key(b"ab\x01", 0));
    assert!(KeyPrefix::of_key(b"ab", 0) < KeyPrefix::of_key(b"b", 0));
    assert_eq!(KeyPrefix::of_key(b"ab", 5), KeyPrefix::default());
}

// an entry whose offsets would underflow or run past the buffer is refused before any read uses them
#[test]
fn shape_refusals() {
    let entry = |block_offset: u32, filter_offset: u32| BlockEntry {
        first_key: KeyPrefix::default(),
        block_offset,
        filter_offset,
        live_rows: 1,
    };
    let header = reel::segment::row_block::BLOCK_HEADER_BYTES as u32;
    assert!(entry(1_000, 2_000).is_shaped(0, 4_096));
    // the header would start before the offset 0
    assert!(!entry(header - 1, 2_000).is_shaped(0, 4_096));
    // the header starts inside what came before
    assert!(!entry(1_000, 2_000).is_shaped(u64::from(1_000 - header + 1), 4_096));
    // the filter starts before the block, or where it does
    assert!(!entry(2_000, 1_000).is_shaped(0, 4_096));
    assert!(!entry(2_000, 2_000).is_shaped(0, 4_096));
    // the block is longer than any read buffer holds
    assert!(!entry(1_000, 6_000).is_shaped(0, 4_096));
}
