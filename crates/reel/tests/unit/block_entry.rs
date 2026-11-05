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
