use reel::row_kind::RowKind;
use reel::segment::row_block::*;
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset, WritePlace};

const ROWS: u64 = 500;

fn key_of(number: u64) -> Vec<u8> {
    format!("column-key-{number:08}").into_bytes()
}

fn row_of(number: u64) -> FooterRow {
    FooterRow {
        place: WritePlace(number * 3),
        offset: FileOffset(number * 1_000),
        length: ByteCount(number),
        kind: if number.is_multiple_of(5) { RowKind::Grave } else { RowKind::Value },
    }
}

fn filled() -> RowBlockBuilder {
    let mut builder = RowBlockBuilder::new();
    for number in 0..ROWS {
        assert!(builder.push(&key_of(number * 2), &row_of(number)));
    }
    builder
}

// rows walk back in order with their keys whole
#[test]
fn block_walk() {
    let mut builder = filled();
    let block = builder.finish();
    let mut walk = RowBlockWalk::open(block, WritePlace(0)).expect("whole block");
    let mut key = Vec::new();

    for number in 0..ROWS {
        assert_eq!(walk.next_row(&mut key), Some(row_of(number)));
        assert_eq!(key, key_of(number * 2));
    }
    assert_eq!(walk.next_row(&mut key), None);
}

// a seek lands on the key, or on the first one after it, or past the end
#[test]
fn block_seek() {
    let mut builder = filled();
    let block = builder.finish();
    let mut key = Vec::new();

    for number in 0..ROWS {
        let mut walk = RowBlockWalk::open(block, WritePlace(0)).expect("whole block");
        assert_eq!(walk.seek(&key_of(number * 2), &mut key), Some(row_of(number)));
        assert_eq!(key, key_of(number * 2));

        let mut walk = RowBlockWalk::open(block, WritePlace(0)).expect("whole block");
        let after = walk.seek(&key_of(number * 2 + 1), &mut key);
        if number + 1 < ROWS {
            assert_eq!(after, Some(row_of(number + 1)));
        } else {
            assert_eq!(after, None);
        }
    }
}

// a block stays inside its size and refuses the row that would overflow it
#[test]
fn block_limit() {
    let mut builder = RowBlockBuilder::new();
    let mut number = 0;
    while builder.push(&key_of(number), &row_of(number)) {
        number += 1;
    }

    assert!(number > ROWS);
    assert!(builder.finish().len() <= ROW_BLOCK_BYTES);
}

// one flipped bit fails the block
#[test]
fn block_rot() {
    let mut builder = filled();
    let mut block = builder.finish().to_vec();

    block[100] ^= 1;

    assert!(RowBlockWalk::open(&block, WritePlace(0)).is_none());
}

// a header reads back, and any flipped byte or a foreign marker is refused
#[test]
fn header_round() {
    let header = BlockHeader {
        place_base: WritePlace(77),
        block_length: 60_000,
        filter_length: 2_500,
        column: ColumnId(3),
    };
    let bytes = write_block_header(&header);

    assert_eq!(read_block_header(&bytes), Some(header));
    for at in 0..BLOCK_HEADER_BYTES {
        let mut damaged = bytes;
        damaged[at] ^= 1;
        assert_eq!(read_block_header(&damaged), None);
    }
}

// a block with no rows opens and has nothing to find
#[test]
fn empty_block() {
    let mut builder = RowBlockBuilder::new();
    let block = builder.finish();
    let mut walk = RowBlockWalk::open(block, WritePlace(0)).expect("whole block");
    let mut key = Vec::new();

    assert_eq!(walk.seek(b"any", &mut key), None);
    assert_eq!(walk.next_row(&mut key), None);
}

// empty keys and repeated keys survive the shared prefixes
#[test]
fn odd_keys() {
    let mut builder = RowBlockBuilder::new();
    assert!(builder.push(b"", &row_of(1)));
    assert!(builder.push(b"same", &row_of(2)));
    assert!(builder.push(b"same", &row_of(3)));

    let block = builder.finish();
    let mut walk = RowBlockWalk::open(block, WritePlace(0)).expect("whole block");
    let mut key = Vec::new();

    assert_eq!(walk.next_row(&mut key), Some(row_of(1)));
    assert!(key.is_empty());
    assert_eq!(walk.next_row(&mut key), Some(row_of(2)));
    assert_eq!(key, b"same");
    assert_eq!(walk.next_row(&mut key), Some(row_of(3)));
    assert_eq!(key, b"same");
}

// a key past the builder's key list is refused before any of its bytes go in, so a retry can't write the row twice
#[test]
fn long_key_writes_nothing() {
    let mut builder = RowBlockBuilder::new();
    let long = vec![b'k'; reel::config::MAX_KEY_BYTES as usize + 1];

    assert!(!builder.push(&long, &row_of(1)));

    assert!(builder.is_empty());
    assert!(builder.push(&key_of(1), &row_of(1)));
    let block = builder.finish().to_vec();
    let mut walk = RowBlockWalk::open(&block, WritePlace(0)).expect("a block");
    let mut key = Vec::new();
    assert_eq!(walk.next_row(&mut key), Some(row_of(1)));
    assert_eq!(key, key_of(1));
    assert_eq!(walk.next_row(&mut key), None);
}

// a row whose place or length overflows reads as no row, where it once overflowed the add. The fuzzer found these bytes
#[test]
fn overflowing_row() {
    let block = [
        0x41, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0xd2, 0x20, 0x00, 0x3a, 0x01, 0xd2, 0x20, 0x5a, 0x5a,
        0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x00, 0x3a, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x41,
    ];
    let mut key: Vec<u8> = Vec::with_capacity(64);
    let mut walk = RowBlockWalk::open_checked(&block, WritePlace(1 << 32)).expect("a walk");
    while walk.next_row(&mut key).is_some() {}
}
