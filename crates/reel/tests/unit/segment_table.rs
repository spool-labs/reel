use reel::segment::block_entry::BlockEntry;
use reel::segment::row_block::BLOCK_HEADER_BYTES;
use reel::segment::segment_table::*;
use reel::segment::segment_tail::{SegmentNumber, TailHead};

const NUMBERS: u64 = 500;

fn table_with(segments_per_shard: usize) -> SegmentTable<u32> {
    SegmentTable::new(&TableSizing {
        segments_per_shard,
        columns_per_shard: segments_per_shard,
        blocks_per_shard: segments_per_shard * 2,
    })
}

fn line(offset: u32) -> BlockEntry {
    BlockEntry {
        block_offset: offset + BLOCK_HEADER_BYTES as u32,
        ..BlockEntry::default()
    }
}

// closing the holes keeps each segment on its own lines, whatever order slots were taken in
#[test]
fn pool_compact() {
    let mut shard = TableShard {
        segments: vec![None, None, None],
        map: NumberMap::new(3),
        free_slots: Vec::new(),
        columns: Vec::new(),
        blocks: vec![line(10), line(20), line(30), line(40)],
    };
    let entry = |number: u64, start: u32, length: u32| TableEntry {
        number: SegmentNumber(number),
        file: 0_u32,
        head: TailHead::default(),
        columns: Extent::default(),
        blocks: Extent { start, length },
    };
    // the slots run against pool order, and the run at 1 has retired
    shard.segments[0] = Some(entry(1, 3, 1));
    shard.segments[2] = Some(entry(2, 0, 1));
    shard.segments[1] = Some(entry(3, 2, 1));

    shard.compact();

    let lines_of = |slot: usize| {
        let run = shard.segments[slot].as_ref().expect("segment").blocks;
        shard.blocks[run.start as usize].block_offset - BLOCK_HEADER_BYTES as u32
    };
    assert_eq!(lines_of(0), 40);
    assert_eq!(lines_of(1), 30);
    assert_eq!(lines_of(2), 10);
    assert_eq!(shard.blocks.len(), 3);
}

// numbers that end alike still spread across the shards
#[test]
fn shard_spread() {
    let mut seen = [false; TABLE_SHARDS];
    for batch in 1..256 {
        seen[shard_of(SegmentNumber(batch * 1_024 + 1))] = true;
    }

    assert!(seen.iter().filter(|hit| **hit).count() > TABLE_SHARDS / 2);
}

// a table with no room refuses a segment
#[test]
fn table_full() {
    let table = table_with(1);
    let head = |number: u64| TailHead {
        segment: SegmentNumber(number),
        ..TailHead::default()
    };
    let same_shard = |number: u64| shard_of(SegmentNumber(number)) == shard_of(SegmentNumber(1));

    assert!(table.install(1, &head(1), &[], &[]));
    let mut other = 2;
    while !same_shard(other) {
        other += 1;
    }
    assert!(!table.install(2, &head(other), &[], &[]));
}

// numbers go in, read back and come out with every other number still found
#[test]
fn number_map() {
    let mut map = NumberMap::new(NUMBERS as usize);
    for number in 0..NUMBERS {
        map.insert(SegmentNumber(number * 16), number as u32);
    }

    for number in (0..NUMBERS).step_by(3) {
        map.remove(SegmentNumber(number * 16));
    }

    for number in 0..NUMBERS {
        let expected = if number.is_multiple_of(3) { None } else { Some(number as u32) };
        assert_eq!(map.get(SegmentNumber(number * 16)), expected);
    }
}
