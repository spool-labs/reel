use reel::growing_list::GrowingList;
use reel::segment::block_entry::BlockEntry;
use reel::segment::row_block::BLOCK_HEADER_BYTES;
use reel::segment::segment_table::*;
use reel::segment::segment_tail::{SegmentNumber, TailHead};
use reel_core::units::{RootId, WritePlace};

const NUMBERS: u64 = 500;

fn table_with(segments_per_shard: usize) -> SegmentTable<u32> {
    SegmentTable::new(&TableSizing {
        segments_per_shard,
        columns_per_shard: segments_per_shard,
        blocks_per_shard: segments_per_shard * 2,
    }).expect("reserve a table")
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
        columns: GrowingList::reserved(0).expect("reserve"),
        blocks: {
            let mut blocks = GrowingList::reserved(4).expect("reserve");
            blocks.make_ready(4).expect("ready");
            assert!(blocks.extend_from_slice(&[line(10), line(20), line(30), line(40)]));
            blocks
        },
        live_columns: 0,
        live_blocks: 4,
    };
    let entry = |number: u64, start: u32, length: u32| TableEntry {
        number: SegmentNumber(number),
        file: 0_u32,
        root: RootId(0),
        head: TailHead::default(),
        columns: Extent::default(),
        blocks: Extent { start, length },
        is_walked: false,
        highest_replacement: WritePlace(0),
        live_blocks: 0,
        unbacked: 0,
        unbacked_walk: 0,
        has_held_frees: false,
        is_output: false,
        is_unproven: false,
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

    assert!(table.install(1, RootId(0), &head(1), &[], &[]).expect("populate"));
    let mut other = 2;
    while !same_shard(other) {
        other += 1;
    }
    assert!(!table.install(2, RootId(0), &head(other), &[], &[]).expect("populate"));
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

// a retired segment keeps its slot until it's given back, so a new segment's reads never count as its own
#[test]
fn retired_keeps_slot() {
    let table = table_with(4);
    let head = |number: u64| TailHead {
        segment: SegmentNumber(number),
        ..TailHead::default()
    };
    let mut other = 2;
    while shard_of(SegmentNumber(other)) != shard_of(SegmentNumber(1)) {
        other += 1;
    }
    assert!(table.install(1, RootId(0), &head(1), &[], &[]).expect("populate"));
    let retired = table.retire(SegmentNumber(1)).expect("retired");

    assert!(table.install(2, RootId(0), &head(other), &[], &[]).expect("populate"));
    let pin = table.pin(SegmentNumber(other)).expect("a pin");
    assert_eq!(table.retired_pins(&retired), 0);
    drop(pin);

    table.free_retired(&retired);
    assert_eq!(table.shards.read(retired.shard).free_slots.len(), 3);
}

// the past-half word follows a shard's slots as segments come and go, with no shard lock taken to read it
#[test]
fn past_half_follows_the_slots() {
    let head = |number: u64| TailHead {
        segment: SegmentNumber(number),
        ..TailHead::default()
    };
    let per_shard = 4;
    let table = table_with(per_shard);
    let numbers: Vec<u64> = (1..NUMBERS).filter(|number| shard_of(SegmentNumber(*number)) == 0).take(per_shard).collect();
    assert!(!table.is_past_half());
    for (taken, number) in numbers.iter().enumerate() {
        assert!(table.install(1, RootId(0), &head(*number), &[], &[]).expect("populate"));
        assert_eq!(table.is_past_half(), 2 * (taken + 1) > per_shard, "{} taken", taken + 1);
    }
    for number in &numbers {
        let retired = table.retire(SegmentNumber(*number)).expect("a segment");
        table.free_retired(&retired);
    }
    assert!(!table.is_past_half());
}

// a segment reads wholly dead at the drop that empties its last live block, and not one row before
#[test]
fn dead_at_the_last_live_block() {
    let table = table_with(4);
    let number = SegmentNumber(1);
    let head = TailHead { segment: number, ..TailHead::default() };
    let live = |offset: u32| BlockEntry { live_rows: 5, ..line(offset) };
    assert!(table.install(1, RootId(0), &head, &[], &[live(0), live(10), live(20)]).expect("populate"));
    for block in 0..2 {
        table.drop_live(number, block, 5, WritePlace(9));
        assert_eq!(table.dead_replacement(number), None, "block {block}");
    }
    // a drop on a block already dead takes nothing more off
    table.drop_live(number, 0, 5, WritePlace(9));
    table.drop_live(number, 2, 4, WritePlace(9));
    assert_eq!(table.dead_replacement(number), None, "one row left");
    table.drop_live(number, 2, 1, WritePlace(12));
    assert_eq!(table.dead_replacement(number), Some(WritePlace(12)));
}

// a drop past a segment's last block lands nowhere: neither the segment nor the neighbour after it reads dead
#[test]
fn drop_past_the_last_block_lands_nowhere() {
    let table = table_with(4);
    let first = SegmentNumber(1);
    let second = (2..).map(SegmentNumber).find(|number| shard_of(*number) == shard_of(first)).expect("a neighbour");
    let live = |offset: u32| BlockEntry { live_rows: 5, ..line(offset) };
    for number in [first, second] {
        let head = TailHead { segment: number, ..TailHead::default() };
        assert!(table.install(1, RootId(0), &head, &[], &[live(0)]).expect("populate"));
    }
    table.drop_live(first, 1, 5, WritePlace(9));
    assert_eq!(table.dead_replacement(first), None);
    assert_eq!(table.dead_replacement(second), None);
    assert_eq!(table.live_rows(second), Some(5));
}

// charges read apart only while the walk that made them is in flight, and a new walk's first charge drops what a finished
// walk left
#[test]
fn unbacked_charges_follow_the_walk() {
    let table = table_with(4);
    let number = SegmentNumber(1);
    let head = TailHead { segment: number, ..TailHead::default() };
    assert!(table.install(1, RootId(0), &head, &[], &[BlockEntry { live_rows: 5, ..line(0) }]).expect("populate"));
    table.note_unbacked(number, 100, 3);
    assert_eq!(table.unbacked(number, Some(3)), 100, "the walk in flight's charge read backed");
    assert_eq!(table.unbacked(number, Some(4)), 0, "a finished walk's charge read unbacked");
    assert_eq!(table.unbacked(number, None), 0, "a charge read unbacked with no walk in flight");
    // the next walk's first charge drops what the finished one left
    table.note_unbacked(number, 40, 4);
    assert_eq!(table.unbacked(number, Some(4)), 40, "the new walk kept the old walk's charge");
}

// an output's kept counts land one after another from the walk in flight, and each adds to what came before
#[test]
fn kept_counts_add_up_on_a_fresh_output() {
    let table = table_with(4);
    let number = SegmentNumber(1);
    let head = TailHead { segment: number, ..TailHead::default() };
    assert!(table.install(1, RootId(0), &head, &[], &[BlockEntry { live_rows: 9, ..line(0) }]).expect("populate"));
    for (bytes, place) in [(10, 20), (20, 30), (30, 40)] {
        table.note_unbacked(number, bytes, 7);
        table.drop_live(number, 0, 1, WritePlace(place));
    }
    assert_eq!(table.unbacked(number, Some(7)), 60, "a later kept count wiped the earlier ones");
}

// a shard's block lines come into memory as segments go in and go back once retired segments' holes outweigh the live lines, and
// every live segment keeps its own lines through the closing of holes, in a seeded order of installs and retirements
#[test]
fn block_lines_grow_and_give_back() {
    const LINES: u32 = 64;
    let rows_of = |number: u64| (number % 97) as u32 + 1;
    let head = |number: u64| TailHead { segment: SegmentNumber(number), ..TailHead::default() };
    for seed in 1..=4_u64 {
        let table: SegmentTable<u32> = SegmentTable::new(&TableSizing { segments_per_shard: 4_096, columns_per_shard: 4_096, blocks_per_shard: 4_096 * LINES as usize }).expect("reserve a table");
        let lines_held = |table: &SegmentTable<u32>| table.shards.read(0).blocks.ready_bytes() as u64;
        let empty = lines_held(&table);
        let table_fixed = table.heap_bytes().0 - empty;
        let mut numbers = (1_u64..).filter(|number| shard_of(SegmentNumber(*number)) == 0);
        let mut live: Vec<u64> = Vec::new();
        let mut install = |table: &SegmentTable<u32>, live: &mut Vec<u64>| {
            let number = numbers.next().expect("a number");
            let lines: Vec<BlockEntry> = (0..LINES).map(|_line| BlockEntry { live_rows: rows_of(number), ..BlockEntry::default() }).collect();
            assert!(table.install(number as u32, RootId(0), &head(number), &[], &lines).expect("populate"), "seed {seed}: no room for {number}");
            live.push(number);
        };
        for _segment in 0..2_000 {
            install(&table, &mut live);
        }
        let grown = lines_held(&table);
        assert!(empty == 0 && grown >= 2_000 * u64::from(LINES) * size_of::<BlockEntry>() as u64, "seed {seed}: the lines held {empty} empty and {grown} full");
        // retire most in a seeded order, as merges and unlinks would
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        for _gone in 0..1_900 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let at = (state % live.len() as u64) as usize;
            let number = live.swap_remove(at);
            let retired = table.retire(SegmentNumber(number)).expect("a segment");
            table.free_retired(&retired);
        }
        for _segment in 0..400 {
            install(&table, &mut live);
        }
        let after = lines_held(&table);
        assert!(after < grown / 2, "seed {seed}: the lines held {after} bytes after most went, {grown} at their most");
        assert_eq!(table.heap_bytes().0 - lines_held(&table), table_fixed, "seed {seed}: a part past the lines moved");
        for number in &live {
            assert_eq!(table.live_rows(SegmentNumber(*number)), Some(u64::from(LINES * rows_of(*number))), "seed {seed}: segment {number} read another's lines");
        }
    }
}
