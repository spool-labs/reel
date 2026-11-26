use reel::engine::dead_bits::{DeadBits, DeadRow};
use reel::fixed_list::FixedList;
use reel::segment::block_entry::BlockEntry;
use reel::segment::footer_reader::RowAt;
use reel::segment::segment_table::{SegmentTable, TableSizing};
use reel::segment::segment_tail::{SegmentNumber, TailHead};
use reel::segment::summary::DeadCount;
use reel_core::units::{ByteCount, RootId, WritePlace};

const SEGMENT: SegmentNumber = SegmentNumber(7);
const BLOCK_ROWS: u32 = 5;

fn bits() -> DeadBits {
    let bits = DeadBits::new(ByteCount(4096), 8);
    assert!(bits.begin(SEGMENT, 1, BLOCK_ROWS));
    bits
}

fn row(index: u32) -> DeadRow {
    DeadRow { segment: SEGMENT, block: 0, at: RowAt { index, rows: BLOCK_ROWS }, length: 10, is_grave: false }
}

fn counts(rows: u32) -> FixedList<DeadCount> {
    let mut counts = FixedList::on_heap(4);
    counts.push(DeadCount { segment: SEGMENT, block: 0, rows, bytes: ByteCount(u64::from(rows) * 10), replacement: WritePlace(9), ..DeadCount::default() }).expect("room");
    counts
}

// a row charged twice lowers its block's count once, in one landing or in two
#[test]
fn a_row_charged_twice_counts_once() {
    let bits = bits();
    let mut both = counts(2);
    bits.land(&[row(1), row(1)], &mut both);
    assert_eq!(both[0].rows, 1, "one landing counted a row twice");
    let mut again = counts(1);
    bits.land(&[row(1)], &mut again);
    assert_eq!(again[0].rows, 0, "a second landing counted a row again");
    assert_eq!(bits.block_set(SEGMENT, 0), Some(1));
}

// a row set by a count's own bit and then landed by a walk counts once, and so does the reverse order
#[test]
fn a_set_row_and_a_walk_count_once_in_either_order() {
    let bits = bits();
    assert_eq!(bits.set_row(SEGMENT, 0, 2), Some(true));
    let mut walk = counts(1);
    bits.land(&[row(2)], &mut walk);
    assert_eq!(walk[0].rows, 0, "the walk counted a row a set bit had counted");

    let mut first = counts(1);
    bits.land(&[row(3)], &mut first);
    assert_eq!(first[0].rows, 1);
    assert_eq!(bits.set_row(SEGMENT, 0, 3), Some(false), "a row the walk counted set its bit again");
    assert_eq!(bits.block_set(SEGMENT, 0), Some(2));
}

// a cover that kills a block whole after two of its rows were charged leaves no live row, every bit set, and as many dead
// rows as the block holds
#[test]
fn a_whole_block_cover_after_two_charges_sets_every_bit() {
    let bits = bits();
    let table = SegmentTable::new(&TableSizing { segments_per_shard: 4, columns_per_shard: 4, blocks_per_shard: 8 }).expect("reserve a table");
    let head = TailHead { segment: SEGMENT, ..TailHead::default() };
    assert!(table.install(0_u32, RootId(0), &head, &[], &[BlockEntry { live_rows: BLOCK_ROWS, ..BlockEntry::default() }]).expect("populate"));
    let mut walk = counts(2);
    bits.land(&[row(0), row(4)], &mut walk);
    table.drop_live(SEGMENT, 0, walk[0].rows, WritePlace(9));

    // what the cover apply does for a block wholly under it
    let (line, _replacement) = table.block_line(SEGMENT, 0).expect("line");
    let charged = bits.block_set(SEGMENT, 0).unwrap_or(0);
    bits.set_block(SEGMENT, 0, charged + line.live_rows);
    let mut cover = counts(line.live_rows);
    bits.land(&[], &mut cover);
    table.drop_live(SEGMENT, 0, cover[0].rows, WritePlace(10));

    let (line, _replacement) = table.block_line(SEGMENT, 0).expect("line");
    assert_eq!(line.live_rows, 0);
    assert_eq!(bits.block_set(SEGMENT, 0), Some(BLOCK_ROWS));
    assert_eq!(walk[0].rows + cover[0].rows, BLOCK_ROWS, "the dead count isn't the block's rows");
}
