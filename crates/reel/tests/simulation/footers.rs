//! Footers written to the simulated disk and read back through their tail and block index

use std::path::PathBuf;

use crate::seed_sets::seeds;
use reel::index::key_hash::hash_key;
use reel_sim::file_image::BitMask;
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_io::SimulatedIo;
use reel::io::{Io, IoFile};
use reel_sim::simulated_disk::OperationCount;
use reel::record::record_check::RecordKey;
use reel::row_kind::RowKind;
use reel::segment::block_entry::{BLOCK_ENTRY_BYTES, BlockEntry};
use reel::segment::footer_reader::{ColumnIndex, FooterAnswer, FooterQuery, FooterReader};
use reel::segment::footer_writer::{ColumnStart, FooterTotals, FooterWriter};
use reel::segment::row_block::{BLOCK_HEADER_BYTES, FooterRow, ROW_BLOCK_BYTES, read_block_header};
use reel::segment::segment_tail::{
    BlockCount, COPY_GAP_BYTES, ColumnTail, MAX_TAIL_BYTES, SECOND_TAIL_DISTANCE, SegmentNumber, TailHead,
};
use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, FileOffset, RowCount, WritePlace};

const RECORD_KEY: RecordKey = RecordKey(41);
const ROOT: &str = "/disk/footers";
const SEGMENT: SegmentNumber = SegmentNumber(12);
const DATA_END: FileOffset = FileOffset(5_000);
const MOST_BLOCKS: BlockCount = BlockCount(4_096);
const FIRST_COLUMN: ColumnId = ColumnId(3);
const SECOND_COLUMN: ColumnId = ColumnId(7);
const ROWS: u64 = 20_000;
const LATEST: WritePlace = WritePlace(u64::MAX);
const RANDOM_KEY_BYTES: usize = 32;
const MOST_ROW_BYTES: f64 = 44.0;
const COPY_BYTES: u64 = 240_000;
const LARGE_PLACE: u64 = 40_000_000_000;
const CRASH_POINTS: u64 = 12;
const CRASH_SEEDS: u64 = 48;

type ColumnRows = (ColumnId, Vec<(Vec<u8>, FooterRow)>);

struct Written {
    io: SimulatedIo,
    path: PathBuf,
    file: <SimulatedIo as Io>::File,
    totals: FooterTotals,
}

struct Loaded {
    head: TailHead,
    columns: Vec<ColumnTail>,
    entries: Vec<BlockEntry>,
}

fn key_of(number: u64) -> Vec<u8> {
    format!("epoch-0042-slot-{number:010}").into_bytes()
}

fn row_of(number: u64) -> FooterRow {
    FooterRow {
        place: WritePlace(number + 1),
        offset: FileOffset(number * 100),
        length: ByteCount(100),
        kind: if number.is_multiple_of(7) { RowKind::Grave } else { RowKind::Value },
    }
}

fn lowest_place(columns: &[ColumnRows]) -> WritePlace {
    let mut lowest = WritePlace(u64::MAX);
    for (_column, rows) in columns {
        for (_key, row) in rows {
            lowest = lowest.min(row.place);
        }
    }
    lowest
}

fn write_rows(columns: &[ColumnRows]) -> Written {
    let io = SimulatedIo::new(Seed(1));
    let root = PathBuf::from(ROOT);
    io.create_directory(&root).expect("directory");
    let path = root.join("segment");
    let file = io.create_file(&path, ByteCount(DATA_END.0)).expect("create");

    let mut writer = FooterWriter::new(MOST_BLOCKS);
    writer.start(DATA_END, lowest_place(columns));
    for (column, rows) in columns {
        let start = ColumnStart {
            column: *column,
            lowest: &rows[0].0,
            highest: &rows[rows.len() - 1].0,
            block_bytes: ROW_BLOCK_BYTES,
        };
        writer.start_column(&file, &start).expect("column");
        for (key, row) in rows {
            writer.push_row(&file, key, row).expect("row");
        }
    }
    let totals = writer.finish(&file, SEGMENT, DATA_END, b"", RECORD_KEY, &[]).expect("finish");
    Written { io, path, file, totals }
}

fn load(written: &Written, reader: &mut FooterReader) -> Loaded {
    let mut columns = Vec::new();
    let mut entries = Vec::new();
    let head = reader
        .read_tail(&written.file, RECORD_KEY, &mut columns)
        .expect("read")
        .expect("sealed");
    assert!(reader.read_index(&written.file, &head, &mut entries).expect("index"));
    Loaded { head, columns, entries }
}

fn find(
    written: &Written,
    reader: &mut FooterReader,
    loaded: &Loaded,
    position: usize,
    key: &[u8],
    read_point: WritePlace,
) -> FooterAnswer {
    let column = &loaded.columns[position];
    let first = column.first_block as usize;
    let end = first + column.block_count.0 as usize;
    let filter_end = match loaded.entries.get(end) {
        Some(next) => next.header_at(),
        None => loaded.head.index_at,
    };
    let index = ColumnIndex {
        column,
        entries: &loaded.entries[first..end],
        filter_end,
        place_base: loaded.head.place_base,
    };
    let query = FooterQuery {
        hash: hash_key(column.column, key),
        key,
        read_point,
    };
    reader.lookup(&written.file, &index, &query).expect("lookup")
}

fn ordered_rows() -> Vec<(Vec<u8>, FooterRow)> {
    let mut rows = Vec::new();
    for number in 0..ROWS {
        rows.push((key_of(number * 2), row_of(number)));
    }
    rows
}

// every row is found by key, and a key between rows is absent, across many blocks
#[test]
fn footer_lookups() {
    let written = write_rows(&[(FIRST_COLUMN, ordered_rows())]);
    let mut reader = FooterReader::new();
    let loaded = load(&written, &mut reader);

    assert!(loaded.head.block_count.0 > 1);
    assert_eq!(loaded.head.segment, SEGMENT);
    assert_eq!(loaded.head.data_end, DATA_END);
    assert_eq!(loaded.columns[0].row_count, RowCount(ROWS));
    assert_eq!(written.totals.row_count, RowCount(ROWS));

    for number in (0..ROWS).step_by(37) {
        let found = find(&written, &mut reader, &loaded, 0, &key_of(number * 2), LATEST);
        assert_eq!(found, FooterAnswer::Found(row_of(number)));

        let between = find(&written, &mut reader, &loaded, 0, &key_of(number * 2 + 1), LATEST);
        assert_eq!(between, FooterAnswer::Absent);
    }
}

// two columns keep their own rows, and a key of one is absent from the other
#[test]
fn two_columns() {
    let first = vec![(b"alpha".to_vec(), row_of(1)), (b"beta".to_vec(), row_of(2))];
    let second = vec![(b"alpha".to_vec(), row_of(3))];
    let written = write_rows(&[(FIRST_COLUMN, first), (SECOND_COLUMN, second)]);
    let mut reader = FooterReader::new();
    let loaded = load(&written, &mut reader);

    assert_eq!(loaded.columns.len(), 2);
    let found = find(&written, &mut reader, &loaded, 1, b"alpha", LATEST);
    assert_eq!(found, FooterAnswer::Found(row_of(3)));
    assert_eq!(find(&written, &mut reader, &loaded, 1, b"beta", LATEST), FooterAnswer::Absent);
}

// versions of one key come newest first, and a read point picks among them
#[test]
fn footer_versions() {
    let version = |place: u64| FooterRow {
        place: WritePlace(place),
        offset: FileOffset(place),
        length: ByteCount(1),
        kind: RowKind::Value,
    };
    let rows = vec![
        (b"key".to_vec(), version(30)),
        (b"key".to_vec(), version(20)),
        (b"key".to_vec(), version(10)),
    ];
    let written = write_rows(&[(FIRST_COLUMN, rows)]);
    let mut reader = FooterReader::new();
    let loaded = load(&written, &mut reader);

    for (read_point, place) in [(35, 30), (30, 30), (29, 20), (10, 10)] {
        let found = find(&written, &mut reader, &loaded, 0, b"key", WritePlace(read_point));
        assert_eq!(found, FooterAnswer::Found(version(place)));
    }
    assert_eq!(find(&written, &mut reader, &loaded, 0, b"key", WritePlace(9)), FooterAnswer::Absent);
}

// one rotted tail copy costs nothing, and with both gone the segment reads as not sealed
#[test]
fn torn_tail() {
    let written = write_rows(&[(FIRST_COLUMN, ordered_rows())]);
    let last = FileOffset(written.totals.file_length.0 - 3);
    let mut columns = Vec::new();
    let mut reader = FooterReader::new();

    written.io.rot(&written.path, last, BitMask(1)).expect("rot");
    let one_lost = reader.read_tail(&written.file, RECORD_KEY, &mut columns).expect("read");
    assert_eq!(one_lost, Some(written.totals.head));

    let second = FileOffset(written.totals.file_length.0 - SECOND_TAIL_DISTANCE - 3);
    written.io.rot(&written.path, second, BitMask(1)).expect("rot");
    let both_lost = reader.read_tail(&written.file, RECORD_KEY, &mut columns).expect("read");
    assert_eq!(both_lost, None);
}

// the second copy alone still seals the segment
#[test]
fn second_tail_only() {
    let written = write_rows(&[(FIRST_COLUMN, ordered_rows())]);
    let second = FileOffset(written.totals.file_length.0 - SECOND_TAIL_DISTANCE - 3);
    written.io.rot(&written.path, second, BitMask(1)).expect("rot");

    let mut columns = Vec::new();
    let mut reader = FooterReader::new();
    let head = reader.read_tail(&written.file, RECORD_KEY, &mut columns).expect("read");

    assert_eq!(head, Some(written.totals.head));
}

// the two copies of the tail and of the index never share a span of the file
#[test]
fn copy_distance() {
    let written = write_rows(&[(FIRST_COLUMN, ordered_rows())]);
    let head = written.totals.head;
    let lines = u64::from(head.block_count.0) * BLOCK_ENTRY_BYTES as u64;

    assert!(head.second_index_at.0 >= head.index_at.0 + lines + COPY_GAP_BYTES);
    assert!(SECOND_TAIL_DISTANCE >= MAX_TAIL_BYTES as u64 + COPY_GAP_BYTES);
}

// rot in a row block reads as damaged, never as a row or as absent
#[test]
fn block_rot() {
    let written = write_rows(&[(FIRST_COLUMN, ordered_rows())]);
    let mut reader = FooterReader::new();
    let loaded = load(&written, &mut reader);
    let at = FileOffset(loaded.entries[0].block_at().0 + 40);
    written.io.rot(&written.path, at, BitMask(0b100)).expect("rot");

    let found = find(&written, &mut reader, &loaded, 0, &key_of(0), LATEST);

    assert_eq!(found, FooterAnswer::Damaged);
}

// every row block's header carries the base its rows count from, so a walk needs no tail
#[test]
fn header_base() {
    let written = write_rows(&[(FIRST_COLUMN, ordered_rows())]);
    let mut reader = FooterReader::new();
    let loaded = load(&written, &mut reader);
    assert!(loaded.entries.len() > 1);

    let mut bytes = [0; BLOCK_HEADER_BYTES];
    for entry in &loaded.entries {
        written.file.read_exact_at(entry.header_at(), &mut bytes).expect("read");
        let header = read_block_header(&bytes).expect("header");
        assert_eq!(header.place_base, loaded.head.place_base);
    }
}

// a footer costs about what the index layer budgets on random 32 B keys, filters included
#[test]
fn row_cost() {
    let mut random = SeededRandom::new(Seed(5));
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for _row in 0..ROWS {
        let mut key = Vec::with_capacity(RANDOM_KEY_BYTES);
        for _word in 0..RANDOM_KEY_BYTES / size_of::<u64>() {
            key.extend_from_slice(&random.next_word().to_le_bytes());
        }
        keys.push(key);
    }
    keys.sort();

    let mut rows = Vec::new();
    for (index, key) in keys.into_iter().enumerate() {
        let place = LARGE_PLACE + random.below(ROWS * 100);
        let row = FooterRow {
            place: WritePlace(place),
            offset: FileOffset(index as u64 * 152),
            length: ByteCount(152),
            kind: RowKind::Value,
        };
        rows.push((key, row));
    }

    let written = write_rows(&[(FIRST_COLUMN, rows)]);
    // the second copies cost a fixed span per segment, whatever its row count
    let footer_bytes = written.totals.file_length.0 - DATA_END.0 - COPY_BYTES;
    let per_row = footer_bytes as f64 / ROWS as f64;

    assert!(per_row <= MOST_ROW_BYTES);
}

// rot in a filter reads as damaged, so a present key can't turn into a miss
#[test]
fn filter_rot() {
    let written = write_rows(&[(FIRST_COLUMN, ordered_rows())]);
    let mut reader = FooterReader::new();
    let loaded = load(&written, &mut reader);
    let at = FileOffset(u64::from(loaded.entries[0].filter_offset) + 5);
    written.io.rot(&written.path, at, BitMask(0b100)).expect("rot");

    let found = find(&written, &mut reader, &loaded, 0, &key_of(0), LATEST);

    assert_eq!(found, FooterAnswer::Damaged);
}

// one rotted index copy costs nothing, and with both gone the read fails
#[test]
fn index_rot() {
    let written = write_rows(&[(FIRST_COLUMN, ordered_rows())]);
    let mut reader = FooterReader::new();
    let loaded = load(&written, &mut reader);
    let at = FileOffset(loaded.head.index_at.0 + 3);
    written.io.rot(&written.path, at, BitMask(0b100)).expect("rot");

    let mut entries = Vec::new();
    assert!(reader.read_index(&written.file, &loaded.head, &mut entries).expect("read"));
    assert_eq!(entries, loaded.entries);

    let second = FileOffset(loaded.head.second_index_at.0 + 3);
    written.io.rot(&written.path, second, BitMask(0b100)).expect("rot");
    assert!(!reader.read_index(&written.file, &loaded.head, &mut entries).expect("read"));
}

// keys sharing more than a prefix's length keep ranges that tell their segments apart
#[test]
fn long_shared() {
    let shared = "address-that-runs-past-sixteen-bytes-";
    let rows = vec![
        (format!("{shared}0100").into_bytes(), row_of(1)),
        (format!("{shared}0200").into_bytes(), row_of(2)),
    ];
    let written = write_rows(&[(FIRST_COLUMN, rows)]);
    let mut reader = FooterReader::new();
    let loaded = load(&written, &mut reader);

    assert_eq!(loaded.columns[0].shared_length as usize, shared.len() + 1);
    assert_ne!(loaded.columns[0].lowest, loaded.columns[0].highest);
}

// a power cut at any write of a footer leaves no tail, or a tail with its whole footer behind it
#[test]
fn power_cut() {
    let mut rows = ordered_rows();
    rows.truncate(4_000);
    let mut sealed = 0;

    for crash_at in 0..CRASH_POINTS {
        for seed in seeds(0..CRASH_SEEDS) {
            sealed += cut_during_footer(&rows, OperationCount(crash_at), Seed(seed));
        }
    }

    assert!(sealed > 0);
}

// run one footer into a power cut, check what's left, and count it when it reads as sealed
fn cut_during_footer(rows: &[(Vec<u8>, FooterRow)], crash_at: OperationCount, seed: Seed) -> u64 {
    let io = SimulatedIo::new(seed);
    let root = PathBuf::from(ROOT);
    io.create_directory(&root).expect("directory");
    let path = root.join("segment");
    let file = io.create_file(&path, ByteCount(DATA_END.0)).expect("create");
    file.sync().expect("sync");
    io.sync_directory(&root).expect("directory sync");

    io.arm_crash(crash_at);
    let mut writer = FooterWriter::new(MOST_BLOCKS);
    writer.start(DATA_END, WritePlace(1));
    let start = ColumnStart {
        column: FIRST_COLUMN,
        lowest: &rows[0].0,
        highest: &rows[rows.len() - 1].0,
        block_bytes: ROW_BLOCK_BYTES,
    };
    let mut outcome = writer.start_column(&file, &start);
    for (key, row) in rows {
        if outcome.is_ok() {
            outcome = writer.push_row(&file, key, row);
        }
    }
    if outcome.is_ok() {
        let finished = writer.finish(&file, SEGMENT, DATA_END, b"", RECORD_KEY, &[]);
        outcome = finished.map(|_totals| ());
    }
    // a crash that never fired still loses whatever the last sync didn't cover
    let _ended = outcome;
    io.power_cut();

    let reopened = io.open_file(&path).expect("reopen");
    let mut reader = FooterReader::new();
    let mut columns = Vec::new();
    let head = reader.read_tail(&reopened, RECORD_KEY, &mut columns).expect("read");
    let Some(head) = head else {
        return 0;
    };

    let mut entries = Vec::new();
    assert!(reader.read_index(&reopened, &head, &mut entries).expect("index"));
    let written = Written {
        io,
        path,
        file: reopened,
        totals: FooterTotals {
            head,
            file_length: ByteCount(0),
            block_count: head.block_count,
            row_count: RowCount(0),
            most_block_rows: 0,
        },
    };
    let loaded = Loaded { head, columns, entries };
    for (key, row) in rows.iter().step_by(97) {
        let found = find(&written, &mut reader, &loaded, 0, key, LATEST);
        assert_eq!(found, FooterAnswer::Found(*row));
    }
    1
}

// a row past what an empty block holds is refused, and the writer drops no row on the way
#[test]
fn row_past_an_empty_block_is_refused() {
    let io = SimulatedIo::new(Seed(2));
    let root = PathBuf::from(ROOT);
    io.create_directory(&root).expect("directory");
    let file = io.create_file(&root.join("segment"), ByteCount(DATA_END.0)).expect("create");
    let small = b"a".to_vec();
    let long = vec![b'z'; 200];
    let mut writer = FooterWriter::new(MOST_BLOCKS);
    writer.start(DATA_END, WritePlace(0));
    let start = ColumnStart { column: FIRST_COLUMN, lowest: &small, highest: &long, block_bytes: 128 };
    writer.start_column(&file, &start).expect("column");
    let row = FooterRow { place: WritePlace(1), offset: FileOffset(0), length: ByteCount(1), kind: RowKind::Value };

    writer.push_row(&file, &small, &row).expect("a short row");
    assert!(writer.push_row(&file, &long, &row).is_err(), "a row past an empty block went nowhere");
}

// the totals hold the most rows any one block took, and the written index says the same of each block
#[test]
fn most_block_rows_in_totals() {
    let written = write_rows(&[(FIRST_COLUMN, ordered_rows())]);
    let mut reader = FooterReader::new();
    let head = written.totals.head;
    let mut entries = Vec::new();
    assert!(reader.read_index(&written.file, &head, &mut entries).expect("index"));
    let most = entries.iter().map(|entry| entry.live_rows).max().expect("a block");
    assert_eq!(written.totals.most_block_rows, most);
    assert!(most > 1 && u64::from(most) < ROWS);
}
