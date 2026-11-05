//! Records read back whole through the simulated disk, and every damaged byte is caught

use std::path::{Path, PathBuf};

use reel::io::simulated::file_image::BitMask;
use reel::io::simulated::seeded_random::Seed;
use reel::io::simulated::simulated_io::SimulatedIo;
use reel::io::{Io, IoFile};
use reel::record::chunk_table::ENTRIES_PER_PAGE;
use reel::record::codec::{CHUNK_BYTES, Codec};
use reel::record::record_check::{RecordIdentity, RecordKey};
use reel::record::record_encoder::RecordEncoder;
use reel::record::record_head::SMALL_HEAD_BYTES;
use reel::record::record_reader::{FIRST_READ_BYTES, RecordLocation, RecordRead, RecordReader};
use reel::row_kind::RowKind;
use reel_api::column_id::ColumnId;
use reel_api::reads::ValueWindow;
use reel_api::units::{ByteCount, FileOffset};

const SEED: Seed = Seed(3);
const RECORD_KEY: RecordKey = RecordKey(21);
const COLUMN: ColumnId = ColumnId(2);
const KEY: &[u8] = b"track-key";
const ROOT: &str = "/disk/records";
const LARGEST: ByteCount = ByteCount(64 * 1024 * 1024);
const LEADING_BYTES: u64 = 100;
const ROT: BitMask = BitMask(0b0001_0000);

struct Stored {
    io: SimulatedIo,
    path: PathBuf,
    location: RecordLocation,
}

fn identity() -> RecordIdentity<'static> {
    RecordIdentity {
        record_key: RECORD_KEY,
        column: COLUMN,
        key: KEY,
        kind: RowKind::Value,
    }
}

fn compressible(length: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(length);
    for index in 0..length {
        bytes.push(((index / 7) % 31) as u8);
    }
    bytes
}

fn noise(length: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(length);
    let mut state: u64 = 5;
    for _ in 0..length {
        state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        bytes.push((state >> 33) as u8);
    }
    bytes
}

fn store(codec: Codec, value: &[u8]) -> Stored {
    let largest = ByteCount(LARGEST.0.max(value.len() as u64));
    let mut encoder = RecordEncoder::new(largest, largest);
    let shape = encoder.encode(&identity(), codec, value).expect("encode");

    let io = SimulatedIo::new(SEED);
    let root = PathBuf::from(ROOT);
    io.create_directory(&root).expect("directory");
    let path = root.join("segment");
    let file = io.create_file(&path, ByteCount(0)).expect("create");
    file.write_at(FileOffset(LEADING_BYTES), &encoder.parts(&shape, value)).expect("write");

    let location = RecordLocation {
        offset: FileOffset(LEADING_BYTES),
        length: shape.total_length(),
    };
    Stored { io, path, location }
}

fn open(stored: &Stored) -> <SimulatedIo as Io>::File {
    stored.io.open_read_only(&stored.path).expect("open")
}

fn read_all(stored: &Stored, who: &RecordIdentity) -> (RecordRead, Vec<u8>) {
    let file = open(stored);
    let mut reader = RecordReader::new();
    let mut buf = Vec::new();
    let read = reader.read_value(&file, stored.location, who, &mut buf).expect("read");
    (read, buf)
}

fn read_window(stored: &Stored, window: ValueWindow) -> (RecordRead, Vec<u8>) {
    let file = open(stored);
    let mut reader = RecordReader::new();
    let mut buf = Vec::new();
    let read = reader
        .read_window(&file, stored.location, &identity(), window, &mut buf)
        .expect("read");
    (read, buf)
}

fn rot_at(stored: &Stored, path: &Path, at: u64) {
    let offset = FileOffset(stored.location.offset.0 + at);
    stored.io.rot(path, offset, ROT).expect("rot");
}

fn window(offset: usize, length: usize) -> ValueWindow {
    ValueWindow {
        offset: ByteCount(offset as u64),
        length: ByteCount(length as u64),
    }
}

// values of every shape come back whole on both codecs
#[test]
fn value_shapes() {
    let lengths = [0, 1, 1_200, CHUNK_BYTES - 1, CHUNK_BYTES, CHUNK_BYTES + 1, 3 * CHUNK_BYTES + 5];
    for codec in [Codec::None, Codec::Lz4] {
        for length in lengths {
            for value in [compressible(length), noise(length)] {
                let stored = store(codec, &value);

                let (read, restored) = read_all(&stored, &identity());

                assert_eq!(read, RecordRead::Valid);
                assert_eq!(restored, value);
            }
        }
    }
}

// a small record costs nine bytes beside its value
#[test]
fn record_overhead() {
    let value = noise(1_200);

    let stored = store(Codec::None, &value);

    assert_eq!(stored.location.length.0 as usize, value.len() + SMALL_HEAD_BYTES);
}

// compressible chunks shrink, and noise beside them stays as written
#[test]
fn mixed_chunks() {
    let mut value = compressible(2 * CHUNK_BYTES);
    value.extend_from_slice(&noise(2 * CHUNK_BYTES));

    let stored = store(Codec::Lz4, &value);
    let (read, restored) = read_all(&stored, &identity());

    assert!((stored.location.length.0 as usize) < value.len());
    assert_eq!(read, RecordRead::Valid);
    assert_eq!(restored, value);
}

// a value past one table page reads back whole and by window
#[test]
fn second_page() {
    let chunks = ENTRIES_PER_PAGE + 2;
    let mut value = vec![0; chunks * CHUNK_BYTES];
    let last = value.len() - 1;
    value[0] = 1;
    value[last] = 2;

    let stored = store(Codec::None, &value);
    let (read, restored) = read_all(&stored, &identity());
    assert_eq!(read, RecordRead::Valid);
    assert!(restored == value);

    let (read, tail) = read_window(&stored, window(value.len() - 10, 10));
    assert_eq!(read, RecordRead::Valid);
    assert_eq!(tail, &value[value.len() - 10..]);
}

// windows at every edge return exactly those bytes, cut at the value's end
#[test]
fn window_edges() {
    let value = compressible(3 * CHUNK_BYTES + 5);
    let edges = [
        (0, 1),
        (0, value.len()),
        (CHUNK_BYTES - 1, 2),
        (CHUNK_BYTES, CHUNK_BYTES),
        (CHUNK_BYTES + 10, 2 * CHUNK_BYTES - 10),
        (value.len() - 1, 1),
    ];

    for codec in [Codec::None, Codec::Lz4] {
        let stored = store(codec, &value);
        for (offset, length) in edges {
            let (read, bytes) = read_window(&stored, window(offset, length));
            assert_eq!(read, RecordRead::Valid);
            assert_eq!(bytes, &value[offset..offset + length]);
        }

        let (_read, past_end) = read_window(&stored, window(value.len() - 2, 10));
        assert_eq!(past_end, &value[value.len() - 2..]);
        let (_read, outside) = read_window(&stored, window(value.len() + 5, 10));
        assert!(outside.is_empty());
    }
}

// a window of a small record comes from the one read of that record
#[test]
fn small_window() {
    let value = noise(1_200);
    let stored = store(Codec::Lz4, &value);

    let (read, bytes) = read_window(&stored, window(100, 50));

    assert_eq!(read, RecordRead::Valid);
    assert_eq!(bytes, &value[100..150]);
}

// a window of a large value is two reads, whatever the value's size
#[test]
fn window_cost() {
    let value = vec![9; LARGEST.0 as usize];
    for codec in [Codec::None, Codec::Lz4] {
        let stored = store(codec, &value);
        let file = open(&stored);
        let mut reader = RecordReader::new();
        let mut buf = Vec::new();
        let (reads_before, bytes_before) = stored.io.read_totals();

        let read = reader
            .read_window(&file, stored.location, &identity(), window(5 * CHUNK_BYTES + 10, 4_096), &mut buf)
            .expect("read");

        let (reads_after, bytes_after) = stored.io.read_totals();
        assert_eq!(read, RecordRead::Valid);
        assert_eq!(reads_after.0 - reads_before.0, 2);
        assert!(bytes_after.0 - bytes_before.0 <= (FIRST_READ_BYTES + CHUNK_BYTES) as u64);
    }
}

// a record read under another key, kind, column or store is refused
#[test]
fn foreign_record() {
    let value = noise(CHUNK_BYTES + 100);
    for length in [100, value.len()] {
        let stored = store(Codec::None, &value[..length]);

        let mut other_key = identity();
        other_key.key = b"other-key";
        let mut other_kind = identity();
        other_kind.kind = RowKind::RangeDelete;
        let mut other_column = identity();
        other_column.column = ColumnId(3);
        let mut other_store = identity();
        other_store.record_key = RecordKey(22);

        for who in [other_key, other_kind, other_column, other_store] {
            let (read, _value) = read_all(&stored, &who);
            assert_eq!(read, RecordRead::Failed);
        }
    }
}

// rot anywhere in a small record fails the read
#[test]
fn small_rot() {
    let value = noise(300);
    for at in [0, 8, 9, 200, 308] {
        let stored = store(Codec::None, &value);
        rot_at(&stored, &stored.path, at);

        let (read, _value) = read_all(&stored, &identity());

        assert_eq!(read, RecordRead::Failed);
    }
}

// rot in the head, the table or a chunk fails a read that touches it
#[test]
fn chunked_rot() {
    let value = noise(4 * CHUNK_BYTES);
    let last_chunk = stored_end(&value) - 10;
    for at in [0, 8, 12, 30, 50, last_chunk] {
        let stored = store(Codec::None, &value);
        rot_at(&stored, &stored.path, at);

        let (read, _value) = read_all(&stored, &identity());

        assert_eq!(read, RecordRead::Failed);
    }
}

// rot in one chunk leaves a window of another chunk readable
#[test]
fn rot_contained() {
    let value = noise(4 * CHUNK_BYTES);
    let stored = store(Codec::None, &value);
    rot_at(&stored, &stored.path, stored_end(&value) - 10);

    let (read, bytes) = read_window(&stored, window(10, 100));
    assert_eq!(read, RecordRead::Valid);
    assert_eq!(bytes, &value[10..110]);

    let (read, _bytes) = read_window(&stored, window(value.len() - 100, 50));
    assert_eq!(read, RecordRead::Failed);
}

fn stored_end(value: &[u8]) -> u64 {
    store(Codec::None, value).location.length.0
}
