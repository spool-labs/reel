//! Records read back whole through the simulated disk, and every damaged byte is caught

use std::io::IoSlice; use std::path::{Path, PathBuf};

use reel_sim::file_image::BitMask;
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;
use reel::io::{InPlace, Io, IoFile, in_place};
use reel::record::chunk_table::{ChunkEntry, ENTRIES_PER_PAGE};
use reel::record::codec::{CHUNK_BYTES, Codec};
use reel::record::record_check::{Checksum, RecordIdentity, RecordKey, checksum};
use reel::pool::Pool;
use reel::record::record_encoder::{MOST_RECORD_RUNS, PlainRecord, RecordEncoder, SHARE_BYTES};
use reel::record::record_head::{ PIECE_BYTES, PIECE_SUM_BYTES, PIECES_PER_CHUNK, SMALL_HEAD_BYTES, chunk_count, chunked_head_bytes, plain_length, };
use reel::record::record_reader::{FIRST_READ_BYTES, RecordLocation, RecordRead, RecordReader, written_shape};
use reel::row_kind::RowKind;
use reel_api::column_id::ColumnId;
use reel_api::reads::ValueWindow;
use reel_api::units::{ByteCount, FileOffset, WritePlace};

const SEED: Seed = Seed(3);
const RECORD_KEY: RecordKey = RecordKey(21);
const COLUMN: ColumnId = ColumnId(2);
const KEY: &[u8] = b"track-key";
const ROOT: &str = "/disk/records";
const LARGEST: ByteCount = ByteCount(64 * 1024 * 1024);
const LEADING_BYTES: u64 = 100;
const ROT: BitMask = BitMask(0b0001_0000);
const PLAIN_MOST: usize = 2 * 1024 * 1024;
const LZ4_MOST: u64 = 64 * 1024 * 1024;
const LITTLE_SHRINK: usize = 16;

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
        place: WritePlace(0),
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
    let shares = (value.len() / SHARE_BYTES) + 1;
    let pool = Pool::new(vec![vec![0; SHARE_BYTES]; shares]);
    let mut encoder = RecordEncoder::new(largest, shares);
    assert!(encoder.take_shares(&pool, codec, value));
    let shape = encoder.encode(&identity(), codec, value).expect("encode");

    let io = SimulatedIo::new(SEED);
    let root = PathBuf::from(ROOT);
    io.create_directory(&root).expect("directory");
    let path = root.join("segment");
    let file = io.create_file(&path, ByteCount(0)).expect("create");
    encoder.write_to(&file, FileOffset(LEADING_BYTES), &shape, value).expect("write");

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
    read_declared(stored, who, Codec::None)
}

fn read_declared(stored: &Stored, who: &RecordIdentity, declared: Codec) -> (RecordRead, Vec<u8>) {
    let file = open(stored);
    let mut reader = RecordReader::new();
    let mut buf = Vec::new();
    let read = in_place(reader.read_value(&InPlace, &file, stored.location, who, declared, &mut buf)).expect("read");
    (read, buf)
}

fn read_window(stored: &Stored, window: ValueWindow) -> (RecordRead, Vec<u8>) {
    let file = open(stored);
    let mut reader = RecordReader::new();
    let mut buf = Vec::new();
    let read = in_place(reader.read_window(&InPlace, &file, stored.location, &identity(), Codec::None, window, &mut buf)).expect("read");
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

// a window of a large value is a few reads sent together, whatever the value's size. A value stored as written reads its head
// and table, the touched chunks' sums and the touched 4 KiB pieces, and a compressed one its front and one chunk
#[test]
fn window_cost() {
    let value = vec![9; LARGEST.0 as usize];
    for codec in [Codec::None, Codec::Lz4] {
        let stored = store(codec, &value);
        let (reads, bytes, buf) = window_totals(&stored, codec, window(5 * CHUNK_BYTES + 10, 4_096));

        assert_eq!(buf, &value[5 * CHUNK_BYTES + 10..5 * CHUNK_BYTES + 10 + 4_096]);
        let most = match codec {
            Codec::None => written_head(&stored) + CHUNK_SUMS + 2 * PIECE_BYTES as u64,
            Codec::Lz4 => (FIRST_READ_BYTES + CHUNK_BYTES) as u64,
        };
        assert!(reads <= 3, "{codec:?} took {reads} reads");
        assert!(bytes <= most, "{codec:?} read {bytes} B");
    }
}

// a window over two chunks of a value stored as written reads both chunks' sums and only the pieces it touches
#[test]
fn window_over_two_chunks() {
    let value = noise(LARGEST.0 as usize);
    let stored = store(Codec::None, &value);
    let start = 3 * CHUNK_BYTES - 100;

    let (reads, bytes, buf) = window_totals(&stored, Codec::None, window(start, 4_096));

    assert_eq!(buf, &value[start..start + 4_096]);
    assert_eq!(reads, 3);
    assert!(bytes <= written_head(&stored) + 2 * CHUNK_SUMS + 2 * PIECE_BYTES as u64, "read {bytes} B");
}

// a 4 KiB window of a 1 MiB value reads its front and one or two pieces, under 12 KiB
#[test]
fn window_reads_its_pieces() {
    let value = noise(1024 * 1024);
    let stored = store(Codec::None, &value);

    let (_reads, bytes, buf) = window_totals(&stored, Codec::None, window(300_000, 4_096));

    assert_eq!(buf, &value[300_000..304_096]);
    assert!(bytes < 12 * 1024, "read {bytes} B");
}

// windows at every edge give the value's bytes: inside a piece, across pieces and chunks, the short last piece and chunk,
// and past the end
#[test]
fn window_piece_edges() {
    let value = noise(3 * CHUNK_BYTES + PIECE_BYTES + 100);
    let stored = store(Codec::None, &value);
    let end = value.len();
    let edges = [
        (10, 100),
        (PIECE_BYTES - 1, 2),
        (PIECE_BYTES, PIECE_BYTES),
        (CHUNK_BYTES - 5, 10),
        (CHUNK_BYTES - PIECE_BYTES, 3 * PIECE_BYTES),
        (end - 50, 50),
        (end - 50, 500),
        (end - PIECE_BYTES - 100, PIECE_BYTES + 100),
        (0, end),
        (end, 10),
        (end + 10, 10),
    ];
    for (offset, length) in edges {
        let (read, bytes) = read_window(&stored, window(offset, length));
        let from = offset.min(end);
        let to = offset.saturating_add(length).min(end);
        assert_eq!(read, RecordRead::Valid, "window {offset} + {length}");
        assert_eq!(bytes, &value[from..to], "window {offset} + {length}");
    }
}

// a rotten byte fails the windows that read it: in a touched piece, or in the touched chunk's sums. One in a piece the
// window didn't read leaves the window whole, and the whole read finds it
#[test]
fn window_rotten_chunk() {
    let value = noise(LARGEST.0 as usize);
    let stored = store(Codec::None, &value);
    let shape = written_shape(stored.location.length).expect("stored as written");
    rot_at(&stored, &stored.path, (shape.front_length + 2 * CHUNK_BYTES + 7) as u64);

    let (read, _bytes) = read_window(&stored, window(2 * CHUNK_BYTES, 100));
    let (beside, bytes) = read_window(&stored, window(2 * CHUNK_BYTES + PIECE_BYTES, 100));
    let (whole, _value) = read_all(&stored, &identity());

    assert_eq!(read, RecordRead::Failed);
    assert_eq!(beside, RecordRead::Valid);
    assert_eq!(bytes, &value[2 * CHUNK_BYTES + PIECE_BYTES..2 * CHUNK_BYTES + PIECE_BYTES + 100]);
    assert_eq!(whole, RecordRead::Failed);
}

// a rotten byte in a chunk's sums fails a window over that chunk and the whole read, and leaves windows over other chunks whole
#[test]
fn window_rotten_sums() {
    let value = noise(4 * CHUNK_BYTES);
    let stored = store(Codec::None, &value);
    let shape = written_shape(stored.location.length).expect("stored as written");
    rot_at(&stored, &stored.path, (shape.sums_at + CHUNK_SUMS as usize + 3) as u64);

    let (read, _bytes) = read_window(&stored, window(CHUNK_BYTES + 10, 100));
    let (beside, bytes) = read_window(&stored, window(2 * CHUNK_BYTES + 10, 100));

    assert_eq!(read, RecordRead::Failed);
    assert_eq!(beside, RecordRead::Valid);
    assert_eq!(bytes, &value[2 * CHUNK_BYTES + 10..2 * CHUNK_BYTES + 110]);
    let (whole, _value) = read_all(&stored, &identity());
    assert_eq!(whole, RecordRead::Failed);
}

// a record a merge copies as stored reads back whole and by window
#[test]
fn copied_record_reads_back() {
    let value = noise(2 * CHUNK_BYTES + 777);
    let stored = store(Codec::None, &value);
    let file = open(&stored);
    let mut copied = vec![0; stored.location.length.0 as usize];
    file.read_exact_at(stored.location.offset, &mut copied).expect("read");
    let copy_path = PathBuf::from(format!("{ROOT}/copy"));
    let copy = stored.io.create_file(&copy_path, ByteCount(0)).expect("create");
    copy.write_at(FileOffset(LEADING_BYTES), &[IoSlice::new(&copied)]).expect("write");
    let moved = Stored { io: stored.io.clone(), path: copy_path, location: stored.location };

    let (whole, bytes) = read_all(&moved, &identity());
    let (part, window_bytes) = read_window(&moved, window(CHUNK_BYTES + 5, 9_000));

    assert_eq!((whole, bytes.as_slice()), (RecordRead::Valid, value.as_slice()));
    assert_eq!((part, window_bytes.as_slice()), (RecordRead::Valid, &value[CHUNK_BYTES + 5..CHUNK_BYTES + 9_005]));
}

/// The sums of one full chunk's pieces
const CHUNK_SUMS: u64 = (PIECES_PER_CHUNK * PIECE_SUM_BYTES) as u64;

// the head and table of a value stored as written, all a window reads ahead of the sums it needs
fn written_head(stored: &Stored) -> u64 {
    written_shape(stored.location.length).expect("stored as written").sums_at as u64
}

// the reads and bytes one window takes, and what it gave back
fn window_totals(stored: &Stored, declared: Codec, window: ValueWindow) -> (u64, u64, Vec<u8>) {
    let file = open(stored);
    let mut reader = RecordReader::new();
    let mut buf = Vec::new();
    let (reads_before, bytes_before) = stored.io.read_totals();
    let read = in_place(reader.read_window(&InPlace, &file, stored.location, &identity(), declared, window, &mut buf)).expect("read");
    let (reads_after, bytes_after) = stored.io.read_totals();
    assert_eq!(read, RecordRead::Valid);
    (reads_after.0 - reads_before.0, bytes_after.0 - bytes_before.0, buf)
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

fn count_reads(stored: &Stored, declared: Codec) -> u64 {
    let (before, _bytes) = stored.io.read_totals();
    let (read, _value) = read_declared(stored, &identity(), declared);
    let (after, _bytes) = stored.io.read_totals();
    assert_eq!(read, RecordRead::Valid);
    after.0 - before.0
}

// a whole value stored as written is one read, whatever its size
#[test]
fn one_read() {
    for length in [1_200, CHUNK_BYTES, CHUNK_BYTES + 1, 256 * CHUNK_BYTES] {
        let stored = store(Codec::None, &noise(length));

        assert_eq!(count_reads(&stored, Codec::None), 1);
    }
}

// a column that changed its codec still reads every record that was written before
#[test]
fn changed_codec() {
    let value = compressible(4 * CHUNK_BYTES);
    for (written, declared) in [(Codec::Lz4, Codec::None), (Codec::None, Codec::Lz4)] {
        let stored = store(written, &value);

        let (read, restored) = read_declared(&stored, &identity(), declared);

        assert_eq!(read, RecordRead::Valid);
        assert_eq!(restored, value);
    }
}

// a table that lists a span shorter than its chunk fails, so no stale buffer bytes come back
#[test]
fn short_span() {
    let value = noise(3 * CHUNK_BYTES);
    let largest = ByteCount(value.len() as u64);
    let mut encoder = RecordEncoder::new(largest, 0);
    let mut shape = encoder.encode(&identity(), Codec::None, &value).expect("encode");

    let chunks = chunk_count(largest);
    let head_length = chunked_head_bytes(largest);
    let payload_start = (head_length + shape.table_length) as u64;
    let short = CHUNK_BYTES - 100;
    let cut = ChunkEntry {
        stored_offset: FileOffset(payload_start),
        value_length: ByteCount(CHUNK_BYTES as u64),
        checksum: checksum(&value[..short]),
    };
    let moved = ChunkEntry {
        stored_offset: FileOffset(payload_start + short as u64),
        value_length: ByteCount(CHUNK_BYTES as u64),
        checksum: Checksum::default(),
    };
    encoder.write_table_entry(0, &cut);
    encoder.write_table_entry(1, &moved);
    encoder.write_chunked_head(&identity(), Codec::None, largest, chunks, head_length);
    shape.is_payload_stored = false;

    let io = SimulatedIo::new(SEED);
    let root = PathBuf::from(ROOT);
    io.create_directory(&root).expect("directory");
    let path = root.join("segment");
    let file = io.create_file(&path, ByteCount(0)).expect("create");
    file.write_at(FileOffset(0), &encoder.parts(&shape, &value)).expect("write");
    let stored = Stored {
        io,
        path,
        location: RecordLocation {
            offset: FileOffset(0),
            length: shape.total_length(),
        },
    };

    let (read, _bytes) = read_window(&stored, window(0, 10));
    assert_eq!(read, RecordRead::Failed);
    let (read, _value) = read_all(&stored, &identity());
    assert_eq!(read, RecordRead::Failed);
}

// an lz4 value nothing shrank stores as written, so it reads in two reads
#[test]
fn nothing_shrank() {
    let value = noise(32 * CHUNK_BYTES);
    let stored = store(Codec::Lz4, &value);

    assert_eq!(count_reads(&stored, Codec::Lz4), 2);
    assert_eq!(count_reads(&stored, Codec::None), 1);
    let (read, restored) = read_declared(&stored, &identity(), Codec::Lz4);
    assert_eq!(read, RecordRead::Valid);
    assert_eq!(restored, value);
}

// a record checks only for the write that made it, so bytes left at a reused offset fail
#[test]
fn wrong_place() {
    let stored = store(Codec::None, b"value");
    let mut other = identity();
    other.place = WritePlace(1);

    let (read, _bytes) = read_all(&stored, &other);

    assert_eq!(read, RecordRead::Failed);
    assert_eq!(read_all(&stored, &identity()).0, RecordRead::Valid);
}

// the length a batch adds up with no hashing is the length the encoder lays out, on either side of a chunk
#[test]
fn plain_lengths() {
    let identity = RecordIdentity {
        record_key: RecordKey(1),
        column: ColumnId(1),
        key: b"key",
        kind: RowKind::Value,
        place: WritePlace(1),
    };
    let mut encoder = RecordEncoder::new(ByteCount(PLAIN_MOST as u64), 0);
    for length in [0, 1, CHUNK_BYTES - 1, CHUNK_BYTES, CHUNK_BYTES + 1, CHUNK_BYTES * 3, PLAIN_MOST] {
        let value = vec![1; length];

        let shape = encoder.encode(&identity, Codec::None, &value).expect("encode");

        assert_eq!(plain_length(ByteCount(length as u64)), shape.total_length(), "{length} bytes");
    }
}

// a head built on the stack is the head the encoder lays out, so a get can't tell which writer made a record
#[test]
fn stack_head() {
    let identity = RecordIdentity {
        record_key: RECORD_KEY,
        column: COLUMN,
        key: KEY,
        kind: RowKind::Value,
        place: WritePlace(7),
    };
    let mut encoder = RecordEncoder::new(LARGEST, 0);
    for length in [0, 1, CHUNK_BYTES] {
        let value = vec![3; length];
        let shape = encoder.encode(&identity, Codec::None, &value).expect("encode");

        let parts = encoder.parts(&shape, &value);

        let head = PlainRecord::new(&identity, &value).head(RECORD_KEY, identity.place);
        assert_eq!(&*parts[0], head.as_slice(), "{length} bytes");
    }
}

// chunks that take turns shrinking would need a part each: past the limit the rest stay as written, one write call
// holds the record, and every byte reads back
#[test]
fn run_limit() {
    let plain = noise(CHUNK_BYTES);
    let packed = compressible(CHUNK_BYTES);
    let mut value = Vec::with_capacity(LZ4_MOST as usize);
    while value.len() < LZ4_MOST as usize {
        value.extend_from_slice(&packed);
        value.extend_from_slice(&plain);
    }
    let pool = Pool::new(vec![vec![0; SHARE_BYTES]; LZ4_MOST as usize / SHARE_BYTES]);
    let mut encoder = RecordEncoder::new(ByteCount(LZ4_MOST), LZ4_MOST as usize / SHARE_BYTES);
    assert!(encoder.take_shares(&pool, Codec::Lz4, &value));

    encoder.encode(&identity(), Codec::Lz4, &value).expect("encode");

    assert_eq!(encoder.run_count(), MOST_RECORD_RUNS);
    encoder.give_shares(&pool);
    let stored = store(Codec::Lz4, &value);
    let (read, buf) = read_declared(&stored, &identity(), Codec::Lz4);
    assert_eq!(read, RecordRead::Valid);
    assert!(buf == value);
}

// compressed chunks cross from one share to the next, in a value larger than a share
#[test]
fn across_shares() {
    let mut chunk = noise(CHUNK_BYTES);
    chunk[..CHUNK_BYTES / LITTLE_SHRINK].fill(0);
    let value = chunk.repeat(5 * SHARE_BYTES / CHUNK_BYTES);

    let stored = store(Codec::Lz4, &value);

    let (read, buf) = read_declared(&stored, &identity(), Codec::Lz4);
    assert_eq!(read, RecordRead::Valid);
    assert!(buf == value);
}

// a value past the shares a pool holds is refused, and takes none of them
#[test]
fn too_many_shares() {
    let pool = Pool::new(vec![vec![0; SHARE_BYTES]; 1]);
    let mut encoder = RecordEncoder::new(ByteCount(2 * SHARE_BYTES as u64), 1);
    let value = vec![1; 2 * SHARE_BYTES];

    assert!(!encoder.take_shares(&pool, Codec::Lz4, &value));

    assert_eq!(pool.heap_bytes(|share| ByteCount(share.len() as u64)), ByteCount(SHARE_BYTES as u64));
}
