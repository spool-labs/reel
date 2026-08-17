//! What a sealed row carrying its value is worth, and what it costs
//!
//! `row_carry` puts a column's value into the footer row beside its key, so a read that
//! resolves through a footer answers from the row rather than from the record. A
//! partition is fixed stride, so a column declaring a carry pays it on every row: a
//! shorter value pays padding, a tombstone pays it, and a value too long to carry pays
//! the width and carries nothing. Most of the rows here carry a fixed 165 byte value,
//! and a uniform shape must not be read as a verdict on a mixed one.
//!
//! Run the measurements with:
//!
//!   cargo test -p reel --test row_carry --release -- --ignored --nocapture

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use tempfile::TempDir;

use reel::config::{CompactRate, IndexResidency, ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec, MapShape, RecordKey};
use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::io::ReelIo;
use reel::units::ByteCount;
use reel::{CompactPass, KeyWidth, Preallocate, ReelStore, MAP_EVERYTHING};

const ROWS: ColumnId = ColumnId(1);

/// Keys written, spread so every segment's range covers every key
const KEYS: u64 = 6_000;

/// Payload the stride sweep writes, sized so the volume seals several segments
const PAYLOAD: usize = 512;

/// Keys looked up that were never written
const MISSES: u64 = 2_000;

/// A column of a given key width carrying a given number of value bytes
const fn column_of(key_width: u16, row_carry: u16) -> ColumnSpec {
    ColumnSpec {
        id: ROWS,
        name: "rows",
        key_width: KeyWidth::Fixed(key_width),
        shard_bytes: 0,
        inline_max: 0,
        row_carry,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    }
}

/// The same column carrying nothing, which is every row's comparator
const PLAIN: ColumnSet = &[column_of(32, 0)];

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(256 * 1024),
        alloc_chunk: ByteCount::from_bytes(64 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        index: IndexResidency::Paged,
        filter_bits: 0,
        map_above: MAP_EVERYTHING,
        ..ReelConfig::default()
    }
}

/// The key widths the stride sweep walks, standing in for carried value bytes
const W16: ColumnSet = &[column_of(16, 0)];
const W32: ColumnSet = &[column_of(32, 0)];
const W48: ColumnSet = &[column_of(48, 0)];
const W72: ColumnSet = &[column_of(72, 0)];
const W108: ColumnSet = &[column_of(108, 0)];

const STRIDES: &[(u16, ColumnSet)] = &[(16, W16), (32, W32), (48, W48), (72, W72), (108, W108)];

/// A scattered key of a given width, so no segment's range rules another one out
fn wide_key(at: u64, width: usize) -> RecordKey {
    let mixed = at
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .rotate_left(29)
        .wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let mut bytes = vec![0u8; width];
    bytes[..8].copy_from_slice(&mixed.to_be_bytes());
    bytes[8..16].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(ROWS, &bytes).expect("key")
}

/// A paged volume of one column, with the backend kept so its reads can be counted
fn paged_of(columns: ColumnSet, width: usize, payload_len: usize) -> (ReelStore, Arc<SimIo>) {
    let io = Arc::new(SimIo::new(FaultPlan::new(5)));
    let store = ReelStore::open_with_io(
        PathBuf::from("/carry"),
        config(),
        columns,
        Arc::clone(&io) as Arc<dyn ReelIo>,
    )
    .expect("open");

    let payload = vec![0x3Cu8; payload_len];
    for at in 0..KEYS {
        store.put(&wide_key(at, width), &payload).expect("put");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");
    (store, io)
}

/// The value a carrying column is measured against
const VALUE: usize = 165;

/// A column whose sealed rows carry their values, which a paged volume may now hold
///
/// The carry is the value's own width rather than a round number above it, since a
/// ceiling above the values is padding on every row.
const CARRIED: ColumnSet = &[ColumnSpec {
    id: ROWS,
    name: "rows",
    key_width: KeyWidth::Fixed(32),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: VALUE as u16,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

// a paged volume serves a column whose rows carry their values
//
// The pairing `inline_max` refuses: a value carried in RAM needs a resident index, and
// `row_carry` is on disk, so the two ceilings answer to different residencies.
#[test]
fn a_paged_volume_carries_values_in_its_rows() {
    let io = Arc::new(SimIo::new(FaultPlan::new(5)));
    let store = ReelStore::open_with_io(
        PathBuf::from("/carried"),
        config(),
        CARRIED,
        Arc::clone(&io) as Arc<dyn ReelIo>,
    )
    .expect("a paged volume takes a carrying column");

    let payload = vec![0x5Au8; VALUE];
    for at in 0..KEYS {
        store.put(&wide_key(at, 32), &payload).expect("put");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");

    // One pass to bring the blocks in, then a pass that must touch the volume for
    // nothing at all: the row holds the value, so there is no record left to read.
    for at in 0..KEYS {
        let found = store.get(&wide_key(at, 32)).expect("get").expect("present");
        assert_eq!(
            found.as_ref(),
            payload.as_slice(),
            "the value a row carries"
        );
    }

    let before = io.read_count();
    for at in 0..KEYS {
        let found = store.get(&wide_key(at, 32)).expect("get").expect("present");
        assert_eq!(
            found.as_ref(),
            payload.as_slice(),
            "the value a row carries"
        );
    }
    let reads = io.read_count() - before;

    // A record read is two reads here, the header and key into one buffer and the
    // payload into another, so a volume that carried nothing has a floor of two a key.
    // What is left over is the open tail, whose keys have no sealed row.
    let floor = KEYS * 2;
    assert!(
        reads * 20 < floor,
        "a carried row should leave nearly no read to make, took {reads} against {floor}"
    );

    assert!(store.get(&wide_key(KEYS + 1, 32)).expect("get").is_none());
}

// a carried row is not believed once a newer version or a delete stands over it
//
// A row still holds the old value after the key moved on, and on a paged volume the row
// is the only thing the search finds, so nothing else would catch a read that answered
// from it without asking what came later.
#[test]
fn a_carried_row_is_not_believed_over_a_newer_version() {
    let io = Arc::new(SimIo::new(FaultPlan::new(5)));
    let store = ReelStore::open_with_io(
        PathBuf::from("/carried-versions"),
        config(),
        CARRIED,
        Arc::clone(&io) as Arc<dyn ReelIo>,
    )
    .expect("open");

    let first = vec![0x11u8; VALUE];
    let second = vec![0x22u8; VALUE];

    // Every key written, sealed and handed to its footer, so the row is what a read
    // finds and the map holds nothing for it.
    for at in 0..KEYS {
        store.put(&wide_key(at, 32), &first).expect("put");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");

    // Half are overwritten and a quarter deleted, both after the rows were sealed.
    for at in 0..KEYS / 2 {
        store.put(&wide_key(at, 32), &second).expect("overwrite");
    }
    for at in KEYS / 2..KEYS * 3 / 4 {
        store.delete(&wide_key(at, 32)).expect("delete");
    }

    for at in 0..KEYS {
        let found = store.get(&wide_key(at, 32)).expect("get");
        match at {
            at if at < KEYS / 2 => assert_eq!(
                found.expect("the newer version").as_ref(),
                second.as_slice(),
                "an overwritten key must not answer from the row it used to have"
            ),
            at if at < KEYS * 3 / 4 => {
                assert!(
                    found.is_none(),
                    "a deleted key must not answer from its row"
                )
            }
            _ => assert_eq!(found.expect("untouched").as_ref(), first.as_slice()),
        }
    }

    // And again once the newer versions have sealed too, where both versions are rows
    // and only the sequence number orders them.
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");
    for at in 0..KEYS {
        let found = store.get(&wide_key(at, 32)).expect("get");
        match at {
            at if at < KEYS / 2 => {
                assert_eq!(
                    found.expect("newer").as_ref(),
                    second.as_slice(),
                    "sealed newer row"
                )
            }
            at if at < KEYS * 3 / 4 => {
                assert!(found.is_none(), "sealed tombstone over a carried row")
            }
            _ => assert_eq!(found.expect("untouched").as_ref(), first.as_slice()),
        }
    }
}

// a cue point reads the version its snapshot held, not the one the row now carries
//
// `get_carried` is the only door that asks for a row's value and `get_at` passes none,
// so a snapshot read of a carried key has to come off the record.
#[test]
fn a_cue_point_ignores_a_newer_carried_row() {
    let io = Arc::new(SimIo::new(FaultPlan::new(5)));
    let store = ReelStore::open_with_io(
        PathBuf::from("/carry-cue"),
        config(),
        CARRIED,
        Arc::clone(&io) as Arc<dyn ReelIo>,
    )
    .expect("open");

    let first = vec![0x11u8; VALUE];
    let second = vec![0x22u8; VALUE];

    for at in 0..KEYS {
        store.put(&wide_key(at, 32), &first).expect("put");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");

    // The cue is taken while the first version is what every row carries.
    let cue = store.cue().expect("cue");

    for at in 0..KEYS {
        store.put(&wide_key(at, 32), &second).expect("overwrite");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");

    for at in 0..KEYS {
        let now = store.get(&wide_key(at, 32)).expect("get").expect("present");
        assert_eq!(now.as_ref(), second.as_slice(), "the live version");

        let held = store
            .get_at(&wide_key(at, 32), &cue)
            .expect("get at")
            .expect("held at the cue");
        assert_eq!(held.as_ref(), first.as_slice(), "the version the cue held");
    }
}

// a compaction carries the carry forward, so a copied row still answers from itself
//
// A copy that dropped the carry would answer correctly and quietly cost every later
// read an io, which no assertion about values would catch.
#[test]
fn a_compaction_carries_the_carry_forward() {
    let io = Arc::new(SimIo::new(FaultPlan::new(5)));
    let store = ReelStore::open_with_io(
        PathBuf::from("/carry-compact"),
        config(),
        CARRIED,
        Arc::clone(&io) as Arc<dyn ReelIo>,
    )
    .expect("open");

    let payload = vec![0x5Au8; VALUE];
    // Twice, so the first copy of every key is dead and a pass has work.
    for round in 0..2u8 {
        let body = vec![0x5Au8 + round; VALUE];
        for at in 0..KEYS {
            store.put(&wide_key(at, 32), &body).expect("put");
        }
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");

    let mut copied = 0u32;
    for _ in 0..8 {
        if matches!(store.compact_once().expect("compact"), CompactPass::Copied) {
            copied += 1;
        }
        store.flush().expect("flush");
        store.page_out_sealed().expect("page out");
    }
    assert!(copied > 0, "the volume has to have copied something");

    let live = vec![0x5Au8 + 1; VALUE];
    // One warm pass, then a pass that must reach the volume for almost nothing.
    for at in 0..KEYS {
        assert_eq!(
            store
                .get(&wide_key(at, 32))
                .expect("get")
                .expect("present")
                .as_ref(),
            live.as_slice()
        );
    }
    let before = io.read_count();
    for at in 0..KEYS {
        assert_eq!(
            store
                .get(&wide_key(at, 32))
                .expect("get")
                .expect("present")
                .as_ref(),
            live.as_slice()
        );
    }
    let reads = io.read_count() - before;
    assert!(
        reads * 20 < KEYS * 2,
        "a relocated carried row should still answer from itself, took {reads} reads"
    );
    drop(payload);
}

// a clustered volume turns each sealed segment into a sorted run and drops the log
//
// What makes a carrying column affordable: without the rewrite the value is on disk
// twice, once in the record and once in the row, and the segment never stops holding
// both. What `rewrite_on_seal` adds to ordinary compaction is a reason to select a
// segment that has no dead bytes, and a tail of its own so a foreground put cannot land
// between two records of a run.
#[test]
fn a_clustered_volume_sorts_its_sealed_segments() {
    let io = Arc::new(SimIo::new(FaultPlan::new(5)));
    let store = ReelStore::open_with_io(
        PathBuf::from("/clustered"),
        ReelConfig {
            rewrite_on_seal: true,
            // The gate off: a gated pass answers `Held` and a caller driving compaction
            // to exhaustion cannot tell that from work remaining.
            compact_mbps: CompactRate::Mbps(100_000),
            compact_dead_ratio: 1.0,
            ..config()
        },
        CARRIED,
        Arc::clone(&io) as Arc<dyn ReelIo>,
    )
    .expect("open");

    let payload = vec![0x5Au8; VALUE];
    for at in 0..KEYS {
        store.put(&wide_key(at, 32), &payload).expect("put");
    }
    store.flush().expect("flush");

    // The rewrite is a compaction pass, so it runs where compaction runs.
    let mut passes = 0u32;
    for _ in 0..64 {
        match store.compact_once().expect("compact") {
            CompactPass::Copied => passes += 1,
            CompactPass::Idle => break,
            CompactPass::Held => {}
        }
        store.flush().expect("flush");
        store.page_out_sealed().expect("page out");
    }
    assert!(passes > 0, "a clustered volume has to rewrite something");

    // Every key still answers, which is the only thing a rewrite may not change.
    for at in 0..KEYS {
        assert_eq!(
            store
                .get(&wide_key(at, 32))
                .expect("get")
                .expect("present")
                .as_ref(),
            payload.as_slice(),
            "a key the rewrite moved"
        );
    }

    // And the volume settles: once every run is sorted there is nothing left to select,
    // so the sorted-order signal terminates rather than churning forever.
    let mut idle = 0u32;
    for round in 0..8 {
        let pass = store.compact_once().expect("compact");
        let _ = round;
        if matches!(pass, CompactPass::Idle) {
            idle += 1;
        }
        store.flush().expect("flush");
        store.page_out_sealed().expect("page out");
    }
    assert!(
        idle > 0,
        "a clustered volume must run out of work rather than loop"
    );
}

/// Bytes a real directory holds, measured rather than computed from a row width
fn volume_bytes(dir: &Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(at) = stack.pop() {
        for entry in std::fs::read_dir(&at).expect("read dir").flatten() {
            let meta = entry.metadata().expect("metadata");
            match meta.is_dir() {
                true => stack.push(entry.path()),
                // Apparent length rather than blocks, because a reel reserves space
                // ahead of its write head and the reservation is not what it holds.
                false => total += meta.len(),
            }
        }
    }
    total
}

/// What a carry costs on disk, and what the clustered seal takes back
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn carry_costs_on_disk() {
    const VALUE_LEN: usize = 165;
    let payload = vec![0x5Au8; VALUE_LEN];
    let values = KEYS as usize * VALUE_LEN;

    println!();
    println!(
        "{:>26} {:>12} {:>10} {:>12}",
        "volume", "bytes", "per key", "over values"
    );

    for (label, columns, rewrite) in [
        ("plain, no carry", PLAIN, false),
        ("carrying", CARRIED, false),
        ("carrying, rewritten", CARRIED, true),
    ] {
        let dir = TempDir::new().expect("tempdir");
        let store = ReelStore::open(
            dir.path().to_path_buf(),
            ReelConfig {
                rewrite_on_seal: rewrite,
                compact_mbps: CompactRate::Mbps(100_000),
                compact_dead_ratio: 1.0,
                ..config()
            },
            columns,
        )
        .expect("open");

        for at in 0..KEYS {
            store.put(&wide_key(at, 32), &payload).expect("put");
        }
        store.flush().expect("flush");
        store.page_out_sealed().expect("page out");

        let mut copied = 0u32;
        if rewrite {
            // Drive the rewrite to exhaustion, which is where the log copies go.
            for _ in 0..64 {
                match store.compact_once().expect("compact") {
                    CompactPass::Idle => break,
                    CompactPass::Copied => copied += 1,
                    CompactPass::Held => {}
                }
                store.flush().expect("flush");
                store.page_out_sealed().expect("page out");
            }
            assert!(
                copied > 0,
                "the rewrite has to have run for this row to mean anything"
            );
        }

        // Every key still answers, whichever state the volume is in.
        for at in 0..KEYS {
            assert_eq!(
                store
                    .get(&wide_key(at, 32))
                    .expect("get")
                    .expect("present")
                    .as_ref(),
                payload.as_slice()
            );
        }

        let bytes = volume_bytes(dir.path());
        println!(
            "{label:>26} {bytes:>12} {:>10.1} {:>11.2}x  passes={copied}",
            bytes as f64 / KEYS as f64,
            bytes as f64 / values as f64,
        );
        drop(store);
    }
}

/// A column carrying 200 bytes, met by values that mostly are not 200 bytes
const MIXED: ColumnSet = &[column_of(32, 200)];

/// The shape a real distribution has, scaled to fit a test segment
///
/// The large classes stand at one size above the carry rather than at their real widths,
/// since the question is what happens either side of the ceiling, not how far past it.
const MIXED_SIZES: [usize; 4] = [0, 165, 200, 4096];
const MIXED_WEIGHTS: [u64; 4] = [3, 75, 20, 2];

/// The size the nth key of a mixed run takes
fn mixed_size(at: u64) -> usize {
    let total: u64 = MIXED_WEIGHTS.iter().sum();
    let mut point = at % total;
    for (size, weight) in MIXED_SIZES.iter().zip(MIXED_WEIGHTS) {
        if point < weight {
            return *size;
        }
        point -= weight;
    }
    MIXED_SIZES[0]
}

// a mixed distribution reads back whole, and pays the stride on every row either way
//
// The carry is paid per row and not per carried value: an empty value pays 200 bytes of
// padding, a 165 byte value pays 35, and a value past the ceiling pays the full 200 and
// carries nothing, so it is read from the record as though the column carried none.
#[test]
fn a_mixed_shape_pays_its_padding() {
    const CARRY: usize = 200;
    let io = Arc::new(SimIo::new(FaultPlan::new(5)));
    let store = ReelStore::open_with_io(
        PathBuf::from("/carry-mixed"),
        config(),
        MIXED,
        Arc::clone(&io) as Arc<dyn ReelIo>,
    )
    .expect("open");

    let mut carried = 0usize;
    let mut past = 0u64;
    for at in 0..KEYS {
        let size = mixed_size(at);
        let payload = vec![(at & 0xff) as u8; size];
        store.put(&wide_key(at, 32), &payload).expect("put");
        match size <= CARRY {
            true => carried += size,
            false => past += 1,
        }
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");

    // Both populations: a value the row holds and a value the row was too narrow for
    // have to answer identically.
    for at in 0..KEYS {
        let size = mixed_size(at);
        let found = store
            .get(&wide_key(at, 32))
            .expect("get")
            .expect("every key that was written");
        assert_eq!(found.as_ref().len(), size, "the length a mixed row answers");
        assert!(
            found.as_ref().iter().all(|byte| *byte == (at & 0xff) as u8),
            "the bytes a mixed row answers"
        );
    }
    assert!(
        past > 0,
        "the run has to reach past the ceiling to prove anything"
    );

    // Every row pays the carry whatever it used of it.
    let reserved = KEYS as usize * CARRY;
    let padding = reserved - carried;
    println!(
        "mixed rows {KEYS}, carried {carried} B of {reserved} B reserved, \
         {padding} B padding, {:.0}% wasted, {past} rows past the ceiling",
        padding as f64 * 100.0 / reserved as f64,
    );
    // A tight distribution wastes very little, so what a mixed shape costs is not the
    // padding but the tail: the rows past the ceiling pay the full stride and carry
    // nothing, and every one is a record read the carry did not remove. A wide
    // distribution rather than a tight one is what makes a carry a bad trade.
    assert!(
        padding * 3 < carried,
        "a tight distribution should waste well under a third: {carried} carried \
         against {padding} padded"
    );
}

/// What a row that carries its value takes off a paged read
///
/// Same keys, same payload, same residency, differing in nothing but whether the column
/// asked its rows to carry the value. A record read is two reads here, the header and
/// key into one buffer and the payload into another, so two a hit is the floor for a
/// volume that carries nothing.
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn carry_cost() {
    // libtest leaves the test name line open, so a header printed into it starts a
    // screen-width right of the rows underneath.
    println!();
    println!(
        "{:>8} {:>9} {:>10} {:>12} {:>10} {:>12}",
        "carry", "value", "us a hit", "reads a hit", "B a hit", "row B"
    );

    for (label, columns) in [("none", PLAIN), ("165 B", CARRIED)] {
        let (store, io) = paged_of(columns, 32, VALUE);

        for at in 0..KEYS {
            assert!(store.get(&wide_key(at, 32)).expect("get").is_some());
        }

        let before = io.read_count();
        let before_bytes = io.read_bytes();
        let start = Instant::now();
        for at in 0..KEYS {
            assert!(store.get(&wide_key(at, 32)).expect("get").is_some());
        }
        let elapsed = start.elapsed();
        let reads = (io.read_count() - before) as f64 / KEYS as f64;
        let bytes = (io.read_bytes() - before_bytes) as f64 / KEYS as f64;
        let per_hit = elapsed.as_secs_f64() * 1e6 / KEYS as f64;
        // What the win costs: a carried row is wider by the carry, and the record it
        // duplicates stands until a seal rewrites the segment and unlinks the log copy.
        let row = 32 + 17 + columns[0].row_carry as usize;
        println!("{label:>8} {VALUE:>9} {per_hit:>10.2} {reads:>12.2} {bytes:>10.0} {row:>12}");
    }
}

/// What a paged hit costs as the row it searches gets wider
///
/// `FooterEntry::from_record` clamps a row's carried value to `INLINE_MAX` whatever the
/// column declares, so the curve is drawn with key width instead: a 108 byte key makes
/// the same 125 byte row that 108 bytes of carried value would. Reads a hit should not
/// move with stride, since `BLOCK_ROWS` is a row count; bytes a hit is that count times
/// a block and grows with stride by definition.
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn stride_cost() {
    // libtest leaves the test name line open, so a header printed into it starts a
    // screen-width right of the rows underneath.
    println!();
    println!(
        "{:>6} {:>8} {:>10} {:>10} {:>11} {:>10} {:>12}",
        "key", "stride", "block B", "us a hit", "reads a hit", "B a hit", "reads a miss"
    );

    for &(width, columns) in STRIDES {
        let (store, io) = paged_of(columns, width as usize, PAYLOAD);
        let stride = width as u64 + 17;
        let block = stride * 64;

        // One untimed pass so the footer maps and the first blocks are in hand, since
        // the question is what a hit costs and not what an open costs.
        for at in 0..KEYS {
            assert!(store
                .get(&wide_key(at, width as usize))
                .expect("get")
                .is_some());
        }

        let before = io.read_count();
        let before_bytes = io.read_bytes();
        let start = Instant::now();
        for at in 0..KEYS {
            assert!(store
                .get(&wide_key(at, width as usize))
                .expect("get")
                .is_some());
        }
        let elapsed = start.elapsed();
        let reads = io.read_count() - before;
        let bytes = io.read_bytes() - before_bytes;

        // A miss walks the same blocks and stops there, so the difference between a miss
        // and a hit is the record read a carried value would remove.
        let before = io.read_count();
        for at in KEYS..KEYS + MISSES {
            assert!(store
                .get(&wide_key(at, width as usize))
                .expect("get")
                .is_none());
        }
        let misses = io.read_count() - before;

        let per_hit = elapsed.as_secs_f64() * 1e6 / KEYS as f64;
        let reads_per_hit = reads as f64 / KEYS as f64;
        let reads_per_miss = misses as f64 / MISSES as f64;
        let bytes_per_hit = bytes as f64 / KEYS as f64;
        println!(
            "{width:>6} {stride:>8} {block:>10} {per_hit:>10.2} {reads_per_hit:>11.2} {bytes_per_hit:>10.0} {reads_per_miss:>12.2}"
        );
    }
}

// a rewritten carrying volume keeps the row and drops the record behind it
//
// The rewrite lists the row and writes no record for it, so the row becomes the value's
// only copy. The restart is the sharper half: a row that only reads while the retired
// segment's pages happen to be around is a cache, not a copy.
#[test]
fn a_rewrite_leaves_the_row_as_the_only_copy() {
    const VALUE_LEN: usize = 165;
    const ROWS_WRITTEN: u64 = 4_000;
    let payload = vec![0xC3u8; VALUE_LEN];
    let values = ROWS_WRITTEN * VALUE_LEN as u64;

    let dir = TempDir::new().expect("tempdir");
    let settings = ReelConfig {
        rewrite_on_seal: true,
        compact_mbps: CompactRate::Mbps(100_000),
        compact_dead_ratio: 1.0,
        ..config()
    };

    let store = ReelStore::open(dir.path().to_path_buf(), settings.clone(), CARRIED).expect("open");
    for at in 0..ROWS_WRITTEN {
        store.put(&wide_key(at, 32), &payload).expect("put");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");
    let before = volume_bytes(dir.path());

    let mut passes = 0;
    for _ in 0..64 {
        match store.compact_once().expect("compact") {
            CompactPass::Idle => break,
            CompactPass::Copied => passes += 1,
            CompactPass::Held => {}
        }
        store.flush().expect("flush");
        store.page_out_sealed().expect("page out");
    }
    assert!(
        passes > 0,
        "the rewrite has to have run for the rest to mean anything"
    );

    // The reclaim has to come from listing rather than from ordinary copying: a listed
    // value moves as footer bytes, so it never shows up in the copied bytes.
    let counters = store.compaction_counters();
    assert!(
        counters.rows_listed > 0,
        "no row was listed, so nothing was deleted"
    );
    assert!(
        counters.compaction_bytes < values / 4,
        "the rewrite copied {} record bytes of {values}, so it was not listing them",
        counters.compaction_bytes
    );

    let after = volume_bytes(dir.path());
    // Most of one copy of the values leaves, not all: the tail the puts are still on has
    // no footer yet, so its records are the only copy they have.
    let reclaimed = before.saturating_sub(after);
    assert!(
        reclaimed > values * 3 / 4,
        "rewrite reclaimed {reclaimed} of {values} value bytes, {before} -> {after}"
    );

    // The rows answer while the volume is up.
    for at in 0..ROWS_WRITTEN {
        assert_eq!(
            store
                .get(&wide_key(at, 32))
                .expect("get")
                .expect("present")
                .as_ref(),
            payload.as_slice(),
            "key {at} lost its value to the rewrite"
        );
    }
    drop(store);

    // And they answer after a restart, which is what makes a row a copy.
    let reopened = ReelStore::open(dir.path().to_path_buf(), settings, CARRIED).expect("reopen");
    for at in 0..ROWS_WRITTEN {
        assert_eq!(
            reopened
                .get(&wide_key(at, 32))
                .expect("get")
                .expect("present")
                .as_ref(),
            payload.as_slice(),
            "key {at} did not survive the restart"
        );
    }
}

// a destination mixing listed rows with copied records loses neither on a later pass
//
// A pass walks the source's records and a listed row has none, so a segment holding both
// kinds would retire with its records copied and its rows gone. Nothing books bytes
// against a row, so a destination full of them is invisible to both pickers: the
// overwrite below is what makes the destination selectable at all.
#[test]
fn a_mixed_rewrite_carries_rows_and_records_both() {
    const CARRY: usize = 200;
    let dir = TempDir::new().expect("tempdir");
    let settings = ReelConfig {
        rewrite_on_seal: true,
        compact_mbps: CompactRate::Mbps(100_000),
        ..config()
    };
    let store = ReelStore::open(dir.path().to_path_buf(), settings.clone(), MIXED).expect("open");

    let mut past = 0u64;
    for at in 0..KEYS {
        let size = mixed_size(at);
        store
            .put(&wide_key(at, 32), &vec![(at & 0xff) as u8; size])
            .expect("put");
        if size > CARRY {
            past += 1;
        }
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");
    assert!(
        past > 0,
        "the run has to reach past the ceiling for the mix to exist"
    );

    let drain = |store: &ReelStore| {
        for _ in 0..256 {
            match store.compact_once().expect("compact") {
                CompactPass::Idle => break,
                CompactPass::Copied | CompactPass::Held => {}
            }
            store.flush().expect("flush");
            store.page_out_sealed().expect("page out");
        }
    };

    // First round: every sealed segment is rewritten, so the destinations hold listed
    // rows for the values that fit and copied records for the ones that did not.
    drain(&store);
    let first = store.compaction_counters();
    assert!(first.rows_listed > 0, "the first round listed no row");

    // Kill the copies those rows share a segment with, by writing the wide values again.
    // Same bytes, so the reads below are unchanged, but the old records are shadowed and
    // their destination becomes the emptiest thing in the volume.
    for at in 0..KEYS {
        let size = mixed_size(at);
        if size > CARRY {
            store
                .put(&wide_key(at, 32), &vec![(at & 0xff) as u8; size])
                .expect("overwrite");
        }
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");

    // Second round: the mixed destinations are selected on their dead records, which is
    // the pass that reads a segment holding rows nothing wrote a record for.
    drain(&store);
    let second = store.compaction_counters();
    // Not `rows_listed`, which grows in this round whether a destination was read or not,
    // since the round also reaches originals the first one left held.
    assert!(
        second.segments_rewritten > first.segments_rewritten,
        "the second round rewrote nothing, so it never read a destination: {} then {}",
        first.segments_rewritten,
        second.segments_rewritten
    );

    let check = |store: &ReelStore, label: &str| {
        for at in 0..KEYS {
            let size = mixed_size(at);
            let found = store
                .get(&wide_key(at, 32))
                .expect("get")
                .unwrap_or_else(|| panic!("{label}: key {at} is gone"));
            assert_eq!(found.as_ref().len(), size, "{label}: length of key {at}");
            assert!(
                found.as_ref().iter().all(|byte| *byte == (at & 0xff) as u8),
                "{label}: bytes of key {at}"
            );
        }
    };
    check(&store, "after the rewrite");
    drop(store);

    let reopened = ReelStore::open(dir.path().to_path_buf(), settings, MIXED).expect("reopen");
    check(&reopened, "after a restart");
}

// an overwritten listed row is reclaimed, so a carrying volume does not grow forever
//
// Nothing books bytes against a row, so a segment holding only rows is chosen by the
// accounting rather than by anything written for it: an overwrite shadows that row's
// segment, its dead bytes climb over a live total of zero, and it ranks emptiest.
#[test]
fn an_overwritten_listed_row_is_reclaimed() {
    const ROWS_WRITTEN: u64 = 4_000;
    let first = vec![0xA1u8; VALUE];
    let second = vec![0xB2u8; VALUE];

    let dir = TempDir::new().expect("tempdir");
    let settings = ReelConfig {
        rewrite_on_seal: true,
        compact_mbps: CompactRate::Mbps(100_000),
        ..config()
    };
    let store = ReelStore::open(dir.path().to_path_buf(), settings, CARRIED).expect("open");

    let drain = |store: &ReelStore| {
        for _ in 0..256 {
            match store.compact_once().expect("compact") {
                CompactPass::Idle => break,
                CompactPass::Copied | CompactPass::Held => {}
            }
            store.flush().expect("flush");
            store.page_out_sealed().expect("page out");
        }
    };

    for at in 0..ROWS_WRITTEN {
        store.put(&wide_key(at, 32), &first).expect("put");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");
    drain(&store);
    let one_copy = volume_bytes(dir.path());
    assert!(
        store.compaction_counters().rows_listed > 0,
        "nothing was listed"
    );

    for at in 0..ROWS_WRITTEN {
        store.put(&wide_key(at, 32), &second).expect("overwrite");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");
    drain(&store);
    let after = volume_bytes(dir.path());

    for at in 0..ROWS_WRITTEN {
        assert_eq!(
            store
                .get(&wide_key(at, 32))
                .expect("get")
                .expect("present")
                .as_ref(),
            second.as_slice(),
            "key {at} did not take its overwrite"
        );
    }

    println!(
        "listed {ROWS_WRITTEN} keys: {one_copy} bytes, overwritten and reclaimed: {after} bytes, \
         {:.2}x",
        after as f64 / one_copy as f64
    );
    // A volume that reclaimed nothing would hold both generations, so the ceiling is one
    // copy and a margin for the open tail.
    assert!(
        after < one_copy * 3 / 2,
        "the stale rows were not reclaimed: {one_copy} bytes became {after}"
    );
}

// a listed row answers a walk, both as a key and as a value
//
// The two paths come apart differently: the key path can lose a segment the merge never
// opened, and the value path can emit a key and then produce nothing for it.
#[test]
fn a_listed_row_is_found_by_a_walk() {
    use reel_core::Store as _;

    const ROWS_WRITTEN: u64 = 2_000;
    let payload = vec![0x7Eu8; VALUE];
    let dir = TempDir::new().expect("tempdir");
    let settings = ReelConfig {
        rewrite_on_seal: true,
        compact_mbps: CompactRate::Mbps(100_000),
        ..config()
    };
    let store = ReelStore::open(dir.path().to_path_buf(), settings, CARRIED).expect("open");

    for at in 0..ROWS_WRITTEN {
        store.put(&wide_key(at, 32), &payload).expect("put");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");
    for _ in 0..128 {
        match store.compact_once().expect("compact") {
            CompactPass::Idle => break,
            CompactPass::Copied | CompactPass::Held => {}
        }
        store.flush().expect("flush");
        store.page_out_sealed().expect("page out");
    }
    assert!(
        store.compaction_counters().rows_listed > 0,
        "nothing was listed"
    );

    let keys = store.iter_keys_prefix("rows", &[]).expect("walk the keys");
    assert_eq!(keys.len() as u64, ROWS_WRITTEN, "the key walk lost rows");

    let counted = store.count_prefix("rows", &[]).expect("count");
    assert_eq!(counted, ROWS_WRITTEN, "the count path disagrees");

    let mut seen = 0u64;
    for (_key, value) in store.iter("rows").expect("walk the values") {
        assert_eq!(value.as_ref(), payload.as_slice(), "a walked value");
        seen += 1;
    }
    assert_eq!(seen, ROWS_WRITTEN, "the value walk lost rows");
}
