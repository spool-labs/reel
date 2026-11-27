//! Free in place: a large dead record gives its blocks back with no copy, and the segment keeps its length and footer

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::compact_job::CompactJob;
use reel::engine::cue::MOST_CUES;
use reel::engine::open_segment::data_path;
use reel::engine::read_path::{MOST_READ_TRIES, RecordWant, SealedRow};
use reel::engine::reel_store::ReelStore;
use reel::engine::rewrite_job::freed_in_place;
use reel::engine::walk_job::{FREE_BLOCK_BYTES, FREE_IN_PLACE_BYTES, FreeRecord, MOST_BLOCK_FREES};
use reel::guard::hold;
use reel::io::{InPlace, in_place};
use reel::record::codec::Codec;
use reel::record::record_head::SMALL_HEAD_BYTES;
use reel::segment::segment_tail::SegmentNumber;
use reel_core::column_id::ColumnId;
use reel_core::direction::Direction;
use reel_core::scan::ScanStep;
use reel_core::store_error::StoreError;
use reel_core::units::{ByteCount, FileOffset, Nanoseconds, VolumeId, WritePlace};
use reel_core::value_list::{ReadBuffers, ValueList};
use reel_sim::file_image::BitMask;
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/free";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(64 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 20_000;
const CRASH_POINTS: u64 = 60;
const HELD_RECORDS: u64 = 2 * MOST_BLOCK_FREES as u64;
const PASS_STEPS: u32 = 4;

type Store = ReelStore<SimulatedIo>;

fn open(io: &SimulatedIo) -> Store {
    let config = StoreConfig {
        memory_budget: BUDGET,
        max_memory: None,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "free".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None, segment_bytes: None, lifetime_groups: None,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
            index: ColumnIndex::Plain,
            least_rows: None,
            least_value: None,
        }],
    };
    let store = ReelStore::open(io.clone(), &config).expect("open");
    // these segments keep the few dead rows they were built with
    for volume in &store.state.volumes {
        volume.holds.reclaim.store(true, Ordering::Release);
    }
    store
}

fn settle(io: &SimulatedIo) {
    for _step in 0..STEPS {
        if io.step_any().is_none() {
            io.advance_clock(TICK);
            if io.step_any().is_none() {
                return;
            }
        }
    }
    panic!("jobs never went quiet");
}

fn seal(store: &Store, io: &SimulatedIo) -> SegmentNumber {
    let number = hold(&store.state.volumes[0].claims).open[0].number;
    store.seal_now();
    settle(io);
    cool(store, io);
    number
}

// keep a segment's dead share under what triggers a rewrite
fn keep_live(store: &Store) {
    store.put(COLUMN, b"keep", b"value").expect("put");
    store.put(COLUMN, b"keep-1", &large(3)).expect("put");
    store.put(COLUMN, b"keep-2", &large(4)).expect("put");
    store.put(COLUMN, b"keep-3", &large(5)).expect("put");
    store.put(COLUMN, b"keep-4", &large(6)).expect("put");
}

fn large(byte: u8) -> Vec<u8> {
    vec![byte; FREE_IN_PLACE_BYTES.0 as usize]
}

fn read(store: &Store, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    store.get(COLUMN, key, &mut buf).expect("get").then_some(buf)
}

// a large record that goes dead gives its blocks back, its segment keeps its live row and its length, and a reopen is clean
#[test]
fn freed() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    keep_live(&store);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    let older = seal(&store, &io);
    store.put(COLUMN, b"key", &large(2)).expect("put");
    seal(&store, &io);

    let volume = &store.state.volumes[0];
    assert!(volume.table.holds(older));
    let whole_blocks = FREE_IN_PLACE_BYTES.0 - FREE_BLOCK_BYTES;
    assert!(volume.bytes_freed.load(Ordering::Acquire) >= whole_blocks);
    assert_eq!(io.freed_bytes().0, volume.bytes_freed.load(Ordering::Acquire));
    assert_eq!(read(&store, b"key"), Some(large(2)));
    assert_eq!(read(&store, b"keep"), Some(b"value".to_vec()));
    store.close().expect("close");

    let store = open(&io);
    settle(&io);
    assert_eq!(read(&store, b"key"), Some(large(2)));
    assert!(hold(&store.state.volumes[0].damaged).is_empty());
    store.close().expect("close");
}

// a dead record under the size waits for the rewrite trigger
#[test]
fn small_stays() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    let short = vec![1; FREE_IN_PLACE_BYTES.0 as usize / 2];
    keep_live(&store);
    store.put(COLUMN, b"key", &short).expect("put");
    seal(&store, &io);
    store.put(COLUMN, b"key", &short).expect("put");
    seal(&store, &io);

    assert_eq!(store.state.volumes[0].bytes_freed.load(Ordering::Acquire), 0);
    store.close().expect("close");
}

// a cue that can still read a record keeps its blocks, and they go with the cue
#[test]
fn cue_keeps_blocks() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    keep_live(&store);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    seal(&store, &io);
    let cue = store.cue().expect("cue");
    store.put(COLUMN, b"key", &large(2)).expect("put");
    seal(&store, &io);

    let volume = &store.state.volumes[0];
    assert_eq!(volume.bytes_freed.load(Ordering::Acquire), 0);
    let mut buf = Vec::new();
    assert!(cue.get(COLUMN, b"key", &mut buf).expect("get"));
    assert_eq!(buf, large(1));

    drop(cue);
    settle(&io);
    assert!(volume.bytes_freed.load(Ordering::Acquire) > 0);
    store.close().expect("close");
}

// a cue taken after the replacement reads the replacement, so it holds nothing back
#[test]
fn newer_cue_frees() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    keep_live(&store);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    seal(&store, &io);
    store.put(COLUMN, b"key", &large(2)).expect("put");
    let cue = store.cue().expect("cue");
    seal(&store, &io);

    assert!(store.state.volumes[0].bytes_freed.load(Ordering::Acquire) > 0);
    drop(cue);
    store.close().expect("close");
}

// a filesystem that can't give blocks back leaves the record alone
#[test]
fn filesystem_refuses() {
    let io = SimulatedIo::new(Seed(5));
    io.refuse_free_blocks();
    let store = open(&io);
    assert!(!store.state.volumes[0].can_free_blocks);
    keep_live(&store);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    seal(&store, &io);
    store.put(COLUMN, b"key", &large(2)).expect("put");
    seal(&store, &io);

    assert_eq!(io.freed_bytes().0, 0);
    assert_eq!(read(&store, b"key"), Some(large(2)));
    store.close().expect("close");
}

// a read that took its point before the free starts over and never reports the zeros
#[test]
fn read_crossed() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    keep_live(&store);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    seal(&store, &io);
    let latest = store.state.latest_read(COLUMN, b"key").expect("read");
    store.put(COLUMN, b"key", &large(2)).expect("put");
    seal(&store, &io);

    let mut buf = Vec::new();
    let want = RecordWant::Whole;
    assert!(crate::latest::answer(&store, &latest, want, &mut buf).is_none());
    assert_eq!(read(&store, b"key"), Some(large(2)));
    store.close().expect("close");
}

// frees between a get's point and its answer leave a read that succeeded alone
#[test]
fn read_outlives_frees() {
    let io = SimulatedIo::new(Seed(12));
    let store = open(&io);
    for number in 0..HELD_RECORDS {
        store.put(COLUMN, format!("key-{number}").as_bytes(), &large(1)).expect("put");
    }
    for number in 0..=HELD_RECORDS {
        store.put(COLUMN, format!("live-{number}").as_bytes(), &large(3)).expect("put");
    }
    seal(&store, &io);
    let latest = store.state.latest_read(COLUMN, b"live-0").expect("read");
    for number in 0..HELD_RECORDS {
        store.put(COLUMN, format!("key-{number}").as_bytes(), &large(2)).expect("put");
    }
    seal(&store, &io);
    assert!(store.state.cues.counts().frees >= latest.counts.frees + HELD_RECORDS);

    let mut buf = Vec::new();
    let answer = crate::latest::answer(&store, &latest, RecordWant::Whole, &mut buf).expect("an answer");
    assert!(answer.expect("get"));
    assert_eq!(buf, large(3));
    store.close().expect("close");
}

// a pin holds a free back from the record its try reads, and the free goes once the pin does
#[test]
fn pin_holds_a_free() {
    let io = SimulatedIo::new(Seed(13));
    let store = open(&io);
    let state = &store.state;
    keep_live(&store);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    seal(&store, &io);

    let mut buf = Vec::new();
    let (read, handovers, held) = state.pinned_read(COLUMN, b"key").expect("pin");
    store.put(COLUMN, b"key", &large(2)).expect("put");
    seal(&store, &io);
    assert_eq!(state.volumes[0].bytes_freed.load(Ordering::Acquire), 0);
    let found = in_place(state.get_at(&InPlace, &read, handovers, RecordWant::Whole, &mut buf));
    drop(held);
    assert!(found.expect("get"));
    assert_eq!(buf, large(1));

    settle(&io);
    assert!(state.volumes[0].bytes_freed.load(Ordering::Acquire) > 0);
    store.close().expect("close");
}

// a get crossed on every try reads under a pin at the end, so no job's rate can starve it
#[test]
fn crossed_every_try() {
    let io = SimulatedIo::new(Seed(14));
    let store = open(&io);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    seal(&store, &io);

    let state = Arc::clone(&store.state);
    let reads = Arc::new(AtomicU64::new(0));
    let counted = Arc::clone(&reads);
    io.on_every_read(Some(Arc::new(move || {
        state.cues.note_reclaim();
        counted.fetch_add(1, Ordering::Relaxed);
    })));
    assert_eq!(read(&store, b"key"), Some(large(1)));
    io.on_every_read(None);

    assert!(reads.load(Ordering::Relaxed) > u64::from(MOST_READ_TRIES));
    assert_eq!(store.state.cues.refused_reads(), 0);
    store.close().expect("close");
}

// a full table has no pin to give, so the last try gives up and the count shows it
#[test]
fn no_pin_to_give() {
    let io = SimulatedIo::new(Seed(15));
    let store = open(&io);
    let state = &store.state;
    store.put(COLUMN, b"key", &large(1)).expect("put");
    let slots: Vec<usize> = (0..MOST_CUES).map(|_slot| state.cues.take(0, WritePlace(u64::MAX - 1)).expect("a slot")).collect();

    assert!(state.pinned_read(COLUMN, b"key").is_err());
    assert_eq!(state.cues.refused_reads(), 1);

    assert_eq!(read(&store, b"key"), Some(large(1)));
    assert_eq!(state.cues.refused_reads(), 1);
    for slot in slots {
        state.cues.release(slot);
    }
    store.close().expect("close");
}

// a get_many with no pin that a free crossed starts over, where a key failed its record check
#[test]
fn many_crossed_by_a_free() {
    let io = SimulatedIo::new(Seed(18));
    let store = open(&io);
    let state = &store.state;
    let volume = &state.volumes[0];
    keep_live(&store);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    seal(&store, &io);
    let slots: Vec<usize> = (0..MOST_CUES).map(|_slot| state.cues.take(0, WritePlace(u64::MAX - 1)).expect("a slot")).collect();

    let many = state.many_read(volume).expect("a read");
    assert_eq!(many.pin, None);
    store.put(COLUMN, b"key", &large(2)).expect("put");
    seal(&store, &io);
    assert!(volume.bytes_freed.load(Ordering::Acquire) > 0);

    let mut values = ValueList::new();
    let mut buffers = ReadBuffers::default();
    assert!(in_place(state.answer_many(&InPlace, &many, COLUMN, &[b"key"], &mut values, &mut buffers)).is_none());
    for slot in slots {
        state.cues.release(slot);
    }
    assert_eq!(read(&store, b"key"), Some(large(2)));
    store.close().expect("close");
}

// a pass over blocks already given back counts nothing, so the freed figure stays what the disk got back
#[test]
fn second_punch_counts_nothing() {
    let io = SimulatedIo::new(Seed(16));
    let store = open(&io);
    keep_live(&store);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    let older = seal(&store, &io);
    store.put(COLUMN, b"key", &large(2)).expect("put");
    seal(&store, &io);

    let state = &store.state;
    let volume = &state.volumes[0];
    let freed = volume.bytes_freed.load(Ordering::Acquire);
    let frees = state.cues.counts().frees;
    assert!(freed > 0);

    volume.table.flag_held_frees(older, true);
    volume.has_held_frees.store(true, Ordering::Release);
    state.ask_compaction();
    settle(&io);

    assert!(state.cues.counts().frees > frees);
    assert_eq!(volume.bytes_freed.load(Ordering::Acquire), freed);
    assert_eq!(io.freed_bytes().0, freed);
    store.close().expect("close");
}

// what a file gave back outlasts a reopen, so the byte trigger never counts those bytes as won back again
#[test]
fn freed_outlasts_a_reopen() {
    let io = SimulatedIo::new(Seed(17));
    let store = open(&io);
    keep_live(&store);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    let older = seal(&store, &io);
    store.put(COLUMN, b"key", &large(2)).expect("put");
    seal(&store, &io);
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    let volume = &store.state.volumes[0];
    // the quiet volume's line would rewrite the older segment, and this looks at its row as the reopen left it
    volume.holds.compaction.store(true, Ordering::Release);
    settle(&io);
    let rows = hold(&volume.summary);
    let row = rows.rows().find(|row| row.facts.segment == older).expect("a row");
    assert!(freed_in_place(volume, &row) >= FREE_IN_PLACE_BYTES.0 - 2 * FREE_BLOCK_BYTES);
    drop(rows);
    store.close().expect("close");
}

// a crash at any step reads the newest value, and blocks freed ahead of a saved count read as a dead record, never as damage
#[test]
fn crashes() {
    let mut freed_at_crash = 0;
    for crash_at in 0..CRASH_POINTS {
        for is_power_cut in [false, true] {
            let io = SimulatedIo::new(Seed(7));
            let store = open(&io);
            keep_live(&store);
            store.put(COLUMN, b"key", &large(1)).expect("put");
            seal(&store, &io);
            store.put(COLUMN, b"key", &large(2)).expect("put");
            store.flush().expect("flush");
            store.seal_now();
            io.arm_crash(OperationCount(crash_at));
            for _step in 0..STEPS {
                if io.is_crashed() {
                    break;
                }
                if io.step_any().is_none() {
                    io.advance_clock(TICK);
                    if io.step_any().is_none() {
                        break;
                    }
                }
            }
            freed_at_crash += u64::from(io.freed_bytes().0 > 0);
            if is_power_cut {
                io.power_cut();
            } else {
                io.kill();
            }
            drop(store);

            let store = open(&io);
            store.seal_now();
            settle(&io);
            assert_eq!(read(&store, b"key"), Some(large(2)), "crash at {crash_at}");
            assert_eq!(read(&store, b"keep"), Some(b"value".to_vec()), "crash at {crash_at}");
            assert!(hold(&store.state.volumes[0].damaged).is_empty(), "crash at {crash_at}");
            store.close().expect("close");
        }
    }
    assert!(freed_at_crash > 0);
}

// a freed range stays inside its record on both ends, so the large records packed against it read back whole
#[test]
fn neighbours_stay() {
    let io = SimulatedIo::new(Seed(8));
    let store = open(&io);
    let beside = large(9);
    store.put(COLUMN, b"before", &beside).expect("put");
    store.put(COLUMN, b"key", &large(1)).expect("put");
    store.put(COLUMN, b"after", &beside).expect("put");
    seal(&store, &io);
    store.put(COLUMN, b"key", &large(2)).expect("put");
    seal(&store, &io);

    assert!(store.state.volumes[0].bytes_freed.load(Ordering::Acquire) > 0);
    assert_eq!(read(&store, b"before"), Some(beside.clone()));
    assert_eq!(read(&store, b"after"), Some(beside));
    store.close().expect("close");
}

// a replacement the mark hasn't reached can't free anything, whatever the walk's order
#[test]
fn unpublished_replacement() {
    let io = SimulatedIo::new(Seed(9));
    let store = open(&io);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    keep_live(&store);
    let older = seal(&store, &io);

    let state = store.state.clone();
    let free = FreeRecord {
        segment: older,
        offset: FileOffset(0),
        length: ByteCount(2 * FREE_BLOCK_BYTES),
        place: WritePlace(1),
        replacement: WritePlace(store.state.read_point(&store.state.volumes[0]).0 + 1),
    };
    assert!(!state.free_record(&store.state.volumes[0], &free));
    assert_eq!(io.freed_bytes().0, 0);
    store.close().expect("close");
}

// a cue over more large dead records than any list holds flags their segment, nothing goes while it lives, and one pass frees
// every one of them after it
#[test]
fn held_past_the_list() {
    let io = SimulatedIo::new(Seed(10));
    let store = open(&io);
    for number in 0..HELD_RECORDS {
        store.put(COLUMN, format!("key-{number}").as_bytes(), &large(1)).expect("put");
    }
    for number in 0..=HELD_RECORDS {
        store.put(COLUMN, format!("live-{number}").as_bytes(), &large(3)).expect("put");
    }
    let older = seal(&store, &io);
    let cue = store.cue().expect("cue");
    for number in 0..HELD_RECORDS {
        store.put(COLUMN, format!("key-{number}").as_bytes(), &large(2)).expect("put");
    }
    seal(&store, &io);

    let volume = &store.state.volumes[0];
    assert_eq!(volume.bytes_freed.load(Ordering::Acquire), 0);
    assert_eq!(volume.table.next_held_frees(), Some(older));

    drop(cue);
    settle(&io);
    let whole_blocks = HELD_RECORDS * (FREE_IN_PLACE_BYTES.0 - FREE_BLOCK_BYTES);
    assert!(volume.bytes_freed.load(Ordering::Acquire) >= whole_blocks);
    assert_eq!(volume.table.next_held_frees(), None);
    assert!(!volume.has_held_frees.load(Ordering::Acquire));
    assert_eq!(read(&store, b"key-0"), Some(large(2)));
    assert_eq!(read(&store, b"live-0"), Some(large(3)));
    store.close().expect("close");
}

// a cue that comes back mid-pass keeps the flag, and its going finishes the job
#[test]
fn pass_waits_for_a_new_cue() {
    let io = SimulatedIo::new(Seed(11));
    let store = open(&io);
    keep_live(&store);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    let older = seal(&store, &io);
    let cue = store.cue().expect("cue");
    store.put(COLUMN, b"key", &large(2)).expect("put");
    seal(&store, &io);

    let volume = &store.state.volumes[0];
    volume.note_held_frees(older);
    let mut job = CompactJob::new(store.state.clone(), 0);
    for _block in 0..=PASS_STEPS {
        job.step_held_pass(volume).expect("a block");
    }
    assert_eq!(volume.table.next_held_frees(), Some(older));
    drop(cue);
    settle(&io);
    assert_eq!(volume.table.next_held_frees(), None);
    assert!(volume.bytes_freed.load(Ordering::Acquire) > 0);
    store.close().expect("close");
}

// the writer pauses once a seal lands, so the hot window closes and the rewrites hot ingest put off can start
fn cool(store: &Store, io: &SimulatedIo) {
    let until = store.state.quiet_at(io.clock_reading());
    io.advance_clock(Nanoseconds(until.0.saturating_sub(io.clock_reading().0)));
    settle(io);
}

// find where a key's newest sealed row sits, with its segment's data file
fn sealed_row(store: &Store, key: &[u8]) -> (SealedRow, PathBuf) {
    let volume = &store.state.volumes[0];
    let read = store.state.key_read(COLUMN, key, store.state.read_point(volume), false).expect("read");
    let row = in_place(store.state.search_sealed(&InPlace, volume, &read)).expect("search").expect("a sealed row");
    let path = data_path(&volume.roots[0], row.segment).to_path_buf();
    (row, path)
}

// a large record freed under a get that took its counts first fails its full check, and the get starts over. Counts taken after
// the free read the replacement
#[test]
fn a_free_under_a_gets_counts_starts_it_over() {
    let io = SimulatedIo::new(Seed(40));
    let store = open(&io);
    keep_live(&store);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    seal(&store, &io);
    let before = store.state.latest_read(COLUMN, b"key").expect("read");
    store.put(COLUMN, b"key", &large(2)).expect("put");
    seal(&store, &io);
    assert!(store.state.cues.counts().frees > before.counts.frees, "nothing was freed");

    let mut buf = Vec::new();
    assert!(crate::latest::answer(&store, &before, RecordWant::Whole, &mut buf).is_none(), "the crossed get kept its answer");
    let after = store.state.latest_read(COLUMN, b"key").expect("read");
    let answer = crate::latest::answer(&store, &after, RecordWant::Whole, &mut buf).expect("an answer");
    assert!(answer.expect("get"));
    assert_eq!(buf, large(2));
    store.close().expect("close");
}

// a scan's batch that located a large row before the row was freed reads it with the full check, which fails, and the walk
// seeks again and finds the replacement
#[test]
fn a_scan_batch_across_a_free_seeks_again() {
    let io = SimulatedIo::new(Seed(41));
    let store = open(&io);
    keep_live(&store);
    store.put(COLUMN, b"a", &large(1)).expect("put");
    store.put(COLUMN, b"b", &large(1)).expect("put");
    seal(&store, &io);
    let mut scan = store.iter_from(COLUMN, b"a", Direction::Ascending).expect("scan");
    let (mut name, mut value) = (Vec::new(), Vec::new());
    assert_eq!(scan.next_row(&mut name, &mut value).expect("step"), ScanStep::Found);
    assert_eq!((name.as_slice(), value.clone()), (&b"a"[..], large(1)));
    let frees = store.state.cues.counts().frees;
    store.put(COLUMN, b"b", &large(2)).expect("put");
    seal(&store, &io);
    assert!(store.state.cues.counts().frees > frees, "nothing was freed");
    assert_eq!(scan.next_row(&mut name, &mut value).expect("step"), ScanStep::Found);
    assert_eq!((name.as_slice(), value), (&b"b"[..], large(2)));
    store.close().expect("close");
}

// the price of the skipped check: a get under its counts skips a small sealed record's identity hashes, so a flipped byte in its
// stored check goes unseen and the value reads back whole
#[test]
fn a_flipped_check_reads_back_unchecked_while_counts_hold() {
    let io = SimulatedIo::new(Seed(42));
    let store = open(&io);
    store.put(COLUMN, b"small", b"small value").expect("put");
    seal(&store, &io);
    let (row, path) = sealed_row(&store, b"small");
    io.rot(&path, FileOffset(row.row.offset.0), BitMask(1)).expect("rot");
    assert_eq!(read(&store, b"small").expect("a value"), b"small value", "the get checked the identity");
    store.close().expect("close");
}

// a get under its counts still sums a small payload against its head, so a byte flipped in the payload reads as corrupt
#[test]
fn a_flipped_payload_is_caught_while_counts_hold() {
    let io = SimulatedIo::new(Seed(42));
    let store = open(&io);
    store.put(COLUMN, b"small", b"small value").expect("put");
    seal(&store, &io);
    let (row, path) = sealed_row(&store, b"small");
    io.rot(&path, FileOffset(row.row.offset.0 + SMALL_HEAD_BYTES as u64), BitMask(1)).expect("rot");
    let mut buf = Vec::new();
    let found = store.get(COLUMN, b"small", &mut buf);
    assert!(matches!(found, Err(StoreError::Corruption { .. })), "a flipped payload answered: {found:?} {buf:?}");
    store.close().expect("close");
}

// the same price on a large record read in one go: its piece sums are skipped while the get's counts hold, so a flipped byte at
// the value's end reads back flipped
#[test]
fn flipped_bytes_in_a_large_record_read_back_unchecked_while_counts_hold() {
    let io = SimulatedIo::new(Seed(43));
    let store = open(&io);
    store.put(COLUMN, b"large", &large(1)).expect("put");
    seal(&store, &io);
    let (row, path) = sealed_row(&store, b"large");
    io.rot(&path, FileOffset(row.row.offset.0 + row.row.length.0 - 1), BitMask(1)).expect("rot");
    let found = read(&store, b"large").expect("a value");
    assert_eq!(found.len(), large(1).len());
    assert_ne!(found, large(1), "the flipped byte was caught");
    store.close().expect("close");
}

const MERGE_ROUNDS: u64 = 6;
const MERGED_KEYS: u64 = 4;

// a merge that began before a free punched a record it keeps fails that record's check with the counts moved, which is no damage:
// merges beside frees in place leave no segment damaged and count no failure
#[test]
fn merges_beside_frees_mark_no_damage() {
    let io = SimulatedIo::new(Seed(43));
    let store = open(&io);
    keep_live(&store);
    for round in 0..MERGE_ROUNDS {
        for number in 0..MERGED_KEYS {
            store.put(COLUMN, format!("key-{number}").as_bytes(), &large(round as u8)).expect("put");
        }
        // a dead row of its own, so a merge of the segment alone has something to drop
        store.put(COLUMN, b"twice", b"first").expect("put");
        store.put(COLUMN, b"twice", b"second").expect("put");
        let segment = seal(&store, &io);
        let mut job = CompactJob::new(Arc::clone(&store.state), 0);
        job.rewrite.forced = Some(segment);
        let is_started = job.rewrite.start();
        if is_started {
            job.rewrite.step();
        }
        // the next round replaces this one's large records, and the walk frees them under the merge
        for number in 0..MERGED_KEYS {
            store.put(COLUMN, format!("key-{number}").as_bytes(), &large(round as u8 + 1)).expect("put");
        }
        seal(&store, &io);
        while is_started && job.rewrite.is_running() {
            job.rewrite.step();
        }
    }
    let volume = &store.state.volumes[0];
    assert!(volume.bytes_freed.load(Ordering::Acquire) > 0, "nothing was freed");
    assert!(hold(&volume.damaged).is_empty(), "a merge beside frees damaged a segment");
    assert_eq!(store.state.merge_check_failures.load(Ordering::Relaxed), 0);
    for number in 0..MERGED_KEYS {
        assert_eq!(read(&store, format!("key-{number}").as_bytes()), Some(large(MERGE_ROUNDS as u8)));
    }
    assert_eq!(read(&store, b"twice"), Some(b"second".to_vec()));
    store.close().expect("close");
}

// an open row's record is the bytes its row names once it's in the file, so a get reads it with no identity check, and a byte
// flipped in its stored check goes unseen. The value is too long for its row to keep, so the get reads the record
#[test]
fn an_open_record_reads_back_unchecked() {
    let io = SimulatedIo::new(Seed(44));
    let store = open(&io);
    let written = vec![b'o'; reel::index::entry_slab::MOST_INLINE_VALUE + 1];
    store.put(COLUMN, b"open", &written).expect("put");
    store.flush().expect("flush");
    let volume = &store.state.volumes[0];
    let row = store.state.index.lookup(COLUMN, b"open", store.state.read_point(volume), volume.view()).row.expect("an open row");
    let path = data_path(&volume.roots[0], row.segment).to_path_buf();
    io.rot(&path, FileOffset(row.offset.0), BitMask(1)).expect("rot");
    assert_eq!(read(&store, b"open").expect("a value"), written, "the get checked the identity");
    store.close().expect("close");
}

// put a key with a small value, then enough after it that the slot's window no longer holds its record, and flip a byte of the
// record's stored check on the disk
fn rot_open_record(store: &Store, io: &SimulatedIo) {
    for number in 0..50_u32 {
        store.put(COLUMN, format!("aaa-{number:04}").as_bytes(), b"open value").expect("put");
    }
    for number in 0..200_u32 {
        store.put(COLUMN, format!("zzz-{number:04}").as_bytes(), &[7_u8; 1_000]).expect("put");
    }
    store.flush().expect("flush");
    let volume = &store.state.volumes[0];
    let row = store.state.index.lookup(COLUMN, b"aaa-0025", store.state.read_point(volume), volume.view()).row.expect("an open row");
    let path = data_path(&volume.roots[0], row.segment).to_path_buf();
    io.rot(&path, FileOffset(row.offset.0), BitMask(1)).expect("rot");
}

// read the value a scan from the start answers for a key, which the batch holds past the walk's first row, or nothing when the
// scan stops first
fn scanned(store: &Store, wanted: &[u8]) -> Option<Vec<u8>> {
    let mut scan = store.iter_from(COLUMN, b"a", Direction::Ascending).expect("scan");
    let (mut name, mut value) = (Vec::new(), Vec::new());
    while let Ok(ScanStep::Found) = scan.next_row(&mut name, &mut value) {
        if name == wanted {
            return Some(value);
        }
    }
    None
}

// a scan reads an open record this run wrote from its batch with no identity check, as a get does, so a byte flipped in its
// stored check goes unseen
#[test]
fn a_scanned_open_record_reads_back_unchecked() {
    let io = SimulatedIo::new(Seed(45));
    let store = open(&io);
    rot_open_record(&store, &io);
    let trusted = store.state.scan_trusted_records.load(Ordering::Relaxed);
    let value = scanned(&store, b"aaa-0025").expect("the row");
    assert!(store.state.scan_trusted_records.load(Ordering::Relaxed) > trusted, "the batch checked every open record");
    assert_eq!(value, b"open value", "the batch checked the identity");
    store.close().expect("close");
}

// an open record a reopen replayed is checked: a scan never answers its flipped bytes
#[test]
fn a_scanned_replayed_record_is_checked() {
    let io = SimulatedIo::new(Seed(46));
    let store = open(&io);
    for number in 0..50_u32 {
        store.put(COLUMN, format!("aaa-{number:04}").as_bytes(), b"open value").expect("put");
    }
    for number in 0..200_u32 {
        store.put(COLUMN, format!("zzz-{number:04}").as_bytes(), &[7_u8; 1_000]).expect("put");
    }
    store.flush().expect("flush");
    io.kill();
    drop(store);
    let store = open(&io);
    let volume = &store.state.volumes[0];
    let row = store.state.index.lookup(COLUMN, b"aaa-0025", store.state.read_point(volume), volume.view()).row.expect("an open row");
    let path = data_path(&volume.roots[0], row.segment).to_path_buf();
    io.rot(&path, FileOffset(row.offset.0 + SMALL_HEAD_BYTES as u64), BitMask(1)).expect("rot");
    if let Some(value) = scanned(&store, b"aaa-0025") {
        assert_eq!(value, b"open value", "a replayed record's flipped bytes were answered");
    }
    assert_eq!(store.state.scan_trusted_records.load(Ordering::Relaxed), 0, "the batch trusted a replayed record");
    store.close().expect("close");
}

// an open record this run wrote isn't summed, since its bytes never left this process, so a byte flipped in its payload on the
// disk under it reads back flipped, where a sealed record's is caught
#[test]
fn an_open_record_payload_is_not_summed() {
    let io = SimulatedIo::new(Seed(47));
    let store = open(&io);
    let written = vec![b'p'; reel::index::entry_slab::MOST_INLINE_VALUE + 1];
    store.put(COLUMN, b"open", &written).expect("put");
    store.flush().expect("flush");
    let volume = &store.state.volumes[0];
    let row = store.state.index.lookup(COLUMN, b"open", store.state.read_point(volume), volume.view()).row.expect("an open row");
    let path = data_path(&volume.roots[0], row.segment).to_path_buf();
    io.rot(&path, FileOffset(row.offset.0 + SMALL_HEAD_BYTES as u64), BitMask(1)).expect("rot");
    let found = read(&store, b"open").expect("a value");
    assert_eq!(found.len(), written.len());
    assert_ne!(found, written, "the open record was summed");
    store.close().expect("close");
}

// a merge that goes by its own rows, as one does whose dead counts a crash took, keeps a large record the walk freed as dead and
// finds its blocks punched: a get finds a newer row for its key, so the merge drops it as superseded, damages nothing, and every
// key reads. A record still its key's newest fails as before
#[test]
fn a_merge_that_keeps_a_freed_record_drops_it() {
    let io = SimulatedIo::new(Seed(45));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    volume.holds.compaction.store(true, Ordering::Release);
    keep_live(&store);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    // a dead row of its own, so a merge of the segment alone has something to drop
    store.put(COLUMN, b"twice", b"first").expect("put");
    store.put(COLUMN, b"twice", b"second").expect("put");
    let older = seal(&store, &io);
    store.put(COLUMN, b"key", &large(2)).expect("put");
    seal(&store, &io);
    assert!(volume.bytes_freed.load(Ordering::Acquire) > 0, "the walk freed nothing");
    assert!(volume.table.holds(older), "the older segment left before the merge");
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.forced = Some(older);
    job.rewrite.is_blind_outside = true;
    assert!(job.rewrite.start(), "the merge never started");
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    assert!(hold(&volume.damaged).is_empty(), "the merge damaged a segment over a record freed as dead");
    assert_eq!(store.state.merge_check_failures.load(Ordering::Relaxed), 0);
    assert!(store.state.merge_freed_drops.load(Ordering::Relaxed) > 0, "the merge never met the freed record");
    assert!(!volume.table.holds(older), "the merge never installed");
    // the freed record's bytes went across in the run and no row names them: they count as the output's gap, which a pick takes as
    // dead
    let rows = hold(&volume.summary);
    let gap: u64 = rows.rows().map(|row| reel::segment::summary_orders::row_gap_bytes(&row)).sum();
    assert!(gap >= FREE_IN_PLACE_BYTES.0, "the output's dead bytes left out the freed record: a gap of {gap}");
    drop(rows);
    assert_eq!(read(&store, b"key"), Some(large(2)));
    assert_eq!(read(&store, b"twice"), Some(b"second".to_vec()));
    assert_eq!(read(&store, b"keep-1"), Some(large(3)));
    store.close().expect("close");
}

// a cover's apply frees a large record it hides before the cover counts as applied, and a merge between the two that asks outside
// keeps the record and finds its blocks punched: the sealed cover replaces it as a newer row does, so the merge drops it and damages nothing
#[test]
fn a_merge_inside_a_cover_apply_drops_the_freed_record() {
    let io = SimulatedIo::new(Seed(50));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    volume.holds.compaction.store(true, Ordering::Release);
    keep_live(&store);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    store.put(COLUMN, b"twice", b"first").expect("put");
    store.put(COLUMN, b"twice", b"second").expect("put");
    let older = seal(&store, &io);
    // a range that starts below the key, so no row of the cover's own segment carries the key
    store.delete_range(COLUMN, b"kex", b"kez").expect("delete");
    store.seal_now();
    // run the jobs only until the apply frees the record, while its cover still counts
    for _step in 0..STEPS {
        if volume.bytes_freed.load(Ordering::Acquire) > 0 {
            break;
        }
        if io.step_any().is_none() {
            io.advance_clock(TICK);
        }
    }
    assert!(volume.bytes_freed.load(Ordering::Acquire) > 0, "the apply freed nothing");
    assert_eq!(volume.covers.newest_applied(COLUMN, b"key"), None, "the cover applied before the merge could run inside it");
    // bits a crash or a damage left inexact make the merge ask the segments outside it, which hold no newer row for the key
    volume.dead_bits.spoil_all();
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.forced = Some(older);
    assert!(job.rewrite.start(), "the merge never started");
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    assert!(hold(&volume.damaged).is_empty(), "the merge damaged a segment over a record its cover freed");
    assert_eq!(store.state.merge_check_failures.load(Ordering::Relaxed), 0);
    assert_eq!(read(&store, b"key"), None);
    assert_eq!(read(&store, b"keep-1"), Some(large(3)));
    store.close().expect("close");
}

// the same across a crash: the apply freed the record and the process died before the cover counted as applied. Open brings the
// cover back from its footer with its apply still to run, and a merge before that apply finds the record punched and drops it
#[test]
fn a_merge_after_a_crash_inside_a_cover_apply_drops_the_freed_record() {
    let io = SimulatedIo::new(Seed(51));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    volume.holds.compaction.store(true, Ordering::Release);
    keep_live(&store);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    store.put(COLUMN, b"twice", b"first").expect("put");
    store.put(COLUMN, b"twice", b"second").expect("put");
    let older = seal(&store, &io);
    store.delete_range(COLUMN, b"kex", b"kez").expect("delete");
    store.seal_now();
    for _step in 0..STEPS {
        if volume.bytes_freed.load(Ordering::Acquire) > 0 {
            break;
        }
        if io.step_any().is_none() {
            io.advance_clock(TICK);
        }
    }
    assert!(volume.bytes_freed.load(Ordering::Acquire) > 0, "the apply freed nothing");
    assert_eq!(volume.covers.newest_applied(COLUMN, b"key"), None, "the cover applied before the crash");
    io.kill();
    drop(store);

    let store = open(&io);
    let volume = &store.state.volumes[0];
    volume.holds.compaction.store(true, Ordering::Release);
    assert!(volume.covers.newest_over(COLUMN, b"key", WritePlace(u64::MAX)).is_some(), "open lost the cover");
    assert_eq!(volume.covers.newest_applied(COLUMN, b"key"), None, "open counted the cover as applied");
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.forced = Some(older);
    assert!(job.rewrite.start(), "the merge never started");
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    assert!(hold(&volume.damaged).is_empty(), "the merge damaged a segment over a record its cover freed before the crash");
    assert_eq!(store.state.merge_check_failures.load(Ordering::Relaxed), 0);
    assert_eq!(read(&store, b"key"), None);
    assert_eq!(read(&store, b"keep-1"), Some(large(3)));
    store.close().expect("close");
}

// a power cut before any sync, while seals and the walk free the old value under an unsynced replacement: the old value or the new
// one reads whole, and nothing is damaged, since a free names only a replacement a seal has synced
#[test]
fn a_power_cut_before_the_replacement_syncs_loses_no_value() {
    for crash_at in 0..CRASH_POINTS {
        let io = SimulatedIo::new(Seed(46));
        let store = open(&io);
        keep_live(&store);
        store.put(COLUMN, b"key", &large(1)).expect("put");
        seal(&store, &io);
        // no flush: the replacement is in the open segment until its seal syncs it
        store.put(COLUMN, b"key", &large(2)).expect("put");
        store.seal_now();
        io.arm_crash(OperationCount(crash_at));
        for _step in 0..STEPS {
            if io.is_crashed() {
                break;
            }
            if io.step_any().is_none() {
                io.advance_clock(TICK);
                if io.step_any().is_none() {
                    break;
                }
            }
        }
        io.power_cut();
        drop(store);

        let store = open(&io);
        store.seal_now();
        settle(&io);
        let value = read(&store, b"key");
        assert!(value == Some(large(1)) || value == Some(large(2)), "crash at {crash_at}: the key read {:?}", value.map(|value| value.len()));
        assert_eq!(read(&store, b"keep-1"), Some(large(3)), "crash at {crash_at}");
        assert!(hold(&store.state.volumes[0].damaged).is_empty(), "crash at {crash_at}");
        store.close().expect("close");
    }
}

// a large record rots with no free behind it, and its key's only newer row is open and unsynced. The merge keeps the record, so
// after a power cut takes the open row the key never reads as unwritten
#[test]
fn a_rotten_record_under_an_unsynced_replacement_never_reads_as_unwritten() {
    let io = SimulatedIo::new(Seed(48));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    volume.holds.compaction.store(true, Ordering::Release);
    keep_live(&store);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    store.put(COLUMN, b"twice", b"first").expect("put");
    store.put(COLUMN, b"twice", b"second").expect("put");
    let older = seal(&store, &io);
    store.flush().expect("flush");
    let read_at = store.state.key_read(COLUMN, b"key", store.state.read_point(volume), false).expect("read");
    let row = in_place(store.state.search_sealed(&InPlace, volume, &read_at)).expect("search").expect("a sealed row");
    assert_eq!(row.segment, older);
    io.rot(&data_path(&volume.roots[0], older), FileOffset(row.row.offset.0 + SMALL_HEAD_BYTES as u64 + 9_000), BitMask(1)).expect("rot");
    // the replacement lands in the open segment and nothing syncs it
    store.put(COLUMN, b"key", &large(2)).expect("put");
    assert_eq!(volume.bytes_freed.load(Ordering::Acquire), 0, "a free punched the record, so this is no rot");
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.forced = Some(older);
    assert!(job.rewrite.start(), "the merge never started");
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    println!("drop: merge_freed_drops {}, merge_check_failures {}, damaged {:?}, older still held {}", store.state.merge_freed_drops.load(Ordering::Relaxed), store.state.merge_check_failures.load(Ordering::Relaxed), hold(&volume.damaged).len(), volume.table.holds(older));
    assert_eq!(store.state.merge_freed_drops.load(Ordering::Relaxed), 0, "the merge dropped the rotten record under an open row");
    assert!(!hold(&volume.damaged).is_empty(), "the merge kept the rotten record and damaged nothing");
    assert_eq!(read(&store, b"key"), Some(large(2)), "a get before the cut misses the replacement");
    io.power_cut();
    drop(store);

    let store = open(&io);
    settle(&io);
    let mut buf = Vec::new();
    let got = store.get(COLUMN, b"key", &mut buf);
    println!("drop: after the power cut the key reads {:?}, damaged segments {}", got.as_ref().map(|found| found.then(|| buf.first().copied())), store.damaged_segments());
    let is_silent_loss = matches!(got, Ok(false)) && store.damaged_segments() == 0;
    assert!(!is_silent_loss, "the key reads as never written and nothing is damaged: its synced value rotted, and the merge dropped it under a row the power cut took");
    store.close().expect("close");
}

// the same with an older version of the key in another sealed segment: after the power cut a get never answers the oldest value
#[test]
fn a_rotten_record_under_an_unsynced_replacement_never_brings_back_an_older_value() {
    let io = SimulatedIo::new(Seed(49));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    volume.holds.compaction.store(true, Ordering::Release);
    keep_live(&store);
    store.put(COLUMN, b"key", b"oldest").expect("put");
    seal(&store, &io);
    store.put(COLUMN, b"key", &large(1)).expect("put");
    store.put(COLUMN, b"keep-5", &large(7)).expect("put");
    store.put(COLUMN, b"twice", b"first").expect("put");
    store.put(COLUMN, b"twice", b"second").expect("put");
    let older = seal(&store, &io);
    store.flush().expect("flush");
    assert_eq!(read(&store, b"key"), Some(large(1)));
    let read_at = store.state.key_read(COLUMN, b"key", store.state.read_point(volume), false).expect("read");
    let row = in_place(store.state.search_sealed(&InPlace, volume, &read_at)).expect("search").expect("a sealed row");
    assert_eq!(row.segment, older);
    io.rot(&data_path(&volume.roots[0], older), FileOffset(row.row.offset.0 + SMALL_HEAD_BYTES as u64 + 9_000), BitMask(1)).expect("rot");
    store.put(COLUMN, b"key", &large(2)).expect("put");
    assert_eq!(volume.bytes_freed.load(Ordering::Acquire), 0, "a free punched the record, so this is no rot");
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.forced = Some(older);
    assert!(job.rewrite.start(), "the merge never started");
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    println!("stale: merge_freed_drops {}, merge_check_failures {}, damaged {:?}, older still held {}", store.state.merge_freed_drops.load(Ordering::Relaxed), store.state.merge_check_failures.load(Ordering::Relaxed), hold(&volume.damaged).len(), volume.table.holds(older));
    assert_eq!(store.state.merge_freed_drops.load(Ordering::Relaxed), 0, "the merge dropped the rotten record under an open row");
    assert!(!hold(&volume.damaged).is_empty(), "the merge kept the rotten record and damaged nothing");
    assert_eq!(read(&store, b"key"), Some(large(2)), "a get before the cut misses the replacement");
    io.power_cut();
    drop(store);

    let store = open(&io);
    settle(&io);
    let mut buf = Vec::new();
    let got = store.get(COLUMN, b"key", &mut buf);
    println!("stale: after the power cut the key reads {:?} with {} bytes {:?}, damaged segments {}", got, buf.len(), String::from_utf8_lossy(&buf[..buf.len().min(8)]), store.damaged_segments());
    assert!(!(matches!(got, Ok(true)) && buf == b"oldest"), "a get answered the key's oldest value: the synced value over it rotted and the merge dropped it under a row the power cut took");
    store.close().expect("close");
}
