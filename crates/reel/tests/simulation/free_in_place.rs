//! Free in place: a large dead record gives its blocks back with no copy, and the segment keeps its length and footer

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::compact_job::CompactJob;
use reel::engine::cue::MOST_CUES;
use reel::engine::read_path::{MOST_READ_TRIES, RecordWant};
use reel::engine::reel_store::ReelStore;
use reel::engine::rewrite_job::freed_in_place;
use reel::engine::walk_job::{FREE_BLOCK_BYTES, FREE_IN_PLACE_BYTES, FreeRecord, MOST_BLOCK_FREES};
use reel::guard::hold;
use reel::io::{InPlace, in_place};
use reel::record::codec::Codec;
use reel::segment::segment_tail::SegmentNumber;
use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, FileOffset, Nanoseconds, VolumeId, WritePlace};
use reel_core::value_list::{ReadBuffers, ValueList};
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
