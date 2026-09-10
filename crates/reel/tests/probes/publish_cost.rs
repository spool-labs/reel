//! What the publish barrier costs a writer and what a reader pays beside it
//!
//! A batch takes the barrier exclusively to move the index, so two batches no longer
//! publish beside each other, and a read spanning several keys holds it shared. The
//! writers take separate groups so their keys land in separate shards, which is the
//! shape the barrier costs the most: without it those publishes never touch each
//! other. An allocator is the other thing that can cost a batch more as threads are
//! added without the engine doing anything differently, so `batch_alloc_only` runs the
//! harness allocation alone and the two curves can be read apart.
//!
//! Opt-in, run with:
//!   cargo test -p tape-reel --release --test probes -- publish_cost

#[cfg(feature = "alloc-mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

// Only one process can name a global allocator, so asking for both picks mimalloc
// rather than failing to build, which is what `--all-features` asks for.
#[cfg(all(feature = "alloc-snmalloc", not(feature = "alloc-mimalloc")))]
#[global_allocator]
static ALLOCATOR: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSpec, IndexResidency, KeyPage, KeyWidth, MapShape, RecordKey,
    RecordWrite, ReelConfig, ReelStore, SyncPolicy, ThreadBudget, MAP_EVERYTHING,
};

const RECORDS: ColumnId = ColumnId(1);

const COLUMNS: &[ColumnSpec] = &[ColumnSpec {
    id: RECORDS,
    name: "records",
    key_width: KeyWidth::Fixed(34),
    shard_bytes: 2,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// Keys one batch carries
const KEYS: u32 = 48;

/// Batches each writer sends
const BATCHES: u32 = 2_000;

/// Writer counts the sweep walks, up to what the machine can actually run
///
/// Serialised publishing only shows itself where there are enough writers to
/// serialise, so the top of the sweep is the machine's rather than a constant.
fn writer_counts() -> Vec<u16> {
    let cores = std::thread::available_parallelism()
        .map(|cores| cores.get())
        .unwrap_or(8);
    [1u16, 2, 4, 8, 16, 32, 64]
        .into_iter()
        .take_while(|writers| usize::from(*writers) <= cores.max(8))
        .collect()
}

/// Which allocator this binary was linked against, so a log says what it measured
fn allocator() -> &'static str {
    if cfg!(feature = "alloc-mimalloc") {
        "mimalloc"
    } else if cfg!(feature = "alloc-snmalloc") {
        "snmalloc"
    } else {
        "system"
    }
}

fn key(group: u16, index: u32) -> RecordKey {
    let mut bytes = [0u8; 34];
    bytes[..2].copy_from_slice(&group.to_be_bytes());
    bytes[2..6].copy_from_slice(&index.to_be_bytes());
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

/// Bytes each key in a batch carries
///
/// The barrier costs the same per batch whatever the payload is, so the payload
/// decides whether the sweep can see it: at a kilobyte a batch drags 48 KiB through
/// the device and pins there, while sixty-four bytes leaves the barrier as the term
/// that moves. `REEL_PAYLOAD` reaches the other regime.
fn payload_bytes() -> usize {
    std::env::var("REEL_PAYLOAD")
        .ok()
        .and_then(|bytes| bytes.parse().ok())
        .unwrap_or(1024)
}

/// Tails the sweep opens against, so the publish barrier can be charged apart from
/// the append side it sits behind
///
/// A barrier every writer queues on costs the same whatever the appends beneath it are
/// doing, so holding writers fixed and varying this says which of the two collapsed.
fn tails() -> u32 {
    std::env::var("REEL_TAILS")
        .ok()
        .and_then(|count| count.parse().ok())
        .unwrap_or(8)
}

fn open_at(dir: &TempDir) -> ReelStore {
    ReelStore::open(
        dir.path().to_path_buf(),
        ReelConfig {
            segment_bytes: ByteCount::mb(256),
            sync: SyncPolicy::Never,
            active_tails: ThreadBudget::threads(tails()),
            index: IndexResidency::Resident,
            // The read path the engine serves callers with
            map_above: MAP_EVERYTHING,
            ..ReelConfig::default()
        },
        COLUMNS,
    )
    .expect("open")
}

fn batch(group: u16, round: u32, payload: &[u8]) -> Vec<RecordWrite> {
    (0..KEYS)
        .map(|at| RecordWrite::Put {
            key: key(group, round * KEYS + at),
            payload: payload.to_vec(),
        })
        .collect()
}

// what a batch publish costs as writers are added
pub fn batch_publish() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    println!("allocator: {}", allocator());
    println!("{:>8} {:>14} {:>14}", "writers", "per batch", "batches/s");
    for writers in writer_counts() {
        let dir = TempDir::new().expect("tempdir");
        let store = Arc::new(open_at(&dir));
        let start = Arc::new(Barrier::new(usize::from(writers) + 1));

        let mut sending = Vec::with_capacity(usize::from(writers));
        for group in 0..writers {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            sending.push(thread::spawn(move || {
                let payload = vec![0xa5u8; payload_bytes()];
                start.wait();
                for round in 0..BATCHES {
                    store
                        .apply_batch(batch(group, round, &payload))
                        .expect("batch");
                }
            }));
        }

        start.wait();
        let began = Instant::now();
        for writer in sending {
            writer.join().expect("writer");
        }
        let took = began.elapsed();

        let sent = u32::from(writers) * BATCHES;
        println!(
            "{:>8} {:>14} {:>14.0}",
            writers,
            format!("{:.2?}", took / sent),
            f64::from(sent) / took.as_secs_f64(),
        );
    }
}

// what the same batch costs to build when nothing stores it
//
// The control for `batch_publish`: identical threads, counts and owned payloads with
// the store taken out, so only what `batch_publish` does beyond this is the engine's.
pub fn batch_alloc_only() {
    println!();
    println!("allocator: {}", allocator());
    println!("{:>8} {:>14} {:>14}", "writers", "per batch", "batches/s");
    for writers in writer_counts() {
        let start = Arc::new(Barrier::new(usize::from(writers) + 1));

        let mut building = Vec::with_capacity(usize::from(writers));
        for group in 0..writers {
            let start = Arc::clone(&start);
            building.push(thread::spawn(move || {
                let payload = vec![0xa5u8; payload_bytes()];
                start.wait();
                for round in 0..BATCHES {
                    drop(std::hint::black_box(batch(group, round, &payload)));
                }
            }));
        }

        start.wait();
        let began = Instant::now();
        for builder in building {
            builder.join().expect("builder");
        }
        let took = began.elapsed();

        let built = u32::from(writers) * BATCHES;
        println!(
            "{:>8} {:>14} {:>14.0}",
            writers,
            format!("{:.2?}", took / built),
            f64::from(built) / took.as_secs_f64(),
        );
    }
}

// what a many-key read costs beside a volume that is publishing batches
pub fn read_under_publish() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    println!(
        "{:>8} {:>16} {:>16} {:>10}",
        "writers", "quiet read", "read beside", "ratio"
    );
    let asked: Vec<RecordKey> = (0..KEYS).map(|at| key(0, at)).collect();

    for writers in [1u16, 4, 16] {
        let dir = TempDir::new().expect("tempdir");
        let store = Arc::new(open_at(&dir));
        let payload = vec![0xa5u8; payload_bytes()];
        store.apply_batch(batch(0, 0, &payload)).expect("batch");

        let sample = 20_000;
        let began = Instant::now();
        for _ in 0..sample {
            store.get_many(&asked).expect("get many");
        }
        let quiet = began.elapsed() / sample;

        let is_writing = Arc::new(AtomicBool::new(true));
        let mut sending = Vec::with_capacity(usize::from(writers));
        for group in 1..=writers {
            let store = Arc::clone(&store);
            let is_writing = Arc::clone(&is_writing);
            sending.push(thread::spawn(move || {
                let payload = vec![0xa5u8; payload_bytes()];
                let mut round = 0u32;
                while is_writing.load(Ordering::Acquire) {
                    store
                        .apply_batch(batch(group, round, &payload))
                        .expect("batch");
                    round += 1;
                }
            }));
        }

        let began = Instant::now();
        for _ in 0..sample {
            store.get_many(&asked).expect("get many");
        }
        let beside = began.elapsed() / sample;
        is_writing.store(false, Ordering::Release);
        for writer in sending {
            writer.join().expect("writer");
        }

        println!(
            "{:>8} {:>16} {:>16} {:>9.2}x",
            writers,
            format!("{quiet:.2?}"),
            format!("{beside:.2?}"),
            beside.as_secs_f64() / quiet.as_secs_f64().max(f64::MIN_POSITIVE),
        );
    }
}

// what a read that cannot name its keys pays, quiet and beside publishers
//
// A page of a column cannot know its keys before it has them, so it stands for every
// walk, total and snapshot in the engine: it fills holding nothing and checks the
// publish counts, falling back to the stripes when a batch lands under it.
pub fn whole_set_read_under_publish() {
    println!();
    println!(
        "{:>8} {:>16} {:>16} {:>10}",
        "writers", "quiet page", "page beside", "ratio"
    );

    for writers in [1u16, 4, 16] {
        let dir = TempDir::new().expect("tempdir");
        let store = Arc::new(open_at(&dir));
        let payload = vec![0xa5u8; payload_bytes()];
        store.apply_batch(batch(0, 0, &payload)).expect("batch");

        let mut page = KeyPage::with_lens();
        let sample = 20_000;
        let began = Instant::now();
        for _ in 0..sample {
            store
                .page(RECORDS, Bound::Unbounded, KEYS as usize, &mut page)
                .expect("page");
        }
        let quiet = began.elapsed() / sample;

        let is_writing = Arc::new(AtomicBool::new(true));
        let mut sending = Vec::with_capacity(usize::from(writers));
        for group in 1..=writers {
            let store = Arc::clone(&store);
            let is_writing = Arc::clone(&is_writing);
            sending.push(thread::spawn(move || {
                let payload = vec![0xa5u8; payload_bytes()];
                let mut round = 0u32;
                while is_writing.load(Ordering::Acquire) {
                    store
                        .apply_batch(batch(group, round, &payload))
                        .expect("batch");
                    round += 1;
                }
            }));
        }

        let began = Instant::now();
        for _ in 0..sample {
            store
                .page(RECORDS, Bound::Unbounded, KEYS as usize, &mut page)
                .expect("page");
        }
        let beside = began.elapsed() / sample;
        is_writing.store(false, Ordering::Release);
        for writer in sending {
            writer.join().expect("writer");
        }

        println!(
            "{:>8} {:>16} {:>16} {:>9.2}x",
            writers,
            format!("{quiet:.2?}"),
            format!("{beside:.2?}"),
            beside.as_secs_f64() / quiet.as_secs_f64().max(f64::MIN_POSITIVE),
        );
    }
}
