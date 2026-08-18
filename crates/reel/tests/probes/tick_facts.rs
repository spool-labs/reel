//! What pricing the standing stack costs a tick, before and after the facts are held
//!
//! A tick asks the same question of every standing run: is this segment a sorted run at
//! all. The answer is settled by the footer at the seal, so the first ask derives it and
//! every ask after it comes off the memo. The first column here is what a tick used to
//! cost whether or not it merged anything, the second is what one costs now.
//!
//! Two footer cache sizes, since the old cost had two halves: a volume whose footers fit
//! paid the row walk, and one past them paid the read as well.
//!
//! Opt-in. Run with:
//!   cargo test -p reel --release --test probes -- tick_facts

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, Preallocate, RecordKey,
    ReelConfig, ReelStore, SyncPolicy, ThreadBudget,
};

/// Virtual root the simulator's files live under
const ROOT: &str = "/bulk";

const RECORDS: ColumnId = ColumnId(1);

/// Bytes a record key occupies
const KEY_LEN: usize = 32;

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: RECORDS,
    name: "records",
    key_width: KeyWidth::Fixed(KEY_LEN as u16),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// Payload every record carries, small because the question is per segment
const PAYLOAD: usize = 64;

/// Segment size, which sets how many segments a fill produces
const SEGMENT: u64 = 64 * 1024;

/// Standing runs the stack is priced at
const SEGMENTS: usize = 512;

/// Footer cache sizes the walk is timed under
const CACHES: &[(&str, ByteCount)] = &[
    ("past its footers", ByteCount::from_bytes(4096)),
    ("holds its footers", ByteCount::mb(64)),
];

fn config(footer_cache: ByteCount) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT),
        alloc_chunk: ByteCount::from_bytes(8 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        rewrite_on_seal: true,
        merge_sorted_runs: true,
        footer_cache,
        ..ReelConfig::default()
    }
}

/// Ascending keys, so every segment seals as a run without a rewrite
fn key(at: u64) -> RecordKey {
    let mut bytes = [0u8; KEY_LEN];
    bytes[..8].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

// what a tick pays to price the stack, deriving the facts against holding them
pub fn pricing_the_stack_by_segment_count() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    println!(
        "{:>18}  {:>9}  {:>12}  {:>12}  {:>12}",
        "volume", "segments", "first ask", "later ask", "per segment"
    );

    for (name, footer_cache) in CACHES {
        let sim = SimIo::new(FaultPlan::new(1));
        let store = ReelStore::open_with_io(
            PathBuf::from(ROOT),
            config(*footer_cache),
            COLUMNS,
            Arc::new(sim.clone()),
        )
        .expect("open");

        let payload = vec![0xa5u8; PAYLOAD];
        let mut at = 0u64;
        while store.index().segments_snapshot().len() < SEGMENTS + 1 {
            store.put(&key(at), &payload).expect("put");
            at += 1;
        }
        store.flush().expect("flush");
        // the runs the walk prices are the ones the index has been told about
        store.page_out_sealed().expect("settle");

        let start = Instant::now();
        let derived = store.sorted_run_dead_ratio().expect("price");
        let first = start.elapsed();

        let start = Instant::now();
        let held = store.sorted_run_dead_ratio().expect("price");
        let later = start.elapsed();

        // a walk that counted no runs timed the exclusions rather than the facts
        assert!(
            derived.is_some(),
            "the stack priced at nothing, so nothing was timed"
        );
        assert_eq!(derived, held, "the memo priced the stack differently");
        let segments = store.index().segments_snapshot().len();

        println!(
            "{:>18}  {:>9}  {:>12.2?}  {:>12.2?}  {:>12.2?}",
            name,
            segments,
            first,
            later,
            first / segments as u32,
        );
        drop(store);
    }
}
