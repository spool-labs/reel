//! Stress: a volume shaped like a devnet node's, 86% graves and 4 graves a graved key, compacted
//!
//! Run with `cargo test --release -p tape-reel --test grave_rewrite_stress -- --ignored --nocapture`.
//! GRAVE_REWRITE_KEYS (default 2M, 24M for 100M rows), GRAVE_REWRITE_ROUNDS, GRAVE_REWRITE_SEAL_EVERY,
//! GRAVE_REWRITE_SEGMENT and GRAVE_REWRITE_FOOTER_CACHE scale it. GRAVE_REWRITE_LAG deletes each
//! key that many puts after it lands, so a segment holds graves over data standing in the one
//! before it, as on a node that deletes while it writes; with ROUNDS=0 that is the only delete.
//! GRAVE_REWRITE_DEAD_RATIO_PCT sets the rewrite ratio. GRAVE_REWRITE_OPEN_ONLY=1 stops after the
//! reopen, for measuring the open alone. GRAVE_REWRITE_REPORT=1 prints the readings and skips the
//! gates.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use tempfile::TempDir;

use reel::config::{CompactRate, ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec};
use reel::format::footer::{directory_span, SegmentFooter};
use reel::units::ByteCount;
use reel::{KeyWidth, ReelStore};
use reel_core::Store;

/// Bytes the process holds from the allocator right now, and the most it ever held
static IN_USE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        grow(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        IN_USE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        grow(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        IN_USE.fetch_sub(layout.size(), Ordering::Relaxed);
        grow(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

fn grow(bytes: usize) {
    let now = IN_USE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(now, Ordering::Relaxed);
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn in_use() -> usize {
    IN_USE.load(Ordering::Relaxed)
}

fn reset_peak() -> usize {
    let now = in_use();
    PEAK.store(now, Ordering::Relaxed);
    now
}

/// The node's slice column: a two-byte spool, then a track address
const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(14),
    name: "slice",
    key_width: KeyWidth::Fixed(34),
    shard_bytes: 2,
    purge_mark: None,
    codec: Codec::None,
}];

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(env("GRAVE_REWRITE_SEGMENT", 64 << 20)),
        footer_cache: ByteCount::from_bytes(env("GRAVE_REWRITE_FOOTER_CACHE", 64 << 20)),
        sync: SyncPolicy::Never,
        compact_mbps: CompactRate::Auto,
        // Unpaced, so the scrub that counts dead bytes behind key runs finishes within the run
        scrub_mbps: 1 << 20,
        active_tails: ThreadBudget::threads(1),
        compact_dead_ratio: env("GRAVE_REWRITE_DEAD_RATIO_PCT", 50) as f64 / 100.0,
        ..ReelConfig::default()
    }
}

/// Run the maintenance tick, as a node does, until compaction stops moving anything
fn settle(store: &ReelStore) -> usize {
    let mut quiet = 0;
    let mut ticks = 0;
    let mut last = store.compaction_counters();
    // The scrub's rate gate pays out by wall time, and an idle tail seals after five seconds, so
    // the ticks are spaced and the quiet outlasts the idle seal
    let began = Instant::now();
    while quiet < 700 && ticks < 100_000 {
        store.maintain_once().expect("maintain");
        std::thread::sleep(std::time::Duration::from_millis(10));
        ticks += 1;
        let now = store.compaction_counters();
        // A trace every few hundred ticks, so a run that dies mid-wave still says where it was
        if ticks % 300 == 0 {
            println!(
                "    tick {ticks} at {:.0}s: {} rewritten, {} unlinked, {} dropped, {} carried, scrub {} MiB, heap {} MiB, footer state {} MiB",
                began.elapsed().as_secs_f64(),
                now.segments_rewritten,
                now.segments_unlinked_whole,
                now.tombstones_dropped,
                now.tombstones_carried,
                now.scrub_bytes >> 20,
                in_use() >> 20,
                store.footer_cache_bytes().to_bytes() >> 20
            );
        }
        // The scrub counts dead rows behind the key runs, so its progress counts as movement too
        let moved = (now.segments_rewritten, now.segments_unlinked_whole, now.tombstones_dropped, now.tombstones_carried, now.scrub_bytes)
            != (last.segments_rewritten, last.segments_unlinked_whole, last.tombstones_dropped, last.tombstones_carried, last.scrub_bytes);
        quiet = if moved { 0 } else { quiet + 1 };
        last = now;
    }
    ticks
}

/// A key in one of 1,024 spools with a random address behind it, so spans overlap everywhere
fn key(n: u64) -> [u8; 34] {
    let mut key = [0u8; 34];
    key[..2].copy_from_slice(&((n % 1024) as u16).to_be_bytes());
    let mut x = n.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    for chunk in key[2..].chunks_mut(8) {
        x ^= x >> 29;
        x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x ^= x >> 32;
        chunk.copy_from_slice(&x.to_le_bytes()[..chunk.len()]);
    }
    key
}

/// Whether a key survives every round: one in ten does
fn stays(n: u64) -> bool {
    n % 10 == 0
}

/// Footer rows on the volume, as graves and data, read off every sealed segment's tail
fn rows_on_disk(root: &std::path::Path) -> (u64, u64, usize) {
    let (mut graves, mut data, mut sealed) = (0u64, 0u64, 0usize);
    for entry in std::fs::read_dir(root).expect("root") {
        let path = entry.expect("entry").path();
        if path.extension().is_none_or(|ext| ext != "reel") {
            continue;
        }
        let bytes = std::fs::read(&path).expect("segment");
        let tail = &bytes[bytes.len().saturating_sub(512)..];
        let Ok(Some(span)) = directory_span(tail, bytes.len() as u64) else {
            continue;
        };
        let footer = SegmentFooter::parse(&bytes[bytes.len() - span.footer_len..]).expect("footer");
        sealed += 1;
        let (mut here_graves, mut here_data) = (0u64, 0u64);
        for partition in &footer.partitions {
            for entry in partition.entries() {
                let entry = entry.expect("entry");
                match entry.is_tombstone() || entry.is_range_tombstone() {
                    true => here_graves += 1,
                    false => here_data += 1,
                }
            }
        }
        if env("GRAVE_REWRITE_SEGMENTS", 0) == 1 {
            println!(
                "    {} graves={here_graves} data={here_data} lsn={:?}..{:?}",
                path.file_name().and_then(|name| name.to_str()).unwrap_or(""),
                footer.min_lsn,
                footer.max_lsn
            );
        }
        graves += here_graves;
        data += here_data;
    }
    (graves, data, sealed)
}

fn check(store: &ReelStore, keys: u64) {
    for n in (0..keys).step_by(7) {
        let got = Store::get(store, "slice", &key(n)).expect("get");
        match stays(n) {
            true => assert_eq!(got.map(|value| value.len()), Some(8), "live key {n} lost"),
            false => assert!(got.is_none(), "deleted key {n} came back"),
        }
    }
}

// the devnet shape: every key written once, nine in ten deleted four times, then compacted
#[test]
#[ignore]
fn rewrite_through_repeat_graves() {
    let keys = env("GRAVE_REWRITE_KEYS", 2_000_000);
    let rounds = env("GRAVE_REWRITE_ROUNDS", 4);
    let seal_every = env("GRAVE_REWRITE_SEAL_EVERY", 300_000);
    let lag = env("GRAVE_REWRITE_LAG", 0);
    let report_only = env("GRAVE_REWRITE_REPORT", 0) == 1;
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().to_path_buf();
    let store = ReelStore::open(root.clone(), config(), COLUMNS).expect("open");

    let began = Instant::now();
    for n in 0..keys {
        Store::put(&store, "slice", &key(n), &n.to_le_bytes()).expect("put");
        if lag > 0 && n >= lag && !stays(n - lag) {
            Store::delete(&store, "slice", &key(n - lag)).expect("delete");
        }
        if n % seal_every == seal_every - 1 {
            drop(store.cue().expect("seal"));
        }
    }
    // The last keys never fell a lag behind a put, so they go now
    if lag > 0 {
        for n in (keys.saturating_sub(lag)..keys).filter(|n| !stays(*n)) {
            Store::delete(&store, "slice", &key(n)).expect("delete");
        }
    }
    store.flush().expect("flush");
    for round in 0..rounds {
        let mut written = 0u64;
        for n in (0..keys).filter(|n| !stays(*n)) {
            Store::delete(&store, "slice", &key(n)).expect("delete");
            written += 1;
            if written % seal_every == 0 {
                drop(store.cue().expect("seal"));
            }
        }
        store.flush().expect("flush");
        println!("  round {round} done at {:.1}s", began.elapsed().as_secs_f64());
    }
    drop(store.cue().expect("seal"));
    store.page_out_sealed().expect("page out");
    // Key runs cover the sealed segments, as on a node whose tick merges keep up
    let mut merges = 0;
    while store.merge_when_due().expect("merge").is_some() {
        store.page_out_sealed().expect("page out");
        merges += 1;
    }
    println!("  {merges} key run merges");
    check(&store, keys);
    let (graves, data, sealed) = rows_on_disk(&root);
    let rows_before = graves + data;
    println!(
        "fill {:.1}s: {rows_before} rows, {graves} graves ({:.1}%), {data} data, {sealed} sealed segments, heap {} MiB",
        began.elapsed().as_secs_f64(),
        100.0 * graves as f64 / rows_before as f64,
        in_use() >> 20
    );

    let began = Instant::now();
    store.close().expect("close");
    drop(store);
    let before_open = reset_peak();
    let store = ReelStore::open(root.clone(), config(), COLUMNS).expect("reopen");
    let open_peak = PEAK.load(Ordering::Relaxed);
    let after_open = reset_peak();
    println!(
        "close and reopen {:.1}s, heap after open {} MiB, open peak {} MiB over {} MiB before",
        began.elapsed().as_secs_f64(),
        after_open >> 20,
        open_peak >> 20,
        before_open >> 20
    );
    // The open takes one slot a distinct key, graves included until the load ends, and grows the
    // shards by rungs from one segment's rows. Its pipeline holds ten parsed footers with their
    // shard scratch, about 100 B a row of a segment, whatever the volume holds. With key runs
    // covering the segments it loads only the picked rows; without them every row past the first
    // of a key waits set aside until the load ends, about 100 B each
    if !report_only {
        let per_segment = rows_before as usize / sealed.max(1);
        let pipeline = 10 * 100 * per_segment;
        let aside = match merges {
            0 => 100 * (rows_before - keys) as usize,
            _ => 0,
        };
        assert!(
            open_peak <= after_open + 48 * keys as usize + pipeline + aside + (64 << 20),
            "the open peaked at {} MiB against {} MiB after it, for {keys} keys, {per_segment} rows a segment and {merges} key run merges",
            open_peak >> 20,
            after_open >> 20
        );
    }
    if env("GRAVE_REWRITE_OPEN_ONLY", 0) == 1 {
        return;
    }

    let began = Instant::now();
    let probes_before = store.filter_probes();
    let ticks = settle(&store);
    let wave = began.elapsed();
    let peak = PEAK.load(Ordering::Relaxed);
    let probes = store.filter_probes();
    let counters = store.compaction_counters();
    let map_reads = probes.map_reads - probes_before.map_reads;
    // Seal the destination tail, so the rows on disk include what the wave carried
    drop(store.cue().expect("seal"));
    store.page_out_sealed().expect("page out");
    let (graves_after, data_after, sealed_after) = rows_on_disk(&root);
    let rows_after = graves_after + data_after;
    println!(
        "wave {:.1}s in {ticks} ticks: {} rewritten, {} unlinked whole, {} graves dropped, {} carried, {map_reads} footer map reads, scrub {} MiB",
        wave.as_secs_f64(),
        counters.segments_rewritten,
        counters.segments_unlinked_whole,
        counters.tombstones_dropped,
        counters.tombstones_carried,
        counters.scrub_bytes >> 20
    );
    println!(
        "after: {rows_after} rows ({:.1}% of before), {graves_after} graves, {data_after} data, {sealed_after} sealed, heap peak {} MiB against {} MiB after open, now {} MiB, footer state {} MiB",
        100.0 * rows_after as f64 / rows_before as f64,
        peak >> 20,
        after_open >> 20,
        in_use() >> 20,
        store.footer_cache_bytes().to_bytes() >> 20
    );
    check(&store, keys);

    let began = Instant::now();
    store.close().expect("close");
    drop(store);
    let store = ReelStore::open(root.clone(), config(), COLUMNS).expect("reopen");
    println!("reopen after the wave {:.1}s", began.elapsed().as_secs_f64());
    check(&store, keys);

    // The second open finds the last grave of each deleted key standing over nothing, so the
    // second wave drops those too, and a third wave has nothing left to move
    let began = Instant::now();
    settle(&store);
    drop(store.cue().expect("seal"));
    store.page_out_sealed().expect("page out");
    let again = store.compaction_counters();
    let (graves_second, data_second, sealed_second) = rows_on_disk(&root);
    println!(
        "second wave {:.1}s: {} rewritten, {} unlinked whole, {} dropped, {} carried; {} rows ({:.1}% of before), {graves_second} graves, {data_second} data, {sealed_second} sealed",
        began.elapsed().as_secs_f64(),
        again.segments_rewritten,
        again.segments_unlinked_whole,
        again.tombstones_dropped,
        again.tombstones_carried,
        graves_second + data_second,
        100.0 * (graves_second + data_second) as f64 / rows_before as f64
    );
    check(&store, keys);
    store.close().expect("close");
    drop(store);
    let store = ReelStore::open(root, config(), COLUMNS).expect("reopen");
    check(&store, keys);
    settle(&store);
    let third = store.compaction_counters();
    println!(
        "third wave: {} rewritten, {} unlinked whole, {} carried",
        third.segments_rewritten,
        third.segments_unlinked_whole,
        third.tombstones_carried
    );

    if report_only {
        return;
    }
    assert!(
        (graves_second + data_second) * 100 <= rows_before * 15,
        "{} rows stayed after the second wave: the last graves did not drop",
        graves_second + data_second
    );
    assert_eq!(
        (third.segments_rewritten, third.tombstones_carried),
        (0, 0),
        "a third wave rewrote segments or carried graves again"
    );
    let touched = counters.tombstones_dropped + counters.tombstones_carried;
    assert!(
        map_reads * 100 <= touched.max(1),
        "{map_reads} footer map reads for {touched} graves: the rewrite reads footers per grave"
    );
    // A grave over data in a segment still standing is carried once. A carried grave that
    // dragged its destination's floor down would carry the rest of its pass with it
    assert!(
        counters.tombstones_carried <= graves,
        "{} carries for {graves} graves: a carried grave carried the graves behind it",
        counters.tombstones_carried
    );
    assert!(
        rows_after * 100 <= rows_before * 15,
        "{rows_after} of {rows_before} rows stayed: the graves did not drop"
    );
    // The wave holds every sealed segment's directory and filters, about 1.5 B a row at ten
    // filter bits, may fill the footer cache, parses one footer for a probe, holds one pass's
    // buffers, and notes each row copied into a destination by key until the destination hands
    // over, about 24 B a live row. Never a share of the graves beyond their filter bits
    let cache = env("GRAVE_REWRITE_FOOTER_CACHE", 64 << 20) as usize;
    let resident = (rows_before as usize * 3) / 2;
    let copies = 24 * data as usize / 10;
    assert!(
        peak <= after_open + resident + copies + 2 * cache + (64 << 20),
        "heap peaked at {} MiB during the wave against {} MiB after open, {} MiB of resident directories and filters, {} MiB of noted copies and a {} MiB footer cache",
        peak >> 20,
        after_open >> 20,
        resident >> 20,
        copies >> 20,
        cache >> 20
    );
}
