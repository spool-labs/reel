//! What each resident index shape costs at the sizes one shard holds
//! Run with `cargo test -p tape-reel --test index_shape --release -- --ignored --nocapture`

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Instant;

use crossbeam_skiplist::SkipMap;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rustc_hash::FxBuildHasher;
use scc::{Guard, TreeIndex};

use reel::format::column::{Codec, ColumnId, ColumnSpec, KeyWidth, RecordKey};
use reel::format::loc::{Loc, SegmentId};
use reel::format::lsn::Lsn;
use reel::index::column::{Shape, ShardMap, Trees, VarTrees, WidthIndex, VAR_NODE_WIDTH};
use reel::index::counters::SegmentTable;
use reel::index::entry::Entry;
use reel::index::tbtreemap::{
    node_width, scan_backend, scans, Shared, TBTreeMap, TreeKey, Whole, MAX_NODE_WIDTH,
    MIN_NODE_WIDTH, NODE_BUDGET, SHARED_CAP,
};

/// Each node holds this many 34 byte record keys
const RECORD_NODES: usize = node_width(34);

/// Each node holds this many 32 byte address keys
const ADDRESS_NODES: usize = node_width(32);

/// The record key shape, two group bytes and a thirty-two byte id
type Key = [u8; 34];

/// A stand-in index entry for the tree
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct TreeVal {
    segment: u32,
    offset: u32,
    len: u32,
    lsn: u64,
    incarnation: u32,
}

/// What an index entry weighs: a location, a sequence number and a stamp
#[derive(Clone, Copy, Debug, PartialEq)]
struct Val {
    segment: u32,
    offset: u32,
    len: u32,
    lsn: u64,
    incarnation: u32,
}

impl Val {
    fn at(seed: u64) -> Val {
        Val {
            segment: seed as u32,
            offset: seed as u32,
            len: 1024,
            lsn: seed,
            incarnation: 1,
        }
    }
}

/// Per-shard occupancies, spanning what a real shard holds
const SIZES: &[usize] = &[64, 256, 1024, 4096, 16384];

/// Lookups timed per arm
const PROBES: usize = 200_000;

fn keys(count: usize, seed: u64) -> Vec<Key> {
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..count)
        .map(|_| {
            let mut key = [0u8; 34];
            rng.fill(&mut key[..]);
            key
        })
        .collect()
}

/// Each arm runs this many times and reports the median
const REPEATS: usize = 5;

/// One arm's three timings: insert, get, scan, in nanoseconds per operation
type Timings = (f64, f64, f64);

/// An arm of the comparison: keys to hold, keys to probe with, three timings back
type Arm<K> = fn(&[K], &[K]) -> Timings;

/// The median of each of the three timings, over repeated runs of one arm
fn median_of<K>(run: Arm<K>, held: &[K], probes: &[K]) -> Timings {
    let mut inserts = Vec::with_capacity(REPEATS);
    let mut gets = Vec::with_capacity(REPEATS);
    let mut scans = Vec::with_capacity(REPEATS);
    for _ in 0..REPEATS {
        let (insert, get, scan) = run(held, probes);
        inserts.push(insert);
        gets.push(get);
        scans.push(scan);
    }
    for column in [&mut inserts, &mut gets, &mut scans] {
        column.sort_by(|a, b| a.partial_cmp(b).expect("a time"));
    }
    (inserts[REPEATS / 2], gets[REPEATS / 2], scans[REPEATS / 2])
}

/// Prints the lead scan backend this process picked
fn scan_note() {
    println!("scan backend: {}", scan_backend());
}

/// Nanoseconds per operation, given a run and how many it did
fn per_op(elapsed: std::time::Duration, ops: usize) -> f64 {
    elapsed.as_nanos() as f64 / ops as f64
}

fn bench_btree(keys: &[Key], probes: &[Key]) -> (f64, f64, f64) {
    let start = Instant::now();
    let mut map: BTreeMap<Key, Val> = BTreeMap::new();
    for (at, key) in keys.iter().enumerate() {
        map.insert(*key, Val::at(at as u64));
    }
    let insert = per_op(start.elapsed(), keys.len());

    let start = Instant::now();
    let mut hits = 0usize;
    for key in probes {
        if map.contains_key(key.as_slice()) {
            hits += 1;
        }
    }
    let get = per_op(start.elapsed(), probes.len());
    assert!(hits > 0, "the probe set never hit");

    let start = Instant::now();
    let mut walked = 0usize;
    for _ in map.iter() {
        walked += 1;
    }
    let scan = per_op(start.elapsed(), walked.max(1));
    (insert, get, scan)
}

fn bench_indexset(keys: &[Key], probes: &[Key]) -> (f64, f64, f64) {
    let start = Instant::now();
    let mut map: indexset::BTreeMap<Key, Val> = indexset::BTreeMap::new();
    for (at, key) in keys.iter().enumerate() {
        map.insert(*key, Val::at(at as u64));
    }
    let insert = per_op(start.elapsed(), keys.len());

    let start = Instant::now();
    let mut hits = 0usize;
    for key in probes {
        if map.get(key).is_some() {
            hits += 1;
        }
    }
    let get = per_op(start.elapsed(), probes.len());
    assert!(hits > 0, "the probe set never hit");

    let start = Instant::now();
    let mut walked = 0usize;
    for _ in map.iter() {
        walked += 1;
    }
    let scan = per_op(start.elapsed(), walked.max(1));
    (insert, get, scan)
}

fn bench_skipmap(keys: &[Key], probes: &[Key]) -> (f64, f64, f64) {
    let start = Instant::now();
    let map: SkipMap<Key, Val> = SkipMap::new();
    for (at, key) in keys.iter().enumerate() {
        map.insert(*key, Val::at(at as u64));
    }
    let insert = per_op(start.elapsed(), keys.len());

    let start = Instant::now();
    let mut hits = 0usize;
    for key in probes {
        if map.get(key).is_some() {
            hits += 1;
        }
    }
    let get = per_op(start.elapsed(), probes.len());
    assert!(hits > 0, "the probe set never hit");

    let start = Instant::now();
    let mut walked = 0usize;
    for _ in map.iter() {
        walked += 1;
    }
    let scan = per_op(start.elapsed(), walked.max(1));
    (insert, get, scan)
}

fn bench_treeindex(keys: &[Key], probes: &[Key]) -> (f64, f64, f64) {
    let start = Instant::now();
    let map: TreeIndex<Key, Val> = TreeIndex::new();
    for (at, key) in keys.iter().enumerate() {
        let _ = map.insert_sync(*key, Val::at(at as u64));
    }
    let insert = per_op(start.elapsed(), keys.len());

    let guard = Guard::new();
    let start = Instant::now();
    let mut hits = 0usize;
    for key in probes {
        if map.peek(key, &guard).is_some() {
            hits += 1;
        }
    }
    let get = per_op(start.elapsed(), probes.len());
    assert!(hits > 0, "the probe set never hit");

    let start = Instant::now();
    let mut walked = 0usize;
    for _ in map.iter(&guard) {
        walked += 1;
    }
    let scan = per_op(start.elapsed(), walked.max(1));
    (insert, get, scan)
}

fn bench_hash(keys: &[Key], probes: &[Key]) -> (f64, f64, f64) {
    let start = Instant::now();
    let mut map: HashMap<Key, Val, FxBuildHasher> = HashMap::default();
    for (at, key) in keys.iter().enumerate() {
        map.insert(*key, Val::at(at as u64));
    }
    let insert = per_op(start.elapsed(), keys.len());

    let start = Instant::now();
    let mut hits = 0usize;
    for key in probes {
        if map.contains_key(key.as_slice()) {
            hits += 1;
        }
    }
    let get = per_op(start.elapsed(), probes.len());
    assert!(hits > 0, "the probe set never hit");

    // A hash map has no order, so its scan sorts the keys
    let start = Instant::now();
    let mut all: Vec<&Key> = map.keys().collect();
    all.sort_unstable();
    let scan = per_op(start.elapsed(), all.len().max(1));
    (insert, get, scan)
}

// what each shape costs per op, at the occupancies one shard actually holds
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn shard_sized_shapes() {
    println!();
    scan_note();
    println!(
        "{:>7}  {:>12}  {:>9}  {:>9}  {:>9}",
        "size", "shape", "insert", "get", "scan/key"
    );

    for &size in SIZES {
        let held = keys(size, 1);
        let mut probes: Vec<Key> = Vec::with_capacity(PROBES);
        let mut rng = SmallRng::seed_from_u64(9);
        for _ in 0..PROBES {
            probes.push(held[rng.gen_range(0..held.len())]);
        }

        for (name, run) in [
            (
                "btreemap",
                bench_btree as fn(&[Key], &[Key]) -> (f64, f64, f64),
            ),
            ("indexset", bench_indexset),
            ("skipmap", bench_skipmap),
            ("treeindex", bench_treeindex),
            ("hash+fx", bench_hash),
        ] {
            let (insert, get, scan) = median_of(run, &held, &probes);
            println!("{size:>7}  {name:>12}  {insert:>7.1}ns  {get:>7.1}ns  {scan:>7.1}ns");
        }
        println!();
    }
}

/// The hand-rolled tree at one key width and one node width, same three timings
fn time_tree<const N: usize, const B: usize>(
    keys: &[[u8; N]],
    probes: &[[u8; N]],
) -> (f64, f64, f64) {
    let start = Instant::now();
    let mut map: TBTreeMap<[u8; N], B, TreeVal> = TBTreeMap::new();
    for (at, key) in keys.iter().enumerate() {
        map.insert(
            *key,
            TreeVal {
                segment: at as u32,
                offset: at as u32,
                len: 1024,
                lsn: at as u64,
                incarnation: 1,
            },
        );
    }
    let insert = per_op(start.elapsed(), keys.len());

    let start = Instant::now();
    let mut hits = 0usize;
    for key in probes {
        if map.get(key).is_some() {
            hits += 1;
        }
    }
    let get = per_op(start.elapsed(), probes.len());
    assert!(hits > 0, "the probe set never hit");

    let start = Instant::now();
    let mut walked = 0usize;
    for _ in map.iter() {
        walked += 1;
    }
    let scan = per_op(start.elapsed(), walked.max(1));
    (insert, get, scan)
}

// the hand-rolled tree against the rest, swept over node width
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn handrolled_node_width() {
    println!();
    scan_note();
    println!(
        "{:>7}  {:>12}  {:>9}  {:>9}  {:>9}",
        "size", "shape", "insert", "get", "scan/key"
    );

    for &size in SIZES {
        let held = keys(size, 1);
        let mut probes: Vec<Key> = Vec::with_capacity(PROBES);
        let mut rng = SmallRng::seed_from_u64(9);
        for _ in 0..PROBES {
            probes.push(held[rng.gen_range(0..held.len())]);
        }

        let (insert, get, scan) = median_of(bench_btree, &held, &probes);
        println!(
            "{size:>7}  {:>12}  {insert:>7.1}ns  {get:>7.1}ns  {scan:>7.1}ns",
            "btreemap"
        );

        for (name, run) in [
            (
                "handroll/8",
                time_tree::<34, 8> as fn(&[Key], &[Key]) -> (f64, f64, f64),
            ),
            ("handroll/16", time_tree::<34, 16>),
            ("handroll/32", time_tree::<34, 32>),
            ("handroll/64", time_tree::<34, 64>),
        ] {
            let (insert, get, scan) = median_of(run, &held, &probes);
            println!("{size:>7}  {name:>12}  {insert:>7.1}ns  {get:>7.1}ns  {scan:>7.1}ns");
        }
        println!();
    }
}

/// A tied draw spreads its keys over this many distinct leads
const TIED_LEADS: usize = 8;

/// Keys whose first `shared` bytes repeat across a few leads, ascending if asked
fn drawn<const N: usize>(count: usize, shared: usize, ascending: bool, seed: u64) -> Vec<[u8; N]> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let leads: Vec<[u8; 32]> = (0..TIED_LEADS)
        .map(|_| {
            let mut prefix = [0u8; 32];
            rng.fill(&mut prefix[..]);
            prefix
        })
        .collect();

    let mut keys: Vec<[u8; N]> = Vec::with_capacity(count);
    match ascending {
        false => {
            for at in 0..count {
                let mut key = [0u8; N];
                rng.fill(&mut key[..]);
                for byte in 0..shared {
                    key[byte] = leads[at % TIED_LEADS][byte % 32];
                }
                keys.push(key);
            }
        }
        true => {
            // Round major, so each lead's keys ascend and land at the right edge of their leaf
            let rounds = count.div_ceil(TIED_LEADS);
            for round in 0..rounds {
                for lead in &leads {
                    if keys.len() == count {
                        break;
                    }
                    let mut key = [0u8; N];
                    for byte in 0..shared {
                        key[byte] = lead[byte % 32];
                    }
                    key[shared..shared + 8].copy_from_slice(&(round as u64).to_be_bytes());
                    keys.push(key);
                }
            }
        }
    }
    keys
}

/// One key width's three shapes, each timed at every node width in the sweep
fn sweep_widths<const N: usize>(size: usize, shared: usize) {
    for (name, shared, ascending) in [
        ("scattered", 0, false),
        ("tied", shared, false),
        ("tied/asc", shared, true),
    ] {
        let held = drawn::<N>(size, shared, ascending, 0x9d1);
        let mut probes: Vec<[u8; N]> = Vec::with_capacity(PROBES);
        let mut rng = SmallRng::seed_from_u64(9);
        for _ in 0..PROBES {
            probes.push(held[rng.gen_range(0..held.len())]);
        }

        for (width, run) in [
            (
                8usize,
                time_tree::<N, 8> as fn(&[[u8; N]], &[[u8; N]]) -> (f64, f64, f64),
            ),
            (16, time_tree::<N, 16>),
            (32, time_tree::<N, 32>),
            (64, time_tree::<N, 64>),
        ] {
            let (insert, get, scan) = median_of(run, &held, &probes);
            let bytes = width * N;
            println!(
                "{N:>4}  {name:>10}  {size:>6}  {width:>6}  {bytes:>6}  {insert:>7.1}ns  {get:>7.1}ns  {scan:>7.1}ns"
            );
        }
    }
    println!();
}

// what a node width costs at the key widths the columns declare
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn node_width_by_key_size() {
    println!();
    scan_note();
    println!(
        "{:>4}  {:>10}  {:>6}  {:>6}  {:>6}  {:>9}  {:>9}  {:>9}",
        "key", "shape", "size", "width", "bytes", "insert", "get", "scan/key"
    );

    for &size in &[1024usize, 16384] {
        // Widths: slot and index, address, record, and the two status columns
        sweep_widths::<16>(size, 8);
        sweep_widths::<32>(size, 24);
        sweep_widths::<34>(size, 26);
        sweep_widths::<72>(size, 64);
        sweep_widths::<108>(size, 32);
    }
}

// every declared key width takes the node width its bytes ask for
#[test]
fn declared_widths_take_the_budget() {
    // The widths `ColumnIndex` declares, against the node width each takes.
    let widths: [(usize, usize); 16] = [
        (0, 64),
        (2, 64),
        (8, 64),
        (12, 64),
        (16, 64),
        (20, 51),
        (24, 42),
        (32, 32),
        (34, 30),
        (36, 28),
        (40, 25),
        (44, 23),
        (48, 21),
        (72, 16),
        (96, 16),
        (108, 16),
    ];

    for (key, width) in widths {
        assert_eq!(
            node_width(key),
            width,
            "{key} byte keys take the wrong node width"
        );
        assert!(
            width >= MIN_NODE_WIDTH,
            "{key} byte keys fell under the floor"
        );
        assert!(
            width <= MAX_NODE_WIDTH,
            "{key} byte keys went past the ceiling"
        );
        // A width at the floor may overshoot the budget, every other width stays inside it
        let bytes = width * key;
        match width {
            MIN_NODE_WIDTH => assert!(
                NODE_BUDGET / key.max(1) <= MIN_NODE_WIDTH,
                "{key} byte keys sat at the floor with budget to spare"
            ),
            _ => assert!(
                bytes <= NODE_BUDGET,
                "{key} byte keys hold {bytes} bytes a node"
            ),
        }
    }

    // At 64 keys a node the status columns would hold 6,912 and 4,608 bytes of key
    assert_eq!(node_width(108) * 108, 1728, "address_signatures node bytes");
    assert_eq!(node_width(72) * 72, 1152, "transaction_status node bytes");

    let _: <Trees<108> as Shape<[u8; 108]>>::Entries =
        TBTreeMap::<[u8; 108], { node_width(108) }, Entry>::new();
    let _: <Trees<72> as Shape<[u8; 72]>>::Entries =
        TBTreeMap::<[u8; 72], { node_width(72) }, Entry>::new();
    let _: <Trees<34> as Shape<[u8; 34]>>::Entries =
        TBTreeMap::<[u8; 34], { node_width(34) }, Entry>::new();
    let _: <Trees<16> as Shape<[u8; 16]>>::Entries =
        TBTreeMap::<[u8; 16], { node_width(16) }, Entry>::new();
}

// from_sorted builds the same tree an insert-driven load would, and packs it
#[test]
fn bulk_load_agrees_and_packs() {
    for size in [0usize, 1, 5, 64, 1000, 4096] {
        let mut held: Vec<Key> = keys(size, 3);
        held.sort_unstable();
        held.dedup();

        let pairs: Vec<(Key, TreeVal)> = held
            .iter()
            .enumerate()
            .map(|(at, key)| {
                (
                    *key,
                    TreeVal {
                        segment: at as u32,
                        offset: 0,
                        len: 0,
                        lsn: at as u64,
                        incarnation: 1,
                    },
                )
            })
            .collect();

        let tree: TBTreeMap<[u8; 34], RECORD_NODES, TreeVal> =
            TBTreeMap::from_sorted(pairs.clone(), RECORD_NODES);
        assert_eq!(tree.len(), held.len(), "size {size}: bulk load lost keys");

        for (at, key) in held.iter().enumerate() {
            let found = tree
                .get(key)
                .unwrap_or_else(|| panic!("size {size}: bulk load dropped a key"));
            assert_eq!(
                found.lsn, at as u64,
                "size {size}: bulk load misplaced a value"
            );
        }

        let walked: Vec<Key> = tree.iter().map(|(key, _)| *key).collect();
        assert_eq!(
            walked, held,
            "size {size}: the bulk built walk is out of order"
        );
    }
}

// what an install costs, insert-driven against built from the sorted run
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn install_from_sorted() {
    println!();
    println!(
        "{:>7}  {:>16}  {:>11}  {:>9}",
        "size", "install", "per key", "get"
    );

    for &size in SIZES {
        let mut held = keys(size, 1);
        held.sort_unstable();
        held.dedup();
        let pairs: Vec<(Key, TreeVal)> = held
            .iter()
            .map(|key| {
                (
                    *key,
                    TreeVal {
                        segment: 0,
                        offset: 0,
                        len: 0,
                        lsn: 1,
                        incarnation: 1,
                    },
                )
            })
            .collect();

        let mut probes: Vec<Key> = Vec::with_capacity(PROBES);
        let mut rng = SmallRng::seed_from_u64(9);
        for _ in 0..PROBES {
            probes.push(held[rng.gen_range(0..held.len())]);
        }

        let mut by_insert = Vec::new();
        let mut by_bulk = Vec::new();
        for _ in 0..REPEATS {
            let start = Instant::now();
            let mut tree: TBTreeMap<[u8; 34], RECORD_NODES, TreeVal> = TBTreeMap::new();
            for (key, val) in &pairs {
                tree.insert(*key, *val);
            }
            by_insert.push(per_op(start.elapsed(), pairs.len()));

            let start = Instant::now();
            let built: TBTreeMap<[u8; 34], RECORD_NODES, TreeVal> =
                TBTreeMap::from_sorted(pairs.clone(), RECORD_NODES);
            by_bulk.push(per_op(start.elapsed(), pairs.len()));
            assert_eq!(built.len(), tree.len(), "the two loads disagree on count");
        }
        by_insert.sort_by(|a, b| a.partial_cmp(b).expect("a time"));
        by_bulk.sort_by(|a, b| a.partial_cmp(b).expect("a time"));

        let built: TBTreeMap<[u8; 34], RECORD_NODES, TreeVal> =
            TBTreeMap::from_sorted(pairs.clone(), RECORD_NODES);
        let start = Instant::now();
        let mut hits = 0usize;
        for key in &probes {
            if built.get(key).is_some() {
                hits += 1;
            }
        }
        let get = per_op(start.elapsed(), probes.len());
        assert!(hits > 0, "the probe set never hit");

        println!(
            "{size:>7}  {:>16}  {:>9.1}ns  {get:>7.1}ns",
            "insert-driven",
            by_insert[REPEATS / 2]
        );
        println!(
            "{size:>7}  {:>16}  {:>9.1}ns  {get:>7.1}ns",
            "from_sorted",
            by_bulk[REPEATS / 2]
        );
    }
}

// keys sharing their eight byte lead still order and resolve exactly
#[test]
fn shared_leads_still_resolve() {
    let mut model: BTreeMap<Key, u64> = BTreeMap::new();
    let mut tree: TBTreeMap<[u8; 34], 16, TreeVal> = TBTreeMap::new();

    let mut step = 0u64;
    for run in 0..8u8 {
        for tail in 0..64u16 {
            let mut key = [0u8; 34];
            // Same lead for a whole run, so the bisect cannot separate them.
            key[..8].copy_from_slice(&(run as u64).to_be_bytes());
            key[8..10].copy_from_slice(&tail.to_be_bytes());
            model.insert(key, step);
            tree.insert(
                key,
                TreeVal {
                    segment: 0,
                    offset: 0,
                    len: 0,
                    lsn: step,
                    incarnation: 1,
                },
            );
            step += 1;
        }
    }

    assert_eq!(tree.len(), model.len(), "a tied lead lost a key");
    for (key, held) in &model {
        let found = tree.get(key).expect("a key with a tied lead went missing");
        assert_eq!(found.lsn, *held, "a tied lead resolved to the wrong entry");
    }
    let walked: Vec<Key> = tree.iter().map(|(key, _)| *key).collect();
    let expected: Vec<Key> = model.keys().copied().collect();
    assert_eq!(walked, expected, "tied leads walk out of order");

    // `chunks` walks the same keys as the pair walk
    let chunked: Vec<Key> = tree
        .chunks()
        .flat_map(|(keys, _)| keys.iter().copied())
        .collect();
    assert_eq!(
        chunked, expected,
        "the chunked walk disagrees with the ordered walk"
    );
}

// what each level of the tree weighs, and what a descent actually reads
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn level_footprints() {
    println!();
    println!("B={RECORD_NODES}, 34 byte keys, separators truncated into the leads");
    let inner_bytes = 8 + RECORD_NODES * 8 + RECORD_NODES * 4 + 24;
    println!("  inner node          {inner_bytes} bytes, all of it read by a descent");
    println!("  full separators ride the spill only where a boundary ties eight bytes");
    for keys in [1024usize, 4096, 16384, 65536] {
        let leaves = keys.div_ceil(RECORD_NODES);
        let inners = leaves.div_ceil(RECORD_NODES) + 1;
        println!(
            "  {keys:>6} keys: {leaves:>4} leaves, {inners:>3} inners, inner level {:>4} KiB, leaf level {:>5} KiB",
            inners * inner_bytes / 1024,
            leaves * (8 + RECORD_NODES * 8 + RECORD_NODES * 34 + RECORD_NODES * 24 + 8) / 1024
        );
    }
}

/// Counts live heap bytes in front of the system allocator
struct Counted;

static ARMED: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicI64 = AtomicI64::new(0);

unsafe impl GlobalAlloc for Counted {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            LIVE.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ARMED.load(Ordering::Relaxed) {
            LIVE.fetch_sub(layout.size() as i64, Ordering::Relaxed);
        }
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            LIVE.fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counted = Counted;

/// Weigh what a closure leaves the allocator holding
fn weighed<T>(build: impl FnOnce() -> T) -> (T, i64) {
    LIVE.store(0, Ordering::Relaxed);
    ARMED.store(true, Ordering::Relaxed);
    let held = build();
    ARMED.store(false, Ordering::Relaxed);
    (held, LIVE.load(Ordering::Relaxed))
}

/// A 32 byte address key
type Pubkey = [u8; 32];

fn pubkeys(count: usize, seed: u64) -> Vec<Pubkey> {
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..count)
        .map(|_| {
            let mut key = [0u8; 32];
            rng.fill(&mut key);
            key
        })
        .collect()
}

/// The engine's own `Entry`, which every arm holds
fn entry_at(at: u64) -> Entry {
    Entry::new(Loc::new(SegmentId(1), at as u32, 200), Lsn(at))
}

/// The pairs in key order, built inside the weighed closure so they net out
fn sorted_run(keys: &[Pubkey]) -> Vec<(Pubkey, Entry)> {
    let mut run: Vec<(Pubkey, Entry)> = Vec::with_capacity(keys.len());
    for (at, key) in keys.iter().enumerate() {
        run.push((*key, entry_at(at as u64)));
    }
    run.sort_unstable_by_key(|entry| entry.0);
    run
}

/// The engine's tree at one of the two loads
fn tbtree_at(keys: &[Pubkey], is_installed: bool) -> TBTreeMap<Pubkey, ADDRESS_NODES, Entry> {
    if is_installed {
        return TBTreeMap::from_sorted(sorted_run(keys), ADDRESS_NODES);
    }
    let mut map: TBTreeMap<Pubkey, ADDRESS_NODES, Entry> = TBTreeMap::new();
    for (at, key) in keys.iter().enumerate() {
        map.insert(*key, entry_at(at as u64));
    }
    map
}

/// The standard ordered map at the same two loads
fn btree_at(keys: &[Pubkey], is_installed: bool) -> BTreeMap<Pubkey, Entry> {
    if is_installed {
        return BTreeMap::from_iter(sorted_run(keys));
    }
    let mut map: BTreeMap<Pubkey, Entry> = BTreeMap::new();
    for (at, key) in keys.iter().enumerate() {
        map.insert(*key, entry_at(at as u64));
    }
    map
}

/// The standard hash map, grown from empty or reserved for its keys up front
fn hash_at(keys: &[Pubkey], is_installed: bool) -> HashMap<Pubkey, Entry, FxBuildHasher> {
    let mut map = match is_installed {
        true => HashMap::with_capacity_and_hasher(keys.len(), FxBuildHasher),
        false => HashMap::with_hasher(FxBuildHasher),
    };
    for (at, key) in keys.iter().enumerate() {
        map.insert(*key, entry_at(at as u64));
    }
    map
}

/// Weigh one container and time a probe of every key it holds
fn weigh_arm<Map>(
    build: impl FnOnce() -> Map,
    probes: &[Pubkey],
    hit: impl Fn(&Map, &Pubkey) -> bool,
) -> (f64, f64) {
    let (map, bytes) = weighed(build);

    let began = Instant::now();
    let mut found = 0usize;
    for key in probes {
        found += usize::from(hit(&map, key));
    }
    let per_probe = per_op(began.elapsed(), probes.len());

    assert_eq!(found, probes.len(), "an arm lost keys");
    (bytes as f64 / probes.len() as f64, per_probe)
}

// what a resident 32 byte key costs, tree against the standard maps
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn key_footprint() {
    // Bare maps with no shards, locks or payload pool, so only the container is weighed
    println!(
        "value is the engine's Entry, {} bytes",
        std::mem::size_of::<Entry>()
    );
    println!(
        "{:>9} {:>10} {:>10} {:>9} {:>10} {:>9} {:>10} {:>9}",
        "keys", "load", "tbtree B", "get ns", "btree B", "get ns", "hash B", "get ns",
    );

    for count in [262_144usize, 1_048_576] {
        let keys = pubkeys(count, 7);
        let probes = pubkeys(count, 7);

        for is_installed in [false, true] {
            let hit_tree = |map: &TBTreeMap<Pubkey, ADDRESS_NODES, Entry>, key: &Pubkey| {
                map.get(key).is_some()
            };
            let (tb_bytes, tb_ns) = weigh_arm(|| tbtree_at(&keys, is_installed), &probes, hit_tree);
            let (bt_bytes, bt_ns) = weigh_arm(
                || btree_at(&keys, is_installed),
                &probes,
                |map, key| map.get(key).is_some(),
            );
            let (hash_bytes, hash_ns) = weigh_arm(
                || hash_at(&keys, is_installed),
                &probes,
                |map, key| map.get(key).is_some(),
            );

            let load = match is_installed {
                true => "installed",
                false => "grown",
            };
            println!(
                "{count:>9} {load:>10} {tb_bytes:>10.1} {tb_ns:>9.1} {bt_bytes:>10.1} \
                 {bt_ns:>9.1} {hash_bytes:>10.1} {hash_ns:>9.1}",
            );
        }
    }
}

// what each byte of an index entry is worth, per step of the shrink
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn entry_shrink_is_worth() {
    #[allow(dead_code)]
    #[derive(Clone, Copy, Default)]
    struct V32([u8; 32]);
    #[allow(dead_code)]
    #[derive(Clone, Copy, Default)]
    struct V24([u8; 24]);
    #[allow(dead_code)]
    #[derive(Clone, Copy, Default)]
    struct V20([u8; 20]);
    #[allow(dead_code)]
    #[derive(Clone, Copy, Default)]
    struct V16([u8; 16]);

    fn tree<const W: usize, V: Default + Copy>(keys: &[Pubkey], make: impl Fn() -> V) -> f64 {
        let (map, bytes) = weighed(|| {
            let mut map: TBTreeMap<Pubkey, W, V> = TBTreeMap::new();
            for key in keys {
                map.insert(*key, make());
            }
            map
        });
        assert_eq!(map.len(), keys.len());
        drop(map);
        bytes as f64 / keys.len() as f64
    }

    fn hash<V: Default + Copy>(keys: &[Pubkey], make: impl Fn() -> V) -> f64 {
        let (map, bytes) = weighed(|| {
            let mut map: HashMap<Pubkey, V, FxBuildHasher> = HashMap::with_hasher(FxBuildHasher);
            for key in keys {
                map.insert(*key, make());
            }
            map
        });
        assert_eq!(map.len(), keys.len());
        drop(map);
        bytes as f64 / keys.len() as f64
    }

    println!();
    println!("1,048,576 pubkeys, 32 byte keys, values of each width");
    println!(
        "{:>7} {:>8} {:>14} {:>14} {:>10}",
        "entry", "payload", "tree B/key", "hash B/key", "tree saved"
    );
    let keys = pubkeys(1_048_576, 7);

    let base = tree::<64, V32>(&keys, V32::default);
    for (label, t, h) in [
        (32, base, hash(&keys, V32::default)),
        (
            24,
            tree::<64, V24>(&keys, V24::default),
            hash(&keys, V24::default),
        ),
        (
            20,
            tree::<64, V20>(&keys, V20::default),
            hash(&keys, V20::default),
        ),
        (
            16,
            tree::<64, V16>(&keys, V16::default),
            hash(&keys, V16::default),
        ),
    ] {
        println!(
            "{label:>7} {:>8} {t:>14.1} {h:>14.1} {:>10.1}",
            32 + label,
            base - t,
        );
    }
}

// what the node width costs in resident bytes and fill
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn node_width_costs() {
    fn arm<const B: usize>(keys: &[Pubkey], probes: &[Pubkey]) -> (f64, f64, f64) {
        let entry_at = |at: u64| Entry::new(Loc::new(SegmentId(1), at as u32, 200), Lsn(at));
        let (map, bytes) = weighed(|| {
            let mut map: TBTreeMap<Pubkey, B, Entry> = TBTreeMap::new();
            for (at, key) in keys.iter().enumerate() {
                map.insert(*key, entry_at(at as u64));
            }
            map
        });
        let began = Instant::now();
        let mut found = 0usize;
        for key in probes {
            found += usize::from(map.get(key).is_some());
        }
        let ns = per_op(began.elapsed(), probes.len());
        assert_eq!(found, keys.len(), "width {B} lost keys");
        let fill = map.fill_factor();
        drop(map);
        (bytes as f64 / keys.len() as f64, ns, fill)
    }

    println!();
    println!("1,048,576 pubkeys, the engine's Entry, insert built");
    println!(
        "{:>7} {:>12} {:>10} {:>8}",
        "width", "bytes/key", "get ns", "fill"
    );

    let keys = pubkeys(1_048_576, 7);
    let probes = pubkeys(1_048_576, 7);
    let (b, ns, fill) = arm::<8>(&keys, &probes);
    println!("{:>7} {b:>12.1} {ns:>10.1} {fill:>8.2}", 8);
    let (b, ns, fill) = arm::<16>(&keys, &probes);
    println!("{:>7} {b:>12.1} {ns:>10.1} {fill:>8.2}", 16);
    let (b, ns, fill) = arm::<32>(&keys, &probes);
    println!("{:>7} {b:>12.1} {ns:>10.1} {fill:>8.2}", 32);
    let (b, ns, fill) = arm::<64>(&keys, &probes);
    println!("{:>7} {b:>12.1} {ns:>10.1} {fill:>8.2}  <- shipped", 64);
    let (b, ns, fill) = arm::<128>(&keys, &probes);
    println!("{:>7} {b:>12.1} {ns:>10.1} {fill:>8.2}", 128);
}

/// Occupancies past the last level of cache
const BIG: &[usize] = &[262_144, 1_048_576, 4_194_304];

/// Fewer repeats past cache, where one arm is slow
const BIG_REPEATS: usize = 3;

fn big_median(run: Arm<Key>, held: &[Key], probes: &[Key]) -> Timings {
    let mut rows = Vec::with_capacity(BIG_REPEATS);
    for _ in 0..BIG_REPEATS {
        rows.push(run(held, probes));
    }
    rows.sort_by(|a, b| a.1.partial_cmp(&b.1).expect("a time"));
    rows[BIG_REPEATS / 2]
}

// the shapes past cache, which is the regime the tree was built for
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn past_cache() {
    println!();
    scan_note();
    println!(
        "{:>9}  {:>12}  {:>9}  {:>9}  {:>9}",
        "size", "shape", "insert", "get", "scan/key"
    );

    for &size in BIG {
        let held = keys(size, 1);
        let mut probes: Vec<Key> = Vec::with_capacity(PROBES);
        let mut rng = SmallRng::seed_from_u64(9);
        for _ in 0..PROBES {
            probes.push(held[rng.gen_range(0..held.len())]);
        }

        for (name, run) in [
            (
                "btreemap",
                bench_btree as fn(&[Key], &[Key]) -> (f64, f64, f64),
            ),
            ("handroll/16", time_tree::<34, 16>),
            ("handroll/32", time_tree::<34, 32>),
            ("handroll/64", time_tree::<34, 64>),
            ("hash+fx", bench_hash),
        ] {
            let (insert, get, scan) = big_median(run, &held, &probes);
            println!("{size:>9}  {name:>12}  {insert:>7.1}ns  {get:>7.1}ns  {scan:>7.1}ns");
        }
        println!();
    }
}

/// Batch sizes handed to `get_many`
const BATCHES: &[usize] = &[1, 4, 16, 64];

// what overlapping the descents buys, past cache where the misses are the cost
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn batched_descent() {
    println!();
    println!("{:>9}  {:>7}  {:>11}", "size", "batch", "per get");

    for &size in &[1_048_576usize, 4_194_304] {
        let mut held = keys(size, 1);
        held.sort_unstable();
        held.dedup();
        let pairs: Vec<(Key, TreeVal)> = held
            .iter()
            .map(|key| {
                (
                    *key,
                    TreeVal {
                        segment: 0,
                        offset: 0,
                        len: 0,
                        lsn: 1,
                        incarnation: 1,
                    },
                )
            })
            .collect();
        let tree: TBTreeMap<[u8; 34], RECORD_NODES, TreeVal> =
            TBTreeMap::from_sorted(pairs, RECORD_NODES);

        let mut rng = SmallRng::seed_from_u64(9);
        let probes: Vec<Key> = (0..PROBES)
            .map(|_| held[rng.gen_range(0..held.len())])
            .collect();

        for &batch in BATCHES {
            let mut out = Vec::with_capacity(batch);
            let mut rows = Vec::new();
            for _ in 0..3 {
                let start = Instant::now();
                let mut answered = 0usize;
                for run in probes.chunks(batch) {
                    tree.get_many(run, &mut out);
                    answered += out.iter().filter(|found| found.is_some()).count();
                }
                rows.push(per_op(start.elapsed(), probes.len()));
                assert!(answered > 0, "the probe set never hit");
            }
            rows.sort_by(|a, b| a.partial_cmp(b).expect("a time"));
            println!("{size:>9}  {batch:>7}  {:>9.1}ns", rows[1]);
        }
        println!();
    }
}

// what deletion does to fill, and what a rebuild takes back
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn fill_under_deletion() {
    println!();
    println!(
        "{:>26}  {:>6}  {:>8}  {:>9}  {:>9}",
        "state", "fill", "leaves", "get", "scan/key"
    );

    let size = 1_048_576usize;
    let mut held = keys(size, 5);
    held.sort_unstable();
    held.dedup();
    let pairs: Vec<(Key, TreeVal)> = held
        .iter()
        .map(|key| {
            (
                *key,
                TreeVal {
                    segment: 0,
                    offset: 0,
                    len: 0,
                    lsn: 1,
                    incarnation: 1,
                },
            )
        })
        .collect();

    let mut rng = SmallRng::seed_from_u64(9);
    let probes: Vec<Key> = (0..PROBES)
        .map(|_| held[rng.gen_range(0..held.len())])
        .collect();

    let mut tree: TBTreeMap<[u8; 34], RECORD_NODES, TreeVal> =
        TBTreeMap::from_sorted(pairs, RECORD_NODES);
    let report = |name: &str, tree: &TBTreeMap<[u8; 34], RECORD_NODES, TreeVal>, probes: &[Key]| {
        let start = Instant::now();
        let mut hits = 0usize;
        for key in probes {
            if tree.get(key).is_some() {
                hits += 1;
            }
        }
        let get = per_op(start.elapsed(), probes.len());
        let start = Instant::now();
        let walked = tree.iter().count();
        let scan = per_op(start.elapsed(), walked.max(1));
        println!(
            "{name:>26}  {:>5.1}%  {:>8}  {get:>7.1}ns  {scan:>7.1}ns",
            100.0 * tree.fill_factor(),
            tree.leaf_count()
        );
        let _ = hits;
    };

    report("built from sorted", &tree, &probes);

    // Delete three keys in four, which is the shape a group drop leaves behind.
    let mut survivors: Vec<Key> = Vec::new();
    for (at, key) in held.iter().enumerate() {
        match at % 4 {
            0 => survivors.push(*key),
            _ => {
                tree.remove(key);
            }
        }
    }
    let live: Vec<Key> = survivors.clone();
    let probes: Vec<Key> = (0..PROBES)
        .map(|_| live[rng.gen_range(0..live.len())])
        .collect();
    report("three in four deleted", &tree, &probes);

    let rebuilt: Vec<(Key, TreeVal)> = survivors
        .iter()
        .map(|key| {
            (
                *key,
                TreeVal {
                    segment: 0,
                    offset: 0,
                    len: 0,
                    lsn: 1,
                    incarnation: 1,
                },
            )
        })
        .collect();
    let start = Instant::now();
    let tree: TBTreeMap<[u8; 34], RECORD_NODES, TreeVal> =
        TBTreeMap::from_sorted(rebuilt, RECORD_NODES);
    let rebuild = per_op(start.elapsed(), live.len());
    report("rebuilt from survivors", &tree, &probes);
    println!("{:>26}  rebuild cost {rebuild:.1}ns a key", "");
}

// the sorted batch variant answers what the plain one does
#[test]
fn batch_variants_agree() {
    let held = {
        let mut all = keys(20_000, 13);
        all.sort_unstable();
        all.dedup();
        all
    };
    let pairs: Vec<(Key, TreeVal)> = held
        .iter()
        .enumerate()
        .map(|(at, key)| {
            (
                *key,
                TreeVal {
                    segment: 0,
                    offset: 0,
                    len: 0,
                    lsn: at as u64,
                    incarnation: 1,
                },
            )
        })
        .collect();
    let tree: TBTreeMap<[u8; 34], RECORD_NODES, TreeVal> =
        TBTreeMap::from_sorted(pairs, RECORD_NODES);

    // Present and absent both, and sorted as get_many_sorted requires.
    let mut asked: Vec<Key> = held.iter().step_by(3).copied().collect();
    asked.extend(keys(2_000, 91));
    asked.sort_unstable();

    let mut plain = Vec::new();
    let mut sorted = Vec::new();
    tree.get_many(&asked, &mut plain);
    tree.get_many_sorted(&asked, &mut sorted);

    let want: Vec<Option<u64>> = asked
        .iter()
        .map(|key| tree.get(key).map(|val| val.lsn))
        .collect();
    let seen = |rows: &Vec<Option<&TreeVal>>| -> Vec<Option<u64>> {
        rows.iter().map(|found| found.map(|val| val.lsn)).collect()
    };
    assert_eq!(
        seen(&plain),
        want,
        "the lane batch disagrees with single gets"
    );
    assert_eq!(seen(&sorted), want, "the sorted batch disagrees");
}

// what sorting the batch is worth on top of the prefetch
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn batch_variants() {
    println!();
    println!(
        "{:>9}  {:>7}  {:>16}  {:>10}",
        "size", "batch", "variant", "per get"
    );

    for &size in &[1024usize, 4096, 16384, 262_144, 1_048_576, 4_194_304] {
        let mut held = keys(size, 1);
        held.sort_unstable();
        held.dedup();
        let pairs: Vec<(Key, TreeVal)> = held
            .iter()
            .map(|key| {
                (
                    *key,
                    TreeVal {
                        segment: 0,
                        offset: 0,
                        len: 0,
                        lsn: 1,
                        incarnation: 1,
                    },
                )
            })
            .collect();
        let tree: TBTreeMap<[u8; 34], RECORD_NODES, TreeVal> =
            TBTreeMap::from_sorted(pairs, RECORD_NODES);

        let mut rng = SmallRng::seed_from_u64(9);
        let probes: Vec<Key> = (0..PROBES)
            .map(|_| held[rng.gen_range(0..held.len())])
            .collect();
        let mut ordered = probes.clone();
        ordered.sort_unstable();

        for &batch in &[16usize, 64] {
            for (name, is_sorted) in [("prefetched", false), ("sorted", true)] {
                let source = if is_sorted { &ordered } else { &probes };
                let mut out = Vec::with_capacity(batch);
                let mut rows = Vec::new();
                for _ in 0..3 {
                    let start = Instant::now();
                    let mut answered = 0usize;
                    for chunk in source.chunks(batch) {
                        match is_sorted {
                            false => tree.get_many(chunk, &mut out),
                            true => tree.get_many_sorted(chunk, &mut out),
                        }
                        answered += out.iter().filter(|found| found.is_some()).count();
                    }
                    rows.push(per_op(start.elapsed(), source.len()));
                    assert!(answered > 0, "the probe set never hit");
                }
                rows.sort_by(|a, b| a.partial_cmp(b).expect("a time"));
                println!("{size:>9}  {batch:>7}  {name:>16}  {:>8.1}ns", rows[1]);
            }
        }
        println!();
    }
}

// an unsorted run handed to the sorted batch is answered correctly anyway
#[test]
fn unsorted_run_still_answers() {
    let held = {
        let mut all = keys(8_000, 17);
        all.sort_unstable();
        all.dedup();
        all
    };
    let pairs: Vec<(Key, TreeVal)> = held
        .iter()
        .enumerate()
        .map(|(at, key)| {
            (
                *key,
                TreeVal {
                    segment: 0,
                    offset: 0,
                    len: 0,
                    lsn: at as u64,
                    incarnation: 1,
                },
            )
        })
        .collect();
    let tree: TBTreeMap<[u8; 34], RECORD_NODES, TreeVal> =
        TBTreeMap::from_sorted(pairs, RECORD_NODES);

    let mut rng = SmallRng::seed_from_u64(5);
    // Deliberately shuffled, with absent keys mixed through.
    let mut asked: Vec<Key> = held.iter().step_by(2).copied().collect();
    asked.extend(keys(400, 23));
    for at in (1..asked.len()).rev() {
        asked.swap(at, rng.gen_range(0..=at));
    }
    assert!(
        asked.windows(2).any(|pair| pair[0] > pair[1]),
        "the run came out sorted, so the guard is not being tested"
    );

    let mut sorted_call = Vec::new();
    tree.get_many_sorted(&asked, &mut sorted_call);
    let want: Vec<Option<u64>> = asked
        .iter()
        .map(|key| tree.get(key).map(|val| val.lsn))
        .collect();
    let seen: Vec<Option<u64>> = sorted_call
        .iter()
        .map(|found| found.map(|val| val.lsn))
        .collect();
    assert_eq!(seen, want, "an unsorted run was answered wrongly");
}

// the tree holds keys of any declared width
#[test]
fn any_declared_width_holds() {
    fn round_trip<const N: usize>(seed: u64) {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut model: BTreeMap<[u8; N], u64> = BTreeMap::new();
        let mut tree: TBTreeMap<[u8; N], 16, TreeVal> = TBTreeMap::new();

        for step in 0..3000u64 {
            let mut key = [0u8; N];
            rng.fill(&mut key[..]);
            model.insert(key, step);
            tree.insert(
                key,
                TreeVal {
                    segment: 0,
                    offset: 0,
                    len: 0,
                    lsn: step,
                    incarnation: 1,
                },
            );
        }
        assert_eq!(tree.len(), model.len(), "width {N}: counts differ");
        for (key, held) in &model {
            assert_eq!(
                tree.get(key).map(|val| val.lsn),
                Some(*held),
                "width {N}: a key is wrong"
            );
        }
        let walked: Vec<[u8; N]> = tree.iter().map(|(key, _)| *key).collect();
        let expected: Vec<[u8; N]> = model.keys().copied().collect();
        assert_eq!(walked, expected, "width {N}: the walk is out of order");
    }

    round_trip::<8>(1);
    round_trip::<24>(2);
    round_trip::<32>(3);
    round_trip::<48>(4);
    round_trip::<108>(5);
}

// range, clear, contains_key and is_empty agree with a `BTreeMap`
#[test]
fn range_and_the_rest_agree() {
    let mut held = keys(6000, 41);
    held.sort_unstable();
    held.dedup();

    let mut model: BTreeMap<Key, u64> = BTreeMap::new();
    let mut tree: TBTreeMap<[u8; 34], 16, TreeVal> = TBTreeMap::new();
    for (at, key) in held.iter().enumerate() {
        model.insert(*key, at as u64);
        tree.insert(
            *key,
            TreeVal {
                segment: 0,
                offset: 0,
                len: 0,
                lsn: at as u64,
                incarnation: 1,
            },
        );
    }

    assert!(!tree.is_empty(), "a filled map reads empty");
    assert!(tree.contains_key(&held[0]), "a held key is not found");
    let mut absent = held[0];
    absent[33] ^= 0xFF;
    assert_eq!(
        tree.contains_key(&absent),
        model.contains_key(&absent),
        "absence disagrees"
    );

    let walk = |tree: &TBTreeMap<[u8; 34], 16, TreeVal>,
                low: Bound<&Key>,
                high: Bound<&Key>|
     -> Vec<u64> { tree.range(low, high).map(|(_, val)| val.lsn).collect() };
    let want = |low: Bound<&Key>, high: Bound<&Key>| -> Vec<u64> {
        model
            .range((low.cloned(), high.cloned()))
            .map(|(_, held)| *held)
            .collect()
    };

    for (name, low, high) in [
        ("whole space", Bound::Unbounded, Bound::Unbounded),
        (
            "from the first",
            Bound::Included(&held[0]),
            Bound::Unbounded,
        ),
        (
            "excluding the first",
            Bound::Excluded(&held[0]),
            Bound::Unbounded,
        ),
        (
            "to the last",
            Bound::Unbounded,
            Bound::Included(&held[held.len() - 1]),
        ),
        (
            "open at the top",
            Bound::Unbounded,
            Bound::Excluded(&held[held.len() - 1]),
        ),
        (
            "a middle span",
            Bound::Included(&held[1000]),
            Bound::Excluded(&held[4000]),
        ),
        (
            "one key wide",
            Bound::Included(&held[77]),
            Bound::Included(&held[77]),
        ),
        (
            "empty span",
            Bound::Excluded(&held[77]),
            Bound::Included(&held[77]),
        ),
    ] {
        assert_eq!(
            walk(&tree, low, high),
            want(low, high),
            "range disagrees: {name}"
        );
    }

    // Resume from a key that is gone, which is what a budgeted sweep hands back.
    let gone = held[2500];
    tree.remove(&gone);
    model.remove(&gone);
    let after: Vec<u64> = tree
        .range(Bound::Included(&gone), Bound::Excluded(&held[2600]))
        .map(|(_, val)| val.lsn)
        .collect();
    let expected: Vec<u64> = model
        .range((Bound::Included(gone), Bound::Excluded(held[2600])))
        .map(|(_, held)| *held)
        .collect();
    assert_eq!(after, expected, "a resume from a deleted key disagrees");

    tree.clear();
    assert!(tree.is_empty(), "a cleared map is not empty");
    assert_eq!(tree.len(), 0, "a cleared map keeps a count");
    assert_eq!(
        tree.range(Bound::Unbounded, Bound::Unbounded).count(),
        0,
        "a cleared map walks"
    );
    assert!(
        !tree.contains_key(&held[0]),
        "a cleared map still holds a key"
    );
    // It has to work again afterwards, since a dropped group refills its shard.
    tree.insert(
        held[0],
        TreeVal {
            segment: 0,
            offset: 0,
            len: 0,
            lsn: 9,
            incarnation: 1,
        },
    );
    assert_eq!(
        tree.get(&held[0]).map(|val| val.lsn),
        Some(9),
        "a cleared map cannot refill"
    );
}

// what a span costs against a `BTreeMap`, per span and per key
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn range_cost() {
    println!();
    println!(
        "{:>9}  {:>7}  {:>12}  {:>11}  {:>11}",
        "size", "span", "shape", "per span", "per key"
    );

    for &size in &[16_384usize, 1_048_576] {
        let mut held = keys(size, 7);
        held.sort_unstable();
        held.dedup();

        let mut btree: BTreeMap<Key, TreeVal> = BTreeMap::new();
        let mut tree: TBTreeMap<[u8; 34], RECORD_NODES, TreeVal> = TBTreeMap::new();
        for (at, key) in held.iter().enumerate() {
            let val = TreeVal {
                segment: 0,
                offset: 0,
                len: 0,
                lsn: at as u64,
                incarnation: 1,
            };
            btree.insert(*key, val);
            tree.insert(*key, val);
        }

        let mut rng = SmallRng::seed_from_u64(3);
        // Where each span starts, as a budgeted sweep would resume.
        let starts: Vec<Key> = (0..2_000)
            .map(|_| held[rng.gen_range(0..held.len())])
            .collect();

        for &span in &[16usize, 256] {
            for shape in ["btreemap", "tbtreemap"] {
                let mut rows = Vec::new();
                for _ in 0..3 {
                    let start = Instant::now();
                    let mut walked = 0usize;
                    for from in &starts {
                        match shape {
                            "btreemap" => {
                                walked += btree
                                    .range((Bound::Included(*from), Bound::Unbounded))
                                    .take(span)
                                    .count()
                            }
                            _ => {
                                walked += tree
                                    .range(Bound::Included(from), Bound::Unbounded)
                                    .take(span)
                                    .count()
                            }
                        }
                    }
                    let elapsed = start.elapsed();
                    rows.push((
                        per_op(elapsed, starts.len()),
                        per_op(elapsed, walked.max(1)),
                    ));
                    assert!(walked > 0, "the spans walked nothing");
                }
                rows.sort_by(|a, b| a.0.partial_cmp(&b.0).expect("a time"));
                let (per_span, per_key) = rows[1];
                println!("{size:>9}  {span:>7}  {shape:>12}  {per_span:>9.1}ns  {per_key:>9.1}ns");
            }
        }
        println!();
    }
}

// the name map answers what a `BTreeMap` answers through `ShardMap`
#[test]
fn the_name_map_agrees_with_a_btreemap() {
    let entry = |lsn: u64| Entry::new(Loc::new(SegmentId(1), lsn as u32, 16), Lsn(lsn));

    let mut held = names::mixed(4000);
    held.sort();
    held.dedup();

    let mut model: BTreeMap<Box<[u8]>, Entry> = BTreeMap::new();
    let mut tree: TBTreeMap<Box<[u8]>, 32, Entry> = TBTreeMap::default();

    for (at, key) in held.iter().enumerate() {
        assert_eq!(
            model.insert(key.clone(), entry(at as u64)),
            ShardMap::put(&mut tree, key.clone(), entry(at as u64)),
            "the shapes displaced differently"
        );
    }
    for (at, key) in held.iter().enumerate().step_by(3) {
        assert_eq!(
            model.insert(key.clone(), entry(9_000 + at as u64)),
            ShardMap::put(&mut tree, key.clone(), entry(9_000 + at as u64)),
            "an overwrite displaced differently"
        );
    }

    assert_eq!(model.len(), tree.count(), "counts differ");
    assert_eq!(model.is_empty(), tree.vacant(), "vacancy differs");
    for key in held.iter().take(400) {
        assert_eq!(model.get(key), tree.at(key), "a lookup differs");
        assert_eq!(model.contains_key(key), tree.holds(key), "presence differs");
    }
    // Keys no bucket holds
    for absent in [b"".as_slice(), b"zzz".as_slice(), &[0xffu8; 40]] {
        assert_eq!(model.get(absent), tree.at(absent), "a miss differs");
    }

    let by_model: Vec<Lsn> = model.values().map(|held| held.lsn).collect();
    let by_tree: Vec<Lsn> = tree.walk().map(|(_, held)| held.lsn).collect();
    assert_eq!(by_model, by_tree, "the walks differ");

    let span_model: Vec<Lsn> = model
        .range::<Box<[u8]>, _>((Bound::Included(&held[500]), Bound::Excluded(&held[1500])))
        .map(|(_, held)| held.lsn)
        .collect();
    let span_tree: Vec<Lsn> = tree
        .span(Bound::Included(&held[500]), Bound::Excluded(&held[1500]))
        .map(|(_, held)| held.lsn)
        .collect();
    assert_eq!(span_model, span_tree, "the spans differ");

    let mut back_model = span_model.clone();
    back_model.reverse();
    let back_tree: Vec<Lsn> = tree
        .span_back(Bound::Included(&held[500]), Bound::Excluded(&held[1500]))
        .map(|(_, held)| held.lsn)
        .collect();
    assert_eq!(back_model, back_tree, "the backward spans differ");

    // `at_many` partitions the run against node separators, so a misplaced key shows here
    let asked: Vec<Box<[u8]>> = held.iter().step_by(7).take(64).cloned().collect();
    let by_model: Vec<Option<&Entry>> = asked.iter().map(|key| model.get(key)).collect();
    let mut by_tree: Vec<Option<&Entry>> = Vec::new();
    ShardMap::at_many(&tree, &asked, &mut by_tree);
    assert_eq!(by_model, by_tree, "the batched door differs");

    for key in held.iter().step_by(2) {
        assert_eq!(
            model.remove(key),
            ShardMap::take(&mut tree, key),
            "a delete differs"
        );
    }
    assert_eq!(model.len(), tree.count(), "counts differ after deletes");
    let by_model: Vec<Lsn> = model.values().map(|held| held.lsn).collect();
    let by_tree: Vec<Lsn> = tree.walk().map(|(_, held)| held.lsn).collect();
    assert_eq!(by_model, by_tree, "the walks differ after deletes");

    tree.empty();
    assert!(tree.vacant(), "a cleared shape is not empty");
    ShardMap::put(&mut tree, held[0].clone(), entry(1));
    assert_eq!(Some(&entry(1)), tree.at(&held[0]), "a refill differs");
}

// a bulk build of name keys holds what a key at a time holds
#[test]
fn a_bulk_name_build_matches_insertion() {
    let entry = |lsn: u64| Entry::new(Loc::new(SegmentId(1), lsn as u32, 16), Lsn(lsn));

    for corpus in names::corpora(2000) {
        let mut held = corpus.keys.clone();
        held.sort();
        held.dedup();

        let mut driven: TBTreeMap<Box<[u8]>, 32, Entry> = TBTreeMap::new();
        for (at, key) in held.iter().enumerate() {
            driven.insert(key.clone(), entry(at as u64));
        }
        let built: TBTreeMap<Box<[u8]>, 32, Entry> = TBTreeMap::from_sorted(
            held.iter()
                .enumerate()
                .map(|(at, key)| (key.clone(), entry(at as u64))),
            32,
        );

        let by_insert: Vec<(&Box<[u8]>, Lsn)> =
            driven.iter().map(|(key, held)| (key, held.lsn)).collect();
        let by_build: Vec<(&Box<[u8]>, Lsn)> =
            built.iter().map(|(key, held)| (key, held.lsn)).collect();
        assert_eq!(by_insert, by_build, "{}: the two loads differ", corpus.name);

        for key in held.iter().step_by(11) {
            assert_eq!(
                driven.get(key),
                built.get(key),
                "{}: a lookup differs",
                corpus.name
            );
        }
    }
}

// every scan backend counts a lead array the same
#[test]
fn every_scan_counts_alike() {
    let mut rng = SmallRng::seed_from_u64(101);
    for len in [0usize, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 64] {
        for shape in 0..4 {
            let mut leads: Vec<u64> = match shape {
                0 => (0..len).map(|_| rng.gen()).collect(),
                1 => vec![7u64; len],
                2 => (0..len as u64).collect(),
                _ => (0..len as u64).map(|at| u64::MAX - at).collect(),
            };
            leads.sort_unstable();

            for want in [0u64, 7, 42, u64::MAX, rng.gen()] {
                let plain = leads.iter().filter(|held| **held < want).count();

                #[cfg(not(target_arch = "x86_64"))]
                {
                    assert_eq!(
                        scans::scalar(&leads, want),
                        plain,
                        "scalar differs: len {len} shape {shape}"
                    );
                }
                #[cfg(target_arch = "x86_64")]
                {
                    assert_eq!(scans::scalar(&leads, want), plain, "scalar differs");
                    if is_x86_feature_detected!("avx2") {
                        assert_eq!(
                            unsafe { scans::avx2(&leads, want) },
                            plain,
                            "avx2 differs: len {len}"
                        );
                    }
                    if is_x86_feature_detected!("avx512f") {
                        assert_eq!(
                            unsafe { scans::avx512(&leads, want) },
                            plain,
                            "avx512 differs: len {len}"
                        );
                    }
                }
            }
        }
    }
}

// how long a rebuild holds a shard's lock, and how many leaves it gives back
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn rebuild_lock_hold() {
    println!();
    println!(
        "{:>8}  {:>7}  {:>11}  {:>10}  {:>9}  {:>9}",
        "per shard", "live", "hold", "per key", "leaves", "reclaimed"
    );

    for &held_keys in &[1024usize, 4096, 16384, 65536] {
        let mut held = keys(held_keys, 13);
        held.sort_unstable();
        held.dedup();

        let pairs: Vec<(Key, TreeVal)> = held
            .iter()
            .map(|key| {
                (
                    *key,
                    TreeVal {
                        segment: 0,
                        offset: 0,
                        len: 0,
                        lsn: 1,
                        incarnation: 1,
                    },
                )
            })
            .collect();
        let mut tree: TBTreeMap<[u8; 34], RECORD_NODES, TreeVal> =
            TBTreeMap::from_sorted(pairs, RECORD_NODES);
        let before = tree.leaf_count();

        // Delete three keys in four, scattered the way a grave prune leaves them
        let mut survivors: Vec<(Key, TreeVal)> = Vec::new();
        for (at, key) in held.iter().enumerate() {
            match at % 4 {
                0 => survivors.push((
                    *key,
                    TreeVal {
                        segment: 0,
                        offset: 0,
                        len: 0,
                        lsn: 1,
                        incarnation: 1,
                    },
                )),
                _ => {
                    tree.remove(key);
                }
            }
        }
        let stranded = tree.leaf_count();

        let mut holds = Vec::new();
        for _ in 0..5 {
            let start = Instant::now();
            let rebuilt: TBTreeMap<[u8; 34], RECORD_NODES, TreeVal> =
                TBTreeMap::from_sorted(survivors.clone(), RECORD_NODES);
            holds.push(start.elapsed());
            assert_eq!(rebuilt.len(), survivors.len(), "the rebuild lost keys");
        }
        holds.sort();
        let hold = holds[2];

        let rebuilt: TBTreeMap<[u8; 34], RECORD_NODES, TreeVal> =
            TBTreeMap::from_sorted(survivors.clone(), RECORD_NODES);
        println!(
            "{held_keys:>8}  {:>7}  {:>9.1}us  {:>8.1}ns  {stranded:>4} -> {:<4}  {:>8.0}%",
            survivors.len(),
            hold.as_nanos() as f64 / 1000.0,
            hold.as_nanos() as f64 / survivors.len() as f64,
            rebuilt.leaf_count(),
            100.0 * (stranded - rebuilt.leaf_count()) as f64 / stranded as f64,
        );
        let _ = before;
    }
}

// what batched reads save at the index, against one lookup at a time
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn batched_index_reads() {
    const RECORDS: ColumnSpec = ColumnSpec {
        id: ColumnId(1),
        name: "records",
        key_width: KeyWidth::Fixed(34),
        shard_bytes: 2,
        purge_mark: None,
        codec: Codec::None,
    };

    // Keys are spread over this many groups, one shard each
    const GROUPS: u16 = 50;

    println!();
    println!(
        "{:>9}  {:>6}  {:>7}  {:>10}  {:>10}  {:>6}",
        "per group", "batch", "spread", "one at a time", "batched", "gain"
    );

    for &per_group in &[65_536usize, 1_048_576] {
        let index: WidthIndex<[u8; 34], Trees<34>> = WidthIndex::new(&RECORDS);
        let segments = SegmentTable::new();
        let mut rng = SmallRng::seed_from_u64(0x51ce);
        let mut held: Vec<[u8; 34]> = Vec::with_capacity(per_group * GROUPS as usize);
        for group in 0..GROUPS {
            for _ in 0..per_group {
                let mut key = [0u8; 34];
                key[..2].copy_from_slice(&group.to_be_bytes());
                rng.fill(&mut key[2..]);
                held.push(key);
            }
        }
        for (at, key) in held.iter().enumerate() {
            index.insert(
                key,
                Entry::new(Loc::new(SegmentId(1), at as u32, 4096), Lsn(at as u64 + 1)),
                &segments,
            );
        }

        for &batch in &[8usize, 64, 256] {
            // One group is a targeted read, every group is a read across blobs
            for (spread, name) in [(1usize, "one group"), (GROUPS as usize, "all")] {
                // Each arm draws fresh rounds and the arms alternate, so neither starts warm
                let rounds = (65_536 / batch).max(16);
                let draw = |rng: &mut SmallRng| -> Vec<Vec<Vec<u8>>> {
                    (0..rounds)
                        .map(|_| {
                            (0..batch)
                                .map(|at| {
                                    let group = (at % spread) as u16;
                                    let pick = rng.gen_range(0..per_group);
                                    held[group as usize * per_group + pick].to_vec()
                                })
                                .collect()
                        })
                        .collect()
                };
                let mut out: Vec<Option<Entry>> = Vec::new();
                let mut looped_runs = Vec::new();
                let mut batched_runs = Vec::new();
                for _ in 0..3 {
                    let drawn = draw(&mut rng);
                    let asked: Vec<Vec<&[u8]>> = drawn
                        .iter()
                        .map(|round| round.iter().map(|key| key.as_slice()).collect())
                        .collect();
                    let start = Instant::now();
                    for round in &asked {
                        for key in round {
                            std::hint::black_box(index.entry_or_grave(key));
                        }
                    }
                    looped_runs.push(per_op(start.elapsed(), rounds * batch));

                    let drawn = draw(&mut rng);
                    let asked: Vec<Vec<RecordKey>> = drawn
                        .iter()
                        .map(|round| {
                            round
                                .iter()
                                .map(|key| RecordKey::from_bytes(ColumnId(1), key).expect("a key"))
                                .collect()
                        })
                        .collect();
                    let run: Vec<usize> = (0..batch).collect();
                    let start = Instant::now();
                    for round in &asked {
                        index.entry_many(round, &run, &mut out);
                        std::hint::black_box(&out);
                    }
                    batched_runs.push(per_op(start.elapsed(), rounds * batch));
                }
                looped_runs.sort_by(|a, b| a.partial_cmp(b).expect("a time"));
                batched_runs.sort_by(|a, b| a.partial_cmp(b).expect("a time"));
                let looped = looped_runs[1];
                let batched = batched_runs[1];

                println!(
                    "{per_group:>9}  {batch:>6}  {name:>7}  {looped:>11.1}ns  {batched:>8.1}ns  {:>5.2}x",
                    looped / batched
                );
            }
        }
    }
}

/// Object keys in the shapes buckets hold: a 32 byte bucket address, then a name
mod names {
    /// Bytes of bucket address in front of every object key
    const BUCKET: usize = 32;

    /// The longest object name the gateway accepts
    const MAX_NAME: usize = 1024;

    pub struct Corpus {
        pub name: &'static str,
        pub keys: Vec<Box<[u8]>>,
    }

    fn hex(value: u64, digits: usize) -> String {
        format!("{value:0digits$x}", digits = digits)
    }

    /// One bucket's address, spread the way a real one is
    fn bucket(at: u64) -> [u8; BUCKET] {
        let mut out = [0u8; BUCKET];
        let mut state = at.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        for slot in out.iter_mut() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *slot = state as u8;
        }
        out
    }

    /// Prefixes each name with a bucket address, over `buckets` buckets
    fn keyed(buckets: u64, names: impl Iterator<Item = Vec<u8>>) -> Vec<Box<[u8]>> {
        let mut keys: Vec<Box<[u8]>> = names
            .enumerate()
            .map(|(at, name)| {
                let mut key = bucket(at as u64 % buckets).to_vec();
                key.extend_from_slice(&name);
                key.into_boxed_slice()
            })
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }

    fn opaque(count: u64) -> impl Iterator<Item = Vec<u8>> {
        (0..count).map(|at| hex(at.wrapping_mul(0x9E37_79B9_7F4A_7C15), 16).into_bytes())
    }

    fn dated(count: u64) -> impl Iterator<Item = Vec<u8>> {
        (0..count).map(|at| {
            let day = at % 28 + 1;
            let month = at / 28 % 12 + 1;
            format!("logs/2026/{month:02}/{day:02}/{}.json", hex(at, 8)).into_bytes()
        })
    }

    fn tenanted(count: u64) -> impl Iterator<Item = Vec<u8>> {
        (0..count).map(|at| {
            let tenant = hex(at / 64, 32);
            format!(
                "tenants/{tenant}/exports/2026/08/02/part-{:05}.parquet",
                at % 64
            )
            .into_bytes()
        })
    }

    fn full_length(count: u64) -> impl Iterator<Item = Vec<u8>> {
        (0..count).map(|at| {
            let mut name = vec![b'F'; MAX_NAME - 6];
            name.extend_from_slice(hex(at, 6).as_bytes());
            name
        })
    }

    /// Every name shape in one bucket, plus dated keys over 8 and 256 buckets
    pub fn corpora(count: u64) -> Vec<Corpus> {
        vec![
            Corpus {
                name: "opaque",
                keys: keyed(1, opaque(count)),
            },
            Corpus {
                name: "dated",
                keys: keyed(1, dated(count)),
            },
            Corpus {
                name: "tenanted",
                keys: keyed(1, tenanted(count)),
            },
            Corpus {
                name: "full length",
                keys: keyed(1, full_length(count)),
            },
            Corpus {
                name: "dated, 8 buckets",
                keys: keyed(8, dated(count)),
            },
            Corpus {
                name: "dated, 256 buckets",
                keys: keyed(256, dated(count)),
            },
        ]
    }

    /// Dated keys over eight buckets, the shape a shard holds
    pub fn mixed(count: u64) -> Vec<Box<[u8]>> {
        keyed(8, dated(count))
    }
}

/// A name key with the window off, as the control for the window
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct Flat(Box<[u8]>);

impl std::borrow::Borrow<[u8]> for Flat {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl TreeKey for Flat {
    type Probe = [u8];
    type Window = Whole;

    fn filler() -> Flat {
        Flat(Box::from([].as_slice()))
    }

    fn head(probe: &[u8]) -> u64 {
        let mut wide = [0u8; 8];
        let take = probe.len().min(8);
        wide[..take].copy_from_slice(&probe[..take]);
        u64::from_be_bytes(wide)
    }

    fn separator(left: &Flat, right: &Flat) -> (Flat, bool) {
        let differs = left
            .0
            .iter()
            .zip(right.0.iter())
            .position(|(low, high)| low != high);
        let take = differs.map_or(left.0.len(), |at| at) + 1;
        (Flat(Box::from(&right.0[..take.min(right.0.len())])), true)
    }
}

/// What one arm of the variable sweep cost
struct VarRow {
    insert: f64,
    get: f64,
    walk: f64,
    bytes: f64,
    tie: f64,
}

/// Nanoseconds a key for insert, get and the ordered walk, plus what it weighs
fn time_var<const B: usize>(keys: &[Box<[u8]>], probes: &[usize]) -> VarRow {
    let start = Instant::now();
    let (map, bytes) = weighed(|| {
        let mut map: TBTreeMap<Box<[u8]>, B, TreeVal> = TBTreeMap::new();
        for (at, key) in keys.iter().enumerate() {
            map.insert(
                key.clone(),
                TreeVal {
                    segment: at as u32,
                    offset: at as u32,
                    len: 1024,
                    lsn: at as u64,
                    incarnation: 1,
                },
            );
        }
        map
    });
    let insert = per_op(start.elapsed(), keys.len());

    let start = Instant::now();
    let mut hits = 0usize;
    for at in probes {
        if map.get(&keys[*at]).is_some() {
            hits += 1;
        }
    }
    let get = per_op(start.elapsed(), probes.len());
    assert_eq!(hits, probes.len(), "a probe missed a key the map holds");

    let start = Instant::now();
    let mut walked = 0usize;
    for _ in map.iter() {
        walked += 1;
    }
    let walk = per_op(start.elapsed(), walked.max(1));

    // The weighed closure clones every key, so the total includes the key bytes
    VarRow {
        insert,
        get,
        walk,
        bytes: bytes as f64 / keys.len() as f64,
        tie: map.tie_rate(),
    }
}

/// The same, with the window off, so the two rows differ only in the lead
fn time_flat<const B: usize>(keys: &[Box<[u8]>], probes: &[usize]) -> VarRow {
    let held: Vec<Flat> = keys.iter().map(|key| Flat(key.clone())).collect();
    let start = Instant::now();
    let (map, bytes) = weighed(|| {
        let mut map: TBTreeMap<Flat, B, TreeVal> = TBTreeMap::new();
        for (at, key) in held.iter().enumerate() {
            map.insert(
                key.clone(),
                TreeVal {
                    segment: at as u32,
                    offset: at as u32,
                    len: 1024,
                    lsn: at as u64,
                    incarnation: 1,
                },
            );
        }
        map
    });
    let insert = per_op(start.elapsed(), held.len());

    let start = Instant::now();
    let mut hits = 0usize;
    for at in probes {
        if map.get(&keys[*at]).is_some() {
            hits += 1;
        }
    }
    let get = per_op(start.elapsed(), probes.len());
    assert_eq!(hits, probes.len(), "a probe missed a key the map holds");

    let start = Instant::now();
    let mut walked = 0usize;
    for _ in map.iter() {
        walked += 1;
    }
    let walk = per_op(start.elapsed(), walked.max(1));

    VarRow {
        insert,
        get,
        walk,
        bytes: bytes as f64 / keys.len() as f64,
        tie: map.tie_rate(),
    }
}

/// The map the tree replaces on the object column, at the same keys
fn time_btreemap(keys: &[Box<[u8]>], probes: &[usize]) -> VarRow {
    let start = Instant::now();
    let (map, bytes) = weighed(|| {
        let mut map: BTreeMap<Box<[u8]>, TreeVal> = BTreeMap::new();
        for (at, key) in keys.iter().enumerate() {
            map.insert(
                key.clone(),
                TreeVal {
                    segment: at as u32,
                    offset: at as u32,
                    len: 1024,
                    lsn: at as u64,
                    incarnation: 1,
                },
            );
        }
        map
    });
    let insert = per_op(start.elapsed(), keys.len());

    let start = Instant::now();
    let mut hits = 0usize;
    for at in probes {
        if map.contains_key(keys[*at].as_ref() as &[u8]) {
            hits += 1;
        }
    }
    let get = per_op(start.elapsed(), probes.len());
    assert_eq!(hits, probes.len(), "a probe missed a key the map holds");

    let start = Instant::now();
    let mut walked = 0usize;
    for _ in map.iter() {
        walked += 1;
    }
    let walk = per_op(start.elapsed(), walked.max(1));

    VarRow {
        insert,
        get,
        walk,
        bytes: bytes as f64 / keys.len() as f64,
        tie: f64::NAN,
    }
}

/// Probe positions drawn over what is held, so every arm asks the same keys
fn probe_slots(count: usize, held: usize, seed: u64) -> Vec<usize> {
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..count).map(|_| rng.gen_range(0..held)).collect()
}

// what a node width costs a column of name keys, against `BTreeMap` and the flat key
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn var_node_width() {
    const KEYS: usize = 16_384;

    println!();
    scan_note();
    println!(
        "{:>18}  {:>10}  {:>9}  {:>9}  {:>9}  {:>7}  {:>6}",
        "corpus", "shape", "insert", "get", "walk/key", "bytes", "tie"
    );

    for corpus in names::corpora(KEYS as u64) {
        let held = corpus.keys;
        let probes = probe_slots(PROBES, held.len(), 0x0b1);
        let mean: f64 = held.iter().map(|key| key.len()).sum::<usize>() as f64 / held.len() as f64;

        let mut rows: Vec<(String, VarRow)> = Vec::new();
        rows.push(("btreemap".to_string(), time_btreemap(&held, &probes)));
        for (width, run) in [
            (
                8usize,
                time_flat::<8> as fn(&[Box<[u8]>], &[usize]) -> VarRow,
            ),
            (16, time_flat::<16>),
            (32, time_flat::<32>),
            (64, time_flat::<64>),
        ] {
            rows.push((format!("flat B={width}"), run(&held, &probes)));
        }
        for (width, run) in [
            (
                8usize,
                time_var::<8> as fn(&[Box<[u8]>], &[usize]) -> VarRow,
            ),
            (16, time_var::<16>),
            (32, time_var::<32>),
            (64, time_var::<64>),
            (128, time_var::<128>),
        ] {
            rows.push((format!("window B={width}"), run(&held, &probes)));
        }

        for (name, row) in rows {
            println!(
                "{:>18}  {name:>10}  {:>7.1}ns  {:>7.1}ns  {:>7.1}ns  {:>7.0}  {:>6.4}",
                format!("{} ({:.0}B)", corpus.name, mean),
                row.insert,
                row.get,
                row.walk,
                row.bytes,
                row.tie,
            );
        }
        // Key bytes a slot: boxed is pointer plus key, front coded is length, offset, suffix
        let shared: f64 = held
            .windows(2)
            .map(|pair| {
                pair[0]
                    .iter()
                    .zip(pair[1].iter())
                    .take_while(|(a, b)| a == b)
                    .count() as f64
            })
            .sum::<f64>()
            / (held.len() - 1) as f64;
        let boxed = 16.0 + mean;
        let coded = 1.0 + 4.0 + (mean - shared);
        println!(
            "{:>18}  key bytes a slot: boxed {boxed:.0}, front coded {coded:.0}, shared {shared:.0} of {mean:.0}",
            corpus.name,
        );
        println!();
    }
}

// a bucket stops tying the lead once the node's window moves past it
#[test]
fn a_bucket_stops_tying_once_the_window_moves() {
    let mut taken: Vec<(&str, f64)> = Vec::new();
    for corpus in names::corpora(4_000) {
        let mut window: TBTreeMap<Box<[u8]>, 32, u64> = TBTreeMap::new();
        let mut flat: TBTreeMap<Flat, 32, u64> = TBTreeMap::new();
        for (at, key) in corpus.keys.iter().enumerate() {
            window.insert(key.clone(), at as u64);
            flat.insert(Flat(key.clone()), at as u64);
        }

        assert_eq!(
            window.len(),
            flat.len(),
            "{}: the two arms hold different counts",
            corpus.name
        );
        for key in corpus.keys.iter().step_by(13) {
            assert_eq!(
                window.get(key).copied(),
                flat.get(key).copied(),
                "{}: the arms answer differently",
                corpus.name,
            );
        }

        let by_window: Vec<&Box<[u8]>> = window.iter().map(|(key, _)| key).collect();
        let by_flat: Vec<&Box<[u8]>> = flat.iter().map(|(key, _)| &key.0).collect();
        assert_eq!(
            by_window, by_flat,
            "{}: the ordered walks differ",
            corpus.name
        );

        // Pairs straddling two buckets do not tie, so the bar sits under one
        assert!(
            flat.tie_rate() > 0.9,
            "{}: a bucket-led key should tie on its lead, and {:.4} says it does not",
            corpus.name,
            flat.tie_rate(),
        );
        // The window never ties more than the flat arm
        assert!(
            window.tie_rate() <= flat.tie_rate() + f64::EPSILON,
            "{}: a window should never tie more than no window, and {:.4} against {:.4} says it did",
            corpus.name,
            window.tie_rate(),
            flat.tie_rate(),
        );
        taken.push((corpus.name, flat.tie_rate() - window.tie_rate()));
    }

    let best = taken.iter().map(|(_, gain)| *gain).fold(0.0f64, f64::max);
    assert!(
        best > 0.9,
        "no corpus had its ties taken by the window, which is what it is for: {taken:?}",
    );
}

/// A name key at one inline window cap, so the cap can be swept like a width
macro_rules! capped_key {
    ($name:ident, $cap:literal) => {
        #[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
        struct $name(Box<[u8]>);

        impl std::borrow::Borrow<[u8]> for $name {
            fn borrow(&self) -> &[u8] {
                &self.0
            }
        }

        impl TreeKey for $name {
            type Probe = [u8];
            type Window = Shared<$cap>;

            fn filler() -> $name {
                $name(Box::from([].as_slice()))
            }

            fn head(probe: &[u8]) -> u64 {
                let mut wide = [0u8; 8];
                let take = probe.len().min(8);
                wide[..take].copy_from_slice(&probe[..take]);
                u64::from_be_bytes(wide)
            }

            fn separator(left: &$name, right: &$name) -> ($name, bool) {
                let differs = left
                    .0
                    .iter()
                    .zip(right.0.iter())
                    .position(|(low, high)| low != high);
                let take = differs.map_or(left.0.len(), |at| at) + 1;
                ($name(Box::from(&right.0[..take.min(right.0.len())])), true)
            }
        }
    };
}

capped_key!(Cap40, 40);
capped_key!(Cap64, 64);
capped_key!(Cap128, 128);
capped_key!(Cap256, 256);

/// Times one capped arm over a corpus
fn time_capped<K, const B: usize>(
    keys: &[Box<[u8]>],
    probes: &[usize],
    wrap: impl Fn(Box<[u8]>) -> K,
) -> VarRow
where
    K: TreeKey<Probe = [u8]>,
{
    let held: Vec<K> = keys.iter().map(|key| wrap(key.clone())).collect();
    let start = Instant::now();
    let (map, bytes) = weighed(|| {
        let mut map: TBTreeMap<K, B, TreeVal> = TBTreeMap::new();
        for (at, key) in held.iter().enumerate() {
            map.insert(
                key.clone(),
                TreeVal {
                    segment: at as u32,
                    offset: at as u32,
                    len: 1024,
                    lsn: at as u64,
                    incarnation: 1,
                },
            );
        }
        map
    });
    let insert = per_op(start.elapsed(), held.len());

    let start = Instant::now();
    let mut hits = 0usize;
    for at in probes {
        if map.get(&keys[*at]).is_some() {
            hits += 1;
        }
    }
    let get = per_op(start.elapsed(), probes.len());
    assert_eq!(hits, probes.len(), "a probe missed a key the map holds");

    let start = Instant::now();
    let mut walked = 0usize;
    for _ in map.iter() {
        walked += 1;
    }
    let walk = per_op(start.elapsed(), walked.max(1));

    VarRow {
        insert,
        get,
        walk,
        bytes: bytes as f64 / keys.len() as f64,
        tie: map.tie_rate(),
    }
}

// how many shared bytes a node should hold inline to move its lead past them
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn var_shared_cap() {
    const KEYS: usize = 16_384;

    println!();
    scan_note();
    println!(
        "{:>18}  {:>6}  {:>9}  {:>9}  {:>7}  {:>6}",
        "corpus", "cap", "insert", "get", "bytes", "tie"
    );

    for corpus in names::corpora(KEYS as u64) {
        let held = corpus.keys;
        let probes = probe_slots(PROBES, held.len(), 0x0b1);
        let shared: usize = held
            .windows(2)
            .map(|pair| {
                pair[0]
                    .iter()
                    .zip(pair[1].iter())
                    .take_while(|(a, b)| a == b)
                    .count()
            })
            .min()
            .unwrap_or(0);

        let rows = [
            ("40", time_capped::<Cap40, 32>(&held, &probes, Cap40)),
            ("64", time_capped::<Cap64, 32>(&held, &probes, Cap64)),
            ("128", time_capped::<Cap128, 32>(&held, &probes, Cap128)),
            ("256", time_capped::<Cap256, 32>(&held, &probes, Cap256)),
        ];
        for (cap, row) in rows {
            println!(
                "{:>18}  {cap:>6}  {:>7.1}ns  {:>7.1}ns  {:>7.0}  {:>6.4}",
                format!("{} (>={}B)", corpus.name, shared),
                row.insert,
                row.get,
                row.bytes,
                row.tie,
            );
        }
        println!();
    }
}

// the name columns use the swept width and cap, and the index holds no `BTreeMap`
#[test]
fn the_name_columns_are_pinned_and_nothing_is_a_btreemap() {
    assert_eq!(
        VAR_NODE_WIDTH, 32,
        "the name columns moved off the width the sweep took"
    );
    assert_eq!(
        SHARED_CAP, 128,
        "the shared window moved off the cap the sweep took"
    );
    let _: <VarTrees as Shape<Box<[u8]>>>::Entries =
        TBTreeMap::<Box<[u8]>, VAR_NODE_WIDTH, Entry>::new();

    let index = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/index");
    let mut named: Vec<String> = Vec::new();
    for file in std::fs::read_dir(&index).expect("the index directory") {
        let path = file.expect("a directory entry").path();
        if path.extension().is_none_or(|kind| kind != "rs") {
            continue;
        }
        let held = std::fs::read_to_string(&path).expect("a source file");
        for (at, line) in held.lines().enumerate() {
            let code = line.trim_start();
            // Skip comment lines, which may mention the type
            if code.starts_with("//") || code.starts_with("*") {
                continue;
            }
            // `TBTreeMap` ends in `BTreeMap`, so a hit with a letter in front is the crate's tree
            for form in ["BTreeMap<", "BTreeMap::"] {
                let mut from = 0usize;
                while let Some(hit) = code[from..].find(form) {
                    let start = from + hit;
                    let prefixed = start > 0 && code.as_bytes()[start - 1].is_ascii_alphanumeric();
                    if !prefixed {
                        named.push(format!("{}:{}", path.display(), at + 1));
                    }
                    from = start + form.len();
                }
            }
        }
    }
    assert!(
        named.is_empty(),
        "the resident index still holds a std BTreeMap at {named:?}",
    );
}
