//! What a fence removes from a blocked search, in reads and in answers
//!
//! A paged column whose footers do not fit its cache answers a key by binary
//! searching the segment's blocks, so every halving is a block read before the block
//! holding the key is read at all. A fence holds the first key of each block, so the
//! halvings happen over the leads instead. The three arms are the same volume written
//! and read three ways: no fence, every lead resident, and the sampled level resident
//! with the leads read a page at a time. Counts rather than microseconds, and the arms
//! have to agree on the answers first, since a fence that lands a search on the wrong
//! block reports a key that exists as missing.
//!
//! Run it with:
//!
//!   cargo test -p reel --test fence_walk --release -- --ignored --nocapture

use std::path::PathBuf;
use std::sync::Arc;

use reel::format::footer::{directory_span, partition_spans, DIRECTORY_ROW_LEN};
use reel::io::fault::FaultPlan;
use reel::io::sim_backend::{DurableImage, SimIo};
use reel::units::ByteCount;
use reel::{
    Codec, ColumnId, ColumnSet, ColumnSpec, FenceResidency, IndexResidency, KeyWidth, MapShape,
    Preallocate, ProbeCounts, RecordKey, ReelConfig, ReelStore, SyncPolicy, ThreadBudget,
    SEGMENT_SUFFIX,
};

const ROWS: ColumnId = ColumnId(1);

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ROWS,
    name: "rows",
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// Keys each segment takes, deep enough that a walk over its blocks costs halvings
const KEYS_PER_SEGMENT: u64 = 8_000;

/// Segments the volume writes; the last is the open tail, so three leaves two sealed
const SEGMENTS: u64 = 3;

/// Payload each key carries, small so the depth comes from the key count
const PAYLOAD: usize = 64;

/// Bytes a record of that size costs a segment, key, header and all
const RECORD_BYTES: u64 = 128;

/// Keys probed, a sample rather than the whole volume
const PROBES: u64 = 400;

/// Filter bits a key when an arm is measured with one, the volume's own default
const FILTER_BITS: u8 = 10;

/// Footer cache every arm is measured under, which the pools take a third of each
///
/// Room for the directories and the blocks a search touches and none for a footer of
/// this segment's key count, so a key is found by descending rather than by parsing
/// the whole index of its segment. A bound of nothing would take the blocks away too,
/// and then a search would pay for the same block twice.
const FOOTER_CACHE: ByteCount = ByteCount::from_bytes(64 * 1024);

/// A key nothing about its bytes tells you the order of, so every segment's range
/// covers it and what is left is the search inside one segment
fn key(at: u64) -> RecordKey {
    let mixed = at
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .rotate_left(29)
        .wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&mixed.to_be_bytes());
    bytes[8..].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(ROWS, &bytes).expect("key")
}

/// A paged volume with no room to hold a footer, which is the blocked search path
///
/// Filters off on purpose: a filter ruling a segment out would take the search away
/// from the thing being measured.
fn config(fence: FenceResidency) -> ReelConfig {
    filtered_config(fence, 0)
}

/// The same volume with a filter of the given size over every sealed partition
fn filtered_config(fence: FenceResidency, filter_bits: u8) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(KEYS_PER_SEGMENT * RECORD_BYTES),
        alloc_chunk: ByteCount::from_bytes(64 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        index: IndexResidency::Paged,
        footer_cache: FOOTER_CACHE,
        filter_bits,
        fence,
        ..ReelConfig::default()
    }
}

fn keys() -> u64 {
    KEYS_PER_SEGMENT * SEGMENTS
}

/// A volume of sealed segments with its keys handed to the footers
fn filled(fence: FenceResidency) -> (ReelStore, SimIo) {
    filled_under(config(fence))
}

fn filled_under(config: ReelConfig) -> (ReelStore, SimIo) {
    let sim = SimIo::new(FaultPlan::new(5));
    let store = ReelStore::open_with_io(
        PathBuf::from("/fence"),
        config,
        COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("open");

    let payload = vec![0x3Cu8; PAYLOAD];
    for at in 0..keys() {
        store.put(&key(at), &payload).expect("put");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");
    (store, sim)
}

/// Probe a sample of the keys, and say what finding them cost
fn hit_counts(store: &ReelStore) -> ProbeCounts {
    let stride = keys() / PROBES;
    let before = store.filter_probes();
    for at in 0..PROBES {
        let at = at * stride;
        assert!(
            store.get(&key(at)).expect("get").is_some(),
            "key {at} went missing",
        );
    }
    store.filter_probes().since(before)
}

/// Every device read a search paid, whichever half of the path it was spent on
///
/// The residencies move the cost between the row blocks and the directory reads that
/// found them, so counting only the blocks would price one arm and hide the other.
fn reads(counts: ProbeCounts) -> u64 {
    counts.block_reads + counts.map_reads
}

// a fenced search reads the block holding the key and no others
//
// The bounds are one search wide rather than one key wide because a key every sealed
// segment's range covers is searched in each of them, and each search lands on a
// block of its own.
#[test]
fn a_fence_lands_a_search_on_one_block() {
    let (walked, _) = filled(FenceResidency::Off);
    let (resident, _) = filled(FenceResidency::Resident);
    let (paged, _) = filled(FenceResidency::Paged);

    let bare = hit_counts(&walked);
    let held = hit_counts(&resident);
    let sampled = hit_counts(&paged);

    let searches = bare.searched();
    assert!(searches > 0, "no search reached a sealed segment");
    assert_eq!(
        held.searched(),
        searches,
        "the arms searched different volumes"
    );
    assert_eq!(
        sampled.searched(),
        searches,
        "the arms searched different volumes"
    );

    println!(
        "off {} blocks {} map, resident {} blocks {} map, paged {} blocks {} map, {searches} searches",
        bare.block_reads, bare.map_reads,
        held.block_reads, held.map_reads,
        sampled.block_reads, sampled.map_reads,
    );

    assert!(
        held.block_reads > 0,
        "the fenced arm read nothing, so it proved nothing"
    );
    assert!(
        held.block_reads <= searches,
        "{} reads over {searches} searches with every lead resident, past one block a search",
        held.block_reads,
    );
    assert!(
        sampled.block_reads <= 2 * searches,
        "{} reads over {searches} searches with the leads on the volume, past a page and a block",
        sampled.block_reads,
    );
    assert!(
        bare.block_reads > 3 * held.block_reads,
        "{} reads a fenced search saves against {} unfenced: the walk is not being removed",
        held.block_reads,
        bare.block_reads,
    );
    assert!(
        reads(held) < reads(bare),
        "{} reads a fenced search costs against {} unfenced, once the directory is counted",
        reads(held),
        reads(bare),
    );
}

// what an in-range miss costs a fenced sorted run, with a filter and without
//
// A key no sealed segment holds is ruled out ahead of the fan-out by the column's
// sealed-key set. The miss that survives is the fan-out's own, and a fence cannot
// stand in for a filter there: the leads say where a key would sit, never whether
// it is there.
#[test]
fn what_a_miss_costs_without_a_filter() {
    let (bare, _) = filled_under(filtered_config(FenceResidency::Resident, 0));
    let (filtered, _) = filled_under(filtered_config(FenceResidency::Resident, FILTER_BITS));

    let unfiltered = hit_counts(&bare);
    let ruled = hit_counts(&filtered);
    let fresh = miss_counts(&bare);
    let fresh_filtered = miss_counts(&filtered);

    println!(
        "fan-out: no filter asked {} searched {} blocks {} map {}; filter asked {} searched {} blocks {} map {}",
        unfiltered.asked, unfiltered.searched(), unfiltered.block_reads, unfiltered.map_reads,
        ruled.asked, ruled.searched(), ruled.block_reads, ruled.map_reads,
    );
    println!(
        "unsealed key: no filter asked {} blocks {}; filter asked {} blocks {}",
        fresh.asked, fresh.block_reads, fresh_filtered.asked, fresh_filtered.block_reads,
    );

    assert_eq!(
        unfiltered.skipped, 0,
        "a volume with no filter ruled a segment out"
    );
    assert!(
        ruled.skipped > 0,
        "the filtered arm ruled nothing out, so it measured nothing"
    );
    // No footer fits the cache, so every ask that reaches a segment costs a block read
    // whether the key is there or not, and the asks the filter rules out are reads it
    // never spends.
    assert!(
        ruled.block_reads < unfiltered.block_reads,
        "{} block reads with a filter against {} without: an in-range miss is costing nothing to rule out",
        ruled.block_reads,
        unfiltered.block_reads,
    );
    assert!(
        ruled.searched() < unfiltered.searched(),
        "{} asks searched with a filter against {} without: the filter is ruling nothing out",
        ruled.searched(),
        unfiltered.searched(),
    );
    assert_eq!(fresh.asked, 0, "a key no segment sealed reached one anyway");
    assert_eq!(
        fresh_filtered.asked, 0,
        "a key no segment sealed reached one anyway"
    );
}

/// Probe keys nothing wrote, which the column's sealed-key set answers on its own
fn miss_counts(store: &ReelStore) -> ProbeCounts {
    let before = store.filter_probes();
    for at in keys()..keys() + PROBES {
        assert!(
            store.get(&key(at)).expect("get").is_none(),
            "key {at} is not there"
        );
    }
    store.filter_probes().since(before)
}

/// Standing run counts the sweep stands up, one sealed segment a run
const STANDING_RUNS: &[u64] = &[1, 4, 16, 64];

/// Bytes a run of this sweep is given, a quarter of what a fence cell's segment takes
///
/// Still about twenty five hundred scattered keys a run, so every run's span covers
/// all but the outermost thousandth of the keyspace and no run is ruled out by range.
const RUN_BYTES: u64 = KEYS_PER_SEGMENT * RECORD_BYTES / 4;

/// Keys written between two readings of how many runs stand, small enough that a batch
/// straddling a seal can be dropped rather than mislabelled
const FILL_BATCH: u64 = 250;

/// Keys rewritten once a run, which puts one population in every run at once
///
/// Their newest copy is in the newest run and a dead copy stands in all the others, so
/// they are the population a per-run filter can rule nothing out of.
const REWRITTEN_PER_RUN: u64 = 200;

/// Where the keys only one run holds start, past the slice every run rewrites
const COLD_AT: u64 = REWRITTEN_PER_RUN;

/// Keys probed out of each population
const RUN_PROBES: u64 = 200;

/// A paged volume standing on the named number of sealed runs, and where its keys went
///
/// The run a key landed in is read off the volume rather than counted out in records:
/// the fill writes a batch, asks how many runs stand before it and after it, and keeps
/// the batch only where the two agree. The rewrite goes in at the head of a run, which
/// keeps the newest copy of every rewritten key in a sealed run rather than in the open
/// tail, where the map would answer for it and no footer would be asked.
fn standing_runs(runs: u64, filter_bits: u8) -> (ReelStore, Vec<Vec<u64>>) {
    let config = ReelConfig {
        segment_bytes: ByteCount::from_bytes(RUN_BYTES),
        alloc_chunk: ByteCount::from_bytes(64 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        index: IndexResidency::Paged,
        footer_cache: FOOTER_CACHE,
        filter_bits,
        fence: FenceResidency::Resident,
        ..ReelConfig::default()
    };
    let store = ReelStore::open_with_io(
        PathBuf::from("/runs"),
        config,
        COLUMNS,
        Arc::new(SimIo::new(FaultPlan::new(5))),
    )
    .expect("open");

    let payload = vec![0x3Cu8; PAYLOAD];
    let mut landed: Vec<Vec<u64>> = vec![Vec::new(); runs as usize];
    let mut written = 0u64;
    let mut rewritten_in = u64::MAX;

    loop {
        let run = store.index().sealed_spans(ROWS) as u64;
        if run >= runs {
            break;
        }
        if run != rewritten_in {
            for at in 0..REWRITTEN_PER_RUN {
                store.put(&key(at), &payload).expect("put");
            }
            rewritten_in = run;
        }

        let first = COLD_AT + written;
        for _ in 0..FILL_BATCH {
            store.put(&key(COLD_AT + written), &payload).expect("put");
            written += 1;
        }
        store.flush().expect("flush");
        store.page_out_sealed().expect("page out");

        if store.index().sealed_spans(ROWS) as u64 == run {
            landed[run as usize].extend(first..COLD_AT + written);
        }
    }

    for (run, keys) in landed.iter().enumerate() {
        assert!(
            keys.len() as u64 >= RUN_PROBES,
            "run {run} took {} keys of its own, under the {RUN_PROBES} a population needs",
            keys.len(),
        );
    }
    (store, landed)
}

/// A sample of one run's own keys, rather than the whole run
fn run_sample(landed: &[u64]) -> impl Iterator<Item = u64> + '_ {
    let stride = (landed.len() as u64 / RUN_PROBES).max(1);
    (0..RUN_PROBES).map(move |probe| landed[(probe * stride) as usize])
}

/// Probe a population of keys that all exist, and say what each get cost
fn population_counts(store: &ReelStore, ids: impl Iterator<Item = u64>) -> (ProbeCounts, u64) {
    let before = store.filter_probes();
    let mut gets = 0u64;
    for at in ids {
        assert!(
            store.get(&key(at)).expect("get").is_some(),
            "key {at} went missing"
        );
        gets += 1;
    }
    (store.filter_probes().since(before), gets)
}

// what a standing run costs a read that was not looking for it
//
// The worst case: one uniform keyspace, scattered keys, so every run's span covers
// every key and no run is ruled out by its range.
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn what_standing_runs_cost() {
    println!();
    println!(
        "{:>5}  {:>6}  {:>9}  {:>8}  {:>10}  {:>8}",
        "runs", "filter", "keys in", "asks/get", "blocks/get", "dirs/get",
    );

    let mut bare_blocks: Vec<f64> = Vec::new();
    let mut filtered_blocks: Vec<f64> = Vec::new();
    let mut bare_asks: Vec<f64> = Vec::new();
    let mut newest_asks: Vec<f64> = Vec::new();
    let mut middle_asks: Vec<f64> = Vec::new();

    for &runs in STANDING_RUNS {
        for filter_bits in [0, FILTER_BITS] {
            let (store, landed) = standing_runs(runs, filter_bits);
            let arm = match filter_bits {
                0 => "off",
                _ => "on",
            };

            for (population, run) in [("newest", runs - 1), ("middle", runs / 2), ("oldest", 0)] {
                let (counts, gets) = population_counts(&store, run_sample(&landed[run as usize]));
                let asks = counts.asked as f64 / gets as f64;
                let blocks = counts.block_reads as f64 / gets as f64;
                let directories = counts.map_reads as f64 / gets as f64;
                println!(
                    "{runs:>5}  {arm:>6}  {population:>9}  {asks:>8.2}  {blocks:>10.2}  {directories:>8.2}",
                );
                // Kept apart from the readings below: the guard on the early exit has
                // to be able to fail while the depth guards still pass.
                if filter_bits == 0 && population == "newest" {
                    newest_asks.push(asks);
                }
                if filter_bits == 0 && population == "middle" {
                    middle_asks.push(asks);
                }
                if population != "oldest" {
                    continue;
                }
                match filter_bits {
                    0 => {
                        bare_blocks.push(blocks);
                        bare_asks.push(asks);
                    }
                    _ => filtered_blocks.push(blocks),
                }
            }

            // The keys every run rewrote, a control on what the filter arm can rule out.
            let (counts, gets) = population_counts(&store, 0..REWRITTEN_PER_RUN);
            println!(
                "{runs:>5}  {arm:>6}  {:>9}  {:>8.2}  {:>10.2}  {:>8.2}",
                "every run",
                counts.asked as f64 / gets as f64,
                counts.block_reads as f64 / gets as f64,
                counts.map_reads as f64 / gets as f64,
            );
        }
        println!();
    }

    // The guard that makes the rest of it mean anything: a get answered off the map
    // never reached a sealed run, and a flat curve then reads as a cheap engine rather
    // than a broken harness.
    let widest = *STANDING_RUNS.last().expect("a run count") as f64;
    let asks = *bare_asks.last().expect("an unfiltered reading");
    assert!(
        asks > widest / 2.0,
        "{asks:.2} asks a get against {widest:.0} standing runs: the fan-out is not being reached",
    );

    // Growth rather than a ratio: one run is answered from the footer the handover left
    // parsed, which is nothing to take a ratio against.
    let bare_growth = bare_blocks.last().expect("an unfiltered reading") - bare_blocks[0];
    assert!(
        bare_growth > widest / 2.0,
        "{bare_growth:.2} more block reads a get at {widest:.0} runs than at one: the fan-out costs nothing",
    );

    let filtered_growth = filtered_blocks.last().expect("a filtered reading") - filtered_blocks[0];
    assert!(
        filtered_growth < bare_growth / 8.0,
        "{filtered_growth:.2} more block reads a get with a filter against {bare_growth:.2} without: the filter is not ruling the runs out",
    );

    // What the ordering bought: take the early exit out and every population reads the
    // run count.
    let newest = *newest_asks.last().expect("an unfiltered reading");
    assert!(
        newest < widest / 8.0,
        "{newest:.2} asks a get for a key in the newest of {widest:.0} runs: the walk is not stopping",
    );
    let middle = *middle_asks.last().expect("an unfiltered reading");
    assert!(
        middle < widest * 3.0 / 4.0,
        "{middle:.2} asks a get for a key halfway down {widest:.0} runs: the walk is reaching past it",
    );
}

// every arm answers with the same bytes, whatever it read to find them
//
// The whole volume rather than a sample: a lead is a truncation, so the key that
// exposes a search landing one block past its row is whichever sits at a boundary.
#[test]
fn a_fence_changes_no_answer() {
    let (walked, _) = filled(FenceResidency::Off);
    let (resident, _) = filled(FenceResidency::Resident);
    let (paged, _) = filled(FenceResidency::Paged);

    for at in 0..keys() {
        let key = key(at);
        let bare = walked.get(&key).expect("get");
        assert!(bare.is_some(), "key {at} went missing without a fence");
        assert_eq!(resident.get(&key).expect("get"), bare, "key {at} resident");
        assert_eq!(paged.get(&key).expect("get"), bare, "key {at} paged");
    }
}

// a key nothing wrote is answered the same way with a fence as without
#[test]
fn a_fence_finds_nothing_that_is_not_there() {
    let (walked, _) = filled(FenceResidency::Off);
    let (resident, _) = filled(FenceResidency::Resident);
    let (paged, _) = filled(FenceResidency::Paged);

    for at in keys()..keys() + PROBES {
        let key = key(at);
        assert!(
            walked.get(&key).expect("get").is_none(),
            "key {at} is not there"
        );
        assert!(
            resident.get(&key).expect("get").is_none(),
            "key {at} resident"
        );
        assert!(paged.get(&key).expect("get").is_none(), "key {at} paged");
    }
}

// a fence a crash left half written is a torn footer, and a torn footer is refused
//
// The leads live inside the footer's own checksum, so a flipped bit fails the whole
// footer and the segment is rebuilt by walking its records.
#[test]
fn a_torn_fence_costs_the_footer() {
    let (store, sim) = filled(FenceResidency::Resident);
    drop(store);

    let mut image = sim.durable_image();
    assert!(flip_a_lead(&mut image), "no sealed segment carried a fence");

    let store = ReelStore::open_with_io(
        PathBuf::from("/fence"),
        config(FenceResidency::Resident),
        COLUMNS,
        Arc::new(SimIo::from_image(image)),
    )
    .expect("reopen");

    for at in 0..keys() {
        assert!(
            store.get(&key(at)).expect("get").is_some(),
            "key {at} went missing behind a torn fence",
        );
    }
}

/// Flip a byte inside the fence of the largest sealed segment in an image
fn flip_a_lead(image: &mut DurableImage) -> bool {
    let mut chosen: Option<usize> = None;
    let mut largest = 0usize;
    for (index, (path, bytes)) in image.iter().enumerate() {
        let is_segment = path
            .file_name()
            .map(|name| name.to_string_lossy().ends_with(SEGMENT_SUFFIX))
            .unwrap_or(false);
        if is_segment && bytes.len() > largest {
            largest = bytes.len();
            chosen = Some(index);
        }
    }
    let Some(index) = chosen else {
        return false;
    };

    let bytes = &mut image[index].1;
    let len = bytes.len() as u64;
    let Ok(Some(span)) = directory_span(bytes, len) else {
        return false;
    };

    // Every offset in a footer is measured from where the footer begins, so reaching
    // the fence means reading the directory the way the engine does.
    let footer_at = bytes.len() - span.footer_len;
    let directory = &bytes[footer_at + span.directory_at..][..span.partitions * DIRECTORY_ROW_LEN];
    let Ok(spans) = partition_spans(directory, span.partitions, 0) else {
        return false;
    };
    let rows_len: usize = spans.iter().map(|span| span.encoded as usize).sum();
    let fence_len = span.directory_at - span.filter_len - rows_len;
    if fence_len == 0 {
        return false;
    }

    bytes[footer_at + rows_len] ^= 0xff;
    true
}
