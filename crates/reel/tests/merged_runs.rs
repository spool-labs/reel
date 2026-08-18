//! What a merge of a volume's sorted runs does, and what it must never do
//!
//! Sealing by rewriting leaves one sorted run a segment, and a paged get asks every
//! standing run for every key population, since a segment number is not a version. These
//! are the tests of the pass that collapses them.
//!
//! The gate is resurrection: a merge reads several versions of a key and writes one,
//! which is the one operation in the engine that could bring a deleted key back, so every
//! tombstone is carried forward whatever it shadows.

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use reel::config::{CompactRate, HotIndex, IndexResidency, ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec, MapShape, RecordKey};
use reel::format::loc::SegmentId;
use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::io::ReelIo;
use reel::sync::rendezvous;
use reel::units::ByteCount;
use reel::{CompactPass, FenceResidency, KeyWidth, Preallocate, ReelStore};

const ROWS: ColumnId = ColumnId(1);

/// Keys the volume holds, spread so every run's range covers every key
const KEYS: u64 = 900;

/// Payload short enough for the carrying column to hold in its row
const NARROW: usize = 200;

/// Payload too wide for that row, so a carrying volume still writes records
///
/// A volume whose every value rode in a row would never roll its output segment, since a
/// listed row reserves no space, and the whole file would be one run with nothing to merge.
const WIDE: usize = 900;

/// Rounds of rewrites, each one leaving a run behind
const ROUNDS: u64 = 4;

/// Passes a caller drives compaction for before it gives up on it settling
const COMPACT_ROUNDS: u32 = 128;

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ROWS,
    name: "rows",
    key_width: KeyWidth::Fixed(32),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// The same column with its sealed rows holding the value itself where it fits
const CARRIED: ColumnSet = &[ColumnSpec {
    id: ROWS,
    name: "rows",
    key_width: KeyWidth::Fixed(32),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: NARROW as u16,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// A paged volume that seals by rewriting and will take a merge when asked
///
/// The dead ratio is at one, so only a wholly dead segment is reclaimed. Anything lower
/// and compaction rewrites the half-live runs itself, dropping the shadowed versions
/// before a merge could see two of them.
fn merging_config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(128 * 1024),
        alloc_chunk: ByteCount::from_bytes(32 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        index: IndexResidency::Paged,
        rewrite_on_seal: true,
        merge_sorted_runs: true,
        compact_dead_ratio: 1.0,
        // The gate off, since a gated pass answers Held and a caller driving compaction
        // to exhaustion cannot tell that from work remaining.
        compact_mbps: CompactRate::Mbps(100_000),
        ..ReelConfig::default()
    }
}

/// How far either side of the measured debt the boundary's thresholds are set
const MARGIN: f64 = 0.05;

/// Ticks an unarmed volume is driven for, well past what one merge would take
const TICKS: usize = 8;

/// The merging volume with the tick's trigger set where a test wants it
///
/// Unpaced rather than merely fast, since a tick charges the gate for the rewrite it runs
/// ahead of the merge and a shut gate would hold the merge off for reasons of its own.
fn triggered_config(merge_dead_ratio: f64) -> ReelConfig {
    ReelConfig {
        merge_dead_ratio,
        compact_mbps: CompactRate::Auto,
        ..merging_config()
    }
}

/// A scattered key, so no run's range rules another one out
fn key_at(at: u64) -> RecordKey {
    let mixed = at
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .rotate_left(29)
        .wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&mixed.to_be_bytes());
    bytes[8..16].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(ROWS, &bytes).expect("key")
}

/// Whether a round rewrites this key
///
/// Overlapping subsets rather than whole rounds: a run every one of whose keys was
/// rewritten goes wholly dead and is unlinked before a merge could reach it.
fn wrote_in(round: u64, at: u64) -> bool {
    match round {
        0 => true,
        1 => at.is_multiple_of(2),
        2 => at.is_multiple_of(3),
        _ => at.is_multiple_of(5),
    }
}

/// The last round that rewrote this key, which is what a read has to answer with
fn newest_round(at: u64) -> u64 {
    (0..ROUNDS)
        .rfind(|round| wrote_in(*round, at))
        .expect("round zero writes every key")
}

fn payload_of(round: u64, at: u64) -> Vec<u8> {
    let len = match at.is_multiple_of(2) {
        true => NARROW,
        false => WIDE,
    };
    vec![round as u8 + 1; len]
}

fn open_over(config: ReelConfig, columns: ColumnSet) -> (ReelStore, Arc<SimIo>) {
    let io = Arc::new(SimIo::new(FaultPlan::new(5)));
    let store = ReelStore::open_with_io(
        PathBuf::from("/merged"),
        config,
        columns,
        Arc::clone(&io) as Arc<dyn ReelIo>,
    )
    .expect("open");
    (store, io)
}

/// Drive compaction until it settles, which is what turns segments into sorted runs
fn settle_compaction(store: &ReelStore) {
    for _ in 0..COMPACT_ROUNDS {
        let pass = store.compact_once().expect("compact");
        store.flush().expect("flush");
        store.page_out_sealed().expect("page out");
        if matches!(pass, CompactPass::Idle) {
            return;
        }
    }
}

/// Seal every tail, so what a round wrote is under a footer rather than in the open
///
/// The cue is taken and dropped rather than held, since its floor would hold a merge off
/// every run it can see.
fn seal_tails(store: &ReelStore) {
    drop(store.cue().expect("cue"));
    store.page_out_sealed().expect("page out");
}

/// Write the rounds, settling the rewrites between them, so each leaves a run
fn fill_runs(store: &ReelStore) {
    for round in 0..ROUNDS {
        for at in 0..KEYS {
            if wrote_in(round, at) {
                store.put(&key_at(at), &payload_of(round, at)).expect("put");
            }
        }
        store.flush().expect("flush");
        seal_tails(store);
        settle_compaction(store);
    }
}

/// Sealed segments the volume is holding right now
fn standing_runs(store: &ReelStore) -> usize {
    store.index().segments_snapshot().len()
}

/// The segments that have a footer, which leaves out whatever a tail is still on
fn sealed_segments(store: &ReelStore) -> Vec<SegmentId> {
    let mut sealed = Vec::new();
    for (segment, _) in store.index().segments_snapshot() {
        if store.segment_footer(segment).expect("footer").is_some() {
            sealed.push(segment);
        }
    }
    sealed
}

/// Every live key answers with its newest version, which is what a merge may not change
fn assert_every_key_reads(store: &ReelStore, deleted: &[u64]) {
    for at in 0..KEYS {
        let found = store.get(&key_at(at)).expect("get");
        match deleted.contains(&at) {
            true => assert!(found.is_none(), "key {at} came back from the dead"),
            false => assert_eq!(
                found.expect("a live key").as_ref(),
                payload_of(newest_round(at), at).as_slice(),
                "key {at} answers with something other than its newest version",
            ),
        }
    }
}

// a merge collapses the standing runs and every key still answers with its newest
#[test]
fn collapses_the_runs() {
    let (store, _io) = open_over(merging_config(), COLUMNS);
    fill_runs(&store);
    let before = standing_runs(&store);
    assert!(
        before >= 3,
        "the fill left {before} runs, which is not a merge worth making"
    );

    let report = store.merge_once().expect("merge");

    assert!(
        report.runs_merged >= 2,
        "the pass read {} runs",
        report.runs_merged
    );
    assert_eq!(report.sources_retired, report.runs_merged);
    assert_eq!(report.sources_kept_by_rot, 0);
    assert!(
        report.rows_shadowed > 0,
        "overlapping rounds leave older versions for the merge to drop",
    );
    assert!(standing_runs(&store) < before, "the runs did not collapse");
    assert_every_key_reads(&store, &[]);
}

// a merged volume asks fewer segments per get than the runs it replaced
#[test]
fn fewer_asks_a_get() {
    let (store, _io) = open_over(merging_config(), COLUMNS);
    fill_runs(&store);

    let before = store.filter_probes();
    for at in 0..KEYS {
        store.get(&key_at(at)).expect("get").expect("present");
    }
    let asked_before = store.filter_probes().since(before).asked;

    store.merge_once().expect("merge");
    store.page_out_sealed().expect("page out");

    let after = store.filter_probes();
    for at in 0..KEYS {
        store.get(&key_at(at)).expect("get").expect("present");
    }
    let asked_after = store.filter_probes().since(after).asked;

    assert!(
        asked_after < asked_before,
        "a get asked {asked_after} segments after the merge against {asked_before} before it",
    );
}

// a deleted key stays deleted through a merge and through the reopen after it
#[test]
fn a_grave_survives() {
    let (store, io) = open_over(merging_config(), COLUMNS);
    fill_runs(&store);

    // Deleted in key order, so the segment the tombstones land in is itself a sorted
    // run and the merge takes them as input rather than leaving them outside it.
    let mut gone: Vec<u64> = (0..KEYS).filter(|at| at % 7 == 0).collect();
    gone.sort_by_key(|at| key_at(*at).as_slice().to_vec());
    for at in &gone {
        store.delete(&key_at(*at)).expect("delete");
    }
    store.flush().expect("flush");
    seal_tails(&store);

    let report = store.merge_once().expect("merge");
    assert!(
        report.tombstones_kept >= gone.len() as u64,
        "the merge carried {} tombstones of {} deletes",
        report.tombstones_kept,
        gone.len(),
    );
    assert_every_key_reads(&store, &gone);

    // And after the reopen, where a grave the merge dropped would surface as the older
    // version standing alone.
    store.flush().expect("flush");
    let reopened = ReelStore::open_with_io(
        PathBuf::from("/merged"),
        merging_config(),
        COLUMNS,
        Arc::new(SimIo::from_image(io.durable_image())) as Arc<dyn ReelIo>,
    )
    .expect("reopen");
    assert_every_key_reads(&reopened, &gone);
}

// merge output carries no filter, and the runs it has not reached still carry theirs
#[test]
fn only_merge_output_drops_its_filter() {
    let config = ReelConfig {
        filter_bits: 10,
        fence: FenceResidency::Resident,
        ..merging_config()
    };
    let (store, _io) = open_over(config, COLUMNS);
    fill_runs(&store);

    let standing = sealed_segments(&store);
    let mut checked = 0u64;
    for segment in &standing {
        let footer = store
            .segment_footer(*segment)
            .expect("footer")
            .expect("sealed");
        for partition in &footer.partitions {
            assert!(
                partition.is_filtered(),
                "a fenced sorted run gave up its filter before a base run existed",
            );
            checked += 1;
        }
    }
    assert!(checked > 0, "the fill sealed no run to check");

    store.merge_once().expect("merge");

    let merged: Vec<SegmentId> = sealed_segments(&store)
        .into_iter()
        .filter(|segment| !standing.contains(segment))
        .collect();
    assert!(!merged.is_empty(), "the merge wrote nothing to check");
    for segment in &merged {
        let footer = store
            .segment_footer(*segment)
            .expect("footer")
            .expect("sealed");
        for partition in &footer.partitions {
            assert!(
                !partition.is_filtered(),
                "merge output spent bits its own fence already answers for",
            );
        }
    }
}

// a hot index hands merge output over ahead of everything, rather than behind it
#[test]
fn merge_output_is_not_promotable() {
    let config = ReelConfig {
        index: IndexResidency::Hot(HotIndex {
            after_secs: 3600,
            budget: ByteCount::gb(1),
        }),
        ..merging_config()
    };
    let (store, _io) = open_over(config, COLUMNS);
    fill_runs(&store);

    // Everything standing sealed within the hour, so nothing is owed a handover yet.
    assert_eq!(
        store.page_out_sealed().expect("page out"),
        0,
        "a hot volume gave its keys up inside the residency window",
    );

    let report = store.merge_once().expect("merge");
    assert!(
        report.rows_written > 0,
        "the merge wrote nothing to hand over"
    );

    assert!(
        store.page_out_sealed().expect("page out") > 0,
        "merge output took the residency the recent segments had earned",
    );
}

// a merge carries a row holding its own value forward rather than losing it
#[test]
fn carried_rows_survive() {
    let (store, _io) = open_over(merging_config(), CARRIED);
    fill_runs(&store);

    let report = store.merge_once().expect("merge");

    assert!(report.rows_listed > 0, "a carrying column listed nothing");
    assert_every_key_reads(&store, &[]);
}

// every key answers while the output and the runs it replaces are both standing
#[test]
fn a_read_crosses_a_merge() {
    let (store, _io) = open_over(merging_config(), COLUMNS);
    let store = Arc::new(store);
    fill_runs(&store);

    let mut gone: Vec<u64> = (0..KEYS).filter(|at| at % 7 == 0).collect();
    gone.sort_by_key(|at| key_at(*at).as_slice().to_vec());
    for at in &gone {
        store.delete(&key_at(*at)).expect("delete");
    }
    store.flush().expect("flush");
    seal_tails(&store);

    let script = rendezvous::script();
    script.hold("merge/sealed");

    let merging = {
        let store = Arc::clone(&store);
        thread::spawn(move || store.merge_once().expect("merge"))
    };
    script.await_reached("merge/sealed", 1);

    assert_every_key_reads(&store, &gone);

    script.release("merge/sealed");
    let report = merging.join().expect("merge thread");

    assert!(
        report.sources_retired > 0,
        "the pass parked without retiring anything"
    );
    assert_every_key_reads(&store, &gone);
}

// a capped volume paces its merge as it runs, rather than owning the device for it
#[test]
fn a_capped_merge_is_paced() {
    let (store, io) = open_over(merging_config(), COLUMNS);
    fill_runs(&store);
    store.flush().expect("flush");

    // Reopened rather than filled under the cap: the fill drives compaction to
    // exhaustion, and a gate this tight would turn every one of those passes away.
    let capped = ReelConfig {
        compact_mbps: CompactRate::Mbps(1),
        ..merging_config()
    };
    let store = ReelStore::open_with_io(
        PathBuf::from("/merged"),
        capped,
        COLUMNS,
        Arc::new(SimIo::from_image(io.durable_image())) as Arc<dyn ReelIo>,
    )
    .expect("reopen");

    let started = Instant::now();
    let report = store.merge_once().expect("merge");
    let elapsed = started.elapsed();

    let charged = report.bytes_read + report.bytes_written;
    assert!(
        charged > 300_000,
        "the pass moved {charged} bytes, too few to time"
    );
    // A megabyte a second, and a simulated device hands the bytes over in none of it,
    // so the pass is inside the gate for what it moved less the tail it settles.
    assert!(
        elapsed >= Duration::from_millis(200),
        "a merge of {charged} bytes at a megabyte a second ran in {elapsed:?}",
    );
    assert_every_key_reads(&store, &[]);
}

// the stack's dead share is what the tick decides on, either side of the threshold
#[test]
fn the_stack_debt_triggers_the_tick() {
    // The threshold is read at open, so each half is filled under its own, and what the
    // fill leaves has to be measured before either can be set.
    let (probe, _probe_io) = open_over(merging_config(), COLUMNS);
    fill_runs(&probe);
    let debt = probe
        .sorted_run_dead_ratio()
        .expect("debt")
        .expect("a stack of runs");
    assert!(
        (MARGIN..1.0 - MARGIN).contains(&debt),
        "the fill left a stack {debt} dead, which no threshold sits either side of",
    );
    drop(probe);

    let (under, _under_io) = open_over(triggered_config(debt + MARGIN), COLUMNS);
    fill_runs(&under);
    let standing = standing_runs(&under);
    assert!(
        under
            .sorted_run_dead_ratio()
            .expect("debt")
            .expect("a stack")
            < debt + MARGIN,
        "this half's stack reached a threshold it is meant to sit under",
    );
    assert!(
        under.merge_when_due().expect("merge").is_none(),
        "a stack under the threshold was collapsed anyway",
    );
    under.maintain_once().expect("tick");
    assert_eq!(
        under.compaction_counters().runs_merged,
        0,
        "a tick collapsed a stack the volume did not ask it to",
    );
    assert_eq!(
        standing_runs(&under),
        standing,
        "the stack collapsed without a merge"
    );
    drop(under);

    let (over, _over_io) = open_over(triggered_config(debt - MARGIN), COLUMNS);
    fill_runs(&over);
    let standing = standing_runs(&over);
    assert!(
        over.sorted_run_dead_ratio()
            .expect("debt")
            .expect("a stack")
            >= debt - MARGIN,
        "this half's stack never reached the threshold it is meant to clear",
    );
    over.maintain_once().expect("tick");
    let merged = over.compaction_counters().runs_merged;

    assert!(
        merged >= 2,
        "the tick read {merged} runs of a stack that reached the threshold"
    );
    assert!(
        standing_runs(&over) < standing,
        "the stack did not collapse"
    );
    assert_every_key_reads(&over, &[]);
}

// a volume that armed nothing runs the tick it always ran, byte for byte
#[test]
fn an_unarmed_tick_writes_nothing() {
    let unarmed = ReelConfig {
        merge_sorted_runs: false,
        ..triggered_config(0.0)
    };
    let (store, io) = open_over(unarmed, COLUMNS);
    fill_runs(&store);
    store.flush().expect("flush");

    // Taken with the runs standing and the trigger at a mark every stack clears, so the
    // only thing holding the merge off is the volume never having armed it.
    let before = io.durable_image();
    for _ in 0..TICKS {
        store.maintain_once().expect("tick");
    }
    store.flush().expect("flush");

    assert_eq!(
        io.durable_image(),
        before,
        "an unarmed tick moved bytes on the volume"
    );
    assert_eq!(
        store.compaction_counters().runs_merged,
        0,
        "an unarmed volume merged"
    );
    assert!(
        store.merge_when_due().expect("merge").is_none(),
        "an unarmed volume took a pass when asked for one directly",
    );
    assert_every_key_reads(&store, &[]);
}

// a volume that did not arm the knob refuses a merge rather than doing nothing
#[test]
fn refused_when_unarmed() {
    let config = ReelConfig {
        merge_sorted_runs: false,
        ..merging_config()
    };
    let (store, _io) = open_over(config, COLUMNS);

    assert!(store.merge_once().is_err());
}

// a merge over a volume that does not seal by rewriting is refused at open
#[test]
fn refused_without_the_rewrite() {
    let config = ReelConfig {
        rewrite_on_seal: false,
        merge_sorted_runs: true,
        ..merging_config()
    };

    assert!(config.validate().is_err());
}
