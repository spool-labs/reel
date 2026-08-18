//! Engine tests over the simulated backend

use super::*;

use crate::format::column::KeyWidth;
use std::future::Future;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tempfile::{tempdir, TempDir};

use crate::units::ByteCount;

use crate::config::{
    CompactRate, HotIndex, IndexResidency, PointReads, Preallocate, RangedReads, RepairPath,
    SyncPolicy, ThreadBudget,
};
use crate::format::column::{Codec, ColumnId, ColumnSpec, MapShape};
use crate::format::footer::SegmentFooter;
use crate::format::loc::{Loc, SegmentId};
use crate::format::record::HEADER_LEN;
use crate::index::page::KeyPage;
use crate::io::direct::{covering_span, DIRECT_ALIGN};
use crate::io::fault::{FaultKind, FaultPlan};
use crate::io::posix_backend::PosixBackend;
use crate::io::sim_backend::{DurableImage, SimIo};
use crate::reel::segment_file_name;
use crate::sync::tension::block_on;

/// Virtual volume root the simulator files live under
const ROOT: &str = "/bulk";

const RECORD: ColumnId = ColumnId(1);
const BLOB: ColumnId = ColumnId(2);
const FLAG: ColumnId = ColumnId(3);
const CARRY: ColumnId = ColumnId(4);
const CODED: ColumnId = ColumnId(5);

const COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: RECORD,
        name: "record",
        key_width: KeyWidth::Fixed(34),
        shard_bytes: 2,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
    ColumnSpec {
        id: BLOB,
        name: "blob_data",
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
    ColumnSpec {
        id: FLAG,
        name: "flag",
        key_width: KeyWidth::Fixed(8),
        shard_bytes: 0,
        inline_max: 4,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
];

/// A column whose payloads a codec produced, which a window cannot address
const CODED_COLUMNS: ColumnSet = &[ColumnSpec {
    id: CODED,
    name: "coded",
    key_width: KeyWidth::Fixed(8),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::Lz4,
    map_shape: MapShape::Tree,
}];

/// A carrying column set of its own, since a paged volume refuses one
const CARRY_COLUMNS: ColumnSet = &[ColumnSpec {
    id: CARRY,
    name: "carry",
    key_width: KeyWidth::Fixed(8),
    shard_bytes: 0,
    inline_max: 512,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

const NAMES: ColumnId = ColumnId(6);

/// A variable-keyed column set of its own, for the packed partition reads
const NAME_COLUMNS: ColumnSet = &[ColumnSpec {
    id: NAMES,
    name: "names",
    key_width: KeyWidth::Variable,
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

fn config(active_tails: u32, sync: SyncPolicy) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::mb(1),
        alloc_chunk: ByteCount::from_bytes(16_384),
        preallocate: Preallocate::Chunk,
        sync,
        active_tails: ThreadBudget::threads(active_tails),
        ..ReelConfig::default()
    }
}

fn record(group: u16, byte: u8) -> RecordKey {
    let mut bytes = group.to_be_bytes().to_vec();
    bytes.extend_from_slice(&[byte; 32]);
    RecordKey::from_bytes(RECORD, &bytes).expect("key")
}

fn blob(byte: u8) -> RecordKey {
    RecordKey::from_bytes(BLOB, &[byte; 32]).expect("key")
}

fn flag(byte: u8) -> RecordKey {
    RecordKey::from_bytes(FLAG, &[byte; 8]).expect("key")
}

fn carry(byte: u8) -> RecordKey {
    RecordKey::from_bytes(CARRY, &[byte; 8]).expect("key")
}

fn coded(byte: u8) -> RecordKey {
    RecordKey::from_bytes(CODED, &[byte; 8]).expect("key")
}

/// A payload whose every byte says where it sits, so a window off by one shows
fn stripes(len: usize) -> Vec<u8> {
    (0..len).map(|at| (at % 251) as u8).collect()
}

fn coded_store(config: ReelConfig) -> (ReelStore, SimIo) {
    let sim = SimIo::new(FaultPlan::new(1));
    let store = ReelStore::open_with_io(
        PathBuf::from(ROOT),
        config,
        CODED_COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("open");
    (store, sim)
}

fn carried_held(store: &ReelStore) -> u64 {
    store.index.column(CARRY).expect("column").carried_bytes()
}

fn carried_store(config: ReelConfig) -> (ReelStore, SimIo) {
    let sim = SimIo::new(FaultPlan::new(1));
    let store = ReelStore::open_with_io(
        PathBuf::from(ROOT),
        config,
        CARRY_COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("open");
    (store, sim)
}

/// Run work with a thread draining the simulator behind it
///
/// The simulator neither answers at submission nor files from a thread of its
/// own, so a pending read lands only when somebody drains it.
fn reaping<Out>(store: &ReelStore, work: impl FnOnce() -> Out) -> Out {
    let is_done = AtomicBool::new(false);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            while !is_done.load(Ordering::Relaxed) {
                if store.driver.reap().expect("reap") == 0 {
                    std::thread::sleep(Duration::from_micros(50));
                }
            }
        });
        let out = work();
        is_done.store(true, Ordering::Relaxed);
        out
    })
}

/// A store over a real directory and a backend that services ops in place
///
/// The directory comes back with it, since a temporary one dropped early
/// takes the volume with it.
fn posix_store(config: ReelConfig) -> (ReelStore, Arc<PosixBackend>, TempDir) {
    let dir = tempdir().expect("tempdir");
    let backend = Arc::new(PosixBackend::new());
    let store = ReelStore::open_with_io(
        dir.path().to_path_buf(),
        config,
        COLUMNS,
        Arc::clone(&backend) as Arc<dyn ReelIo>,
    )
    .expect("open");
    (store, backend, dir)
}

fn sim_store(config: ReelConfig) -> (ReelStore, SimIo) {
    let sim = SimIo::new(FaultPlan::new(1));
    let store =
        ReelStore::open_with_io(PathBuf::from(ROOT), config, COLUMNS, Arc::new(sim.clone()))
            .expect("open");
    (store, sim)
}

// a paged volume gives a sealed segment's keys to its footer and still reads them
#[test]
fn a_paged_column_reads_from_its_footer() {
    let paged = ReelConfig {
        index: IndexResidency::Paged,
        ..config(1, SyncPolicy::Never)
    };
    let (store, _sim) = sim_store(paged);

    // A megabyte of segment against 8 KiB records, so the tail rolls partway.
    let payload = vec![0xa5u8; 8 * 1024];
    for byte in 0..200u8 {
        store.put(&record(7, byte), &payload).expect("put");
    }
    // The seal runs on the sealer thread and the flush is what drains it, so the
    // handover below is asserted against a seal that has finished.
    store.flush().expect("flush");
    let sealed = store.page_out_sealed().expect("page out");
    assert!(
        sealed > 0,
        "a rolled tail sealed a segment and it was handed over"
    );

    // The key is gone from the map, so what answers now is the footer.
    let handed = (0..200u8)
        .map(|byte| record(7, byte))
        .find(|key| {
            store
                .index
                .column(key.column)
                .expect("column")
                .entry_or_grave(key.as_slice())
                .is_none()
        })
        .expect("at least one key left the map");

    let found = store.get(&handed).expect("read").expect("still live");
    assert_eq!(found, payload, "the footer answered with the record itself");
    assert!(store.contains(&handed).expect("read"));
}

// a sealed segment's ceiling covers its tombstone rows and not only its records
#[test]
fn a_ceiling_covers_a_tombstone() {
    let (store, _sim) = sim_store(ReelConfig {
        index: IndexResidency::Paged,
        ..config(1, SyncPolicy::Never)
    });

    let payload = vec![0xa5u8; 8 * 1024];
    for byte in 0..8u8 {
        store.put(&record(7, byte), &payload).expect("put");
    }
    for byte in 0..8u8 {
        store.delete(&record(7, byte)).expect("delete");
    }
    let sealed = store.reel.tails()[0].seal().expect("seal");
    store.flush().expect("flush");
    store.hold_sealed().expect("name the sealed segment");

    let footer = store
        .reel
        .shared()
        .footer_of(sealed)
        .expect("footer")
        .expect("the segment sealed");
    let mut newest_delete = Lsn::NONE;
    let mut newest_record = Lsn::NONE;
    for partition in &footer.partitions {
        for entry in partition.entries() {
            let entry = entry.expect("a footer row");
            match entry.is_tombstone() {
                true => newest_delete = newest_delete.max(entry.lsn),
                false => newest_record = newest_record.max(entry.lsn),
            }
        }
    }

    assert!(
        newest_delete > newest_record,
        "the segment's newest row is a record, so a ceiling off the records alone would have held",
    );
    assert_eq!(
        store.index.segments().max_lsn_of(sealed),
        Some(newest_delete),
        "the ceiling stopped short of the delete the segment holds",
    );
}

// a handover never puts a segment compaction has retired back in the search
#[test]
fn a_handover_never_names_a_retired_segment() {
    let (store, _sim) = sim_store(ReelConfig {
        index: IndexResidency::Paged,
        compact_mbps: CompactRate::Mbps(64),
        ..config(1, SyncPolicy::Never)
    });
    let payload = vec![0xa5u8; 8 * 1024];
    for byte in 0..8u8 {
        store.put(&record(7, byte), &payload).expect("put");
    }
    let retired = store.reel.tails()[0].seal().expect("seal");
    store.flush().expect("flush");

    // Named and queued, which is the state the retire raced: the segment is
    // in the search, and its number is still sitting on the handover queue.
    store.hold_sealed().expect("name the sealed segment");

    // Everything it holds is written again, so the pass takes it whole.
    for byte in 0..8u8 {
        store.put(&record(7, byte), &payload).expect("overwrite");
    }
    store.flush().expect("flush");
    store.compact_once().expect("compact");
    assert!(
        !store
            .index
            .segments_snapshot()
            .iter()
            .any(|(segment, _)| *segment == retired),
        "the pass left {retired:?} standing, so this test never reached the race"
    );

    store.page_out_sealed().expect("hand over");

    for byte in 0..8u8 {
        let key = record(7, byte);
        let sites = store.index.sites(&key).expect("sites");
        assert!(
            !sites.candidates.contains(&retired),
            "a read of key {byte} would search retired {retired:?}: {sites:?}"
        );
        let found = store.get(&key).expect("read").expect("still live");
        assert_eq!(found, payload, "key {byte} came back changed");
    }
}

// a pass leaves alone a segment no search has been told about yet
#[test]
fn a_pass_leaves_an_unnamed_segment_alone() {
    let (store, _sim) = sim_store(ReelConfig {
        index: IndexResidency::Paged,
        compact_mbps: CompactRate::Mbps(64),
        ..config(1, SyncPolicy::Never)
    });
    let payload = vec![0xa5u8; 8 * 1024];
    for byte in 0..8u8 {
        store.put(&record(7, byte), &payload).expect("put");
    }
    let sealed = store.reel.tails()[0].seal().expect("seal");
    for byte in 0..8u8 {
        store.put(&record(7, byte), &payload).expect("overwrite");
    }
    store.flush().expect("flush");

    // Wholly dead by the counters and named by nothing. The selection is asked
    // directly, since a whole pass would settle the queue before it picked.
    let picked = store
        .compactor
        .select_whole_dead(&store.reel, &store.index, None);
    assert!(
        picked.is_none(),
        "a pass offered itself {picked:?} while no search could offer {sealed:?}"
    );

    store.hold_sealed().expect("name the sealed segment");
    let picked = store
        .compactor
        .select_whole_dead(&store.reel, &store.index, None);
    assert_eq!(
        picked.map(|(segment, _)| segment),
        Some(sealed),
        "the pass never offered {sealed:?} even once it was named"
    );
}

// a second caller cannot settle a segment the first still holds a footer for
#[test]
fn a_second_caller_cannot_take_a_held_segment() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let payload = vec![0xc7u8; 8 * 1024];
    for byte in 0..8u8 {
        store.put(&record(7, byte), &payload).expect("put");
    }
    let sealed = store.reel.tails()[0].seal().expect("seal");
    // Dead, so the pass would take it the moment the queue stopped saying no.
    for byte in 0..8u8 {
        store.put(&record(7, byte), &payload).expect("overwrite");
    }
    store.flush().expect("flush");

    let store = Arc::new(store);
    let script = crate::sync::rendezvous::script();
    script.hold("seal/spans");

    let noting = {
        let store = Arc::clone(&store);
        std::thread::spawn(move || store.hold_sealed())
    };
    // The footer is read and the spans are not down.
    script.await_reached("seal/spans", 1);

    // The second caller, which is what a read behind a fresh seal is. It must
    // come away with nothing rather than with the segment somebody is holding.
    store.hold_sealed().expect("second caller");
    assert!(
        store.reel.shared().pending_seals().contains(&sealed),
        "the second caller settled a segment the first still had a footer for"
    );
    assert_eq!(
        store
            .compactor
            .select_whole_dead(&store.reel, &store.index, None)
            .map(|(segment, _)| segment),
        None,
        "the pass was offered a segment whose spans are still on their way"
    );

    script.release("seal/spans");
    noting.join().expect("noting thread").expect("hold sealed");

    // Settled by the caller that took it, and only then is it the pass's.
    assert!(
        !store.reel.shared().pending_seals().contains(&sealed),
        "the caller that held it never settled it"
    );
    for byte in 0..8u8 {
        assert!(
            store
                .index
                .sites(&record(7, byte))
                .expect("sites")
                .candidates
                .contains(&sealed),
            "the note that held {sealed:?} never named it",
        );
    }
}

// one unreadable footer costs its own segment and none of the batch behind it
#[test]
fn an_unreadable_footer_does_not_lose_the_batch() {
    let (store, sim) = sim_store(ReelConfig {
        index: IndexResidency::Paged,
        ..config(1, SyncPolicy::Never)
    });
    let payload = vec![0xa5u8; 8 * 1024];
    for byte in 0..8u8 {
        store.put(&record(7, byte), &payload).expect("put");
    }
    let first = store.reel.tails()[0].seal().expect("seal");
    for byte in 8..16u8 {
        store.put(&record(7, byte), &payload).expect("put");
    }
    let second = store.reel.tails()[0].seal().expect("seal");
    store.flush().expect("flush");

    // Wide enough for the length and the two reads one footer costs, narrow
    // enough that the segment behind it is read from a working device.
    sim.arm_next_ops(3, FaultKind::ReadError);
    let outcome = store.hold_sealed();
    sim.disarm();
    assert!(
        outcome.is_ok(),
        "one unreadable footer failed the whole pass: {outcome:?}"
    );
    let (fired, _) = sim.fault_reach();
    assert!(
        fired > 0,
        "the armed read error never landed, so nothing was tested"
    );

    assert!(
        store
            .index
            .sites(&record(7, 8))
            .expect("sites")
            .candidates
            .contains(&second),
        "the segment behind the failure went unnamed: {second:?}"
    );
    assert!(
        !store
            .index
            .sites(&record(7, 0))
            .expect("sites")
            .candidates
            .contains(&first),
        "the failed read named {first:?} anyway, so nothing was tested"
    );
    assert!(
        store.reel.shared().pending_seals().contains(&first),
        "the segment whose footer would not read was dropped rather than kept"
    );

    store.hold_sealed().expect("name what the fault held back");
    assert!(
        store
            .index
            .sites(&record(7, 0))
            .expect("sites")
            .candidates
            .contains(&first),
        "the retry never named {first:?}"
    );
}

// a paged reopen leaves its sealed keys in the footers instead of installing them
#[test]
fn a_paged_open_never_installs_its_sealed_keys() {
    let (store, sim) = sim_store(config(1, SyncPolicy::Never));
    let payload = vec![0xa5u8; 8 * 1024];
    for byte in 0..200u8 {
        store.put(&record(7, byte), &payload).expect("put");
    }
    store.flush().expect("flush");
    let image = sim.durable_image();
    drop(store);

    let resident = reopen_image(image.clone(), config(1, SyncPolicy::Never));
    let paged = reopen_image(
        image,
        ReelConfig {
            index: IndexResidency::Paged,
            ..config(1, SyncPolicy::Never)
        },
    );

    // Nothing was queued to hand over, because nothing was installed to evict.
    assert_eq!(paged.page_out_sealed().expect("page out"), 0);
    assert!(
        paged.resident_bytes() < resident.resident_bytes(),
        "the paged open held as much as the resident one: {:?} against {:?}",
        paged.resident_bytes(),
        resident.resident_bytes()
    );

    // And it answers every key regardless, from the footers it swept.
    for byte in 0..200u8 {
        let key = record(7, byte);
        assert_eq!(
            paged.get(&key).expect("read"),
            resident.get(&key).expect("read"),
            "the two opens disagree about {byte}"
        );
    }
    // What it does not carry over is the count: how many sealed records a newer
    // version shadows is the join a paged open does not do.
    assert!(paged.totals().count < resident.totals().count);
}

// a failed seal parks the segment and the tick's retry finishes it
#[test]
fn a_failed_seal_is_retried_on_the_tick() {
    let (store, sim) = sim_store(config(1, SyncPolicy::Never));
    let payload = vec![0xa5u8; 8 * 1024];
    for byte in 0..64u8 {
        store.put(&record(7, byte), &payload).expect("put");
    }
    // Nothing flushed: the records are acknowledged and cached, which is
    // what makes a failed seal cost a past-saving count.

    // Every sync the seal takes fails, so the footer cannot be answered for.
    sim.arm_next_ops(8, FaultKind::SyncError);
    store.reel.tails()[0]
        .seal()
        .expect_err("a seal whose syncs fail reported sealed");
    sim.disarm();
    assert!(
        store.reel.shared().past_saving_count() > 0,
        "the failed seal counted nothing past saving"
    );

    let sealed = store.retry_broken_seals();
    assert_eq!(sealed, 1, "the parked seal did not land");
    assert_eq!(store.reel.shared().past_saving_count(), 0);
    store
        .flush()
        .expect("a volume with every seal down flushes clean");

    // And the segment is a real sealed segment: a paged reopen resolves its
    // keys through the footer the retry wrote.
    let image = sim.durable_image();
    drop(store);
    let paged = reopen_image(
        image,
        ReelConfig {
            index: IndexResidency::Paged,
            ..config(1, SyncPolicy::Never)
        },
    );
    assert!(paged.get(&record(7, 5)).expect("get").is_some());
}

// a walked tail's overwrites reach the sealed split at open, not at the scrub
#[test]
fn a_walked_tail_settles_the_sealed_split() {
    let settings = config(1, SyncPolicy::Never);
    let (store, sim) = sim_store(settings.clone());

    // Enough distinct keys that the tail rolls and seals a segment.
    let payload = vec![0xa5u8; 8 * 1024];
    for byte in 0..150u8 {
        store.put(&record(7, byte), &payload).expect("put");
    }
    store.flush().expect("flush");
    assert!(
        !store.index.segments_snapshot().is_empty(),
        "nothing sealed, so there is no tally to settle"
    );

    // Rewrite a few of the sealed keys. These land in the open tail, after
    // every seal, which is the shadowing no tally can carry.
    for byte in 0..20u8 {
        store.put(&record(7, byte), &payload).expect("rewrite");
    }
    store.flush().expect("flush");

    let dead_when_written: u64 = store
        .index
        .segments_snapshot()
        .iter()
        .map(|(_, bytes)| bytes.dead)
        .sum();
    assert!(dead_when_written > 0, "the rewrites booked nothing dead");
    let image = sim.durable_image();
    drop(store);

    let paged = reopen_image(
        image,
        ReelConfig {
            index: IndexResidency::Paged,
            ..settings
        },
    );
    let dead_at_open: u64 = paged
        .index
        .segments_snapshot()
        .iter()
        .map(|(_, bytes)| bytes.dead)
        .sum();
    assert_eq!(
        dead_at_open, dead_when_written,
        "the open's split disagrees with what the live process booked"
    );
}

// the tally carries the split through a paged open, and the scrub finishes it
#[test]
fn a_paged_open_recovers_its_split_from_the_tally() {
    let settings = ReelConfig {
        scrub_mbps: 64,
        ..config(1, SyncPolicy::Never)
    };
    let (store, sim) = sim_store(settings.clone());

    // Write every key twice, so half of what is on disk is shadowed.
    let payload = vec![0xa5u8; 8 * 1024];
    for round in 0..2 {
        for byte in 0..120u8 {
            store
                .put(&record(7, byte), &[payload.as_slice(), &[round]].concat())
                .expect("put");
        }
    }
    store.flush().expect("flush");
    let dead_when_written: u64 = store
        .index
        .segments_snapshot()
        .iter()
        .map(|(_, bytes)| bytes.dead)
        .sum();
    assert!(dead_when_written > 0, "the overwrites booked dead bytes");
    let image = sim.durable_image();
    drop(store);

    let paged = reopen_image(
        image,
        ReelConfig {
            index: IndexResidency::Paged,
            ..settings
        },
    );
    let dead_at_open: u64 = paged
        .index
        .segments_snapshot()
        .iter()
        .map(|(_, bytes)| bytes.dead)
        .sum();
    assert!(
        dead_at_open > 0,
        "the seal wrote no tally, so the open resolved no split at all"
    );
    assert!(
        dead_at_open <= dead_when_written,
        "the tally claimed {dead_at_open} dead against the {dead_when_written} actually booked"
    );

    // Sweep the volume. The scrub earns its budget from the clock, so a pass
    // taken the instant a volume opens has almost nothing to spend.
    for _ in 0..64 {
        std::thread::sleep(Duration::from_millis(20));
        paged.scrub_once().expect("scrub");
        if paged.compactor.scrub_resume_point().is_none() {
            break;
        }
    }

    let dead_after_scrub: u64 = paged
        .index
        .segments_snapshot()
        .iter()
        .map(|(_, bytes)| bytes.dead)
        .sum();
    assert!(
        dead_after_scrub >= dead_at_open,
        "the scrub lost ground: {dead_after_scrub} against {dead_at_open} at open"
    );
    assert!(
        dead_after_scrub >= dead_when_written,
        "the open and the scrub together recovered {dead_after_scrub} of {dead_when_written}"
    );
}

// a paged read reaches its key through blocks rather than the whole footer
#[test]
fn a_paged_read_reads_blocks_not_footers() {
    let paged = ReelConfig {
        index: IndexResidency::Paged,
        // Small segments against small records, so each footer holds thousands
        // of rows and a block is a fraction of one.
        segment_bytes: ByteCount::from_bytes(256 * 1024),
        ..config(1, SyncPolicy::Never)
    };
    let (store, sim) = sim_store(paged);

    let wide = |at: u32| {
        let mut bytes = 7u16.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[0u8; 32]);
        bytes[2..6].copy_from_slice(&at.to_be_bytes());
        RecordKey::from_bytes(RECORD, &bytes).expect("key")
    };
    let payload = vec![0xa5u8; 64];
    for at in 0..8_000u32 {
        store.put(&wide(at), &payload).expect("put");
    }
    // Drain the sealer before asserting on the handover, as above.
    store.flush().expect("flush");
    assert!(store.page_out_sealed().expect("page out") > 0);
    store.flush().expect("flush");

    let handed: Vec<RecordKey> = (0..8_000u32)
        .map(wide)
        .filter(|key| is_paged(&store, key))
        .collect();
    assert!(!handed.is_empty(), "some keys went to their footer");

    // What one pass of the footers weighs, which is what answering a single key
    // costs without the blocks.
    let footers: usize = store
        .index
        .segments_snapshot()
        .into_iter()
        .filter_map(|(segment, _)| {
            store
                .reel
                .shared()
                .footer_of(segment)
                .expect("footer")
                .map(|footer| footer.encoded_len())
        })
        .sum();
    assert!(footers > 0, "the sealed segments carry footers");

    // Resolving a key rather than reading it, so what this counts is the index
    // and not the record behind it. The cache starts empty either way.
    store.reel.shared().footers.clear();
    let before = sim.read_bytes();
    for key in handed.iter().take(16) {
        assert!(store.contains(key).expect("resolve"), "a key went missing");
    }
    let through_blocks = sim.read_bytes() - before;

    // A quarter, not merely less: parsing whole footers also comes in under one
    // pass once the cache holds them, so a bound of one pass would pin nothing.
    assert!(
        through_blocks * 4 < footers as u64,
        "16 resolves through blocks cost {through_blocks} bytes against {footers} for one pass of the footers"
    );
}

// the footers, the directories and the blocks answer for one bound between them
#[test]
fn footer_pools_share_one_bound() {
    // A bound every pool has more than enough sealed state to fill on its own, so
    // pools each held to the whole of it would keep a multiple of what was asked for.
    let knob = ByteCount::from_bytes(24 * 1024);
    let paged = ReelConfig {
        index: IndexResidency::Paged,
        footer_cache: knob,
        ..config(1, SyncPolicy::Never)
    };
    let (store, _sim) = sim_store(paged);

    let wide = |at: u32| {
        let mut bytes = 7u16.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[0u8; 32]);
        bytes[2..6].copy_from_slice(&at.to_be_bytes());
        RecordKey::from_bytes(RECORD, &bytes).expect("key")
    };
    let payload = vec![0xa5u8; 8 * 1024];
    for at in 0..800u32 {
        store.put(&wide(at), &payload).expect("put");
    }
    store.flush().expect("flush");
    assert!(store.page_out_sealed().expect("page out") > 0);
    store.flush().expect("flush");

    let handed: Vec<RecordKey> = (0..800u32)
        .map(wide)
        .filter(|key| is_paged(&store, key))
        .collect();
    assert!(!handed.is_empty(), "some keys went to their footer");

    // Cleared first, since a resolve that finds a footer the handover parsed never
    // reaches for a block. The resolves fill two pools and the pass after them the third.
    store.reel.shared().footers.clear();
    for key in &handed {
        assert!(store.contains(key).expect("resolve"), "a key went missing");
    }
    for (segment, _) in store.index.segments_snapshot() {
        let _ = store.reel.shared().footer_of(segment).expect("footer");
    }

    let (footers, maps, blocks) = store.reel.shared().footers.held_split();
    assert!(
        footers > 0 && maps > 0 && blocks > 0,
        "a pool held nothing, so their sum proves nothing: {footers} footers, {maps} directories, {blocks} blocks"
    );
    assert!(
        footers + maps + blocks <= knob.to_bytes() as usize,
        "the pools hold {} bytes against a bound of {}: {footers} footers, {maps} directories, {blocks} blocks",
        footers + maps + blocks,
        knob.to_bytes(),
    );
}

// a key no sealed segment holds is answered without asking any of them
#[test]
fn a_fresh_key_skips_the_sealed_search() {
    let paged = ReelConfig {
        index: IndexResidency::Paged,
        ..config(1, SyncPolicy::Never)
    };
    let sim = SimIo::new(FaultPlan::new(1));
    let store = ReelStore::open_with_io(
        PathBuf::from(ROOT),
        paged.clone(),
        COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("open");

    let payload = vec![0xa5u8; 8 * 1024];
    for byte in 0..200u8 {
        store.put(&record(7, byte), &payload).expect("put");
    }
    store.flush().expect("flush");
    assert!(store.page_out_sealed().expect("page out") > 0);
    store.flush().expect("flush");

    let sealed = (0..200u8)
        .map(|byte| record(7, byte))
        .find(|key| is_paged(&store, key));
    let sealed = sealed.expect("a key went to its footer");

    // A key never written: ruled out by the filter, no candidate asked.
    let before = store.index.sealed_skips();
    assert!(store.get(&record(9, 250)).expect("get").is_none());
    assert!(
        store.index.sealed_skips() > before,
        "the fresh key searched segments"
    );
    // A sealed key still resolves through the fan-out the filter guards.
    assert!(store.get(&sealed).expect("get").is_some());

    store.close().expect("close");
    drop(store);

    // The paged rebuild feeds the filter from the footer sweep, so the same
    // pair of answers holds on the reopened volume.
    let restored = SimIo::from_image(sim.durable_image());
    let reopened = ReelStore::open_with_io(PathBuf::from(ROOT), paged, COLUMNS, Arc::new(restored))
        .expect("reopen");
    let before = reopened.index.sealed_skips();
    assert!(reopened.get(&record(9, 250)).expect("get").is_none());
    assert!(
        reopened.index.sealed_skips() > before,
        "the rebuild fed no filter"
    );
    assert!(reopened.get(&sealed).expect("get").is_some());
}

// the same resolve on a packed partition reads restart cuts, not the partition
#[test]
fn a_paged_read_of_packed_rows_reads_cuts() {
    let paged = ReelConfig {
        index: IndexResidency::Paged,
        segment_bytes: ByteCount::from_bytes(256 * 1024),
        ..config(1, SyncPolicy::Never)
    };
    let sim = SimIo::new(FaultPlan::new(1));
    let store = ReelStore::open_with_io(
        PathBuf::from(ROOT),
        paged,
        NAME_COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("open");

    let name = |at: u32| {
        let path = format!(
            "tenants/{:08x}/exports/2026/08/02/part-{:05}.parquet",
            at / 64,
            at % 64
        );
        RecordKey::from_bytes(NAMES, path.as_bytes()).expect("key")
    };
    let payload = vec![0xa5u8; 64];
    for at in 0..8_000u32 {
        store.put(&name(at), &payload).expect("put");
    }
    store.flush().expect("flush");
    assert!(store.page_out_sealed().expect("page out") > 0);
    store.flush().expect("flush");

    let handed: Vec<RecordKey> = (0..8_000u32)
        .map(name)
        .filter(|key| is_paged(&store, key))
        .collect();
    assert!(!handed.is_empty(), "some keys went to their footer");

    // What the partitions weigh on disk, which is what answering one key costs
    // without the cuts.
    let packed: u64 = store
        .index
        .segments_snapshot()
        .into_iter()
        .filter_map(|(segment, _)| {
            store
                .reel
                .shared()
                .footer_map_of(segment)
                .expect("map")
                .and_then(|map| map.span_of(NAMES))
                .map(|span| span.encoded)
        })
        .sum();
    assert!(packed > 0, "the sealed segments carry packed partitions");

    store.reel.shared().footers.clear();
    let before = sim.read_bytes();
    for key in handed.iter().take(16) {
        assert!(store.contains(key).expect("resolve"), "a key went missing");
    }
    let through_cuts = sim.read_bytes() - before;

    assert!(
        through_cuts * 10 < packed,
        "16 resolves through cuts cost {through_cuts} bytes against {packed} for one pass of the partitions"
    );
}

// compaction leaves what it rewrote sorted on disk, not in the order it found it
#[test]
fn compaction_leaves_its_output_in_key_order() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));

    // Written in an order that is not key order, which is the case that matters.
    let payload = vec![0xa5u8; 8 * 1024];
    let mut wrote: Vec<u8> = (0..120u8).collect();
    wrote.rotate_left(37);
    for byte in &wrote {
        store.put(&record(7, *byte), &payload).expect("put");
    }
    // Shadow half of them, so the pass has dead records to leave behind.
    for byte in wrote.iter().filter(|byte| **byte % 2 == 0) {
        store.put(&record(7, *byte), &payload).expect("overwrite");
    }
    store.reel.tails()[0].seal().expect("seal");
    store.flush().expect("flush");

    // Which segments existed before the pass, so the check below looks only at
    // what compaction wrote.
    let existing: std::collections::HashSet<SegmentId> = store
        .index
        .segments_snapshot()
        .into_iter()
        .map(|(segment, _)| segment)
        .collect();

    for _ in 0..16 {
        let before = store.compaction_counters();
        store.compact_once().expect("compact");
        if store.compaction_counters() == before {
            break;
        }
    }
    assert!(
        store.compaction_counters().segments_rewritten > 0,
        "nothing was rewritten, so this proves nothing"
    );

    // Where every surviving key now sits, grouped by the segment holding it,
    // since an offset only means anything inside its own file.
    let mut placed: std::collections::HashMap<SegmentId, Vec<(u8, u32)>> =
        std::collections::HashMap::new();
    for byte in 0..120u8 {
        let key = record(7, byte);
        if let Some(entry) = store.index.get(&key).expect("resolve") {
            placed
                .entry(entry.loc.segment)
                .or_default()
                .push((byte, entry.loc.offset));
        }
    }

    let mut checked = 0usize;
    for (segment, mut keys) in placed {
        if existing.contains(&segment) || keys.len() < 8 {
            continue;
        }
        keys.sort_unstable_by_key(|(byte, _)| *byte);
        let offsets: Vec<u32> = keys.iter().map(|(_, at)| *at).collect();
        let mut ascending = offsets.clone();
        ascending.sort_unstable();
        assert_eq!(
            offsets, ascending,
            "segment {segment:?} holds its keys out of offset order, so a walk of it seeks"
        );
        checked += 1;
    }
    assert!(
        checked > 0,
        "compaction wrote no new segment with enough survivors to say anything"
    );
}

// paging a key out is not deleting it, so the totals do not move
#[test]
fn paging_out_keeps_the_count() {
    let paged = ReelConfig {
        index: IndexResidency::Paged,
        ..config(1, SyncPolicy::Never)
    };
    let (store, _sim) = sim_store(paged);

    let payload = vec![0x5au8; 8 * 1024];
    for byte in 0..200u8 {
        store.put(&record(7, byte), &payload).expect("put");
    }
    // A roll hands its segment to the sealer thread and returns, so without this
    // there is nothing sealed to page out yet.
    store.flush().expect("flush");
    let before = store.totals();

    assert!(store.page_out_sealed().expect("page out") > 0);

    assert_eq!(store.totals(), before, "a paged key is still a live key");
}

/// A paged volume holding one filled group, and one key it has handed over
///
/// The segment is a megabyte against 8 KiB records, so the tail rolls partway
/// through and the keys of what it sealed go to their footer.
fn paged_fixture(payload: &[u8]) -> (ReelStore, RecordKey) {
    let paged = ReelConfig {
        index: IndexResidency::Paged,
        ..config(1, SyncPolicy::Never)
    };
    let (store, _sim) = sim_store(paged);
    for byte in 0..200u8 {
        store.put(&record(7, byte), payload).expect("put");
    }
    // The roll seals on the sealer thread, so a handover asked for the instant
    // the last put returns can find nothing sealed yet.
    store.flush().expect("flush");
    assert!(store.page_out_sealed().expect("page out") > 0);

    let handed = (0..200u8)
        .map(|byte| record(7, byte))
        .find(|key| is_paged(&store, key))
        .expect("at least one key left the map");
    (store, handed)
}

fn is_paged(store: &ReelStore, key: &RecordKey) -> bool {
    store
        .index
        .column(key.column)
        .expect("column")
        .entry_or_grave(key.as_slice())
        .is_none()
}

/// Every key one column serves, in order, through the paging playback
fn played(store: &ReelStore, column: ColumnId) -> Vec<Vec<u8>> {
    let mut page = KeyPage::default();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    loop {
        let bound = match keys.last() {
            Some(last) => Bound::Excluded(last.as_slice()),
            None => Bound::Unbounded,
        };
        store.page(column, bound, 16, &mut page).expect("page");
        if page.is_empty() {
            return keys;
        }
        for at in 0..page.len() {
            keys.push(page.key_at(at));
        }
    }
}

// a playback of a paged column sees the keys its footers hold
#[test]
fn a_walk_sees_paged_keys() {
    let payload = vec![0xa5u8; 8 * 1024];
    let (store, _handed) = paged_fixture(&payload);

    let keys = played(&store, RECORD);

    let mut expected: Vec<Vec<u8>> = (0..200u8)
        .map(|byte| record(7, byte).as_slice().to_vec())
        .collect();
    expected.sort();
    assert_eq!(keys, expected, "every key once, in key order");
    assert_eq!(store.totals().count, 200, "and the counters agree with it");
}

// deleting a key a footer answers for settles the record it was holding
#[test]
fn a_paged_delete_settles_its_record() {
    let payload = vec![0xa5u8; 8 * 1024];
    let (store, handed) = paged_fixture(&payload);
    let dead_before = store.dead_bytes().to_bytes();

    store.delete(&handed).expect("delete");

    assert!(store.get(&handed).expect("read").is_none());
    assert_eq!(
        store.totals().count,
        199,
        "the deleted key stopped counting"
    );
    assert!(
        store.dead_bytes().to_bytes() > dead_before,
        "and its record became reclaimable"
    );
    assert_eq!(played(&store, RECORD).len(), 199, "the playback agrees");
}

// overwriting a key a footer answers for is one key, not two
#[test]
fn a_paged_overwrite_replaces_rather_than_adds() {
    let payload = vec![0xa5u8; 8 * 1024];
    let (store, handed) = paged_fixture(&payload);
    let dead_before = store.dead_bytes().to_bytes();

    let fresh = vec![0x5au8; 4 * 1024];
    store.put(&handed, &fresh).expect("overwrite");

    assert_eq!(store.get(&handed).expect("read"), Some(Value::new(fresh)));
    assert_eq!(
        store.totals().count,
        200,
        "one key, whichever place holds it"
    );
    assert!(
        store.dead_bytes().to_bytes() > dead_before,
        "the record it replaced became reclaimable"
    );
    assert_eq!(played(&store, RECORD).len(), 200, "the playback agrees");
}

// a grave over a paged key is not given up on a sequence number alone
#[test]
fn a_paged_grave_outlives_the_window() {
    let payload = vec![0xa5u8; 8 * 1024];
    let (store, handed) = paged_fixture(&payload);
    store.delete(&handed).expect("delete");

    // A floor far past every sequence number the volume has issued.
    store.index.prune_tombstones(Lsn(u64::MAX));

    assert!(
        store.get(&handed).expect("read").is_none(),
        "the delete still holds"
    );
    assert_eq!(store.totals().count, 199);
}

// an idle tick prunes a grave to the counter, not to the window
#[test]
fn an_idle_tick_prunes_graves_to_the_counter() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    store.put(&blob(1), &[7u8; 64]).expect("put");
    store.delete(&blob(1)).expect("delete");
    assert_eq!(store.index.grave_count(), 1);

    let pruned = store.prune_tombstones();

    assert_eq!(
        pruned, 1,
        "nothing was in flight, so the grave went at once"
    );
    assert_eq!(store.index.grave_count(), 0);
    assert!(
        store.get(&blob(1)).expect("read").is_none(),
        "the delete still holds without its grave"
    );
}

// ingest heat is a rate over the ask interval, not a point sample
#[test]
fn ingest_heat_is_a_rate_not_a_point_sample() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    assert!(!store.is_ingest_hot(), "an idle volume is not hot");

    let payload = vec![3u8; 256 * 1024];
    for byte in 0..36u8 {
        store.put(&record(0, byte), &payload).expect("put");
    }
    assert!(
        store.is_ingest_hot(),
        "nine mebibytes since the last ask is ingest"
    );
    assert!(
        !store.is_ingest_hot(),
        "the ask moved the marker, and nothing was admitted since"
    );

    store.put(&blob(200), &[1u8; 64]).expect("put");
    assert!(
        !store.is_ingest_hot(),
        "a trickle below the floor is not hot"
    );
}

/// A paged volume with one group paged out, and the sim holding its bytes
fn paged_image(payload: &[u8], settings: ReelConfig) -> (ReelStore, SimIo, RecordKey) {
    let (store, sim) = sim_store(settings);
    for byte in 0..200u8 {
        store.put(&record(7, byte), payload).expect("put");
    }
    // The roll seals on the sealer thread, so a handover asked for the instant
    // the last put returns can find nothing sealed yet.
    store.flush().expect("flush");
    assert!(store.page_out_sealed().expect("page out") > 0);
    let handed = (0..200u8)
        .map(|byte| record(7, byte))
        .find(|key| is_paged(&store, key))
        .expect("at least one key left the map");
    (store, sim, handed)
}

// a delete stays done through a paged reopen
#[test]
fn a_delete_survives_a_paged_reopen() {
    let settings = ReelConfig {
        index: IndexResidency::Paged,
        ..config(1, SyncPolicy::Never)
    };
    let payload = vec![0xa5u8; 8 * 1024];
    let (store, sim, handed) = paged_image(&payload, settings.clone());

    store.delete(&handed).expect("delete");
    assert!(
        store.get(&handed).expect("read").is_none(),
        "the delete took"
    );
    store.flush().expect("flush");
    let image = sim.durable_image();
    drop(store);

    let reopened = reopen_image(image, settings);

    assert!(
        reopened.get(&handed).expect("read").is_none(),
        "the deleted key came back through the reopen"
    );
    assert_eq!(
        played(&reopened, RECORD).len(),
        199,
        "and a walk found it again"
    );
}

// a range delete stays done through a paged reopen
#[test]
fn a_range_delete_survives_a_paged_reopen() {
    let settings = ReelConfig {
        index: IndexResidency::Paged,
        ..config(1, SyncPolicy::Never)
    };
    let payload = vec![0xa5u8; 8 * 1024];
    let (store, sim, handed) = paged_image(&payload, settings.clone());

    let start = RecordKey::from_bytes(RECORD, &group_bound(7)).expect("key");
    store
        .delete_range(&start, Some(&group_bound(8)))
        .expect("range delete");
    assert!(
        store.get(&handed).expect("read").is_none(),
        "the range took"
    );
    store.flush().expect("flush");
    let image = sim.durable_image();
    drop(store);

    let reopened = reopen_image(image, settings);

    assert!(
        reopened.get(&handed).expect("read").is_none(),
        "a key the range covered came back through the reopen"
    );
    assert!(
        played(&reopened, RECORD).is_empty(),
        "and a walk found the group again"
    );
}

// a group drop is one push, and its records settle at the sweep
#[test]
fn a_dropped_group_settles_at_the_sweep() {
    let settings = ReelConfig {
        index: IndexResidency::Paged,
        ..config(1, SyncPolicy::Never)
    };
    let payload = vec![0xa5u8; 8 * 1024];
    let (store, _sim, handed) = paged_image(&payload, settings);
    let before = store.totals();

    let start = RecordKey::from_bytes(RECORD, &group_bound(7)).expect("key");
    store
        .delete_range(&start, Some(&group_bound(8)))
        .expect("range delete");

    assert!(
        store.get(&handed).expect("read").is_none(),
        "gone before the sweep"
    );
    assert!(
        played(&store, RECORD).is_empty(),
        "a walk sees no covered key"
    );
    assert_eq!(store.totals(), before, "the counters wait for the sweep");
    store
        .compact_once()
        .expect("a pass that must retire nothing");

    while store.sweep_covers().expect("sweep") {}

    assert_eq!(store.totals().count, 0);
    assert_eq!(store.totals().bytes.to_bytes(), 0);
    assert!(store.get(&handed).expect("read").is_none());
    assert!(played(&store, RECORD).is_empty());
}

// two drops over one range settle each record exactly once
#[test]
fn overlapping_drops_settle_once() {
    let settings = ReelConfig {
        index: IndexResidency::Paged,
        ..config(1, SyncPolicy::Never)
    };
    let payload = vec![0xa5u8; 8 * 1024];
    let (store, _sim, _handed) = paged_image(&payload, settings);

    let start = RecordKey::from_bytes(RECORD, &group_bound(7)).expect("key");
    store
        .delete_range(&start, Some(&group_bound(8)))
        .expect("first drop");
    while store.sweep_covers().expect("sweep") {}
    store
        .delete_range(&start, Some(&group_bound(8)))
        .expect("second drop");
    while store.sweep_covers().expect("sweep") {}

    assert_eq!(store.totals().count, 0);
    assert_eq!(store.totals().bytes.to_bytes(), 0);
    assert!(played(&store, RECORD).is_empty());
}

// compaction of a paged segment leaves its keys readable and counted
#[test]
fn compaction_carries_a_paged_key() {
    let payload = vec![0xa5u8; 8 * 1024];
    let (store, handed) = paged_fixture(&payload);
    let target = store
        .index
        .segments_snapshot()
        .into_iter()
        .map(|(segment, _)| segment)
        .find(|segment| {
            store
                .index
                .get(&handed)
                .expect("read")
                .is_some_and(|entry| entry.loc.segment == *segment)
        })
        .expect("the key resolves to a sealed segment");

    store
        .compactor
        .compact_segment(&store.reel, &store.index, target)
        .expect("compact");

    assert_eq!(
        store.get(&handed).expect("read"),
        Some(Value::new(payload)),
        "the key reads from wherever compaction put it"
    );
    assert_eq!(store.totals().count, 200);
    assert_eq!(played(&store, RECORD).len(), 200, "the playback agrees");
}

// compaction after a paged reopen keeps the footer's keys readable
#[test]
fn compaction_carries_a_key_through_a_paged_reopen() {
    let settings = ReelConfig {
        index: IndexResidency::Paged,
        ..config(1, SyncPolicy::Never)
    };
    let payload = vec![0xa5u8; 8 * 1024];
    let (store, sim, handed) = paged_image(&payload, settings.clone());
    store.flush().expect("flush");
    let image = sim.durable_image();
    drop(store);
    let reopened = reopen_image(image, settings);

    let target = reopened
        .index
        .get(&handed)
        .expect("read")
        .expect("present")
        .loc
        .segment;

    reopened
        .compactor
        .compact_segment(&reopened.reel, &reopened.index, target)
        .expect("compact");

    assert_eq!(
        reopened.get(&handed).expect("read"),
        Some(Value::new(payload)),
        "the compacted key went unreadable through the retire"
    );
    assert_eq!(
        played(&reopened, RECORD).len(),
        200,
        "the walk keeps every key"
    );
}

// a cue point keeps serving the old value after an overwrite
#[test]
fn cue_holds_the_old_version() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let key = record(7, 1);
    store.put(&key, b"first").expect("put");

    let cue = store.cue().expect("cue");
    store.put(&key, b"second").expect("overwrite");

    assert_eq!(
        store.get(&key).expect("read"),
        Some(Value::new(b"second".to_vec()))
    );
    assert_eq!(
        store.get_at(&key, &cue).expect("read at cue"),
        Some(Value::new(b"first".to_vec())),
        "the cue point saw the overwrite it was taken before"
    );
}

// a key written after the cue point is invisible to it
#[test]
fn cue_hides_later_writes() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let cue = store.cue().expect("cue");
    store.put(&record(7, 1), b"after").expect("put");

    assert!(store.contains(&record(7, 1)).expect("read"));
    assert_eq!(
        store.get_at(&record(7, 1), &cue).expect("read at cue"),
        None
    );
}

// a delete after the cue point does not take the value from it
#[test]
fn cue_outlives_a_delete() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let key = record(7, 1);
    store.put(&key, b"kept").expect("put");
    let cue = store.cue().expect("cue");
    store.delete(&key).expect("delete");

    assert!(!store.contains(&key).expect("read"));
    assert_eq!(
        store.get_at(&key, &cue).expect("read at cue"),
        Some(Value::new(b"kept".to_vec())),
    );
}

// a range delete drawn after the cue point is invisible to it
#[test]
fn cue_ignores_a_later_drop() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let key = record(7, 1);
    store.put(&key, b"kept").expect("put");
    let cue = store.cue().expect("cue");

    let start = RecordKey::from_bytes(RECORD, &group_bound(7)).expect("key");
    store
        .delete_range(&start, Some(&group_bound(8)))
        .expect("drop");
    while store.sweep_covers().expect("sweep") {}

    assert!(!store.contains(&key).expect("read"));
    assert_eq!(
        store.get_at(&key, &cue).expect("read at cue"),
        Some(Value::new(b"kept".to_vec())),
        "the drop reached back past the cue point",
    );
}

// a held cue point stops compaction retiring what it can still read
#[test]
fn cue_pins_compaction() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let key = record(7, 1);
    store.put(&key, b"first").expect("put");
    let cue = store.cue().expect("cue");
    store.put(&key, b"second").expect("overwrite");
    store.flush().expect("flush");

    for _ in 0..8 {
        store.compact_once().expect("compact");
    }

    assert_eq!(
        store.get_at(&key, &cue).expect("read at cue"),
        Some(Value::new(b"first".to_vec())),
        "compaction reclaimed a version the cue point still needed",
    );
}

// the floor lifts once the last holder lets go
#[test]
fn floor_lifts_on_drop() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    store.put(&record(7, 1), b"one").expect("put");

    let cue = store.cue().expect("cue");
    assert!(!store.cue_points().is_empty());
    let at = cue.at();
    drop(cue);

    assert!(store.cue_points().is_empty());
    assert_eq!(store.cue_points().floor(), None);
    assert!(at.as_u64() > 0);
}

// a resident volume records which segments cover which keys
#[test]
fn a_resident_volume_records_its_sealed_spans() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let payload = vec![0xa5u8; 8 * 1024];
    for byte in 0..200u8 {
        store.put(&record(7, byte), &payload).expect("put");
    }

    // Drain the sealer, whose thread is what records the spans, so the
    // assertions below race nothing.
    store.flush().expect("flush");
    assert_eq!(
        store.page_out_sealed().expect("page out"),
        0,
        "nothing is handed over"
    );
    assert!(
        store.index.answers_from_footers(RECORD) || store.index.sealed_spans(RECORD) > 0,
        "a resident volume sealed segments and recorded none of them"
    );
    assert!(
        (0..200u8).all(|byte| !is_paged(&store, &record(7, byte))),
        "every key still answers from the map"
    );
    assert_eq!(store.totals().count, 200);
}

// a hot index keeps a freshly sealed segment resident and reads it from the map
#[test]
fn a_hot_index_holds_recent_keys() {
    let hot = ReelConfig {
        index: IndexResidency::Hot(HotIndex {
            after_secs: 3600,
            budget: ByteCount::gb(1),
        }),
        ..config(1, SyncPolicy::Never)
    };
    let (store, _sim) = sim_store(hot);

    let payload = vec![0xa5u8; 8 * 1024];
    for byte in 0..200u8 {
        store.put(&record(7, byte), &payload).expect("put");
    }

    assert_eq!(
        store.page_out_sealed().expect("page out"),
        0,
        "nothing is old enough or dear enough to hand over"
    );
    assert!(
        (0..200u8).all(|byte| !is_paged(&store, &record(7, byte))),
        "every key still answers from the map"
    );
    assert_eq!(store.totals().count, 200);
}

// a hot index over its budget hands the oldest segments over early
#[test]
fn a_hot_index_pages_when_it_runs_out_of_room() {
    let hot = ReelConfig {
        index: IndexResidency::Hot(HotIndex {
            after_secs: 3600,
            budget: ByteCount::from_bytes(1),
        }),
        ..config(1, SyncPolicy::Never)
    };
    let (store, _sim) = sim_store(hot);

    let payload = vec![0xa5u8; 8 * 1024];
    for byte in 0..200u8 {
        store.put(&record(7, byte), &payload).expect("put");
    }

    assert!(
        store.page_out_sealed().expect("page out") > 0,
        "over budget, the oldest sealed segment goes"
    );
    assert_eq!(
        store.totals().count,
        200,
        "handing keys over is not deleting them"
    );
    assert_eq!(
        played(&store, RECORD).len(),
        200,
        "and the playback still sees them"
    );
}

// a segment holding only tombstones is still accounted for, so it can be seen
#[test]
fn a_segment_of_only_tombstones_is_accounted_for() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));

    // Fill one segment with records, then seal it so the next tail is fresh.
    let payload = vec![0xa5u8; 8 * 1024];
    for byte in 0..200u8 {
        store.put(&record(7, byte), &payload).expect("put");
    }
    store.reel.tails()[0].seal().expect("seal");
    let sealed: Vec<SegmentId> = store
        .index
        .segments_snapshot()
        .into_iter()
        .map(|(segment, _)| segment)
        .collect();

    // Now delete every one of them. The tombstones land in the fresh tail,
    // which holds nothing else.
    for byte in 0..200u8 {
        store.delete(&record(7, byte)).expect("delete");
    }
    store.flush().expect("flush");

    let after: Vec<(SegmentId, _)> = store.index.segments_snapshot();
    let fresh: Vec<SegmentId> = after
        .iter()
        .map(|(segment, _)| *segment)
        .filter(|segment| !sealed.contains(segment))
        .collect();
    assert!(
        !fresh.is_empty(),
        "the segment the tombstones landed in has a row of its own"
    );
    for &segment in &fresh {
        let bytes = store.index.segment_bytes(segment);
        assert!(
            bytes.total() > 0,
            "and the row counts the space they hold, not zero"
        );
        // Not yet reclaimable: the records these tombstones shadow are still on
        // disk in the sealed segments, so dropping them would resurrect those.
        assert_eq!(
            bytes.droppable(store.index.min_lsn_excluding(segment)),
            0,
            "tombstones still shadowing live segments are not reclaimable"
        );
    }

    // Seal the tombstones' own segment, so it is a candidate rather than the
    // open tail the compactor skips.
    store.reel.tails()[0].seal().expect("seal");
    store.flush().expect("flush");

    // Compact until nothing more moves. The sealed data segments retire, and
    // once they are gone the tombstones have nothing left to shadow.
    for _ in 0..16 {
        let before = store.compaction_counters();
        store.compact_once().expect("compact");
        let after = store.compaction_counters();
        if after == before {
            break;
        }
    }

    // Nothing at all is left. Ranking on dead alone leaves the tombstone segment
    // at a fraction of zero forever, so it is never chosen.
    let left = store.index.segments_snapshot();
    assert!(
        left.iter().all(|(_, bytes)| bytes.total() == 0),
        "every segment came back, tombstones included, but these were left: {left:?}"
    );
}

// a compaction pass under way turns a second caller away rather than queueing it
#[test]
fn a_running_pass_turns_the_next_caller_away() {
    let script = crate::sync::rendezvous::script();
    script.hold("compaction/retire");

    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let payload = vec![0xa5u8; 8 * 1024];
    for byte in 0..8u8 {
        store.put(&record(7, byte), &payload).expect("put");
    }
    store.reel.tails()[0].seal().expect("seal");
    for byte in 0..8u8 {
        store.put(&record(7, byte), &payload).expect("overwrite");
    }
    store.flush().expect("flush");

    let turned_away = AtomicU64::new(0);
    let is_stopped = AtomicBool::new(false);
    std::thread::scope(|scope| {
        // The winner cannot finish a pass while the point is held, so a Held here
        // is the guard turning a caller away rather than a charged gate.
        let racers: Vec<_> = (0..2)
            .map(|_| {
                let store = &store;
                let turned_away = &turned_away;
                let is_stopped = &is_stopped;
                scope.spawn(move || {
                    while !is_stopped.load(Ordering::Relaxed) {
                        match store.compact_once().expect("compact") {
                            CompactPass::Held => {
                                turned_away.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                            _ => std::thread::sleep(Duration::from_millis(1)),
                        }
                    }
                })
            })
            .collect();
        while turned_away.load(Ordering::Relaxed) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        script.release("compaction/retire");
        is_stopped.store(true, Ordering::Relaxed);
        for racer in racers {
            racer.join().expect("racer");
        }
    });

    assert!(
        turned_away.load(Ordering::Relaxed) > 0,
        "no racer was turned away"
    );
}

fn reopen(sim: &SimIo, config: ReelConfig) -> ReelStore {
    reopen_image(sim.durable_image(), config)
}

fn reopen_image(image: DurableImage, config: ReelConfig) -> ReelStore {
    let restored = SimIo::from_image(image);
    ReelStore::open_with_io(PathBuf::from(ROOT), config, COLUMNS, Arc::new(restored))
        .expect("reopen")
}

// a short value on an unsealed tail is read from its record like any other
#[test]
fn a_short_value_on_the_tail_reads_its_record() {
    let (store, sim) = sim_store(config(1, SyncPolicy::Never));
    store.put(&flag(1), &[7]).expect("put");
    store.put(&flag(2), &[1, 2, 3, 4]).expect("put");

    let before = sim.read_count();
    assert_eq!(store.get(&flag(1)).expect("get"), Some(Value::new(vec![7])));
    assert_eq!(
        store.get(&flag(2)).expect("get"),
        Some(Value::new(vec![1, 2, 3, 4]))
    );
    assert!(sim.read_count() > before, "an unsealed record is read");
}

// a value past the ceiling is read from the volume like any other
#[test]
fn long_value_still_reads() {
    let (store, sim) = sim_store(config(1, SyncPolicy::Never));
    store.put(&flag(1), &[9; 64]).expect("put");

    let before = sim.read_count();
    assert_eq!(
        store.get(&flag(1)).expect("get"),
        Some(Value::new(vec![9; 64]))
    );
    assert!(
        sim.read_count() > before,
        "a value too long to carry is read"
    );
}

// a column that declared no ceiling reads its records however short they are
#[test]
fn plain_column_reads_short_values() {
    let (store, sim) = sim_store(config(1, SyncPolicy::Never));
    store.put(&blob(1), &[3]).expect("put");

    let before = sim.read_count();
    assert_eq!(store.get(&blob(1)).expect("get"), Some(Value::new(vec![3])));
    assert!(sim.read_count() > before);
}

// a value survives a seal and a reopen, read from the record the footer names
#[test]
fn a_value_survives_a_seal() {
    let config = config(1, SyncPolicy::EveryPut);
    let (store, sim) = sim_store(config.clone());
    store.put(&flag(1), &[7, 7]).expect("put");
    store.close().expect("close");

    let restored = SimIo::from_image(sim.durable_image());
    let reopened = ReelStore::open_with_io(
        PathBuf::from(ROOT),
        config,
        COLUMNS,
        Arc::new(restored.clone()),
    )
    .expect("reopen");

    assert_eq!(
        reopened.get(&flag(1)).expect("get"),
        Some(Value::new(vec![7, 7]))
    );
}

// a record reads back exactly, and its size and presence answer from the index
#[test]
fn put_get_roundtrip() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));

    store.put(&record(7, 1), &[0x11; 512]).expect("put");

    assert_eq!(
        store.get(&record(7, 1)).expect("get"),
        Some(Value::new(vec![0x11; 512]))
    );
    assert_eq!(
        store.size_of(&record(7, 1)).expect("read"),
        Some(ByteCount::from_bytes(512))
    );
    assert!(store.contains(&record(7, 1)).expect("read"));
    assert_eq!(store.totals().count, 1);
}

// several keys come back in the order they were asked for, misses included
#[test]
fn get_many_answers_in_order() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    for byte in 1..=4u8 {
        store.put(&record(7, byte), &[byte; 300]).expect("put");
    }

    let asked = [record(7, 3), record(7, 9), record(7, 1), record(7, 4)];
    let answers: Vec<Option<Vec<u8>>> = store
        .get_many(&asked)
        .expect("get many")
        .into_iter()
        .map(|found| found.map(Value::into_vec))
        .collect();

    assert_eq!(
        answers,
        vec![
            Some(vec![3u8; 300]),
            None,
            Some(vec![1u8; 300]),
            Some(vec![4u8; 300]),
        ]
    );
}

// the batch is one submission rather than a read, a wait, and the next read
#[test]
fn get_many_submits_once() {
    let (store, sim) = sim_store(config(1, SyncPolicy::Never));
    let asked: Vec<RecordKey> = (1..=8u8).map(|byte| record(7, byte)).collect();
    for (byte, key) in (1..=8u8).zip(&asked) {
        store.put(key, &[byte; 200]).expect("put");
    }

    // What a loop over the single-key path leaves behind, for the contrast.
    for key in &asked {
        store.get(key).expect("get");
    }
    let looped = sim.read_count();

    store.get_many(&asked).expect("get many");
    let batched = sim.read_count() - looped;

    assert!(
        looped >= asked.len() as u64,
        "the single-key path reads at least once per record"
    );
    assert!(
        batched < asked.len() as u64,
        "the batch read {batched} times for {} records, against {looped} one at a time",
        asked.len()
    );
}

// with no budget the carried tier keeps every write
#[test]
fn carried_values_stay_resident_unarmed() {
    let (store, sim) = carried_store(config(1, SyncPolicy::Never));
    for byte in 1..=4u8 {
        store.put(&carry(byte), &[byte; 200]).expect("put");
    }
    assert_eq!(carried_held(&store), 800, "every capture is resident");

    let before = sim.read_count();
    for byte in 1..=4u8 {
        assert_eq!(
            store.get(&carry(byte)).expect("read").map(Value::into_vec),
            Some(vec![byte; 200])
        );
    }
    assert_eq!(
        sim.read_count(),
        before,
        "every value answered from the index"
    );
}

// an armed budget sheds a write burst back down on the tick
#[test]
fn an_armed_budget_sheds_a_write_burst() {
    let armed = ReelConfig {
        carried_budget: ByteCount::from_bytes(1024),
        ..config(1, SyncPolicy::Never)
    };
    let (store, _sim) = carried_store(armed);
    for byte in 1..=10u8 {
        store.put(&carry(byte), &[byte; 256]).expect("put");
    }
    assert_eq!(
        carried_held(&store),
        2560,
        "captures land ahead of the tick"
    );

    store.maintain_once().expect("tick");

    assert!(
        carried_held(&store) <= 1024,
        "the shed honours the budget, held {}",
        carried_held(&store)
    );
}

// an armed read admits on the second touch, not the first
#[test]
fn an_armed_read_admits_on_the_second_touch() {
    let armed = || ReelConfig {
        carried_budget: ByteCount::from_bytes(64 * 1024),
        ..config(1, SyncPolicy::Never)
    };
    let (store, sim) = carried_store(armed());
    store.put(&carry(7), &[7u8; 200]).expect("put");
    store.flush().expect("flush");
    drop(store);

    // Recovery rebuilds entries and not carried values, so reads warm them.
    let store = ReelStore::open_with_io(
        PathBuf::from(ROOT),
        armed(),
        CARRY_COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("reopen");
    store.get(&carry(7)).expect("read").expect("found");
    assert_eq!(carried_held(&store), 0, "one touch is a ghost, not a value");

    store.get(&carry(7)).expect("read").expect("found");
    assert_eq!(carried_held(&store), 200, "the second touch admits");

    let before = sim.read_count();
    store.get(&carry(7)).expect("read").expect("found");
    assert_eq!(sim.read_count(), before, "the third answers from the index");
}

// a key that keeps answering outlives a bulk load the budget evicts
#[test]
fn a_hot_carried_key_outlives_a_burst() {
    let armed = || ReelConfig {
        carried_budget: ByteCount::from_bytes(600),
        ..config(1, SyncPolicy::Never)
    };
    let (store, sim) = carried_store(armed());
    store.put(&carry(1), &[1u8; 256]).expect("put");
    store.flush().expect("flush");
    drop(store);

    let store = ReelStore::open_with_io(
        PathBuf::from(ROOT),
        armed(),
        CARRY_COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("reopen");
    // Two touches admit, two more bump the countdown to its ceiling.
    for _ in 0..4 {
        store.get(&carry(1)).expect("read").expect("found");
    }
    for byte in 10..=18u8 {
        store.put(&carry(byte), &[byte; 256]).expect("put");
    }

    store.maintain_once().expect("tick");

    assert!(carried_held(&store) <= 600, "the budget holds");
    let before = sim.read_count();
    store.get(&carry(1)).expect("read").expect("found");
    assert_eq!(sim.read_count(), before, "the hot key kept its place");
}

// an armed batch read admits nothing, so a scan cannot displace the tier
#[test]
fn an_armed_batch_read_warms_nothing() {
    let armed = || ReelConfig {
        carried_budget: ByteCount::from_bytes(64 * 1024),
        ..config(1, SyncPolicy::Never)
    };
    let (store, sim) = carried_store(armed());
    store.put(&carry(3), &[3u8; 200]).expect("put");
    store.flush().expect("flush");
    drop(store);

    let store = ReelStore::open_with_io(
        PathBuf::from(ROOT),
        armed(),
        CARRY_COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("reopen");
    let answers = store.get_many(&[carry(3)]).expect("batch");
    assert_eq!(
        answers.into_iter().next().flatten().map(Value::into_vec),
        Some(vec![3u8; 200])
    );
    assert_eq!(
        carried_held(&store),
        0,
        "a bulk read leaves no ghost and no value"
    );
}

// a volume whose disk cannot take the write is refused before it fills
#[test]
fn a_full_volume_refuses_a_foreground_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ReelStore::open(
        dir.path().to_path_buf(),
        config(1, SyncPolicy::Never),
        COLUMNS,
    )
    .expect("open");

    // A real disk has room, so the guard stays out of the way.
    store
        .put(&record(3, 1), &[0xa1; 128])
        .expect("put under the ceiling");

    // Standing where the last tick left it far past any ceiling refuses.
    store.footprint.store(u64::MAX / 2, Ordering::Relaxed);
    let refused = store.put(&record(3, 2), &[0xa2; 128]);

    match refused {
        Err(ReelError::Rejected(why)) => {
            assert!(
                why.contains("compaction"),
                "the reason names what the reserve is for: {why}"
            );
        }
        other => panic!("a full volume took the write: {other:?}"),
    }
}

// a filling volume is slowed across a band before it is refused at the ceiling
#[test]
fn a_filling_volume_is_slowed_before_it_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ReelStore::open(
        dir.path().to_path_buf(),
        config(1, SyncPolicy::Never),
        COLUMNS,
    )
    .expect("open");

    let Some(ceiling) = store.compactor.pressure().foreground_ceiling_bytes() else {
        // Nothing could say how large the disk is, which leaves the model
        // unbounded and both halves of the door open.
        println!("skipped: this filesystem does not report a capacity");
        return;
    };
    let budget = &store.reel.shared().budget;
    let configured = budget.effective_ceiling();

    // Room to spare leaves the budget where the config put it.
    store.apply_footprint(0);
    assert_eq!(budget.effective_ceiling(), configured);

    // Deep in the band the budget is squeezed, and the write still lands:
    // that is the whole difference between this door and the one above.
    store.apply_footprint(ceiling - 4096);
    let squeezed = budget.effective_ceiling();
    assert!(
        squeezed < configured,
        "a volume at its ceiling was never slowed"
    );
    store
        .put(&record(4, 1), &[0xb1; 128])
        .expect("slowed rather than refused");

    // And the band hands the budget back once the space has been freed.
    store.apply_footprint(0);
    assert_eq!(budget.effective_ceiling(), configured);
}

// a real volume records what the machine said, a simulated one has nothing
#[test]
fn a_real_volume_reads_the_machine_and_a_simulated_one_does_not() {
    let dir = tempfile::tempdir().expect("tempdir");
    let real = ReelStore::open(
        dir.path().to_path_buf(),
        config(1, SyncPolicy::Never),
        COLUMNS,
    )
    .expect("open");

    let facts = real.bias().expect("a real volume read the machine");
    assert!(
        facts.volume_capacity_bytes.unwrap_or(0) > 0,
        "a mounted filesystem has a capacity",
    );
    // Whatever the ratio says, the pass reaches a plane rather than nothing.
    let _ = facts.verdict(0).plane;

    let (simulated, _sim) = sim_store(config(1, SyncPolicy::Never));
    assert_eq!(
        simulated.bias(),
        None,
        "a simulated volume has no device to read facts from",
    );
}

// a record larger than the allocation step survives the steps behind it
#[test]
fn a_record_past_the_alloc_chunk_lands_whole_on_posix() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ReelStore::open(
        dir.path().to_path_buf(),
        config(1, SyncPolicy::Never),
        COLUMNS,
    )
    .expect("open");

    // Six small records, then one three times the 16 KiB allocation step.
    for byte in 1..=6u8 {
        store.put(&record(3, byte), &[byte; 400]).expect("put");
    }
    store
        .put(&blob(9), &vec![0xc3u8; 48 * 1024])
        .expect("large put");
    store.put(&record(3, 2), &[0xee; 64]).expect("overwrite");

    let big = store.get(&blob(9)).expect("read").map(Value::into_vec);
    assert_eq!(
        big,
        Some(vec![0xc3u8; 48 * 1024]),
        "the large record lands whole"
    );
    let after = store.get(&record(3, 2)).expect("read").map(Value::into_vec);
    assert_eq!(
        after,
        Some(vec![0xee; 64]),
        "a record behind it lands at all"
    );
}

// a mapped volume answers every read the driver would, byte for byte
#[test]
fn a_mapped_volume_answers_like_the_driver() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mapped = ReelConfig {
        map_above: crate::config::MAP_EVERYTHING,
        ..config(1, SyncPolicy::Never)
    };
    mapped.validate().expect("a buffered volume takes the flag");
    let store = ReelStore::open(dir.path().to_path_buf(), mapped.clone(), COLUMNS).expect("open");

    let big = vec![0xc3u8; 48 * 1024];
    for byte in 1..=6u8 {
        store.put(&record(3, byte), &[byte; 400]).expect("put");
    }
    store.put(&blob(9), &big).expect("large put");
    store.put(&record(3, 2), &[0xee; 64]).expect("overwrite");
    store.delete(&record(3, 5)).expect("delete");

    let read =
        |store: &ReelStore, key: &RecordKey| store.get(key).expect("read").map(Value::into_vec);
    assert_eq!(read(&store, &record(3, 1)), Some(vec![1u8; 400]));
    assert_eq!(read(&store, &record(3, 2)), Some(vec![0xee; 64]));
    assert_eq!(read(&store, &record(3, 5)), None);
    assert_eq!(read(&store, &blob(9)), Some(big.clone()));

    let asked = [record(3, 4), record(3, 5), record(3, 6)];
    let answers: Vec<Option<Vec<u8>>> = store
        .get_many(&asked)
        .expect("get many")
        .into_iter()
        .map(|found| found.map(Value::into_vec))
        .collect();
    assert_eq!(
        answers,
        vec![Some(vec![4u8; 400]), None, Some(vec![6u8; 400])]
    );

    drop(store);
    let reopened = ReelStore::open(dir.path().to_path_buf(), mapped, COLUMNS).expect("reopen");
    assert_eq!(read(&reopened, &record(3, 3)), Some(vec![3u8; 400]));
    assert_eq!(read(&reopened, &blob(9)), Some(big));
    assert_eq!(read(&reopened, &record(3, 5)), None);
}

// the order a batch is asked in does not change what the device is asked for
#[test]
fn a_batch_merges_whatever_order_it_was_asked_in() {
    let reads_for = |asked: &[RecordKey]| {
        let (store, sim) = sim_store(config(1, SyncPolicy::Never));
        for byte in 1..=8u8 {
            store.put(&record(7, byte), &[byte; 200]).expect("put");
        }
        let before = sim.read_count();
        let answers = store.get_many(asked).expect("get many");
        assert!(answers.iter().all(Option::is_some), "every key answers");
        sim.read_count() - before
    };

    let ascending: Vec<RecordKey> = (1..=8u8).map(|byte| record(7, byte)).collect();
    let descending: Vec<RecordKey> = (1..=8u8).rev().map(|byte| record(7, byte)).collect();
    let shuffled: Vec<RecordKey> = [5u8, 1, 8, 3, 7, 2, 6, 4]
        .iter()
        .map(|byte| record(7, *byte))
        .collect();

    let straight = reads_for(&ascending);
    assert_eq!(reads_for(&descending), straight);
    assert_eq!(reads_for(&shuffled), straight);
    assert!(
        straight < 8,
        "eight adjacent records read {straight} times rather than merging"
    );
}

// records written together are read together, however many of them are asked for
#[test]
fn a_contiguous_run_is_one_read() {
    let reads_for = |count: u8| {
        let (store, sim) = sim_store(config(1, SyncPolicy::Never));
        let asked: Vec<RecordKey> = (1..=count).map(|byte| record(7, byte)).collect();
        for (byte, key) in (1..=count).zip(&asked) {
            store.put(key, &[byte; 200]).expect("put");
        }
        store.flush().expect("flush");

        let before = sim.read_count();
        let found = store.get_many(&asked).expect("get many");
        assert!(
            found.iter().all(|answer| answer.is_some()),
            "every record read"
        );
        sim.read_count() - before
    };

    let four = reads_for(4);
    let thirty_two = reads_for(32);

    assert_eq!(
        four, thirty_two,
        "a run of 32 took {thirty_two} reads against {four} for a run of 4"
    );
    assert!(thirty_two < 32, "the run was not merged at all");
}

// an empty ask reaches neither the index nor the device
#[test]
fn get_many_of_nothing() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));

    assert!(store.get_many(&[]).expect("get many").is_empty());
}

// the same key asked for twice answers twice
#[test]
fn get_many_repeats_a_key() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    store.put(&record(7, 5), &[0x55; 128]).expect("put");

    let answers = store
        .get_many(&[record(7, 5), record(7, 5)])
        .expect("get many");

    assert_eq!(
        answers,
        vec![
            Some(Value::new(vec![0x55; 128])),
            Some(Value::new(vec![0x55; 128]))
        ]
    );
}

// a batch answers exactly what the same keys answer one at a time
#[test]
fn get_many_matches_the_loop() {
    let (store, _sim) = sim_store(config(2, SyncPolicy::Never));
    for byte in 1..=16u8 {
        store
            .put(&record(byte as u16, byte), &[byte; 64])
            .expect("put");
    }
    store.delete(&record(4, 4)).expect("delete");

    let asked: Vec<RecordKey> = (1..=20u8).map(|byte| record(byte as u16, byte)).collect();
    let looped: Vec<Option<Vec<u8>>> = asked
        .iter()
        .map(|key| store.get(key).expect("get"))
        .map(|v| v.map(Value::into_vec))
        .collect();

    let batched: Vec<Option<Vec<u8>>> = store
        .get_many(&asked)
        .expect("get many")
        .into_iter()
        .map(|found| found.map(Value::into_vec))
        .collect();
    assert_eq!(batched, looped);
}

// an awaited read answers exactly what the blocking one answers
#[test]
fn async_get_matches_the_block() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    for byte in 1..=8u8 {
        store.put(&record(7, byte), &[byte; 4096]).expect("put");
    }
    store.delete(&record(7, 3)).expect("delete");

    let asked: Vec<RecordKey> = (1..=10u8).map(|byte| record(7, byte)).collect();
    let blocked: Vec<Option<Vec<u8>>> = asked
        .iter()
        .map(|key| store.get(key).expect("get").map(Value::into_vec))
        .collect();

    let awaited = reaping(&store, || {
        let mut answers = Vec::new();
        for key in &asked {
            let found = block_on(store.get_wait(key)).expect("awaited get");
            answers.push(found.map(Value::into_vec));
        }
        answers
    });

    assert_eq!(awaited, blocked);
    assert_eq!(store.driver.outstanding(), 0);
    assert_eq!(store.driver.wakers(), 0);
}

// an awaited batch answers in the order asked, misses included
#[test]
fn async_get_many_in_order() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    for byte in 1..=4u8 {
        store.put(&record(7, byte), &[byte; 300]).expect("put");
    }

    let asked = [record(7, 3), record(7, 9), record(7, 1), record(7, 4)];
    let answers = reaping(&store, || {
        block_on(store.get_many_wait(&asked)).expect("awaited get many")
    });

    let answered: Vec<Option<Vec<u8>>> = answers
        .into_iter()
        .map(|found| found.map(Value::into_vec))
        .collect();
    assert_eq!(
        answered,
        vec![
            Some(vec![3u8; 300]),
            None,
            Some(vec![1u8; 300]),
            Some(vec![4u8; 300]),
        ]
    );
}

// an awaited read dropped mid flight puts its slot and its buffer back
#[test]
fn a_dropped_get_leaks_nothing() {
    // Refused, so the futures stay pending: the premise here is the passive path.
    let script = crate::sync::rendezvous::script();
    script.refuse("slots/self-drain");
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let key = record(7, 1);
    store.put(&key, &[0x33; 4096]).expect("put");
    let reclaimed = store.driver.reclaimed();

    {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut waiting = Box::pin(store.get_wait(&key));
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        assert_eq!(store.driver.outstanding(), 1);
    }

    assert_eq!(store.driver.outstanding(), 1, "the flight keeps its seat");
    store.driver.reap().expect("reap");

    assert_eq!(store.driver.outstanding(), 0);
    assert_eq!(store.driver.reclaimed(), reclaimed + 1);
    assert_eq!(
        store.get(&record(7, 1)).expect("get").map(Value::into_vec),
        Some(vec![0x33; 4096]),
        "the volume still answers after a cancelled read"
    );
}

// an awaited batch dropped mid flight orphans every read it had in flight
#[test]
fn a_dropped_get_many_leaks_nothing() {
    // Refused, so the futures stay pending: the premise here is the passive path.
    let script = crate::sync::rendezvous::script();
    script.refuse("slots/self-drain");
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let asked: Vec<RecordKey> = (1..=4u8).map(|byte| record(7, byte)).collect();
    for (byte, key) in (1..=4u8).zip(&asked) {
        store.put(key, &[byte; 4096]).expect("put");
    }
    let reclaimed = store.driver.reclaimed();

    let flights = {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut waiting = Box::pin(store.get_many_wait(&asked));
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        store.driver.outstanding()
    };

    assert!(flights > 0, "the batch had reads in flight to abandon");
    store.driver.reap().expect("reap");

    assert_eq!(store.driver.outstanding(), 0);
    assert_eq!(store.driver.reclaimed(), reclaimed + flights as u64);
}

// a window of the whole payload is the payload, and every other window is its own bytes
#[test]
fn a_range_matches_the_read() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let key = record(7, 1);
    let payload = stripes(32 * 1024);
    store.put(&key, &payload).expect("put");

    let whole = store
        .get_range(&key, 0, payload.len())
        .expect("range")
        .expect("found");
    let near = store
        .get_range(&key, 64, 128)
        .expect("range")
        .expect("found");
    let deep = store
        .get_range(&key, 20_000, 4_000)
        .expect("range")
        .expect("found");

    assert_eq!(whole, store.get(&key).expect("get").expect("found"));
    assert_eq!(&*near, &payload[64..192]);
    assert_eq!(&*deep, &payload[20_000..24_000]);
}

// a window running off the end answers the bytes that are there
#[test]
fn a_range_past_the_end() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let key = record(7, 1);
    let payload = stripes(4096);
    store.put(&key, &payload).expect("put");

    let found = store
        .get_range(&key, 4000, 4096)
        .expect("range")
        .expect("found");

    assert_eq!(&*found, &payload[4000..]);
}

// a window starting at or past the end answers no bytes rather than nothing
#[test]
fn a_range_at_the_end() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let key = record(7, 1);
    store.put(&key, &stripes(4096)).expect("put");

    let at_end = store
        .get_range(&key, 4096, 16)
        .expect("range")
        .expect("found");
    let past_end = store
        .get_range(&key, 40_960, 16)
        .expect("range")
        .expect("found");

    assert!(at_end.is_empty());
    assert!(past_end.is_empty());
}

// a key the volume does not hold has no window either
#[test]
fn a_missing_key_ranges() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    store.put(&record(7, 1), &stripes(4096)).expect("put");

    assert!(store
        .get_range(&record(7, 2), 0, 16)
        .expect("range")
        .is_none());
}

// a window of a short value is cut from the record, since no entry holds it now
#[test]
fn a_short_range_reads_its_record() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    store.put(&flag(1), &[1, 2, 3, 4]).expect("put");

    let found = store
        .get_range(&flag(1), 1, 2)
        .expect("range")
        .expect("found");

    assert_eq!(&*found, &[2, 3]);
}

// a carried value is cut from the bytes the index is holding
#[test]
fn a_carried_range() {
    let (store, sim) = carried_store(config(1, SyncPolicy::Never));
    let payload = stripes(200);
    store.put(&carry(3), &payload).expect("put");
    let before = sim.read_count();

    let found = store
        .get_range(&carry(3), 8, 16)
        .expect("range")
        .expect("found");

    assert_eq!(&*found, &payload[8..24]);
    assert_eq!(sim.read_count(), before, "the tier answered it");
}

// a column holding what a codec produced answers a window of the decoded payload
#[test]
fn a_coded_column_answers_a_window() {
    let (store, _sim) = coded_store(config(1, SyncPolicy::Never));
    let payload = stripes(4096);
    store.put(&coded(1), &payload).expect("put");

    for (at, len) in [(0u64, 16usize), (1_000, 512), (4_090, 64), (4_096, 8)] {
        let window = payload[at as usize..(at as usize + len).min(payload.len())].to_vec();
        let blocked = store.get_range(&coded(1), at, len).expect("range");
        let awaited = block_on(store.get_range_wait(&coded(1), at, len)).expect("awaited range");

        assert_eq!(blocked.map(|value| value.into_vec()), Some(window.clone()), "at {at}");
        assert_eq!(awaited.map(|value| value.into_vec()), Some(window), "awaited at {at}");
    }

    assert!(
        store.get(&coded(1)).expect("get").is_some(),
        "the whole read still serves it"
    );
}

// both doors answer the same windows of the same record
#[test]
fn async_range_matches_the_block() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let key = record(7, 1);
    let payload = stripes(32 * 1024);
    store.put(&key, &payload).expect("put");
    let windows = [
        (0u64, 4096usize),
        (64, 128),
        (20_000, 4_000),
        (31_000, 8_000),
        (32_768, 16),
    ];

    let mut blocked = Vec::new();
    for (at, len) in windows {
        let found = store
            .get_range(&key, at, len)
            .expect("range")
            .expect("found");
        blocked.push(found.to_vec());
    }
    let awaited = reaping(&store, || {
        let mut answers = Vec::new();
        for (at, len) in windows {
            let found = block_on(store.get_range_wait(&key, at, len))
                .expect("awaited range")
                .expect("found");
            answers.push(found.to_vec());
        }
        answers
    });

    assert_eq!(awaited, blocked);
    for ((at, len), answer) in windows.iter().zip(&blocked) {
        let at = (*at as usize).min(payload.len());
        let end = at.saturating_add(*len).min(payload.len());
        assert_eq!(answer.as_slice(), &payload[at..end], "window at {at}");
    }
    assert_eq!(store.driver.outstanding(), 0);
    assert_eq!(store.driver.wakers(), 0);
}

// an awaited window dropped mid flight puts its slots and its buffers back
#[test]
fn a_dropped_range_leaks_nothing() {
    // Refused, so the futures stay pending: the premise here is the passive path.
    let script = crate::sync::rendezvous::script();
    script.refuse("slots/self-drain");
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let key = record(7, 1);
    let payload = stripes(32 * 1024);
    store.put(&key, &payload).expect("put");
    let reclaimed = store.driver.reclaimed();

    let flights = {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut waiting = Box::pin(store.get_range_wait(&key, 20_000, 4_000));
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        store.driver.outstanding()
    };

    assert_eq!(flights, 1, "a vouched window has only its bytes in flight");
    store.driver.reap().expect("reap");

    assert_eq!(store.driver.outstanding(), 0);
    assert_eq!(store.driver.reclaimed(), reclaimed + flights as u64);
    let found = store
        .get_range(&key, 20_000, 8)
        .expect("range")
        .expect("found");
    assert_eq!(
        &*found,
        &payload[20_000..20_008],
        "the volume still answers"
    );
}

// a vouched window takes one device read wherever it sits in the record
#[test]
fn a_posix_range_reads_a_window() {
    let (store, backend, _dir) = posix_store(config(1, SyncPolicy::Never));
    let key = record(7, 1);
    let payload = stripes(64 * 1024);
    store.put(&key, &payload).expect("put");
    store.get(&key).expect("warm the descriptor");

    let before = backend.ops();
    let near = store
        .get_range(&key, 128, 256)
        .expect("range")
        .expect("found");
    let after_near = backend.ops();
    let deep = store
        .get_range(&key, 40_000, 4_000)
        .expect("range")
        .expect("found");
    let after_deep = backend.ops();

    assert_eq!(&*near, &payload[128..384]);
    assert_eq!(&*deep, &payload[40_000..44_000]);
    assert_eq!(
        after_near - before,
        1,
        "the near window took one read and no echo"
    );
    assert_eq!(
        after_deep - after_near,
        1,
        "the deep window took one read and no echo"
    );
}

// over a real file both doors answer the same windows
#[test]
fn a_posix_range_both_doors() {
    let (store, _backend, _dir) = posix_store(config(1, SyncPolicy::Never));
    let key = record(7, 1);
    let payload = stripes(64 * 1024);
    store.put(&key, &payload).expect("put");

    for (at, len) in [
        (0u64, 64usize),
        (4096, 4096),
        (40_000, 4_000),
        (65_000, 4_000),
    ] {
        let blocked = store
            .get_range(&key, at, len)
            .expect("range")
            .expect("found");
        let awaited = block_on(store.get_range_wait(&key, at, len))
            .expect("awaited range")
            .expect("found");

        let at = at as usize;
        let end = at.saturating_add(len).min(payload.len());
        assert_eq!(&*blocked, &payload[at..end], "window at {at}");
        assert_eq!(
            blocked, awaited,
            "the doors disagree about the window at {at}"
        );
    }
}

// a mapped volume cuts the window out of the mapping and asks the driver for nothing
#[test]
fn a_mapped_range_asks_nothing() {
    let mapped = ReelConfig {
        map_above: crate::config::MAP_EVERYTHING,
        ..config(1, SyncPolicy::Never)
    };
    let (store, backend, _dir) = posix_store(mapped);
    let key = record(7, 1);
    let payload = stripes(12 * 1024);
    store.put(&key, &payload).expect("put");
    store.get(&key).expect("warm the mapping");

    let before = backend.ops();
    let found = store
        .get_range(&key, 8_000, 1_000)
        .expect("range")
        .expect("found");
    let after = backend.ops();

    assert_eq!(&*found, &payload[8_000..9_000]);
    assert_eq!(after, before, "the mapping served the window");
}

// an entry compaction moved is refused by its stamp and the read converges
#[test]
fn a_moved_range_converges() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let key = record(7, 1);
    let payload = stripes(32 * 1024);
    store.put(&key, &payload).expect("put");
    let stale = store.index.get(&key).expect("read").expect("present");
    assert_eq!(stale.loc.segment, SegmentId(1));
    assert!(
        store.window_certain(&stale),
        "a standing segment vouches for its entry"
    );

    store.reel.tails()[0].seal().expect("seal");
    store
        .put(&record(7, 2), &[0x22; 512])
        .expect("roll the tail");
    store
        .compactor
        .compact_segment(&store.reel, &store.index, SegmentId(1))
        .expect("compact");

    assert!(
        !store.window_certain(&stale),
        "a retired segment vouches for nothing"
    );
    let moved = store.index.get(&key).expect("read").expect("still live");
    assert_ne!(
        moved.loc.segment,
        SegmentId(1),
        "compaction moved the record"
    );
    assert!(
        store.window_certain(&moved),
        "the copy's entry is vouched for"
    );

    let found = store
        .get_range(&key, 20_000, 4_000)
        .expect("range")
        .expect("found");
    let awaited = reaping(&store, || {
        block_on(store.get_range_wait(&key, 20_000, 4_000)).expect("awaited range")
    })
    .expect("found");
    assert_eq!(&*found, &payload[20_000..24_000]);
    assert_eq!(found, awaited);
}

// a new record at an old offset is never served as the record the entry named
#[test]
fn a_reused_offset_serves_nobody() {
    use std::os::unix::fs::FileExt;

    let (store, _backend, dir) = posix_store(config(1, SyncPolicy::Never));
    let key = blob(1);
    store.put(&key, &stripes(4096)).expect("put");
    store.flush().expect("flush");
    let entry = store.index.get(&key).expect("read").expect("present");

    // A second volume lays out an identical segment holding another record,
    // which is what the offset would hold if the space were ever reused.
    let (other, _other_backend, other_dir) = posix_store(config(1, SyncPolicy::Never));
    let reused = vec![0xb7u8; 4096];
    other.put(&blob(2), &reused).expect("put");
    other.flush().expect("flush");
    let planted = other.index.get(&blob(2)).expect("read").expect("present");
    assert_eq!(
        planted.loc, entry.loc,
        "the two volumes lay the record out alike"
    );

    let span = HEADER_LEN + blob(2).width() as usize + reused.len();
    let mut framed = vec![0u8; span];
    let source = std::fs::File::open(
        other_dir
            .path()
            .join(segment_file_name(planted.loc.segment)),
    )
    .expect("open source");
    source
        .read_exact_at(&mut framed, u64::from(planted.loc.offset))
        .expect("read framed");
    // Reuse begins by dropping the incarnation, which is the contract the
    // fast path stands on, and only then do the bytes change under the entry.
    store.index.segments().forget(entry.loc.segment);
    let target = std::fs::OpenOptions::new()
        .write(true)
        .open(dir.path().join(segment_file_name(entry.loc.segment)))
        .expect("open target");
    target
        .write_all_at(&framed, u64::from(entry.loc.offset))
        .expect("plant record");

    assert!(
        !store.window_certain(&entry),
        "a dropped incarnation vouches for nothing"
    );
    let found = store.get_range(&key, 1000, 100).expect("range");
    assert!(
        found.is_none(),
        "the planted record never answers as the old one"
    );
}

/// A volume whose segments can hold a record above the routing floor
///
/// The shared config's segment is a megabyte, which is the routing floor, so a
/// record large enough to be routed would not fit in one.
fn routed_config(ranged: RangedReads) -> ReelConfig {
    ReelConfig {
        ranged_reads: ranged,
        segment_bytes: ByteCount::mb(16),
        ..config(1, SyncPolicy::Never)
    }
}

/// A record above the megabyte size floor the route asks for
const ROUTED_RECORD_LEN: usize = 2 * 1024 * 1024;

/// Put a volume under the read pressure the depth floor asks for
///
/// A window goes around the page cache only where a large record meets
/// concurrent readers, and a single-threaded test supplies neither on its own.
fn under_read_pressure(store: &ReelStore) {
    for _ in 0..2 {
        store.reel.hold_cold_read_open_ended();
    }
}

/// Whether the route can be taken here, so a skip is nobody's silent green
///
/// A filesystem that refuses the direct open retires the plane, and a plane test
/// on one asserts nothing. Setting REEL_DIRECT_REQUIRED makes that a failure.
fn cold_plane_or_skip(store: &ReelStore) -> bool {
    if store.cold_direct_live() {
        return true;
    }
    assert!(
        std::env::var_os("REEL_DIRECT_REQUIRED").is_none(),
        "the cold read plane retired on this filesystem and REEL_DIRECT_REQUIRED is set",
    );
    println!("skipped: this filesystem refused a direct open, so the cold plane retired");
    false
}

/// Where a window into one record starts in its segment file
fn window_offset(store: &ReelStore, key: &RecordKey, at: u64) -> u64 {
    let entry = store.index.get(key).expect("read").expect("present");
    u64::from(entry.loc.offset) + HEADER_LEN as u64 + u64::from(key.width()) + at
}

/// A sealed volume holding one record above the routing floor, under pressure
///
/// A window read from a live tail routes to the cache by the settled check, and
/// the depth floor sends a lone reader there whatever the record size.
fn routed_store(
    ranged: RangedReads,
    len: usize,
) -> (ReelStore, Arc<PosixBackend>, TempDir, RecordKey, Vec<u8>) {
    let (store, backend, dir) = posix_store(routed_config(ranged));
    let key = record(7, 1);
    let payload = stripes(len);
    store.put(&key, &payload).expect("put");
    store.reel.tails()[0].seal().expect("seal");
    store.get(&key).expect("warm the descriptor");
    under_read_pressure(&store);
    (store, backend, dir, key, payload)
}

// a window on a sealed segment reads around the page cache in one driver op
#[test]
fn a_direct_window_takes_one_read() {
    let (store, backend, _dir, key, payload) = routed_store(RangedReads::Direct, ROUTED_RECORD_LEN);
    // The first routed window opens the direct descriptor, which is a driver op
    // of its own. Measuring past it is measuring the read.
    store
        .get_range(&key, 100, 4)
        .expect("range")
        .expect("found");
    if !cold_plane_or_skip(&store) {
        return;
    }

    let ops = backend.ops();
    let before = backend.cold_reads();
    let found = store
        .get_range(&key, 40_000, 4_000)
        .expect("range")
        .expect("found");
    let after = backend.cold_reads();

    assert_eq!(&*found, &payload[40_000..44_000]);
    assert_eq!(backend.ops() - ops, 1, "the window took one op");
    assert_eq!(after.routed - before.routed, 1, "the window was routed");
    assert_eq!(
        after.direct - before.direct,
        1,
        "and answered on the device plane"
    );
    assert_eq!(
        after.warm - before.warm,
        0,
        "with no probe under the direct knob"
    );
}

// a routed window asks the device for exactly the blocks that cover it
#[test]
fn a_direct_window_reads_one_covering_span() {
    let (store, backend, _dir, key, _payload) =
        routed_store(RangedReads::Direct, ROUTED_RECORD_LEN);
    store
        .get_range(&key, 100, 4)
        .expect("range")
        .expect("found");
    if !cold_plane_or_skip(&store) {
        return;
    }

    let base = window_offset(&store, &key, 0);
    let align = DIRECT_ALIGN as u64;
    // One window sits inside a single block, the next straddles two.
    let inside = (align - base % align) % align;
    let across = inside + align - 1;

    for (at, len, want) in [(inside, 4_000usize, align), (across, 2usize, 2 * align)] {
        let before = backend.cold_reads();
        store
            .get_range(&key, at, len)
            .expect("range")
            .expect("found");
        let after = backend.cold_reads();

        let (_, span) = covering_span(window_offset(&store, &key, at), len as u64);
        assert_eq!(
            span, want,
            "the span for {len} bytes at {at} is not what was set up"
        );
        assert_eq!(
            after.direct - before.direct,
            1,
            "the window at {at} took one read"
        );
        assert_eq!(
            after.direct_bytes - before.direct_bytes,
            span,
            "the window at {at} asked the device for more than its covering span",
        );
    }
}

// the routed window and the buffered one answer the same bytes
#[test]
fn a_direct_window_matches_the_buffered_one() {
    let (routed, _backend, _dir, key, payload) =
        routed_store(RangedReads::Direct, ROUTED_RECORD_LEN);
    let (cached, _cached_backend, _cached_dir) = posix_store(routed_config(RangedReads::Cached));
    cached.put(&key, &payload).expect("put");
    cached.reel.tails()[0].seal().expect("seal");

    for (at, len) in [
        (0u64, 64usize),
        (4096, 4096),
        (40_000, 4_000),
        (199_000, 4_000),
    ] {
        let blocked = routed
            .get_range(&key, at, len)
            .expect("range")
            .expect("found");
        let awaited = block_on(routed.get_range_wait(&key, at, len))
            .expect("awaited range")
            .expect("found");
        let buffered = cached
            .get_range(&key, at, len)
            .expect("range")
            .expect("found");

        let at = at as usize;
        let end = at.saturating_add(len).min(payload.len());
        assert_eq!(&*blocked, &payload[at..end], "the routed window at {at}");
        assert_eq!(
            blocked, awaited,
            "the doors disagree about the window at {at}"
        );
        assert_eq!(
            blocked, buffered,
            "the planes disagree about the window at {at}"
        );
    }
}

// a record below the floor keeps the page cache, whatever the knob says
#[test]
fn a_small_record_window_keeps_the_cache() {
    let (store, backend, _dir, key, payload) = routed_store(RangedReads::Direct, 8 * 1024);
    let found = store
        .get_range(&key, 4_000, 1_000)
        .expect("range")
        .expect("found");

    assert_eq!(&*found, &payload[4_000..5_000]);
    assert_eq!(backend.cold_reads().routed, 0, "a small record was routed");
}

// a record wider than a segment is refused, not retried for ever
#[test]
fn a_record_wider_than_a_segment_is_refused() {
    // The shared fixture's segment is a megabyte.
    let (store, _backend, _dir) = posix_store(config(1, SyncPolicy::Never));
    let oversize = stripes(2 * 1024 * 1024);

    let refused = store
        .put(&record(7, 1), &oversize)
        .expect_err("an oversize record was taken");
    assert!(
        matches!(refused, ReelError::Rejected(_)),
        "an oversize record failed as {refused} rather than as a rejection",
    );

    // The volume is still usable, which is what says the refusal happened before
    // anything was reserved rather than after a segment was spent on it.
    let small = stripes(1024);
    store
        .put(&record(7, 2), &small)
        .expect("put after a refusal");
    let found = store.get(&record(7, 2)).expect("read").expect("present");
    assert_eq!(&*found, small.as_slice());
}

// a large record read by one reader keeps the page cache
#[test]
fn a_lone_reader_keeps_the_cache_on_a_large_record() {
    let (store, backend, _dir) = posix_store(routed_config(RangedReads::Direct));
    let key = record(7, 1);
    let payload = stripes(ROUTED_RECORD_LEN);
    store.put(&key, &payload).expect("put");
    store.reel.tails()[0].seal().expect("seal");
    store.get(&key).expect("warm the descriptor");

    let found = store
        .get_range(&key, 40_000, 4_000)
        .expect("range")
        .expect("found");
    assert_eq!(&*found, &payload[40_000..44_000]);
    assert_eq!(backend.cold_reads().routed, 0, "a lone reader was routed");

    // And the same read under pressure is, which is what says the first
    // assertion is the depth floor rather than something else refusing.
    under_read_pressure(&store);
    store
        .get_range(&key, 40_000, 4_000)
        .expect("range")
        .expect("found");
    if !cold_plane_or_skip(&store) {
        return;
    }
    assert_eq!(
        backend.cold_reads().routed,
        1,
        "pressure did not route the same read"
    );
}

// a window on a segment something still holds keeps the page cache
#[test]
fn a_held_window_keeps_the_cache() {
    let (store, backend, _dir) = posix_store(routed_config(RangedReads::Direct));
    let key = record(7, 1);
    let payload = stripes(ROUTED_RECORD_LEN);
    store.put(&key, &payload).expect("put");
    store.get(&key).expect("warm the descriptor");
    under_read_pressure(&store);

    let held = store
        .get_range(&key, 40_000, 4_000)
        .expect("range")
        .expect("found");
    assert_eq!(&*held, &payload[40_000..44_000]);
    assert_eq!(
        backend.cold_reads().routed,
        0,
        "a window on a live tail was routed"
    );

    store.reel.tails()[0].seal().expect("seal");
    let sealed = store
        .get_range(&key, 40_000, 4_000)
        .expect("range")
        .expect("found");
    if !cold_plane_or_skip(&store) {
        return;
    }

    assert_eq!(&*sealed, &payload[40_000..44_000]);
    assert_eq!(
        backend.cold_reads().routed,
        1,
        "the sealed segment never routed"
    );
}

// a sealed segment beside an idle tail still routes
#[test]
fn a_settled_window_routes_beside_an_idle_tail() {
    let config = ReelConfig {
        active_tails: ThreadBudget::threads(2),
        ..routed_config(RangedReads::Direct)
    };
    let (store, backend, _dir) = posix_store(config);
    // Roll off the segment the tail opened with, so the record lands in one drawn
    // after open, which is every segment a running volume writes.
    store.put(&record(7, 2), &stripes(1024)).expect("put");
    store.reel.tails()[0].seal().expect("seal");

    let key = record(7, 1);
    let payload = stripes(ROUTED_RECORD_LEN);
    store.put(&key, &payload).expect("put");
    store.reel.tails()[0].seal().expect("seal");
    store.get(&key).expect("warm the descriptor");
    under_read_pressure(&store);

    let sealed = store
        .get_range(&key, 40_000, 4_000)
        .expect("range")
        .expect("found");
    if !cold_plane_or_skip(&store) {
        return;
    }
    assert_eq!(&*sealed, &payload[40_000..44_000]);
    assert_eq!(
        backend.cold_reads().routed,
        1,
        "the sealed segment never routed"
    );
}

// a new record at an old offset is never served as the old one, route on
#[test]
fn a_direct_reused_offset_serves_nobody() {
    use std::os::unix::fs::FileExt;

    let (store, backend, dir) = posix_store(routed_config(RangedReads::Direct));
    let key = blob(1);
    let payload = stripes(ROUTED_RECORD_LEN);
    store.put(&key, &payload).expect("put");
    store.reel.tails()[0].seal().expect("seal");
    under_read_pressure(&store);
    let entry = store.index.get(&key).expect("read").expect("present");

    let served = store
        .get_range(&key, 40_000, 4_000)
        .expect("range")
        .expect("found");
    assert_eq!(&*served, &payload[40_000..44_000]);
    if !cold_plane_or_skip(&store) {
        return;
    }
    assert!(
        backend.cold_reads().direct >= 1,
        "the route never reached the device"
    );

    // A second volume lays out an identical segment holding another record,
    // which is what the offset would hold if the space were ever reused.
    let (other, _other_backend, other_dir) = posix_store(routed_config(RangedReads::Direct));
    let reused = vec![0xb7u8; payload.len()];
    other.put(&blob(2), &reused).expect("put");
    other.reel.tails()[0].seal().expect("seal");
    let planted = other.index.get(&blob(2)).expect("read").expect("present");
    assert_eq!(
        planted.loc, entry.loc,
        "the two volumes lay the record out alike"
    );

    let span = HEADER_LEN + blob(2).width() as usize + reused.len();
    let mut framed = vec![0u8; span];
    let source = std::fs::File::open(
        other_dir
            .path()
            .join(segment_file_name(planted.loc.segment)),
    )
    .expect("open source");
    source
        .read_exact_at(&mut framed, u64::from(planted.loc.offset))
        .expect("read framed");
    // Reuse begins by dropping the incarnation, which is the contract the fast
    // path stands on, and only then do the bytes change under the entry.
    store.index.segments().forget(entry.loc.segment);
    let target = std::fs::OpenOptions::new()
        .write(true)
        .open(dir.path().join(segment_file_name(entry.loc.segment)))
        .expect("open target");
    target
        .write_all_at(&framed, u64::from(entry.loc.offset))
        .expect("plant record");

    assert!(
        !store.window_certain(&entry),
        "a dropped incarnation vouches for nothing"
    );
    let found = store.get_range(&key, 40_000, 4_000).expect("range");
    assert!(
        found.is_none(),
        "the planted record answered as the old one"
    );
}

// the awaited door routes its windows too, and takes the same one read
#[test]
fn an_awaited_direct_window_takes_one_read() {
    let (store, backend, _dir, key, payload) = routed_store(RangedReads::Direct, ROUTED_RECORD_LEN);
    // The first routed window opens the direct descriptor, which is a driver op
    // of its own. Measuring past it is measuring the read.
    block_on(store.get_range_wait(&key, 100, 4))
        .expect("range")
        .expect("found");
    if !cold_plane_or_skip(&store) {
        return;
    }

    let ops = backend.ops();
    let before = backend.cold_reads();
    let found = block_on(store.get_range_wait(&key, 40_000, 4_000))
        .expect("awaited range")
        .expect("found");
    let after = backend.cold_reads();

    assert_eq!(&*found, &payload[40_000..44_000]);
    assert_eq!(backend.ops() - ops, 1, "the awaited window took one op");
    assert_eq!(
        after.routed - before.routed,
        1,
        "the awaited window was not routed"
    );
    assert_eq!(
        after.direct - before.direct,
        1,
        "the awaited window kept the cache"
    );
}

// a window wider than the staging cap still takes one read and answers whole
#[test]
fn a_wide_direct_window_takes_one_read() {
    let (store, backend, _dir, key, payload) = routed_store(RangedReads::Direct, ROUTED_RECORD_LEN);
    store
        .get_range(&key, 100, 4)
        .expect("range")
        .expect("found");
    if !cold_plane_or_skip(&store) {
        return;
    }

    // Off a block boundary, so the span is wider than the window on both ends.
    let at = 4_095u64;
    let len = 192 * 1024;
    let ops = backend.ops();
    let before = backend.cold_reads();
    let found = store
        .get_range(&key, at, len)
        .expect("range")
        .expect("found");
    let after = backend.cold_reads();

    let (_, span) = covering_span(window_offset(&store, &key, at), len as u64);
    assert_eq!(&*found, &payload[at as usize..at as usize + len]);
    assert_eq!(backend.ops() - ops, 1, "the wide window took one op");
    assert_eq!(
        after.direct - before.direct,
        1,
        "the wide window kept the cache"
    );
    assert_eq!(
        after.direct_bytes - before.direct_bytes,
        span,
        "the wide window asked the device for more than its covering span",
    );
}

// a run of windows takes one device read each and asks for exactly its blocks
#[test]
fn a_run_of_direct_windows_keeps_its_blocks_per_read() {
    let (store, backend, _dir, key, payload) = routed_store(RangedReads::Direct, ROUTED_RECORD_LEN);
    store
        .get_range(&key, 100, 4)
        .expect("range")
        .expect("found");
    if !cold_plane_or_skip(&store) {
        return;
    }

    let end = payload.len() as u64;
    let asked = 4_000u64;
    // A prime stride, so the run lands on every offset within a block rather
    // than on a handful of them.
    let mut offsets: Vec<u64> = (0..200)
        .map(|step| (step * 2_609) % (end - asked))
        .collect();
    offsets.extend([end - 1, end - 3, end - 4_096]);

    // Resolved once, since asking the index inside the run would count against
    // the ops the run is measured by.
    let base = window_offset(&store, &key, 0);
    let spans: u64 = offsets
        .iter()
        .map(|at| covering_span(base + at, (at + asked).min(end) - at).1)
        .sum();

    let ops = backend.ops();
    let before = backend.cold_reads();
    for at in &offsets {
        let at = *at;
        let found = store
            .get_range(&key, at, asked as usize)
            .expect("range")
            .unwrap_or_else(|| panic!("the window at {at} went missing"));
        let want = &payload[at as usize..(at + asked).min(end) as usize];
        assert_eq!(&*found, want, "the window at {at}");
    }
    let after = backend.cold_reads();

    let windows = offsets.len() as u64;
    assert_eq!(
        after.direct - before.direct,
        windows,
        "a window left the device plane"
    );
    assert_eq!(backend.ops() - ops, windows, "a window took two ops");
    assert_eq!(
        after.direct_bytes - before.direct_bytes,
        spans,
        "the run asked the device for more than its covering spans",
    );
}

// a segment whose direct open is refused reads buffered, and no other segment does
#[test]
fn a_refused_direct_open_reads_buffered() {
    let (store, backend, dir, key, payload) = routed_store(RangedReads::Direct, ROUTED_RECORD_LEN);
    // A second record on a segment of its own, so what a refusal costs shows.
    let other = record(7, 2);
    let other_payload: Vec<u8> = stripes(ROUTED_RECORD_LEN)
        .into_iter()
        .map(|byte| byte ^ 0x5a)
        .collect();
    store.put(&other, &other_payload).expect("put");
    store.reel.tails()[0].seal().expect("seal");
    store.get(&other).expect("warm the descriptor");

    let entry = store.index.get(&key).expect("read").expect("present");
    let standing = store.index.get(&other).expect("read").expect("present");
    assert_ne!(
        entry.loc.segment, standing.loc.segment,
        "the two share a segment"
    );
    // The standing segment takes its descriptor before anything is unlinked, so a
    // filesystem with no direct open skips rather than faking the refusal.
    store
        .get_range(&other, 100, 4)
        .expect("range")
        .expect("found");
    if !cold_plane_or_skip(&store) {
        return;
    }
    // The buffered descriptor is already open and keeps serving, so the only
    // thing the unlink takes away is the open the route has not made yet.
    std::fs::remove_file(dir.path().join(segment_file_name(entry.loc.segment)))
        .expect("unlink the segment");

    let ops = backend.ops();
    let before = backend.cold_reads();
    let found = store
        .get_range(&key, 40_000, 4_000)
        .expect("range")
        .expect("found");
    let refused = backend.ops() - ops;
    let after = backend.cold_reads();

    assert_eq!(
        &*found,
        &payload[40_000..44_000],
        "the buffered descriptor stopped serving"
    );
    assert_eq!(
        refused, 2,
        "the refused window was not one failed open and one buffered read"
    );
    assert_eq!(
        after.routed - before.routed,
        0,
        "a refused open still routed"
    );
    assert_eq!(
        after.direct - before.direct,
        0,
        "a refused open still reached the device"
    );
    assert!(
        store.cold_direct_live(),
        "one refused open retired the whole volume"
    );

    // The segment's own memo is what keeps the refusal from being bought again.
    let ops = backend.ops();
    store
        .get_range(&key, 60_000, 4_000)
        .expect("range")
        .expect("found");
    assert_eq!(
        backend.ops() - ops,
        1,
        "the second window on the refused segment asked for the open again",
    );

    let before = backend.cold_reads();
    let served = store
        .get_range(&other, 40_000, 4_000)
        .expect("range")
        .expect("found");
    let after = backend.cold_reads();

    assert_eq!(&*served, &other_payload[40_000..44_000]);
    assert_eq!(
        after.routed - before.routed,
        1,
        "the standing segment went unrouted"
    );
    assert_eq!(
        after.direct - before.direct,
        1,
        "the standing segment lost the device"
    );
}

// the direct descriptor is opened once per segment, whoever reaches it first
#[test]
fn racing_direct_windows_open_one_descriptor() {
    let (store, backend, _dir, key, payload) = routed_store(RangedReads::Direct, ROUTED_RECORD_LEN);
    let settled = backend.open_file_count();

    let readers = 32;
    let barrier = std::sync::Barrier::new(readers);
    let ready = &barrier;
    let racing = &store;
    let asked = &key;
    let want = &payload;
    std::thread::scope(|scope| {
        for reader in 0..readers {
            scope.spawn(move || {
                ready.wait();
                let at = 40_000 + (reader as u64 % 8);
                let found = racing
                    .get_range(asked, at, 4_000)
                    .expect("range")
                    .expect("found");
                let at = at as usize;
                assert_eq!(&*found, &want[at..at + 4_000], "a racing window at {at}");
            });
        }
    });
    if !cold_plane_or_skip(&store) {
        return;
    }

    assert_eq!(
        backend.cold_reads().routed,
        readers as u64,
        "a window went unrouted"
    );
    assert_eq!(
        backend.open_file_count(),
        settled + 1,
        "{readers} racing windows opened more than one direct descriptor",
    );
}

/// Whether the non-blocking probe survives on this filesystem
///
/// RWF_NOWAIT is per filesystem, not per kernel, and a refusal retires the plane
/// and leaves a probe test with nothing to assert. REEL_DIRECT_REQUIRED makes
/// the skip a failure.
#[cfg(target_os = "linux")]
fn warm_probe_or_skip(backend: &PosixBackend) -> bool {
    if backend.cold_plane_live() {
        return true;
    }
    assert!(
        std::env::var_os("REEL_DIRECT_REQUIRED").is_none(),
        "this filesystem refused RWF_NOWAIT and REEL_DIRECT_REQUIRED is set",
    );
    println!("skipped: this filesystem refused RWF_NOWAIT, so the probe retired");
    false
}

// a window the cache already holds is answered without a device read
#[cfg(target_os = "linux")]
#[test]
fn a_warm_window_skips_the_device() {
    let (store, backend, _dir, key, payload) = routed_store(RangedReads::Probed, ROUTED_RECORD_LEN);
    // Settle the direct descriptor before anything is measured.
    store
        .get_range(&key, 100, 4)
        .expect("range")
        .expect("found");
    if !cold_plane_or_skip(&store) {
        return;
    }
    // A whole read leaves the record's pages resident, which is what the probe
    // is supposed to find.
    store.get(&key).expect("populate the cache");

    let ops = backend.ops();
    let before = backend.cold_reads();
    let found = store
        .get_range(&key, 40_000, 4_000)
        .expect("range")
        .expect("found");
    let after = backend.cold_reads();

    assert_eq!(&*found, &payload[40_000..44_000]);
    assert_eq!(after.routed - before.routed, 1, "the window was not routed");
    assert_eq!(backend.ops() - ops, 1, "the window took one op");
    if !warm_probe_or_skip(&backend) {
        return;
    }
    assert_eq!(
        after.warm - before.warm,
        1,
        "the cache did not answer a resident window"
    );
    assert_eq!(
        after.direct - before.direct,
        0,
        "a resident window still hit the device"
    );
}

// a cold window on a real filesystem asks for its covering span and no more
#[cfg(target_os = "linux")]
#[test]
fn a_cold_window_reads_its_range_and_no_more() {
    let (store, backend, _dir, key, payload) = routed_store(RangedReads::Direct, ROUTED_RECORD_LEN);
    store
        .get_range(&key, 100, 4)
        .expect("range")
        .expect("found");
    if !cold_plane_or_skip(&store) {
        return;
    }
    drop_cache(&store, &key);

    let before = backend.cold_reads();
    let found = store
        .get_range(&key, 40_000, 4_000)
        .expect("range")
        .expect("found");
    let after = backend.cold_reads();
    let bytes = after.direct_bytes - before.direct_bytes;

    assert_eq!(&*found, &payload[40_000..44_000]);
    assert_eq!(
        after.direct - before.direct,
        1,
        "a cold window did not reach the device"
    );
    assert!(
        (4_000..=4_000 + 2 * DIRECT_ALIGN as u64).contains(&bytes),
        "a four kilobyte window asked the device for {bytes} bytes",
    );
}

/// Drop a segment's pages, so a read of it is genuinely cold
///
/// posix_fadvise needs no privilege, so a test can make a cold read as itself.
#[cfg(target_os = "linux")]
fn drop_cache(store: &ReelStore, key: &RecordKey) {
    use std::os::fd::AsRawFd;

    let entry = store.index.get(key).expect("read").expect("present");
    let path = store.reel.shared().segment_path(entry.loc.segment);
    let file = std::fs::File::open(&path).expect("open the segment");
    // SAFETY: the descriptor is open for the whole call and fadvise takes no
    // pointer, so the only thing it can touch is the kernel's page state.
    let ret = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    assert_eq!(ret, 0, "dropping the segment's pages failed");
}

// a backend that services ops in place answers the future at its first poll
#[test]
fn a_posix_read_answers_at_once() {
    let (store, _backend, _dir) = posix_store(config(1, SyncPolicy::Never));
    let key = record(7, 1);
    store.put(&key, &[0x5a; 4096]).expect("put");

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut waiting = Box::pin(store.get_wait(&key));
    let answered = match waiting.as_mut().poll(&mut cx) {
        Poll::Ready(answer) => Some(answer),
        Poll::Pending => None,
    };

    let read = answered
        .expect("posix answered at the first poll")
        .expect("get");
    assert_eq!(read.map(Value::into_vec), Some(vec![0x5a; 4096]));
    assert_eq!(store.driver.wakers(), 0, "nothing was left waiting");
    assert_eq!(store.driver.outstanding(), 0);
}

// the async door reads through the driver where a mapping would have served
#[test]
fn async_never_maps() {
    let mapped = ReelConfig {
        map_above: crate::config::MAP_EVERYTHING,
        ..config(1, SyncPolicy::Never)
    };
    let (store, backend, _dir) = posix_store(mapped);
    store.put(&record(7, 1), &[0x5a; 4096]).expect("put");
    // The first read of a segment opens it and advises the kernel about it,
    // which are ops of their own, so the counted reads start after that.
    store.get(&record(7, 1)).expect("warm get");

    let before = backend.ops();
    let blocked = store.get(&record(7, 1)).expect("get");
    let after_block = backend.ops();
    let awaited = block_on(store.get_wait(&record(7, 1))).expect("awaited get");
    let after_wait = backend.ops();

    assert_eq!(blocked, awaited, "both doors read the same record");
    assert_eq!(
        after_block, before,
        "the mapped read asked the driver for nothing"
    );
    assert!(
        after_wait > after_block,
        "the awaited read went to the driver"
    );
}

/// A volume whose awaited point reads ask the page cache before they queue
fn probed_config() -> ReelConfig {
    ReelConfig {
        point_reads: PointReads::Probed,
        ..config(1, SyncPolicy::Never)
    }
}

/// Whether the probe served anything here, so a skip is nobody's silent green
///
/// The shim answers EAGAIN off linux, and on it a filesystem can refuse the flag
/// outright, so neither can serve a warm read. REEL_DIRECT_REQUIRED turns the
/// skip back into a failure where the probe is supposed to work.
fn warm_serve_or_skip(served: u64) -> bool {
    if served > 0 {
        return true;
    }
    assert!(
        std::env::var_os("REEL_DIRECT_REQUIRED").is_none(),
        "the warm probe served nothing and REEL_DIRECT_REQUIRED is set",
    );
    println!("skipped: the warm probe serves nothing here, so every read rode the engine");
    false
}

/// Flip one payload byte of a record where it sits in its segment file
fn flip_on_disk(store: &ReelStore, dir: &TempDir, key: &RecordKey) {
    use std::os::unix::fs::FileExt;

    let entry = store.index.get(key).expect("read").expect("present");
    let path = dir.path().join(segment_file_name(entry.loc.segment));
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open the segment");
    let at = u64::from(entry.loc.offset) + HEADER_LEN as u64 + u64::from(key.width()) + 7;
    let mut byte = [0u8; 1];
    file.read_at(&mut byte, at).expect("read the payload byte");
    byte[0] ^= 0xff;
    file.write_at(&byte, at).expect("write the payload byte");
}

// a warm awaited read is answered from the page cache with the engine untouched
#[test]
fn a_warm_awaited_read_skips_the_engine() {
    let (store, backend, _dir) = posix_store(probed_config());
    let key = record(7, 1);
    let payload = stripes(8 * 1024);
    store.put(&key, &payload).expect("put");
    // The first read opens the segment and advises the kernel about it, which are
    // ops of their own, and it is also what leaves the record's pages resident.
    store.get(&key).expect("warm the descriptor and the cache");

    let ops = backend.ops();
    let before = backend.warm_reads();
    let found = block_on(store.get_wait(&key))
        .expect("awaited get")
        .expect("found");
    let after = backend.warm_reads();

    assert_eq!(
        &*found,
        &payload[..],
        "the awaited read answered the wrong bytes"
    );
    assert_eq!(
        after.asked - before.asked,
        1,
        "the first poll never asked the cache"
    );
    if !warm_serve_or_skip(after.served - before.served) {
        assert_eq!(
            backend.ops() - ops,
            1,
            "a read the cache refused took more than its one queued op",
        );
        return;
    }
    assert_eq!(
        after.served - before.served,
        1,
        "the cache answered but was not counted"
    );
    assert_eq!(
        backend.ops() - ops,
        0,
        "a record the page cache answered still went to the engine",
    );
}

// a cold awaited read falls through to the engine and answers correctly
#[cfg(target_os = "linux")]
#[test]
fn a_cold_awaited_read_falls_through() {
    let (store, backend, _dir) = posix_store(probed_config());
    let key = record(7, 1);
    let payload = stripes(8 * 1024);
    store.put(&key, &payload).expect("put");
    // Dropping pages leaves the dirty ones where they are, so the record is on
    // the device before anything is dropped.
    store.flush().expect("flush");
    store.get(&key).expect("warm the descriptor");
    drop_cache(&store, &key);

    let ops = backend.ops();
    let before = backend.warm_reads();
    let found = block_on(store.get_wait(&key))
        .expect("awaited get")
        .expect("found");
    let after = backend.warm_reads();

    assert_eq!(
        &*found,
        &payload[..],
        "a cold awaited read answered the wrong bytes"
    );
    assert_eq!(
        after.asked - before.asked,
        1,
        "the first poll never asked the cache"
    );
    if after.served > before.served {
        // A filesystem whose pages are the file, tmpfs above all, drops nothing,
        // and there is no cold read to be had on one.
        assert!(
            std::env::var_os("REEL_DIRECT_REQUIRED").is_none(),
            "dropping the pages left them resident and REEL_DIRECT_REQUIRED is set",
        );
        println!("skipped: this filesystem kept the pages, so nothing here was cold");
        return;
    }
    assert_eq!(
        backend.ops() - ops,
        1,
        "the cold read did not fall through to the one queued read",
    );
}

// a record the cache answers is verified exactly as the queued read verifies it
#[test]
fn a_warm_awaited_read_still_verifies() {
    let verified = ReelConfig {
        verify_reads: true,
        ..probed_config()
    };
    let (store, backend, dir) = posix_store(verified);
    let awaited = record(7, 1);
    let blocked = record(7, 2);
    let payload = stripes(8 * 1024);
    store.put(&awaited, &payload).expect("put");
    store.put(&blocked, &payload).expect("put");
    store
        .get(&awaited)
        .expect("warm the descriptor and the cache");
    store.get(&blocked).expect("warm the cache");
    flip_on_disk(&store, &dir, &awaited);
    flip_on_disk(&store, &dir, &blocked);

    let before = backend.warm_reads();
    let served = block_on(store.get_wait(&awaited)).expect("awaited get");
    let after = backend.warm_reads();

    assert_eq!(
        served, None,
        "the awaited door served a record that failed its checksum"
    );
    assert_eq!(
        store.get(&blocked).expect("get"),
        None,
        "the blocking door served a record that failed its checksum",
    );
    if !warm_serve_or_skip(after.served - before.served) {
        return;
    }
    assert_eq!(
        after.served - before.served,
        1,
        "the corrupted record reached the engine rather than the probe",
    );
}

// an awaited put lands the record and the index exactly as the blocking one
#[test]
fn async_put_matches_the_block() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::EveryPut));

    store.put(&record(7, 1), &[0x11; 512]).expect("put");
    block_on(store.put_owned_wait(&record(7, 2), vec![0x22; 512])).expect("awaited put");
    block_on(store.put_owned_wait(&record(7, 1), vec![0x33; 512])).expect("awaited overwrite");

    assert_eq!(
        store.get(&record(7, 1)).expect("get").map(Value::into_vec),
        Some(vec![0x33; 512]),
    );
    assert_eq!(
        store.get(&record(7, 2)).expect("get").map(Value::into_vec),
        Some(vec![0x22; 512]),
    );
    assert_eq!(store.column_totals(RECORD).expect("totals").count, 2);
}

// an awaited put is as durable as the blocking put beside it
#[test]
fn async_put_is_durable() {
    let (store, sim) = sim_store(config(1, SyncPolicy::EveryPut));

    store.put(&record(7, 1), &[0x11; 512]).expect("put");
    let blocked = sim.unsynced_bytes();
    block_on(store.put_owned_wait(&record(7, 2), vec![0x22; 512])).expect("awaited put");

    assert_eq!(
        sim.unsynced_bytes(),
        blocked,
        "the forwarded turn left no more on the device than the blocking sync did",
    );
}

// an awaited batch is one durability point and one publish, as the blocking one
#[test]
fn async_batch_matches_the_block() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::EveryPut));
    store.put(&record(7, 9), &[0x99; 128]).expect("put");

    let writes = vec![
        RecordWrite::Put {
            key: record(7, 1),
            payload: vec![0x11; 256],
        },
        RecordWrite::Put {
            key: record(7, 2),
            payload: vec![0x22; 256],
        },
        RecordWrite::Delete { key: record(7, 9) },
    ];
    block_on(store.apply_batch_wait(writes)).expect("awaited batch");

    let answers = store
        .get_many(&[record(7, 1), record(7, 2), record(7, 9)])
        .expect("get many");
    let found: Vec<Option<Vec<u8>>> = answers
        .into_iter()
        .map(|value| value.map(Value::into_vec))
        .collect();
    assert_eq!(
        found,
        vec![Some(vec![0x11; 256]), Some(vec![0x22; 256]), None]
    );
}

// an awaited write dropped mid flight leaves the key unwritten and nothing held
#[test]
fn a_dropped_put_leaks_nothing() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let budget = Arc::clone(&store.reel.shared().budget);
    let key = record(7, 1);
    // One record in flight and the ceiling squeezed to nothing behind it, so the next
    // write has to wait: the idle escape only lets a writer past an empty budget.
    budget.acquire(4096);
    budget.throttle(0.0);

    {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut waiting = Box::pin(store.put_owned_wait(&key, vec![0x44; 512]));
        assert!(
            waiting.as_mut().poll(&mut cx).is_pending(),
            "no room for the write"
        );
    }
    budget.release(4096);
    budget.throttle(1.0);

    assert_eq!(budget.queued_bytes(), ByteCount::from_bytes(0));
    assert_eq!(store.get(&record(7, 1)).expect("get"), None);
    store
        .put(&record(7, 1), &[0x55; 512])
        .expect("the volume still writes");
}

// a write over a backend that services ops in place needs one poll
#[test]
fn a_posix_put_answers_at_once() {
    let (store, _backend, _dir) = posix_store(config(1, SyncPolicy::Never));
    let key = record(7, 1);

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut waiting = Box::pin(store.put_owned_wait(&key, vec![0x5a; 4096]));
    let answered = match waiting.as_mut().poll(&mut cx) {
        Poll::Ready(answer) => Some(answer),
        Poll::Pending => None,
    };

    answered
        .expect("posix answered at the first poll")
        .expect("put");
    assert_eq!(
        store.get(&record(7, 1)).expect("get").map(Value::into_vec),
        Some(vec![0x5a; 4096]),
    );
}

// an awaited flush makes what the awaited writes settled durable
#[test]
fn async_flush_settles_the_tail() {
    let (store, sim) = sim_store(config(1, SyncPolicy::Never));
    for byte in 1..=4u8 {
        block_on(store.put_owned_wait(&record(7, byte), vec![byte; 512])).expect("awaited put");
    }
    let before = sim.sync_count();

    block_on(store.flush_wait()).expect("awaited flush");

    assert!(sim.sync_count() > before, "the flush reached the device");
    assert_eq!(sim.unsynced_bytes(), 0);
}

// two columns holding the same key bytes stay apart, and their totals do too
#[test]
fn columns_are_independent() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));

    store.put(&record(1, 9), &[0x11; 100]).expect("record");
    store.put(&blob(9), &[0x22; 300]).expect("blob");

    assert_eq!(
        store.get(&blob(9)).expect("get"),
        Some(Value::new(vec![0x22; 300]))
    );
    assert_eq!(store.column_totals(RECORD).expect("totals").count, 1);
    assert_eq!(store.column_totals(BLOB).expect("totals").count, 1);
    assert_eq!(store.totals().bytes, ByteCount::from_bytes(400));
}

// a key naming a column the volume does not serve is refused, not stored
#[test]
fn unknown_column_refused() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    let stray = RecordKey::from_bytes(ColumnId(9), &[0u8; 32]).expect("key");

    assert!(store.put(&stray, &[0x11; 8]).is_err());
    assert_eq!(store.totals().count, 0);
}

// an overwrite replaces the payload and keeps the totals exact
#[test]
fn overwrite_updates_totals() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    store.put(&record(7, 1), &[0x11; 400]).expect("put");

    store.put(&record(7, 1), &[0x22; 900]).expect("overwrite");

    assert_eq!(
        store.get(&record(7, 1)).expect("get"),
        Some(Value::new(vec![0x22; 900]))
    );
    assert_eq!(store.totals().count, 1);
    assert_eq!(store.totals().bytes, ByteCount::from_bytes(900));
}

// a delete drops the key and lowers the totals
#[test]
fn delete_updates_totals() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    store.put(&record(7, 1), &[0x11; 400]).expect("put");

    store.delete(&record(7, 1)).expect("delete");

    assert_eq!(store.get(&record(7, 1)).expect("get"), None);
    assert!(!store.contains(&record(7, 1)).expect("read"));
    assert_eq!(store.totals().count, 0);
}

// a batch lands every one of its writes and moves the index once
#[test]
fn batch_applies_together() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::EveryPut));
    store.put(&record(7, 9), &[0x99; 100]).expect("put");

    store
        .apply_batch(vec![
            RecordWrite::Put {
                key: record(7, 1),
                payload: vec![0x11; 200],
            },
            RecordWrite::Put {
                key: blob(2),
                payload: vec![0x22; 300],
            },
            RecordWrite::Delete { key: record(7, 9) },
        ])
        .expect("batch");

    assert_eq!(
        store.get(&record(7, 1)).expect("get"),
        Some(Value::new(vec![0x11; 200]))
    );
    assert_eq!(
        store.get(&blob(2)).expect("get"),
        Some(Value::new(vec![0x22; 300]))
    );
    assert!(!store.contains(&record(7, 9)).expect("read"));
    assert_eq!(store.totals().count, 2);
}

// a range delete rides a batch, sweeping what came before it and sparing what follows
//
// One reservation and one durability point for the whole of it, so a caller with a
// range in the middle of its batch does not have to cut the batch around it.
#[test]
fn a_batch_carries_a_range_delete() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::EveryPut));
    store.put(&record(7, 1), &[0x11; 100]).expect("put");
    store
        .put(&record(8, 1), &[0x88; 100])
        .expect("outside the range");

    let start = RecordKey::from_bytes(RECORD, &group_bound(7)).expect("key");
    store
        .apply_batch(vec![
            RecordWrite::Put {
                key: record(7, 2),
                payload: vec![0x22; 100],
            },
            RecordWrite::DeleteRange {
                start,
                end: Some(group_bound(8)),
            },
            RecordWrite::Put {
                key: record(7, 3),
                payload: vec![0x33; 100],
            },
        ])
        .expect("batch");

    assert!(
        !store.contains(&record(7, 1)).expect("read"),
        "written before the batch"
    );
    assert!(
        !store.contains(&record(7, 2)).expect("read"),
        "written before the range"
    );
    assert_eq!(
        store.get(&record(7, 3)).expect("get"),
        Some(Value::new(vec![0x33; 100])),
        "a put after the range survives it",
    );
    assert!(
        store.contains(&record(8, 1)).expect("read"),
        "outside the range"
    );
}

// a batch carrying a range delete comes back the same way after a reopen
#[test]
fn a_batched_range_delete_survives_a_reopen() {
    let (store, sim) = sim_store(config(1, SyncPolicy::EveryPut));
    store.put(&record(7, 1), &[0x11; 100]).expect("put");

    let start = RecordKey::from_bytes(RECORD, &group_bound(7)).expect("key");
    store
        .apply_batch(vec![
            RecordWrite::DeleteRange {
                start,
                end: Some(group_bound(8)),
            },
            RecordWrite::Put {
                key: record(7, 3),
                payload: vec![0x33; 100],
            },
        ])
        .expect("batch");

    let reopened = reopen(&sim, config(1, SyncPolicy::EveryPut));

    assert!(
        !reopened.contains(&record(7, 1)).expect("read"),
        "the range came back with it"
    );
    assert!(
        reopened.contains(&record(7, 3)).expect("read"),
        "and so did the put behind it"
    );
}

// an empty range in a batch writes no record, the same refusal the single door makes
#[test]
fn a_batched_empty_range_writes_nothing() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::EveryPut));
    store.put(&record(7, 1), &[0x11; 100]).expect("put");

    let start = RecordKey::from_bytes(RECORD, &group_bound(7)).expect("key");
    store
        .apply_batch(vec![RecordWrite::DeleteRange {
            start: start.clone(),
            end: Some(start.as_slice().to_vec()),
        }])
        .expect("batch");

    assert!(
        store.contains(&record(7, 1)).expect("read"),
        "an empty range takes nothing"
    );
}

// a whole batch survives a reopen, since the frame that declares it landed
#[test]
fn batch_survives_a_reopen() {
    let (store, sim) = sim_store(config(1, SyncPolicy::EveryPut));
    store
        .apply_batch(vec![
            RecordWrite::Put {
                key: record(7, 1),
                payload: vec![0x11; 200],
            },
            RecordWrite::Put {
                key: record(7, 2),
                payload: vec![0x22; 200],
            },
        ])
        .expect("batch");

    let reopened = reopen(&sim, config(1, SyncPolicy::EveryPut));

    assert_eq!(reopened.totals().count, 2);
    assert_eq!(
        reopened.get(&record(7, 1)).expect("get"),
        Some(Value::new(vec![0x11; 200]))
    );
}

// a batch a crash landed inside leaves none of itself behind
#[test]
fn torn_batch_leaves_nothing() {
    let (store, sim) = sim_store(config(1, SyncPolicy::EveryPut));
    store.put(&record(7, 9), &[0x99; 100]).expect("put");
    store
        .apply_batch(vec![
            RecordWrite::Put {
                key: record(7, 1),
                payload: vec![0x11; 200],
            },
            RecordWrite::Put {
                key: record(7, 2),
                payload: vec![0x22; 200],
            },
        ])
        .expect("batch");
    let torn = store
        .index
        .get(&record(7, 2))
        .expect("read")
        .expect("present")
        .loc;

    let mut image = sim.durable_image();
    flip_payload(&mut image, torn);
    let reopened = reopen_image(image, config(1, SyncPolicy::EveryPut));

    assert!(
        reopened.contains(&record(7, 9)).expect("read"),
        "the record before the batch stayed"
    );
    assert!(
        !reopened.contains(&record(7, 1)).expect("read"),
        "the batch's first record went too"
    );
    assert!(!reopened.contains(&record(7, 2)).expect("read"));
    assert_eq!(reopened.totals().count, 1);
}

// corruption on a sole copy is an error that keeps the key, not a silent miss
#[test]
fn a_sole_copy_reports_corruption_and_keeps_the_key() {
    let sole = ReelConfig {
        verify_reads: true,
        repair: RepairPath::None,
        ..config(1, SyncPolicy::Never)
    };
    let (store, sim) = sim_store(sole.clone());
    store.put(&record(7, 1), &[0x11; 200]).expect("put");
    store.reel.tails()[0].seal().expect("seal");
    store.flush().expect("flush");
    let loc = store
        .index
        .get(&record(7, 1))
        .expect("read")
        .expect("present")
        .loc;

    let mut image = sim.durable_image();
    flip_payload(&mut image, loc);
    let reopened = reopen_image(image, sole);

    assert!(
        matches!(reopened.get(&record(7, 1)), Err(ReelError::Corruption(_))),
        "a corrupt read reports rather than misses"
    );
    assert!(
        reopened.contains(&record(7, 1)).expect("read"),
        "the key keeps its place"
    );
    assert!(
        matches!(reopened.get(&record(7, 1)), Err(ReelError::Corruption(_))),
        "and the answer repeats"
    );
}

// a range delete drops the keys it covers with one record and spares the rest
#[test]
fn delete_range_drops_the_range() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    for group in [6u16, 7, 7, 8] {
        store
            .put(&record(group, group as u8), &[0x11; 100])
            .expect("put");
    }
    store.put(&record(7, 0xaa), &[0x22; 100]).expect("put");

    let start = RecordKey::from_bytes(RECORD, &group_bound(7)).expect("key");
    store
        .delete_range(&start, Some(&group_bound(8)))
        .expect("range delete");

    assert!(store.contains(&record(6, 6)).expect("read"));
    assert!(store.contains(&record(8, 8)).expect("read"));
    assert!(!store.contains(&record(7, 7)).expect("read"));
    assert!(!store.contains(&record(7, 0xaa)).expect("read"));

    // The counters converge at the sweep the maintenance tick runs.
    while store.sweep_covers().expect("sweep") {}
    assert_eq!(store.totals().count, 2);
    assert!(!store.contains(&record(7, 7)).expect("read"));
}

// a range delete that reaches no key writes nothing, so its start key stands
#[test]
fn an_empty_range_writes_nothing() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));
    store.put(&record(7, 1), &[0x11; 100]).expect("put");
    let before = store.sequence();

    let start = record(7, 1);
    store
        .delete_range(&start, Some(start.as_slice()))
        .expect("range delete");

    assert_eq!(
        store.sequence(),
        before,
        "an empty range took a sequence number"
    );
    assert!(
        store.contains(&record(7, 1)).expect("read"),
        "the start key went with it"
    );
    assert_eq!(store.totals().count, 1);
}

// a range delete is replayed on a reopen, so its keys stay gone
#[test]
fn range_delete_survives_a_reopen() {
    let (store, sim) = sim_store(config(1, SyncPolicy::EveryPut));
    store.put(&record(7, 1), &[0x11; 100]).expect("put");
    store.put(&record(8, 1), &[0x22; 100]).expect("put");
    let start = RecordKey::from_bytes(RECORD, &group_bound(7)).expect("key");
    store
        .delete_range(&start, Some(&group_bound(8)))
        .expect("range delete");

    let reopened = reopen(&sim, config(1, SyncPolicy::EveryPut));

    assert!(!reopened.contains(&record(7, 1)).expect("read"));
    assert!(reopened.contains(&record(8, 1)).expect("read"));
    assert_eq!(reopened.totals().count, 1);
}

// a clean reopen reproduces the whole index and its totals
#[test]
fn reopen_reproduces_index() {
    let (store, sim) = sim_store(config(2, SyncPolicy::EveryPut));
    for byte in 1..=8u8 {
        store.put(&record(7, byte), &[byte; 400]).expect("put");
    }
    store.put(&blob(1), &[0x33; 900]).expect("blob");
    let before = store.totals();
    store.close().expect("close");

    let reopened = reopen(&sim, config(2, SyncPolicy::EveryPut));

    assert_eq!(reopened.totals(), before);
    assert_eq!(
        reopened.get(&blob(1)).expect("get"),
        Some(Value::new(vec![0x33; 900]))
    );
}

// closing seals the tails, so a reopen resolves the volume from footers
#[test]
fn close_seals_every_tail() {
    let (store, sim) = sim_store(config(1, SyncPolicy::Never));
    store.put(&record(7, 1), &[0x11; 512]).expect("put");

    store.close().expect("close");

    let bytes = sim
        .durable_bytes(&Path::new(ROOT).join(segment_file_name(SegmentId(1))))
        .expect("segment durable");
    assert!(
        SegmentFooter::parse(&bytes).is_ok(),
        "a sealed segment ends in a footer"
    );
}

// a read-only open serves reads but rejects every write
#[test]
fn read_only_rejects_writes() {
    let (store, sim) = sim_store(config(1, SyncPolicy::EveryPut));
    store.put(&record(7, 1), &[0x11; 400]).expect("put");

    let reader = ReelStore::open_read_only_with_io(
        PathBuf::from(ROOT),
        config(1, SyncPolicy::EveryPut),
        COLUMNS,
        Arc::new(SimIo::from_image(sim.durable_image())),
    )
    .expect("read only open");

    assert_eq!(
        reader.get(&record(7, 1)).expect("get"),
        Some(Value::new(vec![0x11; 400]))
    );
    assert!(reader.put(&record(7, 2), &[0x22; 8]).is_err());
    assert!(reader.delete(&record(7, 1)).is_err());
    assert!(reader.apply_batch(Vec::new()).is_err());
}

// a reader picks up the writer's later appends only once it refreshes
#[test]
fn refresh_picks_up_later_writes() {
    let (writer, sim) = sim_store(config(1, SyncPolicy::EveryPut));
    writer.put(&record(7, 1), &[0x11; 400]).expect("put");

    let reader = ReelStore::open_read_only_with_io(
        PathBuf::from(ROOT),
        config(1, SyncPolicy::EveryPut),
        COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("read only open");
    writer.put(&record(7, 2), &[0x22; 400]).expect("later put");

    assert!(
        !reader.contains(&record(7, 2)).expect("read"),
        "the reader's index has not moved"
    );
    reader.refresh().expect("refresh");
    assert!(reader.contains(&record(7, 2)).expect("read"));
}

// a reader follows the log rather than reading the volume again
#[test]
fn refresh_reads_only_what_is_new() {
    let (writer, sim) = sim_store(config(1, SyncPolicy::EveryPut));
    for byte in 1..=8u8 {
        writer.put(&record(7, byte), &[byte; 400]).expect("put");
    }

    let reader = ReelStore::open_read_only_with_io(
        PathBuf::from(ROOT),
        config(1, SyncPolicy::EveryPut),
        COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("read only open");
    writer.put(&record(7, 9), &[0x99; 400]).expect("later put");

    let caught = reader.refresh().expect("refresh");

    assert_eq!(caught.applied, 1, "only the later put was read");
    assert!(reader.contains(&record(7, 9)).expect("read"));
    assert_eq!(reader.totals().count, 9);
}

// a reader following the log across a compaction keeps the key it repointed
#[test]
fn refresh_follows_a_relocation() {
    let mut settings = config(1, SyncPolicy::EveryPut);
    settings.segment_bytes = ByteCount::from_bytes(8_192);
    settings.alloc_chunk = ByteCount::from_bytes(4_096);
    let (writer, sim) = sim_store(settings.clone());
    for byte in 1..=4u8 {
        writer.put(&record(7, byte), &[byte; 1_500]).expect("put");
    }
    for byte in 1..=3u8 {
        writer
            .put(&record(7, byte), &[byte + 100; 1_500])
            .expect("overwrite");
    }
    writer.flush().expect("flush");

    let reader = ReelStore::open_read_only_with_io(
        PathBuf::from(ROOT),
        settings,
        COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("read only open");
    let before = reader.totals().count;

    writer.compact_once().expect("compact");
    writer.flush().expect("flush");
    reader.refresh().expect("refresh");

    assert_eq!(
        reader.totals().count,
        before,
        "compaction moved records, it did not lose them"
    );
    for byte in 1..=4u8 {
        assert!(
            reader.contains(&record(7, byte)).expect("read"),
            "key {byte} went missing across the move"
        );
    }
}

// a range delete a reader follows keeps its keys deleted
#[test]
fn refresh_follows_a_range_delete() {
    let (writer, sim) = sim_store(config(1, SyncPolicy::EveryPut));
    writer.put(&record(7, 1), &[0x11; 100]).expect("put");
    writer.put(&record(8, 1), &[0x22; 100]).expect("put");

    let reader = ReelStore::open_read_only_with_io(
        PathBuf::from(ROOT),
        config(1, SyncPolicy::EveryPut),
        COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("read only open");

    let start = RecordKey::from_bytes(RECORD, &group_bound(7)).expect("key");
    writer
        .delete_range(&start, Some(&group_bound(8)))
        .expect("range delete");
    reader.refresh().expect("refresh");

    assert!(!reader.contains(&record(7, 1)).expect("read"));
    assert!(reader.contains(&record(8, 1)).expect("read"));
    assert_eq!(reader.totals().count, 1);
}

// a writable reel already holds the current index, so it refuses a refresh
#[test]
fn refresh_rejects_a_writer() {
    let (store, _sim) = sim_store(config(1, SyncPolicy::Never));

    assert!(store.refresh().is_err());
}

// a read-only volume is driven by the same timer and never writes on the pass
#[test]
fn maintain_once_is_inert_read_only() {
    let (writer, sim) = sim_store(config(1, SyncPolicy::EveryPut));
    writer.put(&record(7, 1), &[0x11; 400]).expect("put");
    let reader = ReelStore::open_read_only_with_io(
        PathBuf::from(ROOT),
        config(1, SyncPolicy::EveryPut),
        COLUMNS,
        Arc::new(SimIo::from_image(sim.durable_image())),
    )
    .expect("read only open");

    reader.maintain_once().expect("maintain");

    assert_eq!(reader.scrub_once().expect("scrub"), 0);
    assert!(reader.contains(&record(7, 1)).expect("read"));
}

/// The low bound of one group's keys
fn group_bound(group: u16) -> Vec<u8> {
    let mut bytes = group.to_be_bytes().to_vec();
    bytes.extend_from_slice(&[0u8; 32]);
    bytes
}

/// Corrupt the payload of the record a location names
fn flip_payload(image: &mut DurableImage, loc: Loc) {
    let path = Path::new(ROOT).join(segment_file_name(loc.segment));
    for (candidate, bytes) in image.iter_mut() {
        if candidate == &path {
            let at = loc.offset as usize + HEADER_LEN + 34;
            if at < bytes.len() {
                bytes[at] ^= 0xff;
            }
        }
    }
}
