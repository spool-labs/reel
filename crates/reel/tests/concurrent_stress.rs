//! Concurrent read, write, and compact stress over the reel store

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

use tempfile::TempDir;

use reel::index::map::KeySites;
use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, RecordKey, ReelConfig, ReelStore,
    SyncPolicy, ThreadBudget,
};

const RECORDS: ColumnId = ColumnId(1);
const BLOB: ColumnId = ColumnId(2);

/// A record-shaped column keyed by group then id, and a blob column keyed by id
const COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: RECORDS,
        name: "records",
        key_width: KeyWidth::Fixed(34),
        shard_bytes: 2,
        purge_mark: None,
        codec: Codec::None,
    },
    ColumnSpec {
        id: BLOB,
        name: "blob_data",
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        purge_mark: None,
        codec: Codec::None,
    },
];

/// One key as its group and id
type Key = (u16, u8);

/// Maps each key to the versions written to it
type Written = BTreeMap<Key, BTreeSet<u64>>;

/// The simulator's files live under this virtual root
const SIM_ROOT: &str = "/bulk";

/// The writers spread their keys across these groups
const GROUPS: &[u16] = &[7, 8];

/// Each group holds this many contested ids, few enough that writers collide
const ID_SPACE: u8 = 12;

/// Ids written once, so their records stay live and compaction has to repoint them
const COLD_IDS: u8 = 3;

/// The cold keys' version, which no writer can produce
const COLD_VERSION: u64 = u64::MAX;

/// Ids per group, contested and cold together
const KEY_SPACE: u8 = ID_SPACE + COLD_IDS;

/// The drain gives up after this many compaction passes
const DRAIN_PASSES: usize = 64;

/// Writer threads racing each other over the shared key space
const WRITERS: usize = 4;

/// Reader threads checking what the writers publish
const READERS: usize = 4;

/// Writes per writer thread, overridden by REEL_STORM_WRITES
const WRITES_PER_WRITER: u64 = 150;

/// Writes per writer, from REEL_STORM_WRITES or `WRITES_PER_WRITER`
fn writes_per_writer() -> u64 {
    std::env::var("REEL_STORM_WRITES")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(WRITES_PER_WRITER)
}

/// The shortest stamped payload, wide enough to hold its version
const MIN_LEN: usize = 16;

/// How much longer than the shortest a payload can be
const LEN_SPREAD: usize = 700;

/// Small enough that every tail rolls several times over the storm
const SEGMENT_BYTES: u64 = 16 * 1024;

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT_BYTES),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(2),
        // A low dead ratio makes the compactor repoint while writers still move records
        compact_dead_ratio: 0.1,
        ..ReelConfig::default()
    }
}

/// The record column key for a group and an id, big endian group at the front
fn record_key(group: u16, id: [u8; 32]) -> RecordKey {
    let mut bytes = [0u8; 34];
    bytes[..2].copy_from_slice(&group.to_be_bytes());
    bytes[2..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

fn record_id(byte: u8) -> [u8; 32] {
    [byte; 32]
}

/// A payload whose length and bytes all follow from the version that wrote it
fn stamped(id: u8, version: u64) -> Vec<u8> {
    let len = MIN_LEN + (version as usize % LEN_SPREAD);
    let mut out = Vec::with_capacity(len);
    out.extend_from_slice(&version.to_le_bytes());
    out.resize(len, id ^ (version as u8));
    out
}

/// Whether a payload is a whole version of this key
fn is_stamped(id: u8, payload: &[u8]) -> bool {
    if payload.len() < std::mem::size_of::<u64>() {
        return false;
    }
    let mut version = [0u8; std::mem::size_of::<u64>()];
    version.copy_from_slice(&payload[..std::mem::size_of::<u64>()]);
    payload == stamped(id, u64::from_le_bytes(version)).as_slice()
}

/// What one store serves and where each answer came from
struct View {
    /// The store's answers, in key order
    held: BTreeMap<Key, Vec<u8>>,

    /// Where each of those answers came from
    sites: BTreeMap<Key, KeySites>,
}

/// Reads everything the store serves, cold keys included, for comparing across a reopen
fn view(store: &ReelStore) -> View {
    let mut held = BTreeMap::new();
    let mut sites = BTreeMap::new();
    for group in GROUPS {
        for byte in 0..KEY_SPACE {
            let key = record_key(*group, record_id(byte));
            if let Some(value) = store.get(&key).expect("get") {
                held.insert((*group, byte), value.into_vec());
            }
            sites.insert((*group, byte), store.index().sites(&key).expect("sites"));
        }
    }
    View { held, sites }
}

/// Writes each cold key once, before anything else reaches the store
fn seed_cold_keys(store: &ReelStore) -> Written {
    let mut written = Written::new();
    for group in GROUPS {
        for byte in ID_SPACE..KEY_SPACE {
            store
                .put_owned(
                    &record_key(*group, record_id(byte)),
                    stamped(byte, COLD_VERSION),
                )
                .expect("cold put");
            written
                .entry((*group, byte))
                .or_default()
                .insert(COLD_VERSION);
        }
    }
    written
}

/// Compacts until a pass finds nothing to do, since the racing compactor may stop early
fn drain_compaction(store: &ReelStore) {
    for _ in 0..DRAIN_PASSES {
        let before = store.compaction_counters();
        store.compact_once().expect("compact");
        let after = store.compaction_counters();
        if after.segments_rewritten == before.segments_rewritten
            && after.segments_unlinked_whole == before.segments_unlinked_whole
        {
            return;
        }
    }
}

/// Races writers, readers, compaction, and handover on one store
fn storm(store: &Arc<ReelStore>) -> (Written, usize) {
    let written: Arc<Mutex<Written>> = Arc::new(Mutex::new(seed_cold_keys(store)));
    let is_running = Arc::new(AtomicBool::new(true));
    let barrier = Arc::new(Barrier::new(WRITERS + READERS + 1));
    let mut handles = Vec::new();

    for writer in 0..WRITERS {
        let store = Arc::clone(store);
        let written = Arc::clone(&written);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let writes = writes_per_writer();
            for step in 0..writes {
                let group = GROUPS[(writer + step as usize) % GROUPS.len()];
                let byte = ((writer as u64 * writes + step) % u64::from(ID_SPACE)) as u8;
                let version = (writer as u64) << 32 | step;
                let payload = stamped(byte, version);
                store
                    .put_owned(&record_key(group, record_id(byte)), payload)
                    .expect("put");
                written
                    .lock()
                    .expect("written")
                    .entry((group, byte))
                    .or_default()
                    .insert(version);
            }
        }));
    }

    for reader in 0..READERS {
        let store = Arc::clone(store);
        let is_running = Arc::clone(&is_running);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut step = reader as u64;
            while is_running.load(Ordering::Relaxed) {
                let group = GROUPS[step as usize % GROUPS.len()];
                let byte = (step % u64::from(ID_SPACE)) as u8;
                if let Some(payload) = store.get(&record_key(group, record_id(byte))).expect("get")
                {
                    assert!(
                        is_stamped(byte, &payload),
                        "a read served a torn or foreign payload for group {group} key {byte}"
                    );
                }
                step += 1;
            }
        }));
    }

    let paged = Arc::new(AtomicUsize::new(0));
    // Maintenance hands sealed keys over before it compacts, so this does too
    let compactor = {
        let store = Arc::clone(store);
        let is_running = Arc::clone(&is_running);
        let paged = Arc::clone(&paged);
        thread::spawn(move || {
            while is_running.load(Ordering::Relaxed) {
                let handed = store.page_out_sealed().expect("page out");
                paged.fetch_add(handed, Ordering::Relaxed);
                store.compact_once().expect("compact");
            }
        })
    };

    let pager = {
        let store = Arc::clone(store);
        let is_running = Arc::clone(&is_running);
        let paged = Arc::clone(&paged);
        thread::spawn(move || {
            while is_running.load(Ordering::Relaxed) {
                let handed = store.page_out_sealed().expect("page out");
                paged.fetch_add(handed, Ordering::Relaxed);
            }
        })
    };

    barrier.wait();
    for handle in handles.drain(..WRITERS) {
        handle.join().expect("writer joins");
    }
    is_running.store(false, Ordering::Relaxed);
    for handle in handles {
        handle.join().expect("reader joins");
    }
    compactor.join().expect("compactor joins");
    pager.join().expect("pager joins");
    drain_compaction(store);
    // Hand over whatever sealed after the pager stopped, so the store ends fully paged
    paged.fetch_add(
        store.page_out_sealed().expect("page out"),
        Ordering::Relaxed,
    );

    let written = Arc::try_unwrap(written)
        .expect("the storm owns the record")
        .into_inner()
        .expect("written");
    (written, paged.load(Ordering::Relaxed))
}

/// Asserts the storm actually raced, so an idle run cannot pass
fn assert_raced(store: &ReelStore, written: &Written) {
    let counters = store.compaction_counters();
    let retired = counters.segments_rewritten + counters.segments_unlinked_whole;
    assert!(
        retired > 0,
        "compaction never retired a segment, so no repoint raced a put"
    );
    assert!(
        counters.segments_rewritten > 0,
        "every retired segment was unlinked whole, so no live record was ever repointed"
    );

    let contested = written
        .values()
        .filter(|versions| versions.len() > 1)
        .count();
    assert!(
        contested > 0,
        "no key was written twice, so no read raced an overwrite"
    );
}

/// Lists the keys two views disagree about, and what each said
fn disagreement(live: &View, reopened: &View) -> String {
    let mut out = format!(
        "live holds {} keys, the reopen holds {}\n",
        live.held.len(),
        reopened.held.len()
    );
    let mut keys: Vec<&Key> = live.held.keys().chain(reopened.held.keys()).collect();
    keys.sort();
    keys.dedup();
    for key in keys {
        let (here, there) = (live.held.get(key), reopened.held.get(key));
        if here == there {
            continue;
        }
        out.push_str(&format!(
            "  group {} key {}: live {}, reopened {}\n",
            key.0,
            key.1,
            stamp_of(here),
            stamp_of(there),
        ));
        out.push_str(&sites_of(" live   ", live.sites.get(key)));
        out.push_str(&sites_of(" reopen ", reopened.sites.get(key)));
    }
    out
}

/// Lists where one view answers a key from, one line per place
fn sites_of(side: &str, sites: Option<&KeySites>) -> String {
    let Some(sites) = sites else {
        return format!("   {side} no sites recorded\n");
    };
    let mut out = format!(
        "   {side} map {}, candidates {:?}\n",
        match sites.resident {
            None => "holds nothing".to_string(),
            Some(entry) if entry.is_grave() => format!("holds a grave at lsn {:?}", entry.lsn),
            Some(entry) => format!(
                "holds segment {:?} offset {} lsn {:?}",
                entry.loc.segment, entry.loc.offset, entry.lsn
            ),
        },
        sites.candidates,
    );
    for site in &sites.sealed {
        out.push_str(&format!(
            "   {side} segment {:?} offset {} lsn {:?}{}{}{}\n",
            site.segment,
            site.offset,
            site.lsn,
            match site.is_grave {
                true => ", a grave",
                false => "",
            },
            match site.is_candidate {
                true => "",
                false => ", NOT A CANDIDATE",
            },
            match site.passes_filter {
                true => "",
                false => ", FILTERED OUT",
            },
        ));
    }
    out
}

/// The version in a payload, or absent when there is none
fn stamp_of(payload: Option<&Vec<u8>>) -> String {
    match payload {
        None => "absent".to_string(),
        Some(bytes) if bytes.len() < std::mem::size_of::<u64>() => "short".to_string(),
        Some(bytes) => {
            let mut version = [0u8; std::mem::size_of::<u64>()];
            version.copy_from_slice(&bytes[..std::mem::size_of::<u64>()]);
            let version = u64::from_le_bytes(version);
            match version == COLD_VERSION {
                true => "the cold version".to_string(),
                false => format!("writer {} step {}", version >> 32, version & 0xffff_ffff),
            }
        }
    }
}

/// Asserts what settled is a written version and survives a reopen
fn assert_settles(live: &View, reopened: &View, written: &Written) {
    assert!(!live.held.is_empty(), "the storm left nothing behind");
    if live.held != reopened.held {
        panic!(
            "a reopen disagrees with what the store served\n{}",
            disagreement(live, reopened)
        );
    }

    for ((group, byte), payload) in &live.held {
        assert!(
            is_stamped(*byte, payload),
            "group {group} key {byte} settled torn"
        );
        let mut version = [0u8; std::mem::size_of::<u64>()];
        version.copy_from_slice(&payload[..std::mem::size_of::<u64>()]);
        let versions = written.get(&(*group, *byte)).expect("a key nobody wrote");
        assert!(
            versions.contains(&u64::from_le_bytes(version)),
            "group {group} key {byte} settled on a version nobody wrote"
        );
    }
}

// readers, writers, and compaction race, then the store agrees with itself
#[test]
fn sim_backend_storm() {
    let sim = SimIo::new(FaultPlan::new(1));
    let root = PathBuf::from(SIM_ROOT);
    let store = Arc::new(
        ReelStore::open_with_io(root.clone(), config(), COLUMNS, Arc::new(sim.clone()))
            .expect("open"),
    );

    let (written, paged) = storm(&store);
    store.flush().expect("flush");
    // Count across the run, since compaction can retire every sealed segment by the end
    assert!(
        paged > 0,
        "no key reached a footer, so the storm raced no handover"
    );
    assert_raced(&store, &written);
    let live = view(&store);
    drop(store);

    let restored = SimIo::from_image(sim.durable_image());
    let reopened =
        ReelStore::open_with_io(root, config(), COLUMNS, Arc::new(restored)).expect("reopen");

    assert_settles(&live, &view(&reopened), &written);
}

// the same storm against real descriptors, real unlinks, and the real page cache
#[test]
fn posix_backend_storm() {
    let dir = TempDir::new().expect("tempdir");
    let store =
        Arc::new(ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("open"));

    let (written, paged) = storm(&store);
    store.flush().expect("flush");
    assert!(
        paged > 0,
        "no key reached a footer, so the storm raced no handover"
    );
    assert_raced(&store, &written);
    let live = view(&store);
    drop(store);

    let reopened = ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("reopen");

    assert_settles(&live, &view(&reopened), &written);
}
