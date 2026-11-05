//! Random work on two volumes with failed syncs, full disks, rot and crashes, also inside recovery

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::seed_sets::seeds;
use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel_sim::file_image::BitMask;
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;
use reel::io::{Io, IoFile};
use reel::record::codec::Codec;
use reel::rows::row_group::parse_group;
use reel::segment::summary::SummaryPaths;
use reel::superblock::Superblock;
use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, VolumeId};

const ROOTS: [&str; 3] = ["/disk/faults-meta", "/disk/faults-meta-two", "/disk/faults-bulk"];
const COLUMNS: [ColumnId; 2] = [ColumnId(1), ColumnId(2)];
const BUDGET: ByteCount = ByteCount(8 * 1024 * 1024);
// each set keeps the seeds that found a bug, and a full sweep passed before these were cut down
const SEEDS: [u64; 7] = [0, 1, 2, 3, 17, 31, 37];
const ROT_SEEDS: [u64; 8] = [125, 349, 1_000, 1_001, 1_002, 1_003, 1_011, 1_031];
const STEPS: u64 = 3_000;
const KEY_SPACE: u64 = 30;
const SETTLE_STEPS: u64 = 2_000_000;
const SETTLE_TICK: Nanoseconds = Nanoseconds(1_000_000);
const CRASH_ODDS: u64 = 250;
const FAULT_ODDS: u64 = 60;
const ROT_ODDS: u64 = 100;
const MOST_CRASH_DELAY: u64 = 40;
const MOST_OPEN_DELAY: u64 = 120;
const MOST_OPENS: u64 = 400;
const TAIL_REACH: u64 = 200_000;
const LARGE_VALUE_ODDS: u64 = 40;
const LARGE_VALUE_BYTES: usize = 200_000;
const POLICY_BYTES: ByteCount = ByteCount(4_096);
const SYNCED_FILES: [&str; 4] = ["data", "rows", "summary", "new"];
const MOST_SYNC_SKIP: u64 = 6;

type Store = ReelStore<SimulatedIo>;
type Key = (ColumnId, Vec<u8>);

/// One key's versions in write order, from the nothing it started as
#[derive(Debug)]
struct Versions {
    history: Vec<Option<Vec<u8>>>,
    floor: usize,
    refused_at: Option<usize>,
}

/// A plain map of versions
type Model = BTreeMap<Key, Versions>;

/// One seeded run
struct Run {
    io: SimulatedIo,
    random: SeededRandom,
    model: Model,
    seed: u64,
    has_rot: bool,
    policy: SyncPolicy,
    steps_left: u64,
}

impl Versions {
    // a refusal tells the model nothing, so the versions it could hold stay in play until a read or a newer write
    fn is_unknown(&self) -> bool {
        self.refused_at == Some(self.history.len())
    }

    fn note_refusal(&mut self) {
        self.refused_at = Some(self.history.len());
    }

    // take what a read found as the truth, once it's at or above what a sync covered
    fn accept(&mut self, found: &Option<Vec<u8>>) -> bool {
        let Some(offset) = self.history[self.floor..].iter().position(|value| value == found) else {
            return false;
        };
        self.history.truncate(self.floor + offset + 1);
        self.refused_at = None;
        true
    }
}

impl Default for Versions {
    fn default() -> Self {
        Self {
            history: vec![None],
            floor: 0,
            refused_at: None,
        }
    }
}

fn config(policy: SyncPolicy) -> StoreConfig {
    let volume = |name: &str, roots: &[&str], sync: SyncPolicy| VolumeConfig {
        name: name.to_string(),
        roots: roots.iter().map(PathBuf::from).collect(),
        sync,
    };
    let column = |id: ColumnId, name: &str, volume: u8| ColumnConfig {
        id,
        name: name.to_string(),
        key_shape: KeyShape::Variable { most: 64 },
        codec: Codec::None,
        volume: VolumeId(volume),
    };
    StoreConfig {
        memory_budget: BUDGET,
        volumes: vec![volume("meta", &ROOTS[..2], policy), volume("bulk", &ROOTS[2..], SyncPolicy::Never)],
        columns: vec![column(COLUMNS[0], "meta", 0), column(COLUMNS[1], "bulk", 1)],
    }
}

fn read(store: &Store, column: ColumnId, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
    let mut buf = Vec::new();
    Ok(store.get(column, key, &mut buf)?.then_some(buf))
}

fn data_files(io: &SimulatedIo) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for root in ROOTS {
        // an armed crash can fire on this very listing, and the rot waits for the next step
        let Ok(listing) = io.list_directory(Path::new(root)) else {
            continue;
        };
        for path in listing {
            if path.extension().is_some_and(|extension| extension == "data") {
                files.push(path);
            }
        }
    }
    files
}

// find how far a rows file is covered by a mark that a later sync covered too, and where the records of those
// rows end: rot below that always has a surviving mark above it, and the spec accepts rot in a synced group
// whose mark a power cut took
fn durable_rows(io: &SimulatedIo, path: &Path) -> Option<(u64, u64)> {
    let superblock = Superblock::read(io, Path::new(ROOTS[0])).ok()??;
    let file = io.open_read_only(path).ok()?;
    let length = file.length();
    let mut bytes = vec![0; length.as_ref().map_or(0, |length| length.0 as usize)];
    let read = file.read_exact_at(FileOffset(0), &mut bytes);
    io.close_file(file);
    read.ok()?;

    let mut groups = Vec::new();
    let mut at = 0;
    while let Some(group) = bytes.get(at..).and_then(|rest| parse_group(rest, superblock.record_key)) {
        let record_end = group.walk().map(|row| row.offset.0 + row.length.0).max().unwrap_or(0);
        groups.push((at as u64, group.synced_through.0, record_end));
        at += group.total_length.0 as usize;
    }
    let last_mark = groups.iter().map(|group| group.1).max()?;
    let covered = groups.iter().filter(|group| group.0 < last_mark).map(|group| group.1).max()?;
    let data_end = groups.iter().filter(|group| group.0 < covered).map(|group| group.2).max().unwrap_or(0);
    Some((covered, data_end))
}

impl Run {
    fn new(seed: u64, has_rot: bool) -> Self {
        Self {
            seed,
            io: SimulatedIo::new(Seed(seed)),
            random: SeededRandom::new(Seed(seed)),
            model: Model::new(),
            has_rot,
            policy: match seed % 3 {
                0 => SyncPolicy::Never,
                1 => SyncPolicy::EveryWrite,
                _other => SyncPolicy::EveryBytes(POLICY_BYTES),
            },
            steps_left: STEPS,
        }
    }

    // open through whatever the seed throws at it, with a crash inside recovery on some tries
    fn open(&mut self) -> Store {
        let mut last_error = None;
        for _try in 0..MOST_OPENS {
            if self.random.one_in(2) {
                self.io.arm_crash(OperationCount(self.random.below(MOST_OPEN_DELAY)));
            }
            if self.random.one_in(4) {
                self.arm_fault();
            }
            let opened = ReelStore::open(self.io.clone(), &config(self.policy));
            if self.io.is_crashed() {
                self.crash();
                drop(opened);
                continue;
            }
            self.clear_faults();
            match opened {
                Ok(store) => {
                    self.io.disarm_crash();
                    return store;
                },
                Err(error) => last_error = Some(error),
            }
        }
        panic!("seed {}: the store never opened: {last_error:?}", self.seed);
    }

    fn crash(&mut self) {
        match self.random.one_in(2) {
            true => self.io.power_cut(),
            false => self.io.kill(),
        }
    }

    fn arm_fault(&mut self) {
        let count = OperationCount(1 + self.random.below(3));
        match self.random.below(6) {
            0 => self.io.fail_syncs(count),
            1 => self.io.fill_writes(count),
            2 => self.io.fail_writes(count),
            3 => self.io.fail_creates(count),
            4 => self.io.fail_directory_syncs(count),
            _last => {
                let file = SYNCED_FILES[self.random.below(SYNCED_FILES.len() as u64) as usize];
                self.io.fail_file_sync(file, OperationCount(self.random.below(MOST_SYNC_SKIP)));
            },
        }
    }

    fn clear_faults(&self) {
        let none = OperationCount(0);
        self.io.fail_syncs(none);
        self.io.fill_writes(none);
        self.io.fail_writes(none);
        self.io.fail_creates(none);
        self.io.fail_directory_syncs(none);
        self.io.clear_file_sync();
    }

    fn rot_one(&mut self) {
        if self.io.is_crashed() {
            return;
        }
        match self.random.below(10) {
            0 => self.rot_rows(),
            1 => self.rot_summary(),
            _data => self.rot_data(),
        }
    }

    // rot in the summary costs rows that open builds again from the tails
    fn rot_summary(&mut self) {
        let root = Path::new(ROOTS[self.random.below(2) as usize * 2]);
        let paths = SummaryPaths::new(root);
        let Ok(file) = self.io.open_read_only(&paths.current) else {
            return;
        };
        let length = file.length();
        self.io.close_file(file);
        if let Ok(ByteCount(length)) = length
            && length > 0
        {
            let at = self.random.below(length);
            let _rotted = self.io.rot(&paths.current, FileOffset(at), BitMask(1));
        }
    }

    // rot in what a surviving mark covers, since rot in a synced group with its mark lost reads as a torn end
    fn rot_rows(&mut self) {
        let root = ROOTS[self.random.below(ROOTS.len() as u64) as usize];
        let Ok(listing) = self.io.list_directory(Path::new(root)) else {
            return;
        };
        let rows: Vec<&PathBuf> = listing.iter().filter(|path| path.extension().is_some_and(|name| name == "rows")).collect();
        if rows.is_empty() {
            return;
        }
        let path = rows[self.random.below(rows.len() as u64) as usize];
        let Some((covered, _data_end)) = durable_rows(&self.io, path) else {
            return;
        };
        if covered > 0 {
            let at = self.random.below(covered);
            let _rotted = self.io.rot(path, FileOffset(at), BitMask(1));
        }
    }

    fn rot_data(&mut self) {
        let files = data_files(&self.io);
        if files.is_empty() {
            return;
        }
        let path = &files[self.random.below(files.len() as u64) as usize];
        // an armed crash can fire on this very open
        let Ok(file) = self.io.open_read_only(path) else {
            return;
        };
        let length = file.length();
        self.io.close_file(file);
        let Ok(ByteCount(length)) = length else {
            return;
        };
        if length == 0 {
            return;
        }

        // an open segment takes rot only under records a surviving mark covers
        let rows = path.with_extension("rows");
        let Ok(listing) = self.io.list_directory(path.parent().unwrap_or(Path::new("/"))) else {
            return;
        };
        if listing.contains(&rows) {
            let Some((_covered, data_end)) = durable_rows(&self.io, &rows) else {
                return;
            };
            if data_end > 0 {
                let at = self.random.below(data_end);
                let _rotted = self.io.rot(path, FileOffset(at), BitMask(1));
            }
            return;
        }

        // half of the rot lands in the footer end of the file, where few bytes hold a lot
        let reach = if self.random.one_in(2) { length.min(TAIL_REACH) } else { length };
        let at = length - 1 - self.random.below(reach);
        // a bad sector fails its reads for good, and rot leaves them passing with the wrong bytes
        if self.random.one_in(3) {
            self.io.bad_sector(path, FileOffset(at)).expect("bad");
            return;
        }
        self.io.rot(path, FileOffset(at), BitMask(1 << self.random.below(8))).expect("rot");
    }

    fn random_key(&mut self) -> Key {
        let column = COLUMNS[self.random.below(COLUMNS.len() as u64) as usize];
        (column, format!("key-{:04}", self.random.below(KEY_SPACE)).into_bytes())
    }

    fn write(&mut self, store: &Store) {
        let (column, key) = self.random_key();
        let is_grave = self.random.one_in(3);
        let is_large = self.random.one_in(LARGE_VALUE_ODDS);
        let mut bytes = format!("value-{}", self.random.next_word()).into_bytes();
        if is_large {
            bytes.resize(LARGE_VALUE_BYTES, b'x');
        }
        let value = (!is_grave).then_some(bytes);
        let written = match &value {
            Some(value) => store.put(column, &key, value),
            None => store.delete(column, &key),
        };
        // a crash inside the call leaves its outcome open, and the next open shows which way it went
        // and so does a policy sync that failed after the write landed, which reads see from then on
        let has_landed = matches!(written, Err(StoreError::Io { action: "syncing a volume", .. }));
        if written.is_ok() || has_landed || self.io.is_crashed() {
            let is_covered = written.is_ok() && column == COLUMNS[0] && self.policy == SyncPolicy::EveryWrite;
            let versions = self.model.entry((column, key)).or_default();
            versions.history.push(value);
            if is_covered {
                versions.floor = versions.history.len() - 1;
            }
        }
    }

    fn flush(&mut self, store: &Store) {
        if store.flush().is_ok() {
            for versions in self.model.values_mut() {
                if !versions.is_unknown() {
                    versions.floor = versions.history.len() - 1;
                }
            }
        }
    }

    fn check_read(&mut self, store: &Store) {
        let (column, key) = self.random_key();
        let versions = self.model.entry((column, key.clone())).or_default();
        let newest = versions.history.last().cloned().flatten();
        match read(store, column, &key) {
            Ok(found) if versions.is_unknown() => {
                assert!(versions.accept(&found), "seed {}: get of {key:?} read {found:?} below its floor", self.seed);
            },
            Ok(found) => assert_eq!(found, newest, "get of {key:?}"),
            Err(StoreError::Corruption { .. }) if self.has_rot => versions.note_refusal(),
            Err(_crashed) if self.io.is_crashed() => {},
            Err(error) => panic!("seed {}: get of {key:?} failed: {error}", self.seed),
        }
    }

    // run until the steps run out or a crash fires, and say which
    fn work(&mut self, store: &Store) -> bool {
        while self.steps_left > 0 {
            self.steps_left -= 1;
            if self.io.is_crashed() {
                return true;
            }
            if self.random.one_in(CRASH_ODDS) {
                self.io.arm_crash(OperationCount(self.random.below(MOST_CRASH_DELAY)));
            }
            if self.random.one_in(FAULT_ODDS) {
                self.arm_fault();
            }
            if self.has_rot && self.random.one_in(ROT_ODDS) {
                self.rot_one();
            }

            match self.random.below(10) {
                0..4 => self.write(store),
                4 => self.flush(store),
                5 => store.seal_now(),
                6..8 => {
                    self.io.step_any();
                    self.io.advance_clock(SETTLE_TICK);
                },
                _read => self.check_read(store),
            }
        }
        false
    }

    // take what each key reads after a crash as the truth, once it's at or above what a sync covered
    fn reconcile(&mut self, store: &Store) {
        for ((column, key), versions) in &mut self.model {
            let found = match read(store, *column, key) {
                Ok(found) => found,
                Err(StoreError::Corruption { .. }) if self.has_rot => {
                    versions.note_refusal();
                    continue;
                },
                Err(error) => panic!("seed {}: get of {key:?} failed: {error}", self.seed),
            };
            if !versions.accept(&found) {
                panic!("seed {}: key {key:?} read {found:?} below its floor, in {versions:?}", self.seed);
            }
        }
    }

    fn settle(&self) {
        for _step in 0..SETTLE_STEPS {
            if self.io.step_any().is_none() {
                self.io.advance_clock(SETTLE_TICK);
                if self.io.step_any().is_none() {
                    return;
                }
            }
        }
        panic!("jobs never went quiet");
    }

    fn run(mut self) {
        while self.steps_left > 0 {
            let store = self.open();
            self.reconcile(&store);
            let has_crashed = self.work(&store);
            if has_crashed {
                self.crash();
                drop(store);
                continue;
            }

            self.clear_faults();
            self.settle();
            // a volume a failed sync stopped refuses a clean close, and the next open sorts it out
            let _closed = store.close();
            drop(store);
            self.io.kill();
        }

        let store = self.open();
        self.reconcile(&store);
        store.close().expect("close");
        drop(store);
        assert_eq!(self.io.open_handle_count(), 0, "seed {}", self.seed);
        assert_eq!(self.io.handle_faults(), OperationCount(0), "seed {}", self.seed);
    }
}

// flushed writes and every read hold through failed syncs, full disks and crashes in and out of recovery
#[test]
fn random_faults() {
    for seed in seeds(SEEDS) {
        Run::new(seed, false).run();
    }
}

// the same with rot: a read matches or says it can't, and never answers older or missing
#[test]
fn random_rot() {
    for seed in seeds(ROT_SEEDS) {
        Run::new(seed, true).run();
    }
}
