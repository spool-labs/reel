//! Random work on two volumes with failed syncs, full disks, rot and crashes, also inside recovery

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel_sim::file_image::BitMask;
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;
use reel::io::{Io, IoFile};
use reel::record::codec::Codec;
use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, VolumeId};

const ROOTS: [&str; 2] = ["/disk/faults-meta", "/disk/faults-bulk"];
const COLUMNS: [ColumnId; 2] = [ColumnId(1), ColumnId(2)];
const BUDGET: ByteCount = ByteCount(8 * 1024 * 1024);
// each set keeps the seeds that found a bug, and a full sweep passed before these were cut down
const SEEDS: [u64; 6] = [0, 1, 2, 3, 17, 31];
const ROT_SEEDS: [u64; 6] = [1_000, 1_001, 1_002, 1_003, 1_011, 1_031];
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

type Store = ReelStore<SimulatedIo>;
type Key = (ColumnId, Vec<u8>);

/// One key's versions in write order, from the nothing it started as
#[derive(Debug)]
struct Versions {
    history: Vec<Option<Vec<u8>>>,
    floor: usize,
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
    steps_left: u64,
}

impl Default for Versions {
    fn default() -> Self {
        Self {
            history: vec![None],
            floor: 0,
        }
    }
}

fn config() -> StoreConfig {
    let volume = |name: &str, root: &str| VolumeConfig {
        name: name.to_string(),
        roots: vec![PathBuf::from(root)],
        sync: SyncPolicy::Never,
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
        volumes: vec![volume("meta", ROOTS[0]), volume("bulk", ROOTS[1])],
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

impl Run {
    fn new(seed: u64, has_rot: bool) -> Self {
        Self {
            seed,
            io: SimulatedIo::new(Seed(seed)),
            random: SeededRandom::new(Seed(seed)),
            model: Model::new(),
            has_rot,
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
            let opened = ReelStore::open(self.io.clone(), &config());
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
        match self.random.below(5) {
            0 => self.io.fail_syncs(count),
            1 => self.io.fill_writes(count),
            2 => self.io.fail_writes(count),
            3 => self.io.fail_creates(count),
            _last => self.io.fail_directory_syncs(count),
        }
    }

    fn clear_faults(&self) {
        let none = OperationCount(0);
        self.io.fail_syncs(none);
        self.io.fill_writes(none);
        self.io.fail_writes(none);
        self.io.fail_creates(none);
        self.io.fail_directory_syncs(none);
    }

    fn rot_one(&mut self) {
        if self.io.is_crashed() {
            return;
        }
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

        // half of the rot lands in the footer end of the file, where few bytes hold a lot
        let reach = if self.random.one_in(2) { length.min(TAIL_REACH) } else { length };
        let at = length - 1 - self.random.below(reach);
        self.io.rot(path, FileOffset(at), BitMask(1 << self.random.below(8))).expect("rot");
    }

    fn random_key(&mut self) -> Key {
        let column = COLUMNS[self.random.below(COLUMNS.len() as u64) as usize];
        (column, format!("key-{:04}", self.random.below(KEY_SPACE)).into_bytes())
    }

    fn write(&mut self, store: &Store) {
        let (column, key) = self.random_key();
        let value = (!self.random.one_in(3)).then(|| format!("value-{}", self.random.next_word()).into_bytes());
        let written = match &value {
            Some(value) => store.put(column, &key, value),
            None => store.delete(column, &key),
        };
        // a crash inside the call leaves its outcome open, and the next open shows which way it went
        if written.is_ok() || self.io.is_crashed() {
            self.model.entry((column, key)).or_default().history.push(value);
        }
    }

    fn flush(&mut self, store: &Store) {
        if store.flush().is_ok() {
            for versions in self.model.values_mut() {
                versions.floor = versions.history.len() - 1;
            }
        }
    }

    fn check_read(&mut self, store: &Store) {
        let (column, key) = self.random_key();
        let newest = self.model.get(&(column, key.clone())).and_then(|versions| versions.history.last().cloned());
        match read(store, column, &key) {
            Ok(found) => assert_eq!(found, newest.flatten(), "get of {key:?}"),
            Err(StoreError::Corruption { .. }) if self.has_rot => {},
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
                Err(StoreError::Corruption { .. }) if self.has_rot => continue,
                Err(error) => panic!("seed {}: get of {key:?} failed: {error}", self.seed),
            };
            let Some(offset) = versions.history[versions.floor..].iter().position(|value| *value == found) else {
                panic!("seed {}: key {key:?} read {found:?} below its floor, in {versions:?}", self.seed);
            };
            versions.history.truncate(versions.floor + offset + 1);
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
    for seed in SEEDS {
        Run::new(seed, false).run();
    }
}

// the same with rot: a read matches or says it can't, and never answers older or missing
#[test]
fn random_rot() {
    for seed in ROT_SEEDS {
        Run::new(seed, true).run();
    }
}
