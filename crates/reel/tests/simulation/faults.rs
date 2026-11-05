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
use reel_api::write_batch::WriteBatch;

const ROOTS: [&str; 3] = ["/disk/faults-meta", "/disk/faults-meta-two", "/disk/faults-bulk"];
const COLUMNS: [ColumnId; 2] = [ColumnId(1), ColumnId(2)];
const BUDGET: ByteCount = ByteCount(54 * 1024 * 1024);
// a test holds each seed with a traced bug: 0 `failed_open` and `failed_close`, 1 `rows_sync` and `tail_sync`, 37 and
// 349 `covered_last` and `later_sync`, 125 `one_copy`, 959 `flushed_unknown`, 1000 `open_covers`, 1002 `wrong_place`.
// Seeds 1 and 1000 are mapped from notes, with no replay. The rest are plain extra seeds
const SEEDS: [u64; 7] = [0, 1, 2, 3, 17, 31, 37];
const ROT_SEEDS: [u64; 9] = [125, 349, 959, 1_000, 1_001, 1_002, 1_003, 1_011, 1_031];
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
const BATCH_SETS: u64 = 6;
const SET_KEYS: u64 = 3;
const LONG_SET_ODDS: u64 = 40;
const LONG_SET_ROWS: u64 = 7_000;
const STREAM_MIX: u64 = 0x100_0000_01B3;
const ODDS_SCALE: u64 = 10;
const WRITE_ODDS: u64 = 3;
const SET_ODDS: u64 = 1;
const RANGE_ODDS: u64 = 1;
const RANGE_SCALE: u64 = 40;
const MOST_RANGE_KEYS: u64 = 6;
const FLUSH_ODDS: u64 = 1;
const SEAL_ODDS: u64 = 1;
const STEP_ODDS: u64 = 2;
const READ_ODDS: u64 = 2;

type Store = ReelStore<SimulatedIo>;
type Key = (ColumnId, Vec<u8>);

/// One key's versions in write order, from the nothing it started as
#[derive(Debug)]
struct Versions {
    history: Vec<Option<Vec<u8>>>,
    floor: usize,
    refused_at: Option<usize>,
    has_flushed_unknown: bool,
}

/// A plain map of versions
type Model = BTreeMap<Key, Versions>;

/// One random stream per kind of step, so a new kind leaves the draws of the others as they are
struct Streams {
    open: SeededRandom,
    crash: SeededRandom,
    fault: SeededRandom,
    rot: SeededRandom,
    write: SeededRandom,
    set: SeededRandom,
    range: SeededRandom,
    flush: SeededRandom,
    seal: SeededRandom,
    step: SeededRandom,
    read: SeededRandom,
}

impl Streams {
    fn new(seed: u64) -> Self {
        let stream = |kind: &str| {
            let tag = kind.bytes().fold(seed, |tag, byte| tag.wrapping_mul(STREAM_MIX).wrapping_add(u64::from(byte)));
            SeededRandom::new(Seed(tag))
        };
        Self {
            open: stream("open"),
            crash: stream("crash"),
            fault: stream("fault"),
            rot: stream("rot"),
            write: stream("write"),
            set: stream("set"),
            range: stream("range"),
            flush: stream("flush"),
            seal: stream("seal"),
            step: stream("step"),
            read: stream("read"),
        }
    }
}

/// One seeded run
struct Run {
    io: SimulatedIo,
    streams: Streams,
    model: Model,
    seed: u64,
    has_rot: bool,
    policy: SyncPolicy,
    steps_left: u64,
}

impl Versions {
    // a refusal after a crash tells the model nothing, so the versions it could hold stay in play until a read
    // or a newer write, and a live refusal changes nothing, since a known key's only answer is its newest
    fn is_unknown(&self) -> bool {
        self.refused_at == Some(self.history.len())
    }

    fn note_refusal(&mut self) {
        self.refused_at = Some(self.history.len());
    }

    // a flush covers whatever a key holds, so a flush while it's unknown lands on the version a read finds
    fn note_flush(&mut self) {
        if self.is_unknown() {
            self.has_flushed_unknown = true;
        } else {
            self.floor = self.history.len() - 1;
        }
    }

    // check a read with no crash since the key was known: a refusal is allowed, and an answer must be the newest
    fn check_live(&mut self, found: Option<&Option<Vec<u8>>>) -> bool {
        let Some(found) = found else {
            return true;
        };
        if self.is_unknown() {
            return self.accept(found);
        }
        self.history.last() == Some(found)
    }

    // take what a read found as the truth, once it's at or above what a sync covered
    fn accept(&mut self, found: &Option<Vec<u8>>) -> bool {
        let Some(offset) = self.history[self.floor..].iter().position(|value| value == found) else {
            return false;
        };
        self.history.truncate(self.floor + offset + 1);
        if self.has_flushed_unknown {
            self.floor = self.history.len() - 1;
        }
        self.has_flushed_unknown = false;
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
            has_flushed_unknown: false,
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

fn random_key(random: &mut SeededRandom) -> Key {
    let column = COLUMNS[random.below(COLUMNS.len() as u64) as usize];
    (column, format!("key-{:04}", random.below(KEY_SPACE)).into_bytes())
}

fn set_key(set: u64, number: u64) -> Vec<u8> {
    format!("set-{set:02}-{number}").into_bytes()
}

impl Run {
    fn new(seed: u64, has_rot: bool) -> Self {
        Self {
            seed,
            io: SimulatedIo::new(Seed(seed)),
            streams: Streams::new(seed),
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
            if self.streams.open.one_in(2) {
                self.io.arm_crash(OperationCount(self.streams.open.below(MOST_OPEN_DELAY)));
            }
            if self.streams.open.one_in(4) {
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
        match self.streams.crash.one_in(2) {
            true => self.io.power_cut(),
            false => self.io.kill(),
        }
    }

    fn arm_fault(&mut self) {
        let count = OperationCount(1 + self.streams.fault.below(3));
        match self.streams.fault.below(6) {
            0 => self.io.fail_syncs(count),
            1 => self.io.fill_writes(count),
            2 => self.io.fail_writes(count),
            3 => self.io.fail_creates(count),
            4 => self.io.fail_directory_syncs(count),
            _last => {
                let file = SYNCED_FILES[self.streams.fault.below(SYNCED_FILES.len() as u64) as usize];
                self.io.fail_file_sync(file, OperationCount(self.streams.fault.below(MOST_SYNC_SKIP)));
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
        match self.streams.rot.below(10) {
            0 => self.rot_rows(),
            1 => self.rot_summary(),
            _data => self.rot_data(),
        }
    }

    // rot in the summary costs rows that open builds again from the tails
    fn rot_summary(&mut self) {
        let root = Path::new(ROOTS[self.streams.rot.below(2) as usize * 2]);
        let paths = SummaryPaths::new(root);
        let Ok(file) = self.io.open_read_only(&paths.current) else {
            return;
        };
        let length = file.length();
        self.io.close_file(file);
        if let Ok(ByteCount(length)) = length
            && length > 0
        {
            let at = self.streams.rot.below(length);
            let _rotted = self.io.rot(&paths.current, FileOffset(at), BitMask(1));
        }
    }

    // rot in what a surviving mark covers, since rot in a synced group with its mark lost reads as a torn end
    fn rot_rows(&mut self) {
        let root = ROOTS[self.streams.rot.below(ROOTS.len() as u64) as usize];
        let Ok(listing) = self.io.list_directory(Path::new(root)) else {
            return;
        };
        let rows: Vec<&PathBuf> = listing.iter().filter(|path| path.extension().is_some_and(|name| name == "rows")).collect();
        if rows.is_empty() {
            return;
        }
        let path = rows[self.streams.rot.below(rows.len() as u64) as usize];
        let Some((covered, _data_end)) = durable_rows(&self.io, path) else {
            return;
        };
        if covered > 0 {
            let at = self.streams.rot.below(covered);
            let _rotted = self.io.rot(path, FileOffset(at), BitMask(1));
        }
    }

    fn rot_data(&mut self) {
        let files = data_files(&self.io);
        if files.is_empty() {
            return;
        }
        let path = &files[self.streams.rot.below(files.len() as u64) as usize];
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
                let at = self.streams.rot.below(data_end);
                let _rotted = self.io.rot(path, FileOffset(at), BitMask(1));
            }
            return;
        }

        // half of the rot lands in the footer end of the file, where few bytes hold a lot
        let reach = if self.streams.rot.one_in(2) { length.min(TAIL_REACH) } else { length };
        let at = length - 1 - self.streams.rot.below(reach);
        // a bad sector fails its reads for good, and rot leaves them passing with the wrong bytes
        if self.streams.rot.one_in(3) {
            self.io.bad_sector(path, FileOffset(at)).expect("bad");
            return;
        }
        self.io.rot(path, FileOffset(at), BitMask(1 << self.streams.rot.below(8))).expect("rot");
    }

    fn write(&mut self, store: &Store) {
        let (column, key) = random_key(&mut self.streams.write);
        let is_grave = self.streams.write.one_in(3);
        let is_large = self.streams.write.one_in(LARGE_VALUE_ODDS);
        let mut bytes = format!("value-{}", self.streams.write.next_word()).into_bytes();
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

    // a set's keys always hold one value, so a read of them after a crash shows a batch that landed in part
    fn write_set(&mut self, store: &Store) {
        let column = COLUMNS[self.streams.set.below(COLUMNS.len() as u64) as usize];
        let set = self.streams.set.below(BATCH_SETS);
        let is_grave = self.streams.set.one_in(4);
        let bytes = format!("set-{}", self.streams.set.next_word()).into_bytes();
        let value = (!is_grave).then_some(bytes);
        let is_long = self.streams.set.one_in(LONG_SET_ODDS);
        let is_range = self.streams.set.one_in(2);
        let mut batch = WriteBatch::new();
        for number in 0..SET_KEYS {
            let key = set_key(set, number);
            // the first of two operations on a key never shows, since the later one wins
            batch.put(column, &key, b"overwritten");
            match &value {
                Some(value) => batch.put(column, &key, value),
                None if is_range => {},
                None => batch.delete(column, &key),
            }
            // a long batch's first key sits in its first piece and the rest in its last, with graves no read looks at between
            if is_long && number == 0 {
                for filler in 0..LONG_SET_ROWS {
                    batch.delete(column, format!("filler-{filler:05}").as_bytes());
                }
            }
        }

        // one range delete takes the whole set, after the puts above, so a read must find all of it gone or none
        if value.is_none() && is_range {
            batch.delete_range(column, &set_key(set, 0), &set_key(set, SET_KEYS));
        }

        let written = store.write_batch(&batch);
        let has_landed = matches!(written, Err(StoreError::Io { action: "syncing a volume", .. }));
        if written.is_ok() || has_landed || self.io.is_crashed() {
            let is_covered = written.is_ok() && column == COLUMNS[0] && self.policy == SyncPolicy::EveryWrite;
            for number in 0..SET_KEYS {
                let versions = self.model.entry((column, set_key(set, number))).or_default();
                versions.history.push(value.clone());
                if is_covered {
                    versions.floor = versions.history.len() - 1;
                }
            }
        }
    }

    // a range of plain keys goes in one record, and every known key in it gets a nothing on top
    fn write_range(&mut self, store: &Store) {
        let column = COLUMNS[self.streams.range.below(COLUMNS.len() as u64) as usize];
        let first = self.streams.range.below(KEY_SPACE);
        let last = first + 1 + self.streams.range.below(MOST_RANGE_KEYS);
        let (start, end) = (format!("key-{first:04}").into_bytes(), format!("key-{last:04}").into_bytes());

        let written = store.delete_range(column, &start, &end);
        let has_landed = matches!(written, Err(StoreError::Io { action: "syncing a volume", .. }));
        if written.is_ok() || has_landed || self.io.is_crashed() {
            let is_covered = written.is_ok() && column == COLUMNS[0] && self.policy == SyncPolicy::EveryWrite;
            for number in first..last {
                let key = format!("key-{number:04}").into_bytes();
                let versions = self.model.entry((column, key)).or_default();
                versions.history.push(None);
                if is_covered {
                    versions.floor = versions.history.len() - 1;
                }
            }
        }
    }

    // a refused key says nothing, and every key that answers must agree
    fn check_sets(&self, store: &Store) {
        for column in COLUMNS {
            for set in 0..BATCH_SETS {
                let answers: Vec<_> = (0..SET_KEYS).filter_map(|number| read(store, column, &set_key(set, number)).ok()).collect();
                let is_whole = answers.iter().all(|answer| *answer == answers[0]);
                assert!(is_whole, "seed {}: set {set} landed in part: {answers:?}", self.seed);
            }
        }
    }

    fn flush(&mut self, store: &Store) {
        if store.flush().is_ok() {
            for versions in self.model.values_mut() {
                versions.note_flush();
            }
        }
    }

    fn check_read(&mut self, store: &Store) {
        let (column, key) = random_key(&mut self.streams.read);
        let versions = self.model.entry((column, key.clone())).or_default();
        match read(store, column, &key) {
            Ok(found) => assert!(versions.check_live(Some(&found)), "seed {}: get of {key:?} read {found:?}, in {versions:?}", self.seed),
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
            if self.streams.crash.one_in(CRASH_ODDS) {
                self.io.arm_crash(OperationCount(self.streams.crash.below(MOST_CRASH_DELAY)));
            }
            if self.streams.fault.one_in(FAULT_ODDS) {
                self.arm_fault();
            }
            if self.has_rot && self.streams.rot.one_in(ROT_ODDS) {
                self.rot_one();
            }

            // each kind draws on every step, used or not, so nothing another kind does moves its stream
            let is_write = self.streams.write.below(ODDS_SCALE) < WRITE_ODDS;
            let is_set = self.streams.set.below(ODDS_SCALE) < SET_ODDS;
            let is_range = self.streams.range.below(RANGE_SCALE) < RANGE_ODDS;
            let is_flush = self.streams.flush.below(ODDS_SCALE) < FLUSH_ODDS;
            let is_seal = self.streams.seal.below(ODDS_SCALE) < SEAL_ODDS;
            let is_step = self.streams.step.below(ODDS_SCALE) < STEP_ODDS;
            let is_read = self.streams.read.below(ODDS_SCALE) < READ_ODDS;
            if is_write {
                self.write(store);
            }
            if is_set && !self.io.is_crashed() {
                self.write_set(store);
            }
            if is_range && !self.io.is_crashed() {
                self.write_range(store);
            }
            if is_flush && !self.io.is_crashed() {
                self.flush(store);
            }
            if is_seal && !self.io.is_crashed() {
                store.seal_now();
            }
            if is_step && !self.io.is_crashed() {
                self.io.step_any();
                self.io.advance_clock(SETTLE_TICK);
            }
            if is_read && !self.io.is_crashed() {
                self.check_read(store);
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
            // open syncs what it shows, so what it showed can't go back
            versions.floor = versions.history.len() - 1;
        }
        self.check_sets(store);
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

// the model holds what the harness checks, so a stand-in that answers wrongly has to fail it
fn versions_of(values: &[Option<&[u8]>], floor: usize) -> Versions {
    Versions {
        history: values.iter().map(|value| value.map(<[u8]>::to_vec)).collect(),
        floor,
        ..Versions::default()
    }
}

// a live refusal widens nothing: an older version after it fails
#[test]
fn live_refusal() {
    let mut versions = versions_of(&[None, Some(b"a"), Some(b"b"), Some(b"c")], 1);

    assert!(versions.check_live(None));

    assert!(!versions.check_live(Some(&Some(b"b".to_vec()))));
    assert!(versions.check_live(Some(&Some(b"c".to_vec()))));
}

// a flush while a key is unknown holds once a read shows the version: an older one after a crash fails
#[test]
fn flushed_unknown() {
    let mut versions = versions_of(&[None, Some(b"a"), Some(b"b"), Some(b"c")], 1);
    versions.note_refusal();
    versions.note_flush();

    assert!(versions.accept(&Some(b"c".to_vec())));

    assert!(!versions.accept(&Some(b"a".to_vec())));
}
