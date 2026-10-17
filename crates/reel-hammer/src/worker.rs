//! One life of the hammer: a tape node's writes, reads and upkeep against a real volume

use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use reel::ReelStore;
use reel_core::{Direction, Store, Value, WriteBatch};

use crate::model::{check_window, header_of, show, value_of, Baseline, Held, Op, OpLog, Slot, HEADER};
use crate::stats::{heap, Stats};
use crate::{column_name, columns, open_store, Options};

const TAPE: u8 = 2;
const TRACK: u8 = 3;
const TRACK_DATA: u8 = 5;
const OBJECT_INFO: u8 = 6;
const SYNC_CURSOR: u8 = 9;
const SPOOL_STATUS: u8 = 11;
const PENDING_REPAIR: u8 = 12;
const SLICE: u8 = 14;
const REMOVAL_LOG: u8 = 35;

/// A node holds a few spools, so each track has a few slice keys
const SPOOLS: u16 = 3;

/// The node polls upkeep on its own task about this often
const MAINTAIN_EVERY: Duration = Duration::from_millis(250);

/// The first failure any thread saw, which stops the life
type Failure = Arc<RwLock<Option<String>>>;

fn fail(failure: &Failure, stop: &AtomicBool, error: String) {
    let mut first = failure.write().unwrap_or_else(|poisoned| poisoned.into_inner());
    first.get_or_insert(error);
    stop.store(true, Ordering::Relaxed);
}

/// Slices hold random bytes, metadata repeats so lz4 has work
fn plain(column: u8) -> bool {
    column != SLICE
}

fn address(tape: u64, track: u64) -> Vec<u8> {
    let mut address = Vec::with_capacity(32);
    for lane in 0..4u64 {
        let mut state = tape.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ track.rotate_left(17) ^ lane;
        state = (state ^ (state >> 31)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        address.extend_from_slice(&(state ^ (state >> 29)).to_be_bytes());
    }
    address
}

fn slice_key(tape: u64, track: u64, spool: u16) -> Vec<u8> {
    let mut key = address(tape, track);
    key.extend_from_slice(&spool.to_be_bytes());
    key
}

fn info_key(tape: u64, track: u64) -> Vec<u8> {
    let mut key = tape.to_be_bytes().to_vec();
    key.extend_from_slice(&track.to_be_bytes());
    key
}

fn number_at(key: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_be_bytes(key.get(at..at + 8)?.try_into().ok()?))
}

/// Most slices are small and a few reach 3 MiB, as on the fleet
fn slice_len(rng: &mut SmallRng) -> u32 {
    match rng.gen_range(0..100) {
        0..=69 => rng.gen_range(64 << 10..256 << 10),
        70..=94 => rng.gen_range(256 << 10..1 << 20),
        _ => rng.gen_range(1 << 20..3 << 20),
    }
}

struct TapeRow {
    number: u64,
    tracks: Vec<u64>,
    next_track: u64,
}

/// The one writer: it calls the store, logs what returned and keeps the model
struct Writer {
    store: Arc<ReelStore>,
    log: OpLog,
    held: HashMap<Slot, Held>,
    version: u64,
    stats: Arc<Stats>,
    batch: Option<WriteBatch>,
    pending: Vec<Op>,
    tapes: Vec<TapeRow>,
    next_tape: u64,
    removals: u64,
}

impl Writer {
    fn put(&mut self, column: u8, key: Vec<u8>, len: u32) -> Result<(), String> {
        let version = self.take_version();
        let value = value_of(column, &key, version, len, plain(column));
        let len = value.len() as u32;
        let name = column_name(column)?;
        match self.batch.as_mut() {
            Some(batch) => batch.put(name, &key, &value),
            None => {
                let start = Instant::now();
                Store::put(&*self.store, name, &key, &value).map_err(|error| format!("put: {error}"))?;
                self.stats.put.record(start.elapsed(), value.len());
            }
        }
        self.held.insert((column, key.clone()), Held { version, len });
        self.log_op(Op::Put { column, key, version, len })
    }

    fn delete(&mut self, column: u8, key: Vec<u8>) -> Result<(), String> {
        let version = self.take_version();
        let name = column_name(column)?;
        match self.batch.as_mut() {
            Some(batch) => batch.delete(name, &key),
            None => {
                let start = Instant::now();
                Store::delete(&*self.store, name, &key).map_err(|error| format!("delete: {error}"))?;
                self.stats.delete.record(start.elapsed(), 0);
            }
        }
        self.held.remove(&(column, key.clone()));
        self.log_op(Op::Delete { column, key, version })
    }

    fn delete_if_held(&mut self, column: u8, key: Vec<u8>) -> Result<(), String> {
        match self.held.contains_key(&(column, key.clone())) {
            true => self.delete(column, key),
            false => Ok(()),
        }
    }

    fn take_version(&mut self) -> u64 {
        self.version += 1;
        self.version - 1
    }

    /// A batch's ops reach the log once the batch has landed
    fn log_op(&mut self, op: Op) -> Result<(), String> {
        match self.batch.is_some() {
            true => {
                self.pending.push(op);
                Ok(())
            }
            false => self.log.append(&op),
        }
    }

    fn commit(&mut self) -> Result<(), String> {
        let Some(batch) = self.batch.take() else {
            return Ok(());
        };
        let start = Instant::now();
        self.store
            .write_batch(batch)
            .map_err(|error| format!("write_batch: {error}"))?;
        self.stats.batch.record(start.elapsed(), 0);
        self.log.append(&Op::BatchBegin)?;
        for op in std::mem::take(&mut self.pending) {
            self.log.append(&op)?;
        }
        self.log.append(&Op::BatchEnd)
    }

    fn flush(&mut self) -> Result<(), String> {
        let start = Instant::now();
        self.store.flush().map_err(|error| format!("flush: {error}"))?;
        self.stats.flush.record(start.elapsed(), 0);
        self.log.append(&Op::Flush)
    }

    /// One slot of chain events: new tracks with their rows and slices, then the cursor
    fn ingest_slot(&mut self, rng: &mut SmallRng) -> Result<(), String> {
        let batched = rng.gen_bool(0.2);
        if batched {
            self.batch = Some(WriteBatch::new());
        }
        if self.tapes.is_empty() || rng.gen_bool(0.05) {
            let number = self.next_tape;
            self.next_tape += 1;
            self.tapes.push(TapeRow { number, tracks: Vec::new(), next_track: 0 });
            self.put(TAPE, number.to_be_bytes().to_vec(), 180)?;
        }
        for _ in 0..rng.gen_range(1..=8) {
            self.ingest_track(rng)?;
        }
        let spool = rng.gen_range(0..20u16);
        self.put(SPOOL_STATUS, spool.to_be_bytes().to_vec(), rng.gen_range(60..200))?;
        self.put(SYNC_CURSOR, b"cursor".to_vec(), 40)?;
        if batched {
            self.commit()?;
        }
        Ok(())
    }

    /// The node writes a track, its tape row, its data and its object info, in that order
    fn ingest_track(&mut self, rng: &mut SmallRng) -> Result<(), String> {
        let at = rng.gen_range(0..self.tapes.len());
        let tape = &mut self.tapes[at];
        let (number, track) = (tape.number, tape.next_track);
        tape.next_track += 1;
        tape.tracks.push(track);
        self.put(TRACK, address(number, track), rng.gen_range(96..256))?;
        self.put(TAPE, number.to_be_bytes().to_vec(), 180)?;
        self.put(TRACK_DATA, address(number, track), rng.gen_range(40..120))?;
        self.put(OBJECT_INFO, info_key(number, track), rng.gen_range(80..400))?;
        for spool in 0..SPOOLS {
            match rng.gen_range(0..10) {
                0..=5 => self.put(SLICE, slice_key(number, track, spool), slice_len(rng))?,
                6 => self.put(PENDING_REPAIR, slice_key(number, track, spool), 48)?,
                _ => {}
            }
        }
        Ok(())
    }

    /// A tape expires or rewinds: every track goes in the node's delete order
    fn remove_tape(&mut self, rng: &mut SmallRng, expired: bool) -> Result<(), String> {
        if self.tapes.is_empty() {
            return Ok(());
        }
        let tape = self.tapes.swap_remove(rng.gen_range(0..self.tapes.len()));
        for track in &tape.tracks {
            self.delete(TRACK, address(tape.number, *track))?;
            self.delete(TRACK_DATA, address(tape.number, *track))?;
            self.delete(OBJECT_INFO, info_key(tape.number, *track))?;
            for spool in 0..SPOOLS {
                self.delete_if_held(SLICE, slice_key(tape.number, *track, spool))?;
                self.delete_if_held(PENDING_REPAIR, slice_key(tape.number, *track, spool))?;
            }
        }
        match expired {
            true => self.delete(TAPE, tape.number.to_be_bytes().to_vec())?,
            false => {
                self.put(TAPE, tape.number.to_be_bytes().to_vec(), 180)?;
                self.tapes.push(TapeRow { number: tape.number, tracks: Vec::new(), next_track: tape.next_track });
            }
        }
        let mut key = tape.number.to_be_bytes().to_vec();
        key.extend_from_slice(&self.removals.to_be_bytes());
        self.removals += 1;
        self.put(REMOVAL_LOG, key, 64)?;
        self.put(SYNC_CURSOR, b"cursor".to_vec(), 40)
    }

    /// Repair lands: pending rows clear and their slices arrive
    fn repair(&mut self, rng: &mut SmallRng) -> Result<(), String> {
        let pending: Vec<Vec<u8>> = self
            .held
            .keys()
            .filter(|(column, _)| *column == PENDING_REPAIR)
            .take(16)
            .map(|(_, key)| key.clone())
            .collect();
        for key in pending {
            self.delete(PENDING_REPAIR, key.clone())?;
            self.put(SLICE, key, slice_len(rng))?;
        }
        Ok(())
    }

    /// Object info and spool status get rewritten in place
    fn churn(&mut self, rng: &mut SmallRng) -> Result<(), String> {
        for _ in 0..rng.gen_range(10..200) {
            if let Some((number, track)) = self.pick_track(rng) {
                self.put(OBJECT_INFO, info_key(number, track), rng.gen_range(80..400))?;
            }
            let spool = rng.gen_range(0..20u16);
            self.put(SPOOL_STATUS, spool.to_be_bytes().to_vec(), rng.gen_range(60..200))?;
        }
        Ok(())
    }

    fn pick_track(&self, rng: &mut SmallRng) -> Option<(u64, u64)> {
        let tape = self.tapes.get(rng.gen_range(0..self.tapes.len().max(1)))?;
        let track = tape.tracks.get(rng.gen_range(0..tape.tracks.len().max(1)))?;
        Some((tape.number, *track))
    }

    /// The volume and the model agree key for key, in every column and in walk order
    fn checkpoint(&mut self, rng: &mut SmallRng) -> Result<(), String> {
        self.flush()?;
        let mut by_column: HashMap<u8, Vec<Vec<u8>>> = HashMap::new();
        for ((column, key), held) in &self.held {
            by_column.entry(*column).or_default().push(key.clone());
            let head = Store::get_range(&*self.store, column_name(*column)?, key, 0, HEADER)
                .map_err(|error| format!("get_range: {error}"))?
                .ok_or_else(|| format!("{} lost version {}", show(&(*column, key.clone())), held.version))?;
            let seen = header_of(*column, key, &head)?;
            if seen != *held {
                return Err(format!("{} serves version {} for {}", show(&(*column, key.clone())), seen.version, held.version));
            }
        }
        for spec in columns() {
            let column = spec.id.as_u8();
            let mut want = by_column.remove(&column).unwrap_or_default();
            want.sort();
            self.check_column(spec.name, column, &want, rng)?;
        }
        Ok(())
    }

    fn check_column(&self, name: &'static str, column: u8, want: &[Vec<u8>], rng: &mut SmallRng) -> Result<(), String> {
        let keys = self
            .store
            .iter_keys_prefix(name, b"")
            .map_err(|error| format!("keys of {name}: {error}"))?;
        if keys != want {
            let stray = keys
                .iter()
                .find(|key| want.binary_search(key).is_err())
                .map(|key| show(&(column, key.clone())));
            return Err(format!("{name} lists {} keys for {} held, first stray {stray:?}", keys.len(), want.len()));
        }
        if want.is_empty() {
            return Ok(());
        }
        let at = rng.gen_range(0..want.len());
        let start = Instant::now();
        let walked: Vec<Vec<u8>> = self
            .store
            .iter_from(name, &want[at], Direction::Asc)
            .map_err(|error| format!("iter_from: {error}"))?
            .take(64)
            .map(|(key, _)| key)
            .collect();
        self.stats.walk.record(start.elapsed(), 0);
        let expected: Vec<Vec<u8>> = want.iter().skip(at).take(64).cloned().collect();
        match walked == expected {
            true => Ok(()),
            false => Err(format!("a walk of {name} from {} strays from the held keys", show(&(column, want[at].clone())))),
        }
    }

    /// Rebuild the tape list from what the baseline holds
    fn adopt(&mut self) {
        let mut tapes: HashMap<u64, TapeRow> = HashMap::new();
        for (column, key) in self.held.keys() {
            let Some(number) = number_at(key, 0) else {
                continue;
            };
            let tape = tapes
                .entry(number)
                .or_insert(TapeRow { number, tracks: Vec::new(), next_track: 0 });
            if let (OBJECT_INFO, Some(track)) = (*column, number_at(key, 8)) {
                tape.tracks.push(track);
                tape.next_track = tape.next_track.max(track + 1);
            }
        }
        tapes.retain(|number, _| self.held.contains_key(&(TAPE, number.to_be_bytes().to_vec())));
        self.tapes = tapes.into_values().collect();
        self.tapes.sort_by_key(|tape| tape.number);
        let after_last = self.tapes.last().map_or(0, |tape| tape.number + 1);
        self.next_tape = after_last.max(self.version);
    }
}

/// Readers check every value they get while the writer runs
struct Reader {
    store: Arc<ReelStore>,
    sample: Arc<RwLock<Vec<Slot>>>,
    stats: Arc<Stats>,
    rng: SmallRng,
}

impl Reader {
    fn run(mut self, stop: Arc<AtomicBool>, failure: Failure) {
        while !stop.load(Ordering::Relaxed) {
            let Some((column, key)) = self.pick() else {
                thread::sleep(Duration::from_millis(50));
                continue;
            };
            if let Err(error) = self.read_one(column, &key) {
                fail(&failure, &stop, format!("reader: {error}"));
            }
        }
    }

    fn pick(&mut self) -> Option<Slot> {
        let keys = self.sample.read().unwrap_or_else(|poisoned| poisoned.into_inner());
        match keys.is_empty() {
            true => None,
            false => Some(keys[self.rng.gen_range(0..keys.len())].clone()),
        }
    }

    fn read_one(&mut self, column: u8, key: &[u8]) -> Result<(), String> {
        let name = column_name(column)?;
        match (self.rng.gen_range(0..10), column) {
            (0..=1, _) => self.walk(name, column, key),
            (2..=4, SLICE) => self.window(name, column, key),
            _ => self.get(name, column, key),
        }
    }

    /// The challenger and the health handler walk a few rows from a key
    fn walk(&mut self, name: &'static str, column: u8, key: &[u8]) -> Result<(), String> {
        let take = self.rng.gen_range(1..50);
        let start = Instant::now();
        let rows: Vec<(Vec<u8>, Value)> = self
            .store
            .iter_from(name, key, Direction::Asc)
            .map_err(|error| format!("walk: {error}"))?
            .take(take)
            .collect();
        self.stats.walk.record(start.elapsed(), 0);
        if !rows.windows(2).all(|pair| pair[0].0 < pair[1].0) {
            return Err(format!("a walk of {name} came back out of order"));
        }
        for (row_key, value) in &rows {
            let held = header_of(column, row_key, value)?;
            check_window(column, row_key, held, 0, value, plain(column))?;
        }
        Ok(())
    }

    /// A ranged read of a slice, the way a gateway asks for part of a track
    fn window(&mut self, name: &'static str, column: u8, key: &[u8]) -> Result<(), String> {
        let start = Instant::now();
        let head = Store::get_range(&*self.store, name, key, 0, HEADER).map_err(|error| format!("get_range: {error}"))?;
        let Some(head) = head else {
            return Ok(());
        };
        let held = header_of(column, key, &head)?;
        let offset = self.rng.gen_range(0..held.len as usize);
        let len = self.rng.gen_range(1..=(held.len as usize - offset).min(256 << 10));
        let window = Store::get_range(&*self.store, name, key, offset as u64, len).map_err(|error| format!("get_range: {error}"))?;
        self.stats.range.record(start.elapsed(), len);
        match window {
            Some(window) => check_window(column, key, held, offset, &window, plain(column)),
            None => Ok(()),
        }
    }

    fn get(&mut self, name: &'static str, column: u8, key: &[u8]) -> Result<(), String> {
        let start = Instant::now();
        let value = Store::get(&*self.store, name, key).map_err(|error| format!("get: {error}"))?;
        self.stats.get.record(start.elapsed(), value.as_ref().map_or(0, |value| value.len()));
        let Some(value) = value else {
            return Ok(());
        };
        let held = header_of(column, key, &value)?;
        check_window(column, key, held, 0, &value, plain(column))
    }
}

fn spawn_maintainer(store: Arc<ReelStore>, stats: Arc<Stats>, stop: Arc<AtomicBool>, failure: Failure) -> JoinHandle<()> {
    thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            let start = Instant::now();
            if let Err(error) = store.maintain() {
                fail(&failure, &stop, format!("maintain: {error}"));
            }
            stats.maintain.record(start.elapsed(), 0);
            thread::sleep(MAINTAIN_EVERY);
        }
    })
}

fn count_segments(volume: &std::path::Path) -> usize {
    let Ok(entries) = std::fs::read_dir(volume) else {
        return 0;
    };
    entries
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().is_some_and(|extension| extension == "reel"))
        .count()
}

fn spawn_metrics(options: &Options, life: u64, store: Arc<ReelStore>, stats: Arc<Stats>, stop: Arc<AtomicBool>) -> Result<JoinHandle<()>, String> {
    let path = options.dir.join("metrics.jsonl");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|error| format!("open {}: {error}", path.display()))?;
    let volume = options.dir.join("volume");
    let tick = options.tick;
    Ok(thread::spawn(move || {
        let mut last = Instant::now();
        while !stop.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(200));
            if last.elapsed() < tick {
                continue;
            }
            let seconds = last.elapsed().as_secs_f64();
            last = Instant::now();
            let line = serde_json::json!({
                "unix": SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |since| since.as_secs()),
                "life": life,
                "ops": stats.tick(seconds),
                "heap": heap(),
                "live_mib": store.live_data_size_bytes().ok().flatten().map(|bytes| bytes >> 20),
                "disk_mib": store.actual_size_bytes().ok().map(|bytes| bytes >> 20),
                "segments": count_segments(&volume),
            });
            let _ = writeln!(file, "{line}");
        }
    }))
}

/// Run one life, and say what broke if anything did
pub fn run(options: &Options, life: u64, base: Baseline) -> Result<(), String> {
    let store = Arc::new(open_store(options).map_err(|error| format!("open: {error}"))?);
    let stats = Arc::new(Stats::default());
    let stop = Arc::new(AtomicBool::new(false));
    let failure: Failure = Arc::new(RwLock::new(None));
    let sample: Arc<RwLock<Vec<Slot>>> = Arc::new(RwLock::new(base.held.keys().cloned().collect()));
    let mut rng = SmallRng::seed_from_u64(options.seed ^ life.wrapping_mul(0x9e37_79b9_7f4a_7c15));

    let mut threads = Vec::new();
    for _ in 0..options.readers {
        let reader = Reader { store: store.clone(), sample: sample.clone(), stats: stats.clone(), rng: SmallRng::seed_from_u64(rng.gen()) };
        let (stop, failure) = (stop.clone(), failure.clone());
        threads.push(thread::spawn(move || reader.run(stop, failure)));
    }
    threads.push(spawn_maintainer(store.clone(), stats.clone(), stop.clone(), failure.clone()));
    threads.push(spawn_metrics(options, life, store.clone(), stats.clone(), stop.clone())?);

    let mut writer = Writer {
        store: store.clone(),
        log: OpLog::create(&options.dir.join(format!("life-{life}.ops")))?,
        held: base.held,
        version: base.next_version,
        stats,
        batch: None,
        pending: Vec::new(),
        tapes: Vec::new(),
        next_tape: 0,
        removals: base.next_version,
    };
    writer.adopt();

    let outcome = write_until(&mut writer, options, &stop, &sample, &mut rng).and_then(|()| writer.checkpoint(&mut rng));
    stop.store(true, Ordering::Relaxed);
    for thread in threads {
        let _ = thread.join();
    }
    let failed = failure.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
    if let Some(error) = failed {
        return Err(error);
    }
    outcome?;
    store.close().map_err(|error| format!("close: {error}"))
}

/// The writer picks phases at random until the life ends or a thread fails
fn write_until(writer: &mut Writer, options: &Options, stop: &AtomicBool, sample: &RwLock<Vec<Slot>>, rng: &mut SmallRng) -> Result<(), String> {
    let deadline = Instant::now() + options.life;
    while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
        let live = writer.store.live_data_size_bytes().ok().flatten().unwrap_or(0);
        // A full volume only sheds tapes until it is back under the cap
        let phase = match live > options.max_gib << 30 {
            true => 40,
            false => rng.gen_range(0..100),
        };
        match phase {
            0..=39 => {
                for _ in 0..rng.gen_range(5..200) {
                    writer.ingest_slot(rng)?;
                }
            }
            40..=52 => writer.remove_tape(rng, true)?,
            53..=55 => writer.remove_tape(rng, false)?,
            56..=65 => writer.churn(rng)?,
            66..=73 => writer.repair(rng)?,
            // Idle tails, then a read storm with the writer at rest
            74..=80 => thread::sleep(Duration::from_millis(rng.gen_range(500..12_000))),
            81..=88 => thread::sleep(Duration::from_millis(rng.gen_range(500..5_000))),
            89..=94 => writer.flush()?,
            _ => writer.checkpoint(rng)?,
        }
        *sample.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = writer.held.keys().cloned().collect();
    }
    Ok(())
}
