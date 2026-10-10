//! Reel store engine, which routes every write and read by column

mod maintain;
mod read;
pub mod store_impl;
mod write;

#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::units::ByteCount;

use std::sync::atomic::{AtomicU32, AtomicU64};

use crate::append::admission::InflightBudget;
use crate::compaction::compactor::{CompactionCounters, Compactor};
use crate::config::{ReelConfig, DEFAULT_FD_CACHE};
use crate::error::{ReelError, Result};
use crate::format::column::{spec_by_name, ColumnId, ColumnSet, ColumnSpec, RecordKey};
use crate::format::footer::SegmentFooter;
use crate::format::loc::SegmentId;
use crate::format::lsn::Lsn;
use crate::index::counters::ReadCounters;
use crate::index::lockfile::OwnershipLock;
use crate::index::map::ReelIndex;
use crate::reel::bias::MachineFacts;
use crate::reel::cue::CuePoints;

use crate::compaction::pressure::PassPlane;
use crate::index::paged::FooterSource;
use crate::index::recovery::rebuild_reel;
use crate::index::spot::RecordSource;
use crate::index::tailer::LogCursor;
use crate::io::select::select_backend;
use crate::io::ReelIo;
use crate::reel::segment::{FdCache, IoDriver};
use crate::reel::{Reel, ReelShared};

/// A writable open takes the volume's ownership lock on this file
pub(crate) const LOCK_FILE: &str = "reel.lock";

/// A tombstone holds a key's place for this many sequence numbers
pub(crate) const GRAVE_WINDOW: u64 = 1 << 20;

/// Ingest is hot once this many bytes are admitted between maintenance asks
const INGEST_HOT_BYTES: u64 = 8 * 1024 * 1024;

/// One maintenance tick settles at most this many keys under standing covers
const SWEEP_RUN: usize = 1 << 16;

/// Counts this process's openings so each opening mints distinct sweep marks
static OPENINGS: AtomicU32 = AtomicU32::new(0);

/// One write in a batch
pub enum RecordWrite {
    /// A payload to land under a key
    Put {
        /// The record's column and key
        key: RecordKey,

        /// The payload, handed to the tail without a copy
        payload: Vec<u8>,
    },

    /// A key to tombstone
    Delete {
        /// Column and key to drop
        key: RecordKey,
    },

    /// A half-open key range to tombstone, written as one record in the batch
    DeleteRange {
        /// Column and inclusive start of the range
        start: RecordKey,

        /// Exclusive end, or nothing for a range with no upper bound
        end: Option<Vec<u8>>,
    },
}

/// A put's stored payload and codec, settled before the tail takes the buffer
struct Planned {
    /// Payload as it will be stored, compressed if the column asks for it
    payload: Vec<u8>,

    /// The codec byte that produced those bytes
    codec: u8,
}

/// One key of a planned batch and what the index needs to land it
struct BatchKey {
    /// The record's column and key
    key: RecordKey,

    /// What the index does for this key once the record has landed
    op: KeyOp,
}

/// What one record of a batch asks of the index
enum KeyOp {
    /// Point the key at the record that landed
    Put,

    /// Drop the key
    Delete,

    /// Put a cover over the half-open range that starts at the key
    Range(Option<Vec<u8>>),
}

/// Live count and byte total for a column or the whole store
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Totals {
    /// Number of live records
    pub count: u64,

    /// Total live payload bytes
    pub bytes: ByteCount,
}

/// What one compaction pass did
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactPass {
    /// The rate gate, the pressure model, read-only, or a running pass held it back
    Held,

    /// It ran and found nothing above the threshold
    Idle,

    /// It rewrote or unlinked at least one segment
    Copied,
}

/// Reel bulk-volume store over one directory of segment files
pub struct ReelStore {
    /// The volume's home directory
    root: PathBuf,

    /// This opening stamps this nonce into the sweep marks it mints
    sweep_nonce: u64,

    /// The configuration at open
    config: ReelConfig,

    /// Every op goes through this io driver
    driver: Arc<IoDriver>,

    /// Ceiling on the bytes writers may have in flight
    budget: Arc<InflightBudget>,

    /// Read-side counters, for a caller that reports them
    reads: Arc<ReadCounters>,

    /// Segment descriptors kept open across reads
    fd_cache: Arc<FdCache>,

    /// Picks segments, rewrites them and runs the scrub
    compactor: Compactor,

    /// Caps how many segment rewrites run at once
    compaction_plane: PassPlane,

    /// The append-only log of segment files
    reel: Reel,

    /// Index over every column served
    index: ReelIndex,

    /// Held so this owner keeps the volume until the store drops, never read
    _lock: Option<OwnershipLock>,

    /// How far a read-only follower has read the log
    cursor: Mutex<LogCursor>,

    /// Sealed segments whose keys have not been handed to their footers yet, in seal order
    held: Mutex<VecDeque<(SegmentId, Arc<SegmentFooter>)>>,

    /// Whether this open refuses every write
    is_read_only: bool,

    /// Whether the volume runs on a real filesystem, false under a harness
    is_real_fs: bool,

    /// Open read views, whose floor bounds what compaction may reclaim
    cues: Arc<CuePoints>,

    /// Each tail's write head at its last move, so the tick can seal a quiet tail
    idle: Mutex<Vec<Option<(SegmentId, u64, Instant)>>>,

    /// What the machine said about itself at open, absent on a simulated volume
    bias: Option<MachineFacts>,

    /// Bytes this volume occupies on disk, live and dead, as of the last tick
    footprint: AtomicU64,

    /// Admitted bytes at the last heat ask, which turns a counter into a rate
    ingest_marker: AtomicU64,
}

impl ReelStore {
    /// Open a reel store rooted at a bulk directory, taking the ownership lock
    pub fn open(root: PathBuf, config: ReelConfig, columns: ColumnSet) -> Result<Self> {
        let backend = select_backend(&config);
        ReelStore::open_inner(root, config, columns, backend, false, true)
    }

    /// Open read-only, rebuilding the index without taking the ownership lock
    pub fn open_read_only(root: PathBuf, config: ReelConfig, columns: ColumnSet) -> Result<Self> {
        let backend = select_backend(&config);
        ReelStore::open_inner(root, config, columns, backend, true, true)
    }

    /// Open against a supplied backend, for test harnesses
    pub fn open_with_io(
        root: PathBuf,
        config: ReelConfig,
        columns: ColumnSet,
        io: Arc<dyn ReelIo>,
    ) -> Result<Self> {
        ReelStore::open_inner(root, config, columns, io, false, false)
    }

    /// Read-only open against a supplied backend, for a crash-point reopen
    pub fn open_read_only_with_io(
        root: PathBuf,
        config: ReelConfig,
        columns: ColumnSet,
        io: Arc<dyn ReelIo>,
    ) -> Result<Self> {
        ReelStore::open_inner(root, config, columns, io, true, false)
    }

    fn open_inner(
        root: PathBuf,
        config: ReelConfig,
        columns: ColumnSet,
        io: Arc<dyn ReelIo>,
        is_read_only: bool,
        is_real_fs: bool,
    ) -> Result<Self> {
        config.validate()?;
        let driver = Arc::new(IoDriver::new(io));
        let budget = Arc::new(InflightBudget::default());
        let reads = Arc::new(ReadCounters::new());
        // One cache for the volume, since descriptors are a process-wide resource
        let fd_cache = Arc::new(FdCache::new(DEFAULT_FD_CACHE as usize));
        let is_writable = is_real_fs && !is_read_only;
        if is_writable {
            std::fs::create_dir_all(&root)?;
        }
        let lock = match is_writable {
            true => Some(OwnershipLock::try_acquire(&root.join(LOCK_FILE))?),
            false => None,
        };

        // The facts are logged and kept for callers, nothing acts on them
        let bias = is_real_fs.then(|| {
            let facts = MachineFacts::read(&root);
            let verdict = facts.verdict();
            tracing::info!(
                configured = ?config.io_backend,
                would_open_on = ?verdict.plane,
                because = verdict.because,
                memory_bytes = ?facts.memory_bytes,
                capacity_bytes = ?facts.volume_capacity_bytes,
                logical_block_bytes = ?facts.logical_block_bytes,
                is_rotational = ?facts.is_rotational,
                access_ranges = ?facts.access_ranges,
                actuator_spans = ?crate::reel::bias::access_ranges(&root),
                open_file_limit = ?facts.open_file_limit,
                ring = ?facts.ring,
                would_map_above = ?verdict.map_above,
                mapping_because = verdict.map_because,
                would_cache_descriptors = verdict.fd_cache,
                "bias pass read the machine",
            );
            facts
        });

        let mut roots = vec![root.clone()];
        roots.extend(config.volumes.iter().map(|volume| volume.path.clone()));

        // Total capacity under every root caps the deadlock guard, zero admits every write
        let capacity_bytes = match is_real_fs {
            true => roots
                .iter()
                .filter_map(|at| crate::reel::bias::capacity_bytes(at))
                .sum(),
            false => 0,
        };
        // The fast tier's capacity alone sizes the demotion threshold
        let fast_capacity = match is_real_fs {
            true => {
                crate::reel::bias::capacity_bytes(&root).unwrap_or(0)
                    + config
                        .volumes
                        .iter()
                        .filter(|volume| volume.class == crate::config::VolumeClass::Fast)
                        .filter_map(|volume| crate::reel::bias::capacity_bytes(&volume.path))
                        .sum::<u64>()
            }
            false => 0,
        };
        let compactor = Compactor::new(&config, capacity_bytes, fast_capacity);

        let index = ReelIndex::new(columns)?;
        // The manifest lists every root before any read, so a missing mount fails here
        let dead: Vec<bool> = std::iter::once(false)
            .chain(config.volumes.iter().map(|volume| volume.dead))
            .collect();
        if dead.contains(&true) {
            for (at, root) in roots.iter().enumerate().filter(|(at, _)| dead[*at]) {
                tracing::warn!(
                    volume = %root.display(),
                    at,
                    "opening degraded: every record this volume held answers as \
                     missing until it is refetched",
                );
            }
        }
        crate::reel::volumes::ensure_manifest(&driver, &roots, &dead, is_read_only)?;
        // The rebuild takes covered segments' keys from the key runs, so load them first
        if !is_read_only {
            index.key_runs().load(&driver, &root)?;
        }
        let rebuilt = rebuild_reel(&driver, &roots, &dead, &index)?;
        // Compaction copies only declared columns, so a writable open must declare every column
        if let Some(column) = rebuilt.undeclared.filter(|_| !is_read_only) {
            return Err(ReelError::Config(format!(
                "the volume holds column {} and this open doesn't declare it",
                column.0,
            )));
        }
        for path in &rebuilt.quarantined {
            tracing::warn!("quarantined a foreign reel segment at {}", path.display());
        }
        // Cut each walked tail to its length to free what a crash left reserved, best effort
        if !is_read_only {
            for (path, len) in &rebuilt.walked {
                let released = (|| -> Result<()> {
                    let file = driver.open(path, false)?;
                    let outcome = driver.truncate(file, *len);
                    driver.close(file)?;
                    outcome
                })();
                if let Err(error) = released {
                    tracing::warn!(
                        segment = %path.display(),
                        "failed to release a walked tail's reservation: {error}",
                    );
                }
            }
        }
        // Readers look for the footer at the file's end, so redo lost cuts before any read
        if !is_read_only {
            for (path, end) in &rebuilt.cuts {
                let file = driver.open(path, false)?;
                let cut = driver
                    .truncate(file, *end)
                    .and_then(|()| driver.sync_full(file));
                driver.close(file)?;
                cut?;
            }
        }
        // The cursor starts where the rebuild stopped, so catch-up reads only newer writes
        let mut cursor = LogCursor::new();
        cursor.start_from(&rebuilt.consumed);

        let shared = Arc::new(ReelShared::new(
            root.clone(),
            Arc::clone(&driver),
            Arc::clone(&budget),
            Arc::clone(&fd_cache),
            config.clone(),
            columns,
            1,
        ));
        shared.lsn.recover_to(rebuilt.highest_lsn);
        shared.recover_next_segment(rebuilt.highest_segment);
        for (segment, at) in &rebuilt.placements {
            shared.volumes.place(*segment, *at as usize);
        }

        let reel = match is_read_only {
            true => Reel::open_read_only(Arc::clone(&shared)),
            false => Reel::open(Arc::clone(&shared), rebuilt.resumable)?,
        };
        // The volume exists now, so the index can read footers and records through it
        index.set_footers(Arc::clone(&shared) as Arc<dyn FooterSource>);
        index.set_records(Arc::clone(&shared) as Arc<dyn RecordSource>);
        if is_read_only {
            index.follow();
        }
        // A seal records its segment's weight in the index's segment counters
        shared.set_segments(index.segments_handle());
        index.finish_open()?;

        // A rebuild leaves only tail keys in the map, and a tail hands its keys over at seal
        let held: VecDeque<(SegmentId, Arc<SegmentFooter>)> = VecDeque::new();

        Ok(ReelStore {
            bias,
            footprint: AtomicU64::new(0),
            ingest_marker: AtomicU64::new(0),
            sweep_nonce: (std::process::id() as u64) << 32
                | OPENINGS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u64,
            root,
            driver,
            budget,
            reads,
            fd_cache,
            compactor,
            compaction_plane: PassPlane::new(config.compact_passes()),
            config,
            reel,
            index,
            _lock: lock,
            cursor: Mutex::new(cursor),
            held: Mutex::new(held),
            is_read_only,
            is_real_fs,
            cues: Arc::new(CuePoints::new()),
            idle: Mutex::new(Vec::new()),
        })
    }

    /// The store's bulk root directory
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Effective configuration
    pub fn config(&self) -> &ReelConfig {
        &self.config
    }

    /// The columns this store serves
    pub fn columns(&self) -> ColumnSet {
        self.index.columns()
    }

    /// The declaration of one column by name, or nothing if it is not served
    pub fn column_spec(&self, name: &str) -> Option<&ColumnSpec> {
        spec_by_name(self.index.columns(), name)
    }

    /// The index, for a playback that pages it directly
    pub fn index(&self) -> &ReelIndex {
        &self.index
    }

    /// A sealed segment's footer, or nothing when the segment has none
    pub fn segment_footer(&self, segment: SegmentId) -> Result<Option<Arc<SegmentFooter>>> {
        self.reel.shared().footer_of(segment)
    }

    /// The io driver, for a caller that has to drain the backend itself
    pub fn driver(&self) -> &Arc<IoDriver> {
        &self.driver
    }

    /// The backend serving this volume, which may differ from the one configured
    pub fn serving_backend(&self) -> crate::io::ServingBackend {
        self.driver.serving()
    }

    /// The sequence number the volume has reached
    pub fn sequence(&self) -> Lsn {
        Lsn(self.reel.shared().lsn.peek().as_u64().saturating_sub(1))
    }

    /// The io driver's filing ledger, for diagnosing a stalled awaited op
    pub fn debug_io(&self) -> String {
        self.reel.shared().driver.debug_flights()
    }

    /// Cue points this volume is currently holding open
    pub fn cue_points(&self) -> &CuePoints {
        &self.cues
    }

    /// Slack in the byte counters from overwrites booked by length class
    pub fn spot_slack(&self) -> u64 {
        self.index.spot_slack()
    }

    /// Sync every active tail
    pub fn flush(&self) -> Result<()> {
        self.reel.flush()
    }

    /// The same flush awaited, with the fsync run where blocking is allowed
    pub async fn flush_wait(&self) -> Result<()> {
        self.reel.flush_wait().await
    }

    /// Compact away every sealed segment past the dead ratio, then flush and stop every active tail
    pub fn close(&self) -> Result<()> {
        if self.is_read_only {
            return Ok(());
        }
        self.drain()?;
        self.reel.close()
    }

    /// Delete a store from disk, failing if another owner holds its lock
    pub fn destroy(root: &Path) -> Result<()> {
        if !root.exists() {
            return Ok(());
        }
        let held = OwnershipLock::try_acquire(&root.join(LOCK_FILE))?;
        let outcome = (|| -> std::io::Result<()> {
            if let Ok(manifest) =
                std::fs::read_to_string(root.join(crate::reel::volumes::MANIFEST_NAME))
            {
                for line in manifest
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                {
                    let named = Path::new(line);
                    if named == root {
                        continue;
                    }
                    match std::fs::remove_dir_all(named) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error),
                    }
                }
            }
            std::fs::remove_dir_all(root)
        })();
        drop(held);
        match outcome {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(ReelError::Io(error)),
        }
    }

    /// The memory the index holds, by its own accounting
    pub fn resident_bytes(&self) -> ByteCount {
        self.index.resident_bytes()
    }

    /// Total bytes in the footer cache across its footers, directories and blocks
    pub fn footer_cache_bytes(&self) -> ByteCount {
        ByteCount::from_bytes(self.reel.shared().footers.held_bytes() as u64)
    }

    /// The volumes the operator declared dead, empty on a whole store
    pub fn dead_volumes(&self) -> Vec<PathBuf> {
        self.config
            .volumes
            .iter()
            .filter(|volume| volume.dead)
            .map(|volume| volume.path.clone())
            .collect()
    }

    /// Reclaimable bytes across every segment, the dead-space gauge
    pub fn dead_bytes(&self) -> ByteCount {
        ByteCount::from_bytes(self.index.dead_bytes())
    }

    /// The in-flight budget in force, which pressure lowers as the volume fills
    pub fn write_budget_bytes(&self) -> ByteCount {
        self.reel.shared().budget.effective_ceiling()
    }

    /// A read of the maintenance counters
    pub fn compaction_counters(&self) -> CompactionCounters {
        self.compactor.counters()
    }

    /// Each column's tie rate on the tree's eight-byte lead search
    pub fn lead_tie_rates(&self) -> Vec<(ColumnId, Option<f64>, u64)> {
        self.index.lead_tie_rates()
    }

    /// Which door this volume's ops took, ring or the fallback
    pub fn door_counts(&self) -> crate::io::DoorCounts {
        self.driver.door_counts()
    }

    /// How many device flushes this volume has asked for
    pub fn sync_count(&self) -> u64 {
        self.driver.sync_count()
    }

    /// What the machine reported at open, absent on a simulated volume
    pub fn bias(&self) -> Option<MachineFacts> {
        self.bias
    }

    /// Nanoseconds this volume has spent waiting on device flushes
    pub fn sync_nanos(&self) -> u64 {
        self.driver.sync_nanos()
    }

    /// How many records a playback could not read and left out of its results
    pub fn unreadable_records(&self) -> u64 {
        self.reads.unreadable_records()
    }

    /// Note a record a playback could not read
    pub fn note_unreadable(&self) {
        self.reads.note_unreadable();
    }

    /// Whether the volume runs on a real filesystem, false under a harness
    pub fn is_real_fs(&self) -> bool {
        self.is_real_fs
    }
}

impl Drop for ReelStore {
    /// Close the store on the way out, so what the tails hold is durable
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            tracing::warn!("failed to seal a reel tail while closing the store: {error}");
        }
    }
}

fn read_only() -> ReelError {
    ReelError::Rejected("the reel was opened read only".to_string())
}
