//! Reel store engine root type
//!
//! The engine owns one reel over the volume and routes every operation by column:
//! a write appends through a tail and moves the index once the record has landed,
//! and a read resolves a key to a location and the refcounted segment handle that
//! keeps its file alive.

mod maintain;
mod read;
pub mod store_impl;
mod write;

#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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

/// Name of the file a writable open takes the volume's ownership lock on
pub(crate) const LOCK_FILE: &str = "reel.lock";

/// Sequence numbers a tombstone holds a key's place for before it is given back
///
/// A grave refuses a record drawn before it and published after it, so it is done
/// once no such record can still arrive.
pub(crate) const GRAVE_WINDOW: u64 = 1 << 20;

/// Bytes admitted between maintenance asks that make ingest hot
///
/// Deferring compaction is worth it only while ingest is spending the device, so a
/// trickle below this floor never holds a pass off. Escalated debt overrides hot.
const INGEST_HOT_BYTES: u64 = 8 * 1024 * 1024;

/// Keys one maintenance tick spends settling what standing covers took
///
/// Bounded so a tick stays a bounded pass whatever was dropped.
const SWEEP_RUN: usize = 1 << 16;

/// Openings this process has made, which is what tells their sweep marks apart
///
/// A counter rather than a clock or a random source: the only thing a mark has
/// to distinguish is one opening from another.
static OPENINGS: AtomicU32 = AtomicU32::new(0);

/// One write in a batch
pub enum RecordWrite {
    /// A payload to land under a key
    Put {
        /// Column and key the record is addressed by
        key: RecordKey,

        /// Payload to store, handed to the tail without a copy
        payload: Vec<u8>,
    },

    /// A key to tombstone
    Delete {
        /// Column and key to drop
        key: RecordKey,
    },

    /// A half-open key range to tombstone, which rides the batch like any record
    ///
    /// One record covers the whole range, so a caller cutting its batch around a range
    /// delete would be paying for reservations the engine does not need.
    DeleteRange {
        /// Column and inclusive start of the range
        start: RecordKey,

        /// Exclusive end, or nothing for a range with no upper bound
        end: Option<Vec<u8>>,
    },
}

/// What one put settles before the tail takes its buffer
///
/// Both doors plan through this, so a write reaches the device having done the same
/// work whichever one it came in through.
struct Planned {
    /// Payload as it will be stored, compressed if the column asks for it
    payload: Vec<u8>,

    /// The codec byte that produced those bytes
    codec: u8,
}

/// One key of a planned batch and what the index needs to land it
struct BatchKey {
    /// Column and key the record is addressed by
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

    /// Stand a cover over the half-open range opening at the key
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
///
/// A caller driving compaction to completion has to tell being held back from
/// having nothing left.
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
    /// Directory the volume's home piece lives in
    root: PathBuf,

    /// What this opening stamps into the sweep marks it mints
    sweep_nonce: u64,

    /// Configuration this volume was opened with
    config: ReelConfig,

    /// The io backend every op is filed through
    driver: Arc<IoDriver>,

    /// Ceiling on the bytes writers may have in flight
    budget: Arc<InflightBudget>,

    /// Read-side counters, for a caller that reports them
    reads: Arc<ReadCounters>,

    /// Segment descriptors kept open across reads
    fd_cache: Arc<FdCache>,

    /// Selection, rewrites and the scrub
    compactor: Compactor,

    /// Segment rewrites admitted at once
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

    /// Whether the volume runs against a real filesystem rather than a harness
    is_real_fs: bool,

    /// Read views held open, whose floor bounds what compaction may reclaim
    cues: Arc<CuePoints>,

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

    /// Open against a supplied backend, the injection point the harnesses drive
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
        // One cache for the volume: a descriptor is a process-wide resource, so a
        // bound per anything smaller would not be the ceiling it names.
        let fd_cache = Arc::new(FdCache::new(DEFAULT_FD_CACHE as usize));
        let is_writable = is_real_fs && !is_read_only;
        if is_writable {
            std::fs::create_dir_all(&root)?;
        }
        let lock = match is_writable {
            true => Some(OwnershipLock::try_acquire(&root.join(LOCK_FILE))?),
            false => None,
        };

        // Logged and kept, acted on by nothing: a log line needs a subscriber to
        // exist, so the facts stay on the store for a caller that reports them.
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

        // The disks the volumes sit on, together, so the append-only deadlock guard
        // has a ceiling. Zero where nothing can say, which admits every write.
        let capacity_bytes = match is_real_fs {
            true => roots
                .iter()
                .filter_map(|at| crate::reel::bias::capacity_bytes(at))
                .sum(),
            false => 0,
        };
        // The fast tier alone sizes the demotion threshold, since that tier's room
        // is what demotion protects.
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
        // The manifest names every root before anything reads one, so a missing
        // mount refuses here rather than reading as loss below.
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
        // Ahead of the rebuild, whose load takes covered segments' keys from the key runs
        if !is_read_only {
            index.key_runs().load(&driver, &root)?;
        }
        let rebuilt = rebuild_reel(&driver, &roots, &dead, &index)?;
        // Compaction only copies what the index can find, so a writable open over a
        // column it doesn't declare would drop that column's records.
        if let Some(column) = rebuilt.undeclared.filter(|_| !is_read_only) {
            return Err(ReelError::Config(format!(
                "the volume holds column {} and this open doesn't declare it",
                column.0,
            )));
        }
        for path in &rebuilt.quarantined {
            tracing::warn!("quarantined a foreign reel segment at {}", path.display());
        }
        // A crash keeps a tail's reservation claimed past its end, and nothing
        // writes that file again. Cutting each walked tail at its own length
        // gives the blocks back without touching a byte it holds. Best effort:
        // a store that cannot release still serves.
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
        // A reader starts its cursor where the rebuild left the volume, so its
        // first catch-up reads only what has been written since the open.
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
        // The volume exists now, so the index can be told where to read the footers
        // its sealed keys resolve through.
        index.set_footers(Arc::clone(&shared) as Arc<dyn FooterSource>);
        index.set_records(Arc::clone(&shared) as Arc<dyn RecordSource>);
        if is_read_only {
            index.follow();
        }
        // And the other direction: a seal writes down what its segment weighs, and
        // these are the counters that know.
        shared.set_segments(index.segments_handle());
        index.finish_open()?;

        // Nothing is waiting to be handed over: the only keys a rebuild leaves
        // in the map are the tails', and a tail is handed over when it seals.
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
        })
    }

    /// Bulk directory this store is rooted at
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

    /// What one sealed segment's footer says it holds, for a caller inspecting it
    ///
    /// Nothing for a segment with no footer, still being written or left by a seal.
    pub fn segment_footer(&self, segment: SegmentId) -> Result<Option<Arc<SegmentFooter>>> {
        self.reel.shared().footer_of(segment)
    }

    /// The io driver, for a caller that has to drain the backend itself
    ///
    /// A backend that neither files its own completions nor answers at submission
    /// leaves a pending future waiting for somebody to reap it.
    pub fn driver(&self) -> &Arc<IoDriver> {
        &self.driver
    }

    /// Which backend serves this volume, which is not always the one asked for
    ///
    /// `config().io_backend` is the request; this is the outcome. They differ
    /// whenever a ring was configured and the kernel would not set one up.
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

    /// Sync every active tail, the durability surface
    pub fn flush(&self) -> Result<()> {
        self.reel.flush()
    }

    /// The same flush awaited, with the fsync run where blocking is allowed
    pub async fn flush_wait(&self) -> Result<()> {
        self.reel.flush_wait().await
    }

    /// Flush and stop every active tail, leaving each where the next open resumes it
    ///
    /// Nothing seals on a close: a tail's segment persists across processes and only
    /// takes a footer when it fills. The open that follows appends where this one stopped.
    pub fn close(&self) -> Result<()> {
        if self.is_read_only {
            return Ok(());
        }
        self.reel.close()
    }

    /// Take a store off the disk, refusing one another owner still holds
    ///
    /// The ownership lock is taken first, since unlinking the files under a live
    /// writer leaves it appending into nothing. Every root the manifest names goes
    /// under that one claim, extras first and home last, so a destroy that dies
    /// partway leaves the manifest standing to finish the job.
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

    /// Memory the index is holding
    ///
    /// Accounted rather than observed, since a process footprint is an allocator's
    /// answer and a warm heap hides what a map just took off the free list.
    pub fn resident_bytes(&self) -> ByteCount {
        self.index.resident_bytes()
    }

    /// Total bytes in the footer cache across its footers, directories and blocks
    pub fn footer_cache_bytes(&self) -> ByteCount {
        ByteCount::from_bytes(self.reel.shared().footers.held_bytes() as u64)
    }

    /// The volumes the operator declared dead, empty on a whole store
    ///
    /// The records those drives held answer as missing, and the reel has nothing
    /// more to say about what was lost.
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
    ///
    /// Equal to the shipped ceiling while there is room, and falling through the band
    /// below the refusal ceiling. Tells a volume that is slow apart from one that is
    /// being slowed.
    pub fn write_budget_bytes(&self) -> ByteCount {
        self.reel.shared().budget.effective_ceiling()
    }

    /// A read of the maintenance counters
    pub fn compaction_counters(&self) -> CompactionCounters {
        self.compactor.counters()
    }

    /// What each column's keys do to the tree's lead search
    ///
    /// A column whose keys share their first eight bytes gets nothing from the lead
    /// array and falls back to comparing whole keys, which is correct and silent.
    pub fn lead_tie_rates(&self) -> Vec<(ColumnId, Option<f64>, u64)> {
        self.index.lead_tie_rates()
    }

    /// Which door this volume's ops took, ring or the fallback beside it
    ///
    /// A ring backend hands what it cannot serve to posix without saying so, so a
    /// leg that reports a ring number reports an assumption unless it reads this.
    pub fn door_counts(&self) -> crate::io::DoorCounts {
        self.driver.door_counts()
    }

    /// Device flushes this volume has asked for, the bill durability is paid in
    pub fn sync_count(&self) -> u64 {
        self.driver.sync_count()
    }

    /// What the machine said about itself when this volume opened
    ///
    /// Absent on a simulated volume. Nothing acts on it.
    pub fn bias(&self) -> Option<MachineFacts> {
        self.bias
    }

    /// Nanoseconds this volume spent waiting on the drive inside those flushes
    pub fn sync_nanos(&self) -> u64 {
        self.driver.sync_nanos()
    }

    /// Records a playback could not read and left out of its results
    pub fn unreadable_records(&self) -> u64 {
        self.reads.unreadable_records()
    }

    /// Note a record a playback could not read
    pub fn note_unreadable(&self) {
        self.reads.note_unreadable();
    }

    /// Whether the volume runs against a real filesystem rather than a harness
    pub fn is_real_fs(&self) -> bool {
        self.is_real_fs
    }
}

impl Drop for ReelStore {
    /// Flush the tails on the way out, so what they hold is durable
    ///
    /// A caller that wants to hear about a failure calls close itself; here a failure
    /// is traced and the volume is left the way a crash would leave it.
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            tracing::warn!("failed to seal a reel tail while closing the store: {error}");
        }
    }
}

fn read_only() -> ReelError {
    ReelError::Rejected("the reel was opened read only".to_string())
}
