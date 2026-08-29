//! Reel store engine root type
//!
//! The engine owns one reel over the volume and routes every operation by column:
//! a write appends through a tail and moves the index once the record has landed,
//! and a read resolves a key to a location and the refcounted segment handle that
//! keeps its file alive.

pub mod index_checkpoint;
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
use crate::format::band::Band;
use crate::format::column::{spec_by_name, ColumnId, ColumnSet, ColumnSpec, RecordKey};
use crate::format::footer::SegmentFooter;
use crate::format::loc::SegmentId;
use crate::format::lsn::Lsn;
use crate::index::column::ColumnMark;
use crate::index::counters::ReadCounters;
use crate::index::lockfile::OwnershipLock;
use crate::index::map::ReelIndex;
use crate::index::page::KeyPage;
use crate::reel::bias::MachineFacts;
use crate::reel::cue::CuePoints;

use crate::compaction::pressure::PassPlane;
use crate::index::paged::FooterSource;
use crate::index::persisted::{admits, PersistedReader};
use crate::index::recovery::rebuild_from_persisted;
use crate::index::tailer::LogCursor;
use crate::io::select::select_backend;
use crate::io::ReelIo;
use crate::reel::segment::{FdCache, IoDriver};
use crate::reel::{Reel, ReelShared};
use reel_core::Value;

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

/// Segment rewrites the volume admits at once
///
/// One, because a second pass buys reclaim the device was already spending on the
/// first and takes the foreground's tail with it.
const COMPACT_PASSES: usize = 1;

/// Openings this process has made, which is what tells their sweep marks apart
///
/// A counter rather than a clock or a random source: the only thing a mark has
/// to distinguish is one opening from another.
static OPENINGS: AtomicU32 = AtomicU32::new(0);

/// A sealed segment whose keys are still resident, and when they stop being
///
/// A paged volume drains this queue every tick. A hot one keeps a segment here until
/// its wait is over or the budget reaches it, which makes the recent past cost one io.
struct Held {
    /// The sealed segment, kept in seal order so the oldest leaves first
    segment: SegmentId,

    /// When its keys may be handed over, at once for one an earlier process sealed
    ready_at: Instant,
}

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

    /// The whole payload where the column carries it in the index
    carried: Option<Arc<[u8]>>,
}

/// A resolved batch split into what the index answered and what the device owes
///
/// Both doors plan through this, so a batch reaches the device having done the same
/// work whichever one it came in through.
struct FoundPlan {
    /// One place per key asked for, filled where the index answered
    ///
    /// The one list a batch buys, since it is the one the caller takes away. What
    /// the device is asked for goes in the reading thread's own list beside it.
    answers: Vec<Option<Value>>,
}

/// One key of a planned batch and what the index needs to land it
struct BatchKey {
    /// Column and key the record is addressed by
    key: RecordKey,

    /// What the index does for this key once the record has landed
    op: KeyOp,

    /// The whole payload where the column carries it in the index
    carried: Option<Arc<[u8]>>,
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

    /// Resident index over every column served
    index: ReelIndex,

    /// Held so this owner keeps the volume until the store drops, never read
    _lock: Option<OwnershipLock>,

    /// How far a read-only follower has read the log
    cursor: Mutex<LogCursor>,

    /// Sealed segments whose keys have not been handed to their footers yet
    held: Mutex<VecDeque<Held>>,

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

/// The index a previous cue wrote down, where this open may believe any of it
///
/// Unarmed, nothing is read at all, and a paging volume never wrote one. A file
/// describing columns the store no longer serves is refused whole rather than in
/// part, since dropping a block's rows while still skipping its segments is the one
/// way this file loses a live record.
fn offered_index(
    driver: &IoDriver,
    root: &Path,
    config: &ReelConfig,
    columns: ColumnSet,
) -> Result<Option<PersistedReader>> {
    if !config.index_checkpoint || config.index.pages() {
        return Ok(None);
    }
    let Some(reader) = PersistedReader::open(driver, root)? else {
        return Ok(None);
    };
    if !admits(&reader.index, columns) {
        tracing::warn!(
            "the persisted index at {} describes another column set, so this open \
             sweeps the footers",
            root.display(),
        );
        reader.close(driver)?;
        return Ok(None);
    }
    Ok(Some(reader))
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
            // What `Preallocate::Full` would claim before a byte is written.
            let reservation = config.segment_bytes.to_bytes() * config.tail_count() as u64;
            let verdict = facts.verdict(reservation);
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
                would_read_windows_on = ?verdict.ranged_reads,
                would_preallocate = ?verdict.preallocate,
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

        let index = ReelIndex::new(columns, config.index, config.shard_shapes)?;
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
        let persisted = offered_index(&driver, &root, &config, columns)?;
        let rebuilt =
            rebuild_from_persisted(&driver, &roots, &dead, config.index.pages(), persisted)?;
        for path in &rebuilt.quarantined {
            tracing::warn!("quarantined a foreign reel segment at {}", path.display());
        }
        // Taken before the install, which takes the map with it.
        let mut on_disk: Vec<SegmentId> = rebuilt.segments.keys().copied().collect();
        on_disk.sort();
        // A paged rebuild left its sealed keys in the footers, so what it installs for
        // them is the span each segment covers, with nothing queued to hand over.
        index.install(
            rebuilt.entries,
            rebuilt.covers,
            rebuilt.segments,
            rebuilt.segment_min_lsn,
            rebuilt.segment_max_lsn,
            rebuilt.sealed,
            rebuilt.sealed_keys,
        );

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
            false => Reel::open(Arc::clone(&shared))?,
        };
        // The volume exists now, so the index can be told where to read the footers
        // a paged column resolves through. A resident one never asks.
        index.set_footers(Arc::clone(&shared) as Arc<dyn FooterSource>);
        // And the other direction: a seal writes down what its segment weighs, and
        // these are the counters that know.
        shared.set_segments(index.segments_handle());

        // Nothing is waiting to be handed over: the only keys a rebuild leaves
        // resident are the tails', and a tail is handed over when it seals.
        let held: VecDeque<Held> = VecDeque::new();

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
            compaction_plane: PassPlane::new(COMPACT_PASSES),
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

    /// The resident index, for a playback that pages it directly
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

    /// One page of a column's live keys, in no promised order
    ///
    /// The mark is opaque bytes: hand back whatever the last page answered, and
    /// nothing to start. `None` back means the column is done. Every live key is
    /// handed out at least once; a shard that resizes mid sweep starts over, so
    /// a caller has to be idempotent, which every caller of this is.
    pub fn sweep_column(
        &self,
        column: ColumnId,
        from: Option<&[u8]>,
        limit: usize,
        out: &mut KeyPage,
    ) -> Option<Vec<u8>> {
        let resumed = from.and_then(ColumnMark::unpack);
        let index = self.index.column(column)?;
        index
            .sweep(self.sweep_nonce, resumed.as_ref(), limit, out)
            .map(|mark| mark.pack())
    }

    /// One page of the keys under a shard-aligned prefix, in no promised order
    ///
    /// Nothing back where the prefix is not exactly the column's shard key, so a
    /// caller cannot turn a prefix walk into a scan of the family by asking for
    /// the wrong width.
    pub fn sweep_column_prefix(
        &self,
        column: ColumnId,
        prefix: &[u8],
        from: Option<&[u8]>,
        limit: usize,
        out: &mut KeyPage,
    ) -> Option<Vec<u8>> {
        let resumed = from.and_then(ColumnMark::unpack);
        let index = self.index.column(column)?;
        index
            .sweep_prefix(self.sweep_nonce, prefix, resumed.as_ref(), limit, out)
            .map(|mark| mark.pack())
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

    /// Sealed segments a rebuild left uncounted, standing until they retire
    ///
    /// While any stand, the totals promise only a floor: their keys were never
    /// joined, and each enters the counters as compaction or an overwrite touches it.
    pub fn born_segments(&self) -> usize {
        self.index.segments().born_count()
    }

    /// Sync every active tail, the durability surface
    pub fn flush(&self) -> Result<()> {
        self.reel.flush()
    }

    /// The same flush awaited, with the fsync run where blocking is allowed
    pub async fn flush_wait(&self) -> Result<()> {
        self.reel.flush_wait().await
    }

    /// Seal every active tail so the next open resolves the volume from footers
    ///
    /// A store dropped without this leaves one unsealed segment per tail, and the
    /// open that follows reads each of them back record by record.
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

    /// Memory the index is holding, which is what a residency tier trades away
    ///
    /// Accounted rather than observed, since a process footprint is an allocator's
    /// answer and a warm heap hides what a map just took off the free list.
    pub fn resident_bytes(&self) -> ByteCount {
        self.index.resident_bytes()
    }

    /// Bytes of carried values resident across every column
    pub fn carried_bytes(&self) -> ByteCount {
        ByteCount::from_bytes(self.index.carried_total())
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

    /// The band each foreground tail is drawing under right now
    pub fn tail_bands(&self) -> Vec<Option<Band>> {
        self.reel.tail_bands()
    }

    /// Banded writes that found no tail free and went to the unbanded ones instead
    ///
    /// A number that keeps climbing says the volume holds more live bands than it has
    /// tails, so the placement the caller asked for is not the one it is getting.
    pub fn band_fallbacks(&self) -> u64 {
        self.reel.band_fallbacks()
    }

    /// Give a closed window's tail back, sealing it behind the band
    ///
    /// A caller that knows when a window stops taking writes hands its tail back here
    /// rather than leaving the pool to work it out, which is the difference between a
    /// pool sized for the windows that are live and one sized for every window ever
    /// opened. False where no tail was on the band.
    pub fn release_band(&self, band: Band) -> Result<bool> {
        self.reel.release_band(band)
    }

    /// Whether windows may still be read around the page cache on this volume
    ///
    /// A filesystem that serves no direct open retires the route, so this tells a
    /// route that ran apart from one that retired. One segment refused a descriptor
    /// of its own does not show here.
    pub fn cold_direct_live(&self) -> bool {
        self.reel.shared().cold_direct_live()
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
    /// Seal the tails on the way out, so a clean shutdown reopens from footers
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
