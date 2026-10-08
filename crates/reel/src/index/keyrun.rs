//! Key runs: the address of each key's newest footer row, in key order, so a lost run only returns its segments to the walk

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock};

use crate::error::{ReelError, Result};
use crate::format::column::ColumnId;
use crate::format::footer::{FooterRow, VARYING_WIDTH};
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::Lsn;
use crate::format::prefix::PackedCursor;
use crate::format::record::{read_u32_le, read_u64_le, Flags};
use crate::index::paged::{FooterSource, MappedRows, RowsAt};
use crate::io::mapping::Mapping;
use crate::io::op::{FileId, WriteBuf};
use crate::reel::segment::IoDriver;

/// A row takes this many bytes: its segment's place in the covered list, then its place in that segment's footer partition
pub const ROW_LEN: usize = 4 + 4;

/// A block holds this many rows, and each block's first key stands as its fence
const RUN_BLOCK_ROWS: u32 = 64;

/// The writer gathers this many bytes before each write, so rows go down in large writes
const WRITE_BYTES: usize = 1 << 20;

/// A directory row takes this many bytes: column, width, block rows, rows, rows at, fences at
const DIRECTORY_ROW: usize = 1 + 2 + 4 + 8 + 8 + 8;

/// The trailer takes this many bytes: directory at, columns, covered count, magic
const TRAILER: usize = 8 + 4 + 4 + 4;

const MAGIC: u32 = u32::from_le_bytes(*b"KRN2");

/// Every key run's file name ends in this, so no segment scan mistakes one for a segment
pub const KEY_RUN_SUFFIX: &str = ".keys";

/// Format a key run's file name from its id
pub fn key_run_name(id: u64) -> String {
    format!("{id:012}{KEY_RUN_SUFFIX}")
}

/// The id in a key run's file name, or nothing for a file that is not one
pub fn key_run_id(name: &str) -> Option<u64> {
    name.strip_suffix(KEY_RUN_SUFFIX)?.parse().ok()
}

/// Where a key run's file sits under a root
pub fn key_run_path(root: &Path, id: u64) -> PathBuf {
    root.join(key_run_name(id))
}

/// What one row says about its record, once read through its footer
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RunRow {
    /// Sequence number of the record, which orders a key's versions
    pub lsn: Lsn,

    /// Where the record lies, in whichever segment it was written to
    pub loc: Loc,

    /// The record's own flags, which say which kind of record it is
    pub flags: Flags,
}

impl RunRow {
    /// A footer row of a segment, as a run row
    pub fn of(segment: SegmentId, found: FooterRow) -> RunRow {
        RunRow {
            lsn: found.lsn,
            loc: Loc::new(segment, found.offset, found.len),
            flags: found.flags,
        }
    }
}

/// Where a run row's footer row lies: which covered segment, and the row's place in its partition
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RunPointer {
    /// The segment's place in the run's covered list
    pub covered: u32,

    /// The row's place in that segment's footer partition for the column
    pub row: u32,
}

/// Reads the footer row a run row points at
pub trait RowReader {
    /// The key and row a pointer leads to, or nothing for a segment the reader skips
    fn read(&mut self, pointer: RunPointer) -> Result<Option<(&[u8], RunRow)>>;
}

/// A segment a reader was told stands had gone by the time its rows were asked for
pub fn vanished(segment: SegmentId) -> ReelError {
    ReelError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("segment {} retired under a key run read", segment.as_u32()),
    ))
}

/// Whether an error is a standing segment gone mid-read, which a caller retries or skips
pub fn is_vanished(error: &ReelError) -> bool {
    matches!(error, ReelError::Io(source) if source.kind() == std::io::ErrorKind::NotFound)
}

/// What every reader of one run column shares while the sealed set stands: which covered segments stand, and each one's mapped rows
pub struct RunViews {
    /// Whether each covered segment still stands, by its place in the covered list
    stands: Vec<bool>,

    /// Each covered segment's strided partition in place, taken on the first read
    mapped: Vec<OnceLock<Option<MappedRows>>>,
}

impl RunViews {
    /// The views of a run's covered segments, standing as `stands` says now
    pub fn new(run: &KeyRun, stands: &dyn Fn(SegmentId) -> bool) -> RunViews {
        RunViews {
            stands: run.covered.iter().map(|segment| stands(*segment)).collect(),
            mapped: (0..run.covered.len()).map(|_| OnceLock::new()).collect(),
        }
    }
}

/// Reads one run column's rows through the footers, keeping each covered segment's last block so a walk in key order reads each block once
pub struct FooterRows {
    footers: Arc<dyn FooterSource>,
    run: Arc<KeyRun>,
    column: ColumnId,
    views: Arc<RunViews>,

    /// Each covered segment's last block read, where its rows are not mapped
    held: Vec<Option<RowsAt>>,

    /// Each covered segment's place in its packed rows, so a walk decodes on from the row before
    cursors: Vec<PackedCursor>,
}

impl FooterRows {
    /// A reader of one run's column, skipping rows into segments `stands` rules out
    pub fn new(
        footers: Arc<dyn FooterSource>,
        run: Arc<KeyRun>,
        column: ColumnId,
        stands: &dyn Fn(SegmentId) -> bool,
    ) -> FooterRows {
        let views = Arc::new(RunViews::new(&run, stands));
        FooterRows::sharing(footers, run, column, views)
    }

    /// A reader sharing views another reader of the same run column already holds
    pub fn sharing(
        footers: Arc<dyn FooterSource>,
        run: Arc<KeyRun>,
        column: ColumnId,
        views: Arc<RunViews>,
    ) -> FooterRows {
        FooterRows {
            footers,
            run,
            column,
            views,
            held: Vec::new(),
            cursors: Vec::new(),
        }
    }

    /// The run this reader reads
    pub fn run(&self) -> &Arc<KeyRun> {
        &self.run
    }
}

impl RowReader for FooterRows {
    fn read(&mut self, pointer: RunPointer) -> Result<Option<(&[u8], RunRow)>> {
        let at = pointer.covered as usize;
        let Some(&segment) = self.run.covered.get(at) else {
            return Err(ReelError::Corruption(format!(
                "a key run row names covered segment {at} of {}",
                self.run.covered.len()
            )));
        };
        if !self.views.stands[at] {
            return Ok(None);
        }
        let mapped = self.views.mapped[at].get_or_init(|| {
            self.footers
                .mapped_rows(segment, self.column)
                .ok()
                .flatten()
        });
        if let Some(mapped) = mapped {
            if self.cursors.len() <= at {
                self.cursors.resize_with(self.run.covered.len(), PackedCursor::default);
            }
            let (key, found) = mapped.read(&mut self.cursors[at], pointer.row)?;
            return Ok(Some((key, RunRow::of(segment, found))));
        }
        if self.held.len() <= at {
            self.held.resize_with(self.run.covered.len(), || None);
        }
        if !self.held[at]
            .as_ref()
            .is_some_and(|rows| rows.holds(pointer.row))
        {
            let rows = self
                .footers
                .rows_holding(segment, self.column, pointer.row)?
                .ok_or_else(|| vanished(segment))?;
            self.held[at] = Some(rows);
        }
        let rows = self.held[at].as_ref().ok_or_else(|| vanished(segment))?;
        let (key, found) = rows.read(pointer.row)?;
        Ok(Some((key, RunRow::of(segment, found))))
    }
}

/// One column's rows in a key run
#[derive(Clone, Debug)]
pub struct RunColumn {
    /// Every row belongs to this column
    pub column: ColumnId,

    /// Every key in the column has this width, or VARYING_WIDTH where keys differ
    pub key_width: u16,

    /// Each block holds this many rows, the last one holding the rest
    block_rows: u32,

    /// The column's row count
    rows: u64,

    /// Where the column's first row lies in the file
    rows_at: u64,

    /// Each block's first key, back to back
    fences: Vec<u8>,

    /// Where each block's lead starts in `fences`, and one past the last
    fence_at: Vec<u32>,

    /// The column's last key, which bounds the run's reach
    last: Vec<u8>,
}

impl RunColumn {
    /// The column's row count
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// The column's block count
    pub fn blocks(&self) -> u32 {
        self.rows.div_ceil(u64::from(self.block_rows)) as u32
    }

    /// The column's lowest and highest key, nothing for an empty column
    pub fn key_range(&self) -> Option<(&[u8], &[u8])> {
        match self.rows {
            0 => None,
            _ => Some((self.lead(0), self.last.as_slice())),
        }
    }

    /// The first key of one block
    pub fn lead(&self, block: u32) -> &[u8] {
        let block = block as usize;
        &self.fences[self.fence_at[block] as usize..self.fence_at[block + 1] as usize]
    }

    /// The block holding the first row at or past a key
    pub fn block_for(&self, key: &[u8]) -> u32 {
        let (mut low, mut high) = (0u32, self.blocks());
        while low < high {
            let mid = (low + high) / 2;
            match self.lead(mid) <= key {
                true => low = mid + 1,
                false => high = mid,
            }
        }
        low.saturating_sub(1)
    }

    /// One block's first row index and row count
    pub fn block_span(&self, block: u32) -> (u64, u32) {
        let first = u64::from(block) * u64::from(self.block_rows);
        let count = (self.rows - first).min(u64::from(self.block_rows)) as u32;
        (first, count)
    }
}

/// A key run's bytes, mapped from a real file or read whole through the driver
enum Backing {
    Mapped(Mapping),
    Read(Vec<u8>),
}

impl Backing {
    /// The whole file
    fn bytes(&self) -> &[u8] {
        match self {
            Backing::Mapped(map) => map.slice(0, map.len() as usize).unwrap_or_default(),
            Backing::Read(bytes) => bytes,
        }
    }
}

/// A key run open for reading, its fences and directory held and its rows read in place
pub struct KeyRun {
    /// The run's id, which its file name holds and which orders runs by age
    pub id: u64,

    /// Where the file is
    pub path: PathBuf,

    /// The file's bytes, for the run's life
    backing: Backing,

    /// What unlinks the file once the run is retired
    driver: Arc<IoDriver>,

    /// Each column's rows, in column order
    columns: Vec<RunColumn>,

    /// The segments whose footers the run stands in for, which its rows point into by place
    pub covered: Vec<SegmentId>,

    /// The file's size in bytes, which a merge reading it pays
    pub bytes: u64,
}

impl KeyRun {
    /// Open one key run, reading its directory, fences and covered segments
    pub fn open(driver: &Arc<IoDriver>, path: &Path, id: u64) -> Result<KeyRun> {
        let backing = match Mapping::open(path, 0) {
            Some(map) => Backing::Mapped(map),
            None => {
                let file = driver.open(path, false)?;
                let read = driver
                    .length(file)
                    .and_then(|len| driver.pread(file, 0, len));
                let _ = driver.close(file);
                Backing::Read(read?)
            }
        };
        let file = backing.bytes();
        let bytes = file.len() as u64;
        let corrupt = |what: &str| ReelError::Corruption(format!("key run {id}: {what}"));
        let span = |at: u64, len: u64| {
            at.checked_add(len)
                .filter(|end| *end <= bytes)
                .map(|end| &file[at as usize..end as usize])
                .ok_or_else(|| corrupt("a region runs past the file"))
        };
        if bytes < TRAILER as u64 {
            return Err(corrupt("shorter than its trailer"));
        }
        let trailer = span(bytes - TRAILER as u64, TRAILER as u64)?;
        if read_u32_le(&trailer[16..20]) != MAGIC {
            return Err(corrupt("no magic"));
        }
        let directory_at = read_u64_le(&trailer[0..8]);
        let column_count = read_u32_le(&trailer[8..12]) as usize;
        let covered_count = read_u32_le(&trailer[12..16]) as usize;
        let directory_len = (column_count * DIRECTORY_ROW + covered_count * 4) as u64;
        if directory_at + directory_len + TRAILER as u64 != bytes {
            return Err(corrupt("directory does not fit the file"));
        }
        let directory = span(directory_at, directory_len)?;
        let mut columns = Vec::with_capacity(column_count);
        for at in 0..column_count {
            let row = &directory[at * DIRECTORY_ROW..(at + 1) * DIRECTORY_ROW];
            let key_width = u16::from_le_bytes([row[1], row[2]]);
            let block_rows = read_u32_le(&row[3..7]);
            let rows = read_u64_le(&row[7..15]);
            let rows_at = read_u64_le(&row[15..23]);
            let fences_at = read_u64_le(&row[23..31]);
            if key_width == 0 || block_rows == 0 {
                return Err(corrupt("a column with no width or no block size"));
            }
            span(
                rows_at,
                rows.checked_mul(ROW_LEN as u64)
                    .ok_or_else(|| corrupt("a column too long to hold"))?,
            )?;
            let blocks = rows.div_ceil(u64::from(block_rows));
            let mut fences = Vec::new();
            let mut fence_at = Vec::with_capacity(blocks as usize + 1);
            let mut at = fences_at;
            for _ in 0..blocks {
                let len = u64::from(u16::from_le_bytes(
                    span(at, 2)?.try_into().unwrap_or_default(),
                ));
                fence_at.push(fences.len() as u32);
                fences.extend_from_slice(span(at + 2, len)?);
                at += 2 + len;
            }
            fence_at.push(fences.len() as u32);
            let len = u64::from(u16::from_le_bytes(
                span(at, 2)?.try_into().unwrap_or_default(),
            ));
            let last = span(at + 2, len)?.to_vec();
            columns.push(RunColumn {
                column: ColumnId(row[0]),
                key_width,
                block_rows,
                rows,
                rows_at,
                fences,
                fence_at,
                last,
            });
        }
        let covered_at = column_count * DIRECTORY_ROW;
        let covered = (0..covered_count)
            .map(|at| {
                SegmentId(read_u32_le(
                    &directory[covered_at + at * 4..covered_at + at * 4 + 4],
                ))
            })
            .collect();
        Ok(KeyRun {
            id,
            path: path.to_path_buf(),
            backing,
            driver: Arc::clone(driver),
            columns,
            covered,
            bytes,
        })
    }

    /// One column's rows, or nothing where the run holds none of it
    pub fn column(&self, column: ColumnId) -> Option<&RunColumn> {
        self.columns.iter().find(|held| held.column == column)
    }

    /// The run's columns, in column order
    pub fn columns(&self) -> &[RunColumn] {
        &self.columns
    }

    /// Where one of a column's rows points
    pub fn pointer(&self, column: &RunColumn, at: u64) -> RunPointer {
        // Open checked that every column's rows lie inside the file
        let start = (column.rows_at + at * ROW_LEN as u64) as usize;
        let row = &self.backing.bytes()[start..start + ROW_LEN];
        RunPointer {
            covered: read_u32_le(&row[0..4]),
            row: read_u32_le(&row[4..8]),
        }
    }

    /// The segment a pointer points into, or nothing for a place past the covered list
    pub fn segment_of(&self, pointer: RunPointer) -> Option<SegmentId> {
        self.covered.get(pointer.covered as usize).copied()
    }

    /// The first row at or past a key, or strictly past it with `is_past`, stepping over rows the reader skips
    pub fn seek(
        &self,
        column: &RunColumn,
        key: &[u8],
        is_past: bool,
        rows: &mut dyn RowReader,
    ) -> Result<u64> {
        if column.rows == 0 {
            return Ok(0);
        }
        let (first, count) = column.block_span(column.block_for(key));
        let (mut low, mut high) = (first, first + u64::from(count));
        while low < high {
            let mid = (low + high) / 2;
            // The first row from the middle on that reads, standing in for the skipped ones before it
            let mut probe = mid;
            let mut found = None;
            while probe < high {
                if let Some((held, _)) = rows.read(self.pointer(column, probe))? {
                    found = Some(match is_past {
                        true => held <= key,
                        false => held < key,
                    });
                    break;
                }
                probe += 1;
            }
            match found {
                Some(true) => low = probe + 1,
                Some(false) | None => high = mid,
            }
        }
        Ok(low)
    }

    /// Unlink a retired run's file, which walks still holding the run can keep reading
    pub fn retire(&self) {
        let _ = self.driver.unlink(&self.path);
    }
}

/// A volume's key runs and the segments they stand in for during a walk
#[derive(Default)]
pub struct KeyRunSet {
    held: RwLock<KeyRunsHeld>,
    generation: AtomicU64,
    next_id: AtomicU64,
    merging: Mutex<()>,
}

#[derive(Default)]
struct KeyRunsHeld {
    runs: Vec<Arc<KeyRun>>,
    covered: HashSet<SegmentId>,
}

impl KeyRunSet {
    /// The set's change count, so a walk can tell its cached runs are stale
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// The runs held now, oldest first
    pub fn runs(&self) -> Vec<Arc<KeyRun>> {
        crate::sync::read(&self.held).runs.clone()
    }

    /// Whether a run answers for this segment in a walk
    pub fn covers(&self, segment: SegmentId) -> bool {
        crate::sync::read(&self.held).covered.contains(&segment)
    }

    /// The covered segments over all runs
    pub fn covered(&self) -> HashSet<SegmentId> {
        crate::sync::read(&self.held).covered.clone()
    }

    /// Hold the set for one merge, so two merges never take the same run
    pub fn try_merge(&self) -> Option<MutexGuard<'_, ()>> {
        crate::sync::try_lock(&self.merging)
    }

    /// Whether a run holds a row for a key below a sequence number in a standing segment, the only rows a walk can serve
    pub fn holds_older(
        &self,
        column: ColumnId,
        key: &[u8],
        lsn: Lsn,
        footers: &Arc<dyn FooterSource>,
        stands: &dyn Fn(SegmentId) -> bool,
    ) -> Result<bool> {
        for run in self.runs() {
            let Some(held) = run.column(column) else {
                continue;
            };
            let mut rows = FooterRows::new(Arc::clone(footers), Arc::clone(&run), column, stands);
            let mut at = match run.seek(held, key, false, &mut rows) {
                Ok(at) => at,
                Err(error) if is_vanished(&error) => continue,
                Err(error) => return Err(error),
            };
            // The seek lands past rows whose segment is gone, so the first one that reads is the answer
            while at < held.rows() {
                match rows.read(run.pointer(held, at)) {
                    Ok(Some((found, row))) => {
                        if found == key && row.lsn < lsn {
                            return Ok(true);
                        }
                        break;
                    }
                    Ok(None) => at += 1,
                    Err(error) if is_vanished(&error) => at += 1,
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(false)
    }

    /// Take the id for the next run
    pub fn draw_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Put a merged run in place of the runs it merged, handing those back to be unlinked
    pub fn install(&self, run: Arc<KeyRun>, merged: &[u64]) -> Vec<Arc<KeyRun>> {
        let mut held = crate::sync::write(&self.held);
        self.next_id.fetch_max(run.id, Ordering::AcqRel);
        let mut retired = Vec::new();
        held.runs.retain(|kept| match merged.contains(&kept.id) {
            true => {
                retired.push(Arc::clone(kept));
                false
            }
            false => true,
        });
        held.runs.push(run);
        held.runs.sort_by_key(|kept| kept.id);
        held.covered = held
            .runs
            .iter()
            .flat_map(|kept| kept.covered.iter().copied())
            .collect();
        self.generation.fetch_add(1, Ordering::AcqRel);
        retired
    }

    /// Read back the key runs on disk, unlinking unreadable ones and any covered whole by a newer run
    pub fn load(&self, driver: &Arc<IoDriver>, root: &Path) -> Result<()> {
        let mut runs = Vec::new();
        for entry in driver.list_or_empty(root)? {
            let path = root.join(&entry.name);
            if entry.name.ends_with(".part") && entry.name.contains(KEY_RUN_SUFFIX) {
                let _ = driver.unlink(&path);
                continue;
            }
            let Some(id) = key_run_id(&entry.name) else {
                continue;
            };
            match KeyRun::open(driver, &path, id) {
                Ok(run) => runs.push(run),
                Err(_) => {
                    let _ = driver.unlink(&path);
                }
            }
        }
        runs.sort_by_key(|run| run.id);
        let mut kept: Vec<KeyRun> = Vec::new();
        while let Some(run) = runs.pop() {
            let is_within = kept.iter().any(|newer| {
                run.covered
                    .iter()
                    .all(|segment| newer.covered.contains(segment))
            });
            match is_within {
                true => run.retire(),
                false => kept.push(run),
            }
        }
        for run in kept {
            self.install(Arc::new(run), &[]);
        }
        Ok(())
    }
}

/// A column being written, its rows already down and its fences gathered
struct Building {
    column: ColumnId,
    key_width: u16,
    rows: u64,
    rows_at: u64,
    fences: Vec<u8>,
    last: Vec<u8>,
}

/// Writes one key run, which shows up under its own name only once whole and synced
pub struct RunWriter<'d> {
    driver: &'d IoDriver,
    file: FileId,
    temp: PathBuf,
    path: PathBuf,
    root: PathBuf,
    at: u64,
    pending: Vec<u8>,
    columns: Vec<Building>,
}

impl<'d> RunWriter<'d> {
    /// Start a key run under a root
    pub fn create(driver: &'d IoDriver, root: &Path, id: u64) -> Result<RunWriter<'d>> {
        let path = key_run_path(root, id);
        let temp = root.join(format!("{}.part", key_run_name(id)));
        let file = driver.open(&temp, true)?;
        Ok(RunWriter {
            driver,
            file,
            temp,
            path,
            root: root.to_path_buf(),
            at: 0,
            pending: Vec::with_capacity(WRITE_BYTES),
            columns: Vec::new(),
        })
    }

    /// Open the next column, which has to come after the one before it
    pub fn begin_column(&mut self, column: ColumnId, key_width: u16) -> Result<()> {
        if self
            .columns
            .last()
            .is_some_and(|held| held.column >= column)
        {
            return Err(ReelError::Rejected(
                "a key run's columns arrived out of order".to_string(),
            ));
        }
        self.columns.push(Building {
            column,
            key_width,
            rows: 0,
            rows_at: self.at + self.pending.len() as u64,
            fences: Vec::new(),
            last: Vec::new(),
        });
        Ok(())
    }

    /// Add one row to the open column, at a key above the last one it took
    pub fn push(&mut self, key: &[u8], pointer: RunPointer) -> Result<()> {
        let building = self.columns.last_mut().ok_or_else(|| {
            ReelError::Rejected("a key run row came before any column".to_string())
        })?;
        let width = u16::try_from(key.len())
            .ok()
            .filter(|width| *width != VARYING_WIDTH)
            .ok_or_else(|| ReelError::Rejected("a key run row's key is too wide".to_string()))?;
        if building.key_width != VARYING_WIDTH && width != building.key_width {
            return Err(ReelError::Rejected(
                "a key run row is not its column's width".to_string(),
            ));
        }
        if building.rows > 0 && key <= building.last.as_slice() {
            return Err(ReelError::Rejected(
                "a key run's rows arrived out of order".to_string(),
            ));
        }
        if building.rows.is_multiple_of(u64::from(RUN_BLOCK_ROWS)) {
            building.fences.extend_from_slice(&width.to_le_bytes());
            building.fences.extend_from_slice(key);
        }
        building.last.clear();
        building.last.extend_from_slice(key);
        building.rows += 1;
        self.pending
            .extend_from_slice(&pointer.covered.to_le_bytes());
        self.pending.extend_from_slice(&pointer.row.to_le_bytes());
        if self.pending.len() >= WRITE_BYTES {
            self.write_pending()?;
        }
        Ok(())
    }

    /// The run's row count so far, over every column
    pub fn rows(&self) -> u64 {
        self.columns.iter().map(|column| column.rows).sum()
    }

    fn write_pending(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let bytes = std::mem::replace(&mut self.pending, Vec::with_capacity(WRITE_BYTES));
        let len = bytes.len() as u64;
        self.driver
            .writev_all(self.file, self.at, vec![WriteBuf::owned(bytes)])?;
        self.at += len;
        Ok(())
    }

    /// Write the fences, the covered segments the rows were written against, the directory and the trailer, and rename the run in place
    pub fn finish(mut self, covered: &[SegmentId]) -> Result<PathBuf> {
        self.write_pending()?;
        let mut tail = Vec::new();
        let mut fences_at = Vec::with_capacity(self.columns.len());
        for building in &self.columns {
            fences_at.push(self.at + tail.len() as u64);
            tail.extend_from_slice(&building.fences);
            tail.extend_from_slice(&(building.last.len() as u16).to_le_bytes());
            tail.extend_from_slice(&building.last);
        }
        let directory_at = self.at + tail.len() as u64;
        for (building, fences_at) in self.columns.iter().zip(&fences_at) {
            tail.push(building.column.0);
            tail.extend_from_slice(&building.key_width.to_le_bytes());
            tail.extend_from_slice(&RUN_BLOCK_ROWS.to_le_bytes());
            tail.extend_from_slice(&building.rows.to_le_bytes());
            tail.extend_from_slice(&building.rows_at.to_le_bytes());
            tail.extend_from_slice(&fences_at.to_le_bytes());
        }
        for segment in covered {
            tail.extend_from_slice(&segment.as_u32().to_le_bytes());
        }
        tail.extend_from_slice(&directory_at.to_le_bytes());
        tail.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());
        tail.extend_from_slice(&(covered.len() as u32).to_le_bytes());
        tail.extend_from_slice(&MAGIC.to_le_bytes());
        self.driver
            .writev_all(self.file, self.at, vec![WriteBuf::owned(tail)])?;
        self.driver.sync_full(self.file)?;
        self.driver.close(self.file)?;
        self.driver.rename(&self.temp, &self.path)?;
        self.driver.sync_dir(&self.root)?;
        Ok(self.path)
    }

    /// Drop a run that will not be finished, its temporary file with it
    pub fn abandon(self) {
        let _ = self.driver.close(self.file);
        let _ = self.driver.unlink(&self.temp);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use super::*;
    use crate::io::fault::FaultPlan;
    use crate::io::sim_backend::SimIo;

    const ROOT: &str = "/runs";

    fn driver(sim: &SimIo) -> Arc<IoDriver> {
        Arc::new(IoDriver::new(Arc::new(sim.clone())))
    }

    fn key(n: u32) -> [u8; 8] {
        u64::from(n).wrapping_mul(0x9E37_79B9).to_be_bytes()
    }

    /// Row n of the run points at row n of covered segment n % 3
    fn pointer(n: u32) -> RunPointer {
        RunPointer {
            covered: n % 3,
            row: n,
        }
    }

    /// Reads a pointer's key out of the keys the run was written with, skipping the covered places listed gone
    struct Keys {
        keys: Vec<Vec<u8>>,
        gone: HashSet<u32>,
    }

    impl RowReader for Keys {
        fn read(&mut self, pointer: RunPointer) -> Result<Option<(&[u8], RunRow)>> {
            if self.gone.contains(&pointer.covered) {
                return Ok(None);
            }
            let row = RunRow {
                lsn: Lsn(u64::from(pointer.row)),
                loc: Loc::new(SegmentId(pointer.covered + 1), pointer.row, 1),
                flags: Flags::DATA,
            };
            Ok(Some((self.keys[pointer.row as usize].as_slice(), row)))
        }
    }

    fn written(driver: &Arc<IoDriver>, id: u64, keys: &[Vec<u8>], width: u16) -> KeyRun {
        let mut writer = RunWriter::create(driver, Path::new(ROOT), id).expect("create");
        writer.begin_column(ColumnId(1), width).expect("column");
        for (n, key) in keys.iter().enumerate() {
            writer.push(key, pointer(n as u32)).expect("push");
        }
        let path = writer
            .finish(&[SegmentId(4), SegmentId(9), SegmentId(12)])
            .expect("finish");
        KeyRun::open(driver, &path, id).expect("open")
    }

    // a run reads back every pointer it was written with, in order, and its fences bound its keys
    #[test]
    fn a_written_run_reads_back_its_pointers() {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = driver(&sim);
        let mut keys: Vec<Vec<u8>> = (0..2_000u32).map(|n| key(n).to_vec()).collect();
        keys.sort();
        let run = written(&driver, 3, &keys, 8);
        assert_eq!(run.covered, vec![SegmentId(4), SegmentId(9), SegmentId(12)]);
        let column = run.column(ColumnId(1)).expect("column");
        assert_eq!(column.rows(), 2_000);
        for at in 0..keys.len() as u32 {
            assert_eq!(run.pointer(column, u64::from(at)), pointer(at));
        }
        assert_eq!(run.segment_of(pointer(4)), Some(SegmentId(9)));
        assert_eq!(
            column
                .key_range()
                .map(|(low, high)| (low.to_vec(), high.to_vec())),
            Some((keys[0].clone(), keys[keys.len() - 1].clone()))
        );
        assert_eq!(run.bytes, std::fs::metadata(&run.path).map_or(run.bytes, |held| held.len()));
    }

    // a seek lands on the first row at or past a key, or past it alone
    #[test]
    fn a_seek_lands_on_the_first_row_at_or_past_the_key() {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = driver(&sim);
        let keys: Vec<Vec<u8>> = (0..5_000u64).map(|n| (n * 2).to_be_bytes().to_vec()).collect();
        let run = written(&driver, 1, &keys, 8);
        let column = run.column(ColumnId(1)).expect("column");
        let mut rows = Keys {
            keys: keys.clone(),
            gone: HashSet::new(),
        };
        for probe in [0u64, 1, 2, 1_001, 4_000, 9_998, 9_999, 20_000] {
            let target = probe.to_be_bytes();
            let at = keys
                .iter()
                .position(|key| key.as_slice() >= target.as_slice())
                .unwrap_or(keys.len()) as u64;
            let past = keys
                .iter()
                .position(|key| key.as_slice() > target.as_slice())
                .unwrap_or(keys.len()) as u64;
            assert_eq!(run.seek(column, &target, false, &mut rows).expect("seek"), at, "probe {probe} at");
            assert_eq!(run.seek(column, &target, true, &mut rows).expect("seek"), past, "probe {probe} past");
        }
    }

    // a seek steps past rows whose segment is gone, landing where the first row that reads at or past the key is next
    #[test]
    fn a_seek_steps_past_rows_into_segments_gone() {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = driver(&sim);
        let keys: Vec<Vec<u8>> = (0..5_000u64).map(|n| (n * 2).to_be_bytes().to_vec()).collect();
        let run = written(&driver, 2, &keys, 8);
        let column = run.column(ColumnId(1)).expect("column");
        let mut rows = Keys {
            keys: keys.clone(),
            gone: [1u32].into_iter().collect(),
        };
        let reads = |at: usize| pointer(at as u32).covered != 1;
        for probe in [0u64, 1, 2, 3, 7, 1_001, 4_000, 9_996, 9_997, 9_998, 9_999, 20_000] {
            let target = probe.to_be_bytes();
            let want = (0..keys.len()).find(|at| reads(*at) && keys[*at].as_slice() >= target.as_slice());
            let mut at = run.seek(column, &target, false, &mut rows).expect("seek") as usize;
            while at < keys.len() && !reads(at) {
                at += 1;
            }
            assert_eq!((at < keys.len()).then_some(at), want, "probe {probe}");
        }
    }

    // keys of different widths keep their own widths in the fences and seek the way fixed ones do
    #[test]
    fn a_varying_column_seeks_through_its_fences() {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = driver(&sim);
        let mut keys: Vec<Vec<u8>> = (0..3_000u32)
            .map(|n| format!("k{}", n * 7).into_bytes())
            .collect();
        keys.sort();
        keys.dedup();
        let run = written(&driver, 5, &keys, VARYING_WIDTH);
        let column = run.column(ColumnId(1)).expect("varying");
        assert_eq!(
            column
                .key_range()
                .map(|(low, high)| (low.to_vec(), high.to_vec())),
            Some((keys[0].clone(), keys[keys.len() - 1].clone()))
        );
        let mut rows = Keys {
            keys: keys.clone(),
            gone: HashSet::new(),
        };
        for probe in [
            b"k".to_vec(),
            b"k0".to_vec(),
            b"k10".to_vec(),
            b"k5000".to_vec(),
            b"z".to_vec(),
        ] {
            let at = keys
                .iter()
                .position(|key| key >= &probe)
                .unwrap_or(keys.len()) as u64;
            let past = keys
                .iter()
                .position(|key| key > &probe)
                .unwrap_or(keys.len()) as u64;
            assert_eq!(run.seek(column, &probe, false, &mut rows).expect("seek"), at, "probe {probe:?} at");
            assert_eq!(run.seek(column, &probe, true, &mut rows).expect("seek"), past, "probe {probe:?} past");
        }
    }

    // rows out of order are refused, so a walk can trust the run is sorted
    #[test]
    fn rows_out_of_order_are_refused() {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = driver(&sim);
        let mut writer = RunWriter::create(&driver, Path::new(ROOT), 2).expect("create");
        writer.begin_column(ColumnId(1), 8).expect("column");
        writer.push(&key(9), pointer(9)).expect("push");
        assert!(writer.push(&key(9), pointer(9)).is_err());
        writer.abandon();
    }

    // a row takes eight bytes, whatever the key
    #[test]
    fn a_row_takes_eight_bytes_whatever_the_key() {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = driver(&sim);
        let narrow: Vec<Vec<u8>> = (0..4_096u64).map(|n| n.to_be_bytes().to_vec()).collect();
        let wide: Vec<Vec<u8>> = (0..4_096u64)
            .map(|n| [n.to_be_bytes().as_slice(), &[7u8; 56]].concat())
            .collect();
        let narrow = written(&driver, 7, &narrow, 8);
        let wide = written(&driver, 8, &wide, 64);
        let rows = 4_096 * ROW_LEN as u64;
        // The fences add a key per block, so the wide run grows only by its keys there
        let fences = (4_096 / u64::from(RUN_BLOCK_ROWS) + 1) * 56;
        assert_eq!(wide.bytes - narrow.bytes, fences, "a row grew with its key");
        assert!(narrow.bytes < rows + rows / 4, "the run weighs {} for {rows} of rows", narrow.bytes);
    }
}
