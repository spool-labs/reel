//! Key runs: sorted rows pointing at each record where it lies, merged without moving one
//! Footers still serve point reads and recovery, so a lost run just returns its segments to the walk

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use crate::error::{ReelError, Result};
use crate::format::column::ColumnId;
use crate::format::footer::VARYING_WIDTH;
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::Lsn;
use crate::format::record::{read_u32_le, read_u64_le, Flags};
use crate::io::mapping::Mapping;
use crate::io::op::{FileId, WriteBuf};
use crate::reel::segment::IoDriver;

/// One fence lead covers this many bytes of rows, so a search touches a page or two
const BLOCK_BYTES: usize = 8 * 1024;

/// A row takes this many bytes past its key: sequence, segment, offset, length and flags
pub const ROW_TAIL: usize = 8 + 4 + 4 + 4 + 1;

/// The writer gathers this many bytes before each write, so rows go down in large writes
const WRITE_BYTES: usize = 1 << 20;

/// A block of a varying-width column holds this many rows
const VARYING_BLOCK_ROWS: u32 = 128;

/// A directory row takes this many bytes: column, width, block rows, rows, rows at, row bytes, fences at
const DIRECTORY_ROW: usize = 1 + 2 + 4 + 8 + 8 + 8 + 8;

/// The trailer takes this many bytes: directory at, columns, covered count, magic
const TRAILER: usize = 8 + 4 + 4 + 4;

const MAGIC: u32 = u32::from_le_bytes(*b"KRUN");

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

/// What one row says about its record
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RunRow {
    /// Sequence number of the record, which orders a key's versions
    pub lsn: Lsn,

    /// Where the record lies, in whichever segment it was written to
    pub loc: Loc,

    /// The record's own flags, which say which kind of record it is
    pub flags: Flags,
}

/// One column's rows in a key run
#[derive(Clone, Debug)]
pub struct RunColumn {
    /// Every row belongs to this column
    pub column: ColumnId,

    /// Every key in the column has this width, or VARYING_WIDTH where each row says its own
    pub key_width: u16,

    /// Each block holds this many rows, the last one holding the rest
    block_rows: u32,

    /// The column's row count
    rows: u64,

    /// Where the column's first row lies in the file
    rows_at: u64,

    /// The rows' length in bytes, ahead of a varying column's table of row starts
    rows_len: u64,

    /// Each block's first key, back to back
    fences: Vec<u8>,

    /// Where each block's lead starts in `fences`, and one past the last
    fence_at: Vec<u32>,

    /// The column's last key, which bounds the run's reach
    last: Vec<u8>,
}

impl RunColumn {
    /// Row size in bytes, in a column of one key width
    pub fn stride(&self) -> usize {
        self.key_width as usize + ROW_TAIL
    }

    /// Whether each row holds its own key width
    pub fn is_varying(&self) -> bool {
        self.key_width == VARYING_WIDTH
    }

    /// The region's length in bytes: its rows, and a varying column's table of starts
    fn region_len(&self) -> u64 {
        match self.is_varying() {
            true => self.rows_len + self.rows * 8,
            false => self.rows_len,
        }
    }

    /// The column's row count
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// The column's block count
    pub fn blocks(&self) -> u32 {
        self.rows.div_ceil(u64::from(self.block_rows)) as u32
    }

    /// Rows per block, fewer only in the last one
    pub fn block_rows(&self) -> u32 {
        self.block_rows
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

    /// The run stands in for these data segments' footers in a walk
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
            let rows_len = read_u64_le(&row[23..31]);
            let fences_at = read_u64_le(&row[31..39]);
            if key_width == 0 || block_rows == 0 {
                return Err(corrupt("a column with no width or no block size"));
            }
            let is_varying = key_width == VARYING_WIDTH;
            let stride = u64::from(key_width) + ROW_TAIL as u64;
            if !is_varying && rows.checked_mul(stride) != Some(rows_len) {
                return Err(corrupt("a column's rows do not fill its stride"));
            }
            let table = match is_varying {
                true => rows
                    .checked_mul(8)
                    .ok_or_else(|| corrupt("a column too long to hold"))?,
                false => 0,
            };
            span(
                rows_at,
                rows_len
                    .checked_add(table)
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
                rows_len,
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

    /// A column's rows back to back, and a varying column's table of starts, in place in the file
    pub fn rows(&self, column: &RunColumn) -> &[u8] {
        let at = column.rows_at as usize;
        // Open checked that every column's region lies inside the file
        &self.backing.bytes()[at..at + column.region_len() as usize]
    }

    /// The first row at or past a key, or strictly past it with `is_past`
    pub fn seek(&self, column: &RunColumn, key: &[u8], is_past: bool) -> u64 {
        let rows = self.rows(column);
        let (first, count) = column.block_span(column.block_for(key));
        let (mut low, mut high) = (first, first + u64::from(count));
        while low < high {
            let mid = (low + high) / 2;
            let held = key_in(rows, column, mid as usize);
            let is_before = match is_past {
                true => held <= key,
                false => held < key,
            };
            match is_before {
                true => low = mid + 1,
                false => high = mid,
            }
        }
        low
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

    /// Whether a run holds a row for a key below a sequence number
    pub fn holds_older(&self, column: ColumnId, key: &[u8], lsn: Lsn) -> bool {
        self.runs().iter().any(|run| {
            let Some(held) = run.column(column) else {
                return false;
            };
            if !held.is_varying() && key.len() != held.key_width as usize {
                return false;
            }
            let at = run.seek(held, key, false);
            at < held.rows()
                && row_in(run.rows(held), held, at as usize)
                    .is_ok_and(|(found, row)| found == key && row.lsn < lsn)
        })
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

/// Where one row's key starts in a column's rows, and how long it is
fn key_span(rows: &[u8], column: &RunColumn, at: usize) -> Option<(usize, usize)> {
    if !column.is_varying() {
        return Some((at * column.stride(), column.key_width as usize));
    }
    let table = column.rows_len as usize + at * 8;
    let start = read_u64_le(rows.get(table..table + 8)?) as usize;
    let len = u16::from_le_bytes([*rows.get(start)?, *rows.get(start + 1)?]) as usize;
    Some((start + 2, len))
}

/// The key and the row at one row of a column's rows
pub fn row_in<'a>(rows: &'a [u8], column: &RunColumn, at: usize) -> Result<(&'a [u8], RunRow)> {
    let corrupt = || ReelError::Corruption("key run row is past its column".to_string());
    let (start, width) = key_span(rows, column, at).ok_or_else(corrupt)?;
    let row = rows
        .get(start..start + width + ROW_TAIL)
        .ok_or_else(corrupt)?;
    let tail = &row[width..];
    Ok((
        &row[..width],
        RunRow {
            lsn: Lsn(read_u64_le(&tail[0..8])),
            loc: Loc::new(
                SegmentId(read_u32_le(&tail[8..12])),
                read_u32_le(&tail[12..16]),
                read_u32_le(&tail[16..20]),
            ),
            flags: Flags::from_bits(tail[20])?,
        },
    ))
}

/// The key at one row of a column's rows, without decoding the row
pub fn key_in<'a>(rows: &'a [u8], column: &RunColumn, at: usize) -> &'a [u8] {
    if !column.is_varying() {
        let stride = column.stride();
        return &rows[at * stride..at * stride + column.key_width as usize];
    }
    match key_span(rows, column, at) {
        Some((start, width)) => rows.get(start..start + width).unwrap_or_default(),
        None => &[],
    }
}

/// A column being written, its rows already down and its fences gathered
struct Building {
    column: ColumnId,
    key_width: u16,
    block_rows: u32,
    rows: u64,
    rows_at: u64,
    rows_len: u64,
    fences: Vec<u8>,
    last: Vec<u8>,
    starts: Vec<u64>,
}

impl Building {
    fn is_varying(&self) -> bool {
        self.key_width == VARYING_WIDTH
    }
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
        self.end_column()?;
        let block_rows = match key_width == VARYING_WIDTH {
            true => VARYING_BLOCK_ROWS,
            false => (BLOCK_BYTES / (key_width as usize + ROW_TAIL)).max(1) as u32,
        };
        self.columns.push(Building {
            column,
            key_width,
            block_rows,
            rows: 0,
            rows_at: self.at + self.pending.len() as u64,
            rows_len: 0,
            fences: Vec::new(),
            last: Vec::new(),
            starts: Vec::new(),
        });
        Ok(())
    }

    /// Close the open column, writing a varying one's table of row starts after its rows
    fn end_column(&mut self) -> Result<()> {
        let Some(building) = self.columns.last_mut() else {
            return Ok(());
        };
        for start in std::mem::take(&mut building.starts) {
            self.pending.extend_from_slice(&start.to_le_bytes());
            if self.pending.len() >= WRITE_BYTES {
                self.write_pending()?;
            }
        }
        Ok(())
    }

    /// Add one row to the open column, at a key above the last one it took
    pub fn push(&mut self, key: &[u8], row: RunRow) -> Result<()> {
        let building = self.columns.last_mut().ok_or_else(|| {
            ReelError::Rejected("a key run row came before any column".to_string())
        })?;
        let width = u16::try_from(key.len())
            .ok()
            .filter(|width| *width != VARYING_WIDTH)
            .ok_or_else(|| ReelError::Rejected("a key run row's key is too wide".to_string()))?;
        if !building.is_varying() && width != building.key_width {
            return Err(ReelError::Rejected(
                "a key run row is not its column's width".to_string(),
            ));
        }
        if building.rows > 0 && key <= building.last.as_slice() {
            return Err(ReelError::Rejected(
                "a key run's rows arrived out of order".to_string(),
            ));
        }
        if building.rows.is_multiple_of(u64::from(building.block_rows)) {
            building.fences.extend_from_slice(&width.to_le_bytes());
            building.fences.extend_from_slice(key);
        }
        building.last.clear();
        building.last.extend_from_slice(key);
        building.rows += 1;
        if building.is_varying() {
            building.starts.push(building.rows_len);
            building.rows_len += 2;
            self.pending.extend_from_slice(&width.to_le_bytes());
        }
        building.rows_len += (key.len() + ROW_TAIL) as u64;
        self.pending.extend_from_slice(key);
        self.pending.extend_from_slice(&row.lsn.pack());
        self.pending
            .extend_from_slice(&row.loc.segment.as_u32().to_le_bytes());
        self.pending
            .extend_from_slice(&row.loc.offset.to_le_bytes());
        self.pending.extend_from_slice(&row.loc.len.to_le_bytes());
        self.pending.push(row.flags.bits());
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

    /// Write the fences, the covered segments, the directory and the trailer, and put the run under its name
    pub fn finish(mut self, covered: &[SegmentId]) -> Result<PathBuf> {
        self.end_column()?;
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
            tail.extend_from_slice(&building.block_rows.to_le_bytes());
            tail.extend_from_slice(&building.rows.to_le_bytes());
            tail.extend_from_slice(&building.rows_at.to_le_bytes());
            tail.extend_from_slice(&building.rows_len.to_le_bytes());
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

    fn row(n: u32) -> RunRow {
        RunRow {
            lsn: Lsn(u64::from(n) + 7),
            loc: Loc::new(SegmentId(n % 5 + 1), n * 3, n + 1),
            flags: Flags::DATA,
        }
    }

    // a run reads back every row it was written with, in order
    #[test]
    fn a_written_run_reads_back_its_rows() {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = driver(&sim);
        let mut keys: Vec<(Vec<u8>, u32)> = (0..2_000u32).map(|n| (key(n).to_vec(), n)).collect();
        keys.sort();
        let mut writer = RunWriter::create(&driver, Path::new(ROOT), 3).expect("create");
        writer.begin_column(ColumnId(1), 8).expect("column");
        for (key, n) in &keys {
            writer.push(key, row(*n)).expect("push");
        }
        writer.begin_column(ColumnId(2), 8).expect("second column");
        writer.push(&key(1), row(1)).expect("push");
        let path = writer
            .finish(&[SegmentId(4), SegmentId(9)])
            .expect("finish");

        let run = KeyRun::open(&driver, &path, 3).expect("open");
        assert_eq!(run.covered, vec![SegmentId(4), SegmentId(9)]);
        let column = run.column(ColumnId(1)).expect("column one");
        assert_eq!(column.rows(), 2_000);
        let rows = run.rows(column);
        for (at, (want, n)) in keys.iter().enumerate() {
            let (got, got_row) = row_in(rows, column, at).expect("row");
            assert_eq!(got, want.as_slice());
            assert_eq!(got_row, row(*n));
        }
        assert!(
            row_in(rows, column, keys.len()).is_err(),
            "a row past the column read back"
        );
        assert_eq!(run.column(ColumnId(2)).expect("column two").rows(), 1);
    }

    // a seek lands on the first row at or past a key, or past it alone
    #[test]
    fn a_seek_lands_on_the_first_row_at_or_past_the_key() {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = driver(&sim);
        let keys: Vec<[u8; 8]> = (0..5_000u64).map(|n| (n * 2).to_be_bytes()).collect();
        let mut writer = RunWriter::create(&driver, Path::new(ROOT), 1).expect("create");
        writer.begin_column(ColumnId(1), 8).expect("column");
        for (n, key) in keys.iter().enumerate() {
            writer.push(key, row(n as u32)).expect("push");
        }
        let path = writer.finish(&[]).expect("finish");
        let run = KeyRun::open(&driver, &path, 1).expect("open");
        let column = run.column(ColumnId(1)).expect("column");
        for probe in [0u64, 1, 2, 1_001, 4_000, 9_998, 9_999, 20_000] {
            let target = probe.to_be_bytes();
            let at = keys
                .iter()
                .position(|key| key >= &target)
                .unwrap_or(keys.len()) as u64;
            let past = keys
                .iter()
                .position(|key| key > &target)
                .unwrap_or(keys.len()) as u64;
            assert_eq!(run.seek(column, &target, false), at, "probe {probe} at");
            assert_eq!(run.seek(column, &target, true), past, "probe {probe} past");
        }
    }

    // a column whose keys vary in width reads back every row and seeks the way a fixed one does
    #[test]
    fn a_varying_column_reads_back_and_seeks() {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = driver(&sim);
        let mut keys: Vec<Vec<u8>> = (0..3_000u32)
            .map(|n| format!("k{}", n * 7).into_bytes())
            .collect();
        keys.sort();
        keys.dedup();
        let mut writer = RunWriter::create(&driver, Path::new(ROOT), 5).expect("create");
        writer.begin_column(ColumnId(1), 8).expect("fixed column");
        writer.push(&key(1), row(1)).expect("push");
        writer
            .begin_column(ColumnId(2), VARYING_WIDTH)
            .expect("varying column");
        for (n, key) in keys.iter().enumerate() {
            writer.push(key, row(n as u32)).expect("push");
        }
        let path = writer.finish(&[SegmentId(2)]).expect("finish");
        let run = KeyRun::open(&driver, &path, 5).expect("open");
        assert_eq!(run.column(ColumnId(1)).expect("fixed").rows(), 1);
        let column = run.column(ColumnId(2)).expect("varying");
        assert!(column.is_varying());
        let rows = run.rows(column);
        for (at, want) in keys.iter().enumerate() {
            let (got, got_row) = row_in(rows, column, at).expect("row");
            assert_eq!(got, want.as_slice());
            assert_eq!(key_in(rows, column, at), want.as_slice());
            assert_eq!(got_row, row(at as u32));
        }
        assert_eq!(
            column
                .key_range()
                .map(|(low, high)| (low.to_vec(), high.to_vec())),
            Some((keys[0].clone(), keys[keys.len() - 1].clone()))
        );
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
            assert_eq!(run.seek(column, &probe, false), at, "probe {probe:?} at");
            assert_eq!(run.seek(column, &probe, true), past, "probe {probe:?} past");
        }
    }

    // rows out of order are refused, so a walk can trust the run is sorted
    #[test]
    fn rows_out_of_order_are_refused() {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = driver(&sim);
        let mut writer = RunWriter::create(&driver, Path::new(ROOT), 2).expect("create");
        writer.begin_column(ColumnId(1), 8).expect("column");
        writer.push(&key(9), row(9)).expect("push");
        assert!(writer.push(&key(9), row(9)).is_err());
        writer.abandon();
    }
}
