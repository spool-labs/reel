//! The resident index written down at a cue, so an open reads it instead of sweeping
//!
//! Rebuilding the index by sweeping footers is work per key, so at a huge key count
//! it is the whole of the start-up time. At a cue every tail is sealed, which makes
//! the index over those files a value that can be written down. The file names the
//! segments it stands for and how long each one was, and a sealed segment never
//! changes and its number is never reused, so the same number at the same length is
//! the same bytes. Anything the check does not cover is swept.
//!
//! Neither end ever holds the whole file: it is a head and then a run of blocks a
//! megabyte apiece, each carrying its own checksum so a block is proved as it lands.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use crate::error::{ReelError, Result};
use crate::format::column::{ColumnId, ColumnSet, KeyWidth, MAX_KEY_LEN};
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::Lsn;
use crate::format::record::{digest, read_u16_le, read_u32_le, read_u64_le};
use crate::io::op::{FileId, WriteBuf};
use crate::reel::segment::IoDriver;

/// Name the persisted index takes in a volume root
pub const PERSISTED_INDEX: &str = "reel.index";

/// Name it is built under before the rename that publishes it
const STAGING_INDEX: &str = "reel.index.tmp";

/// Leading bytes that say a file is one of these at all
const MAGIC: u32 = 0x5849_4552;

/// Format version this build writes, and the only one it reads
///
/// A file from another version is refused whole rather than read in part: the
/// fallback is a sweep, which is slow and always right.
const FORMAT_VERSION: u16 = 2;

const MAGIC_AT: usize = 0;
const VERSION_AT: usize = MAGIC_AT + 4;
const COLUMNS_AT: usize = VERSION_AT + 2;
const AT_LSN_AT: usize = COLUMNS_AT + 2;
const SEGMENTS_AT: usize = AT_LSN_AT + 8;
const HEAD_CRC_AT: usize = SEGMENTS_AT + 4;

/// Bytes the fixed head occupies, its checksum included
const HEAD_LEN: usize = HEAD_CRC_AT + 4;

/// Bytes one column declaration occupies
const COLUMN_LEN: usize = 4;

/// Bytes one segment stamp occupies
const STAMP_LEN: usize = 4 + 8 * 5;

/// Bytes a row carries past its key
const ROW_TAIL_LEN: usize = 8 + 4 + 4 + 4;

const BLOCK_COLUMN_AT: usize = 0;
const BLOCK_WIDTH_AT: usize = BLOCK_COLUMN_AT + 2;
const BLOCK_ROWS_AT: usize = BLOCK_WIDTH_AT + 2;
const BLOCK_BYTES_AT: usize = BLOCK_ROWS_AT + 4;
const BLOCK_CRC_AT: usize = BLOCK_BYTES_AT + 4;

/// Bytes one block's frame occupies, its checksum included
const BLOCK_HEAD_LEN: usize = BLOCK_CRC_AT + 4;

/// Row bytes one block carries before the writer empties it
///
/// The whole of what either end holds at once: small enough that a volume of any key
/// count writes and reads it in constant space, large enough that the per-block frame
/// and its two reads are lost against the rows.
const BLOCK_BYTES: usize = 1 << 20;

/// Widest row the format can produce, which a block always has room for
const ROW_CAP: usize = 2 + MAX_KEY_LEN + ROW_TAIL_LEN;

/// A block that could not hold one row would never fill, so the writer would loop
const _: () = assert!(ROW_CAP < BLOCK_BYTES);

/// Width recorded for a column whose keys carry their own length
///
/// Also what tells a reader whether a row's key is measured or counted: one width
/// is read off the column, and no width puts a length in front of every key.
pub const VARIABLE_WIDTH: u16 = 0;

/// One segment a persisted index resolved, and the length that proves it unchanged
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PersistedSegment {
    /// The segment the rows below point into
    pub segment: SegmentId,

    /// Length of the file when the index was written
    pub len: u64,

    /// Footprint of records already shadowed when the index was written
    pub dead: u64,

    /// Tombstone footprint the segment holds, which no row of this file names
    pub held: u64,

    /// Newest tombstone version the segment holds, if it holds one
    pub held_lsn: Option<Lsn>,

    /// Oldest data record the segment can still surface, which a rebuild cannot
    /// recompute from the rows
    pub min_lsn: Option<Lsn>,
}

/// One column the file wrote rows for, and the width it wrote their keys at
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PersistedColumn {
    /// Column the rows belong to
    pub column: ColumnId,

    /// Declared key width, or zero where the column's keys carry their own
    pub key_width: u16,
}

/// The head of a persisted index: what it stands at, and what it speaks for
///
/// Everything about the file bounded by the volume's shape rather than by its key
/// count, which is what makes it a value worth holding. The rows are not here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedIndex {
    /// Sequence number the index is the volume as of
    pub at: Lsn,

    /// Segments the rows point into, each with what proves it unchanged
    pub segments: Vec<PersistedSegment>,

    /// Columns the file wrote blocks for, in the order it wrote them
    pub columns: Vec<PersistedColumn>,
}

impl PersistedIndex {
    /// Serialize the head to the bytes the front of the file holds
    pub fn pack(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.packed_len());
        buf.extend_from_slice(&MAGIC.to_le_bytes());
        buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&(self.columns.len() as u16).to_le_bytes());
        buf.extend_from_slice(&self.at.as_u64().to_le_bytes());
        buf.extend_from_slice(&(self.segments.len() as u32).to_le_bytes());
        // The checksum covers everything after itself, so its own place is left
        // open and filled in once the rest is down.
        buf.extend_from_slice(&0u32.to_le_bytes());
        for column in &self.columns {
            buf.extend_from_slice(&u16::from(column.column.as_u8()).to_le_bytes());
            buf.extend_from_slice(&column.key_width.to_le_bytes());
        }
        for stamp in &self.segments {
            buf.extend_from_slice(&stamp.segment.as_u32().to_le_bytes());
            buf.extend_from_slice(&stamp.len.to_le_bytes());
            buf.extend_from_slice(&stamp.dead.to_le_bytes());
            buf.extend_from_slice(&stamp.held.to_le_bytes());
            buf.extend_from_slice(&lsn_bytes(stamp.held_lsn));
            buf.extend_from_slice(&lsn_bytes(stamp.min_lsn));
        }
        let mut crc = digest();
        crc.update(&buf[..HEAD_CRC_AT]);
        crc.update(&buf[HEAD_LEN..]);
        let crc = crc.finalize() as u32;
        buf[HEAD_CRC_AT..HEAD_LEN].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Bytes `pack` will produce, so the buffer is taken in one allocation
    fn packed_len(&self) -> usize {
        HEAD_LEN + self.columns.len() * COLUMN_LEN + self.segments.len() * STAMP_LEN
    }

    /// Parse the head, refusing anything that is not exactly one of these
    ///
    /// Every refusal here is the caller's signal to sweep instead, so the checks are
    /// blunt on purpose.
    pub fn unpack(bytes: &[u8]) -> Result<PersistedIndex> {
        if bytes.len() < HEAD_LEN {
            return Err(short("its head"));
        }
        if read_u32_le(&bytes[MAGIC_AT..VERSION_AT]) != MAGIC {
            return Err(ReelError::Corruption(
                "a file under the persisted index name is not one".to_string(),
            ));
        }
        let version = read_u16_le(&bytes[VERSION_AT..COLUMNS_AT]);
        if version != FORMAT_VERSION {
            return Err(ReelError::Corruption(format!(
                "a persisted index written at version {version}, which this build does not read",
            )));
        }
        let column_count = usize::from(read_u16_le(&bytes[COLUMNS_AT..AT_LSN_AT]));
        let at = Lsn(read_u64_le(&bytes[AT_LSN_AT..SEGMENTS_AT]));
        let segment_count = read_u32_le(&bytes[SEGMENTS_AT..HEAD_CRC_AT]) as usize;
        let stored = read_u32_le(&bytes[HEAD_CRC_AT..HEAD_LEN]);

        let table = column_count * COLUMN_LEN + segment_count * STAMP_LEN;
        let body = bytes
            .get(HEAD_LEN..HEAD_LEN + table)
            .ok_or_else(|| short("the columns and segments its head counts"))?;
        let mut crc = digest();
        crc.update(&bytes[..HEAD_CRC_AT]);
        crc.update(body);
        if crc.finalize() as u32 != stored {
            return Err(ReelError::Corruption(
                "a persisted index head failed its checksum".to_string(),
            ));
        }

        let mut columns = Vec::with_capacity(column_count);
        for at in 0..column_count {
            let named = read_u16_le(&body[at * COLUMN_LEN..at * COLUMN_LEN + 2]);
            let Ok(column) = u8::try_from(named) else {
                return Err(ReelError::Corruption(format!(
                    "a persisted index names column {named}, which no record header can carry",
                )));
            };
            columns.push(PersistedColumn {
                column: ColumnId(column),
                key_width: read_u16_le(&body[at * COLUMN_LEN + 2..at * COLUMN_LEN + COLUMN_LEN]),
            });
        }

        let stamps = &body[column_count * COLUMN_LEN..];
        let mut segments = Vec::with_capacity(segment_count);
        for at in 0..segment_count {
            segments.push(unpack_stamp(&stamps[at * STAMP_LEN..(at + 1) * STAMP_LEN]));
        }
        Ok(PersistedIndex {
            at,
            segments,
            columns,
        })
    }
}

/// Whether this file describes the columns the store was opened with
///
/// A block naming a column the store does not serve, or one whose keys are a
/// different width now, would have its rows dropped while its segments were still
/// skipped as read, which is the one way this file can lose a live record.
pub fn admits(persisted: &PersistedIndex, columns: ColumnSet) -> bool {
    for block in &persisted.columns {
        let Some(spec) = columns.iter().find(|spec| spec.id == block.column) else {
            return false;
        };
        let declared = match spec.key_width {
            KeyWidth::Fixed(width) => width,
            KeyWidth::Variable => VARIABLE_WIDTH,
        };
        if declared != block.key_width {
            return false;
        }
    }
    true
}

/// The segments this file still speaks for, given what the directory holds now
///
/// A file under the same number at the same length holds the bytes the rows were
/// resolved from. A segment compaction retired is absent and one whose seal landed
/// after the checkpoint is longer, so both fall out of the set and are read. Nothing
/// outside the set is ever skipped.
pub fn trusted(
    persisted: &PersistedIndex,
    present: &HashMap<SegmentId, u64>,
) -> BTreeSet<SegmentId> {
    let mut standing = BTreeSet::new();
    for stamp in &persisted.segments {
        if present.get(&stamp.segment) == Some(&stamp.len) {
            standing.insert(stamp.segment);
        }
    }
    standing
}

/// A persisted index built a block at a time, published by one rename
///
/// The caller drives its own walk and hands the rows over here, so the bytes exist a
/// block at a time and never as a value. Staged under its own name, synced, then
/// renamed onto the published one, so a crash leaves either the file that was there
/// before or the whole new one and never a half. An unfinished attempt is swept
/// rather than refused, since it describes a volume that has moved on.
pub struct PersistedWriter {
    /// Volume root the file is published into, synced once at the end
    root: PathBuf,

    /// Name the file is built under
    staging: PathBuf,

    /// Name an open reads
    published: PathBuf,

    /// The staging file, open until the writer is done with it
    file: FileId,

    /// Byte the next block lands at
    at: u64,

    /// The one block, holding its frame and then the rows staged behind it
    block: Vec<u8>,

    /// Column the staged rows belong to
    column: ColumnId,

    /// Width their keys are written at
    key_width: u16,

    /// Rows staged in the block
    rows: u32,

    /// Rows written down so far, which is the per-key work an open skips
    keys: u64,
}

impl PersistedWriter {
    /// Open the staging file and put the head every block hangs off down
    pub fn try_new(
        driver: &IoDriver,
        root: &Path,
        head: &PersistedIndex,
    ) -> Result<PersistedWriter> {
        let staging = root.join(STAGING_INDEX);
        // An open does not truncate, so a longer attempt left under this name by a
        // crash would ride out past the new bytes. Taken off rather than written over.
        match driver.unlink(&staging) {
            Ok(()) => {}
            Err(error) if error.is_missing() => {}
            Err(error) => return Err(error),
        }
        let file = driver.open(&staging, true)?;
        let mut writer = PersistedWriter {
            root: root.to_path_buf(),
            staging,
            published: root.join(PERSISTED_INDEX),
            file,
            at: 0,
            block: Vec::with_capacity(BLOCK_HEAD_LEN + BLOCK_BYTES),
            column: ColumnId(0),
            key_width: VARIABLE_WIDTH,
            rows: 0,
            keys: 0,
        };
        match writer.put(driver, head.pack()) {
            Ok(_) => {
                writer.clear_block();
                Ok(writer)
            }
            Err(error) => {
                writer.abandon(driver);
                Err(error)
            }
        }
    }

    /// Start the run of blocks one column's rows go into
    ///
    /// Every row handed over after this belongs to that column until the next one
    /// starts, and a block never spans two columns.
    pub fn open_column(
        &mut self,
        driver: &IoDriver,
        column: ColumnId,
        key_width: u16,
    ) -> Result<()> {
        self.flush(driver)?;
        self.column = column;
        self.key_width = key_width;
        Ok(())
    }

    /// Write one live key down
    ///
    /// The keys of a column must arrive in ascending order, which is the order the
    /// index walks them in and the order a reader holds the file to.
    pub fn row(&mut self, driver: &IoDriver, key: &[u8], lsn: Lsn, loc: Loc) -> Result<()> {
        let width = key.len();
        match self.key_width {
            VARIABLE_WIDTH if width > MAX_KEY_LEN => {
                return Err(ReelError::Corruption(format!(
                    "a key of {width} bytes, past the format's ceiling",
                )));
            }
            VARIABLE_WIDTH => {}
            fixed if width != usize::from(fixed) => {
                return Err(ReelError::Corruption(format!(
                    "a key of {width} bytes in a column declared {fixed} bytes wide",
                )));
            }
            _ => {}
        }
        if self.staged() + row_len(self.key_width, width) > BLOCK_BYTES {
            self.flush(driver)?;
        }
        if self.key_width == VARIABLE_WIDTH {
            self.block.extend_from_slice(&(width as u16).to_le_bytes());
        }
        self.block.extend_from_slice(key);
        self.block.extend_from_slice(&lsn.as_u64().to_le_bytes());
        self.block
            .extend_from_slice(&loc.segment.as_u32().to_le_bytes());
        self.block.extend_from_slice(&loc.offset.to_le_bytes());
        self.block.extend_from_slice(&loc.len.to_le_bytes());
        self.rows += 1;
        self.keys += 1;
        Ok(())
    }

    /// Keys written down so far
    pub fn keys(&self) -> u64 {
        self.keys
    }

    /// Empty the last block, make everything durable, and take the published name
    pub fn publish(mut self, driver: &IoDriver) -> Result<()> {
        let written = (|| -> Result<()> {
            self.flush(driver)?;
            driver.sync_full(self.file)
        })();
        driver.close(self.file)?;
        written?;
        // Everything is on the medium and nothing has taken the published name yet, so
        // a crash here leaves the previous file standing beside a staging name to sweep.
        crate::sync::rendezvous::at("index/persisted-staged");
        driver.rename(&self.staging, &self.published)?;
        driver.sync_dir(&self.root)?;
        Ok(())
    }

    /// Give the staging file up, publishing nothing
    pub fn abandon(self, driver: &IoDriver) {
        let _ = driver.close(self.file);
        let _ = driver.unlink(&self.staging);
    }

    /// Row bytes staged behind the block's frame
    fn staged(&self) -> usize {
        self.block.len() - BLOCK_HEAD_LEN
    }

    /// Stamp the block's frame, put it down, and take its room back
    fn flush(&mut self, driver: &IoDriver) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let staged = self.staged() as u32;
        self.block[BLOCK_COLUMN_AT..BLOCK_WIDTH_AT]
            .copy_from_slice(&u16::from(self.column.as_u8()).to_le_bytes());
        self.block[BLOCK_WIDTH_AT..BLOCK_ROWS_AT].copy_from_slice(&self.key_width.to_le_bytes());
        self.block[BLOCK_ROWS_AT..BLOCK_BYTES_AT].copy_from_slice(&self.rows.to_le_bytes());
        self.block[BLOCK_BYTES_AT..BLOCK_CRC_AT].copy_from_slice(&staged.to_le_bytes());
        let mut crc = digest();
        crc.update(&self.block[..BLOCK_CRC_AT]);
        crc.update(&self.block[BLOCK_HEAD_LEN..]);
        let crc = crc.finalize() as u32;
        self.block[BLOCK_CRC_AT..BLOCK_HEAD_LEN].copy_from_slice(&crc.to_le_bytes());

        let block = std::mem::take(&mut self.block);
        self.block = self.put(driver, block)?;
        self.rows = 0;
        self.clear_block();
        Ok(())
    }

    /// Write one buffer at the write head, handing the buffer back so the next block
    /// fills the same room
    fn put(&mut self, driver: &IoDriver, bytes: Vec<u8>) -> Result<Vec<u8>> {
        let framed = bytes.len() as u64;
        let (wrote, mut bufs) =
            driver.writev_reusing(self.file, self.at, vec![WriteBuf::owned(bytes)])?;
        // The write head below is what every later block is placed against, so a
        // short write has to stop this file rather than leave a hole in it.
        if wrote != framed {
            return Err(ReelError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                format!("a persisted index block framed {framed} bytes and wrote {wrote}"),
            )));
        }
        self.at += framed;
        match bufs.pop() {
            Some(WriteBuf::Owned(taken)) => Ok(taken),
            Some(_) | None => Ok(Vec::new()),
        }
    }

    /// Empty the block and open the room its frame takes
    fn clear_block(&mut self) {
        self.block.clear();
        self.block.resize(BLOCK_HEAD_LEN, 0);
    }
}

/// A persisted index read a block at a time, a row at a time out of the block
///
/// The rows are handed out through the reader rather than returned, since a row's key
/// lives in the block the reader is holding. Advance, then read.
pub struct PersistedReader {
    /// What the file's head stood for, read whole before any row is
    pub index: PersistedIndex,

    /// The file, open until the reader is closed
    file: FileId,

    /// Bytes the file holds, which bounds every block it claims
    end: u64,

    /// Byte the next block's frame sits at
    at: u64,

    /// The frame of the block in hand
    frame: Vec<u8>,

    /// The rows of the block in hand
    block: Vec<u8>,

    /// Column those rows belong to
    column: ColumnId,

    /// Width their keys are written at
    key_width: u16,

    /// Rows of the block not handed out yet
    rows_left: u32,

    /// Byte the next row starts at
    row_at: usize,

    /// Where the row last handed out keeps its key
    key_at: usize,
    key_len: usize,

    /// What that row resolves to
    lsn: Lsn,
    loc: Loc,

    /// The last key of the block before this one, so the order check crosses blocks
    carried: Option<Vec<u8>>,
}

/// What one block's frame says about the rows behind it
#[derive(Clone, Copy)]
struct BlockFrame {
    /// Column those rows belong to
    column: ColumnId,

    /// Width their keys are written at
    key_width: u16,

    /// Rows the block holds
    rows: u32,

    /// Bytes those rows occupy
    staged: u64,

    /// Checksum over the frame and the rows together
    crc: u32,
}

impl PersistedReader {
    /// Open the index a previous cue wrote down, or nothing where none can be believed
    ///
    /// Absent, unreadable and malformed all answer the same way, since the caller's
    /// fallback is the sweep and the sweep is always right.
    pub fn open(driver: &IoDriver, root: &Path) -> Result<Option<PersistedReader>> {
        let path = root.join(PERSISTED_INDEX);
        let file = match driver.open(&path, false) {
            Ok(file) => file,
            Err(error) if error.is_missing() => return Ok(None),
            Err(error) => return Err(error),
        };
        // The length comes off the open file rather than off a listing of the root,
        // which would answer about a file this is not holding open.
        let read = (|| -> Result<PersistedReader> {
            let end = driver.length(file)?;
            let index = read_head(driver, file, end)?;
            Ok(PersistedReader::new(index, file, end))
        })();
        match read {
            Ok(reader) => Ok(Some(reader)),
            Err(error) => {
                let _ = driver.close(file);
                tracing::warn!(
                    "the persisted index at {} is not readable, so this open sweeps the footers: {error}",
                    path.display(),
                );
                Ok(None)
            }
        }
    }

    fn new(index: PersistedIndex, file: FileId, end: u64) -> PersistedReader {
        PersistedReader {
            at: (HEAD_LEN + index.columns.len() * COLUMN_LEN + index.segments.len() * STAMP_LEN)
                as u64,
            index,
            file,
            end,
            frame: Vec::with_capacity(BLOCK_HEAD_LEN),
            block: Vec::with_capacity(BLOCK_BYTES),
            column: ColumnId(0),
            key_width: VARIABLE_WIDTH,
            rows_left: 0,
            row_at: 0,
            key_at: 0,
            key_len: 0,
            lsn: Lsn::NONE,
            loc: Loc::new(SegmentId(0), 0, 0),
            carried: None,
        }
    }

    /// Move to the next row, reading a block where the one in hand is spent
    pub fn advance(&mut self, driver: &IoDriver) -> Result<bool> {
        while self.rows_left == 0 {
            if !self.next_block(driver)? {
                return Ok(false);
            }
        }
        let (key_at, key_len, next) = self.row_span(self.row_at)?;
        let tail = next - ROW_TAIL_LEN;
        let lsn = Lsn(read_u64_le(&self.block[tail..tail + 8]));
        let segment = SegmentId(read_u32_le(&self.block[tail + 8..tail + 12]));
        let offset = read_u32_le(&self.block[tail + 12..tail + 16]);
        let len = read_u32_le(&self.block[tail + 16..tail + ROW_TAIL_LEN]);
        self.key_at = key_at;
        self.key_len = key_len;
        self.lsn = lsn;
        self.loc = Loc::new(segment, offset, len);
        self.row_at = next;
        self.rows_left -= 1;
        Ok(true)
    }

    /// Column the row in hand belongs to
    pub fn column(&self) -> ColumnId {
        self.column
    }

    /// Width that column writes its keys at
    pub fn key_width(&self) -> u16 {
        self.key_width
    }

    /// The key of the row in hand, borrowed out of the block holding it
    pub fn key(&self) -> &[u8] {
        &self.block[self.key_at..self.key_at + self.key_len]
    }

    /// Sequence number of the version that row resolves to
    pub fn lsn(&self) -> Lsn {
        self.lsn
    }

    /// Where its record sits on the volume
    pub fn loc(&self) -> Loc {
        self.loc
    }

    /// Release the file
    pub fn close(self, driver: &IoDriver) -> Result<()> {
        driver.close(self.file)
    }

    /// Read the next block's frame and its rows, proving both before serving either
    fn next_block(&mut self, driver: &IoDriver) -> Result<bool> {
        if self.at >= self.end {
            return Ok(false);
        }
        if self.end - self.at < BLOCK_HEAD_LEN as u64 {
            return Err(short("a block frame"));
        }
        self.frame = driver.pread_reusing(
            self.file,
            self.at,
            BLOCK_HEAD_LEN as u64,
            std::mem::take(&mut self.frame),
        )?;
        let frame = self.frame_of()?;

        self.block = driver.pread_reusing(
            self.file,
            self.at + BLOCK_HEAD_LEN as u64,
            frame.staged,
            std::mem::take(&mut self.block),
        )?;
        if self.block.len() as u64 != frame.staged {
            return Err(short("a block's rows"));
        }
        let mut crc = digest();
        crc.update(&self.frame[..BLOCK_CRC_AT]);
        crc.update(&self.block);
        if crc.finalize() as u32 != frame.crc {
            return Err(ReelError::Corruption(
                "a persisted index block failed its checksum".to_string(),
            ));
        }

        // A column starts its keys over, so nothing about the block before it bounds
        // the first key of this one.
        if frame.column != self.column {
            self.carried = None;
        }
        self.column = frame.column;
        self.key_width = frame.key_width;
        self.rows_left = frame.rows;
        self.row_at = 0;
        self.verify_rows()?;
        self.at += BLOCK_HEAD_LEN as u64 + frame.staged;
        Ok(true)
    }

    /// Parse the frame in hand, refusing one that describes bytes the file lacks
    fn frame_of(&self) -> Result<BlockFrame> {
        if self.frame.len() < BLOCK_HEAD_LEN {
            return Err(short("a block frame"));
        }
        let named = read_u16_le(&self.frame[BLOCK_COLUMN_AT..BLOCK_WIDTH_AT]);
        let key_width = read_u16_le(&self.frame[BLOCK_WIDTH_AT..BLOCK_ROWS_AT]);
        let staged = u64::from(read_u32_le(&self.frame[BLOCK_BYTES_AT..BLOCK_CRC_AT]));
        if staged > BLOCK_BYTES as u64 {
            return Err(ReelError::Corruption(format!(
                "a persisted index block claims {staged} row bytes, past the {BLOCK_BYTES} one holds",
            )));
        }
        if self.end - self.at - (BLOCK_HEAD_LEN as u64) < staged {
            return Err(short("a block's rows"));
        }
        Ok(BlockFrame {
            column: self.declared(named, key_width)?,
            key_width,
            rows: read_u32_le(&self.frame[BLOCK_ROWS_AT..BLOCK_BYTES_AT]),
            staged,
            crc: read_u32_le(&self.frame[BLOCK_CRC_AT..BLOCK_HEAD_LEN]),
        })
    }

    /// The column a block names, refused where the head never declared it
    ///
    /// The head is what an open weighs against its own column set, so a block naming a
    /// column outside it would have its rows dropped while its segments were skipped.
    fn declared(&self, named: u16, key_width: u16) -> Result<ColumnId> {
        let Ok(column) = u8::try_from(named) else {
            return Err(ReelError::Corruption(format!(
                "a persisted index block names column {named}, which no record header can carry",
            )));
        };
        let column = ColumnId(column);
        let is_declared = self
            .index
            .columns
            .iter()
            .any(|held| held.column == column && held.key_width == key_width);
        if !is_declared {
            return Err(ReelError::Corruption(format!(
                "a persisted index block names column {named} at width {key_width}, which its \
                 head does not declare",
            )));
        }
        Ok(column)
    }

    /// Walk the block once, proving every row parses and that the keys rise
    ///
    /// Done before any row is served, so a block is either whole or refused. The
    /// rising keys are what let a caller take the rows as a sorted run.
    fn verify_rows(&mut self) -> Result<()> {
        let mut at = 0usize;
        let mut previous: Option<(usize, usize)> = None;
        for _ in 0..self.rows_left {
            let (key_at, key_len, next) = self.row_span(at)?;
            let key = &self.block[key_at..key_at + key_len];
            let rises = match previous {
                Some((from, len)) => self.block[from..from + len] < *key,
                None => match &self.carried {
                    Some(held) => held.as_slice() < key,
                    None => true,
                },
            };
            if !rises {
                return Err(ReelError::Corruption(
                    "a persisted index block hands its keys out of order".to_string(),
                ));
            }
            previous = Some((key_at, key_len));
            at = next;
        }
        if at != self.block.len() {
            return Err(ReelError::Corruption(
                "a persisted index block holds bytes past the rows it counts".to_string(),
            ));
        }
        self.carried = previous.map(|(from, len)| self.block[from..from + len].to_vec());
        Ok(())
    }

    /// Where a row's key sits and where the row after it starts
    fn row_span(&self, at: usize) -> Result<(usize, usize, usize)> {
        let mut cursor = at;
        let width = match self.key_width {
            VARIABLE_WIDTH => {
                let len = self
                    .block
                    .get(cursor..cursor + 2)
                    .ok_or_else(|| short("a key length"))?;
                cursor += 2;
                usize::from(read_u16_le(len))
            }
            fixed => usize::from(fixed),
        };
        if width > MAX_KEY_LEN {
            return Err(ReelError::Corruption(format!(
                "a persisted row names a key of {width} bytes, past the format's ceiling",
            )));
        }
        let end = cursor + width + ROW_TAIL_LEN;
        if end > self.block.len() {
            return Err(short("a row"));
        }
        Ok((cursor, width, end))
    }
}

/// Read the head and the tables behind it, bounded by what the file actually holds
fn read_head(driver: &IoDriver, file: FileId, end: u64) -> Result<PersistedIndex> {
    let mut bytes = driver.pread(file, 0, HEAD_LEN as u64)?;
    if bytes.len() < HEAD_LEN {
        return Err(short("its head"));
    }
    let columns = u64::from(read_u16_le(&bytes[COLUMNS_AT..AT_LSN_AT]));
    let segments = u64::from(read_u32_le(&bytes[SEGMENTS_AT..HEAD_CRC_AT]));
    let table = columns * COLUMN_LEN as u64 + segments * STAMP_LEN as u64;
    // Asked of the file before it is asked of the allocator: a rotted count would
    // otherwise reserve room for a table no volume ever wrote.
    if end - (HEAD_LEN as u64) < table {
        return Err(short("the columns and segments its head counts"));
    }
    let rest = driver.pread(file, HEAD_LEN as u64, table)?;
    if rest.len() as u64 != table {
        return Err(short("the columns and segments its head counts"));
    }
    bytes.extend_from_slice(&rest);
    PersistedIndex::unpack(&bytes)
}

/// Bytes one row of this shape occupies
fn row_len(key_width: u16, key: usize) -> usize {
    match key_width {
        VARIABLE_WIDTH => 2 + key + ROW_TAIL_LEN,
        _ => key + ROW_TAIL_LEN,
    }
}

fn unpack_stamp(bytes: &[u8]) -> PersistedSegment {
    PersistedSegment {
        segment: SegmentId(read_u32_le(&bytes[0..4])),
        len: read_u64_le(&bytes[4..12]),
        dead: read_u64_le(&bytes[12..20]),
        held: read_u64_le(&bytes[20..28]),
        held_lsn: lsn_of(read_u64_le(&bytes[28..36])),
        min_lsn: lsn_of(read_u64_le(&bytes[36..STAMP_LEN])),
    }
}

fn lsn_bytes(lsn: Option<Lsn>) -> [u8; 8] {
    lsn.unwrap_or(Lsn::NONE).as_u64().to_le_bytes()
}

fn lsn_of(raw: u64) -> Option<Lsn> {
    match raw {
        0 => None,
        value => Some(Lsn(value)),
    }
}

fn short(what: &str) -> ReelError {
    ReelError::Corruption(format!("a persisted index ends before {what}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::format::column::{Codec, ColumnSpec, MapShape};
    use crate::io::fault::FaultPlan;
    use crate::io::sim_backend::SimIo;

    const RECORDS: ColumnId = ColumnId(1);
    const NOTES: ColumnId = ColumnId(2);

    /// Virtual volume the simulator files live under
    const ROOT: &str = "/bulk";

    /// Rows one test writes to prove a run crosses several blocks
    const CROSSING_ROWS: u64 = 40_000;

    const COLUMNS: ColumnSet = &[
        ColumnSpec {
            id: RECORDS,
            name: "records",
            key_width: KeyWidth::Fixed(34),
            shard_bytes: 2,
            inline_max: 0,
            row_carry: 0,
            purge_mark: None,
            codec: Codec::None,
            map_shape: MapShape::Tree,
        },
        ColumnSpec {
            id: NOTES,
            name: "notes",
            key_width: KeyWidth::Variable,
            shard_bytes: 1,
            inline_max: 0,
            row_carry: 0,
            purge_mark: None,
            codec: Codec::None,
            map_shape: MapShape::Tree,
        },
    ];

    fn driver(sim: &SimIo) -> IoDriver {
        IoDriver::new(Arc::new(sim.clone()))
    }

    fn root() -> PathBuf {
        PathBuf::from(ROOT)
    }

    fn head() -> PersistedIndex {
        PersistedIndex {
            at: Lsn(90),
            segments: vec![
                PersistedSegment {
                    segment: SegmentId(2),
                    len: 65_536,
                    dead: 1_200,
                    held: 300,
                    held_lsn: Some(Lsn(88)),
                    min_lsn: Some(Lsn(4)),
                },
                PersistedSegment {
                    segment: SegmentId(3),
                    len: 4_096,
                    dead: 0,
                    held: 0,
                    held_lsn: None,
                    min_lsn: None,
                },
            ],
            columns: vec![
                PersistedColumn {
                    column: RECORDS,
                    key_width: 34,
                },
                PersistedColumn {
                    column: NOTES,
                    key_width: VARIABLE_WIDTH,
                },
            ],
        }
    }

    /// A fixed width key, ordered by the number it carries
    fn wide_key(at: u64) -> Vec<u8> {
        let mut key = vec![0u8; 34];
        key[..8].copy_from_slice(&at.to_be_bytes());
        key
    }

    /// A key of whatever length the number asks for, still ordered by it
    fn thin_key(at: u64) -> Vec<u8> {
        let mut key = at.to_be_bytes().to_vec();
        key.extend(std::iter::repeat_n(0xAB, at as usize % 7));
        key
    }

    fn row_loc(at: u64) -> Loc {
        Loc::new(SegmentId(2 + (at % 2) as u32), at as u32 * 64, 700)
    }

    /// Write a file of this many rows a column and hand back its bytes
    fn written(sim: &SimIo, rows: u64) -> Vec<u8> {
        let driver = driver(sim);
        let head = head();
        let mut writer = PersistedWriter::try_new(&driver, &root(), &head).expect("open writer");
        writer.open_column(&driver, RECORDS, 34).expect("column");
        for at in 0..rows {
            writer
                .row(&driver, &wide_key(at), Lsn(at + 1), row_loc(at))
                .expect("row");
        }
        writer
            .open_column(&driver, NOTES, VARIABLE_WIDTH)
            .expect("column");
        for at in 0..rows {
            writer
                .row(&driver, &thin_key(at), Lsn(at + 1), row_loc(at))
                .expect("row");
        }
        assert_eq!(writer.keys(), rows * 2);
        writer.publish(&driver).expect("publish");
        sim.durable_bytes(&root().join(PERSISTED_INDEX))
            .expect("a file")
    }

    /// One row as the reader hands it back: its column, key, version and location
    type Row = (ColumnId, Vec<u8>, Lsn, Loc);

    /// Put a file's bytes back on a fresh volume and read every row off it
    fn read_back(bytes: Vec<u8>) -> Result<Vec<Row>> {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = driver(&sim);
        let file = driver
            .open(&root().join(PERSISTED_INDEX), true)
            .expect("create");
        driver
            .writev(file, 0, vec![WriteBuf::owned(bytes)])
            .expect("write");
        driver.close(file).expect("close");

        // A head that fails its own checks answers as an absence, so a test asserting
        // a refusal reads it here rather than through the open.
        let Some(mut reader) = PersistedReader::open(&driver, &root())? else {
            return Err(ReelError::Corruption("the head was refused".to_string()));
        };
        let mut rows = Vec::new();
        let read = (|| -> Result<()> {
            while reader.advance(&driver)? {
                rows.push((
                    reader.column(),
                    reader.key().to_vec(),
                    reader.lsn(),
                    reader.loc(),
                ));
            }
            Ok(())
        })();
        reader.close(&driver).expect("close");
        read?;
        Ok(rows)
    }

    // every row written comes back, in the column and the order it went down in
    #[test]
    fn roundtrip() {
        let sim = SimIo::new(FaultPlan::new(1));

        let rows = read_back(written(&sim, 3)).expect("read");

        assert_eq!(rows.len(), 6);
        assert_eq!(rows[0], (RECORDS, wide_key(0), Lsn(1), row_loc(0)));
        assert_eq!(rows[2], (RECORDS, wide_key(2), Lsn(3), row_loc(2)));
        assert_eq!(rows[3], (NOTES, thin_key(0), Lsn(1), row_loc(0)));
        assert_eq!(rows[5], (NOTES, thin_key(2), Lsn(3), row_loc(2)));
    }

    // a run longer than one block crosses the boundary without losing or repeating a row
    #[test]
    fn crosses_blocks() {
        let sim = SimIo::new(FaultPlan::new(1));
        let bytes = written(&sim, CROSSING_ROWS);
        assert!(
            bytes.len() > BLOCK_HEAD_LEN + BLOCK_BYTES,
            "{} bytes fits in one block, so nothing crossed",
            bytes.len(),
        );

        let rows = read_back(bytes).expect("read");

        assert_eq!(rows.len() as u64, CROSSING_ROWS * 2);
        for at in 0..CROSSING_ROWS {
            assert_eq!(rows[at as usize].1, wide_key(at), "row {at}");
        }
    }

    // the head round trips through its byte form on its own
    #[test]
    fn head_roundtrip() {
        let head = head();

        let parsed = PersistedIndex::unpack(&head.pack()).expect("unpack");

        assert_eq!(parsed, head);
        assert_eq!(head.pack().len(), head.packed_len());
    }

    // a flipped byte anywhere in the file is refused
    #[test]
    fn flipped_byte() {
        let sim = SimIo::new(FaultPlan::new(1));
        let bytes = written(&sim, 200);

        for at in [0, HEAD_LEN, bytes.len() / 2, bytes.len() - 1] {
            let mut rotted = bytes.clone();
            rotted[at] ^= 0xff;
            assert!(read_back(rotted).is_err(), "a flip at {at} was believed");
        }
    }

    // a file cut short is refused rather than read as far as it goes
    #[test]
    fn truncated() {
        let sim = SimIo::new(FaultPlan::new(1));
        let bytes = written(&sim, 200);

        for len in [0, HEAD_LEN, bytes.len() - 1] {
            assert!(
                read_back(bytes[..len].to_vec()).is_err(),
                "read {len} bytes"
            );
        }
    }

    // a version this build does not write is refused whole
    #[test]
    fn other_version() {
        let mut head = head().pack();
        head[VERSION_AT..COLUMNS_AT].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());

        assert!(PersistedIndex::unpack(&head).is_err());
    }

    // a segment count larger than the file holds is refused rather than reserved for
    #[test]
    fn overlong_segment_count() {
        let mut head = head().pack();
        head[SEGMENTS_AT..HEAD_CRC_AT].copy_from_slice(&u32::MAX.to_le_bytes());

        assert!(PersistedIndex::unpack(&head).is_err());
    }

    // a block naming a column the head never declared is refused
    #[test]
    fn undeclared_column() {
        let sim = SimIo::new(FaultPlan::new(1));
        let mut bytes = written(&sim, 3);
        let block =
            HEAD_LEN + head().columns.len() * COLUMN_LEN + head().segments.len() * STAMP_LEN;
        bytes[block + BLOCK_COLUMN_AT..block + BLOCK_WIDTH_AT].copy_from_slice(&9u16.to_le_bytes());

        assert!(read_back(bytes).is_err());
    }

    // rows handed over out of key order are refused rather than taken as a sorted run
    #[test]
    fn keys_must_rise() {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = driver(&sim);
        let mut writer = PersistedWriter::try_new(&driver, &root(), &head()).expect("open writer");
        writer.open_column(&driver, RECORDS, 34).expect("column");
        writer
            .row(&driver, &wide_key(5), Lsn(1), row_loc(5))
            .expect("row");
        writer
            .row(&driver, &wide_key(4), Lsn(2), row_loc(4))
            .expect("row");
        writer.publish(&driver).expect("publish");

        let bytes = sim
            .durable_bytes(&root().join(PERSISTED_INDEX))
            .expect("a file");

        assert!(read_back(bytes).is_err());
    }

    // a key of the wrong width for its column is refused as it is written
    #[test]
    fn declared_width_holds() {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = driver(&sim);
        let mut writer = PersistedWriter::try_new(&driver, &root(), &head()).expect("open writer");
        writer.open_column(&driver, RECORDS, 34).expect("column");

        let put = writer.row(&driver, &[7u8; 32], Lsn(1), row_loc(0));

        assert!(put.is_err());
        writer.abandon(&driver);
    }

    // no file at all is an absence rather than a fault
    #[test]
    fn no_file() {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = driver(&sim);

        let reader = PersistedReader::open(&driver, &root()).expect("open");

        assert!(reader.is_none());
    }

    // only a segment standing at the length it was written at is believed
    #[test]
    fn trust_needs_the_length() {
        let head = head();
        let present = HashMap::from([(SegmentId(2), 65_536), (SegmentId(3), 8_192)]);

        let standing = trusted(&head, &present);

        assert_eq!(standing, BTreeSet::from([SegmentId(2)]));
    }

    // a column the store no longer serves refuses the whole file
    #[test]
    fn unknown_column() {
        let mut changed = head();
        changed.columns[1].column = ColumnId(9);

        assert!(!admits(&changed, COLUMNS));
        assert!(admits(&head(), COLUMNS));
    }

    // a column whose keys are a different width now refuses the whole file
    #[test]
    fn changed_width() {
        let mut changed = head();
        changed.columns[0].key_width = 32;

        assert!(!admits(&changed, COLUMNS));
    }
}
