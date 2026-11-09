//! Open's reading of the roots: sealed segments from their tails, open ones from their rows

use std::path::PathBuf;

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset, RootId, RowCount, WritePlace};

use crate::lifetime::MarkRange;
use crate::io::device_error::SECTOR_BYTES;
use crate::record::record_check::{Checksum, RecordKey};
use crate::segment::block_entry::BlockEntry;
use crate::segment::segment_tail::{ColumnTail, SegmentNumber, TailHead, TailInputs};
use crate::segment::summary::{
    LostRows,
    SummaryPaths, SummaryRows,
};

/// Steps a seal on the opening thread can take before open reports it stuck
pub const MOST_INLINE_STEPS: u64 = 100_000_000;
/// Bytes of a sector, the most a bad spot of a disk hides at once
pub const GROUP_SEARCH_BYTES: usize = SECTOR_BYTES;

/// A root's files, listed before any of them is read
#[derive(Clone, Debug)]
pub struct RootListing {
    pub volume: usize,
    pub root: RootId,
    pub path: PathBuf,
    pub files: Vec<PathBuf>,
}

/// A segment whose tail is valid
#[derive(Debug)]
pub struct FoundSealed<File> {
    pub volume: usize,
    pub path: PathBuf,
    pub file: File,
    pub head: TailHead,
    pub inputs: TailInputs,
    pub columns: Vec<ColumnTail>,
    pub entries: Vec<BlockEntry>,
    pub has_rows: bool,
}

/// Which segment file an open is sorting, and how
#[derive(Clone, Copy, Debug)]
pub struct SegmentAt<'listing> {
    pub record_key: RecordKey,
    pub listing: &'listing RootListing,
    pub number: SegmentNumber,
    pub mode: OpenMode,
}

/// Whether an open owns the store or only looks at it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenMode {
    /// It holds the lock and writes
    Writable,
    /// It reads beside a writer, and changes nothing
    ReadOnly,
}

/// A sealed segment whose copies of the tail or the index all fail
#[derive(Clone, Debug)]
pub struct FoundDamaged {
    pub volume: usize,
    pub path: PathBuf,
    pub number: SegmentNumber,
    pub file_length: ByteCount,
    pub head: Option<TailHead>,
    pub columns: Vec<ColumnTail>,
}

/// A data file that never took a record, unlinked unless the summary lists it
#[derive(Clone, Debug)]
pub struct FoundEmpty {
    pub volume: usize,
    pub root: PathBuf,
    pub number: SegmentNumber,
    pub file_length: ByteCount,
}

/// What one segment turned out to be
#[derive(Debug)]
pub enum SegmentRead {
    /// Its tail and index read
    Sealed {
        head: TailHead,
        inputs: TailInputs,
        columns: Vec<ColumnTail>,
        entries: Vec<BlockEntry>,
    },
    /// It has a rows file, so it was open when the store stopped
    Open,
    /// Its copies fail, and its file stays for the rebuild
    Damaged {
        head: Option<TailHead>,
        columns: Vec<ColumnTail>,
        file_length: ByteCount,
    },
    /// Its first head is empty and nothing else says what it is
    Empty {
        file_length: ByteCount,
    },
}

/// A column's facts with its keys copied out of the summary
#[derive(Clone, Debug)]
pub struct OwnedColumn {
    pub column: ColumnId,
    pub lowest: Vec<u8>,
    pub bound: Vec<u8>,
    pub row_count: RowCount,
    pub grave_count: RowCount,
    pub dead_rows: RowCount,
    pub dead_graves: RowCount,
    pub live_bytes: ByteCount,
    pub dead_bytes: ByteCount,
}

/// A volume's summary as open found it
#[derive(Debug)]
pub struct SummaryFound {
    pub rows: SummaryRows,
    pub paths: SummaryPaths,
    pub lost: LostRows,
}

/// A segment with no valid tail and a rows file
#[derive(Debug)]
pub struct FoundOpen<File> {
    pub volume: usize,
    pub root: RootId,
    pub number: SegmentNumber,
    pub data: File,
}

/// A file open couldn't remove, which the park holds until a retry gets it gone
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeldFile {
    pub volume: usize,
    pub number: SegmentNumber,
}

/// What a read of every root found
#[derive(Debug)]
pub struct RootScan<File> {
    pub sealed: Vec<FoundSealed<File>>,
    pub scanned: Vec<ScannedOpen<File>>,
    pub damaged: Vec<FoundDamaged>,
    pub empty: Vec<FoundEmpty>,
    pub highest_segment: SegmentNumber,
    pub held: Vec<HeldFile>,
}

/// An open segment with its rows file walked, waiting for its turn
#[derive(Debug)]
pub struct ScannedOpen<File> {
    pub found: FoundOpen<File>,
    pub rows: File,
    pub scan: RowsScan,
}

/// What a walk of one rows file found
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RowsScan {
    pub valid_end: FileOffset,
    pub synced_through: FileOffset,
    pub row_count: RowCount,
    pub has_rot: bool,
    pub rot_sum: Checksum,
    pub highest_place: WritePlace,
}

/// The rows of a recovered segment so far
#[derive(Clone, Copy, Debug)]
pub struct RecoveredRows {
    pub write_end: FileOffset,
    pub row_count: RowCount,
    pub lowest_place: WritePlace,
    pub highest_place: WritePlace,
    pub lost_region: Option<(FileOffset, FileOffset)>,
    pub cover_rows: usize,
    pub marks: MarkRange,
    pub has_deletes: bool,
}

mod replay;
mod scan;
mod summaries;

pub use replay::{ReplayTarget, RowsWalked, is_tail_stale, load_covers, next_group, pass_open_rows, recover_segments, walk_listed_rows, walk_rows};
pub use scan::{has_rows, holds_nothing, list_roots, open_for, root_of_path, scan_roots};
pub use summaries::{damage_open, read_summaries, row_ranges, save_cleared_spans, settle_summaries, whole_columns, widen_row};
#[cfg(any(test, feature = "test-hooks"))]
pub use replay::{copy_row, seal_every_open};
