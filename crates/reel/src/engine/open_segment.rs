//! One segment taking writes: its files, its claims and its waiting rows

use std::path::{Path, PathBuf};

use reel_api::units::{ByteCount, FileOffset, RootId, RowCount, WritePlace};

use crate::index::open_slots::OpenSlot;
use crate::rows::row_group::RowGroupBuffer;
use crate::segment::segment_tail::SegmentNumber;

/// Hex digits of a segment number in a file name
pub const NUMBER_DIGITS: usize = 16;
/// The end of a segment's data file name
pub const DATA_SUFFIX: &str = ".data";
/// The end of a segment's rows file name
pub const ROWS_SUFFIX: &str = ".rows";

/// What an open segment is doing
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentPhase {
    /// It takes new claims
    Taking,
    /// It takes no new claims, and seals once its writers land
    Sealing,
}

/// Where a segment stands, behind the volume's claim lock
#[derive(Clone, Debug)]
pub struct OpenSegment<File> {
    pub number: SegmentNumber,
    pub slot: OpenSlot,
    pub root: RootId,
    pub data: File,
    pub rows: File,
    pub phase: SegmentPhase,
    pub write_end: FileOffset,
    pub row_count: RowCount,
    pub in_flight: u32,
    pub lowest_place: WritePlace,
    pub highest_place: WritePlace,
}

/// The rows of a segment waiting to reach the kernel
#[derive(Debug)]
pub struct RowQueue {
    pub filling: RowGroupBuffer,
    pub rows_end: FileOffset,
    pub synced_through: FileOffset,
    pub is_writing: bool,
    pub reserved: usize,
}

impl RowQueue {
    /// Allocate one group's buffer
    pub fn new(capacity: ByteCount) -> Self {
        Self {
            filling: RowGroupBuffer::new(capacity),
            rows_end: FileOffset(0),
            synced_through: FileOffset(0),
            is_writing: false,
            reserved: 0,
        }
    }

    /// Start a new segment in the same buffer
    pub fn reset(&mut self, rows_end: FileOffset, synced_through: FileOffset) {
        self.filling.clear();
        self.rows_end = rows_end;
        self.synced_through = synced_through;
        self.is_writing = false;
        self.reserved = 0;
    }
}

/// Build a segment's data file path
pub fn data_path(root: &Path, number: SegmentNumber) -> PathBuf {
    root.join(format!("{:0width$x}{DATA_SUFFIX}", number.0, width = NUMBER_DIGITS))
}

/// Build a segment's rows file path
pub fn rows_path(root: &Path, number: SegmentNumber) -> PathBuf {
    root.join(format!("{:0width$x}{ROWS_SUFFIX}", number.0, width = NUMBER_DIGITS))
}

/// Read a segment number from a file name, or nothing when it isn't one of a segment's files
pub fn parse_segment(path: &Path, suffix: &str) -> Option<SegmentNumber> {
    let name = path.file_name()?.to_str()?;
    let digits = name.strip_suffix(suffix)?;
    if digits.len() != NUMBER_DIGITS {
        return None;
    }
    Some(SegmentNumber(u64::from_str_radix(digits, 16).ok()?))
}
