//! One segment taking writes: its files, its claims and its waiting rows

use std::ffi::OsStr;
use std::mem;
use std::ops::Deref;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use reel_api::units::{ByteCount, FileOffset, RootId, RowCount, WritePlace};

use crate::index::open_slots::OpenSlot;
use crate::pool::touched_zeroes;
use crate::rows::row_group::RowGroupBuffer;
use crate::segment::segment_tail::SegmentNumber;

/// Hex digits of a segment number in a file name
pub const NUMBER_DIGITS: usize = 16;
/// The end of a segment's data file name
pub const DATA_SUFFIX: &str = ".data";
/// The end of a segment's rows file name
pub const ROWS_SUFFIX: &str = ".rows";
/// The longest path a segment file can have, with its root, under what the standard library keeps on the stack
pub const MOST_PATH_BYTES: usize = 376;
/// The digits of a segment number
pub const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// What an open segment is doing
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentPhase {
    /// It takes new claims
    Taking,
    /// One long batch has it, so no new claim lands rows behind that batch's writing turn
    Alone,
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
    pub cover_rows: usize,
    pub lowest_place: WritePlace,
    pub highest_place: WritePlace,
}

/// Bytes of the file range a slot's small records fill before they reach the kernel in one write call
pub const RECORD_WINDOW_BYTES: usize = 64 * 1024;
/// The largest record that goes through a window, so bulk values are never copied into one
pub const MOST_WINDOW_RECORD_BYTES: usize = 4 * 1024;

/// Records of an open segment on their way to the kernel, in a range of its data file claimed ahead
#[derive(Debug)]
pub struct RecordWindow {
    bytes: Vec<u8>,
    start: FileOffset,
    used: usize,
    room: usize,
}

impl Default for RecordWindow {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordWindow {
    /// Allocate a window with no range yet
    pub fn new() -> Self {
        Self {
            bytes: touched_zeroes(RECORD_WINDOW_BYTES),
            start: FileOffset(0),
            used: 0,
            room: 0,
        }
    }

    /// Read the heap this window holds
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount(self.bytes.capacity() as u64)
    }

    /// Check the window holds no record
    pub fn is_empty(&self) -> bool {
        self.used == 0
    }

    /// Check a record of this length fits
    pub fn has_room(&self, length: usize) -> bool {
        self.used + length <= self.room
    }

    /// Start on a new range of the file
    pub fn reset(&mut self, start: FileOffset, room: usize) {
        self.start = start;
        self.used = 0;
        self.room = room;
    }

    /// Copy a record in and return where the file will hold it
    pub fn push(&mut self, parts: [&[u8]; 2]) -> FileOffset {
        let offset = FileOffset(self.start.0 + self.used as u64);
        for part in parts {
            self.bytes[self.used..self.used + part.len()].copy_from_slice(part);
            self.used += part.len();
        }
        offset
    }

    /// Read where the window starts
    pub fn start(&self) -> FileOffset {
        self.start
    }

    /// Read the records so far
    pub fn records(&self) -> &[u8] {
        &self.bytes[..self.used]
    }

    /// Find a record in the window
    pub fn record(&self, offset: FileOffset, length: ByteCount) -> Option<&[u8]> {
        let at = offset.0.checked_sub(self.start.0)? as usize;
        self.records().get(at..at + length.0 as usize)
    }

    /// Hand the records to a spare window, and go on in what's left of the range
    pub fn hand_over(&mut self, spare: &mut Self) {
        spare.reset(FileOffset(self.start.0 + self.used as u64), self.room - self.used);
        mem::swap(self, spare);
    }
}

/// The rows of a segment waiting to reach the kernel
#[derive(Debug)]
pub struct RowQueue {
    pub filling: RowGroupBuffer,
    pub rows_end: FileOffset,
    pub synced_through: FileOffset,
    pub marked_end: FileOffset,
    pub is_writing: bool,
    pub reserved: usize,
    pub records: RecordWindow,
    pub sending: Option<(FileOffset, FileOffset)>,
    pub is_refilling: bool,
}

impl RowQueue {
    /// Allocate one group's buffer
    pub fn new(capacity: ByteCount) -> Self {
        Self {
            filling: RowGroupBuffer::new(capacity),
            rows_end: FileOffset(0),
            synced_through: FileOffset(0),
            marked_end: FileOffset(0),
            is_writing: false,
            reserved: 0,
            records: RecordWindow::new(),
            sending: None,
            is_refilling: false,
        }
    }

    /// Start a new segment in the same buffer
    pub fn reset(&mut self, rows_end: FileOffset, synced_through: FileOffset) {
        self.filling.clear();
        self.rows_end = rows_end;
        self.synced_through = synced_through;
        self.marked_end = FileOffset(0);
        self.is_writing = false;
        self.reserved = 0;
        self.records.reset(FileOffset(0), 0);
        self.sending = None;
        self.is_refilling = false;
    }
}

/// A segment file's path, built on the stack so a new segment or a seal allocates nothing
#[derive(Clone, Copy, Debug)]
pub struct SegmentPath {
    bytes: [u8; MOST_PATH_BYTES],
    length: usize,
}

impl Deref for SegmentPath {
    type Target = Path;

    fn deref(&self) -> &Path {
        Path::new(OsStr::from_bytes(&self.bytes[..self.length]))
    }
}

/// Check a root leaves room for a segment file's name, which open does once
pub fn fits_a_segment_path(root: &Path) -> bool {
    root.as_os_str().len() + 1 + NUMBER_DIGITS + DATA_SUFFIX.len() <= MOST_PATH_BYTES
}

/// Build a path from a root, a segment's number in hex and a suffix
pub fn segment_path(root: &Path, number: SegmentNumber, suffix: &str) -> SegmentPath {
    let mut path = SegmentPath {
        bytes: [0; MOST_PATH_BYTES],
        length: 0,
    };
    let root = root.as_os_str().as_bytes();
    path.bytes[..root.len()].copy_from_slice(root);
    path.bytes[root.len()] = b'/';
    let digits_at = root.len() + 1;
    for digit in 0..NUMBER_DIGITS {
        let shift = 4 * (NUMBER_DIGITS - 1 - digit);
        path.bytes[digits_at + digit] = HEX_DIGITS[((number.0 >> shift) & 0xF) as usize];
    }
    let suffix_at = digits_at + NUMBER_DIGITS;
    path.bytes[suffix_at..suffix_at + suffix.len()].copy_from_slice(suffix.as_bytes());
    path.length = suffix_at + suffix.len();
    path
}

/// Build a segment's data file path
pub fn data_path(root: &Path, number: SegmentNumber) -> SegmentPath {
    segment_path(root, number, DATA_SUFFIX)
}

/// Build a segment's rows file path
pub fn rows_path(root: &Path, number: SegmentNumber) -> SegmentPath {
    segment_path(root, number, ROWS_SUFFIX)
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
