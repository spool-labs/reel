//! One segment taking writes: its files, its claims and its waiting rows

use std::ffi::OsStr;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering, fence};
use std::ops::Deref;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use reel_api::units::{ByteCount, FileOffset, RootId, RowCount, WritePlace};

use crate::index::open_slots::OpenSlot;
use crate::pool::TOUCH_BYTE;
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

/// Records of an open segment on their way to the kernel, in a range of its data file claimed ahead. Only the holder
/// of the slot's queue lock writes here. A read takes no lock: bytes below `used` never change until a reset, and a
/// reset moves `generation`, so a read that saw the same generation on both sides read whole bytes
#[derive(Debug)]
pub struct RecordWindow {
    bytes: Vec<AtomicU8>,
    generation: AtomicU64,
    start: AtomicU64,
    used: AtomicUsize,
}

impl RecordWindow {
    /// Allocate a window with no range yet
    pub fn new() -> Self {
        let mut bytes = Vec::with_capacity(RECORD_WINDOW_BYTES);
        for _byte in 0..RECORD_WINDOW_BYTES {
            bytes.push(AtomicU8::new(TOUCH_BYTE));
        }
        Self {
            bytes,
            generation: AtomicU64::new(0),
            start: AtomicU64::new(0),
            used: AtomicUsize::new(0),
        }
    }

    /// Read the heap this window holds
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount((self.bytes.capacity() * size_of::<AtomicU8>()) as u64)
    }

    /// Count the bytes of records in the window
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    /// Check the window holds no record
    pub fn is_empty(&self) -> bool {
        self.used() == 0
    }

    /// Read where the window starts
    pub fn start(&self) -> FileOffset {
        FileOffset(self.start.load(Ordering::Acquire))
    }

    /// Read where the window's records end
    pub fn end(&self) -> FileOffset {
        FileOffset(self.start().0 + self.used() as u64)
    }

    /// Start on a new range of the file, once nothing in the window needs reading from here
    pub fn reset(&self, start: FileOffset) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.used.store(0, Ordering::Release);
        self.start.store(start.0, Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    /// Copy a record in and return where the file will hold it, which only the queue lock's holder may do
    pub fn push(&self, parts: [&[u8]; 2]) -> FileOffset {
        let used = self.used();
        let offset = FileOffset(self.start().0 + used as u64);
        let mut at = used;
        for part in parts {
            for byte in part {
                self.bytes[at].store(*byte, Ordering::Relaxed);
                at += 1;
            }
        }
        self.used.store(at, Ordering::Release);
        offset
    }

    /// Copy the records out for a write call
    pub fn copy_out(&self, out: &mut [u8]) -> usize {
        let used = self.used();
        for (byte, stored) in out[..used].iter_mut().zip(&self.bytes) {
            *byte = stored.load(Ordering::Relaxed);
        }
        used
    }

    /// Copy a record out with no lock, or return false when the window doesn't hold it whole
    pub fn read(&self, offset: FileOffset, record: &mut [u8]) -> bool {
        let before = self.generation.load(Ordering::Acquire);
        if !before.is_multiple_of(2) {
            return false;
        }
        let Some(at) = offset.0.checked_sub(self.start().0).map(|at| at as usize) else {
            return false;
        };
        if at + record.len() > self.used() {
            return false;
        }
        for (byte, stored) in record.iter_mut().zip(&self.bytes[at..]) {
            *byte = stored.load(Ordering::Relaxed);
        }
        fence(Ordering::Acquire);
        self.generation.load(Ordering::Relaxed) == before
    }
}

impl Default for RecordWindow {
    fn default() -> Self {
        Self::new()
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
    pub live_window: usize,
    pub window_room: usize,
    pub has_kept_window: bool,
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
            live_window: 0,
            window_room: 0,
            has_kept_window: false,
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
        self.window_room = 0;
        self.has_kept_window = false;
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
