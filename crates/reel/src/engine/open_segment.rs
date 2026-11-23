//! One segment taking writes: its files, its claims and its waiting rows

use std::ffi::OsStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering, fence};
use std::ops::Deref;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use reel_core::units::{ByteCount, DeathMark, FileOffset, Nanoseconds, RootId, RowCount, WritePlace};

use crate::index::open_slots::OpenSlot;
use crate::lifetime::MarkRange;
use crate::pool::TOUCH_BYTE;
use crate::rows::row_group::RowGroupBuffer;
use crate::segment::segment_tail::SegmentNumber;

/// Hex digits of a segment number in a file name
pub const NUMBER_DIGITS: usize = 16;
/// The end of a segment's data file name
pub const DATA_SUFFIX: &str = ".data";
/// The end of a segment's rows file name
pub const ROWS_SUFFIX: &str = ".rows";
/// The suffix of a rewrite's output before it has a tail
pub const OUTPUT_SUFFIX: &str = ".out";
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
    pub reserved_end: FileOffset,
    pub is_reserve_asked: bool,
    /// The reserve job is growing this segment's room with no lock held, so its seal waits: a cut file grown again would bury its tails
    pub is_room_holding: bool,
    pub row_count: RowCount,
    pub in_flight: u32,
    pub cover_rows: usize,
    pub lowest_place: WritePlace,
    pub highest_place: WritePlace,
    pub ceiling: Option<DeathMark>,
    pub marks: MarkRange,
    pub has_deletes: bool,
    pub seen_rows: RowCount,
    pub quiet_since: Nanoseconds,
    pub first_row_at: Option<Nanoseconds>,
    pub delete_seen: Option<Nanoseconds>,
    /// Its rows came back from its rows file at open and no write has landed in it since, so it seals at the idle wait
    /// whatever its size: nothing trickles into it
    pub is_replayed: bool,
}

/// Bytes an open segment holds on its disk ahead of its writes. It holds this much at create and gains it again a step at a
/// time, so a store holds no whole segment of disk it hasn't written
pub const RESERVE_STEP_BYTES: u64 = 64 * 1024 * 1024;
/// A step is at most this share of a segment, so a small store holds little room ahead of each open slot
pub const RESERVE_STEP_SHARE: u64 = 8;
/// The least step, so a small segment still grows a few writes at a time
pub const LEAST_RESERVE_STEP: u64 = 1024 * 1024;

/// Work out a volume's room step from its segment size
pub fn reserve_step(segment_bytes: ByteCount) -> u64 {
    (segment_bytes.0 / RESERVE_STEP_SHARE).clamp(LEAST_RESERVE_STEP, RESERVE_STEP_BYTES)
}

/// The smallest step a set segment size reserves by: one page
pub const LEAST_SET_RESERVE_STEP: u64 = 4096;

/// Read the step a volume with a set segment size reserves by: an eighth of the segment, down to a page, with no 1 MiB floor
pub const fn set_reserve_step(segment_bytes: ByteCount) -> u64 {
    let step = segment_bytes.0 / RESERVE_STEP_SHARE;
    if step < LEAST_SET_RESERVE_STEP { LEAST_SET_RESERVE_STEP } else { step }
}

/// Bytes of the file range a slot's small records fill before they reach the kernel in one write call
pub const RECORD_WINDOW_BYTES: usize = 64 * 1024;
/// The largest record that goes through a window, so bulk values are never copied into one
pub const MOST_WINDOW_RECORD_BYTES: usize = 4 * 1024;
/// A set segment size's share each record window takes, as a divisor, so a small segment counts little of a window it barely used
pub const SET_WINDOW_DIVISOR: usize = 32;

/// Read the window bytes for a volume's set segment size: a 32nd of it, between the largest record a window
/// takes and RECORD_WINDOW_BYTES, or RECORD_WINDOW_BYTES when it sets none. It stays a whole number of words
pub fn window_bytes_for(set_size: Option<u64>) -> usize {
    let bytes = set_size.map_or(RECORD_WINDOW_BYTES, |bytes| (bytes as usize / SET_WINDOW_DIVISOR).clamp(MOST_WINDOW_RECORD_BYTES, RECORD_WINDOW_BYTES));
    bytes - bytes % WORD_BYTES
}

/// The bytes in one of a window's words
pub const WORD_BYTES: usize = size_of::<u64>();

/// Records of an open segment on their way to the kernel, in a range of its data file claimed ahead. Only the holder
/// of the slot's queue lock writes here. A read takes no lock: bytes below `used` never change until a reset, and a
/// reset moves `generation`, so a read that saw the same generation on both sides read whole bytes. The bytes sit in
/// words, so a copy moves eight at a time and a read beside a write stays atomic
#[derive(Debug)]
pub struct RecordWindow {
    words: Vec<AtomicU64>,
    generation: AtomicU64,
    start: AtomicU64,
    used: AtomicUsize,
}

impl RecordWindow {
    /// Allocate a window of RECORD_WINDOW_BYTES with no range yet
    pub fn new() -> Self {
        Self::with_bytes(RECORD_WINDOW_BYTES)
    }

    /// Allocate a window of this many bytes, a whole number of words, with no range yet. A reader's windows take none,
    /// since it writes nothing
    pub fn with_bytes(length: usize) -> Self {
        let count = length / WORD_BYTES;
        let mut words = Vec::with_capacity(count);
        for _word in 0..count {
            words.push(AtomicU64::new(u64::from_ne_bytes([TOUCH_BYTE; WORD_BYTES])));
        }
        Self {
            words,
            generation: AtomicU64::new(0),
            start: AtomicU64::new(0),
            used: AtomicUsize::new(0),
        }
    }

    /// Read the heap this window holds
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount((self.words.capacity() * size_of::<AtomicU64>()) as u64)
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
            self.store_bytes(at, part);
            at += part.len();
        }
        self.used.store(at, Ordering::Release);
        offset
    }

    /// Lend the window's records for a write call, with no copy
    ///
    /// # Safety
    /// No push may run on this window until the loan ends. The window a slot keeps for its send is one: pushes go to the
    /// live window, and the kept one takes none until its reset
    pub unsafe fn records(&self) -> &[u8] {
        let used = self.used().min(self.words.len() * WORD_BYTES);
        // SAFETY: the words are plain memory a u64 at a time, nothing stores to them for the loan, and used stays inside them
        unsafe { std::slice::from_raw_parts(self.words.as_ptr().cast::<u8>(), used) }
    }

    /// Check the window may hold a record whole, with no copy. A read still checks it
    pub fn may_hold(&self, offset: FileOffset, length: usize) -> bool {
        offset.0.checked_sub(self.start().0).is_some_and(|at| at as usize + length <= self.used())
    }

    /// Copy a record out with no lock in two parts, its head and its tail, so the tail can land where its reader keeps the value.
    /// False when the window doesn't hold it whole
    pub fn read_split(&self, offset: FileOffset, (head, tail): (&mut [u8], &mut [u8])) -> bool {
        let before = self.generation.load(Ordering::Acquire);
        if !before.is_multiple_of(2) {
            return false;
        }
        let Some(at) = offset.0.checked_sub(self.start().0).map(|at| at as usize) else {
            return false;
        };
        if at + head.len() + tail.len() > self.used() {
            return false;
        }
        self.load_bytes(at, head);
        self.load_bytes(at + head.len(), tail);
        fence(Ordering::Acquire);
        self.generation.load(Ordering::Relaxed) == before
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
        self.load_bytes(at, record);
        fence(Ordering::Acquire);
        self.generation.load(Ordering::Relaxed) == before
    }

    /// Store bytes from a byte offset: a word at a time between the ends, and the words at the ends merged with what they hold
    fn store_bytes(&self, at: usize, bytes: &[u8]) {
        let lead = (WORD_BYTES - at % WORD_BYTES) % WORD_BYTES;
        let (head, rest) = bytes.split_at(lead.min(bytes.len()));
        self.merge_word(at, head);
        let first = (at + head.len()) / WORD_BYTES;
        let (whole, tail) = rest.as_chunks::<WORD_BYTES>();
        for (word, chunk) in self.words[first..first + whole.len()].iter().zip(whole) {
            word.store(u64::from_ne_bytes(*chunk), Ordering::Relaxed);
        }
        self.merge_word((first + whole.len()) * WORD_BYTES, tail);
    }

    /// Load bytes from a byte offset: a word at a time between the ends, and part of the words at the ends
    fn load_bytes(&self, at: usize, out: &mut [u8]) {
        let lead = (WORD_BYTES - at % WORD_BYTES) % WORD_BYTES;
        let (head, rest) = out.split_at_mut(lead.min(out.len()));
        self.load_part(at, head);
        let first = (at + head.len()) / WORD_BYTES;
        let (whole, tail) = rest.as_chunks_mut::<WORD_BYTES>();
        let count = whole.len();
        for (chunk, word) in whole.iter_mut().zip(&self.words[first..first + count]) {
            *chunk = word.load(Ordering::Relaxed).to_ne_bytes();
        }
        self.load_part((first + count) * WORD_BYTES, tail);
    }

    /// Store bytes inside one word, keeping the word's other bytes. Only the queue lock's holder writes, so nothing changes
    /// the word between its load and its store
    fn merge_word(&self, at: usize, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let word = &self.words[at / WORD_BYTES];
        let mut held = word.load(Ordering::Relaxed).to_ne_bytes();
        let within = at % WORD_BYTES;
        held[within..within + bytes.len()].copy_from_slice(bytes);
        word.store(u64::from_ne_bytes(held), Ordering::Relaxed);
    }

    /// Load bytes inside one word
    fn load_part(&self, at: usize, out: &mut [u8]) {
        if out.is_empty() {
            return;
        }
        let held = self.words[at / WORD_BYTES].load(Ordering::Relaxed).to_ne_bytes();
        let within = at % WORD_BYTES;
        out.copy_from_slice(&held[within..within + out.len()]);
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
    pub marks: MarkRange,
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
            marks: MarkRange::NONE,
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
        self.marks = MarkRange::NONE;
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

/// Build the path a rewrite's output has until its tail makes it a segment
pub fn output_path(root: &Path, number: SegmentNumber) -> SegmentPath {
    segment_path(root, number, OUTPUT_SUFFIX)
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
