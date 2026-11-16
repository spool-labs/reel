//! One simulated file: what a reader sees and what a power cut leaves

use std::collections::BTreeSet;
use std::path::PathBuf;

use reel_core::units::{ByteCount, FileOffset};

use crate::seeded_random::SeededRandom;

/// A torn write keeps or loses each sector of this size on its own
pub const SECTOR_BYTES: u64 = 512;
/// The block a free in place gives back
pub const FREED_BLOCK_BYTES: u64 = 4_096;
/// Each sector of a torn write survives one time in this many
pub const SECTOR_SURVIVAL_ODDS: u64 = 2;
/// What a power cut can do to one unsynced change, each as likely as the others
pub const POWER_CUT_OUTCOMES: [PowerCutOutcome; 3] = [PowerCutOutcome::Lost, PowerCutOutcome::Kept, PowerCutOutcome::Torn];

/// What a power cut does to one unsynced change
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PowerCutOutcome {
    /// None of it reached the disk
    Lost,
    /// All of it reached the disk
    Kept,
    /// Some of its sectors reached the disk
    Torn,
}

/// The bits to flip in one byte
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct BitMask(pub u8);

/// A change the kernel holds and no sync has covered
#[derive(Clone, Debug)]
pub enum PendingChange {
    /// Bytes written at an offset
    Write { offset: FileOffset, bytes: Vec<u8> },
    /// The file cut back to a length
    Truncate { length: ByteCount },
    /// Room set aside at create, which reads as zeros and holds no bytes
    Reserve { length: ByteCount },
}

/// A file's bytes: those written, then zeros out to its length, which take no memory
#[derive(Clone, Debug, Default)]
pub struct LazyBytes {
    written: Vec<u8>,
    length: usize,
}

impl LazyBytes {
    /// Read the file's length
    pub fn len(&self) -> usize {
        self.length
    }

    /// Check the file has no bytes
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Copy bytes out, with zeros past what was written
    pub fn read(&self, start: usize, out: &mut [u8]) {
        let from = self.written.len().min(start);
        let to = self.written.len().min(start + out.len());
        out[..to - from].copy_from_slice(&self.written[from..to]);
        out[to - from..].fill(0);
    }

    /// Copy bytes in, growing the file when needed
    pub fn write(&mut self, offset: FileOffset, bytes: &[u8]) {
        let start = offset.0 as usize;
        let end = start + bytes.len();
        if self.written.len() < end {
            self.written.resize(end, 0);
        }
        self.written[start..end].copy_from_slice(bytes);
        self.length = self.length.max(end);
    }

    /// Set the length, cutting what was written or adding zeros that hold nothing
    pub fn resize(&mut self, length: usize) {
        self.written.truncate(length);
        self.length = length;
    }

    /// Flip bits of one byte, when the file reaches it
    pub fn flip(&mut self, offset: FileOffset, mask: BitMask) {
        let at = offset.0 as usize;
        if at >= self.length {
            return;
        }
        if self.written.len() <= at {
            self.written.resize(at + 1, 0);
        }
        self.written[at] ^= mask.0;
    }
}

/// The bytes of one file
#[derive(Clone, Debug, Default)]
pub struct FileImage {
    pub visible: LazyBytes,
    pub durable: LazyBytes,
    pub pending: Vec<PendingChange>,
    pub disk: Option<PathBuf>,
    pub handle_count: u64,
    pub is_named: bool,
    pub bad_sectors: BTreeSet<u64>,
    pub freed_blocks: BTreeSet<u64>,
}

impl FileImage {
    /// Set room aside, which a reader sees at once as zeros
    pub fn reserve(&mut self, length: ByteCount) {
        self.visible.resize(length.0 as usize);
        self.pending.push(PendingChange::Reserve { length });
    }

    /// Apply a write that a reader sees at once
    pub fn write(&mut self, offset: FileOffset, bytes: Vec<u8>) {
        self.visible.write(offset, &bytes);
        self.pending.push(PendingChange::Write { offset, bytes });
    }

    /// Cut the file back, which a reader sees at once
    pub fn truncate(&mut self, length: ByteCount) {
        self.visible.resize(length.0 as usize);
        self.pending.push(PendingChange::Truncate { length });
    }

    /// Make every pending change durable
    pub fn sync(&mut self) {
        for change in self.pending.drain(..) {
            apply_change(&mut self.durable, &change);
        }
    }

    /// Forget the pending changes, as a kernel does after a failed sync
    pub fn fail_sync(&mut self) {
        self.pending.clear();
    }

    /// Replay what a power cut leaves: each change survives, tears or goes on its own
    pub fn power_cut(&mut self, random: &mut SeededRandom) {
        for change in self.pending.drain(..) {
            let pick = random.below(POWER_CUT_OUTCOMES.len() as u64) as usize;
            match POWER_CUT_OUTCOMES[pick] {
                PowerCutOutcome::Lost => {},
                PowerCutOutcome::Kept => apply_change(&mut self.durable, &change),
                PowerCutOutcome::Torn => apply_torn(&mut self.durable, &change, random),
            }
        }
        self.visible.clone_from(&self.durable);
        self.freed_blocks.clear();
    }

    /// Flip bits of one byte in what readers see and in what survives
    pub fn rot(&mut self, offset: FileOffset, mask: BitMask) {
        self.visible.flip(offset, mask);
        self.durable.flip(offset, mask);
    }

    /// Note a range's whole blocks as given back, and return the bytes that were still held
    pub fn note_freed(&mut self, offset: FileOffset, length: ByteCount) -> ByteCount {
        let blocks = offset.0 / FREED_BLOCK_BYTES..(offset.0 + length.0) / FREED_BLOCK_BYTES;
        let newly = blocks.filter(|block| self.freed_blocks.insert(*block)).count();
        ByteCount(newly as u64 * FREED_BLOCK_BYTES)
    }

    /// Read the bytes the file takes on its disk
    pub fn held_bytes(&self) -> ByteCount {
        let whole = (self.visible.len() as u64).next_multiple_of(FREED_BLOCK_BYTES);
        ByteCount(whole - self.freed_blocks.len() as u64 * FREED_BLOCK_BYTES)
    }

    /// Check a read can reach these bytes
    pub fn is_readable(&self, start: usize, end: usize) -> bool {
        let first = start as u64 / SECTOR_BYTES;
        let last = end.saturating_sub(1) as u64 / SECTOR_BYTES;
        start == end || self.bad_sectors.range(first..=last).next().is_none()
    }

    /// Check whether the bytes still take space on the disk
    pub fn is_held(&self) -> bool {
        self.is_named || self.handle_count > 0
    }
}

/// Apply one change whole
pub fn apply_change(image: &mut LazyBytes, change: &PendingChange) {
    match change {
        PendingChange::Write { offset, bytes } => image.write(*offset, bytes),
        PendingChange::Truncate { length } => image.resize(length.0 as usize),
        PendingChange::Reserve { length } => image.resize(image.len().max(length.0 as usize)),
    }
}

/// Apply any set of a write's sectors, since pages reach a disk in any order
pub fn apply_torn(image: &mut LazyBytes, change: &PendingChange, random: &mut SeededRandom) {
    match change {
        PendingChange::Write { offset, bytes } => {
            for (index, sector) in bytes.chunks(SECTOR_BYTES as usize).enumerate() {
                if random.one_in(SECTOR_SURVIVAL_ODDS) {
                    let start = offset.0 + index as u64 * SECTOR_BYTES;
                    image.write(FileOffset(start), sector);
                }
            }
        },
        PendingChange::Truncate { .. } => apply_change(image, change),
        // zeros that tore read like zeros that never came, so all a tear changes is how far the file reaches
        PendingChange::Reserve { length } => {
            let sectors = length.0.div_ceil(SECTOR_BYTES);
            let reach = (random.below(sectors + 1) * SECTOR_BYTES).min(length.0);
            image.resize(image.len().max(reach as usize));
        },
    }
}
