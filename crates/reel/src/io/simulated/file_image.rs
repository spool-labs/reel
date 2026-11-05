//! One simulated file: what a reader sees and what a power cut leaves

use crate::io::simulated::seeded_random::SeededRandom;
use reel_api::units::{ByteCount, FileOffset};

/// A torn write keeps a whole number of these
pub const SECTOR_BYTES: u64 = 512;
/// Each unsynced change is lost, kept or torn with equal odds
pub const POWER_CUT_OUTCOMES: u64 = 3;

/// A change the kernel holds and no sync has covered
#[derive(Clone, Debug)]
pub enum PendingChange {
    /// Bytes written at an offset
    Write { offset: FileOffset, bytes: Vec<u8> },
    /// The file cut back to a length
    Truncate { length: ByteCount },
}

/// The bytes of one file
#[derive(Clone, Debug, Default)]
pub struct FileImage {
    pub visible: Vec<u8>,
    pub durable: Vec<u8>,
    pub pending: Vec<PendingChange>,
    pub handle_count: u64,
    pub is_named: bool,
}

impl FileImage {
    /// Apply a write that a reader sees at once
    pub fn write(&mut self, offset: FileOffset, bytes: Vec<u8>) {
        apply_write(&mut self.visible, offset, &bytes);
        self.pending.push(PendingChange::Write { offset, bytes });
    }

    /// Cut the file back, which a reader sees at once
    pub fn truncate(&mut self, length: ByteCount) {
        self.visible.resize(length.0 as usize, 0);
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
            match random.below(POWER_CUT_OUTCOMES) {
                0 => {},
                1 => apply_change(&mut self.durable, &change),
                _ => apply_torn(&mut self.durable, &change, random),
            }
        }
        self.visible.clone_from(&self.durable);
    }

    /// Flip one bit in what readers see and in what survives
    pub fn rot(&mut self, offset: FileOffset, mask: u8) {
        if let Some(byte) = self.visible.get_mut(offset.0 as usize) {
            *byte ^= mask;
        }
        if let Some(byte) = self.durable.get_mut(offset.0 as usize) {
            *byte ^= mask;
        }
    }
}

/// Copy bytes into an image, growing it with zeros when needed
pub fn apply_write(image: &mut Vec<u8>, offset: FileOffset, bytes: &[u8]) {
    let start = offset.0 as usize;
    let end = start + bytes.len();
    if image.len() < end {
        image.resize(end, 0);
    }
    image[start..end].copy_from_slice(bytes);
}

/// Apply one change whole
pub fn apply_change(image: &mut Vec<u8>, change: &PendingChange) {
    match change {
        PendingChange::Write { offset, bytes } => apply_write(image, *offset, bytes),
        PendingChange::Truncate { length } => image.resize(length.0 as usize, 0),
    }
}

/// Apply the leading sectors of a write, and a truncate whole
pub fn apply_torn(image: &mut Vec<u8>, change: &PendingChange, random: &mut SeededRandom) {
    match change {
        PendingChange::Write { offset, bytes } => {
            let sectors = (bytes.len() as u64).div_ceil(SECTOR_BYTES);
            let kept = random.below(sectors.saturating_add(1)).saturating_mul(SECTOR_BYTES);
            let end = bytes.len().min(kept as usize);
            apply_write(image, *offset, &bytes[..end]);
        },
        PendingChange::Truncate { .. } => apply_change(image, change),
    }
}
