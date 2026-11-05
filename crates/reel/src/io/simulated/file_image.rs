//! One simulated file: what a reader sees and what a power cut leaves

use std::path::PathBuf;

use reel_api::units::{ByteCount, FileOffset};

use crate::io::simulated::seeded_random::SeededRandom;

/// A torn write keeps or loses each sector of this size on its own
pub const SECTOR_BYTES: u64 = 512;
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
}

/// The bytes of one file
#[derive(Clone, Debug, Default)]
pub struct FileImage {
    pub visible: Vec<u8>,
    pub durable: Vec<u8>,
    pub pending: Vec<PendingChange>,
    pub disk: Option<PathBuf>,
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
            let pick = random.below(POWER_CUT_OUTCOMES.len() as u64) as usize;
            match POWER_CUT_OUTCOMES[pick] {
                PowerCutOutcome::Lost => {},
                PowerCutOutcome::Kept => apply_change(&mut self.durable, &change),
                PowerCutOutcome::Torn => apply_torn(&mut self.durable, &change, random),
            }
        }
        self.visible.clone_from(&self.durable);
    }

    /// Flip bits of one byte in what readers see and in what survives
    pub fn rot(&mut self, offset: FileOffset, mask: BitMask) {
        if let Some(byte) = self.visible.get_mut(offset.0 as usize) {
            *byte ^= mask.0;
        }
        if let Some(byte) = self.durable.get_mut(offset.0 as usize) {
            *byte ^= mask.0;
        }
    }

    /// Check whether the bytes still take space on the disk
    pub fn is_held(&self) -> bool {
        self.is_named || self.handle_count > 0
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

/// Apply any set of a write's sectors, since pages reach a disk in any order
pub fn apply_torn(image: &mut Vec<u8>, change: &PendingChange, random: &mut SeededRandom) {
    match change {
        PendingChange::Write { offset, bytes } => {
            for (index, sector) in bytes.chunks(SECTOR_BYTES as usize).enumerate() {
                if random.one_in(SECTOR_SURVIVAL_ODDS) {
                    let start = offset.0 + index as u64 * SECTOR_BYTES;
                    apply_write(image, FileOffset(start), sector);
                }
            }
        },
        PendingChange::Truncate { .. } => apply_change(image, change),
    }
}
