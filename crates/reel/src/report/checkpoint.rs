//! What a checkpoint stood at and what it linked to get there
//!
//! Taking one seals every tail to draw the line it copies at, which is a write
//! and needs the ownership lock, so it stays an engine method. Only saying what
//! came back belongs here.

use std::path::Path;

use crate::reel::checkpoint::Checkpoint;

/// A checkpoint that was taken, and where it landed
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CheckpointReport {
    /// The sequence number every version in the copy is at or below
    pub at: u64,

    /// Segment files linked into the target, which is the size of the work
    pub segments: usize,

    /// The directory the copy was published under
    pub target: String,
}

/// Report a checkpoint the engine has already taken
pub fn checkpoint(taken: &Checkpoint, target: &Path) -> CheckpointReport {
    CheckpointReport {
        at: taken.at.as_u64(),
        segments: taken.segments,
        target: target.display().to_string(),
    }
}
