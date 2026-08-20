//! What a checkpoint stood at and what it linked to get there
//!
//! Taking one seals every tail to draw the line it copies at, which is a write
//! and needs the ownership lock, so it stays an engine method. Only saying what
//! came back belongs here.

use std::path::Path;

use crate::reel::checkpoint::Checkpoint;
use crate::report::doc::{Doc, Tone};
use crate::report::fmt;
use crate::report::render::Report;

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

impl Report for CheckpointReport {
    fn doc(&self) -> Doc {
        Doc::new()
            .head(fmt::volume_name(&self.target))
            .head("checkpoint")
            .head(format!("seq {}", self.at))
            .verdict(
                Tone::Good,
                "TAKEN",
                format!(
                    "{} at sequence {}",
                    fmt::plural(self.segments as u64, "segment linked", "segments linked"),
                    self.at,
                ),
            )
            .facts([("target".to_string(), self.target.clone())])
            .line("The copy is hard links, so it costs metadata rather than bytes and shares")
            .line("them with the volume until compaction moves on.")
            .footer([format!("restoring is opening it: reel {} cue", self.target)])
    }
}
