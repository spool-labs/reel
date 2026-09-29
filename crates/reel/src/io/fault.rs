//! Fault plan the deterministic simulator replays
//!
//! A plan pins a seed, an optional crash boundary, and a list of faults keyed to
//! global op positions. The simulator is a pure function of an op stream and a
//! plan, so the same plan reproduces the same on-disk image.

/// One fault the simulator injects when a scheduled op executes or completes
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultKind {
    /// An append persists only a prefix of its payload and reports a short write
    ShortWrite { written_bytes: u64 },
    /// An append caches every byte and reports success, but only a prefix persists
    TornWrite { durable_bytes: u64 },
    /// A file sync returns success without persisting cached bytes
    LyingSync,
    /// A range sync returns success without persisting cached bytes
    LyingSyncRange,
    /// An append fails as though the volume is full
    EnospcAppend,
    /// A space reservation fails as though the volume is full
    EnospcAllocate,
    /// A sync fails with an input output error
    SyncError,
    /// A directory listing fails with an input output error
    ListError,
    /// A read fails with an input output error
    ReadError,
    /// A truncate fails with an input output error
    TruncateError,
    /// Directory renames and unlinks in the batch apply in reverse order
    ReorderDir,
    /// A stored byte has one bit flipped after it is written
    BitFlip { at_byte: u64, bit: u8 },
    /// The op executes and mutates state but its completion is never queued
    DropCompletion,
    /// The op executes but its completion is withheld for a number of polls
    DelayCompletion { polls: u32 },
}

/// A fault scheduled against a global op position
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduledFault {
    /// Global op position this fault triggers at
    pub at_op: u64,
    /// Fault injected at that position
    pub kind: FaultKind,
}

/// Seeded description of the faults the simulator injects
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FaultPlan {
    /// Seed identifying this plan for reproduction
    pub seed: u64,
    /// Global op position to crash before, if any
    pub crash_at: Option<u64>,
    /// Faults scheduled against op positions
    pub faults: Vec<ScheduledFault>,
    /// Whether drained completions come back in reverse of their submit order
    pub reorder_completions: bool,
    /// Sector size a crash scatters unsynced bytes at, leaving holes not a prefix
    pub scatter_bytes: Option<u32>,
}

impl FaultPlan {
    /// Build a fault free plan from a seed
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            crash_at: None,
            faults: Vec::new(),
            reorder_completions: false,
            scatter_bytes: None,
        }
    }

    /// Schedule a fault at a global op position
    pub fn with_fault(mut self, at_op: u64, kind: FaultKind) -> Self {
        self.faults.push(ScheduledFault { at_op, kind });
        self
    }

    /// Crash before executing the op at a global position
    pub fn with_crash(mut self, at_op: u64) -> Self {
        self.crash_at = Some(at_op);
        self
    }

    /// Drain completions in reverse of their submit order
    pub fn with_reorder(mut self) -> Self {
        self.reorder_completions = true;
        self
    }

    /// Let a crash leave unsynced sectors scattered rather than truncated
    pub fn with_scatter(mut self, sector_bytes: u32) -> Self {
        self.scatter_bytes = Some(sector_bytes.max(1));
        self
    }

    /// Fault scheduled at a global op position, if any
    pub fn fault_at(&self, position: u64) -> Option<FaultKind> {
        self.faults
            .iter()
            .find(|fault| fault.at_op == position)
            .map(|fault| fault.kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // a new plan carries no faults, no crash, and no reordering
    #[test]
    fn empty_plan() {
        let plan = FaultPlan::new(9);

        assert_eq!(plan.seed, 9);
        assert_eq!(plan.crash_at, None);
        assert!(plan.faults.is_empty());
        assert!(!plan.reorder_completions);
    }

    // the builders assemble crash, fault, and reorder settings
    #[test]
    fn builders_compose() {
        let plan = FaultPlan::new(3)
            .with_crash(5)
            .with_fault(2, FaultKind::LyingSync)
            .with_reorder();

        assert_eq!(plan.crash_at, Some(5));
        assert_eq!(plan.fault_at(2), Some(FaultKind::LyingSync));
        assert_eq!(plan.fault_at(4), None);
        assert!(plan.reorder_completions);
    }
}
