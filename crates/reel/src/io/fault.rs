//! The fault plan the deterministic simulator replays, keyed to global op positions

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
    /// The fault fires at this global op position
    pub at_op: u64,
    /// The fault to inject
    pub kind: FaultKind,
}

/// A seeded schedule of the faults the simulator injects
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FaultPlan {
    /// The seed that reproduces this plan
    pub seed: u64,
    /// Global op position to crash before, if any
    pub crash_at: Option<u64>,
    /// Faults scheduled against op positions
    pub faults: Vec<ScheduledFault>,
    /// Whether drained completions come back in reverse of their submit order
    pub reorder_completions: bool,
    /// Sector size for scattering unsynced bytes on a crash, if set
    pub scatter_bytes: Option<u32>,
}

impl FaultPlan {
    /// Build a fault-free plan from a seed
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

    /// Let a crash scatter unsynced sectors of this size
    pub fn with_scatter(mut self, sector_bytes: u32) -> Self {
        self.scatter_bytes = Some(sector_bytes.max(1));
        self
    }

    /// The fault scheduled at a global op position, if any
    pub fn fault_at(&self, position: u64) -> Option<FaultKind> {
        self.faults
            .iter()
            .find(|fault| fault.at_op == position)
            .map(|fault| fault.kind)
    }
}
