//! File I/O trait and its backends
//! Ops move in by value on submit and each yields one tagged completion on poll

pub mod direct;
#[cfg(feature = "sim")]
pub mod fault;
pub mod mapping;
pub mod op;
pub mod posix_backend;
pub mod select;
#[cfg(feature = "sim")]
pub mod sim_backend;
pub mod slots;
#[cfg(target_os = "linux")]
pub mod uring_backend;

use std::sync::Arc;

use crate::error::Result;
use crate::io::op::{Completion, FileId, Op, ReadBuf};
use crate::io::slots::SlotTable;

thread_local! {
    /// This thread's spare completion list for detached submits answered inline
    static FILED: std::cell::Cell<Vec<Completion>> = const { std::cell::Cell::new(Vec::new()) };
}

/// Which door a backend's ops took
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DoorCounts {
    /// Whether any op on this backend reached a ring
    pub reached_ring: bool,

    /// Ops handed to another backend, which never went on a ring
    pub off_ring: u64,

    /// Whether the kernel refused a thread's buffer pool
    pub pool_refused: bool,

    /// Whether the kernel refused a ring's sparse file table
    pub files_refused: bool,
}

/// The backend that actually serves a volume, which may differ from the configured one
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServingBackend {
    /// Synchronous posix ops through the page cache
    Posix,

    /// Synchronous posix ops on descriptors that bypass the page cache
    PosixDirect,

    /// A ring submitting through the page cache
    Ring,

    /// A ring submitting on descriptors that bypass the page cache
    RingDirect,

    /// The in-memory backend a simulation runs on, never chosen from config
    Sim,
}

impl ServingBackend {
    /// Whether a ring serves this volume
    pub fn is_ring(self) -> bool {
        matches!(self, ServingBackend::Ring | ServingBackend::RingDirect)
    }

    /// Whether this volume's descriptors bypass the page cache
    pub fn is_direct(self) -> bool {
        matches!(
            self,
            ServingBackend::PosixDirect | ServingBackend::RingDirect
        )
    }

    /// This backend as a short lowercase string
    pub fn as_str(self) -> &'static str {
        match self {
            ServingBackend::Posix => "posix",
            ServingBackend::PosixDirect => "posix_direct",
            ServingBackend::Ring => "ring",
            ServingBackend::RingDirect => "ring_direct",
            ServingBackend::Sim => "sim",
        }
    }
}

impl std::fmt::Display for ServingBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The file I/O interface every reel backend implements
pub trait ReelIo: Send + Sync {
    /// Which backend is serving, from this backend's own state
    fn serving(&self) -> ServingBackend;

    /// Move owned ops into the ring, and only a poll on this thread takes their completions
    fn submit(&self, ops: Vec<Op>) -> Result<()>;

    /// Drain this thread's ready completions into the vector, returning how many moved
    fn poll(&self, out: &mut Vec<Completion>) -> Result<usize>;

    /// Run one op on the calling thread and return its completion, or hand the op back
    fn submit_inline(&self, op: Op) -> std::result::Result<Completion, Op> {
        Err(op)
    }

    /// Run a batch in place with completions in submit order, false leaves the ops untouched
    fn submit_batch(&self, _ops: &mut Vec<Op>, _out: &mut Vec<Completion>) -> bool {
        false
    }

    /// Submit ops with completions filed to the slot table, for callers with no reaping thread
    fn submit_detached(&self, mut ops: Vec<Op>, sink: &Arc<SlotTable>) -> Result<()> {
        let mut filed = FILED.with(std::cell::Cell::take);
        let served = self.submit_batch(&mut ops, &mut filed);
        if served {
            sink.file(&mut filed);
        }
        filed.clear();
        FILED.with(|spare| spare.set(filed));
        match served {
            true => Ok(()),
            false => self.submit(ops),
        }
    }

    /// Submit one detached op without allocating a list for it
    fn submit_detached_one(&self, op: Op, sink: &Arc<SlotTable>) -> Result<()> {
        match self.submit_inline(op) {
            Ok(completion) => {
                sink.file_one(completion);
                Ok(())
            }
            // The backend cannot answer inline, so submit a batch of one.
            Err(op) => self.submit(vec![op]),
        }
    }

    /// Fill both buffers from resident pages without blocking, false leaves them untouched
    fn warm_split(
        &self,
        _file: FileId,
        _offset: u64,
        _head: &mut ReadBuf,
        _body: &mut ReadBuf,
    ) -> bool {
        false
    }

    /// Which door this backend's ops actually took
    fn door_counts(&self) -> DoorCounts {
        DoorCounts::default()
    }

    /// Device flushes asked for so far, zero when the backend cannot count them
    fn sync_count(&self) -> u64 {
        0
    }

    /// Nanoseconds spent waiting inside those flushes
    fn sync_nanos(&self) -> u64 {
        0
    }

    /// Wait in the kernel for at least one completion, then drain
    fn poll_blocking(&self, out: &mut Vec<Completion>) -> Result<usize> {
        self.poll(out)
    }

    /// Whether waiting on this backend parks the thread
    fn parks_on_wait(&self) -> bool {
        false
    }
}
