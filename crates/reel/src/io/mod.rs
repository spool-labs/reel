//! Ring-shaped file I/O trait and its backends
//!
//! Ops move into the ring by value on submit and each yields one tagged
//! completion drained on poll, so a completion backend can stand in for a
//! synchronous one without a call site changing shape.

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
    /// Completions this thread's inline batches come back through
    ///
    /// A detached submit the backend answers on the spot has nowhere of the
    /// caller's to file into, so the list it files through is this thread's.
    static FILED: std::cell::Cell<Vec<Completion>> = const { std::cell::Cell::new(Vec::new()) };
}

/// Which door a backend's ops took, asked of a leg that has to know
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DoorCounts {
    /// Whether any op on this backend reached a ring
    pub reached_ring: bool,

    /// Ops handed to another backend instead of going on a ring
    pub off_ring: u64,

    /// Whether the kernel refused a thread's buffer pool
    pub pool_refused: bool,

    /// Whether the kernel refused a ring's sparse file table
    pub files_refused: bool,
}

/// Which backend actually serves a volume, as opposed to the one it asked for
///
/// A configured ring downgrades to posix when the kernel will not set one up, so
/// the request and the outcome are two different facts. Each backend answers
/// from its own state rather than from the request that built it, which is what
/// keeps the answer from drifting into a restatement of the config.
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

    /// The name this backend reports itself under
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

/// Ring-shaped file I/O every reel backend implements
pub trait ReelIo: Send + Sync {
    /// Which backend is serving, answered from what this one is rather than
    /// from what was asked for
    ///
    /// Required rather than defaulted: a backend that forgot to answer would
    /// report someone else's identity, which is the failure this exists to stop.
    fn serving(&self) -> ServingBackend;

    /// Move owned ops into the ring, each yielding one tagged completion later
    ///
    /// Only a poll on the submitting thread can take those completions; pairing
    /// a submit on one thread with a poll on another strands them.
    fn submit(&self, ops: Vec<Op>) -> Result<()>;

    /// Drain ready completions into the output vector, returning how many moved
    ///
    /// Takes only what this thread submitted.
    fn poll(&self, out: &mut Vec<Completion>) -> Result<usize>;

    /// Service one op in place, handing its completion straight back
    ///
    /// A backend that runs the op on the calling thread answers here and the
    /// driver skips the tag inbox. One that completes out of band hands the op
    /// back.
    fn submit_inline(&self, op: Op) -> std::result::Result<Completion, Op> {
        Err(op)
    }

    /// Service a whole batch in place, filling its completions in submit order
    ///
    /// The same bargain as submit_inline, and a batch is where the inbox costs
    /// most: two locks and a tag lookup per op the caller already has in order.
    /// False leaves the ops untouched for the caller to send another way, and the
    /// op list stays the caller's either way, so a batching thread keeps one.
    fn submit_batch(&self, _ops: &mut Vec<Op>, _out: &mut Vec<Completion>) -> bool {
        false
    }

    /// Submit ops for a caller that has no thread to come back and reap them
    ///
    /// The caller may be polled on a different worker every time, so completions
    /// go into the slot table where the future left its waker.
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

    /// Submit one op for a caller with no thread to reap it, without a list to hold it
    ///
    /// The awaited door sends one op at a time, and a channel that takes a batch
    /// made every one of them buy a vector to travel in.
    fn submit_detached_one(&self, op: Op, sink: &Arc<SlotTable>) -> Result<()> {
        match self.submit_inline(op) {
            Ok(completion) => {
                sink.file_one(completion);
                Ok(())
            }
            // Nothing left but the batch door, for a backend that answers at
            // neither of the two above.
            Err(op) => self.submit(vec![op]),
        }
    }

    /// Answer one framed record from resident pages, without blocking or queueing
    ///
    /// True means both buffers hold every byte they asked for; false leaves them
    /// exactly as they arrived and the read goes down as an op.
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
    ///
    /// A ring backend hands some ops to the posix backend it holds without
    /// saying so, and a leg that cannot tell cannot claim it measured the ring.
    fn door_counts(&self) -> DoorCounts {
        DoorCounts::default()
    }

    /// Device flushes asked for so far, the count durability is billed in
    ///
    /// A backend that cannot count them answers zero.
    fn sync_count(&self) -> u64 {
        0
    }

    /// Nanoseconds spent waiting inside those flushes
    fn sync_nanos(&self) -> u64 {
        0
    }

    /// Wait in the kernel until at least one completion is ready, then drain
    ///
    /// A backend that cannot park drains whatever is ready and the driver spins.
    fn poll_blocking(&self, out: &mut Vec<Completion>) -> Result<usize> {
        self.poll(out)
    }

    /// Whether waiting on this backend parks rather than spins
    fn parks_on_wait(&self) -> bool {
        false
    }
}
