//! Who flushes a segment, who waits, and what the answer covers

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::format::loc::SegmentId;
use crate::sync::tension::Tension;

/// The durability state of one segment, shared by every writer flushing it
///
/// Reached through an Arc so a waiter is not holding the tail, and so a flush that
/// finishes after a roll still speaks for the segment it flushed.
pub(super) struct SyncState {
    /// Bytes settled the last time a flush finished, read without taking a lock
    pub(super) synced_at: AtomicU64,

    /// Who is at the device, whether the segment is past saving, and who is waiting
    pub(super) flush: Tension<Flush>,

    /// Page-return pacing, taken only by the writer handing a chunk back
    pub(super) pacing: Mutex<Pacing>,
}

impl SyncState {
    pub(super) fn new() -> SyncState {
        SyncState {
            synced_at: AtomicU64::new(0),
            flush: Tension::new(Flush {
                is_running: false,
                is_broken: false,
                is_retired: false,
            }),
            pacing: Mutex::new(Pacing { started_to: 0 }),
        }
    }

    /// Report the whole segment durable, which a completed seal makes it
    ///
    /// The watermark has to be stored inside the gate and before the wake, or the waiter
    /// it was meant for can miss it and wait on a segment that is already durable.
    pub(super) fn mark_durable(&self) {
        self.flush
            .slack_with(|_| self.synced_at.store(u64::MAX, Ordering::Release));
    }

    /// Report the segment past saving, so waiters hear it instead of waiting
    ///
    /// A segment already reported durable stays that way: a seal answered for every
    /// record it holds, and a later roll off a closed tail does not unsay that.
    pub(super) fn mark_broken(&self) {
        self.flush.slack_with(|flush| {
            if self.synced_at.load(Ordering::Acquire) == u64::MAX {
                return;
            }
            flush.is_broken = true;
        });
    }

    /// Byte position below which this segment's records reached the device
    pub(super) fn covered(&self) -> u64 {
        self.synced_at.load(Ordering::Acquire)
    }
}

/// What a writer that wants its bytes durable has to do about it
pub(super) enum Turn {
    /// Another writer's flush already covers the target
    Settled,

    /// The segment can no longer be made durable at all
    Broken,

    /// Nobody was at the device, so this writer has taken the turn
    Owed,
}

/// The bytes of one segment a caller is waiting to have on the device
///
/// Drawn while the tail is held, so a flush that runs after a roll answers for the
/// segment it named rather than for whichever one replaced it.
#[derive(Clone)]
pub(super) struct Owed {
    /// Durability state of that segment, shared with every writer flushing it
    pub(super) sync: Arc<SyncState>,

    /// The segment the bytes are in
    pub(super) segment: SegmentId,

    /// Byte position the flush has to cover
    pub(super) target: u64,
}

/// What a writer finds when it looks at the segment, taking the turn if it is free
///
/// Nothing comes back while another writer's flush is out, which is the wait: one flush
/// answers for every writer whose bytes are already under it. Nothing comes back on a
/// segment the tail has left either, since the seal behind the roll answers for it.
pub(super) fn turn_at(sync: &SyncState, flush: &mut Flush, target: u64) -> Option<Turn> {
    if sync.synced_at.load(Ordering::Acquire) >= target {
        return Some(Turn::Settled);
    }
    if flush.is_broken {
        return Some(Turn::Broken);
    }
    if flush.is_retired {
        return None;
    }
    if !flush.is_running {
        flush.is_running = true;
        return Some(Turn::Owed);
    }
    None
}

/// A turn at the device, held by the writer that owes the flush
///
/// Dropping it untaken gives the turn up, so nobody waits on a flush that is never
/// coming.
pub struct FlushTurn {
    pub(super) owed: Owed,
    pub(super) is_taken: bool,
}

impl Drop for FlushTurn {
    fn drop(&mut self) {
        if self.is_taken {
            return;
        }
        self.owed
            .sync
            .flush
            .slack_with(|flush| flush.is_running = false);
    }
}

/// What the awaitable flush wait resolved to
pub enum Durability {
    /// Nothing was owed, or another writer's flush covered it
    Settled,

    /// Nobody was at the device, so this caller holds the turn
    Owed(FlushTurn),
}

/// What writers flushing one segment tell each other
pub(super) struct Flush {
    /// Whether a flush of this segment is out at the device right now
    pub(super) is_running: bool,

    /// Whether the segment can no longer be made durable at all
    pub(super) is_broken: bool,

    /// Whether the tail has left this segment, so the writers behind wait for the seal
    pub(super) is_retired: bool,
}

/// How far the device has been started on a segment
pub(super) struct Pacing {
    /// Bytes below which writeback has been handed to the device
    pub(super) started_to: u64,
}
