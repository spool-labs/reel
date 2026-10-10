//! Who flushes a segment, who waits, and what the answer covers

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::format::loc::SegmentId;
use crate::sync::tension::Tension;

/// The durability state of one segment, shared by every writer flushing it
pub(super) struct SyncState {
    /// Bytes settled the last time a flush finished, read without taking a lock
    pub(super) synced_at: AtomicU64,

    /// Bytes written between the last two flushes, a hint of how this volume writes
    pub(super) last_span: AtomicU64,

    /// Who is at the device, whether the segment is past saving, and who is waiting
    pub(super) flush: Tension<Flush>,

    /// Writeback pacing, taken only by the writer starting the next stretch
    pub(super) pacing: Mutex<Pacing>,
}

impl SyncState {
    pub(super) fn new() -> SyncState {
        SyncState {
            synced_at: AtomicU64::new(0),
            last_span: AtomicU64::new(0),
            flush: Tension::new(Flush {
                is_running: false,
                is_broken: false,
                is_retired: false,
            }),
            pacing: Mutex::new(Pacing { started_to: 0 }),
        }
    }

    /// Report the whole segment durable, which a completed seal makes it
    pub(super) fn mark_durable(&self) {
        self.flush
            .slack_with(|_| self.synced_at.store(u64::MAX, Ordering::Release));
    }

    /// Report the segment past saving so waiters stop, unless it was already durable
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
#[derive(Clone)]
pub(super) struct Owed {
    /// Durability state of that segment, shared with every writer flushing it
    pub(super) sync: Arc<SyncState>,

    /// The segment the bytes are in
    pub(super) segment: SegmentId,

    /// Byte position the flush has to cover
    pub(super) target: u64,
}

/// What a writer finds at the segment, taking the turn if free, or nothing while it must wait
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

/// A turn at the device for the writer that owes the flush, given up if dropped untaken
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
