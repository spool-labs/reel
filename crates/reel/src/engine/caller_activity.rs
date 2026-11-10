//! A count of caller operations in a slot for each thread, so maintenance can tell callers are still busy with no counter every
//! read thread shares

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Slots the threads spread over
pub const CALLER_SLOTS: usize = 16;

/// A count on a cache line of its own
#[repr(align(128))]
#[derive(Debug, Default)]
pub struct PaddedCount(pub AtomicU64);

/// The next slot a thread takes
static NEXT_SLOT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static SLOT: Cell<usize> = const { Cell::new(usize::MAX) };
}

/// Read the calling thread's slot, taking the next one the first time
pub fn thread_slot() -> usize {
    SLOT.with(|slot| {
        if slot.get() == usize::MAX {
            slot.set(NEXT_SLOT.fetch_add(1, Ordering::Relaxed) % CALLER_SLOTS);
        }
        slot.get()
    })
}

/// Caller operations, a count in each slot
#[derive(Debug, Default)]
pub struct CallerCounts {
    slots: [PaddedCount; CALLER_SLOTS],
}

impl CallerCounts {
    /// Count one operation in the calling thread's slot
    pub fn note(&self) {
        self.slots[thread_slot()].0.fetch_add(1, Ordering::Relaxed);
    }

    /// Add up every slot
    pub fn total(&self) -> u64 {
        self.slots.iter().map(|slot| slot.0.load(Ordering::Relaxed)).sum()
    }
}
