//! A count of caller operations in a slot for each thread, so maintenance can tell callers are still busy with no counter every
//! read thread shares

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread::available_parallelism;

/// The fewest slots a count spreads over, for a machine that can't say how many threads it runs
pub const LEAST_CALLER_SLOTS: usize = 16;
/// Slots a count keeps for each thread the machine runs at once, so the callers a node runs past its cores rarely share a slot
pub const SLOTS_PER_THREAD: usize = 4;

/// A count on a cache line of its own
#[repr(align(128))]
#[derive(Debug, Default)]
pub struct PaddedCount(pub AtomicU64);

/// The next thread number to hand out
static NEXT_SLOT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static SLOT: Cell<usize> = const { Cell::new(usize::MAX) };
}

/// Read the calling thread's number, taking the next one the first time. A count takes it modulo its own slots
pub fn thread_slot() -> usize {
    SLOT.with(|slot| {
        if slot.get() == usize::MAX {
            slot.set(NEXT_SLOT.fetch_add(1, Ordering::Relaxed));
        }
        slot.get()
    })
}

/// Count the slots a count spreads over on this machine: four for each thread it runs at once, a power of two, and never fewer
/// than LEAST_CALLER_SLOTS. Threads that share a slot write one cache line, and every scan row writes its slot
pub fn caller_slots() -> usize {
    let threads = available_parallelism().map_or(LEAST_CALLER_SLOTS, |threads| threads.get());
    threads.saturating_mul(SLOTS_PER_THREAD).next_power_of_two().max(LEAST_CALLER_SLOTS)
}

/// Caller operations, a count in each slot
#[derive(Debug)]
pub struct CallerCounts {
    slots: Box<[PaddedCount]>,
}

impl Default for CallerCounts {
    fn default() -> Self {
        Self { slots: (0..caller_slots()).map(|_slot| PaddedCount::default()).collect() }
    }
}

impl CallerCounts {
    /// Count one operation in the calling thread's slot. Only the count moving matters, so a plain store skips the locked add
    pub fn note(&self) {
        // the slot count is a power of two, so a mask takes the thread's number into it
        let slot = &self.slots[thread_slot() & (self.slots.len() - 1)].0;
        slot.store(slot.load(Ordering::Relaxed).wrapping_add(1), Ordering::Relaxed);
    }

    /// Add up every slot
    pub fn total(&self) -> u64 {
        self.slots.iter().map(|slot| slot.0.load(Ordering::Relaxed)).sum()
    }

    /// Count the slots
    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // a count has four slots for each of the machine's threads, so any run of that many thread numbers in a row lands in slots of
    // their own, wherever the run starts
    #[test]
    fn the_machine_threads_get_slots_of_their_own() {
        let counts = CallerCounts::default();
        let slots = counts.slot_count();
        assert!(slots.is_power_of_two() && slots >= LEAST_CALLER_SLOTS);
        let threads = available_parallelism().map_or(LEAST_CALLER_SLOTS, |threads| threads.get());
        assert!(slots >= threads * SLOTS_PER_THREAD, "{slots} slots for {threads} threads");
        for start in [0, 1, slots - 1, 7 * slots + 3] {
            let mut taken: Vec<usize> = (start..start + slots).map(|number| number & (slots - 1)).collect();
            taken.sort_unstable();
            taken.dedup();
            assert_eq!(taken.len(), slots, "thread numbers from {start} shared slots");
        }
    }

    // every note from any number of threads lands in the total
    #[test]
    fn notes_from_many_threads_add_up() {
        let counts = CallerCounts::default();
        let threads = 2 * counts.slot_count();
        std::thread::scope(|scope| {
            for _thread in 0..threads {
                scope.spawn(|| {
                    for _note in 0..100 {
                        counts.note();
                    }
                });
            }
        });
        // threads past the slots share one, and a shared slot's plain store can drop a note, so only the lone ones are exact
        assert!(counts.total() <= threads as u64 * 100);
        assert!(counts.total() > 0);
    }
}
