//! The store's scans, a slot each on its own cache line, so a thread that takes and gives back its own slot writes no line
//! another thread uses

use std::sync::Mutex;

use crate::engine::caller_activity::thread_slot;
use crate::engine::scan_state::ScanState;
use crate::guard::hold;

/// One scan's buffers, or none while a caller holds them
#[repr(align(128))]
#[derive(Debug)]
pub struct ScanSlot(Mutex<Option<ScanState>>);

/// Every scan the store runs at once
#[derive(Debug)]
pub struct ScanPool {
    slots: Vec<ScanSlot>,
}

impl ScanPool {
    /// Put each scan in a slot of its own
    pub fn new(scans: Vec<ScanState>) -> Self {
        Self { slots: scans.into_iter().map(|scan| ScanSlot(Mutex::new(Some(scan)))).collect() }
    }

    /// Count the scans the store runs at once
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Check the store runs no scans
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Take a scan from the calling thread's own slot, or from any other on a miss
    pub fn take(&self) -> Option<ScanState> {
        let count = self.slots.len();
        let first = thread_slot() % count.max(1);
        for offset in 0..count {
            let index = (first + offset) % count;
            if let Some(mut scan) = hold(&self.slots[index].0).take() {
                scan.pool_slot = index;
                return Some(scan);
            }
        }
        None
    }

    /// Give a scan back to the slot it came from, or to the first empty one
    pub fn give_back(&self, scan: ScanState) {
        let home = scan.pool_slot.min(self.slots.len().saturating_sub(1));
        let mut slot = hold(&self.slots[home].0);
        if slot.is_none() {
            *slot = Some(scan);
            return;
        }
        drop(slot);
        for other in &self.slots {
            let mut slot = hold(&other.0);
            if slot.is_none() {
                *slot = Some(scan);
                return;
            }
        }
        // every scan came from a slot, so one is always empty here
    }
}
