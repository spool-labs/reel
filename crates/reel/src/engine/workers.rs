//! The store's maintenance workers, which every volume shares, and the mask of sleeping ones a hand-out wakes

use std::io::Result;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::io::IoTask;

/// Workers one word of the idle mask holds
pub const MASK_BITS: usize = 64;

/// One worker's task and whether it holds work now
#[derive(Debug, Default)]
pub struct WorkerCell<Task> {
    /// The worker's task, set once it has started
    pub task: OnceLock<Task>,
    /// Whether the worker holds a block or a rewrite now
    pub is_active: AtomicBool,
}

/// The store's workers, and a bit each that is set while the worker sleeps
#[derive(Debug, Default)]
pub struct Workers<Task> {
    /// Each worker, by its place
    pub cells: Vec<WorkerCell<Task>>,
    /// The idle bits, in as many words as the count needs
    pub idle: Vec<AtomicU64>,
}

impl<Task: IoTask> Workers<Task> {
    /// Build the cells and the mask for a count of workers, none of them idle yet
    pub fn new(count: usize) -> Self {
        Self {
            cells: (0..count).map(|_cell| WorkerCell { task: OnceLock::new(), is_active: AtomicBool::new(false) }).collect(),
            idle: (0..count.div_ceil(MASK_BITS)).map(|_word| AtomicU64::new(0)).collect(),
        }
    }

    /// Count the workers
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    /// Check the store runs no workers
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// Look for work, and when there is none, mark the worker idle and look once more. A hand-out stores its work and then reads
    /// the mask, and the worker sets its bit and then looks, all in SeqCst, so a hand-out the second look misses finds the bit
    /// and wakes the worker, whose wake waits for it if it hasn't gone to sleep yet
    pub fn look_or_idle(&self, index: usize, mut look: impl FnMut() -> bool) -> bool {
        if look() {
            return true;
        }
        let (word, bit) = (index / MASK_BITS, 1u64 << (index % MASK_BITS));
        self.idle[word].fetch_or(bit, Ordering::SeqCst);
        if look() {
            self.idle[word].fetch_and(!bit, Ordering::SeqCst);
            return true;
        }
        false
    }

    /// Wake one sleeping worker for work just handed out, or none when every worker is awake and will look again
    pub fn wake_one(&self) {
        for (word_index, word) in self.idle.iter().enumerate() {
            let mut bits = word.load(Ordering::SeqCst);
            while bits != 0 {
                let bit = 1u64 << bits.trailing_zeros();
                let before = word.fetch_and(!bit, Ordering::SeqCst);
                if before & bit != 0 {
                    let index = word_index * MASK_BITS + bit.trailing_zeros() as usize;
                    if let Some(task) = self.cells[index].task.get() {
                        task.wake();
                    }
                    return;
                }
                bits = before & !bit;
            }
        }
    }

    /// Check a worker is marked idle
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn is_idle(&self, index: usize) -> bool {
        self.idle[index / MASK_BITS].load(Ordering::SeqCst) >> (index % MASK_BITS) & 1 == 1
    }

    /// Count the workers holding a block or a rewrite now
    pub fn active(&self) -> u64 {
        self.cells.iter().filter(|cell| cell.is_active.load(Ordering::Acquire)).count() as u64
    }

    /// Stop every worker that started, each after its current step
    pub fn stop(&self) -> Result<()> {
        for cell in &self.cells {
            if let Some(task) = cell.task.get() {
                task.stop()?;
            }
        }
        Ok(())
    }
}
