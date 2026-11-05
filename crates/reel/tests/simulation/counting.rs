//! Counts what engine code allocates on this thread, with the simulator and the test itself left out

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use reel_sim::count_pause::is_paused;

thread_local! {
    static IS_COUNTING: Cell<bool> = const { Cell::new(false) };
    static COUNT: Cell<u64> = const { Cell::new(0) };
    static FREES: Cell<u64> = const { Cell::new(0) };
}

/// Counts allocations and frees inside `counted`
struct CountingAllocator;

#[global_allocator]
static COUNTER: CountingAllocator = CountingAllocator;

fn note() {
    if IS_COUNTING.with(Cell::get) && !is_paused() {
        COUNT.with(|count| count.set(count.get() + 1));
    }
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if IS_COUNTING.with(Cell::get) && !is_paused() {
            FREES.with(|frees| frees.set(frees.get() + 1));
        }
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        note();
        unsafe { System.realloc(pointer, layout, size) }
    }
}

// run some engine work and count what it allocated and what it freed on this thread
pub fn counted<Answer>(work: impl FnOnce() -> Answer) -> (Answer, u64, u64) {
    COUNT.with(|count| count.set(0));
    FREES.with(|frees| frees.set(0));
    IS_COUNTING.with(|is_counting| is_counting.set(true));
    let answer = work();
    IS_COUNTING.with(|is_counting| is_counting.set(false));
    (answer, COUNT.with(Cell::get), FREES.with(Cell::get))
}
