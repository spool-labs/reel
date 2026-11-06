//! Open fits inside each budget, and memory_report agrees with the heap the allocator sees. These opens run in
//! a process of their own: they leave the allocator holding freed pages, which would hide unwritten pools from
//! the steady harness and make the kernel take pages back inside its window
#![allow(clippy::expect_used, clippy::print_stdout)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};

use reel::engine::reel_store::ReelStore;
use reel::io::plain::PlainIo;
use reel_api::store_error::StoreError;
use reel_api::units::ByteCount;

#[path = "../steady/store_config.rs"]
mod store_config;

use store_config::{config, scratch_root};

const MIB: u64 = 1024 * 1024;
const BUDGETS: [u64; 4] = [128, 192, 512, 4_096];
const WORKER_COUNTS: [usize; 3] = [0, 1, 4];
// the spec lets memory_report miss the process heap by this share, as a divisor
const REPORT_SLACK_DIVISOR: u64 = 10;

static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);

/// Counts the heap every thread holds
struct LiveBytes;

#[global_allocator]
static COUNTER: LiveBytes = LiveBytes;

unsafe impl GlobalAlloc for LiveBytes {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        LIVE_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        LIVE_BYTES.fetch_add(size as u64, Ordering::Relaxed);
        LIVE_BYTES.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.realloc(pointer, layout, size) }
    }
}

fn main() {
    let mut failures = 0;
    for (budget, workers) in BUDGETS.into_iter().flat_map(|budget| WORKER_COUNTS.map(|workers| (budget, workers))) {
        let root = scratch_root();
        let live_before = LIVE_BYTES.load(Ordering::Acquire);
        let budget = ByteCount(budget * MIB);
        let mut config = config(&root, budget);
        config.threads.maintenance_workers = Some(workers);
        let store = match ReelStore::open(PlainIo::new(), &config) {
            Ok(store) => store,
            // a count this budget can't hold is the refusal the store promises
            Err(StoreError::ThreadsPastBudget { .. }) => {
                println!("budget {} MiB refuses {workers} workers", budget.0 / MIB);
                fs::remove_dir_all(&root).expect("remove");
                continue;
            },
            Err(error) => {
                println!("open failed: {error}");
                process::exit(1);
            },
        };
        let live = LIVE_BYTES.load(Ordering::Acquire) - live_before;
        let report = store.memory_report();
        let held = report.heap_total().0 + report.thread_stacks.0;
        let shard = &store.state.sizing.shard;
        println!(
            "budget {} MiB, {workers} workers: {} B held, {live} B live, {:?} rows a segment, {:?} a segment, {} pages {} nodes {} slots a shard",
            budget.0 / MIB,
            held,
            store.state.sizing.segment_rows,
            store.state.sizing.segment_bytes,
            shard.pages.0,
            shard.nodes.0,
            shard.table_slots,
        );
        println!("{report:?}");
        if held > report.budget.0 || live.abs_diff(report.heap_total().0) > report.heap_total().0 / REPORT_SLACK_DIVISOR {
            println!("budget {} MiB fails", budget.0 / MIB);
            failures += 1;
        }
        store.close().expect("close");
        drop(store);
        fs::remove_dir_all(&root).expect("remove");
    }
    if failures > 0 {
        process::exit(1);
    }
}
