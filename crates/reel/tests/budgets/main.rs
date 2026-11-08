//! Open fits inside each budget, and memory_report agrees with the heap the allocator sees. These opens run in
//! a process of their own: they leave the allocator holding freed pages, which would hide unwritten pools from
//! the steady harness and make the kernel take pages back inside its window
#![allow(clippy::expect_used, clippy::print_stdout)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};

use reel::engine::engine_sizing::{ENTRY_LINE_DIVISOR, SEAL_JOB_BYTES, WALK_JOB_BYTES, floor_entries};
use reel::engine::reel_store::{ReelStore, ring_fits, store_shape, volume_disks};
use reel::segment::segment_table::TABLE_SHARDS;
use reel::io::plain::PlainIo;
use reel_api::maintenance_report::MaintenanceReport;
use reel_api::memory_report::MemoryReport;
use reel_api::store_error::StoreError;
use reel_api::units::ByteCount;

#[path = "../steady/store_config.rs"]
mod store_config;

use store_config::{config, scratch_root};

const MIB: u64 = 1024 * 1024;
const BUDGETS: [u64; 4] = [128, 192, 512, 4_096];
const WORKER_COUNTS: [usize; 3] = [0, 1, 4];
// the spec lets memory_report miss the process heap by this share, as a divisor, until maintenance memory has its own report
const REPORT_SLACK_DIVISOR: u64 = 10;
/// The budget a store opens with when its caller gives none
const DEFAULT_CELL_MIB: u64 = 512;
/// The thinnest budget the harness opens, where the ring may not fit
const LEAST_CELL_MIB: u64 = 128;

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
        println!("open {} us, of which the pools {} us", report.open_time.0 / 1_000, report.prefault_time.0 / 1_000);
        if report.prefault_time.0 == 0 || report.prefault_time > report.open_time {
            println!("the pools' time at open is missing or past the open's");
            failures += 1;
        }
        failures += check_entry_line(&store, &report, budget);
        failures += check_job_plans(&report, config.volumes.len() as u64);
        store.close().expect("close");
        drop(store);
        fs::remove_dir_all(&root).expect("remove");
    }
    failures += check_least_cell();
    failures += check_default_cell();
    if failures > 0 {
        process::exit(1);
    }
}

// the roots sit on a real disk, so the plan's entries line comes from it, inside its cap, the tables hold every entry it pays for
// to within a shard's rounding, and the cover is no larger than the disk
fn check_entry_line(store: &ReelStore<PlainIo>, report: &MemoryReport, budget: ByteCount) -> u32 {
    let mut volumes = MaintenanceReport::new();
    store.maintenance_report(&mut volumes);
    let held: u64 = volumes.volumes.iter().map(|volume| (volume.entry_room.0 + TABLE_SHARDS as u64) * volume.entry_bytes.0).sum();
    let segment = volumes.volumes.iter().map(|volume| volume.cover_segment_bytes.0).max().unwrap_or(0);
    println!("entries line {} B covers {} B of a {} B disk at {segment} B a segment", report.entry_line.0, report.disk_covered.0, report.disk_bytes.0);
    let floor: u64 = volumes.volumes.iter().map(|volume| floor_entries() as u64 * volume.entry_bytes.0).sum();
    let cap = floor + budget.0.saturating_sub(report.least_budget.0) / ENTRY_LINE_DIVISOR;
    let is_short = report.disk_bytes.0 == 0 || held < report.entry_line.0 || report.entry_line.0 > cap;
    if is_short || report.disk_covered > report.disk_bytes {
        println!("the entries line is missing, past its cap, past the tables, or covers more than the disk");
        return 1;
    }
    0
}

// the default budget opens with the ring, and the report gives the least budget as a figure
fn check_default_cell() -> u32 {
    let root = scratch_root();
    let budget = ByteCount(DEFAULT_CELL_MIB * MIB);
    let config = config(&root, budget);
    let mut failures = 0;
    match ReelStore::open(PlainIo::new(), &config) {
        Ok(store) => {
            let report = store.memory_report();
            let disks = volume_disks(&PlainIo::new(), &config).expect("disks");
            println!("the default at {DEFAULT_CELL_MIB} MiB: ring fits {}, least {:?}", ring_fits(&config, &disks), report.least_budget);
            failures += u32::from(!ring_fits(&config, &disks) || report.least_budget.0 == 0 || report.least_budget > budget);
            store.close().expect("close");
        },
        Err(error) => {
            println!("the default refused: {error}");
            failures += 1;
        },
    }
    fs::remove_dir_all(&root).expect("remove");
    failures
}

// the seal jobs and the walks hold their buffers at or under the lines the budget plans for them
fn check_job_plans(report: &MemoryReport, volumes: u64) -> u32 {
    let workers = volumes * report.maintenance_workers as u64;
    let footer_plan = volumes * (SEAL_JOB_BYTES + WALK_JOB_BYTES) + workers * WALK_JOB_BYTES;
    println!("jobs: {:?} footer space against {footer_plan} B planned, {:?} compaction", report.footer_space, report.compaction);
    if report.footer_space.0 > footer_plan {
        println!("a seal or a walk holds more than its plan");
        return 1;
    }
    0
}

// the least cell opens on the default and takes the ring exactly when it fits, with the least budget beside the ring and without
fn check_least_cell() -> u32 {
    let root = scratch_root();
    let budget = ByteCount(LEAST_CELL_MIB * MIB);
    let config = config(&root, budget);
    let disks = volume_disks(&PlainIo::new(), &config).expect("disks");
    let (ring, no_ring) = (store_shape(&config, true, &disks).least_budget(), store_shape(&config, false, &disks).least_budget());
    println!("least budget at {LEAST_CELL_MIB} MiB: {ring:?} with the ring, {no_ring:?} without");
    let mut failures = 0;
    match ReelStore::open(PlainIo::new(), &config) {
        Ok(store) => {
            let mut report = MaintenanceReport::new();
            store.maintenance_report(&mut report);
            println!("the default at {LEAST_CELL_MIB} MiB has_ring {}", report.has_ring);
            failures += u32::from(report.has_ring != ring_fits(&config, &disks));
            store.close().expect("close");
        },
        Err(error) => {
            println!("the default refused: {error}");
            failures += 1;
        },
    }
    fs::remove_dir_all(&root).expect("remove");
    failures
}
