//! What the segment counters cost the paths that touch them
//!
//! Three shapes rather than one, because the table is asked three different things.
//! A read asks for a stamp and whether the segment was born, once per resolved
//! entry. A write books bytes, and every writer books into the same active segment,
//! which is the row false sharing lands on. A maintenance tick walks every row for
//! the footprints and the floors. Counts of segments are the axis, since the table
//! this replaces was hashed and the one that replaces it is indexed.
//!
//! Opt-in. Run with:
//!   cargo test -p reel --release --test probes -- segment_price

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use reel::format::loc::SegmentId;
use reel::format::lsn::Lsn;
use reel::SegmentTable;

/// Asks each thread makes per shape
const ASKS: u64 = 2_000_000;

/// Segment counts the shapes are measured at
const SEGMENTS: [u32; 3] = [1, 64, 1_024];

/// Thread counts the contended shapes are measured at
const THREADS: [u64; 2] = [1, 8];

/// A table already holding this many segments, each with a record booked into it
fn filled(count: u32) -> Arc<SegmentTable> {
    let table = Arc::new(SegmentTable::new());
    for number in 0..count {
        let segment = SegmentId(number);
        table.mark_live(segment, Lsn(u64::from(number) + 1), 4_096);
        table.live_incarnation(segment);
    }
    // One born segment, since an unborn volume answers the born question at one
    // relaxed load and never reaches the structure being priced.
    table.mark_born([SegmentId(0)]);
    table
}

/// Per-op nanoseconds, the threads started together and joined
fn timed<Ask>(table: &Arc<SegmentTable>, threads: u64, ask: Ask) -> f64
where
    Ask: Fn(&SegmentTable, u64) -> u64 + Send + Sync + Copy + 'static,
{
    let start = Arc::new(Barrier::new(threads as usize + 1));
    let sink = Arc::new(AtomicU64::new(0));
    let mut running = Vec::with_capacity(threads as usize);
    for thread in 0..threads {
        let table = Arc::clone(table);
        let start = Arc::clone(&start);
        let sink = Arc::clone(&sink);
        running.push(thread::spawn(move || {
            start.wait();
            let mut kept = 0u64;
            for at in 0..ASKS {
                kept = kept.wrapping_add(ask(&table, thread * ASKS + at));
            }
            sink.fetch_add(kept, Ordering::Relaxed);
        }));
    }
    start.wait();
    let began = Instant::now();
    for one in running {
        one.join().expect("thread");
    }
    let took = began.elapsed();
    // Kept so the loop is not optimised away.
    assert!(sink.load(Ordering::Relaxed) != u64::MAX);
    took.as_nanos() as f64 / (threads * ASKS) as f64
}

// what a resolved entry pays to be stamped and to ask whether its segment was born
pub fn read_side() {
    println!();
    println!("| shape | segments | threads | ns per ask |");
    println!("|---|---|---|---|");
    for count in SEGMENTS {
        let table = filled(count);
        for threads in THREADS {
            let stamp = timed(&table, threads, move |table, at| {
                let segment = SegmentId((at % u64::from(count)) as u32);
                u64::from(table.incarnation_of(segment).0)
            });
            let born = timed(&table, threads, move |table, at| {
                let segment = SegmentId((at % u64::from(count)) as u32);
                u64::from(table.is_born(segment))
            });
            println!("| stamp | {count} | {threads} | {stamp:.1} |");
            println!("| born bit | {count} | {threads} | {born:.1} |");
        }
    }
}

// what a publish pays to book a record, every writer booking into one segment
pub fn write_side() {
    println!();
    println!("| shape | segments | threads | ns per ask |");
    println!("|---|---|---|---|");
    for count in SEGMENTS {
        let table = filled(count);
        for threads in THREADS {
            // The active segment is the newest, and every writer is in it.
            let active = SegmentId(count - 1);
            let one_row = timed(&table, threads, move |table, at| {
                table.mark_live(active, Lsn(at + 1), 4_096);
                0
            });
            let spread = timed(&table, threads, move |table, at| {
                let segment = SegmentId((at % u64::from(count)) as u32);
                table.mark_live(segment, Lsn(at + 1), 4_096);
                0
            });
            println!("| one active row | {count} | {threads} | {one_row:.1} |");
            println!("| spread over rows | {count} | {threads} | {spread:.1} |");
        }
    }
}

// what a maintenance tick pays to walk every row for footprints and floors
pub fn tick_side() {
    println!();
    println!("| shape | segments | ns per pass |");
    println!("|---|---|---|");
    for count in SEGMENTS {
        let table = filled(count);
        let passes = 20_000u64;
        let began = Instant::now();
        let mut kept = 0usize;
        for _ in 0..passes {
            let (rows, floors) = table.ranking();
            kept += rows.len() + usize::from(floors.excluding(SegmentId(0)).is_some());
        }
        let ranking = began.elapsed().as_nanos() as f64 / passes as f64;
        let began = Instant::now();
        for _ in 0..passes {
            kept += table.snapshot().len();
        }
        let snapshot = began.elapsed().as_nanos() as f64 / passes as f64;
        assert!(kept > 0);
        println!("| ranking | {count} | {ranking:.0} |");
        println!("| snapshot | {count} | {snapshot:.0} |");
    }
}
