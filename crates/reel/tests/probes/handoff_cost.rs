//! What a batched drain would save against what it would cost
//!
//! Batching trades one syscall per record for one syscall per batch plus a handoff to
//! every writer that did not issue it, so a batch of depth N is only worth building
//! when the handoff costs less than the syscall it replaces. Both terms are properties
//! of the machine, so run this on the hardware the answer is for.
//!
//! Opt-in. Run with:
//!   cargo test -p reel --release --test probes -- handoff_cost

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Instant;

use tempfile::TempDir;

use reel::io::direct::AlignedBuf;

const ROUNDS: usize = 20_000;

/// Cost of the write a batch would fold into its own, per record
fn syscall_cost(record: usize) -> f64 {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("bench.bin");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .expect("open");
    let fd = {
        use std::os::fd::AsRawFd;
        file.as_raw_fd()
    };

    let payload = vec![0xABu8; record];
    // Warm the file so the measurement is the call rather than the first touch of the
    // pages behind it.
    for at in 0..64 {
        write_at(fd, &payload, (at * record) as u64);
    }

    let started = Instant::now();
    for at in 0..ROUNDS {
        write_at(fd, &payload, (at * record) as u64);
    }
    started.elapsed().as_nanos() as f64 / ROUNDS as f64
}

fn write_at(fd: std::os::fd::RawFd, bytes: &[u8], offset: u64) {
    let wrote = unsafe {
        libc::pwrite(
            fd,
            bytes.as_ptr() as *const libc::c_void,
            bytes.len(),
            offset as libc::off_t,
        )
    };
    assert!(wrote > 0, "write made no progress");
}

/// Cost of a real handoff, measured as a round trip so the follower truly parks
///
/// What a drain pays is a follower that slept for the length of the batch write and
/// has to be scheduled back in, so the two threads take turns and neither proceeds
/// until the other has woken it. Half a round trip is one handoff.
fn handoff_cost() -> f64 {
    let state = Arc::new((Mutex::new((0u64, 0u64)), Condvar::new()));
    let stop = Arc::new(AtomicBool::new(false));

    let follower = {
        let state = Arc::clone(&state);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let (lock, signal) = &*state;
            loop {
                let mut turn = lock.lock().unwrap_or_else(|poison| poison.into_inner());
                while turn.0 == turn.1 && !stop.load(Ordering::Relaxed) {
                    turn = signal
                        .wait(turn)
                        .unwrap_or_else(|poison| poison.into_inner());
                }
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                // Answer the leader, which is what makes this a round trip.
                turn.1 = turn.0;
                drop(turn);
                signal.notify_one();
            }
        })
    };

    let (lock, signal) = &*state;
    let started = Instant::now();
    for round in 1..=ROUNDS as u64 {
        {
            let mut turn = lock.lock().unwrap_or_else(|poison| poison.into_inner());
            turn.0 = round;
        }
        signal.notify_one();
        let mut turn = lock.lock().unwrap_or_else(|poison| poison.into_inner());
        while turn.1 != round {
            turn = signal
                .wait(turn)
                .unwrap_or_else(|poison| poison.into_inner());
        }
    }
    // A round trip is two handoffs, so one is half of it.
    let elapsed = started.elapsed().as_nanos() as f64 / ROUNDS as f64 / 2.0;

    stop.store(true, Ordering::Relaxed);
    signal.notify_all();
    follower.join().expect("follower joins");
    elapsed
}

// what a batch saves per record against what it costs per record
pub fn batching_arithmetic() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    let handoff = handoff_cost();
    println!("\nhandoff (notify one parked follower): {handoff:.0} ns\n");
    println!(
        "{:>10}{:>14}{:>16}{:>14}",
        "record", "syscall ns", "batch depth 32", "verdict"
    );

    for record in [100usize, 4096, 65_536] {
        let syscall = syscall_cost(record);
        // A batch of depth N pays one syscall for everyone plus a handoff each.
        let batched = syscall / 32.0 + handoff;
        let verdict = if batched < syscall {
            "batch wins"
        } else {
            "batch loses"
        };
        println!("{record:>10}{syscall:>14.0}{batched:>16.0}{verdict:>14}");
    }
    println!(
        "\nA batch is worth building only where the handoff is cheaper than the \
         syscall it removes."
    );
}

// what the direct path's aligned allocation costs, beside the two terms above
pub fn aligned_alloc_cost() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    let started = Instant::now();
    for _ in 0..ROUNDS {
        let buf = AlignedBuf::uninit(4096).expect("aligned");
        std::hint::black_box(buf.as_ptr());
    }
    let uninit = started.elapsed().as_nanos() as f64 / ROUNDS as f64;

    let started = Instant::now();
    for _ in 0..ROUNDS {
        let buf = AlignedBuf::new(4096).expect("aligned");
        std::hint::black_box(buf.as_ptr());
    }
    let zeroed = started.elapsed().as_nanos() as f64 / ROUNDS as f64;

    println!("\naligned 4 KiB buffer: uninit {uninit:.0} ns, zeroed {zeroed:.0} ns");
    assert!(uninit <= zeroed + 50.0, "skipping the zeroing did not help");
}
