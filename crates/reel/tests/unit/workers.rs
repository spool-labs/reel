use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use reel::engine::workers::Workers;
use reel::io::job::{Job, JobStep};
use reel::io::plain::PlainIo;
use reel::io::plain_task::PlainTask;
use reel::io::{Io, IoTask};
use reel_sim::wait::wait_until;

const WORKERS: usize = 4;
const SLOTS: usize = 4;
const ROUNDS: usize = 20_000;
// a counted block comes back in microseconds, so a block still out after this was handed to no one awake
const LOST_LINE: Duration = Duration::from_secs(2);
const FREE: u8 = 0;
const ASSIGNED: u8 = 1;
const COUNTING: u8 = 2;
// an empty look waits this long before the worker marks itself idle, so a hand-out lands inside the race
const LOOK_GAP: Duration = Duration::from_micros(20);

struct Worker {
    workers: Arc<Workers<PlainTask>>,
    slots: Arc<[AtomicU8; SLOTS]>,
    index: usize,
}

impl Worker {
    // take a handed-out block and give its slot back as counted, or wait out the gap when there is none
    fn take(&self) -> bool {
        let Some(slot) = self.slots.iter().find(|slot| {
            slot.load(Ordering::SeqCst) == ASSIGNED
                && slot.compare_exchange(ASSIGNED, COUNTING, Ordering::SeqCst, Ordering::SeqCst).is_ok()
        }) else {
            let started = Instant::now();
            while started.elapsed() < LOOK_GAP {
                std::hint::spin_loop();
            }
            return false;
        };
        slot.store(FREE, Ordering::Release);
        true
    }
}

impl Job for Worker {
    fn step(&mut self) -> JobStep {
        let workers = Arc::clone(&self.workers);
        if workers.look_or_idle(self.index, || self.take()) { JobStep::Worked } else { JobStep::Idle }
    }
}

// a hand-out that races a worker's way to sleep always reaches a worker: no block stays assigned while every worker sleeps
#[test]
fn hand_out_racing_sleep_wakes_a_worker() {
    let io = PlainIo::new();
    let workers = Arc::new(Workers::<PlainTask>::new(WORKERS));
    let slots: Arc<[AtomicU8; SLOTS]> = Arc::new([const { AtomicU8::new(FREE) }; SLOTS]);
    for (index, cell) in workers.cells.iter().enumerate() {
        let worker = Worker { workers: Arc::clone(&workers), slots: Arc::clone(&slots), index };
        let task = io.spawn_job("test-worker", worker).expect("spawn");
        let _set = cell.task.set(task);
    }
    for round in 0..ROUNDS {
        let slot = &slots[round % SLOTS];
        wait_until(|| slot.load(Ordering::Acquire) == FREE, "a slot never came back");
        slot.store(ASSIGNED, Ordering::SeqCst);
        workers.wake_one();
        let handed = Instant::now();
        while slot.load(Ordering::Acquire) == ASSIGNED {
            let sleeping = (0..WORKERS).filter(|index| workers.is_idle(*index)).count();
            assert!(handed.elapsed() < LOST_LINE, "round {round}: a block stayed assigned with {sleeping} of {WORKERS} workers asleep");
            thread::yield_now();
        }
    }
    for cell in &workers.cells {
        cell.task.get().expect("a task").stop().expect("stop");
    }
}
