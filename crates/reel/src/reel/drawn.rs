//! Writes counted from draw to publish by the epoch they drew in, so a prune never passes a number still on its way

use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::sync::lock;

/// Two epochs can hold writers while the third waits empty
const SLOTS: u64 = 3;

/// The most writers one ticket counts, leaving its low two bits to the slot
pub const MOST_COUNTED: u32 = (1 << 30) - 1;

/// One counter on its own cache line
#[derive(Default)]
#[repr(align(128))]
struct Line(AtomicU64);

/// Writers out, by the epoch each one drew in
#[derive(Default)]
pub struct Drawn {
    /// The epoch a writer counts itself into
    epoch: Line,

    /// Writers out in each slot's epoch
    out: [Line; SLOTS as usize],

    /// Where each slot's epoch began on the sequence counter
    starts: Mutex<[u64; SLOTS as usize]>,
}

/// One writer's count and slot in one word, so a segment hold grows by nothing
#[derive(Clone, Copy, Debug)]
pub struct DrawTicket(NonZeroU32);

impl DrawTicket {
    fn new(slot: usize, count: u32) -> DrawTicket {
        DrawTicket(NonZeroU32::new(count << 2 | slot as u32).expect("a ticket counts a writer"))
    }

    fn slot(self) -> usize {
        (self.0.get() & 0b11) as usize
    }

    fn count(self) -> u64 {
        u64::from(self.0.get() >> 2)
    }
}

/// A count given back on drop unless handed on, so a failed placement cannot wedge the floor
pub struct DrawnRecords<'a> {
    drawn: &'a Drawn,
    ticket: DrawTicket,
}

impl DrawnRecords<'_> {
    /// Hand the count to whatever gives it back once the records are published
    pub fn ticket(self) -> DrawTicket {
        let ticket = self.ticket;
        std::mem::forget(self);
        ticket
    }
}

impl Drop for DrawnRecords<'_> {
    fn drop(&mut self) {
        self.drawn.leave(self.ticket);
    }
}

impl Drawn {
    /// Count writers into the current epoch before they draw, from one to `MOST_COUNTED`
    pub fn enter(&self, count: u32) -> DrawnRecords<'_> {
        debug_assert!((1..=MOST_COUNTED).contains(&count), "count {count}");
        loop {
            let epoch = self.epoch.0.load(Ordering::Acquire);
            let slot = slot_of(epoch);
            self.out[slot]
                .0
                .fetch_add(u64::from(count), Ordering::SeqCst);
            // An advance between the two reads may have found the slot empty
            if self.epoch.0.load(Ordering::SeqCst) == epoch {
                return DrawnRecords {
                    drawn: self,
                    ticket: DrawTicket::new(slot, count),
                };
            }
            self.out[slot]
                .0
                .fetch_sub(u64::from(count), Ordering::Release);
        }
    }

    /// Count writers out once their records are published
    pub fn leave(&self, ticket: DrawTicket) {
        self.out[ticket.slot()]
            .0
            .fetch_sub(ticket.count(), Ordering::Release);
    }

    /// The lowest number a writer still out can hold, where `next` reads the counter
    pub fn floor(&self, next: impl Fn() -> u64) -> u64 {
        let mut starts = lock(&self.starts);
        // The epoch moves on only once the one before it is empty
        for _ in 0..2 {
            let epoch = self.epoch.0.load(Ordering::SeqCst);
            let before = &self.out[slot_of(epoch + SLOTS - 1)].0;
            if before.load(Ordering::SeqCst) != 0 {
                break;
            }
            starts[slot_of(epoch + 1)] = next();
            self.epoch.0.store(epoch + 1, Ordering::SeqCst);
        }
        let epoch = self.epoch.0.load(Ordering::SeqCst);
        starts[slot_of(epoch + SLOTS - 1)]
    }
}

/// The slot an epoch counts its writers in
fn slot_of(epoch: u64) -> usize {
    (epoch % SLOTS) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    // a quiet volume's floor is the counter itself
    #[test]
    fn quiet_floor_is_the_counter() {
        let drawn = Drawn::default();
        assert_eq!(drawn.floor(|| 40), 40);
        drop(drawn.enter(3));
        assert_eq!(drawn.floor(|| 90), 90);
    }

    // a writer still out holds the floor at the epoch it drew in, however often it is asked
    #[test]
    fn a_writer_out_holds_the_floor() {
        let drawn = Drawn::default();
        assert_eq!(drawn.floor(|| 10), 10);
        let ticket = drawn.enter(4).ticket();
        for next in [20, 30, 40, 50] {
            assert_eq!(
                drawn.floor(|| next),
                10,
                "the floor passed a writer still out"
            );
        }
        drawn.leave(ticket);
        assert_eq!(drawn.floor(|| 60), 60);
    }

    // a ticket keeps its slot and its count in the one word
    #[test]
    fn a_ticket_packs_its_slot_and_count() {
        for (slot, count) in [(0, 1), (1, 64), (2, MOST_COUNTED)] {
            let ticket = DrawTicket::new(slot, count);
            assert_eq!((ticket.slot(), ticket.count()), (slot, u64::from(count)));
        }
        assert_eq!(std::mem::size_of::<Option<DrawTicket>>(), 4);
    }

    // the floor never passes a number some writer still holds, with writers and the floor racing
    #[test]
    fn the_floor_never_passes_a_held_number() {
        const WRITERS: usize = 4;
        const DRAWS: u64 = 20_000;
        let drawn = Arc::new(Drawn::default());
        let counter = Arc::new(AtomicU64::new(1));
        // Each writer's number still out, or all ones between writes
        let held: Arc<Vec<AtomicU64>> =
            Arc::new((0..WRITERS).map(|_| AtomicU64::new(u64::MAX)).collect());
        let done = Arc::new(AtomicBool::new(false));

        let writers: Vec<_> = (0..WRITERS)
            .map(|at| {
                let (drawn, counter, held) =
                    (Arc::clone(&drawn), Arc::clone(&counter), Arc::clone(&held));
                std::thread::spawn(move || {
                    for _ in 0..DRAWS {
                        let ticket = drawn.enter(1).ticket();
                        let lsn = counter.fetch_add(1, Ordering::SeqCst);
                        held[at].store(lsn, Ordering::SeqCst);
                        std::hint::spin_loop();
                        held[at].store(u64::MAX, Ordering::SeqCst);
                        drawn.leave(ticket);
                    }
                })
            })
            .collect();

        let checker = {
            let (drawn, counter, held, done) = (
                Arc::clone(&drawn),
                Arc::clone(&counter),
                Arc::clone(&held),
                Arc::clone(&done),
            );
            std::thread::spawn(move || {
                let mut checks = 0u64;
                while !done.load(Ordering::SeqCst) {
                    let floor = drawn.floor(|| counter.load(Ordering::SeqCst));
                    for slot in held.iter() {
                        let lsn = slot.load(Ordering::SeqCst);
                        assert!(lsn >= floor, "floor {floor} passed {lsn}, still held");
                    }
                    checks += 1;
                }
                checks
            })
        };

        for writer in writers {
            writer.join().expect("writer");
        }
        done.store(true, Ordering::SeqCst);
        assert!(checker.join().expect("checker") > 0);
        let next = counter.load(Ordering::SeqCst);
        assert_eq!(drawn.floor(|| next), next);
    }
}
