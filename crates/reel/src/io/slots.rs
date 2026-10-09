//! Completion slots for the driver's ops, indexed by each tag's low bits

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::error::{ReelError, Result};
use crate::io::op::{Completion, Outcome, Tag};
use crate::sync::{lock, wait_for};

/// A driver's completion slots, a power of two so a tag's low bits pick the slot
pub const SLOT_COUNT: usize = 512;

/// The bits of a tag that address a slot
const SLOT_MASK: u64 = (SLOT_COUNT - 1) as u64;

/// A parked caller rechecks the table after this long, in case it missed a signal
const CLAIM_PARK: Duration = Duration::from_micros(200);

/// A parked claim gives up on its slot after this long
const CLAIM_LIMIT: Duration = Duration::from_secs(5);

/// The slot a tag addresses
fn index_of(tag: Tag) -> usize {
    (tag.0 & SLOT_MASK) as usize
}

/// How far a slot's op has got
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SlotState {
    /// Nothing in flight, so the slot can be claimed
    Free,

    /// An op is in flight and its caller will take the completion
    Flight,

    /// The completion has landed and nobody has taken it yet
    Landed,

    /// The caller left, so whoever reaps the completion reclaims it
    Orphan,
}

/// One tag's place in the table, with its own lock
struct Slot {
    /// The tag whose op owns the slot, so a stale completion is not filed
    tag: Tag,

    /// How far that flight has got
    state: SlotState,

    /// The completion, from when it lands until its caller takes it
    completion: Option<Completion>,

    /// The waker a pending future left, taken by the completion that wakes it
    waker: Option<Waker>,
}

impl Slot {
    /// Empty the slot and keep its tag, which is never reused
    fn clear(&mut self) {
        self.state = SlotState::Free;
        self.completion = None;
        self.waker = None;
    }

    /// Leave a waker for the completion to wake, unless the same one is already there
    fn seat(&mut self, waker: &Waker) {
        let is_seated = match &self.waker {
            Some(seated) => seated.will_wake(waker),
            None => false,
        };
        if !is_seated {
            self.waker = Some(waker.clone());
        }
    }
}

/// What the blocking door parks on: the drain turn and the buffer it drains into
struct Door {
    /// Whether a thread is inside the backend's poll right now
    is_polling: bool,

    /// The polling thread drains into this, so a drain allocates nothing
    scratch: Vec<Completion>,
}

/// The completion slots one driver files its ops into
pub struct SlotTable {
    /// One lock per slot
    slots: Vec<Mutex<Slot>>,

    /// The drain turn and its buffer, the only state both doors share
    door: Mutex<Door>,

    /// Signalled whenever a slot changes, for the threads parked on the door
    delivered: Condvar,

    /// Threads parked on that condition variable, read without taking the door
    parked: AtomicU64,

    /// Wakers left by claims that found no room, rung when any slot frees
    claims: Mutex<Vec<Waker>>,

    /// How many claims are in that list, read without its lock by whoever frees a slot
    claiming: AtomicU64,

    /// Serializes batches claiming their whole runs
    batching: Mutex<()>,

    /// Completions reclaimed because nothing was waiting for them
    reclaimed: AtomicU64,

    /// Completions filed into a slot whose flight was waiting for them
    landed: AtomicU64,

    /// Completions discarded because their slot had moved on or was not waiting
    stale: AtomicU64,

    /// The counter tags are drawn from
    next_tag: AtomicU64,
}

impl SlotTable {
    /// A table of the fixed width with nothing in flight
    pub fn new() -> SlotTable {
        let mut slots = Vec::with_capacity(SLOT_COUNT);
        for _ in 0..SLOT_COUNT {
            slots.push(Mutex::new(Slot {
                tag: Tag(0),
                state: SlotState::Free,
                completion: None,
                waker: None,
            }));
        }
        SlotTable {
            slots,
            door: Mutex::new(Door {
                is_polling: false,
                scratch: Vec::new(),
            }),
            delivered: Condvar::new(),
            parked: AtomicU64::new(0),
            claims: Mutex::new(Vec::new()),
            claiming: AtomicU64::new(0),
            batching: Mutex::new(()),
            reclaimed: AtomicU64::new(0),
            landed: AtomicU64::new(0),
            stale: AtomicU64::new(0),
            next_tag: AtomicU64::new(0),
        }
    }

    /// How many flights the table holds at once
    pub fn slots(&self) -> usize {
        SLOT_COUNT
    }

    /// A fresh tag from a monotonic counter, unique for the life of this table
    pub fn next_tag(&self) -> Tag {
        Tag(self.next_tag.fetch_add(1, Ordering::Relaxed))
    }

    /// Take a slot for a tag, or return false if another op holds it
    pub fn try_claim(&self, tag: Tag) -> bool {
        let mut slot = lock(&self.slots[index_of(tag)]);
        if slot.state != SlotState::Free {
            return false;
        }
        slot.tag = tag;
        slot.state = SlotState::Flight;
        true
    }

    /// Claim a batch's tags in order up to the first taken slot, returning how many
    pub fn claim_run(&self, wanted: &[Tag]) -> usize {
        let mut taken = 0;
        for tag in wanted {
            if !self.try_claim(*tag) {
                break;
            }
            taken += 1;
        }
        taken
    }

    /// Claim every tag of a batch or none of them
    pub fn claim_all(&self, wanted: &[Tag]) -> bool {
        if wanted.len() < 2 {
            return self.claim_whole(wanted);
        }
        let _turn = lock(&self.batching);
        self.claim_whole(wanted)
    }

    /// Give back claims whose ops never went down
    pub fn release_run(&self, wanted: &[Tag]) {
        let mut is_freed = false;
        for tag in wanted {
            let mut slot = lock(&self.slots[index_of(*tag)]);
            if slot.tag == *tag && slot.state == SlotState::Flight {
                slot.clear();
                is_freed = true;
            }
        }
        self.signal(is_freed);
    }

    /// Take the completion a slot is holding, freeing it for the next flight
    pub fn take(&self, tag: Tag) -> Option<Completion> {
        let taken = {
            let mut slot = lock(&self.slots[index_of(tag)]);
            if slot.tag != tag || slot.state != SlotState::Landed {
                return None;
            }
            let completion = slot.completion.take();
            slot.clear();
            completion
        };
        self.signal(true);
        taken
    }

    /// Take the completion, or leave a waker in the slot for when it lands
    pub fn take_or_seat(&self, tag: Tag, waker: &Waker) -> Option<Completion> {
        let taken = {
            let mut slot = lock(&self.slots[index_of(tag)]);
            if slot.tag != tag {
                return None;
            }
            match slot.state {
                SlotState::Landed => {
                    let completion = slot.completion.take();
                    slot.clear();
                    completion
                }
                SlotState::Flight => {
                    slot.seat(waker);
                    return None;
                }
                SlotState::Free | SlotState::Orphan => return None,
            }
        };
        self.signal(true);
        taken
    }

    /// Give up on an op, so its completion is reclaimed
    pub fn orphan(&self, tag: Tag) {
        let abandoned = {
            let mut slot = lock(&self.slots[index_of(tag)]);
            if slot.tag != tag {
                return;
            }
            match slot.state {
                SlotState::Flight => {
                    slot.state = SlotState::Orphan;
                    slot.waker = None;
                    return;
                }
                SlotState::Landed => {
                    let completion = slot.completion.take();
                    slot.clear();
                    completion
                }
                SlotState::Free | SlotState::Orphan => return,
            }
        };
        if let Some(completion) = abandoned {
            self.discard(completion);
        }
        self.signal(true);
    }

    /// Reclaim the buffers of a completion nobody is waiting for
    pub fn discard(&self, completion: Completion) {
        reclaim(completion);
        self.reclaimed.fetch_add(1, Ordering::Relaxed);
    }

    /// Slots that are not free, counted by walking the table
    pub fn outstanding(&self) -> usize {
        let mut held = 0;
        for slot in &self.slots {
            if lock(slot).state != SlotState::Free {
                held += 1;
            }
        }
        held
    }

    /// Slots holding a waker, which is the futures currently pending
    pub fn wakers(&self) -> usize {
        let mut seated = 0;
        for slot in &self.slots {
            if lock(slot).waker.is_some() {
                seated += 1;
            }
        }
        seated
    }

    /// Completions reclaimed since the table was opened
    pub fn reclaimed(&self) -> u64 {
        self.reclaimed.load(Ordering::Relaxed)
    }

    /// Wait on a condition over the slots, draining the backend when nobody else is
    pub fn pump<Out, Ready, Drain>(&self, mut ready: Ready, mut drain: Drain) -> Result<Out>
    where
        Ready: FnMut() -> Option<Out>,
        Drain: FnMut(&mut Vec<Completion>) -> Result<usize>,
    {
        loop {
            let mut scratch = loop {
                if let Some(out) = ready() {
                    return Ok(out);
                }
                match self.begin_poll() {
                    Some(scratch) => break scratch,
                    None => {
                        if let Some(out) = self.ask_parked(&mut ready) {
                            return Ok(out);
                        }
                    }
                }
            };

            let polled = drain(&mut scratch);
            self.end_poll(scratch);

            // Another thread may have filed the answer during the drain, so check once more
            if polled? == 0 {
                return match ready() {
                    Some(out) => Ok(out),
                    None => Err(never_completed()),
                };
            }
        }
    }

    /// Park a blocking caller until a slot comes free, giving up after `CLAIM_LIMIT`
    pub fn wait_free<Out, Ready, Drain>(&self, mut ready: Ready, mut drain: Drain) -> Result<Out>
    where
        Ready: FnMut() -> Option<Out>,
        Drain: FnMut(&mut Vec<Completion>) -> Result<usize>,
    {
        let deadline = Instant::now() + CLAIM_LIMIT;
        loop {
            if let Some(out) = ready() {
                return Ok(out);
            }

            if let Some(mut scratch) = self.begin_poll() {
                let drained = drain(&mut scratch);
                self.end_poll(scratch);
                if drained? > 0 {
                    continue;
                }
            }

            if let Some(out) = self.ask_parked(&mut ready) {
                return Ok(out);
            }

            if Instant::now() >= deadline {
                return Err(never_freed());
            }
        }
    }

    /// Take the turn to drain the backend, or nothing if a thread already has it
    pub fn begin_poll(&self) -> Option<Vec<Completion>> {
        let mut door = lock(&self.door);
        if door.is_polling {
            return None;
        }
        door.is_polling = true;
        Some(std::mem::take(&mut door.scratch))
    }

    /// Give the turn back, filing what the drain moved and waking its waiters
    pub fn end_poll(&self, mut drained: Vec<Completion>) {
        self.file(&mut drained);

        let mut door = lock(&self.door);
        door.is_polling = false;
        door.scratch = drained;
        // Threads parked for the drain turn need to hear that it is free
        if self.parked.load(Ordering::SeqCst) > 0 {
            self.delivered.notify_all();
        }
    }

    /// File completions into their slots and wake their waiters, leaving the vector empty
    pub fn file(&self, drained: &mut Vec<Completion>) {
        let mut is_freed = false;
        for completion in drained.drain(..) {
            let (woken, freed) = self.land(completion);
            if let Some(waker) = woken {
                waker.wake();
            }
            is_freed = is_freed || freed;
        }
        self.signal(is_freed);
    }

    /// File one completion, for a backend that answered at submission
    pub fn file_one(&self, completion: Completion) {
        let (woken, is_freed) = self.land(completion);
        if let Some(waker) = woken {
            waker.wake();
        }
        self.signal(is_freed);
    }

    /// A claim on every slot a submission needs, taken as a future
    pub fn claim<'table>(&'table self, wanted: &'table [Tag]) -> Claim<'table> {
        Claim {
            table: self,
            wanted,
        }
    }

    /// A future over one tag already claimed and submitted
    pub fn wait_op(&self, tag: Tag) -> IoWait<'_> {
        IoWait { table: self, tag }
    }

    /// A future over the runs of tags one batch went down as
    pub fn wait_batch(&self, runs: Vec<TagRun>, count: usize) -> BatchWait<'_> {
        let mut filled = Vec::new();
        filled.resize_with(count, || None);
        BatchWait {
            table: self,
            runs,
            filled,
            outstanding: count,
        }
    }

    /// Claim every tag or roll the run back to where it was found
    fn claim_whole(&self, wanted: &[Tag]) -> bool {
        for (at, tag) in wanted.iter().enumerate() {
            if self.try_claim(*tag) {
                continue;
            }
            for taken in &wanted[..at] {
                let mut slot = lock(&self.slots[index_of(*taken)]);
                slot.clear();
            }
            return false;
        }
        true
    }

    /// File one completion, returning the waker to wake and whether the slot came free
    fn land(&self, completion: Completion) -> (Option<Waker>, bool) {
        let (stale, is_freed) = {
            let mut slot = lock(&self.slots[index_of(completion.tag)]);
            if slot.tag != completion.tag {
                self.stale.fetch_add(1, Ordering::Relaxed);
                (completion, false)
            } else {
                match slot.state {
                    SlotState::Flight => {
                        slot.completion = Some(completion);
                        slot.state = SlotState::Landed;
                        self.landed.fetch_add(1, Ordering::Relaxed);
                        return (slot.waker.take(), false);
                    }
                    SlotState::Orphan => {
                        slot.clear();
                        (completion, true)
                    }
                    SlotState::Free | SlotState::Landed => {
                        self.stale.fetch_add(1, Ordering::Relaxed);
                        (completion, false)
                    }
                }
            }
        };
        self.discard(stale);
        (None, is_freed)
    }

    /// Take the drain turn if free, file what one poll moves, and say whether it moved any
    pub fn drain_once(&self, drain: impl FnOnce(&mut Vec<Completion>) -> Result<usize>) -> bool {
        // When refused, the future waits for another drainer to wake it
        if crate::sync::rendezvous::refused("slots/self-drain") {
            return false;
        }
        crate::sync::rendezvous::at("slots/self-drain");
        let Some(mut scratch) = self.begin_poll() else {
            return false;
        };
        let moved = drain(&mut scratch).unwrap_or(0);
        self.end_poll(scratch);
        moved > 0
    }

    /// Every non-free slot and the filing counters, for stall diagnosis
    pub fn debug_flights(&self) -> String {
        let mut out = format!(
            "landed {} stale {} reclaimed {} claiming {} parked {}\n",
            self.landed.load(Ordering::Relaxed),
            self.stale.load(Ordering::Relaxed),
            self.reclaimed.load(Ordering::Relaxed),
            self.claiming.load(Ordering::Relaxed),
            self.parked.load(Ordering::Relaxed),
        );
        for (at, slot) in self.slots.iter().enumerate() {
            let slot = lock(slot);
            if slot.state != SlotState::Free {
                out.push_str(&format!(
                    "slot {at}: tag {} {:?} completion {} waker {}\n",
                    slot.tag.0,
                    slot.state,
                    slot.completion.is_some(),
                    slot.waker.is_some(),
                ));
            }
        }
        out
    }

    /// Wake the callers waiting on the table after a slot changes
    fn signal(&self, is_freed: bool) {
        if self.parked.load(Ordering::SeqCst) > 0 {
            let _door = lock(&self.door);
            self.delivered.notify_all();
        }
        if is_freed {
            self.ring_claims();
        }
    }

    /// Wake every seated claim after a slot comes free
    fn ring_claims(&self) {
        if self.claiming.load(Ordering::SeqCst) == 0 {
            return;
        }
        let owed = {
            let mut claims = lock(&self.claims);
            self.claiming.store(0, Ordering::SeqCst);
            std::mem::take(&mut *claims)
        };
        ring(owed);
    }

    /// Seat a waker for a claim whose slot holds another op
    fn seat_claim(&self, waker: &Waker) {
        let mut claims = lock(&self.claims);
        for seated in claims.iter() {
            if seated.will_wake(waker) {
                return;
            }
        }
        claims.push(waker.clone());
        self.claiming.store(claims.len() as u64, Ordering::SeqCst);
    }

    /// Take a seat back, for a claim that got its slots on the second ask
    fn unseat_claim(&self, waker: &Waker) {
        let mut claims = lock(&self.claims);
        if let Some(at) = claims.iter().position(|seated| seated.will_wake(waker)) {
            claims.remove(at);
            self.claiming.store(claims.len() as u64, Ordering::SeqCst);
        }
    }

    /// Check the condition with this thread counted as parked, then park for a bounded time
    fn ask_parked<Out>(&self, ready: &mut impl FnMut() -> Option<Out>) -> Option<Out> {
        self.parked.fetch_add(1, Ordering::SeqCst);
        let answer = ready();
        if answer.is_none() {
            let door = lock(&self.door);
            let _door = wait_for(&self.delivered, door, CLAIM_PARK);
        }
        self.parked.fetch_sub(1, Ordering::SeqCst);
        answer
    }
}

/// Wake these wakers, once the table's locks are released
fn ring(woken: Vec<Waker>) {
    for waker in woken {
        waker.wake();
    }
}

impl Default for SlotTable {
    fn default() -> SlotTable {
        SlotTable::new()
    }
}

/// A contiguous run of tags from one submission
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TagRun {
    /// First tag of the run
    pub first: Tag,

    /// How many tags the run covers, each one past the last
    pub count: usize,
}

/// The runs a batch's tags fall into, which is one run when nothing interleaved
pub fn runs_of(wanted: &[Tag]) -> Vec<TagRun> {
    let mut runs: Vec<TagRun> = Vec::new();
    for tag in wanted {
        if let Some(run) = runs.last_mut() {
            if run.first.0 + run.count as u64 == tag.0 {
                run.count += 1;
                continue;
            }
        }
        runs.push(TagRun {
            first: *tag,
            count: 1,
        });
    }
    runs
}

/// A future that claims every slot a submission's tags need, all or nothing
pub struct Claim<'table> {
    /// The table holding the slots
    table: &'table SlotTable,

    /// The tags whose slots the submission needs, taken together
    wanted: &'table [Tag],
}

impl Future for Claim<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let waiter = self.get_mut();
        if waiter.table.claim_all(waiter.wanted) {
            return Poll::Ready(());
        }

        // Seat the waker before asking again, so a slot freed in between wakes this claim
        waiter.table.seat_claim(cx.waker());
        if waiter.table.claim_all(waiter.wanted) {
            waiter.table.unseat_claim(cx.waker());
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

/// A future over one op in flight
pub struct IoWait<'table> {
    /// The table holding the slot this flight owns
    table: &'table SlotTable,

    /// The tag whose low bits address that slot
    tag: Tag,
}

impl Future for IoWait<'_> {
    type Output = Completion;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Completion> {
        let waiter = self.get_mut();
        match waiter.table.take_or_seat(waiter.tag, cx.waker()) {
            Some(completion) => Poll::Ready(completion),
            None => Poll::Pending,
        }
    }
}

impl Drop for IoWait<'_> {
    /// A future dropped while pending leaves its slot orphaned
    fn drop(&mut self) {
        self.table.orphan(self.tag);
    }
}

/// A batch in flight, one future over the runs of tags it went down as
pub struct BatchWait<'table> {
    /// The table holding the slots these flights own
    table: &'table SlotTable,

    /// The tag runs the batch went down as, in submit order
    runs: Vec<TagRun>,

    /// What each tag came back with, in submit order
    filled: Vec<Option<Completion>>,

    /// Completions still to land before the batch answers
    outstanding: usize,
}

impl BatchWait<'_> {
    /// Take whatever has landed and leave a waker in every slot still in flight
    fn gather(&mut self, waker: &Waker) {
        let mut at = 0;
        for index in 0..self.runs.len() {
            let run = self.runs[index];
            for step in 0..run.count {
                if self.filled[at].is_none() {
                    let tag = Tag(run.first.0 + step as u64);
                    if let Some(completion) = self.table.take_or_seat(tag, waker) {
                        self.filled[at] = Some(completion);
                        self.outstanding -= 1;
                    }
                }
                at += 1;
            }
        }
    }
}

impl Future for BatchWait<'_> {
    type Output = Vec<Completion>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Vec<Completion>> {
        let waiter = self.get_mut();
        if waiter.outstanding > 0 {
            waiter.gather(cx.waker());
        }
        if waiter.outstanding > 0 {
            return Poll::Pending;
        }

        // Clear the runs, so dropping an answered batch orphans nothing
        let mut answered = Vec::with_capacity(waiter.filled.len());
        for completion in waiter.filled.drain(..).flatten() {
            answered.push(completion);
        }
        waiter.runs.clear();
        Poll::Ready(answered)
    }
}

impl Drop for BatchWait<'_> {
    /// A batch dropped while pending orphans its range and reclaims what it took
    fn drop(&mut self) {
        let mut at = 0;
        for index in 0..self.runs.len() {
            let run = self.runs[index];
            for step in 0..run.count {
                match self.filled[at].take() {
                    Some(completion) => self.table.discard(completion),
                    None => self.table.orphan(Tag(run.first.0 + step as u64)),
                }
                at += 1;
            }
        }
    }
}

/// Return an unwanted completion's read buffers to the pool
fn reclaim(completion: Completion) {
    match completion.outcome {
        Outcome::Read { buf, .. } => crate::reel::payload::give(buf.into_vec()),
        Outcome::ReadSplit { head, body, .. } => {
            crate::reel::payload::give(head.into_vec());
            crate::reel::payload::give(body.into_vec());
        }
        Outcome::Opened(_)
        | Outcome::Wrote { .. }
        | Outcome::Listed(_)
        | Outcome::Length(_)
        | Outcome::Done(_) => {}
    }
}

/// The error a wait that outlasted the backend maps to
fn never_completed() -> ReelError {
    ReelError::Io(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "backend never completed a submitted op",
    ))
}

/// The error a claim whose slot never came back maps to
fn never_freed() -> ReelError {
    ReelError::Io(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "completion slot never came free",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::task::Wake;

    use crate::io::op::ReadBuf;

    /// A waker that counts its wakes
    struct Counter(AtomicUsize);

    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn done(tag: u64) -> Completion {
        Completion {
            tag: Tag(tag),
            outcome: Outcome::Done(Ok(())),
        }
    }

    // a claim holds the slot its tag addresses until the completion is taken
    #[test]
    fn claim_holds_a_slot() {
        let table = SlotTable::new();

        assert!(table.try_claim(Tag(1)));

        assert_eq!(table.outstanding(), 1);
        table.file_one(done(1));
        assert_eq!(
            table.outstanding(),
            1,
            "a landed completion still holds its slot"
        );
        assert!(table.take(Tag(1)).is_some());
        assert_eq!(table.outstanding(), 0);
    }

    // two tags a table's width apart address one slot, so the second waits
    #[test]
    fn a_wrapped_tag_waits() {
        let table = SlotTable::new();
        let wrapped = Tag(SLOT_COUNT as u64);

        assert!(table.try_claim(Tag(0)));

        assert!(!table.try_claim(wrapped));
        assert!(table.take(Tag(0)).is_none());
        table.file_one(done(0));
        assert!(table.take(Tag(0)).is_some());
        assert!(table.try_claim(wrapped));
    }

    // a batch claims every tag or none of them
    #[test]
    fn claim_all_is_whole() {
        let table = SlotTable::new();
        let wanted = [Tag(1), Tag(2), Tag(3)];

        assert!(table.try_claim(Tag(2)));

        assert!(!table.claim_all(&wanted));
        assert_eq!(table.outstanding(), 1, "the partial claim was given back");
    }

    // a run stops where the batch's own tags would collide
    #[test]
    fn a_run_stops_at_a_collision() {
        let table = SlotTable::new();
        let wanted = [Tag(0), Tag(1), Tag(SLOT_COUNT as u64)];

        let run = table.claim_run(&wanted);

        assert_eq!(run, 2);
    }

    // a completion for a flight nobody is waiting on is reclaimed
    #[test]
    fn a_stale_completion_is_reclaimed() {
        let table = SlotTable::new();

        table.file_one(done(7));

        assert_eq!(table.reclaimed(), 1);
        assert_eq!(table.outstanding(), 0);
    }

    // an orphaned flight frees its slot where the completion lands
    #[test]
    fn an_orphan_frees_on_landing() {
        let table = SlotTable::new();
        assert!(table.try_claim(Tag(4)));

        table.orphan(Tag(4));
        assert_eq!(
            table.outstanding(),
            1,
            "the flight keeps its seat until it lands"
        );

        table.file_one(done(4));
        assert_eq!(table.outstanding(), 0);
        assert_eq!(table.reclaimed(), 1);
    }

    // consecutive tags fall into one run and a gap starts another
    #[test]
    fn runs_compress_the_tags() {
        let runs = runs_of(&[Tag(4), Tag(5), Tag(6), Tag(9), Tag(10)]);

        assert_eq!(runs.len(), 2);
        assert_eq!(
            runs[0],
            TagRun {
                first: Tag(4),
                count: 3
            }
        );
        assert_eq!(
            runs[1],
            TagRun {
                first: Tag(9),
                count: 2
            }
        );
    }

    // a read completion nobody took hands its buffer back to the pool
    #[test]
    fn a_reclaimed_read_returns_its_buffer() {
        let held = std::thread::spawn(|| {
            let table = SlotTable::new();
            table.file_one(Completion {
                tag: Tag(3),
                outcome: Outcome::Read {
                    result: Ok(0),
                    buf: ReadBuf::new(4096),
                },
            });
            (table.reclaimed(), crate::reel::payload::held_bytes())
        })
        .join()
        .expect("the reclaiming thread joins");

        assert_eq!(held.0, 1);
        assert_eq!(
            held.1, 4096,
            "the buffer went to the allocator, not the pool"
        );
    }

    // a claim that cannot take its whole run wakes nobody, itself included
    #[test]
    fn a_rolled_back_claim_rings_nobody() {
        let table = SlotTable::new();
        let wanted = [Tag(0), Tag(1)];
        assert!(table.try_claim(Tag(1)));

        let woken = Arc::new(Counter(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&woken));
        let mut cx = Context::from_waker(&waker);
        let mut claiming = Box::pin(table.claim(&wanted));

        assert!(claiming.as_mut().poll(&mut cx).is_pending());
        assert!(claiming.as_mut().poll(&mut cx).is_pending());
        assert_eq!(
            table.outstanding(),
            1,
            "the half it took was not given back"
        );
        assert_eq!(
            woken.0.load(Ordering::SeqCst),
            0,
            "the rollback rang its own claim"
        );

        // The take frees the slot, which has to wake the claim
        table.file_one(done(1));
        assert!(table.take(Tag(1)).is_some());

        assert!(
            woken.0.load(Ordering::SeqCst) >= 1,
            "the freed slot rang nothing"
        );
        assert!(claiming.as_mut().poll(&mut cx).is_ready());
        assert_eq!(table.outstanding(), 2);
    }

    // a parked claim waits for a take, since draining the backend frees nothing
    #[test]
    fn a_parked_claim_waits_for_a_take() {
        let table = Arc::new(SlotTable::new());
        let wrapped = [Tag(SLOT_COUNT as u64)];
        assert!(table.try_claim(Tag(0)));
        table.file_one(done(0));

        let claiming = {
            let table = Arc::clone(&table);
            std::thread::spawn(move || {
                table.wait_free(|| table.claim_all(&wrapped).then_some(()), |_| Ok(0))
            })
        };

        std::thread::sleep(Duration::from_millis(50));
        assert!(table.take(Tag(0)).is_some());

        claiming
            .join()
            .expect("the claiming thread joins")
            .expect("the claim took the slot the take freed");
        assert_eq!(table.outstanding(), 1);
    }

    // two callers racing one slot never both hold it, however they interleave
    #[test]
    fn a_contended_slot_serves_one_at_a_time() {
        let table = Arc::new(SlotTable::new());
        let rounds = 4_000u64;

        let mut racers = Vec::new();
        for _ in 0..2 {
            let table = Arc::clone(&table);
            racers.push(std::thread::spawn(move || {
                let mut taken = 0u64;
                for round in 0..rounds {
                    let tag = Tag(round * SLOT_COUNT as u64);
                    if !table.try_claim(tag) {
                        continue;
                    }
                    table.file_one(done(tag.0));
                    assert!(table.take(tag).is_some(), "the slot answered someone else");
                    taken += 1;
                }
                taken
            }));
        }

        let mut taken = 0;
        for racer in racers {
            taken += racer.join().expect("a racer joins");
        }

        assert!(taken > 0);
        assert_eq!(
            table.reclaimed(),
            0,
            "a completion landed on a slot its filer had lost"
        );
        assert_eq!(table.outstanding(), 0);
    }
}
