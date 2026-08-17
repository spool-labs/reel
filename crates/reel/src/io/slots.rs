//! Completion slots the driver's ops land in, addressed by the tag's low bits
//!
//! A tag comes off a monotonic counter, so its low bits index a power-of-two
//! table sized to the ops one driver keeps in flight, which is what replaces a
//! tag-keyed map. A caller with a thread parks on the condition variable; one on
//! a runtime worker leaves a waker where a parked thread would have signalled.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::error::{ReelError, Result};
use crate::io::op::{Completion, Outcome, Tag};
use crate::sync::checked::{lock, wait_for, AtomicU64, Condvar, Mutex, Ordering};

/// Completion slots one driver keeps, which is the ops it may have in flight
///
/// A power of two, so a tag's low bits are its slot and nothing on the op path
/// divides.
#[cfg(not(loom))]
pub const SLOT_COUNT: usize = 512;

/// Two slots under the checker, which is one collision and the whole question
#[cfg(loom)]
pub const SLOT_COUNT: usize = 2;

/// The bits of a tag that address a slot
const SLOT_MASK: u64 = (SLOT_COUNT - 1) as u64;

/// How long a parked caller waits before it looks at the table itself
///
/// Every change signals, so a park normally ends on that; the timeout covers an
/// orphan's slot and the window before a counted-in thread reaches the wait.
const CLAIM_PARK: Duration = Duration::from_micros(200);

/// How long a parked claim waits for its slot before it gives up on it
///
/// A slot turns over in the time one op takes, so a claim still waiting is
/// waiting on a flight nobody will answer, which waiting longer does not fix.
const CLAIM_LIMIT: Duration = Duration::from_secs(5);

/// The slot a tag addresses, which is the whole of the correlation
fn index_of(tag: Tag) -> usize {
    (tag.0 & SLOT_MASK) as usize
}

/// How far the flight one slot carries has got
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SlotState {
    /// Nothing in flight, so the slot is there to be claimed
    Free,

    /// An op is in flight and its caller means to take the completion
    Flight,

    /// The completion has landed and nobody has taken it yet
    Landed,

    /// The caller walked away, so whoever reaps the completion puts it back
    Orphan,
}

/// One tag's place in the table, holding whatever its flight has reached
///
/// Behind a gate of its own, since a tag names one slot and the callers on the
/// table are only ever on the same one by collision.
struct Slot {
    /// The tag whose flight owns the slot, so a stale completion is not filed
    tag: Tag,

    /// How far that flight has got
    state: SlotState,

    /// The completion, from where it lands until its caller takes it
    completion: Option<Completion>,

    /// The waker a pending future left, taken by the completion that wakes it
    waker: Option<Waker>,
}

impl Slot {
    /// Empty the slot, whatever it was carrying
    ///
    /// The tag stays, and since tags are never reused a late completion or drop
    /// finds nothing rather than another flight.
    fn clear(&mut self) {
        self.state = SlotState::Free;
        self.completion = None;
        self.waker = None;
    }

    /// Leave a waker for the completion to ring, unless the same one is already in
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

    /// Buffer the polling thread drains into, kept so a drain allocates nothing
    scratch: Vec<Completion>,
}

/// The completion slots one driver files its ops into
pub struct SlotTable {
    /// One gate per slot, since a tag names one slot and never two
    slots: Vec<Mutex<Slot>>,

    /// The drain turn and its buffer, which is all both doors share
    door: Mutex<Door>,

    /// Signalled whenever a slot changes, for the threads parked on the door
    delivered: Condvar,

    /// Threads parked on that condition variable, read without taking the door
    parked: AtomicU64,

    /// Wakers left by claims that found no room, rung when any slot frees
    claims: Mutex<Vec<Waker>>,

    /// Claims seated in that list, read without taking it by whoever frees a slot
    claiming: AtomicU64,

    /// The turn a batch takes to claim its whole run against another batch
    batching: Mutex<()>,

    /// Completions put back because nothing was waiting for them any more
    reclaimed: AtomicU64,

    /// Completions filed into a slot whose flight was waiting for them
    landed: AtomicU64,

    /// Completions discarded because their slot had moved on to another tag
    stale: AtomicU64,

    /// Draws the tags whose low bits address the slots
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

    /// A fresh tag unique for the life of this table
    ///
    /// Monotonic, so a batch draws a contiguous run and no tag is reused: a late
    /// completion lands on a slot naming another tag and is reclaimed.
    pub fn next_tag(&self) -> Tag {
        Tag(self.next_tag.fetch_add(1, Ordering::Relaxed))
    }

    /// Take a slot for a tag, or say it is still carrying another flight
    pub fn try_claim(&self, tag: Tag) -> bool {
        let mut slot = lock(&self.slots[index_of(tag)]);
        if slot.state != SlotState::Free {
            return false;
        }
        slot.tag = tag;
        slot.state = SlotState::Flight;
        true
    }

    /// Claim as much of a batch's leading run as the table has room for
    ///
    /// The run stops at the first tag whose slot is taken, so a batch wider than
    /// the table splits where its own tags collide.
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
    ///
    /// All or nothing keeps two batches from each holding half the table and
    /// waiting on the other, and a rollback rings nobody. The turn keeps two
    /// batches from rolling back over each other.
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
    ///
    /// A landing takes the waker away, so a future polled again leaves a fresh
    /// one rather than assuming the old survived.
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

    /// Give up on a flight, so its completion is reclaimed rather than delivered
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

    /// Put back what a completion nobody is waiting for is carrying
    pub fn discard(&self, completion: Completion) {
        reclaim(completion);
        self.reclaimed.fetch_add(1, Ordering::Relaxed);
    }

    /// Flights claimed and not yet answered
    ///
    /// Walked rather than counted, since nothing on the op path reads the count.
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

    /// Completions put back rather than delivered, since the table was opened
    pub fn reclaimed(&self) -> u64 {
        self.reclaimed.load(Ordering::Relaxed)
    }

    /// Wait on a condition over the slots, draining the backend when nobody else is
    ///
    /// One thread polls the backend at a time and the rest park, so an empty
    /// drain means the backend is empty rather than busy elsewhere.
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

            // The budget is the backend's, so a completion another thread filed
            // while it ran still answers before this gives up on it.
            if polled? == 0 {
                return match ready() {
                    Some(out) => Ok(out),
                    None => Err(never_completed()),
                };
            }
        }
    }

    /// Park a thread until a slot comes free rather than until the backend answers
    ///
    /// A slot frees when its caller takes the completion, so draining cannot free
    /// one on its own; the drain is there for an orphan's slot, which frees where
    /// its completion lands. For the blocking door alone.
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
    ///
    /// The buffer comes back with it, kept across turns so a drain allocates
    /// nothing. One thread drains at a time, so an empty drain means an empty
    /// backend.
    pub fn begin_poll(&self) -> Option<Vec<Completion>> {
        let mut door = lock(&self.door);
        if door.is_polling {
            return None;
        }
        door.is_polling = true;
        Some(std::mem::take(&mut door.scratch))
    }

    /// Give the turn back, filing what the drain moved and ringing what it wakes
    pub fn end_poll(&self, mut drained: Vec<Completion>) {
        self.file(&mut drained);

        let mut door = lock(&self.door);
        door.is_polling = false;
        door.scratch = drained;
        // The turn coming back is what a thread parked for it is waiting on, and
        // nothing outside this gate can tell it.
        if self.parked.load(Ordering::SeqCst) > 0 {
            self.delivered.notify_all();
        }
    }

    /// File completions into the slots waiting for them and ring what they wake
    ///
    /// The door a ring backend deposits through, taking no turn because the
    /// reaping thread drained a queue of its own. The vector comes back empty.
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
    ///
    /// For the async door, where the thread a park would take is the one holding
    /// the futures whose completions free the slots this is waiting for.
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

    /// File one completion into its slot, answering the waker it has to ring
    ///
    /// A completion whose slot has moved on is one whose caller is gone. What
    /// comes back is the waker to ring and whether the slot came free.
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

    /// Take the drain turn if it is free, file what one poll moves, say so
    ///
    /// The awaited door's self-service, since the caller holding the future may
    /// be the only one polling. A turn already taken is not waited for.
    pub fn drain_once(&self, drain: impl FnOnce(&mut Vec<Completion>) -> Result<usize>) -> bool {
        // Refused, the future stays passive and whoever else drains must wake it.
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
    ///
    /// Flight with no completion means the backend never answered, Landed
    /// unclaimed means the future stopped polling, and no slot at all means the
    /// claim or the submit never happened.
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

    /// Ring what a change to a slot owes the callers waiting on the table
    ///
    /// The counts are read without taking either lock, and a count raised before
    /// the table was read is what keeps this from passing over a waiting caller.
    fn signal(&self, is_freed: bool) {
        if self.parked.load(Ordering::SeqCst) > 0 {
            let _door = lock(&self.door);
            self.delivered.notify_all();
        }
        if is_freed {
            self.ring_claims();
        }
    }

    /// Ring the claims a slot coming free owes a look at the table
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

    /// Leave a waker for a claim that found its slot carrying another flight
    ///
    /// One list for the whole table: an unwanted wake costs a look at the table,
    /// where a seat never taken would cost the wait.
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

    /// Ask the condition with this thread counted in, then park for a bounded time
    ///
    /// The count goes up before the table is read and down after the park, so a
    /// change between the two finds a thread to signal. The park is bounded
    /// because a notify landing between the ask and the wait would be missed.
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

/// Ring what a change to the table owed, once its gates have been given back
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

/// A contiguous run of tags one submission went down as
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TagRun {
    /// First tag of the run
    pub first: Tag,

    /// How many tags the run covers, each one past the last
    pub count: usize,
}

/// The runs a batch's tags fall into, which is one run when nothing interleaved
///
/// Tags come off the counter in order, so an uninterrupted batch is a single run
/// however many ops it carries.
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

/// A submission waiting for the slots its tags name, taken as a future
///
/// Whole or nothing, so two claims cannot each hold half of what they need. It
/// resolves before the ops go down, so a drop here has claimed nothing.
pub struct Claim<'table> {
    /// The table the tags name slots in
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

        // The seat goes down before the table is asked again, so a slot freed
        // between the two rings this claim rather than passing over it.
        waiter.table.seat_claim(cx.waker());
        if waiter.table.claim_all(waiter.wanted) {
            waiter.table.unseat_claim(cx.waker());
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

/// One op in flight, taken as a future rather than by parking a thread
///
/// The op owns its buffers until its completion returns them, so dropping this
/// abandons a slot and never a buffer.
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
    ///
    /// A future that already took its completion names a slot that moved on, so
    /// this is a lookup that finds nothing rather than a state to track.
    fn drop(&mut self) {
        self.table.orphan(self.tag);
    }
}

/// A batch in flight, one future over the runs of tags it went down as
///
/// The batch holds its tags as runs and counts what is outstanding, so a hundred
/// reads cost a range and a counter rather than a hundred futures.
pub struct BatchWait<'table> {
    /// The table holding the slots these flights own
    table: &'table SlotTable,

    /// The tag runs the batch went down as, in submit order
    runs: Vec<TagRun>,

    /// What each tag came back with, in submit order
    filled: Vec<Option<Completion>>,

    /// Completions still to land, which is when the batch answers
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

        // The runs go with the completions, so the drop of an answered batch has
        // no range left to orphan and no slot left to put back.
        let mut answered = Vec::with_capacity(waiter.filled.len());
        for completion in waiter.filled.drain(..).flatten() {
            answered.push(completion);
        }
        waiter.runs.clear();
        Poll::Ready(answered)
    }
}

impl Drop for BatchWait<'_> {
    /// A batch dropped while pending orphans its whole range
    ///
    /// The completions it already took are put back the same way an orphan's is,
    /// since they came out of the pool and the caller never saw them.
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

/// Put back what a completion nobody is waiting for is carrying
///
/// The read buffers are the pool's and go back to it. An open that lands
/// orphaned keeps its descriptor.
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

/// The claim protocol, checked by the model checker
///
/// A claim on the async door leaves a waker and no timeout, so a slot freed
/// without ringing the seat beside it is a future nobody wakes. Parked threads
/// are left out: the checker has no clock and takes a bounded park as open.
#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;

    use loom::sync::Arc;

    fn done(tag: Tag) -> Completion {
        Completion {
            tag,
            outcome: Outcome::Done(Ok(())),
        }
    }

    // no interleaving leaves a claim seated on a slot that came free
    #[test]
    fn a_free_never_passes_over_a_claim() {
        loom::model(|| {
            let table = Arc::new(SlotTable::new());
            let held = Tag(0);
            let wanted = [Tag(SLOT_COUNT as u64)];
            assert!(table.try_claim(held));
            table.file_one(done(held));

            let taking = {
                let table = Arc::clone(&table);
                loom::thread::spawn(move || {
                    table.take(held);
                })
            };

            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);
            let mut claiming = Box::pin(table.claim(&wanted));
            let is_taken = claiming.as_mut().poll(&mut cx).is_ready();

            taking.join().expect("the taking thread joins");

            // The slot is free by the join, so a seated claim is one nothing rings.
            assert!(
                is_taken || table.claiming.load(Ordering::SeqCst) == 0,
                "the free left a claim seated on a slot it could have had"
            );
        });
    }

    // a completion and the drop that gave up on it never both keep the buffer
    #[test]
    fn a_drop_and_a_landing_agree() {
        loom::model(|| {
            let table = Arc::new(SlotTable::new());
            let tag = Tag(0);
            assert!(table.try_claim(tag));

            let filing = {
                let table = Arc::clone(&table);
                loom::thread::spawn(move || {
                    table.file_one(done(tag));
                })
            };

            table.orphan(tag);
            filing.join().expect("the filing thread joins");

            assert_eq!(
                table.reclaimed(),
                1,
                "the completion was put back twice or not at all"
            );
            assert_eq!(table.outstanding(), 0, "the slot never came back");
        });
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::task::Wake;

    use crate::io::op::ReadBuf;

    /// A waker that counts what it takes, for a poll that must ring none
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

    // a completion for a flight nobody is waiting on is reclaimed, not filed
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

    // a claim that cannot take its whole run rings nobody, itself least of all
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

        // The take is what a claim waits for, and the one change that has to ring it.
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

    // the low bits of a tag are its slot, so the table wraps rather than grows
    #[test]
    fn a_tag_addresses_one_slot() {
        assert_eq!(index_of(Tag(0)), 0);
        assert_eq!(index_of(Tag(SLOT_COUNT as u64)), 0);
        assert_eq!(index_of(Tag(SLOT_COUNT as u64 + 3)), 3);
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
