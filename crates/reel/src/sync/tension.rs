//! Waits a caller may take either way, blocking or as a future

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::task::{Context, Poll, Waker};

use crate::sync::{lock, wait};

/// A waitlist that parks threads and futures on one condition
pub struct Tension<State> {
    /// The state every waiter reads, and the seats the futures left
    gate: Mutex<Held<State>>,

    /// Signalled whenever the state changes, for the threads parked on it
    slackened: Condvar,

    /// Threads parked plus futures seated, read without the gate by a releaser
    waiting: AtomicU64,
}

/// What the gate holds: the state waiters read, and the seats futures left
struct Held<State> {
    /// Whatever the waiters are reading
    state: State,

    /// One seat per future currently pending
    seats: Vec<Seat>,

    /// The next seat's ticket, so a drop finds its own seat and no other
    next_ticket: u64,
}

/// One future's place on the waitlist
struct Seat {
    ticket: u64,
    waker: Waker,
}

impl<State> Tension<State> {
    /// A waitlist over some state, with nothing waiting on it
    pub fn new(state: State) -> Tension<State> {
        Tension {
            gate: Mutex::new(Held {
                state,
                seats: Vec::new(),
                next_ticket: 0,
            }),
            slackened: Condvar::new(),
            waiting: AtomicU64::new(0),
        }
    }

    /// Whether anything is waiting, the question a release asks before it locks
    pub fn is_taut(&self) -> bool {
        self.waiting.load(Ordering::SeqCst) != 0
    }

    /// Read or change the state under the gate, waking nothing
    pub fn with<Out>(&self, act: impl FnOnce(&mut State) -> Out) -> Out {
        let mut held = lock(&self.gate);
        act(&mut held.state)
    }

    /// Block the calling thread until the condition yields
    pub fn park<Out>(&self, mut ready: impl FnMut(&mut State) -> Option<Out>) -> Out {
        let mut held = lock(&self.gate);
        if let Some(out) = ready(&mut held.state) {
            return out;
        }
        self.waiting.fetch_add(1, Ordering::SeqCst);
        loop {
            if let Some(out) = ready(&mut held.state) {
                self.waiting.fetch_sub(1, Ordering::SeqCst);
                return out;
            }
            held = wait(&self.slackened, held);
        }
    }

    /// The same wait as a future, for a caller with a worker worth keeping
    pub fn wait<Out, Ready>(&self, ready: Ready) -> Wait<'_, State, Ready>
    where
        Ready: FnMut(&mut State) -> Option<Out>,
    {
        Wait {
            tension: self,
            ready,
            ticket: None,
            is_spent: false,
        }
    }

    /// Wake everything waiting, since one waiter's condition is not another's
    pub fn slack(&self) {
        // The count is SeqCst, so either the waiter or this releaser sees the other's change
        if !self.is_taut() {
            return;
        }
        let woken = {
            let mut held = lock(&self.gate);
            self.slackened.notify_all();
            self.vacate_all(&mut held)
        };
        for waker in woken {
            waker.wake();
        }
    }

    /// Change the state under the gate and wake everything waiting on it
    pub fn slack_with<Out>(&self, act: impl FnOnce(&mut State) -> Out) -> Out {
        let (out, woken) = {
            let mut held = lock(&self.gate);
            let out = act(&mut held.state);
            self.slackened.notify_all();
            (out, self.vacate_all(&mut held))
        };
        for waker in woken {
            waker.wake();
        }
        out
    }

    /// Take every seat off the list, leaving the wakers to be rung outside the gate
    fn vacate_all(&self, held: &mut Held<State>) -> Vec<Waker> {
        if held.seats.is_empty() {
            return Vec::new();
        }
        self.waiting
            .fetch_sub(held.seats.len() as u64, Ordering::SeqCst);
        held.seats.drain(..).map(|seat| seat.waker).collect()
    }
}

/// A wait that has not been taken yet, polled by whoever holds it
pub struct Wait<'tension, State, Ready> {
    /// The waitlist this seat is on
    tension: &'tension Tension<State>,

    /// The condition, asked under the gate
    ready: Ready,

    /// The seat on the waitlist, absent until a pending poll leaves one
    ticket: Option<u64>,

    /// Whether the condition has answered, after which it is never asked again
    is_spent: bool,
}

impl<State, Ready> Wait<'_, State, Ready> {
    /// Leave a seat, or refresh the waker in the one already left
    fn take_seat(&mut self, held: &mut Held<State>, waker: &Waker) {
        if let Some(ticket) = self.ticket {
            if let Some(seat) = held.seats.iter_mut().find(|seat| seat.ticket == ticket) {
                if !seat.waker.will_wake(waker) {
                    seat.waker = waker.clone();
                }
                return;
            }
        }
        let ticket = held.next_ticket;
        held.next_ticket += 1;
        held.seats.push(Seat {
            ticket,
            waker: waker.clone(),
        });
        self.ticket = Some(ticket);
        self.tension.waiting.fetch_add(1, Ordering::SeqCst);
    }

    /// Give up the seat, if a wake has not already taken it
    fn vacate(&mut self, held: &mut Held<State>) {
        let Some(ticket) = self.ticket.take() else {
            return;
        };
        if let Some(at) = held.seats.iter().position(|seat| seat.ticket == ticket) {
            held.seats.remove(at);
            self.tension.waiting.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Give up the seat and spend the wait, for a condition that has answered
    fn finish(&mut self, held: &mut Held<State>) {
        self.vacate(held);
        self.is_spent = true;
    }
}

impl<State, Ready, Out> Future for Wait<'_, State, Ready>
where
    Ready: FnMut(&mut State) -> Option<Out> + Unpin,
    State: Unpin,
{
    type Output = Out;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Out> {
        let waiter = self.get_mut();
        if waiter.is_spent {
            return Poll::Pending;
        }
        let mut held = lock(&waiter.tension.gate);
        // asked once before any seat goes down, so a ready condition costs no seat
        if let Some(out) = (waiter.ready)(&mut held.state) {
            waiter.finish(&mut held);
            return Poll::Ready(out);
        }
        // and again with the seat down, so a slack between the two finds a waiter
        waiter.take_seat(&mut held, cx.waker());
        match (waiter.ready)(&mut held.state) {
            Some(out) => {
                waiter.finish(&mut held);
                Poll::Ready(out)
            }
            None => Poll::Pending,
        }
    }
}

impl<State, Ready> Drop for Wait<'_, State, Ready> {
    fn drop(&mut self) {
        if self.ticket.is_none() {
            return;
        }
        let mut held = lock(&self.tension.gate);
        self.vacate(&mut held);
    }
}

/// Drive one future to its answer on the calling thread, for tests with no runtime
pub fn block_on<Answered: Future>(future: Answered) -> Answered::Output {
    use std::sync::Arc;
    use std::task::Wake;
    use std::thread::{self, Thread};

    struct Unparker(Thread);

    impl Wake for Unparker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let mut future = Box::pin(future);
    let waker = Waker::from(Arc::new(Unparker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(answer) = future.as_mut().poll(&mut cx) {
            return answer;
        }
        thread::park();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering as StdOrdering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    // a condition that already holds answers without leaving a seat behind
    #[test]
    fn ready_condition_never_waits() {
        let tension = Tension::new(7u64);

        let seen = block_on(tension.wait(|state| Some(*state)));

        assert_eq!(seen, 7);
        assert!(!tension.is_taut());
    }

    // a future left pending resolves when a slack changes what it reads
    #[test]
    fn future_resolves_on_slack() {
        let tension = Arc::new(Tension::new(false));

        let opener = Arc::clone(&tension);
        let open = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            opener.slack_with(|state| *state = true);
        });

        block_on(tension.wait(|state| state.then_some(())));
        open.join().expect("opener joins");

        assert!(!tension.is_taut());
    }

    // a parked thread and a seated future wait on the same condition and both wake
    #[test]
    fn park_and_wait_wake_together() {
        let tension = Arc::new(Tension::new(false));
        let woken = Arc::new(AtomicUsize::new(0));

        let mut waiters = Vec::new();
        for _ in 0..2 {
            let parked = Arc::clone(&tension);
            let count = Arc::clone(&woken);
            waiters.push(thread::spawn(move || {
                parked.park(|state| state.then_some(()));
                count.fetch_add(1, StdOrdering::SeqCst);
            }));
        }
        for _ in 0..2 {
            let seated = Arc::clone(&tension);
            let count = Arc::clone(&woken);
            waiters.push(thread::spawn(move || {
                block_on(seated.wait(|state| state.then_some(())));
                count.fetch_add(1, StdOrdering::SeqCst);
            }));
        }

        thread::sleep(Duration::from_millis(50));
        assert_eq!(woken.load(StdOrdering::SeqCst), 0);
        assert!(tension.is_taut());

        tension.slack_with(|state| *state = true);
        for waiter in waiters {
            waiter.join().expect("waiter joins");
        }

        assert_eq!(woken.load(StdOrdering::SeqCst), 4);
        assert!(!tension.is_taut());
    }

    // a future dropped while pending takes its seat with it
    #[test]
    fn dropped_wait_leaves_no_seat() {
        let tension = Tension::new(false);

        {
            let mut pending = Box::pin(tension.wait(|state: &mut bool| state.then_some(())));
            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);
            assert!(pending.as_mut().poll(&mut cx).is_pending());
            assert!(tension.is_taut());
        }

        assert!(!tension.is_taut());
        tension.with(|state| assert!(!*state));
    }

    // a future polled again after a wake it did not answer keeps exactly one seat
    #[test]
    fn repolled_wait_holds_one_seat() {
        let tension = Tension::new(0u64);
        let mut pending = Box::pin(tension.wait(|state: &mut u64| (*state >= 2).then_some(*state)));
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        assert!(pending.as_mut().poll(&mut cx).is_pending());
        tension.slack_with(|state| *state = 1);
        assert!(pending.as_mut().poll(&mut cx).is_pending());
        assert!(tension.is_taut());

        tension.slack_with(|state| *state = 2);
        assert!(pending.as_mut().poll(&mut cx).is_ready());
        assert!(!tension.is_taut());
    }

    // the condition may take the state it waits on, which is what a permit is
    #[test]
    fn wait_takes_from_the_state() {
        let tension = Arc::new(Tension::new(Vec::<u64>::new()));

        let filler = Arc::clone(&tension);
        let fill = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            filler.slack_with(|queue| queue.push(9));
        });

        let taken = block_on(tension.wait(|queue: &mut Vec<u64>| queue.pop()));
        fill.join().expect("filler joins");

        assert_eq!(taken, 9);
        tension.with(|queue| assert!(queue.is_empty()));
    }
}
