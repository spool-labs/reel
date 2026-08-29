//! Named rendezvous points, so a race is a test rather than a lottery
//!
//! One script runs at a time, and arming takes a global turn, so choreographed
//! tests serialise against each other while everything else pays one load. A
//! script that panics or finishes releases everyone it held.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, OnceLock};
use std::thread::{self, JoinHandle, ThreadId};
use std::time::{Duration, Instant};

use crate::sync::lock;

/// How long a script waits for an arrival before failing the test instead
///
/// A rendezvous that never happens must fail loudly, not hang the suite.
const STALL: Duration = Duration::from_secs(10);

/// How long a parked arrival waits before proceeding as though unheld
///
/// The gates are process global, so a held point parks bystanders belonging to
/// tests the script never met. Three stalls is longer than any live script can
/// hold a thread, since every scripted wait is bounded by one.
const PARKED: Duration = Duration::from_secs(30);

/// Whether any script is armed, the one load every site pays
static ARMED: AtomicBool = AtomicBool::new(false);

/// Who is gated and who has arrived, while a script runs
struct Stage {
    /// Points a script refuses outright: an arrival returns without acting
    refused: HashSet<&'static str>,

    /// Gated points, each with the arrivals it will still let through
    gates: HashMap<&'static str, u64>,

    /// How many times each point has been reached since the script armed
    reached: HashMap<&'static str, Arrivals>,

    /// The threads the script owns, empty while it speaks for everyone
    cast: HashSet<ThreadId>,

    /// The thread that armed the script, which its refusals are scoped to
    owner: Option<ThreadId>,

    /// Which script the stage belongs to, so a late arrival cannot join the next
    run: u64,
}

/// Arrivals at one point, split by who made them
#[derive(Default)]
struct Arrivals {
    /// Every arrival, whoever the thread belonged to
    all: u64,

    /// Arrivals made by a thread the script cast
    cast: u64,
}

impl Stage {
    /// Whether a cast script has narrowed past the arriving thread
    fn bystander(&self, who: ThreadId) -> bool {
        !self.cast.is_empty() && !self.cast.contains(&who)
    }

    /// Whether the thread is one the script itself runs on
    ///
    /// A gate holds whoever reaches it, but a refusal takes work away from the
    /// thread that meets it, and a thread of some other test cannot be asked to
    /// go without work it is waiting on.
    fn owns(&self, who: ThreadId) -> bool {
        self.owner == Some(who) || self.cast.contains(&who)
    }

    /// Arrivals the script speaks for: its cast's, or everyone's when it has none
    fn count(&self, name: &'static str) -> u64 {
        let Some(arrivals) = self.reached.get(name) else {
            return 0;
        };
        match self.cast.is_empty() {
            true => arrivals.all,
            false => arrivals.cast,
        }
    }
}

fn stage() -> &'static (Mutex<Stage>, Condvar) {
    static STAGE: OnceLock<(Mutex<Stage>, Condvar)> = OnceLock::new();
    STAGE.get_or_init(|| {
        (
            Mutex::new(Stage {
                refused: HashSet::new(),
                gates: HashMap::new(),
                reached: HashMap::new(),
                cast: HashSet::new(),
                owner: None,
                run: 0,
            }),
            Condvar::new(),
        )
    })
}

/// Whether a script has refused the point, for a site guarding optional work
///
/// Only for the script's own threads: the refused work is what a caller elsewhere
/// in the suite is waiting on, and taking it away wedges that caller for good.
/// One load while nothing is armed.
#[inline]
pub fn refused(name: &'static str) -> bool {
    if !ARMED.load(Ordering::Acquire) {
        return false;
    }
    let (mutex, _) = stage();
    let stage = lock(mutex);
    stage.owns(thread::current().id()) && stage.refused.contains(name)
}

/// Mark a named moment, parking here while a script gates it
///
/// The arrival is counted before any wait, so a script watching for it sees the
/// thread while it stands parked.
#[inline]
pub fn at(name: &'static str) {
    if !ARMED.load(Ordering::Acquire) {
        return;
    }
    arrive(name);
}

/// The armed half, out of line so the unarmed check inlines to a load and a branch
#[cold]
fn arrive(name: &'static str) {
    let me = thread::current().id();
    let (mutex, condvar) = stage();
    let mut stage = lock(mutex);
    if stage.bystander(me) {
        return;
    }
    let named = !stage.cast.is_empty();
    let arrivals = stage.reached.entry(name).or_default();
    arrivals.all += 1;
    if named {
        arrivals.cast += 1;
    }
    condvar.notify_all();
    let deadline = Instant::now() + PARKED;
    loop {
        // a script that cast its threads after this one parked no longer holds it
        if stage.bystander(me) {
            return;
        }
        match stage.gates.get_mut(name) {
            None => return,
            Some(permits) if *permits > 0 => {
                *permits -= 1;
                return;
            }
            Some(_) => {
                let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                    eprintln!(
                        "a thread parked at rendezvous point {name} for {PARKED:?} goes on unheld"
                    );
                    return;
                };
                let (guard, _) = condvar
                    .wait_timeout(stage, left)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                stage = guard;
            }
        }
    }
}

/// One choreographed interleaving, exclusive while it lives
///
/// Dropping it disarms the sites, clears every hold, and wakes whoever was
/// parked, so the threads a failing test held are released rather than wedged.
pub struct Script {
    _turn: MutexGuard<'static, ()>,
}

/// Arm the sites and take the stage, one script at a time
pub fn script() -> Script {
    static TURN: OnceLock<Mutex<()>> = OnceLock::new();
    let turn = lock(TURN.get_or_init(|| Mutex::new(())));
    let (mutex, _) = stage();
    {
        let mut stage = lock(mutex);
        stage.refused.clear();
        stage.gates.clear();
        stage.reached.clear();
        stage.cast.clear();
        stage.owner = Some(thread::current().id());
        stage.run += 1;
    }
    ARMED.store(true, Ordering::Release);
    Script { _turn: turn }
}

impl Script {
    /// Spawn a thread of this script's own, and narrow every point to its cast
    ///
    /// From the first cast on, a thread the script did not name crosses its points
    /// uncounted and unparked, engine threads included. The narrowing lands before
    /// the spawn, so a neighbour already parked at the hold leaves on this wake.
    pub fn cast<T: Send + 'static>(
        &self,
        body: impl FnOnce() -> T + Send + 'static,
    ) -> JoinHandle<T> {
        let (mutex, condvar) = stage();
        let run = {
            let mut stage = lock(mutex);
            stage.cast.insert(thread::current().id());
            stage.run
        };
        condvar.notify_all();
        thread::spawn(move || {
            {
                let (mutex, _) = stage();
                let mut stage = lock(mutex);
                // a thread outliving its script must not enlist in whatever armed next
                if stage.run == run {
                    stage.cast.insert(thread::current().id());
                }
            }
            body()
        })
    }

    /// Refuse the point outright on this script's threads: they return without acting
    pub fn refuse(&self, name: &'static str) {
        let (mutex, _) = stage();
        lock(mutex).refused.insert(name);
    }

    /// Park whoever reaches the point, until passed one by one or released
    pub fn hold(&self, name: &'static str) {
        let (mutex, _) = stage();
        lock(mutex).gates.insert(name, 0);
    }

    /// Let exactly one arrival through a held point, parking the one after
    pub fn pass_one(&self, name: &'static str) {
        let (mutex, condvar) = stage();
        if let Some(permits) = lock(mutex).gates.get_mut(name) {
            *permits += 1;
        }
        condvar.notify_all();
    }

    /// Take the gate down: everyone parked goes, later arrivals pass through
    pub fn release(&self, name: &'static str) {
        let (mutex, condvar) = stage();
        lock(mutex).gates.remove(name);
        condvar.notify_all();
    }

    /// Wait until the point has been reached this many times in all
    ///
    /// Bounded, and the bound failing is the test failing: an arrival that cannot
    /// happen is an interleaving the engine no longer has.
    pub fn await_reached(&self, name: &'static str, count: u64) {
        let (mutex, condvar) = stage();
        let deadline = Instant::now() + STALL;
        let mut stage = lock(mutex);
        while stage.count(name) < count {
            let left = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_else(|| {
                    panic!("nobody reached rendezvous point {name} within {STALL:?}")
                });
            let (guard, _) = condvar
                .wait_timeout(stage, left)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            stage = guard;
        }
    }

    /// How many times the point has been reached since the script armed
    pub fn reached(&self, name: &'static str) -> u64 {
        let (mutex, _) = stage();
        lock(mutex).count(name)
    }
}

impl Drop for Script {
    fn drop(&mut self) {
        ARMED.store(false, Ordering::Release);
        let (mutex, condvar) = stage();
        let mut stage = lock(mutex);
        stage.refused.clear();
        stage.gates.clear();
        stage.reached.clear();
        stage.cast.clear();
        stage.owner = None;
        condvar.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // a refusal reaches the script's own threads and nobody else's
    #[test]
    fn a_refusal_spares_a_bystander() {
        let script = script();
        script.refuse("test/refusal");

        // Asked before the script casts anything, which is the window a refusal
        // used to speak for every thread in the process through.
        let bystander = thread::spawn(|| refused("test/refusal"))
            .join()
            .expect("the bystander joins");
        let cast = script
            .cast(|| refused("test/refusal"))
            .join()
            .expect("the cast thread joins");

        assert!(
            refused("test/refusal"),
            "the script's own thread was spared"
        );
        assert!(cast, "a thread the script cast was spared");
        assert!(!bystander, "the refusal reached a thread of another test");
    }
}
