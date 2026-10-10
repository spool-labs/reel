//! Rendezvous points, so a test can script a race between threads

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, OnceLock};
use std::thread::{self, JoinHandle, ThreadId};
use std::time::{Duration, Instant};

use crate::sync::lock;

/// A script fails the test if an arrival takes longer than this
const STALL: Duration = Duration::from_secs(10);

/// A parked arrival goes on as though unheld after this long
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

/// Whether a script has refused the point on its own threads, for a site guarding optional work
#[inline]
pub fn refused(name: &'static str) -> bool {
    if !ARMED.load(Ordering::Acquire) {
        return false;
    }
    let (mutex, _) = stage();
    let stage = lock(mutex);
    stage.owns(thread::current().id()) && stage.refused.contains(name)
}

/// Mark a rendezvous point, counting the arrival and parking here while a script gates it
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

/// One choreographed interleaving, exclusive while it lives, which frees every hold on drop
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

    /// Wait until the point has been reached this many times in all, failing the test past `STALL`
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

        // Asked before the script casts anything
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
