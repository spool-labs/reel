//! Which tail each live death window is writing into
//!
//! Bands do not add tails. The pool is the one the configuration already sized, and a
//! band claims one of its tails for as long as it is being written to. One tail is
//! always left unclaimed, so traffic in no band never lands in a banded segment; a band
//! that finds nothing free writes there too rather than stalling. A floor that has
//! passed a band takes its tail back, so nobody has to hand one in.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::RwLock;

use crate::append::Appender;
use crate::error::Result;
use crate::format::band::Band;
use crate::sync::{read, write};

/// The band each foreground tail is drawing under, and what decides which one moves
pub struct BandPool {
    /// One slot per foreground tail, empty where the tail takes unbanded traffic
    owner: RwLock<Vec<Option<Band>>>,

    /// Bumped on every banded write, so a tail can be ranked by when it last took one
    tick: AtomicU64,

    /// The tick at each tail's last banded write, zero where it has taken none
    used: Vec<AtomicU64>,

    /// Tails on a band right now, read before the lock so unbanded traffic on a volume
    /// nothing bands pays nothing at all
    claimed: AtomicUsize,

    /// The tick at which a tail last changed hands, which is the bar a band has to
    /// have gone quiet since to lose its own
    handed_over: AtomicU64,

    /// Highest floor the finished bands have been retired against, so a floor that has
    /// not moved costs one relaxed load rather than the table
    swept: AtomicU64,

    /// Banded writes that found no tail and went where unbanded traffic goes
    fell_back: AtomicU64,
}

/// What a claim came back with
enum Claim {
    /// The tail now drawing under the band
    Tail(usize),

    /// No tail could be taken, so the write goes where unbanded traffic goes
    Unbanded,
}

impl BandPool {
    /// A pool over this many foreground tails, none of them banded yet
    pub fn new(tails: usize) -> BandPool {
        BandPool {
            owner: RwLock::new(vec![None; tails]),
            tick: AtomicU64::new(1),
            used: (0..tails).map(|_| AtomicU64::new(0)).collect(),
            claimed: AtomicUsize::new(0),
            handed_over: AtomicU64::new(0),
            swept: AtomicU64::new(0),
            fell_back: AtomicU64::new(0),
        }
    }

    /// Whether any tail is on a band, which is the question the write path asks first
    pub fn is_idle(&self) -> bool {
        self.claimed.load(Ordering::Relaxed) == 0
    }

    /// The band each tail is drawing under, for a caller reporting on placement
    pub fn owners(&self) -> Vec<Option<Band>> {
        read(&self.owner).clone()
    }

    /// Banded writes that found no tail free and went to the unbanded ones instead
    pub fn fallbacks(&self) -> u64 {
        self.fell_back.load(Ordering::Relaxed)
    }

    /// The tail this write goes to, claiming one for the band where it has none yet
    ///
    /// A band already on a tail is a read of the table and nothing else. Everything
    /// costly is on the claim: it seals the tail it takes, so the band that follows
    /// starts on a segment of its own.
    pub fn place(&self, foreground: &[Appender], band: Option<Band>, floor: u64) -> Result<usize> {
        self.retire_finished(foreground, floor)?;
        // A band the floor has passed holds nothing alive, so it is not worth a tail.
        let Some(band) = band.filter(|band| !band.is_finished(floor)) else {
            return Ok(self.unbanded(foreground));
        };
        if let Some(at) = self.holder(band) {
            return Ok(at);
        }
        match self.claim(foreground, band)? {
            Claim::Tail(at) => Ok(at),
            Claim::Unbanded => {
                self.fell_back.fetch_add(1, Ordering::Relaxed);
                Ok(self.unbanded(foreground))
            }
        }
    }

    /// Give back every tail holding a window the floor has passed, once per floor move
    fn retire_finished(&self, foreground: &[Appender], floor: u64) -> Result<()> {
        if self.is_idle() || self.swept.load(Ordering::Relaxed) >= floor {
            return Ok(());
        }
        let mut owner = write(&self.owner);
        for at in 0..owner.len() {
            match owner[at] {
                Some(band) if band.is_finished(floor) => {}
                Some(_) | None => continue,
            }
            foreground[at].set_band(None)?;
            owner[at] = None;
            self.claimed.fetch_sub(1, Ordering::Relaxed);
        }
        self.swept.fetch_max(floor, Ordering::Relaxed);
        Ok(())
    }

    /// The tail a band is already drawing under, counted as a use of it
    fn holder(&self, band: Band) -> Option<usize> {
        let owner = read(&self.owner);
        let at = owner.iter().position(|held| *held == Some(band))?;
        self.note_used(at);
        Some(at)
    }

    /// The least loaded tail nothing has claimed, or the least loaded of them all
    ///
    /// The fallback is only reachable on a pool with no free tail left, which the
    /// claim rule does not leave behind.
    fn unbanded(&self, foreground: &[Appender]) -> usize {
        if self.is_idle() {
            return least_loaded(foreground, |_| true);
        }
        let owner = read(&self.owner);
        match least_loaded_free(foreground, &owner) {
            Some(at) => at,
            None => least_loaded(foreground, |_| true),
        }
    }

    /// Take a tail for a band, sealing what it holds so the band starts clean
    fn claim(&self, foreground: &[Appender], band: Band) -> Result<Claim> {
        let mut owner = write(&self.owner);
        // Another writer may have claimed the same band while this one waited.
        if let Some(at) = owner.iter().position(|held| *held == Some(band)) {
            self.note_used(at);
            return Ok(Claim::Tail(at));
        }
        let free = owner.iter().filter(|held| held.is_none()).count();
        // One tail stays unclaimed however many bands are live, or unbanded traffic
        // would have to be mixed into somebody's window.
        let taken = match free > 1 {
            true => least_loaded_free(foreground, &owner),
            false => self.stalest(&owner),
        };
        let Some(at) = taken else {
            return Ok(Claim::Unbanded);
        };
        foreground[at].set_band(Some(band))?;
        if owner[at].is_none() {
            self.claimed.fetch_add(1, Ordering::Relaxed);
        }
        owner[at] = Some(band);
        // The bar is this claim's own tick, so the band that just took the tail is not
        // the stalest thing in the pool a moment later.
        self.handed_over
            .store(self.note_used(at), Ordering::Relaxed);
        Ok(Claim::Tail(at))
    }

    /// The banded tail that has written nothing since a tail last changed hands
    ///
    /// The whole of the pool's rotation on a volume that never moves its floor.
    fn stalest(&self, owner: &[Option<Band>]) -> Option<usize> {
        let bar = self.handed_over.load(Ordering::Relaxed);
        let mut chosen = None;
        let mut oldest = u64::MAX;
        for (at, held) in owner.iter().enumerate() {
            if held.is_none() {
                continue;
            }
            let used = self.used[at].load(Ordering::Relaxed);
            if used < oldest && used < bar {
                oldest = used;
                chosen = Some(at);
            }
        }
        chosen
    }

    /// Mark a tail as having taken a banded write just now, at the tick it took it
    fn note_used(&self, at: usize) -> u64 {
        let now = self.tick.fetch_add(1, Ordering::Relaxed);
        self.used[at].store(now, Ordering::Relaxed);
        now
    }
}

/// The least loaded tail among those the test admits
fn least_loaded(foreground: &[Appender], admits: impl Fn(usize) -> bool) -> usize {
    let mut chosen = 0;
    let mut lowest = u64::MAX;
    for (at, tail) in foreground.iter().enumerate() {
        if !admits(at) {
            continue;
        }
        let load = tail.load();
        if load < lowest {
            lowest = load;
            chosen = at;
        }
    }
    chosen
}

/// The least loaded tail no band has claimed, or nothing where none is free
fn least_loaded_free(foreground: &[Appender], owner: &[Option<Band>]) -> Option<usize> {
    let free = |at: usize| owner.get(at).is_some_and(Option::is_none);
    (0..foreground.len())
        .any(free)
        .then(|| least_loaded(foreground, free))
}
