//! What a planner step works out about a volume's load, once, and every maintenance decision as a plain function of it, so the
//! planner, each lane's pick and the walk's cap can't read the load two ways

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use reel_api::units::Nanoseconds;

use crate::engine::reserve_job::RESERVE_PERIOD;
use crate::engine::rewrite_job::{PLAIN_BAR, QUIET_BAR, ROOM_SHORT_BAR};

/// The facts a step decides by
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Load {
    /// Callers called within the quiet spell, or writes still arrive
    pub is_busy: bool,
    /// The volume fills faster than a merge runs, while writes arrive
    pub is_hot: bool,
    /// The segment table or the park is past half
    pub is_table_short: bool,
    /// The disk lacks the room writers keep back
    pub is_disk_short: bool,
}

/// What the summary owes, beside the load
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Owed {
    /// The walk has counted every segment and some bytes are dead
    pub is_reclaim_due: bool,
    /// A test holds reclaim off
    pub is_reclaim_held: bool,
}

/// How many lanes may take an overlap merge
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OverlapLanes {
    None,
    One,
    All,
}

/// Check hot ingest puts merges and record rewrites off: only while the table has room to spare
pub const fn is_deferring(load: Load) -> bool {
    load.is_hot && !load.is_table_short
}

/// Read how many lanes may take an overlap merge: none while merges wait for ingest or the disk is short, since an overlap merge
/// writes what it reads, one while callers are busy, and every idle lane once they go quiet
pub const fn overlap_lanes(load: Load) -> OverlapLanes {
    if is_deferring(load) || load.is_disk_short {
        OverlapLanes::None
    } else if load.is_busy {
        OverlapLanes::One
    } else {
        OverlapLanes::All
    }
}

/// Read the bar a record rewrite holds unmarked segments to: with reclaim due, none on a short disk whatever the callers do, the
/// quiet bar on a quiet volume, and the plain bar otherwise
pub const fn record_bar(load: Load, owed: Owed) -> u64 {
    if !owed.is_reclaim_due {
        PLAIN_BAR
    } else if load.is_disk_short && !owed.is_reclaim_held {
        ROOM_SHORT_BAR
    } else if load.is_busy || owed.is_reclaim_held {
        PLAIN_BAR
    } else {
        QUIET_BAR
    }
}

/// Check the merge cap and the walk cap hold, which they do while callers are busy
pub const fn is_capped(load: Load) -> bool {
    load.is_busy
}

/// Check the planner should look for a record rewrite with nothing asked: reclaim due on a quiet volume, or on a short disk
pub const fn is_reclaim_wanted(load: Load, owed: Owed) -> bool {
    owed.is_reclaim_due && !owed.is_reclaim_held && (!load.is_busy || load.is_disk_short)
}

/// Bits of a load in one byte
const BUSY: u8 = 1;
const HOT: u8 = 2;
const TABLE_SHORT: u8 = 4;
const DISK_SHORT: u8 = 8;

/// A volume's load as the planner's last step worked it out, which every lane's pick and the walk read
#[derive(Debug, Default)]
pub struct LoadCell {
    bits: AtomicU8,
    is_current: AtomicBool,
    quiet_at: AtomicU64,
    hot_until: AtomicU64,
}

impl LoadCell {
    /// Keep a step's load and the times it ends at
    pub fn set(&self, load: Load, quiet_at: Nanoseconds, hot_until: Nanoseconds) {
        let bit = |is_set: bool, bit: u8| if is_set { bit } else { 0 };
        let bits = bit(load.is_busy, BUSY) | bit(load.is_hot, HOT) | bit(load.is_table_short, TABLE_SHORT) | bit(load.is_disk_short, DISK_SHORT);
        self.quiet_at.store(quiet_at.0, Ordering::Relaxed);
        self.hot_until.store(hot_until.0, Ordering::Relaxed);
        self.bits.store(bits, Ordering::Release);
    }

    /// Mark the load current for the planner step under way, or no longer
    pub fn set_current(&self, is_current: bool) {
        self.is_current.store(is_current, Ordering::Release);
    }

    /// Check a planner step is under way with this load worked out
    pub fn is_current(&self) -> bool {
        self.is_current.load(Ordering::Acquire)
    }

    /// Read the last step's load
    pub fn get(&self) -> Load {
        let bits = self.bits.load(Ordering::Acquire);
        Load {
            is_busy: bits & BUSY != 0,
            is_hot: bits & HOT != 0,
            is_table_short: bits & TABLE_SHORT != 0,
            is_disk_short: bits & DISK_SHORT != 0,
        }
    }

    /// Read when a quiet spell could begin
    pub fn quiet_at(&self) -> Nanoseconds {
        Nanoseconds(self.quiet_at.load(Ordering::Relaxed))
    }

    /// Read when the hot window closes
    pub fn hot_until(&self) -> Nanoseconds {
        Nanoseconds(self.hot_until.load(Ordering::Relaxed))
    }
}

/// What ended a pick that started nothing
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Refusal {
    /// Refused for disk room, park room or a stray output
    Refused,
    /// Put off for hot ingest or the merge cap
    Deferred,
    /// Behind an overlap merge owed
    Behind,
}

/// Read when a refused pick tries again: a reserve period after a refusal for room, the park or a stray output, when the hot
/// window closes for one put off by ingest, and when a quiet spell could begin for one held by the merge cap. A pick behind an
/// overlap merge waits for nothing: the overlap owed brings the next step back, and the merge that clears it ends the wait
pub fn refusal_wait(refusal: Refusal, load: Load, now: Nanoseconds, quiet_at: Nanoseconds, hot_until: Nanoseconds) -> Option<Nanoseconds> {
    let at = match refusal {
        Refusal::Behind => return None,
        Refusal::Refused => now.0 + RESERVE_PERIOD.0,
        Refusal::Deferred if is_deferring(load) => hot_until.0,
        Refusal::Deferred => quiet_at.0,
    };
    Some(Nanoseconds(at.max(now.0 + 1)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every load a step can work out
    fn every_load() -> impl Iterator<Item = Load> {
        (0..16u8).map(|bits| Load {
            is_busy: bits & BUSY != 0,
            is_hot: bits & HOT != 0,
            is_table_short: bits & TABLE_SHORT != 0,
            is_disk_short: bits & DISK_SHORT != 0,
        })
    }

    /// Everything the summary can owe
    fn every_owed() -> impl Iterator<Item = Owed> {
        (0..4u8).map(|bits| Owed { is_reclaim_due: bits & 1 != 0, is_reclaim_held: bits & 2 != 0 })
    }

    #[test]
    fn decisions_follow_the_load_table() {
        for load in every_load() {
            let is_deferred = load.is_hot && !load.is_table_short;
            assert_eq!(is_deferring(load), is_deferred, "{load:?}");
            assert_eq!(is_capped(load), load.is_busy, "{load:?}");
            let lanes = match (is_deferred || load.is_disk_short, load.is_busy) {
                (true, _) => OverlapLanes::None,
                (false, true) => OverlapLanes::One,
                (false, false) => OverlapLanes::All,
            };
            assert_eq!(overlap_lanes(load), lanes, "{load:?}");
            for owed in every_owed() {
                let bar = match (owed.is_reclaim_due, owed.is_reclaim_held, load.is_disk_short, load.is_busy) {
                    (false, ..) => PLAIN_BAR,
                    (true, false, true, _) => ROOM_SHORT_BAR,
                    (true, true, ..) | (true, false, false, true) => PLAIN_BAR,
                    (true, false, false, false) => QUIET_BAR,
                };
                assert_eq!(record_bar(load, owed), bar, "{load:?} {owed:?}");
                let is_wanted = owed.is_reclaim_due && !owed.is_reclaim_held && (!load.is_busy || load.is_disk_short);
                assert_eq!(is_reclaim_wanted(load, owed), is_wanted, "{load:?} {owed:?}");
                // a wanted reclaim always gets a bar below the plain one, so it can pick a segment the plain bar would pass over
                if is_wanted {
                    assert!(record_bar(load, owed) < PLAIN_BAR, "{load:?} {owed:?}");
                }
            }
        }
    }

    #[test]
    fn load_cell_keeps_every_load() {
        let cell = LoadCell::default();
        for load in every_load() {
            cell.set(load, Nanoseconds(7), Nanoseconds(9));
            assert_eq!(cell.get(), load);
            assert_eq!((cell.quiet_at(), cell.hot_until()), (Nanoseconds(7), Nanoseconds(9)));
        }
    }

    #[test]
    fn refused_picks_wait_until_the_load_changes() {
        let now = Nanoseconds(1_000);
        let (quiet_at, hot_until) = (Nanoseconds(5_000), Nanoseconds(9_000));
        for load in every_load() {
            let after_room = refusal_wait(Refusal::Refused, load, now, quiet_at, hot_until);
            assert_eq!(after_room, Some(Nanoseconds(now.0 + RESERVE_PERIOD.0)));
            assert_eq!(refusal_wait(Refusal::Behind, load, now, quiet_at, hot_until), None);
            let deferred = refusal_wait(Refusal::Deferred, load, now, quiet_at, hot_until);
            assert_eq!(deferred, Some(if is_deferring(load) { hot_until } else { quiet_at }), "{load:?}");
            // a time already past still waits a tick, so a refused pick never spins
            assert_eq!(refusal_wait(Refusal::Deferred, load, now, Nanoseconds(0), Nanoseconds(0)), Some(Nanoseconds(now.0 + 1)));
        }
    }
}
