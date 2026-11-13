//! What a planner step works out about a volume's load, once, and every maintenance decision as a plain function of it, so the
//! planner, each lane's pick and the walk's cap can't read the load two ways

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64, Ordering};

use reel_api::units::Nanoseconds;

use crate::engine::reserve_job::RESERVE_PERIOD;
use crate::engine::rewrite_job::{PLAIN_BAR, QUIET_BAR, ROOM_SHORT_BAR};

/// The facts a step decides by
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Load {
    /// Callers called within the quiet spell, or writes still arrive
    pub is_busy: bool,
    /// Writes still arrive: the last seal started within two fill times
    pub is_ingesting: bool,
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

/// Check an ask waits for arriving writes to stop before it comes due again: a pick under ingest finds segments whose rows are
/// still dying, and the ask comes back once they settle. A short table or disk takes what it can at once
pub const fn is_holding_asks(load: Load) -> bool {
    load.is_ingesting && !load.is_table_short && !load.is_disk_short
}

/// Check hot ingest puts merges and record rewrites off: only while the table and the disk have room to spare, since a short disk
/// that waited out the ingest would run out of room with its dead rows still on it
pub const fn is_deferring(load: Load) -> bool {
    load.is_hot && !load.is_table_short && !load.is_disk_short
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

/// Read the bar a record rewrite holds unmarked segments to: with reclaim due, none on a short disk whatever the callers do, and
/// the floor otherwise, for busy callers and quiet ones alike. Busy callers' rewrites under half dead spend the pace's credit,
/// which holds them back. Under ingest a segment is always unwalked, so reclaim isn't due, and the pick gives trusted rows the
/// floor by its own reading
pub const fn record_bar(load: Load, owed: Owed) -> u64 {
    if !owed.is_reclaim_due {
        PLAIN_BAR
    } else if load.is_disk_short && !owed.is_reclaim_held {
        ROOM_SHORT_BAR
    } else if owed.is_reclaim_held {
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

/// The pace's multiplier, copy bytes for each thousand dead bytes the walk counts: the cost at the floor. At a floor of d a rewrite
/// copies (1 - d) / d bytes for each dead byte it frees, so 3,000 keeps up with a 25% floor
pub const PACE_PER_MILLE: u64 = 3_000;

/// Read the copy bytes a walk's dead bytes allow paced rewrites
pub const fn pace_credit(dead_bytes: u64, per_mille: u64) -> u64 {
    dead_bytes.saturating_mul(per_mille) / 1_000
}

/// The space target's built-in value: dead bytes a rewrite must win back, as thousandths of the live bytes, before a busy caller's
/// rewrite under half dead starts. 150 matches what RocksDB holds
pub const SPACE_TARGET_PER_MILLE: u64 = 150;

/// Check a volume holds more dead bytes than its target allows, once the rewrites in flight have freed theirs: only then does a
/// paced rewrite start. The paced band's multiplier then settles wherever the target holds, from none to the pace's ceiling
pub const fn is_over_target(dead: u64, in_flight: u64, live: u64, target_per_mille: u64) -> bool {
    dead.saturating_sub(in_flight) > live.saturating_mul(target_per_mille) / 1_000
}

/// Read the most credit a volume holds: what one walk of a segment of dead bytes earns, and the pick in hand, so a walk pays in
/// full for the picks its dead bytes allow
pub const fn pace_cap(segment_bytes: u64, per_mille: u64) -> u64 {
    pace_credit(segment_bytes, per_mille).saturating_add(segment_bytes)
}

/// Check the credit lets a paced pick start: it covers the estimate, or holds the cap for a pick larger than the cap
pub const fn can_start(credit: i64, estimate: u64, cap: u64) -> bool {
    let needed = if estimate < cap { estimate } else { cap };
    credit >= needed as i64
}

/// Check a record pick spends the pace's credit: while callers are busy and the disk has room, unless every input is past half
/// dead, where a rewrite copies at most a byte for each byte it frees
pub const fn is_paced(load: Load, is_past_half: bool) -> bool {
    load.is_busy && !load.is_disk_short && !is_past_half
}

/// A volume's pace: the copy bytes paced rewrites may still start, which a walk's landing adds to and a paced pick spends
#[derive(Debug, Default)]
pub struct PaceCell {
    credit: AtomicI64,
}

impl PaceCell {
    /// Read the credit
    pub fn credit(&self) -> i64 {
        self.credit.load(Ordering::Acquire)
    }

    /// Add a landing's credit up to the cap, and return what the cap turned away
    pub fn add(&self, credit: u64, cap: u64) -> u64 {
        let cap = cap as i64;
        let held = self.credit.fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| Some(held.saturating_add(credit as i64).min(cap)));
        let held = held.unwrap_or_else(|held| held);
        (held.saturating_add(credit as i64) - cap).max(0) as u64
    }

    /// Charge a paced rewrite its whole estimate when it starts, past zero if it must, so a pick larger than the cap still
    /// starts once the credit is full and the long-run rate holds
    pub fn charge(&self, estimate: u64) {
        self.credit.fetch_sub(estimate as i64, Ordering::AcqRel);
    }

    /// Settle a paced rewrite at its end: what it spent comes back, less what it wrote, so a wrong estimate costs no drift
    pub fn settle(&self, spent: u64, written: u64, cap: u64) {
        let back = spent as i64 - written as i64;
        let cap = cap as i64;
        let _moved = self.credit.fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| Some(held.saturating_add(back).min(cap)));
    }
}

/// Bits of a load in one byte
const BUSY: u8 = 1;
const HOT: u8 = 2;
const TABLE_SHORT: u8 = 4;
const DISK_SHORT: u8 = 8;
const INGESTING: u8 = 16;

/// A volume's load as the planner's last step worked it out, which every lane's pick and the walk read
#[derive(Debug, Default)]
pub struct LoadCell {
    bits: AtomicU8,
    is_current: AtomicBool,
    quiet_at: AtomicU64,
    hot_until: AtomicU64,
    /// When the disk's room is next read
    disk_check_at: AtomicU64,
}

/// How long a disk room reading lasts. A walk of small rows runs tens of thousands of planner steps a second, and a free space
/// call each would cost more than the step, while a merge's own claim reads the disk again before it writes
pub const DISK_CHECK_PERIOD: Nanoseconds = Nanoseconds(1_000_000);

impl LoadCell {
    /// Check the disk's room is due a fresh reading, and start the next period if so
    pub fn is_disk_check_due(&self, now: Nanoseconds) -> bool {
        if now.0 < self.disk_check_at.load(Ordering::Relaxed) {
            return false;
        }
        self.disk_check_at.store(now.0 + DISK_CHECK_PERIOD.0, Ordering::Relaxed);
        true
    }

    /// Keep a step's load and the times it ends at
    pub fn set(&self, load: Load, quiet_at: Nanoseconds, hot_until: Nanoseconds) {
        let bit = |is_set: bool, bit: u8| if is_set { bit } else { 0 };
        let bits = bit(load.is_busy, BUSY)
            | bit(load.is_hot, HOT)
            | bit(load.is_table_short, TABLE_SHORT)
            | bit(load.is_disk_short, DISK_SHORT)
            | bit(load.is_ingesting, INGESTING);
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
            is_ingesting: bits & INGESTING != 0,
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
        (0..32u8).map(|bits| Load {
            is_busy: bits & BUSY != 0,
            is_hot: bits & HOT != 0,
            is_table_short: bits & TABLE_SHORT != 0,
            is_disk_short: bits & DISK_SHORT != 0,
            is_ingesting: bits & INGESTING != 0,
        })
    }

    /// Everything the summary can owe
    fn every_owed() -> impl Iterator<Item = Owed> {
        (0..4u8).map(|bits| Owed { is_reclaim_due: bits & 1 != 0, is_reclaim_held: bits & 2 != 0 })
    }

    #[test]
    fn decisions_follow_the_load_table() {
        for load in every_load() {
            let is_deferred = load.is_hot && !load.is_table_short && !load.is_disk_short;
            assert_eq!(is_deferring(load), is_deferred, "{load:?}");
            assert_eq!(is_capped(load), load.is_busy, "{load:?}");
            let is_holding = load.is_ingesting && !load.is_table_short && !load.is_disk_short;
            assert_eq!(is_holding_asks(load), is_holding, "{load:?}");
            let lanes = match (is_deferred || load.is_disk_short, load.is_busy) {
                (true, _) => OverlapLanes::None,
                (false, true) => OverlapLanes::One,
                (false, false) => OverlapLanes::All,
            };
            assert_eq!(overlap_lanes(load), lanes, "{load:?}");
            for owed in every_owed() {
                let bar = match (owed.is_reclaim_due, owed.is_reclaim_held, load.is_disk_short) {
                    (false, ..) => PLAIN_BAR,
                    (true, false, true) => ROOM_SHORT_BAR,
                    (true, true, _) => PLAIN_BAR,
                    (true, false, false) => QUIET_BAR,
                };
                assert_eq!(record_bar(load, owed), bar, "{load:?} {owed:?}");
                let is_wanted = owed.is_reclaim_due && !owed.is_reclaim_held && (!load.is_busy || load.is_disk_short);
                assert_eq!(is_reclaim_wanted(load, owed), is_wanted, "{load:?} {owed:?}");
            }
        }
    }

    #[test]
    fn short_disk_never_defers_a_record_pick() {
        for load in every_load().filter(|load| load.is_disk_short) {
            assert!(!is_deferring(load), "{load:?}");
        }
    }

    #[test]
    fn short_disk_never_takes_an_overlap_merge() {
        for load in every_load().filter(|load| load.is_disk_short) {
            assert_eq!(overlap_lanes(load), OverlapLanes::None, "{load:?}");
        }
    }

    #[test]
    fn short_disk_never_holds_an_ask() {
        for load in every_load().filter(|load| load.is_disk_short) {
            assert!(!is_holding_asks(load), "{load:?}");
        }
    }

    #[test]
    fn busy_gets_the_floor_and_the_pace() {
        let owed = Owed { is_reclaim_due: true, is_reclaim_held: false };
        for load in every_load().filter(|load| load.is_busy && !load.is_disk_short) {
            assert_eq!(record_bar(load, owed), QUIET_BAR, "{load:?}");
            assert!(is_paced(load, false), "{load:?}");
        }
    }

    #[test]
    fn past_half_quiet_and_short_disks_go_unpaced() {
        for load in every_load() {
            assert!(!is_paced(load, true), "{load:?}");
            assert_eq!(is_paced(load, false), load.is_busy && !load.is_disk_short, "{load:?}");
        }
    }

    #[test]
    fn pace_holds_the_floor() {
        // a rewrite at a 25% floor copies three bytes for each one it frees, which 3,000 a thousand pays for
        let freed = 1_000_000;
        assert_eq!(pace_credit(freed, PACE_PER_MILLE), 3_000_000);
        assert_eq!(pace_credit(freed, 0), 0);
        assert_eq!(pace_credit(u64::MAX, PACE_PER_MILLE), u64::MAX / 1_000);
    }

    #[test]
    fn pace_cell_charges_settles_and_caps() {
        let cell = PaceCell::default();
        let cap = 100;
        assert!(!can_start(cell.credit(), 1, cap), "an empty credit paid for a pick");
        assert_eq!(cell.add(250, cap), 150, "the cap turned away other than the excess");
        assert_eq!(cell.credit(), 100);
        assert!(can_start(cell.credit(), 60, cap));
        cell.charge(60);
        assert!(!can_start(cell.credit(), 60, cap), "a pick started past the credit");
        // a rewrite that wrote four times its estimate pays the difference
        cell.settle(60, 240, cap);
        assert_eq!(cell.credit(), 40 + 60 - 240);
        assert!(!can_start(cell.credit(), 1, cap), "a credit in debt paid for a pick");
        assert_eq!(cell.add(1_000, cap), 1_000 - 100 - 140);
        assert_eq!(cell.credit(), cap as i64);
    }

    #[test]
    fn full_bank_starts_a_pick_past_the_cap() {
        let cell = PaceCell::default();
        let cap = 100;
        cell.add(cap, cap);
        assert!(can_start(cell.credit(), 150, cap), "a pick past the cap never starts");
        cell.charge(150);
        assert_eq!(cell.credit(), -50, "the charge left other than half a cap of debt");
    }

    #[test]
    fn charge_after_a_settle_still_counts() {
        // a seat settles a debt between the planner's check and its charge, and the charge still goes in whole
        let cell = PaceCell::default();
        let cap = 100;
        cell.add(80, cap);
        assert!(can_start(cell.credit(), 70, cap));
        cell.settle(0, 60, cap);
        cell.charge(70);
        assert_eq!(cell.credit(), 80 - 60 - 70);
    }

    #[test]
    fn held_reclaim_never_lowers_the_bar() {
        for load in every_load() {
            for owed in every_owed().filter(|owed| owed.is_reclaim_held) {
                assert_eq!(record_bar(load, owed), PLAIN_BAR, "{load:?} {owed:?}");
                assert!(!is_reclaim_wanted(load, owed), "{load:?} {owed:?}");
            }
        }
    }

    #[test]
    fn wanted_reclaim_gets_a_bar_under_the_plain_one() {
        for load in every_load() {
            for owed in every_owed().filter(|owed| is_reclaim_wanted(load, *owed)) {
                assert!(record_bar(load, owed) < PLAIN_BAR, "{load:?} {owed:?}");
            }
        }
    }

    #[test]
    fn disk_reading_lasts_a_period() {
        let cell = LoadCell::default();
        assert!(cell.is_disk_check_due(Nanoseconds(5)));
        assert!(!cell.is_disk_check_due(Nanoseconds(5 + DISK_CHECK_PERIOD.0 - 1)));
        assert!(cell.is_disk_check_due(Nanoseconds(5 + DISK_CHECK_PERIOD.0)));
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
