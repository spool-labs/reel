//! Free-space pressure tiers, the maintenance reserve, and rate pacing
//!
//! Dead bytes are debt, and the pressure model turns that debt into a tier. A hard
//! fullness ceiling sits one segment below capacity: foreground writes stop at that
//! ceiling while compaction may still write into the reserve band, so the append-only
//! deadlock of needing to write in order to free space cannot happen.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::sync::checked::{AtomicBool, AtomicU64, Ordering};

use crate::config::CompactRate;
use crate::sync::lock;

/// Store-wide dead fraction below which the plane defers to hot ingest
const DEFAULT_SOFT_DEAD_FRACTION: f64 = 0.20;

/// Rewrite threshold the escalated tier lowers the base ratio toward
const ESCALATED_DEAD_RATIO_FLOOR: f64 = 0.20;

/// Share of the escalation mark the plane holds escalated down to
///
/// Without a gap between the mark that escalates and the one that relaxes, a volume
/// resting on the mark re-decides every tick and moves the rewrite threshold under
/// whoever is reading it.
const ESCALATION_RELEASE: f64 = 0.75;

/// Reserves' worth of room above the ceiling where writers are slowed, not refused
///
/// Stated in reserves so a volume with larger segments gets a proportionally wider
/// band without a second number to keep in step with the first.
const SLOWDOWN_RESERVES: u64 = 8;

/// Share of the write budget still admitted at the refusal ceiling itself
///
/// Not zero: zero would stall the writers this band exists to merely slow, and
/// stopping them is the refusal door's job one byte further on.
const SLOWDOWN_FLOOR: f64 = 0.05;

/// Nanoseconds in one second, for turning a byte budget into a cadence
const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// Bytes in one megabyte, decimal as bandwidth figures are quoted
const BYTES_PER_MEGABYTE: u64 = 1_000_000;

/// Stretch of device time one unit of paced maintenance work may hold
///
/// Stated in time rather than bytes, since the byte count it stands for is whatever
/// the rate earns in it, which is what makes a low cap take smaller bites rather than
/// the same bite less often.
const PACE_STEP: Duration = Duration::from_millis(5);

/// What the maintenance plane should do given the current debt and ingest load
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GcTier {
    /// Debt is low and ingest is hot, so hold compaction back
    Deferred,

    /// Debt is low and ingest is idle, so run at the base threshold
    Relaxed,

    /// Debt is high, so run and lower the rewrite threshold
    Escalated,
}

/// Free-space pressure and the maintenance reserve for one volume
///
/// A capacity of zero leaves the volume unbounded, so the reserve gate is dormant and
/// only the tier and threshold logic apply.
pub struct GcPressure {
    /// Bytes the volume may hold, or zero when it is unbounded
    capacity_bytes: u64,

    /// Room above the ceiling that only compaction may write into
    reserve_bytes: u64,

    /// Per-segment dead fraction that selects a rewrite outside escalation
    base_dead_ratio: f64,

    /// Store-wide dead fraction the plane escalates at
    soft_dead_fraction: f64,

    /// Whether the plane stands escalated, which `tier` both reads and writes
    is_escalated: AtomicBool,
}

impl GcPressure {
    /// A pressure model over a capacity, a one-segment reserve, and a base ratio
    pub fn new(capacity_bytes: u64, reserve_bytes: u64, base_dead_ratio: f64) -> GcPressure {
        GcPressure {
            capacity_bytes,
            reserve_bytes,
            base_dead_ratio,
            soft_dead_fraction: DEFAULT_SOFT_DEAD_FRACTION,
            is_escalated: AtomicBool::new(false),
        }
    }

    /// Whether this volume tracks a capacity at all
    pub fn is_bounded(&self) -> bool {
        self.capacity_bytes != 0
    }

    /// The ceiling a foreground write is measured against, when there is one
    pub fn foreground_ceiling_bytes(&self) -> Option<u64> {
        self.is_bounded()
            .then(|| self.capacity_bytes.saturating_sub(self.reserve_bytes))
    }

    /// The tier the plane runs at for a store-wide dead fraction and ingest state
    ///
    /// Escalating and relaxing happen at different marks, so a volume sitting on the
    /// mark settles on one answer rather than re-deciding every tick.
    pub fn tier(&self, dead_fraction: f64, is_ingest_hot: bool) -> GcTier {
        let mark = match self.is_escalated.load(Ordering::Relaxed) {
            true => self.soft_dead_fraction * ESCALATION_RELEASE,
            false => self.soft_dead_fraction,
        };
        let is_escalated = dead_fraction >= mark;
        self.is_escalated.store(is_escalated, Ordering::Relaxed);
        if is_escalated {
            return GcTier::Escalated;
        }
        if is_ingest_hot {
            GcTier::Deferred
        } else {
            GcTier::Relaxed
        }
    }

    /// Whether a compaction pass should run now rather than defer to hot ingest
    pub fn should_compact(&self, dead_fraction: f64, is_ingest_hot: bool) -> bool {
        !matches!(self.tier(dead_fraction, is_ingest_hot), GcTier::Deferred)
    }

    /// The per-segment dead fraction that selects a rewrite, lowered when escalated
    pub fn effective_dead_ratio(&self, dead_fraction: f64, is_ingest_hot: bool) -> f64 {
        match self.tier(dead_fraction, is_ingest_hot) {
            GcTier::Escalated => self.base_dead_ratio.min(ESCALATED_DEAD_RATIO_FLOOR),
            GcTier::Deferred | GcTier::Relaxed => self.base_dead_ratio,
        }
    }

    /// Whether a foreground write of this size fits below the reserve ceiling
    pub fn can_admit_foreground(&self, used_bytes: u64, request_bytes: u64) -> bool {
        if !self.is_bounded() {
            return true;
        }
        let ceiling = self.capacity_bytes.saturating_sub(self.reserve_bytes);
        used_bytes + request_bytes <= ceiling
    }

    /// Whether a compaction write of this size fits, the reserve band included
    pub fn can_admit_compaction(&self, used_bytes: u64, request_bytes: u64) -> bool {
        if !self.is_bounded() {
            return true;
        }
        used_bytes + request_bytes <= self.capacity_bytes
    }

    /// The share of the write budget a foreground writer should be given
    ///
    /// One while the volume has room, falling linearly to the floor across the band
    /// below the refusal ceiling. It is spent by shrinking the in-flight budget rather
    /// than by delaying anyone, and an unbounded volume is never throttled.
    pub fn foreground_throttle(&self, used_bytes: u64) -> f64 {
        let Some(ceiling) = self.foreground_ceiling_bytes() else {
            return 1.0;
        };
        let band = self.reserve_bytes.saturating_mul(SLOWDOWN_RESERVES);
        let opens_at = ceiling.saturating_sub(band);
        if used_bytes <= opens_at {
            return 1.0;
        }
        // past the ceiling the write is refused outright, and a zero-width band lands here
        if used_bytes >= ceiling {
            return SLOWDOWN_FLOOR;
        }
        let into = (used_bytes - opens_at) as f64 / band as f64;
        1.0 - into * (1.0 - SLOWDOWN_FLOOR)
    }

    /// Whether foreground writes are blocked at the reserve ceiling, the alarm
    pub fn is_foreground_blocked(&self, used_bytes: u64) -> bool {
        if !self.is_bounded() {
            return false;
        }
        used_bytes >= self.capacity_bytes.saturating_sub(self.reserve_bytes)
    }
}

/// Byte-rate pacing for the maintenance plane
///
/// The bytes are device traffic, read plus write, not the bytes a pass reclaims.
pub struct RateLimiter {
    target_mbps: u64,
}

impl RateLimiter {
    /// A limiter for the compaction rate, where zero and auto are unpaced
    ///
    /// There is no default cap: an uncapped pass runs at device speed only while there
    /// is debt to drain, and a volume wanting maintenance held back names a number.
    pub fn for_compaction(rate: CompactRate) -> RateLimiter {
        let target_mbps = match rate {
            CompactRate::Auto => 0,
            CompactRate::Mbps(mbps) => mbps,
        };
        RateLimiter { target_mbps }
    }

    /// A limiter for the scrub rate, or nothing when the scrub is disabled
    ///
    /// Held at or below a named compaction rate, since an integrity sweep must never
    /// outbid space reclamation. An unpaced compaction rate names no bid to protect, so
    /// the scrub keeps its own rate rather than inheriting unpaced.
    pub fn for_scrub(scrub_mbps: u64, compact_mbps: u64) -> Option<RateLimiter> {
        if scrub_mbps == 0 {
            return None;
        }
        let target_mbps = match compact_mbps {
            0 => scrub_mbps,
            cap => scrub_mbps.min(cap),
        };
        Some(RateLimiter { target_mbps })
    }

    /// The resolved rate in megabytes per second
    pub fn target_mbps(&self) -> u64 {
        self.target_mbps
    }

    /// Bytes the target rate earns over a stretch of time
    pub fn bytes_in(&self, elapsed: Duration) -> u64 {
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.target_mbps.saturating_mul(micros)
    }

    /// The time this many bytes should take at the target rate
    pub fn cadence(&self, bytes: u64) -> Duration {
        if self.target_mbps == 0 {
            return Duration::ZERO;
        }
        let nanos =
            bytes.saturating_mul(NANOS_PER_SECOND) / (self.target_mbps * BYTES_PER_MEGABYTE);
        Duration::from_nanos(nanos)
    }
}

/// A gate that stays shut for as long as the work already done owes at its rate
///
/// Whether a pass may start is asked and never waited on, since the caller drives
/// maintenance on a thread carrying other work. Inside a pass the thread is already
/// the plane's, and the meter consults the gate step by step so that one pass does not
/// own the device from end to end.
pub struct RateGate {
    limiter: RateLimiter,
    ready_at: Mutex<Option<Instant>>,
    ran_at: Mutex<Instant>,
}

impl RateGate {
    /// A gate over a resolved rate, open until the first pass charges it
    pub fn new(limiter: RateLimiter) -> RateGate {
        RateGate {
            limiter,
            ready_at: Mutex::new(None),
            // the clock starts at open, so the first pass earns from the stretch before it
            ran_at: Mutex::new(Instant::now()),
        }
    }

    /// The resolved rate in megabytes per second
    pub fn target_mbps(&self) -> u64 {
        self.limiter.target_mbps()
    }

    /// Whether the rate allows another pass to start now
    pub fn is_open(&self) -> bool {
        match *lock(&self.ready_at) {
            Some(ready_at) => Instant::now() >= ready_at,
            None => true,
        }
    }

    /// Bytes the rate has earned since the last pass asked, capped for one pass
    ///
    /// A pass bounded by a fixed size would run at the caller's cadence rather than at
    /// the configured rate, so asking time what it owes is what makes the rate decide.
    pub fn allowance(&self, cap: Duration) -> u64 {
        let mut ran_at = lock(&self.ran_at);
        let now = Instant::now();
        let earned = self
            .limiter
            .bytes_in(now.saturating_duration_since(*ran_at));
        *ran_at = now;
        earned.min(self.limiter.bytes_in(cap))
    }

    /// Charge the gate for bytes moved since a moment, shutting it for what they owe
    ///
    /// Debt accumulates: a charge landing while the gate is already shut pushes the
    /// reopening out rather than replacing it, or a drain loop retiring many segments
    /// would owe only the last of them.
    ///
    /// The moment the charged work began is what makes the target the achieved rate,
    /// since charging from now hands the mover its own runtime for free. And a charge
    /// never earns credit from before the work it charges for, so a plane left quiet
    /// banks nothing.
    pub fn charge_from(&self, since: Instant, bytes: u64) {
        let owed = self.limiter.cadence(bytes);
        if owed.is_zero() {
            return;
        }
        let mut ready_at = lock(&self.ready_at);
        let standing = match *ready_at {
            Some(standing) if standing > since => standing,
            _ => since,
        };
        *ready_at = Some(standing + owed);
    }

    /// Hold the caller until the rate lets the next unit of work start
    ///
    /// For a caller already inside a pass, where the alternative to waiting is owning
    /// the device until the pass ends. A caller deciding whether to begin one asks
    /// whether the gate is open and goes and does something else.
    pub fn wait_until_open(&self) {
        loop {
            let owed = match *lock(&self.ready_at) {
                Some(ready_at) => ready_at.saturating_duration_since(Instant::now()),
                None => Duration::ZERO,
            };
            if owed.is_zero() {
                return;
            }
            std::thread::sleep(owed);
        }
    }

    /// A meter for one pass, charging this gate step by step as the pass moves bytes
    pub fn pace(&self) -> PassPace<'_> {
        PassPace {
            gate: self,
            charged: 0,
            step_bytes: self.limiter.bytes_in(PACE_STEP),
            since: Instant::now(),
        }
    }
}

/// One pass's running account with its rate gate
///
/// The meter charges what the pass has moved as it moves it and waits out the last step
/// before the next one starts, so the rate binds inside a pass and the device is held in
/// step-sized bites. An unpaced gate earns nothing in a step, so the meter is then a
/// comparison and a return.
pub struct PassPace<'gate> {
    /// The gate this pass charges as it moves bytes
    gate: &'gate RateGate,

    /// Bytes of this pass already charged to the gate
    charged: u64,

    /// Bytes a step may move before the gate is charged and consulted again
    step_bytes: u64,

    /// When the bytes not yet charged began moving
    since: Instant,
}

impl PassPace<'_> {
    /// Bytes one step may move, zero when the gate is unpaced
    ///
    /// A caller that cannot get its own unit of work under the step charges what it
    /// moved and the gate waits the difference out.
    pub fn step_bytes(&self) -> u64 {
        self.step_bytes
    }

    /// Wait out the step before, then charge the gate for what the pass has moved
    ///
    /// The count is the pass's running total rather than a delta, so a caller reading a
    /// counter sums nothing and cannot double-charge by asking twice.
    pub fn reached(&mut self, moved: u64) {
        if self.step_bytes == 0 {
            return;
        }
        self.gate.wait_until_open();
        let fresh = moved.saturating_sub(self.charged);
        if fresh < self.step_bytes {
            return;
        }
        self.charged = moved;
        self.gate.charge_from(self.since, fresh);
        self.since = Instant::now();
    }

    /// Charge whatever the pass moved past its last step, without waiting for it
    ///
    /// The tail of a pass is left standing on the gate rather than slept off inside it,
    /// so the debt it leaves is what holds the next pass back.
    pub fn settle(&mut self, moved: u64) {
        let fresh = moved.saturating_sub(self.charged);
        self.charged = moved;
        self.gate.charge_from(self.since, fresh);
        self.since = Instant::now();
    }
}

/// How many segment rewrites may be under way at once
///
/// A counter rather than a mutex: each pass claims its own segment, so what has to be
/// bounded is how many run together. Entry is tried and never queued for.
pub struct PassPlane {
    running: AtomicU64,
    width: u64,
}

impl PassPlane {
    /// A plane admitting this many passes, at least one
    pub fn new(width: usize) -> PassPlane {
        PassPlane {
            running: AtomicU64::new(0),
            width: (width as u64).max(1),
        }
    }

    /// Places the plane admits at once, for a caller that wants all of them
    pub fn width(&self) -> usize {
        self.width as usize
    }

    /// Take a place on the plane, or nothing when it is already full
    pub fn enter(&self) -> Option<PassSeat<'_>> {
        let mut running = self.running.load(Ordering::Acquire);
        loop {
            if running >= self.width {
                return None;
            }
            match self.running.compare_exchange_weak(
                running,
                running + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(PassSeat { plane: self }),
                Err(found) => running = found,
            }
        }
    }
}

/// A place on the plane, given back however the pass leaves
pub struct PassSeat<'plane> {
    plane: &'plane PassPlane,
}

impl Drop for PassSeat<'_> {
    fn drop(&mut self) {
        self.plane.running.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // a volume told its capacity reports a ceiling and refuses past it
    #[test]
    fn a_bounded_volume_has_a_ceiling() {
        let bounded = GcPressure::new(1000, 100, 0.5);

        assert_eq!(bounded.foreground_ceiling_bytes(), Some(900));
        assert!(bounded.can_admit_foreground(800, 100));
        assert!(!bounded.can_admit_foreground(800, 101));
        // compaction may spend the reserve, which is what the reserve is for
        assert!(bounded.can_admit_compaction(900, 100));
    }

    // escalating and relaxing happen at different marks, so the tier settles
    #[test]
    fn the_tier_relaxes_lower_than_it_escalates() {
        let pressure = GcPressure::new(0, 0, 0.5);
        let mark = DEFAULT_SOFT_DEAD_FRACTION;

        assert_eq!(pressure.tier(mark - 0.01, false), GcTier::Relaxed);
        assert_eq!(pressure.tier(mark, false), GcTier::Escalated);
        assert_eq!(pressure.tier(mark - 0.01, false), GcTier::Escalated);
        assert_eq!(
            pressure.tier(mark * ESCALATION_RELEASE - 0.001, false),
            GcTier::Relaxed
        );
        assert_eq!(pressure.tier(mark - 0.01, false), GcTier::Relaxed);
    }

    // a volume resting on the mark holds one rewrite threshold
    #[test]
    fn a_volume_resting_on_the_mark_holds_one_rewrite_threshold() {
        let pressure = GcPressure::new(0, 0, 0.5);
        let mark = DEFAULT_SOFT_DEAD_FRACTION;

        pressure.tier(mark, false);
        let settled = pressure.effective_dead_ratio(mark, false);

        // dithering either side of the mark must not move the rewrite threshold
        for step in 0..8 {
            let jitter = match step % 2 {
                0 => mark - 0.001,
                _ => mark + 0.001,
            };
            assert_eq!(
                pressure.effective_dead_ratio(jitter, false),
                settled,
                "the threshold moved while the volume sat still",
            );
        }
    }

    // deciding twice in one pass is what the two callers do, and it is stable
    #[test]
    fn asking_twice_in_one_pass_answers_the_same() {
        let pressure = GcPressure::new(0, 0, 0.5);
        let mark = DEFAULT_SOFT_DEAD_FRACTION;

        assert!(pressure.should_compact(mark, true));
        assert_eq!(
            pressure.effective_dead_ratio(mark, true),
            ESCALATED_DEAD_RATIO_FLOOR
        );
        assert!(pressure.should_compact(mark, true));
        assert_eq!(
            pressure.effective_dead_ratio(mark, true),
            ESCALATED_DEAD_RATIO_FLOOR
        );
    }

    // the band opens below the ceiling and reaches its floor exactly at it
    #[test]
    fn the_slowdown_band_spans_the_reserves_below_the_ceiling() {
        // ceiling 900, a band of eight 100 byte reserves, so it opens at 100
        let bounded = GcPressure::new(1000, 100, 0.5);

        assert_eq!(bounded.foreground_throttle(0), 1.0);
        assert_eq!(bounded.foreground_throttle(100), 1.0);
        assert_eq!(bounded.foreground_throttle(900), SLOWDOWN_FLOOR);
        assert_eq!(
            bounded.foreground_throttle(500),
            1.0 - 0.5 * (1.0 - SLOWDOWN_FLOOR)
        );
    }

    // the share falls the whole way across the band and never climbs back
    #[test]
    fn the_slowdown_never_reverses() {
        let bounded = GcPressure::new(1000, 100, 0.5);

        let mut last = f64::MAX;
        for used in (0..=1000).step_by(10) {
            let share = bounded.foreground_throttle(used);
            assert!(share <= last, "share rose at {used}");
            assert!(
                share >= SLOWDOWN_FLOOR,
                "share fell under the floor at {used}"
            );
            last = share;
        }
    }

    // slowing is a region before refusing, so the two doors cannot swap places
    #[test]
    fn writers_are_slowed_before_they_are_refused() {
        let bounded = GcPressure::new(1000, 100, 0.5);

        assert!(bounded.foreground_throttle(880) < 1.0);
        assert!(bounded.can_admit_foreground(880, 10));
        assert!(!bounded.can_admit_foreground(880, 21));
    }

    // an unbounded volume is never throttled, as it is never refused
    #[test]
    fn an_unbounded_volume_is_never_slowed() {
        let unbounded = GcPressure::new(0, 100, 0.5);

        assert_eq!(unbounded.foreground_throttle(0), 1.0);
        assert_eq!(unbounded.foreground_throttle(u64::MAX / 2), 1.0);
    }

    // an unbounded volume admits anything, which is what a capacity of zero means
    #[test]
    fn an_unbounded_volume_reports_no_ceiling() {
        let unbounded = GcPressure::new(0, 100, 0.5);

        assert_eq!(unbounded.foreground_ceiling_bytes(), None);
        assert!(unbounded.can_admit_foreground(u64::MAX / 2, u64::MAX / 4));
    }

    fn pressure() -> GcPressure {
        GcPressure::new(1_000, 100, 0.50)
    }

    // low debt with hot ingest defers, idle ingest runs relaxed
    #[test]
    fn defers_when_hot() {
        let pressure = pressure();

        assert_eq!(pressure.tier(0.05, true), GcTier::Deferred);
        assert_eq!(pressure.tier(0.05, false), GcTier::Relaxed);
        assert!(!pressure.should_compact(0.05, true));
        assert!(pressure.should_compact(0.05, false));
    }

    // high debt escalates and runs regardless of ingest
    #[test]
    fn escalates_on_debt() {
        let pressure = pressure();

        assert_eq!(pressure.tier(0.40, true), GcTier::Escalated);
        assert!(pressure.should_compact(0.40, true));
    }

    // escalation lowers the rewrite threshold, the relaxed tier keeps the base
    #[test]
    fn threshold_lowers_under_pressure() {
        let pressure = pressure();

        assert_eq!(pressure.effective_dead_ratio(0.05, false), 0.50);
        assert_eq!(pressure.effective_dead_ratio(0.40, false), 0.20);
    }

    // foreground stops at the reserve ceiling while compaction writes into it
    #[test]
    fn reserve_admits_only_compaction() {
        let pressure = pressure();
        let at_ceiling = 900;

        assert!(!pressure.can_admit_foreground(at_ceiling, 50));
        assert!(pressure.is_foreground_blocked(at_ceiling));
        assert!(pressure.can_admit_compaction(at_ceiling, 50));
        assert!(!pressure.can_admit_compaction(at_ceiling, 200));
    }

    // an unbounded volume never blocks either plane
    #[test]
    fn unbounded_admits_all() {
        let pressure = GcPressure::new(0, 100, 0.50);

        assert!(!pressure.is_bounded());
        assert!(pressure.can_admit_foreground(u64::MAX / 2, 4096));
        assert!(!pressure.is_foreground_blocked(u64::MAX / 2));
    }

    // the automatic compaction rate is unpaced, a cap exists only when named
    #[test]
    fn auto_rate_unpaced() {
        let auto = RateLimiter::for_compaction(CompactRate::Auto);
        let fixed = RateLimiter::for_compaction(CompactRate::Mbps(120));

        assert_eq!(auto.target_mbps(), 0);
        assert_eq!(fixed.target_mbps(), 120);
    }

    // a zero scrub rate disables the scrub limiter
    #[test]
    fn scrub_zero_disables() {
        assert!(RateLimiter::for_scrub(0, 40).is_none());
        assert_eq!(
            RateLimiter::for_scrub(4, 40)
                .expect("enabled")
                .target_mbps(),
            4
        );
    }

    // the scrub never outbids a named compaction cap
    #[test]
    fn scrub_held_under_compaction() {
        assert_eq!(
            RateLimiter::for_scrub(64, 40)
                .expect("enabled")
                .target_mbps(),
            40
        );
        assert_eq!(
            RateLimiter::for_scrub(16, 40)
                .expect("enabled")
                .target_mbps(),
            16
        );
    }

    // an unpaced compaction rate does not drag the scrub to zero with it
    #[test]
    fn scrub_survives_unpaced_compaction() {
        assert_eq!(
            RateLimiter::for_scrub(64, 0)
                .expect("enabled")
                .target_mbps(),
            64
        );
    }

    // the cadence grows with the byte count at the target rate
    #[test]
    fn cadence_scales() {
        let limiter = RateLimiter::for_compaction(CompactRate::Mbps(100));

        assert_eq!(limiter.cadence(100_000_000), Duration::from_secs(1));
        assert_eq!(limiter.cadence(0), Duration::ZERO);
    }

    // a charged gate shuts for what the bytes owe and a free one never shuts
    #[test]
    fn gate_shuts_on_what_it_owes() {
        let gate = RateGate::new(RateLimiter::for_compaction(CompactRate::Mbps(1)));
        assert!(gate.is_open());

        gate.charge_from(Instant::now(), 10_000_000);
        assert!(!gate.is_open(), "ten seconds of work leaves the gate shut");

        let free = RateGate::new(RateLimiter::for_compaction(CompactRate::Mbps(0)));
        free.charge_from(Instant::now(), u64::MAX);
        assert!(free.is_open(), "an uncapped rate never shuts the gate");
    }

    // consecutive charges owe their sum, so a small one cannot erase a large one
    #[test]
    fn charges_accumulate() {
        let gate = RateGate::new(RateLimiter::for_compaction(CompactRate::Mbps(1)));

        gate.charge_from(Instant::now(), 10_000_000);
        gate.charge_from(Instant::now(), 1_000);

        std::thread::sleep(Duration::from_millis(20));
        assert!(
            !gate.is_open(),
            "ten seconds of debt still stands after a millisecond charge on top"
        );
    }

    // work slower than its own debt leaves the gate open, having already paid
    #[test]
    fn a_pass_pays_with_its_runtime() {
        let gate = RateGate::new(RateLimiter::for_compaction(CompactRate::Mbps(1)));

        let since = Instant::now();
        std::thread::sleep(Duration::from_millis(20));
        gate.charge_from(since, 10_000);

        assert!(
            gate.is_open(),
            "ten milliseconds of debt inside twenty of work"
        );
    }

    // a gate left quiet banks nothing, so a fresh charge owes the whole of it
    #[test]
    fn quiet_time_is_not_banked() {
        let gate = RateGate::new(RateLimiter::for_compaction(CompactRate::Mbps(1)));

        gate.charge_from(Instant::now(), 1_000);
        std::thread::sleep(Duration::from_millis(20));
        gate.charge_from(Instant::now(), 100_000);

        // a charge only ever earns from the work it charges for
        assert!(
            !gate.is_open(),
            "a hundred milliseconds owed the moment it is charged"
        );
    }

    // the rate a run of passes holds is the rate it was set to
    #[test]
    fn achieved_tracks_target() {
        const PASSES: u64 = 10;
        const PASS_BYTES: u64 = 100_000;
        const PASS_WORK: Duration = Duration::from_millis(10);
        let gate = RateGate::new(RateLimiter::for_compaction(CompactRate::Mbps(10)));

        let started = Instant::now();
        for _ in 0..PASSES {
            gate.wait_until_open();
            let since = Instant::now();
            std::thread::sleep(PASS_WORK);
            gate.charge_from(since, PASS_BYTES);
        }
        let elapsed = started.elapsed();

        assert!(elapsed >= Duration::from_millis(90), "the passes ran free");
        assert!(
            elapsed < Duration::from_millis(150),
            "the old law would take 200 ms"
        );
    }

    // a paced pass consults its gate as it runs rather than only at its end
    #[test]
    fn a_pass_paces_itself() {
        const STEPS: u64 = 4;
        let gate = RateGate::new(RateLimiter::for_compaction(CompactRate::Mbps(1)));
        let mut pace = gate.pace();
        let step = pace.step_bytes();

        let started = Instant::now();
        for count in 1..=STEPS {
            pace.reached(step * count);
        }

        assert_eq!(step, 5_000, "a megabyte a second earns 5 kB in a step");
        assert!(
            started.elapsed() >= Duration::from_millis(14),
            "the pass never waited"
        );
    }

    // an unpaced meter never waits and never charges, which is what unpaced means
    #[test]
    fn an_unpaced_pass_is_never_held() {
        let gate = RateGate::new(RateLimiter::for_compaction(CompactRate::Auto));
        let mut pace = gate.pace();

        let started = Instant::now();
        pace.reached(u64::MAX / 2);
        pace.settle(u64::MAX / 2);

        assert_eq!(pace.step_bytes(), 0);
        assert!(gate.is_open());
        assert!(started.elapsed() < Duration::from_millis(5));
    }

    // the tail of a pass is left owed on the gate rather than slept off inside it
    #[test]
    fn a_settled_tail_shuts_the_gate() {
        let gate = RateGate::new(RateLimiter::for_compaction(CompactRate::Mbps(1)));
        let mut pace = gate.pace();

        pace.settle(1_000_000);

        assert!(!gate.is_open(), "a second of debt holds the next pass back");
    }
}
