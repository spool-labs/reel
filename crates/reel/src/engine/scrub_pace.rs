//! The scrub's rate: a floor while the device is busy, and up to many times it while the scrub's own block reads stay as quick
//! as they are on a quiet device. The signal is the scrub's read time a KiB, so it needs no device class and follows a node's
//! steady trickle of work without a busy or idle switch

use reel_core::units::Nanoseconds;

/// The most the rate climbs to, in floors
pub const MOST_FLOORS: u64 = 64;
/// A second's median read time under this many tenths of the baseline lets the rate climb by one floor
pub const CLIMB_TENTHS: u64 = 15;
/// A second's median read time over this many tenths of the baseline halves the rate. The gap to CLIMB_TENTHS keeps it steady
pub const HALVE_TENTHS: u64 = 20;
/// A second with more caller operations than this many times the last minute's mean drops the rate to the floor
pub const BURST_TIMES: u64 = 2;
/// How often the scrub drops to its floor to measure the device again, and how long each measure lasts
pub const MEASURE_EVERY: Nanoseconds = Nanoseconds(300_000_000_000);
pub const MEASURE_FOR: Nanoseconds = Nanoseconds(2_000_000_000);
/// Measures the baseline remembers. It is the lowest second over them, so a measure taken under load is replaced by any quieter
/// second, and a low one expires after this many measures
pub const BASELINE_MEASURES: usize = 4;
/// Read times a second keeps for its median
pub const SECOND_SAMPLES: usize = 64;
const NANOS_PER_SECOND: u64 = 1_000_000_000;
/// Seconds the caller mean covers
const MEAN_SECONDS: u64 = 60;
/// Caller operations a second under which no second counts as a burst, so a trickle over a quiet store moves nothing
pub const LEAST_BURST: u64 = 50;

/// One volume's scrub rate and what it learned of the device
#[derive(Clone, Debug)]
pub struct ScrubPace {
    floor: u64,
    rate: u64,
    /// The current second: when it began, and the read times a KiB it saw
    second_at: Nanoseconds,
    samples: [u64; SECOND_SAMPLES],
    sample_count: usize,
    /// The lowest second's median in each of the last measures, the newest at `measure_slot`
    lows: [u64; BASELINE_MEASURES],
    measure_slot: usize,
    /// The current measure at the floor ends then, and the next one starts at `next_measure`
    measuring_until: Nanoseconds,
    next_measure: Nanoseconds,
    /// Caller operations at the last second's end, and their mean a second over the last minute in thousandths, so a slow trickle
    /// still has a mean
    callers_at: u64,
    caller_mean_milli: u64,
    is_mean_seeded: bool,
}

impl ScrubPace {
    /// Start at the floor, measuring
    pub fn new(floor: u64) -> Self {
        Self {
            floor,
            rate: floor,
            second_at: Nanoseconds(0),
            samples: [0; SECOND_SAMPLES],
            sample_count: 0,
            lows: [u64::MAX; BASELINE_MEASURES],
            measure_slot: 0,
            measuring_until: Nanoseconds(0),
            next_measure: Nanoseconds(0),
            callers_at: 0,
            caller_mean_milli: 0,
            is_mean_seeded: false,
        }
    }

    /// Read the floor the rate never falls under
    pub fn floor(&self) -> u64 {
        self.floor
    }

    /// Read the rate the scrub checks at now, in bytes a second
    pub fn rate(&self) -> u64 {
        self.rate
    }

    /// Read the baseline: the lowest second over the remembered measures, or nothing before the first second
    pub fn baseline(&self) -> Option<u64> {
        self.lows.iter().copied().min().filter(|low| *low != u64::MAX)
    }

    /// Note one block read: how long it took and how many bytes it read
    pub fn note_read(&mut self, took: Nanoseconds, bytes: u64) {
        if bytes == 0 || self.sample_count == SECOND_SAMPLES {
            return;
        }
        self.samples[self.sample_count] = took.0.saturating_mul(1024) / bytes;
        self.sample_count += 1;
    }

    /// Move the rate on once a second has passed, from that second's reads and the callers' operations so far
    pub fn tick(&mut self, now: Nanoseconds, callers: u64) {
        if self.second_at.0 == 0 {
            self.second_at = now;
            self.callers_at = callers;
            self.start_measure(now);
            return;
        }
        let elapsed = now.0.saturating_sub(self.second_at.0) / NANOS_PER_SECOND;
        if elapsed == 0 {
            return;
        }
        // a scrub that waited several seconds between blocks moves on by each of them, and counts its callers by the second
        let ops = callers.saturating_sub(self.callers_at) / elapsed;
        let ops_milli = ops.saturating_mul(1000);
        let is_burst = ops > LEAST_BURST && ops_milli > self.caller_mean_milli.saturating_mul(BURST_TIMES);
        // the first second seeds the mean, so a store's steady work never reads as a burst while the mean catches up
        let weight = if self.is_mean_seeded { elapsed.min(MEAN_SECONDS) } else { MEAN_SECONDS };
        self.is_mean_seeded = true;
        self.caller_mean_milli = (self.caller_mean_milli * (MEAN_SECONDS - weight) + ops_milli * weight) / MEAN_SECONDS;
        self.callers_at = callers;
        self.second_at = now;
        let median = self.take_median();
        if let Some(median) = median {
            let low = &mut self.lows[self.measure_slot];
            *low = (*low).min(median);
        }
        if now < self.measuring_until {
            self.rate = self.floor;
            return;
        }
        if now >= self.next_measure {
            self.start_measure(now);
            return;
        }
        if is_burst {
            self.rate = self.floor;
            return;
        }
        let (Some(median), Some(baseline)) = (median, self.baseline()) else {
            return;
        };
        let baseline = baseline.max(1);
        if median.saturating_mul(10) > baseline.saturating_mul(HALVE_TENTHS) {
            self.rate = (self.rate / 2).max(self.floor);
        } else if median.saturating_mul(10) < baseline.saturating_mul(CLIMB_TENTHS) {
            // one floor a tick that read something, however long the scrub waited before it, so a long wait never buys a jump
            self.rate = self.rate.saturating_add(self.floor).min(self.floor.saturating_mul(MOST_FLOORS));
        }
    }

    /// Drop to the floor to measure the device again, in a fresh slot of the remembered measures
    fn start_measure(&mut self, now: Nanoseconds) {
        self.rate = self.floor;
        self.measure_slot = (self.measure_slot + 1) % BASELINE_MEASURES;
        self.lows[self.measure_slot] = u64::MAX;
        self.measuring_until = Nanoseconds(now.0.saturating_add(MEASURE_FOR.0));
        self.next_measure = Nanoseconds(now.0.saturating_add(MEASURE_EVERY.0));
    }

    /// Take the median of the second's read times and start the next second empty
    fn take_median(&mut self) -> Option<u64> {
        let count = self.sample_count;
        self.sample_count = 0;
        if count == 0 {
            return None;
        }
        let samples = &mut self.samples[..count];
        samples.sort_unstable();
        Some(samples[count / 2])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FLOOR: u64 = 4 * 1024 * 1024;
    const KIB: u64 = 1024;

    /// Run whole seconds of block reads, each taking `nanos_a_kib` a KiB, with `ops` caller operations each second
    fn seconds(pace: &mut ScrubPace, now: &mut Nanoseconds, callers: &mut u64, count: u64, nanos_a_kib: u64, ops: u64) {
        for _second in 0..count {
            for _read in 0..8 {
                pace.note_read(Nanoseconds(nanos_a_kib * 64), 64 * KIB);
            }
            *now = Nanoseconds(now.0 + NANOS_PER_SECOND);
            *callers += ops;
            pace.tick(*now, *callers);
        }
    }

    fn start() -> (ScrubPace, Nanoseconds, u64) {
        let mut pace = ScrubPace::new(FLOOR);
        let now = Nanoseconds(NANOS_PER_SECOND);
        pace.tick(now, 0);
        (pace, now, 0)
    }

    // a long wait between blocks followed by one quick read climbs one floor, never the seconds it waited
    #[test]
    fn a_long_wait_climbs_one_floor() {
        let (mut pace, mut now, mut callers) = start();
        seconds(&mut pace, &mut now, &mut callers, 4, 100, 10);
        let before = pace.rate();
        pace.note_read(Nanoseconds(100 * 64), 64 * KIB);
        now = Nanoseconds(now.0 + 256 * NANOS_PER_SECOND);
        pace.tick(now, callers);
        assert_eq!(pace.rate(), before + FLOOR);
    }

    // reads as quick as the measure climb a floor a second to the most, and stay there
    #[test]
    fn a_quiet_device_climbs_to_the_most() {
        let (mut pace, mut now, mut callers) = start();
        seconds(&mut pace, &mut now, &mut callers, 1, 100, 10);
        assert_eq!(pace.rate(), FLOOR, "the measure left the floor");
        seconds(&mut pace, &mut now, &mut callers, MOST_FLOORS + 4, 100, 10);
        assert_eq!(pace.rate(), FLOOR * MOST_FLOORS);
    }

    // reads that slow past twice the baseline halve the rate each second down to the floor, and reads between the two lines
    // hold it
    #[test]
    fn slow_reads_halve_and_the_gap_holds() {
        let (mut pace, mut now, mut callers) = start();
        seconds(&mut pace, &mut now, &mut callers, 20, 100, 10);
        let high = pace.rate();
        assert!(high > FLOOR * 8);
        seconds(&mut pace, &mut now, &mut callers, 1, 170, 10);
        assert_eq!(pace.rate(), high, "a read time inside the gap moved the rate");
        seconds(&mut pace, &mut now, &mut callers, 1, 300, 10);
        assert_eq!(pace.rate(), high / 2);
        seconds(&mut pace, &mut now, &mut callers, 10, 300, 10);
        assert_eq!(pace.rate(), FLOOR);
    }

    // a second with a burst of caller operations drops the rate to the floor at once
    #[test]
    fn a_caller_burst_drops_to_the_floor() {
        let (mut pace, mut now, mut callers) = start();
        seconds(&mut pace, &mut now, &mut callers, 120, 100, 1_000);
        assert!(pace.rate() > FLOOR * 8);
        seconds(&mut pace, &mut now, &mut callers, 1, 100, 5_000);
        assert_eq!(pace.rate(), FLOOR);
    }

    // a measure taken while the device was loaded sets a high baseline, and one quieter second lowers it, so when the load comes
    // back the slow reads halve the rate and don't license a climb
    #[test]
    fn a_loaded_measure_gives_way_to_a_quieter_second() {
        let (mut pace, mut now, mut callers) = start();
        seconds(&mut pace, &mut now, &mut callers, 3, 1_000, 10);
        assert_eq!(pace.baseline(), Some(1_000));
        seconds(&mut pace, &mut now, &mut callers, 1, 100, 10);
        assert_eq!(pace.baseline(), Some(100), "a quieter second left the loaded baseline");
        seconds(&mut pace, &mut now, &mut callers, 5, 100, 10);
        let before = pace.rate();
        seconds(&mut pace, &mut now, &mut callers, 1, 1_000, 10);
        assert!(pace.rate() < before, "the load came back and the rate didn't fall");
    }

    // the pace drops to the floor to measure again on its period, and a low baseline expires once newer measures replace it
    #[test]
    fn measures_come_round_and_old_lows_expire() {
        let (mut pace, mut now, mut callers) = start();
        seconds(&mut pace, &mut now, &mut callers, 20, 100, 10);
        assert!(pace.rate() > FLOOR);
        let period = MEASURE_EVERY.0 / NANOS_PER_SECOND;
        for _measure in 0..BASELINE_MEASURES {
            seconds(&mut pace, &mut now, &mut callers, period, 400, 10);
        }
        assert_eq!(pace.baseline(), Some(400), "a low from four measures back still stood");
    }
}
