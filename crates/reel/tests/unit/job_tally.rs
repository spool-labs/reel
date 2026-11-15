//! A kind's unit tally: its largest work, wall time and thread CPU, each on its own

use reel::engine::job_tally::UnitTally;
use reel_api::units::Nanoseconds;

// a long unit that mostly waited and a short one that mostly worked give the long wall time and the high CPU
#[test]
fn wall_time_and_cpu_keep_their_own_largest() {
    let tally = UnitTally::default();
    tally.note(10, Nanoseconds(600_000_000), Nanoseconds(12_000_000));
    tally.note(5, Nanoseconds(80_000_000), Nanoseconds(75_000_000));
    let reading = tally.reading();
    assert_eq!(reading.time, Nanoseconds(600_000_000));
    assert_eq!(reading.cpu, Nanoseconds(75_000_000));
    assert_eq!(reading.work, 10);
    assert_eq!(reading.time_sum, Nanoseconds(680_000_000));
}
