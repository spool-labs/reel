use std::time::Duration;

use reel::io::ring::reap_spin;

const MOST: Duration = Duration::from_micros(150);

// a reaper spins only while reads are out, so it costs nothing between reads at a trickle
#[test]
fn reaper_spins_only_with_reads_out() {
    assert_eq!(reap_spin(0, MOST), Duration::ZERO);
    assert_eq!(reap_spin(1, MOST), MOST);
    assert_eq!(reap_spin(64, MOST), MOST);
}
