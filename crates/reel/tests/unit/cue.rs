//! The cue table: which reads still stand below a place

use reel::engine::cue::CueTable;
use reel_core::units::WritePlace;

// a cue counts inside a row's window from the row's place up to, and not including, its replacement's
#[test]
fn cue_window() {
    let table = CueTable::new(1);
    let slot = table.take(0, WritePlace(10)).expect("a slot");
    assert!(table.has_cue_between(0, WritePlace(10), WritePlace(11)));
    assert!(!table.has_cue_between(0, WritePlace(11), WritePlace(20)));
    assert!(!table.has_cue_between(0, WritePlace(5), WritePlace(10)));
    table.release(slot);
}
