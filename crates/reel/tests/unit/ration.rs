use reel::engine::ration::*;
use reel_api::units::{Nanoseconds, RowCount};

const SEAL_ROWS: u64 = 100_000;
const HELD: RowCount = RowCount(1_000);
const FREE: RowCount = RowCount(80_000);
const WALK_SHARE: u64 = FREE.0 / 8 * WALK_RATION_EIGHTHS;
const STARTED: Nanoseconds = Nanoseconds(1_000);
const WALK_TIME: u64 = 8_000;
const LATER: Nanoseconds = Nanoseconds(STARTED.0 + 1);

fn started() -> Ration {
    let ration = Ration::default();
    ration.start(RowCount(SEAL_ROWS), HELD, FREE, STARTED);
    ration
}

// no seal means no line, so every write goes on
#[test]
fn no_seal_no_line() {
    let ration = Ration::default();
    assert_eq!(ration.line(|| LATER), None);
    assert!(ration.lets_through(RowCount(u64::MAX / 2), RowCount(1), || LATER));
}

// the line starts one step ahead of the walk
#[test]
fn line_leads_the_walk() {
    let ration = started();
    assert_eq!(ration.line(|| LATER), Some(RowCount(HELD.0 + share(WALK_SHARE, RATION_LEAD_ROWS, SEAL_ROWS))));
}

// the walk's end draws the walk's share, and the rest waits for the steps after it
#[test]
fn walk_draws_its_share() {
    let ration = started();
    ration.note_walked(RowCount(SEAL_ROWS));
    assert_eq!(ration.line(|| LATER), Some(RowCount(HELD.0 + WALK_SHARE)));
    ration.note_cleaned(RowCount(SEAL_ROWS / 2));
    assert_eq!(ration.line(|| LATER), Some(RowCount(HELD.0 + WALK_SHARE + (FREE.0 - WALK_SHARE) / 2)));
    ration.note_cleaned(RowCount(SEAL_ROWS));
    assert_eq!(ration.line(|| LATER), Some(RowCount(HELD.0 + FREE.0)));
}

// a walk that passes its count never draws more than the room there was
#[test]
fn line_stops_at_the_room() {
    let ration = started();
    ration.note_walked(RowCount(SEAL_ROWS * 4));
    ration.note_cleaned(RowCount(SEAL_ROWS * 4));
    assert_eq!(ration.line(|| LATER), Some(RowCount(HELD.0 + FREE.0)));
}

// a write past the line waits, and one that fits goes on
#[test]
fn line_decides() {
    let ration = started();
    let line = ration.line(|| LATER).expect("line");
    assert!(ration.lets_through(RowCount(line.0 - 1), RowCount(1), || LATER));
    assert!(!ration.lets_through(line, RowCount(1), || LATER));
}

// a writer in line holds back a write that would fit, so a big batch keeps its place ahead of single puts
#[test]
fn ticket_holds_the_line() {
    let ration = started();
    let first = ration.take_ticket();
    let second = ration.take_ticket();
    assert!(!ration.lets_through(HELD, RowCount(1), || LATER));
    assert!(ration.is_served(first));
    assert!(!ration.is_served(second));
    ration.next_in_line();
    assert!(ration.is_served(second));
    ration.next_in_line();
    assert!(ration.lets_through(HELD, RowCount(1), || LATER));
}

// the end of a seal clears the line for every write
#[test]
fn clear_ends_the_line() {
    let ration = started();
    ration.clear();
    assert_eq!(ration.line(|| LATER), None);
}

// the share held back goes out on the clock once the walk ends, at the walk's own pace
#[test]
fn held_share_goes_out_on_the_clock() {
    let ration = started();
    let ended = Nanoseconds(STARTED.0 + WALK_TIME);
    ration.note_walked(RowCount(SEAL_ROWS));
    assert_eq!(ration.line(|| ended), Some(RowCount(HELD.0 + WALK_SHARE)));

    ration.note_walk_end(ended);
    assert_eq!(ration.line(|| ended), Some(RowCount(HELD.0 + WALK_SHARE)));
    let a_quarter_on = Nanoseconds(ended.0 + WALK_TIME / 28);
    assert_eq!(ration.line(|| a_quarter_on), Some(RowCount(HELD.0 + WALK_SHARE + share(WALK_SHARE, WALK_TIME / 28, WALK_TIME))));
    let long_after = Nanoseconds(ended.0 + 10 * WALK_TIME);
    assert_eq!(ration.line(|| long_after), Some(RowCount(HELD.0 + FREE.0)));
}

// the clean can only hurry the clock, and the two never add up past the room
#[test]
fn clean_hurries_the_clock() {
    let ration = started();
    let ended = Nanoseconds(STARTED.0 + WALK_TIME);
    ration.note_walked(RowCount(SEAL_ROWS));
    ration.note_walk_end(ended);
    ration.note_cleaned(RowCount(SEAL_ROWS));
    assert_eq!(ration.line(|| ended), Some(RowCount(HELD.0 + FREE.0)));
    let long_after = Nanoseconds(ended.0 + 10 * WALK_TIME);
    assert_eq!(ration.line(|| long_after), Some(RowCount(HELD.0 + FREE.0)));
}
