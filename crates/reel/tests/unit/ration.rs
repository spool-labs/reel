use reel::engine::ration::*;
use reel_api::units::RowCount;

const SEAL_ROWS: u64 = 100_000;
const HELD: RowCount = RowCount(1_000);
const FREE: RowCount = RowCount(80_000);
const WALK_SHARE: u64 = FREE.0 / 8 * WALK_RATION_EIGHTHS;

fn started() -> Ration {
    let ration = Ration::default();
    ration.start(RowCount(SEAL_ROWS), HELD, FREE);
    ration
}

// no seal means no line, so every write goes on
#[test]
fn no_seal_no_line() {
    let ration = Ration::default();
    assert_eq!(ration.line(), None);
    assert!(ration.lets_through(RowCount(u64::MAX / 2), RowCount(1)));
}

// the line starts one step ahead of the walk
#[test]
fn line_leads_the_walk() {
    let ration = started();
    assert_eq!(ration.line(), Some(RowCount(HELD.0 + share(WALK_SHARE, RATION_LEAD_ROWS, SEAL_ROWS))));
}

// the walk's end draws the walk's share, and the rest waits for the steps after it
#[test]
fn walk_draws_its_share() {
    let ration = started();
    ration.note_walked(RowCount(SEAL_ROWS));
    assert_eq!(ration.line(), Some(RowCount(HELD.0 + WALK_SHARE)));
    ration.note_cleaned(RowCount(SEAL_ROWS / 2));
    assert_eq!(ration.line(), Some(RowCount(HELD.0 + WALK_SHARE + (FREE.0 - WALK_SHARE) / 2)));
    ration.note_cleaned(RowCount(SEAL_ROWS));
    assert_eq!(ration.line(), Some(RowCount(HELD.0 + FREE.0)));
}

// a walk that passes its count never draws more than the room there was
#[test]
fn line_stops_at_the_room() {
    let ration = started();
    ration.note_walked(RowCount(SEAL_ROWS * 4));
    ration.note_cleaned(RowCount(SEAL_ROWS * 4));
    assert_eq!(ration.line(), Some(RowCount(HELD.0 + FREE.0)));
}

// a write past the line waits, and one that fits goes on
#[test]
fn line_decides() {
    let ration = started();
    let line = ration.line().expect("line");
    assert!(ration.lets_through(RowCount(line.0 - 1), RowCount(1)));
    assert!(!ration.lets_through(line, RowCount(1)));
}

// a writer in line holds back a write that would fit, so a big batch keeps its place ahead of single puts
#[test]
fn ticket_holds_the_line() {
    let ration = started();
    let first = ration.take_ticket();
    let second = ration.take_ticket();
    assert!(!ration.lets_through(HELD, RowCount(1)));
    assert!(ration.is_served(first));
    assert!(!ration.is_served(second));
    ration.next_in_line();
    assert!(ration.is_served(second));
    ration.next_in_line();
    assert!(ration.lets_through(HELD, RowCount(1)));
}

// the end of a seal clears the line for every write
#[test]
fn clear_ends_the_line() {
    let ration = started();
    ration.clear();
    assert_eq!(ration.line(), None);
}
