use reel::lifetime::*;
use reel_api::units::DeathMark;

const NOW: DeathMark = DeathMark(1_000);

// a ceiling sits on a grid no coarser than the slack, inside what the mark may reach
#[test]
fn ceiling_inside_reach() {
    for mark in [1_001, 1_004, 1_999, 5_000, 123_456, 1 << 40] {
        let mark = DeathMark(mark);
        let ceiling = ceiling_of(mark, NOW);
        assert!(ceiling >= mark);
        assert!(ceiling <= reach_of(mark, NOW), "mark {mark:?}");
    }
}

// nearby lifetimes round to one ceiling, so they share a group
#[test]
fn nearby_marks_share() {
    assert_eq!(ceiling_of(DeathMark(5_001), NOW), ceiling_of(DeathMark(5_100), NOW));
}

// a record waits at most a quarter of its life past its death
#[test]
fn reach_is_a_quarter_of_life() {
    assert_eq!(reach_of(DeathMark(5_000), NOW), DeathMark(6_000));
}

// a mark with no life left has no slack, so it only meets its exact ceiling
#[test]
fn no_life_no_slack() {
    for mark in [DeathMark(1_000), DeathMark(10)] {
        assert_eq!(reach_of(mark, NOW), mark);
        assert_eq!(ceiling_of(mark, NOW), mark);
    }
}

// a mark near the top stays a ceiling at or above itself
#[test]
fn top_mark_saturates() {
    let mark = DeathMark(u64::MAX - 1);
    assert!(ceiling_of(mark, DeathMark(0)) >= mark);
    assert_eq!(reach_of(mark, DeathMark(0)), DeathMark(u64::MAX));
}

// an empty range holds nothing, whatever a mark's number is, zero included
#[test]
fn empty_range() {
    let mut marks = MarkRange::default();
    assert!(marks.is_empty());
    assert_eq!(marks.highest(), None);
    marks.add(DeathMark(0));
    assert_eq!(marks.highest(), Some(DeathMark(0)));
}

// two ranges join over both, and an empty one adds nothing
#[test]
fn ranges_join() {
    let (mut first, mut second) = (MarkRange::NONE, MarkRange::NONE);
    first.add(DeathMark(10));
    second.add(DeathMark(30));
    let joined = first.join(second);
    assert_eq!((joined.lowest, joined.highest), (DeathMark(10), DeathMark(30)));
    assert_eq!(first.join(MarkRange::NONE), first);
}

// a merge joins unmarked with unmarked and overlapping marks with each other, and nothing else
#[test]
fn lifetimes_match() {
    let range = |lowest, highest| MarkRange { lowest: DeathMark(lowest), highest: DeathMark(highest) };
    assert!(MarkRange::NONE.shares_lifetime(MarkRange::NONE));
    assert!(range(10, 20).shares_lifetime(range(20, 30)));
    assert!(!range(10, 20).shares_lifetime(range(21, 30)));
    assert!(!range(10, 20).shares_lifetime(MarkRange::NONE));
    assert!(!MarkRange::NONE.shares_lifetime(range(10, 20)));
}
