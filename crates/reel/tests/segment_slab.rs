//! The segment counter window against the map it replaced
//!
//! The oracle is the hashed table this window was built from: the same arithmetic in
//! `HashMap`s, with the two rules the window adds written out. A booking that names a
//! segment the table has let go of is dropped rather than given a row back, and a
//! booking that only moves bytes somebody else counted needs a row already standing.
//! Everything else has to agree op for op, and the floors are what the agreement is
//! for: a lost floor is a tombstone dropped early and a key that comes back.

use std::collections::{HashMap, HashSet};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use reel::format::loc::SegmentId;
use reel::format::lsn::Lsn;
use reel::index::counters::SegmentTable;
use reel::SegmentBytes;

/// Segment numbers the streams draw from, banded so the window spans chunks
const NUMBERS: [u32; 36] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310,
    311, 900, 901, 902, 903, 904, 905, 906, 907, 908, 909, 910, 911,
];

/// Ops one stream applies
const OPS: usize = 4_000;

/// One segment's row in the oracle, holding what the map held
#[derive(Clone, Copy, Debug, Default)]
struct Row {
    live: u64,
    dead: u64,
    held: u64,
    held_lsn: Option<Lsn>,
    min_lsn: Option<Lsn>,
    max_lsn: Option<Lsn>,
}

/// The hashed table, plus the two rules the window adds
#[derive(Default)]
struct Oracle {
    /// Rows the table counts, which is what ranks and what holds a floor
    rows: HashMap<SegmentId, Row>,

    /// Segments the table has let go of, which no booking brings back
    retired: HashSet<SegmentId>,

    /// Segments a rebuild left sealed
    born: HashSet<SegmentId>,

    /// Segments wearing a life, whether or not they hold bytes
    stamped: HashSet<SegmentId>,
}

impl Oracle {
    /// A row for a caller booking into a segment it holds open
    fn opened(&mut self, segment: SegmentId) -> Option<&mut Row> {
        match self.retired.contains(&segment) {
            true => None,
            false => Some(self.rows.entry(segment).or_default()),
        }
    }

    /// A row that already stands, for a caller moving bytes it did not book
    fn booked(&mut self, segment: SegmentId) -> Option<&mut Row> {
        self.rows.get_mut(&segment)
    }

    fn mark_live(&mut self, segment: SegmentId, lsn: Lsn, span: u64) {
        if let Some(row) = self.opened(segment) {
            row.min_lsn = Some(row.min_lsn.map_or(lsn, |min| min.min(lsn)));
            row.live += span;
        }
    }

    fn mark_dead(&mut self, segment: SegmentId, lsn: Lsn, span: u64) {
        if let Some(row) = self.opened(segment) {
            row.min_lsn = Some(row.min_lsn.map_or(lsn, |min| min.min(lsn)));
            row.dead += span;
        }
    }

    fn mark_held(&mut self, segment: SegmentId, lsn: Lsn, span: u64) {
        if let Some(row) = self.opened(segment) {
            row.live += span;
            row.held += span;
            row.held_lsn = Some(row.held_lsn.map_or(lsn, |newest| newest.max(lsn)));
        }
    }

    fn note_min(&mut self, segment: SegmentId, lsn: Lsn) {
        if let Some(row) = self.opened(segment) {
            row.min_lsn = Some(row.min_lsn.map_or(lsn, |min| min.min(lsn)));
        }
    }

    fn note_max(&mut self, segment: SegmentId, lsn: Lsn) {
        if lsn == Lsn::NONE {
            return;
        }
        if let Some(row) = self.opened(segment) {
            row.max_lsn = Some(row.max_lsn.map_or(lsn, |newest| newest.max(lsn)));
        }
    }

    fn shadow(&mut self, segment: SegmentId, span: u64) {
        if let Some(row) = self.booked(segment) {
            row.live = row.live.saturating_sub(span);
            row.dead += span;
        }
    }

    fn release_live(&mut self, segment: SegmentId, span: u64) {
        if let Some(row) = self.booked(segment) {
            row.live = row.live.saturating_sub(span);
        }
    }

    fn settle_dead(&mut self, segment: SegmentId, counted: u64) {
        if let Some(row) = self.booked(segment) {
            if counted > row.dead {
                let owed = counted - row.dead;
                row.live = row.live.saturating_sub(owed);
                row.dead += owed;
            }
        }
    }

    fn live_incarnation(&mut self, segment: SegmentId) {
        if !self.retired.contains(&segment) {
            self.stamped.insert(segment);
        }
    }

    fn mark_born(&mut self, segment: SegmentId) {
        if !self.retired.contains(&segment) {
            self.born.insert(segment);
            self.stamped.insert(segment);
        }
    }

    /// A number the table never knew is left alone, since nothing stood there
    fn forget(&mut self, segment: SegmentId) {
        let known = self.rows.contains_key(&segment)
            || self.born.contains(&segment)
            || self.stamped.contains(&segment);
        self.rows.remove(&segment);
        self.born.remove(&segment);
        self.stamped.remove(&segment);
        if known {
            self.retired.insert(segment);
        }
    }

    fn install(&mut self, segments: &HashMap<SegmentId, SegmentBytes>) {
        self.rows.clear();
        self.born.clear();
        self.stamped.clear();
        self.retired.clear();
        for (segment, bytes) in segments {
            self.rows.insert(
                *segment,
                Row {
                    live: bytes.live,
                    dead: bytes.dead,
                    held: bytes.held,
                    held_lsn: bytes.held_lsn,
                    min_lsn: None,
                    max_lsn: None,
                },
            );
            self.stamped.insert(*segment);
        }
    }

    fn bytes_of(&self, segment: SegmentId) -> SegmentBytes {
        self.rows
            .get(&segment)
            .map(|row| SegmentBytes {
                live: row.live,
                dead: row.dead,
                held: row.held,
                held_lsn: row.held_lsn,
            })
            .unwrap_or_default()
    }

    /// The oldest number any segment but this one can still surface
    fn min_lsn_excluding(&self, segment: SegmentId) -> Option<Lsn> {
        self.rows
            .iter()
            .filter(|(held, _)| **held != segment)
            .filter_map(|(_, row)| row.min_lsn)
            .min()
    }

    fn dead_bytes(&self) -> u64 {
        self.rows.values().map(|row| row.dead).sum()
    }

    fn snapshot(&self) -> Vec<(SegmentId, SegmentBytes)> {
        let mut out: Vec<(SegmentId, SegmentBytes)> = self
            .rows
            .keys()
            .map(|segment| (*segment, self.bytes_of(*segment)))
            .collect();
        out.sort_by_key(|(segment, _)| segment.as_u32());
        out
    }
}

fn sorted(mut rows: Vec<(SegmentId, SegmentBytes)>) -> Vec<(SegmentId, SegmentBytes)> {
    rows.sort_by_key(|(segment, _)| segment.as_u32());
    rows
}

/// Every question the table answers, asked of both and compared
fn agree(table: &SegmentTable, oracle: &Oracle, step: usize) {
    assert_eq!(
        sorted(table.snapshot()),
        oracle.snapshot(),
        "footprints parted at step {step}"
    );
    assert_eq!(table.len(), oracle.rows.len(), "row count parted at {step}");
    assert_eq!(table.is_empty(), oracle.rows.is_empty());
    assert_eq!(
        table.dead_bytes(),
        oracle.dead_bytes(),
        "dead gauge parted at {step}"
    );
    assert_eq!(
        table.born_count(),
        oracle.born.len(),
        "born count parted at {step}"
    );

    let floors = table.floors();
    let (ranked, ranked_floors) = table.ranking();
    assert_eq!(
        sorted(ranked),
        oracle.snapshot(),
        "ranking parted at {step}"
    );
    for number in NUMBERS.iter().copied().chain([12, 512, 1_000]) {
        let segment = SegmentId(number);
        assert_eq!(
            table.bytes_of(segment),
            oracle.bytes_of(segment),
            "segment {number} parted at step {step}"
        );
        let owed = oracle.min_lsn_excluding(segment);
        assert_eq!(
            table.min_lsn_excluding(segment),
            owed,
            "floor excluding {number} parted at step {step}"
        );
        assert_eq!(floors.excluding(segment), owed);
        assert_eq!(ranked_floors.excluding(segment), owed);
        assert_eq!(
            table.min_lsn_of(segment),
            oracle.rows.get(&segment).and_then(|row| row.min_lsn),
            "floor of {number} parted at step {step}"
        );
        assert_eq!(
            table.max_lsn_of(segment),
            oracle.rows.get(&segment).and_then(|row| row.max_lsn),
            "ceiling of {number} parted at step {step}"
        );
        assert_eq!(
            table.is_born(segment),
            oracle.born.contains(&segment),
            "born bit of {number} parted at step {step}"
        );
        assert_eq!(
            !table.incarnation_of(segment).is_none(),
            oracle.stamped.contains(&segment),
            "stamp of {number} parted at step {step}"
        );
    }
    let stamps = table.stamps();
    assert_eq!(stamps.len(), oracle.rows.len());
    for (segment, stamp) in stamps {
        assert_eq!(stamp.bytes, oracle.bytes_of(segment));
        assert_eq!(stamp.min_lsn, oracle.rows[&segment].min_lsn);
    }
}

/// One seeded stream of every op the table takes, applied to both
fn stream(seed: u64) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let table = SegmentTable::new();
    let mut oracle = Oracle::default();

    for step in 0..OPS {
        let segment = SegmentId(NUMBERS[rng.gen_range(0..NUMBERS.len())]);
        let lsn = Lsn(rng.gen_range(1..500));
        let span = rng.gen_range(1..4_000);
        match rng.gen_range(0..14u32) {
            0..=2 => {
                table.mark_live(segment, lsn, span);
                oracle.mark_live(segment, lsn, span);
            }
            3..=4 => {
                table.mark_dead(segment, lsn, span);
                oracle.mark_dead(segment, lsn, span);
            }
            5 => {
                table.mark_held(segment, lsn, span);
                oracle.mark_held(segment, lsn, span);
            }
            6 => {
                table.shadow(segment, span);
                oracle.shadow(segment, span);
            }
            7 => {
                table.release_live(segment, span);
                oracle.release_live(segment, span);
            }
            8 => {
                table.settle_dead(segment, span);
                oracle.settle_dead(segment, span);
            }
            9 => {
                table.note_min(segment, lsn);
                oracle.note_min(segment, lsn);
            }
            10 => {
                let ceiling = match rng.gen_bool(0.1) {
                    true => Lsn::NONE,
                    false => lsn,
                };
                table.note_max(segment, ceiling);
                oracle.note_max(segment, ceiling);
            }
            11 => {
                table.live_incarnation(segment);
                oracle.live_incarnation(segment);
            }
            12 => {
                table.mark_born([segment]);
                oracle.mark_born(segment);
            }
            _ => {
                table.forget(segment);
                oracle.forget(segment);
            }
        }
        // Every step is compared on a short stream; a long one checks every hundredth
        // and the last, since the walk is quadratic in the segment count.
        if OPS <= 200 || step % 100 == 0 || step + 1 == OPS {
            agree(&table, &oracle, step);
        }
    }
}

// the window answers everything the map answered, over seeded op streams
#[test]
fn the_window_agrees_with_the_map() {
    for seed in 0..24 {
        stream(seed);
    }
}

// an install replaces the whole table and takes the retires with it
#[test]
fn an_install_starts_the_window_over() {
    let table = SegmentTable::new();
    let mut oracle = Oracle::default();

    table.mark_live(SegmentId(3), Lsn(4), 900);
    oracle.mark_live(SegmentId(3), Lsn(4), 900);
    table.forget(SegmentId(3));
    oracle.forget(SegmentId(3));

    let mut segments = HashMap::new();
    segments.insert(
        SegmentId(3),
        SegmentBytes {
            live: 700,
            dead: 50,
            ..SegmentBytes::default()
        },
    );
    segments.insert(
        SegmentId(900),
        SegmentBytes {
            live: 10,
            ..SegmentBytes::default()
        },
    );
    table.install(segments.clone(), HashMap::new(), HashMap::new());
    oracle.install(&segments);

    // The retire is undone by the install, so the number books again.
    table.mark_live(SegmentId(3), Lsn(2), 100);
    oracle.mark_live(SegmentId(3), Lsn(2), 100);
    assert_eq!(table.bytes_of(SegmentId(3)).live, 800);
    assert_eq!(table.min_lsn_excluding(SegmentId(900)), Some(Lsn(2)));
    agree(&table, &oracle, 0);
}

// a booking that arrives after the retire is dropped rather than reopening the row
#[test]
fn a_late_booking_never_brings_a_segment_back() {
    let table = SegmentTable::new();
    table.mark_live(SegmentId(4), Lsn(9), 500);
    table.mark_live(SegmentId(5), Lsn(20), 500);

    let before = table.dropped_bookings();
    table.forget(SegmentId(4));

    table.mark_live(SegmentId(4), Lsn(1), 700);
    table.mark_dead(SegmentId(4), Lsn(1), 700);
    table.mark_held(SegmentId(4), Lsn(1), 700);
    table.note_min(SegmentId(4), Lsn(1));
    table.shadow(SegmentId(4), 700);
    table.release_live(SegmentId(4), 700);
    table.settle_dead(SegmentId(4), 700);
    table.note_max(SegmentId(4), Lsn(1));
    table.mark_born([SegmentId(4)]);

    assert_eq!(table.len(), 1, "the retired row stayed gone");
    assert_eq!(table.bytes_of(SegmentId(4)), SegmentBytes::default());
    assert!(table.incarnation_of(SegmentId(4)).is_none());
    assert!(table.live_incarnation(SegmentId(4)).is_none());
    assert!(!table.is_born(SegmentId(4)));
    // The floor the retired segment held is gone with it, and the late booking's
    // older number never became one.
    assert_eq!(table.min_lsn_excluding(SegmentId(9)), Some(Lsn(20)));
    assert_eq!(table.dropped_bookings() - before, 10);
}

// a floor booked at landing survives the publish that books it again
#[test]
fn a_landing_floor_holds_through_its_publish() {
    let table = SegmentTable::new();
    table.note_min(SegmentId(1), Lsn(7));
    table.mark_live(SegmentId(1), Lsn(9), 400);
    table.mark_live(SegmentId(2), Lsn(30), 400);

    assert_eq!(table.min_lsn_of(SegmentId(1)), Some(Lsn(7)));
    assert_eq!(table.min_lsn_excluding(SegmentId(2)), Some(Lsn(7)));
    // The landing alone is enough: a caller that never publishes still holds it.
    table.note_min(SegmentId(3), Lsn(2));
    assert_eq!(table.min_lsn_excluding(SegmentId(1)), Some(Lsn(2)));
}

// a number a tail drew and has not written into yet holds the window open
#[test]
fn an_unbooked_number_is_not_slid_past() {
    let table = SegmentTable::new();
    // Ten is a tail's fresh segment: drawn, no record landed in it yet. The tails
    // above it fill, seal and retire while it sits there.
    for number in 11..40 {
        table.mark_live(SegmentId(number), Lsn(u64::from(number)), 100);
    }
    for number in 11..40 {
        table.forget(SegmentId(number));
    }
    assert!(table.is_empty());

    // The first record lands in ten long after every number above it went away.
    table.note_min(SegmentId(10), Lsn(3));
    table.mark_live(SegmentId(10), Lsn(3), 100);

    assert_eq!(table.len(), 1);
    assert_eq!(table.min_lsn_of(SegmentId(10)), Some(Lsn(3)));
    assert_eq!(table.min_lsn_excluding(SegmentId(99)), Some(Lsn(3)));
    assert_eq!(table.dropped_bookings(), 0);
}

// a low number still books after a higher one seated the window
#[test]
fn a_number_below_the_first_one_still_books() {
    let table = SegmentTable::new();
    table.mark_live(SegmentId(300), Lsn(50), 100);
    table.mark_live(SegmentId(12), Lsn(7), 100);

    assert_eq!(table.len(), 2, "the low number lost its row");
    assert_eq!(table.bytes_of(SegmentId(12)).live, 100);
    assert_eq!(table.min_lsn_of(SegmentId(12)), Some(Lsn(7)));
    assert_eq!(table.min_lsn_excluding(SegmentId(300)), Some(Lsn(7)));
    assert_eq!(table.dropped_bookings(), 0);
}

// a rebuild's born marks land whatever order the segments arrive in
#[test]
fn born_marks_land_out_of_number_order() {
    let table = SegmentTable::new();
    table.mark_born([SegmentId(600), SegmentId(300), SegmentId(12)]);

    for number in [600u32, 300, 12] {
        assert!(
            table.is_born(SegmentId(number)),
            "segment {number} lost its born mark"
        );
        assert!(!table.incarnation_of(SegmentId(number)).is_none());
    }
    assert_eq!(table.born_count(), 3);
}

// the window gives its chunks back as the oldest segments retire
#[test]
fn the_window_slides_as_it_retires() {
    let table = SegmentTable::new();
    for number in 0..3_000 {
        table.mark_live(SegmentId(number), Lsn(u64::from(number) + 1), 10);
    }
    for number in 0..2_900 {
        table.forget(SegmentId(number));
    }

    assert_eq!(table.len(), 100);
    assert_eq!(table.min_lsn_excluding(SegmentId(9_999)), Some(Lsn(2_901)));
    // A number below the slid floor is gone whether or not its chunk still stands.
    table.mark_live(SegmentId(7), Lsn(1), 10);
    assert_eq!(table.len(), 100);
    assert_eq!(table.min_lsn_excluding(SegmentId(9_999)), Some(Lsn(2_901)));

    // And the window keeps taking numbers above it.
    table.mark_live(SegmentId(3_000), Lsn(3_001), 10);
    assert_eq!(table.len(), 101);
}
