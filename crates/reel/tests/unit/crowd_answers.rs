use std::cell::Cell;

use reel::engine::rewrite_job::{CROWD_ANSWERS, CrowdAnswers, CrowdStamp};
use reel_api::column_id::ColumnId;

const COLUMN: ColumnId = ColumnId(1);
const STAMP: CrowdStamp = CrowdStamp { directory: 7, filters: 3, is_sized: true };

// a point asked again under the same stamp reuses its answer, and a new stamp, another column or a pushed-out answer works it out
#[test]
fn answers_hold_while_the_stamp_does() {
    let mut answers = CrowdAnswers::new();
    let worked = Cell::new(0);
    let work = |is_crowded: bool| {
        worked.set(worked.get() + 1);
        is_crowded
    };
    assert!(answers.answer(COLUMN, b"point", STAMP, || work(true)));
    assert!(answers.answer(COLUMN, b"point", STAMP, || work(false)));
    assert_eq!((worked.get(), answers.reused), (1, 1));

    let moved = CrowdStamp { directory: 8, ..STAMP };
    assert!(!answers.answer(COLUMN, b"point", moved, || work(false)));
    assert!(answers.answer(ColumnId(2), b"point", moved, || work(true)));
    assert!(!answers.answer(COLUMN, b"point", CrowdStamp { filters: 4, ..moved }, || work(false)));
    assert!(!answers.answer(COLUMN, b"point", CrowdStamp { is_sized: false, ..moved }, || work(false)));
    assert_eq!(worked.get(), 5);

    for number in 0..CROWD_ANSWERS {
        answers.answer(COLUMN, format!("other-{number}").as_bytes(), STAMP, || work(false));
    }
    let before = worked.get();
    answers.answer(COLUMN, b"point", CrowdStamp { is_sized: false, ..moved }, || work(true));
    assert_eq!(worked.get(), before + 1);
}
