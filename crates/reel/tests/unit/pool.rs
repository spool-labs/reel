use std::panic::{self, AssertUnwindSafe};

use reel::pool::*;

// an item goes back after use, also when the work panics
#[test]
fn item_returns() {
    let pool = Pool::new(vec![1_u32]);

    assert_eq!(pool.with(|item| *item + 1), 2);
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| pool.with(|_item| panic!("failed work"))));

    assert!(outcome.is_err());
    assert_eq!(pool.with(|item| *item), 1);
}
