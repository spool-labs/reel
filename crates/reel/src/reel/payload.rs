//! Payload buffers the engine lends to readers, pooled per thread and per size class

use std::cell::RefCell;

/// The smallest buffer worth pooling, since the allocator is cheap below it
const MIN_POOLED: usize = 512;

/// The largest buffer worth pooling, since a bigger high-water mark costs more than it saves
const MAX_POOLED: usize = 16 * 1024 * 1024;

/// A size class never keeps more than this many buffers
const PER_CLASS: usize = 4;

/// One size class may hold this many bytes across the buffers it keeps
const CLASS_BUDGET: usize = 2 * 1024 * 1024;

/// Size classes, one per power of two from MIN_POOLED to MAX_POOLED
const CLASSES: usize = 16;

thread_local! {
    /// This thread's spare payload buffers, bucketed by size class
    static SPARE: RefCell<[Vec<Vec<u8>>; CLASSES]> =
        const { RefCell::new([const { Vec::new() }; CLASSES]) };
}

/// The class a request of this size rounds up to, or nothing outside the classes
fn class_of(wanted: usize) -> Option<usize> {
    if !(MIN_POOLED..=MAX_POOLED).contains(&wanted) {
        return None;
    }
    let steps = (wanted - 1).max(MIN_POOLED - 1).ilog2() as usize;
    let floor = (MIN_POOLED - 1).ilog2() as usize;
    let class = steps.saturating_sub(floor);
    (class < CLASSES).then_some(class)
}

/// Capacity a class hands out
const fn capacity_of(class: usize) -> usize {
    MIN_POOLED << class
}

/// Buffers a class keeps, deep where they are small and shallow where they are not
const fn depth_of(class: usize) -> usize {
    match CLASS_BUDGET / capacity_of(class) {
        0 => 1,
        allowed if allowed > PER_CLASS => PER_CLASS,
        allowed => allowed,
    }
}

/// The capacity the pool takes back for this size, or the size itself outside the classes
pub fn pooled_capacity(wanted: usize) -> usize {
    match class_of(wanted) {
        Some(class) => capacity_of(class),
        None => wanted,
    }
}

/// This thread's spare buffer of a class, or nothing once the pool is torn down
fn pop(class: usize) -> Option<Vec<u8>> {
    SPARE
        .try_with(|spare| spare.borrow_mut()[class].pop())
        .ok()
        .flatten()
}

/// An empty buffer with room for at least this many bytes, from the pool when it has one
pub fn take(wanted: usize) -> Vec<u8> {
    let Some(class) = class_of(wanted) else {
        return Vec::with_capacity(wanted);
    };
    match pop(class) {
        Some(mut bytes) => {
            bytes.clear();
            bytes
        }
        None => Vec::with_capacity(capacity_of(class)),
    }
}

/// A buffer of exactly this length, holding whatever was last written into it
pub fn take_written(wanted: usize) -> Vec<u8> {
    let Some(class) = class_of(wanted) else {
        return vec![0u8; wanted];
    };
    let mut bytes = match pop(class) {
        Some(bytes) => bytes,
        // A zeroed allocation comes back as blank pages the allocator never touched
        None => vec![0u8; capacity_of(class)],
    };
    match bytes.len() >= wanted {
        true => bytes.truncate(wanted),
        false => bytes.resize(wanted, 0),
    }
    bytes
}

/// Offer a buffer back for the next read on this thread, dropping it if it does not fit
pub fn give(bytes: Vec<u8>) {
    let capacity = bytes.capacity();
    let Some(class) = class_of(capacity) else {
        return;
    };
    if capacity_of(class) != capacity {
        return;
    }
    // A pool already torn down drops the offer, since this can run from Drop
    let _ = SPARE.try_with(|spare| {
        let mut spare = spare.borrow_mut();
        if spare[class].len() < depth_of(class) {
            spare[class].push(bytes);
        }
    });
}

/// Bytes this thread's pool is holding, for a test asserting nothing leaked
#[cfg(test)]
pub fn held_bytes() -> usize {
    SPARE
        .try_with(|spare| {
            let mut total = 0;
            for class in spare.borrow().iter() {
                for bytes in class {
                    total += bytes.capacity();
                }
            }
            total
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    // a buffer offered back serves the next read of its class
    #[test]
    fn a_returned_buffer_comes_back() {
        let first = take(MIN_POOLED);
        let address = first.as_ptr();
        give(first);

        let second = take(MIN_POOLED);
        assert_eq!(second.as_ptr(), address, "the same allocation served both");
    }

    // the classes reach both the floor and the ceiling
    #[test]
    fn the_classes_span_the_range() {
        assert_eq!(class_of(MIN_POOLED), Some(0));
        assert_eq!(capacity_of(0), MIN_POOLED);
        assert!(
            class_of(MAX_POOLED).is_some(),
            "the ceiling fell outside its own classes"
        );
        assert_eq!(capacity_of(CLASSES - 1), MAX_POOLED);
    }

    // a request outside the classes is served without the pool
    #[test]
    fn sizes_outside_the_classes_are_left_alone() {
        assert!(class_of(MIN_POOLED - 1).is_none());
        assert!(class_of(MAX_POOLED + 1).is_none());
        assert_eq!(class_of(MIN_POOLED), Some(0));
        assert_eq!(capacity_of(0), MIN_POOLED);
    }

    // a class holds only what it is asked to and drops the rest
    #[test]
    fn a_full_class_drops_the_offer() {
        SPARE.with_borrow_mut(|spare| spare.iter_mut().for_each(|class| class.clear()));
        for _ in 0..PER_CLASS + 3 {
            give(Vec::with_capacity(MIN_POOLED));
        }

        let held = SPARE.with_borrow(|spare| spare[0].len());
        assert_eq!(held, depth_of(0));
    }

    // the budget keeps the small classes deep and thins the large ones
    #[test]
    fn a_class_is_as_deep_as_its_budget_allows() {
        assert_eq!(depth_of(0), PER_CLASS, "a small class lost its depth");
        assert_eq!(
            depth_of(CLASSES - 1),
            1,
            "the largest class kept more than one"
        );
        for class in 0..CLASSES {
            let held = depth_of(class) * capacity_of(class);
            assert!(
                held <= CLASS_BUDGET.max(capacity_of(class)),
                "class {class} holds {held} over its budget"
            );
        }
    }

    // a written buffer comes back at the length asked for, reusing what it holds
    #[test]
    fn a_written_buffer_is_as_long_as_it_was_asked_for() {
        SPARE.with_borrow_mut(|spare| spare.iter_mut().for_each(|class| class.clear()));
        let mut first = take_written(MIN_POOLED);
        assert_eq!(first.len(), MIN_POOLED);
        first.iter_mut().for_each(|byte| *byte = 7);
        give(first);

        // Back at the same length, which zeroes nothing
        let again = take_written(MIN_POOLED);
        assert_eq!(again.len(), MIN_POOLED);
        assert!(
            again.iter().all(|&byte| byte == 7),
            "the buffer was rewritten"
        );
    }

    // a buffer offered back keeps its length, which is how far it is known written
    #[test]
    fn an_offered_buffer_keeps_its_length() {
        SPARE.with_borrow_mut(|spare| spare.iter_mut().for_each(|class| class.clear()));
        let mut bytes = take(MIN_POOLED);
        bytes.resize(MIN_POOLED, 1);
        give(bytes);

        let held = SPARE.with_borrow(|spare| spare[0][0].len());
        assert_eq!(held, MIN_POOLED, "the offer was cleared on the way in");
    }

    // a taken buffer is empty however long it was when it was offered back
    #[test]
    fn a_taken_buffer_is_empty() {
        SPARE.with_borrow_mut(|spare| spare.iter_mut().for_each(|class| class.clear()));
        let mut bytes = take(MIN_POOLED);
        bytes.resize(MIN_POOLED, 1);
        give(bytes);

        assert!(take(MIN_POOLED).is_empty());
    }

    // a capacity to grow to is one the pool will take back
    #[test]
    fn a_pooled_capacity_is_one_the_pool_accepts() {
        for wanted in [MIN_POOLED, MIN_POOLED + 1, 1200, 1024 * 1024 + 1] {
            let room = pooled_capacity(wanted);
            assert!(room >= wanted);
            let class = class_of(room).expect("a pooled capacity outside the classes");
            assert_eq!(capacity_of(class), room);
        }
        assert_eq!(pooled_capacity(MAX_POOLED + 1), MAX_POOLED + 1);
    }

    // a buffer serves a read no larger than its class
    #[test]
    fn a_buffer_has_room_for_what_it_was_asked_for() {
        for wanted in [MIN_POOLED, MIN_POOLED + 1, 100 * 1024, 1024 * 1024] {
            let bytes = take(wanted);
            assert!(
                bytes.capacity() >= wanted,
                "{wanted} got {}",
                bytes.capacity()
            );
            assert!(bytes.is_empty());
        }
    }
}
