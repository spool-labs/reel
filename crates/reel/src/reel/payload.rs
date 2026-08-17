//! Payload buffers the engine lends to a reader rather than gives away
//!
//! A read hands back a reel_core::Value rather than a Vec: a caller that only
//! reads the bytes drops the handle and the buffer comes back for the next read on
//! that thread, and one that needs an owned vector takes it with into_vec. Nothing
//! is copied on either path. The pool is per thread and bounded per size class, so
//! a volume with idle readers gives its memory back.

use std::cell::RefCell;

/// Smallest buffer worth keeping, below which the allocator is already cheap
const MIN_POOLED: usize = 512;

/// Largest buffer worth keeping, above which one reader's high-water mark is
/// more memory than the faults it saves are worth
const MAX_POOLED: usize = 16 * 1024 * 1024;

/// Deepest a size class goes, whatever room its budget leaves
const PER_CLASS: usize = 4;

/// Bytes one size class may hold across the buffers it keeps
///
/// A flat count per class would price a 16 MiB buffer the same as a 512 byte one.
const CLASS_BUDGET: usize = 2 * 1024 * 1024;

/// Size classes, one per power of two from MIN_POOLED to MAX_POOLED
const CLASSES: usize = 16;

thread_local! {
    /// This thread's spare payload buffers, bucketed by size class
    static SPARE: RefCell<[Vec<Vec<u8>>; CLASSES]> =
        const { RefCell::new([const { Vec::new() }; CLASSES]) };
}

/// The class a request of this size is served from, or nothing if it is outside
///
/// A buffer serves a request no larger than its class, so a class holds buffers
/// of exactly its capacity and a request rounds up to the class that fits it.
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

/// The capacity a buffer of this size is pooled at, or the size itself when it
/// falls outside the classes
///
/// A capacity that is not exactly a class's is one the pool refuses, so a caller
/// growing a buffer it means to hand back grows to this rather than to what it
/// needs.
pub fn pooled_capacity(wanted: usize) -> usize {
    match class_of(wanted) {
        Some(class) => capacity_of(class),
        None => wanted,
    }
}

/// This thread's spare buffer of a class, if it has one
///
/// A thread whose pool has already been torn down is answered as an empty one,
/// since this runs from Drop where a panic during an unwind aborts the process.
fn pop(class: usize) -> Option<Vec<u8>> {
    SPARE
        .try_with(|spare| spare.borrow_mut()[class].pop())
        .ok()
        .flatten()
}

/// A buffer with room for this many bytes, from the pool when it has one
///
/// What comes back is always empty and always has at least the room asked for,
/// so a caller fills it exactly as it would fill a fresh allocation.
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
///
/// For the caller that fills every byte, since zeroing first is a pass over the
/// payload that nothing reads. The bytes are not cleared, so a caller that writes
/// less than the whole length hands out what the previous record left behind.
pub fn take_written(wanted: usize) -> Vec<u8> {
    let Some(class) = class_of(wanted) else {
        return vec![0u8; wanted];
    };
    let mut bytes = match pop(class) {
        Some(bytes) => bytes,
        // Asked for zeroed rather than reserved, so a large buffer comes back as
        // blank pages the allocator never touched.
        None => vec![0u8; capacity_of(class)],
    };
    match bytes.len() >= wanted {
        true => bytes.truncate(wanted),
        false => bytes.resize(wanted, 0),
    }
    bytes
}

/// Offer a buffer back for the next read on this thread
///
/// A buffer whose capacity is not a class's, or whose class is full, is dropped
/// here. What it holds is left alone rather than cleared, since the length is how
/// far the buffer is known to be written.
pub fn give(bytes: Vec<u8>) {
    let capacity = bytes.capacity();
    let Some(class) = class_of(capacity) else {
        return;
    };
    if capacity_of(class) != capacity {
        return;
    }
    // A pool already torn down drops the offer, since this runs from Drop on
    // whichever thread let the last reader go.
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

    // a small payload is pooled, which is the size a point read asks for most
    #[test]
    fn a_small_payload_is_pooled() {
        const SMALL: usize = 1_200;
        assert!(
            class_of(SMALL).is_some(),
            "a small payload falls outside the pool"
        );

        let taken = take(SMALL);
        assert!(taken.capacity() >= SMALL);
        let capacity = taken.capacity();
        give(taken);

        // Back on the next read of that size rather than bought again.
        let again = take(SMALL);
        assert_eq!(again.capacity(), capacity, "the buffer did not come back");
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

        // Back at the same length, which is the case that zeroes nothing.
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
