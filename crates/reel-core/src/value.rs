//! What a read hands back: bytes, and who owns the buffer holding them
//!
//! A value derefs to its bytes and records where the buffer goes afterwards, so a
//! backend with a pool can lend a buffer rather than give it away, and one read's
//! buffer can be cut into windows returned once the last window drops. Reading
//! through the borrow copies nothing; `into_vec` copies when the buffer is shared.

use std::ops::Deref;
use std::sync::Arc;

/// One read's buffer, shared by every value cut out of it
#[derive(Debug)]
struct Block {
    bytes: Vec<u8>,
    recycle: fn(Vec<u8>),
}

impl Drop for Block {
    fn drop(&mut self) {
        (self.recycle)(std::mem::take(&mut self.bytes));
    }
}

/// Bytes a read produced, with the buffer's owner recorded
#[derive(Debug, Default)]
pub struct Value {
    held: Held,
}

/// Which of the arrangements a value's bytes are under
#[derive(Debug, Default)]
enum Held {
    /// Bytes this value has to itself, with where the buffer goes when dropped
    Owned {
        bytes: Vec<u8>,
        recycle: Option<fn(Vec<u8>)>,
    },

    /// A window into a block this value shares with its neighbours
    Window {
        block: Arc<Block>,
        at: usize,
        len: usize,
    },

    /// A window into a buffer this value has to itself
    ///
    /// What one read of a wider span than the caller asked for leaves: there are no
    /// neighbours to share the buffer with, so there is nothing to refcount.
    Cut {
        bytes: Vec<u8>,
        at: usize,
        len: usize,
        recycle: fn(Vec<u8>),
    },

    /// Bytes shared with an index that keeps them warm for the next reader
    Shared { bytes: Arc<[u8]> },

    /// Nothing, which is only ever what a taken value leaves behind
    #[default]
    Nothing,
}

impl Value {
    /// Bytes whose buffer belongs to whoever holds them
    pub fn new(bytes: Vec<u8>) -> Value {
        Value {
            held: Held::Owned {
                bytes,
                recycle: None,
            },
        }
    }

    /// Bytes shared with an index that keeps them warm
    pub fn shared(bytes: Arc<[u8]>) -> Value {
        Value {
            held: Held::Shared { bytes },
        }
    }

    /// Bytes whose buffer goes back to a pool once the reader is done
    pub fn pooled(bytes: Vec<u8>, recycle: fn(Vec<u8>)) -> Value {
        Value {
            held: Held::Owned {
                bytes,
                recycle: Some(recycle),
            },
        }
    }

    /// Cut one read's buffer into the windows the records inside it occupy
    ///
    /// The block goes back to the pool once every window has dropped, so a caller
    /// keeping one record keeps the whole run's buffer with it. A cut falling
    /// outside the buffer yields nothing for that record. The windows land in a
    /// vector the caller keeps, so a thread cutting one run after another buys one
    /// list rather than one per run.
    pub fn windows_into(
        bytes: Vec<u8>,
        recycle: fn(Vec<u8>),
        cuts: &[(usize, usize)],
        out: &mut Vec<Option<Value>>,
    ) {
        out.clear();
        out.reserve(cuts.len());
        let held = bytes.len();
        let block = Arc::new(Block { bytes, recycle });
        out.extend(cuts.iter().map(|&(at, len)| {
            let end = at.checked_add(len)?;
            (end <= held).then(|| Value {
                held: Held::Window {
                    block: Arc::clone(&block),
                    at,
                    len,
                },
            })
        }));
    }

    /// One window of a buffer nothing else holds, or nothing when the buffer is short
    ///
    /// The single-window case, which needs no refcount: this value owns the whole
    /// buffer and hands it back when it drops.
    pub fn cut(bytes: Vec<u8>, recycle: fn(Vec<u8>), at: usize, len: usize) -> Option<Value> {
        let end = at.checked_add(len)?;
        if end > bytes.len() {
            recycle(bytes);
            return None;
        }
        Some(Value {
            held: Held::Cut {
                bytes,
                at,
                len,
                recycle,
            },
        })
    }

    /// Take the buffer itself, which keeps it from going back
    ///
    /// A window has no buffer of its own to give, so it copies here.
    pub fn into_vec(mut self) -> Vec<u8> {
        match &mut self.held {
            Held::Owned { bytes, recycle } => {
                *recycle = None;
                std::mem::take(bytes)
            }
            Held::Window { block, at, len } => block.bytes[*at..*at + *len].to_vec(),
            // The window sits inside a buffer that is longer than it, so the bytes
            // are copied out rather than the buffer handed over.
            Held::Cut { bytes, at, len, .. } => bytes[*at..*at + *len].to_vec(),
            Held::Shared { bytes } => bytes.to_vec(),
            Held::Nothing => Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        match &self.held {
            Held::Owned { bytes, .. } => bytes.len(),
            Held::Window { len, .. } => *len,
            Held::Cut { len, .. } => *len,
            Held::Shared { bytes } => bytes.len(),
            Held::Nothing => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Deref for Value {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match &self.held {
            Held::Owned { bytes, .. } => bytes,
            Held::Window { block, at, len } => &block.bytes[*at..*at + *len],
            Held::Cut { bytes, at, len, .. } => &bytes[*at..*at + *len],
            Held::Shared { bytes } => bytes,
            Held::Nothing => &[],
        }
    }
}

impl AsRef<[u8]> for Value {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl From<Vec<u8>> for Value {
    fn from(bytes: Vec<u8>) -> Value {
        Value::new(bytes)
    }
}

impl From<Value> for Vec<u8> {
    fn from(value: Value) -> Vec<u8> {
        value.into_vec()
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Value) -> bool {
        **self == **other
    }
}

impl PartialEq<[u8]> for Value {
    fn eq(&self, other: &[u8]) -> bool {
        **self == *other
    }
}

impl PartialEq<Vec<u8>> for Value {
    fn eq(&self, other: &Vec<u8>) -> bool {
        **self == **other
    }
}

impl Eq for Value {}

impl Drop for Value {
    fn drop(&mut self) {
        match &mut self.held {
            Held::Owned {
                bytes,
                recycle: Some(recycle),
            } => recycle(std::mem::take(bytes)),
            Held::Cut { bytes, recycle, .. } => recycle(std::mem::take(bytes)),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    thread_local! {
        static RETURNED: Cell<usize> = const { Cell::new(0) };
    }

    fn count_it(bytes: Vec<u8>) {
        RETURNED.with(|held| held.set(held.get() + bytes.capacity()));
    }

    // a value with nowhere to go behaves as the vector it replaced
    #[test]
    fn a_plain_value_owns_its_bytes() {
        let value = Value::new(vec![1, 2, 3]);
        assert_eq!(&*value, &[1, 2, 3]);
        assert_eq!(value.into_vec(), vec![1, 2, 3]);
    }

    // a pooled value hands its buffer back when it is dropped
    #[test]
    fn a_pooled_value_goes_back() {
        RETURNED.with(|held| held.set(0));
        drop(Value::pooled(Vec::with_capacity(64), count_it));
        assert_eq!(RETURNED.with(|held| held.get()), 64);
    }

    // windows onto one block read the records that were cut out of it
    #[test]
    fn windows_read_their_own_bytes() {
        RETURNED.with(|held| held.set(0));
        let mut block = Vec::with_capacity(64);
        block.extend_from_slice(b"onetwothree");

        let mut cut = Vec::new();
        Value::windows_into(block, count_it, &[(0, 3), (3, 3), (6, 5), (9, 9)], &mut cut);

        assert_eq!(&**cut[0].as_ref().expect("first"), b"one");
        assert_eq!(&**cut[1].as_ref().expect("second"), b"two");
        assert_eq!(&**cut[2].as_ref().expect("third"), b"three");
        assert!(cut[3].is_none(), "a cut past the block yields nothing");
        assert_eq!(cut[0].as_ref().expect("first").len(), 3);
    }

    // the block goes back only once every window cut from it has been dropped
    #[test]
    fn the_last_window_returns_the_block() {
        RETURNED.with(|held| held.set(0));
        let mut block = Vec::with_capacity(64);
        block.extend_from_slice(b"onetwo");
        let mut cut = Vec::new();
        Value::windows_into(block, count_it, &[(0, 3), (3, 3)], &mut cut);

        drop(cut.pop());
        assert_eq!(
            RETURNED.with(|held| held.get()),
            0,
            "one window still holds it"
        );

        drop(cut);
        assert_eq!(
            RETURNED.with(|held| held.get()),
            64,
            "the last one gave it back"
        );
    }

    // a window has no buffer of its own, so owning its bytes copies them
    #[test]
    fn taking_a_window_copies() {
        RETURNED.with(|held| held.set(0));
        let mut block = Vec::with_capacity(64);
        block.extend_from_slice(b"onetwo");
        let mut cut = Vec::new();
        Value::windows_into(block, count_it, &[(0, 3), (3, 3)], &mut cut);

        let taken = cut.pop().expect("second").expect("window").into_vec();

        assert_eq!(taken, b"two".to_vec());
        assert_eq!(
            RETURNED.with(|held| held.get()),
            0,
            "the block is still held"
        );
        drop(cut);
        assert_eq!(RETURNED.with(|held| held.get()), 64, "and goes back after");
    }

    // one window of a buffer nothing else holds reads its own bytes
    #[test]
    fn a_cut_reads_its_window() {
        RETURNED.with(|held| held.set(0));
        let mut block = Vec::with_capacity(64);
        block.extend_from_slice(b"onetwothree");

        let cut = Value::cut(block, count_it, 3, 3).expect("a window");

        assert_eq!(&*cut, b"two");
        assert_eq!(cut.len(), 3);
        assert_eq!(
            RETURNED.with(|held| held.get()),
            0,
            "the window still holds it"
        );
        drop(cut);
        assert_eq!(
            RETURNED.with(|held| held.get()),
            64,
            "and gave it back after"
        );
    }

    // a cut past the buffer is nothing, and hands the buffer back rather than losing it
    #[test]
    fn a_cut_past_the_buffer_gives_it_back() {
        RETURNED.with(|held| held.set(0));
        let mut block = Vec::with_capacity(64);
        block.extend_from_slice(b"onetwo");

        assert!(Value::cut(block, count_it, 4, 9).is_none());
        assert_eq!(RETURNED.with(|held| held.get()), 64);
    }

    // a cut has no buffer of its own to give, so owning its bytes copies them
    #[test]
    fn taking_a_cut_copies() {
        RETURNED.with(|held| held.set(0));
        let mut block = Vec::with_capacity(64);
        block.extend_from_slice(b"onetwo");

        let taken = Value::cut(block, count_it, 0, 3)
            .expect("a window")
            .into_vec();

        assert_eq!(taken, b"one".to_vec());
        assert_eq!(RETURNED.with(|held| held.get()), 64, "the buffer went back");
    }

    // a pooled value whose bytes are taken keeps them from going back
    #[test]
    fn taking_the_bytes_keeps_them() {
        RETURNED.with(|held| held.set(0));
        let taken = Value::pooled(Vec::with_capacity(64), count_it).into_vec();
        assert_eq!(taken.capacity(), 64);
        assert_eq!(RETURNED.with(|held| held.get()), 0, "nothing went back");
    }
}
