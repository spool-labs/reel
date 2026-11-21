//! The answers to a batched get, kept in buffers the caller reuses

use crate::units::ByteCount;

/// A value longer than this keeps a buffer of its own, which the read filled and the caller can take with no copy. A shorter one
/// is copied into the list's shared bytes
pub const OWNED_VALUE_BYTES: usize = 8 * 1024;

/// Where one found value sits in the list's bytes
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValueSpan {
    pub start: ByteCount,
    pub length: ByteCount,
}

/// How one key was answered
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoredAnswer {
    /// The key has a value in the list's shared bytes
    Found(ValueSpan),
    /// The key has a value in a buffer of its own, at this place in the list's owned buffers
    Owned(usize),
    /// The key has no value
    Missing,
    /// The key's record failed its check
    Corrupt,
}

/// One key's answer, read back from a list
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueAnswer<'list> {
    /// The key has this value
    Found(&'list [u8]),
    /// The key has no value
    Missing,
    /// The key's record failed its check, and the caller can repair it from peers
    Corrupt,
}

/// One key's answer, taken out of a list
#[derive(Debug, Eq, PartialEq)]
pub enum TakenAnswer {
    /// The key has this value
    Found(Vec<u8>),
    /// The key has no value
    Missing,
    /// The key's record failed its check
    Corrupt,
}

/// The buffers a read lends from its list: one a value lands in on its way to the list, and one a batch reads records into
#[derive(Debug, Default)]
pub struct ReadBuffers {
    pub value: Vec<u8>,
    pub records: Vec<u8>,
}

/// One answer per key, in the order of the keys asked for
#[derive(Debug, Default)]
pub struct ValueList {
    bytes: Vec<u8>,
    answers: Vec<StoredAnswer>,
    buffers: ReadBuffers,
    /// The buffers of long values the list answers with now
    owned: Vec<Vec<u8>>,
    /// Buffers of long values a cleared list answered with, kept for the next reads to fill
    spare: Vec<Vec<u8>>,
}

impl ValueList {
    /// Start an empty list
    pub fn new() -> Self {
        Self::default()
    }

    /// Forget every answer and keep the buffers
    pub fn clear(&mut self) {
        self.bytes.clear();
        self.answers.clear();
        for mut buffer in self.owned.drain(..) {
            buffer.clear();
            self.spare.push(buffer);
        }
    }

    /// Add the answer for a key with no value
    pub fn push_missing(&mut self) {
        self.answers.push(StoredAnswer::Missing);
    }

    /// Add the answer for a key whose record failed its check
    pub fn push_corrupt(&mut self) {
        self.answers.push(StoredAnswer::Corrupt);
    }

    /// Add the answer for a key with a value, copied into the list's bytes
    pub fn push_found(&mut self, value: &[u8]) {
        let span = ValueSpan {
            start: ByteCount(self.bytes.len() as u64),
            length: ByteCount(value.len() as u64),
        };
        self.bytes.extend_from_slice(value);
        self.answers.push(StoredAnswer::Found(span));
    }

    /// Add the answer for a key with a value a read filled into `buffer`. A long one swaps into the list, with no copy, and
    /// `buffer` gets an empty one to fill next, a spare's room when the list has one. A short one is copied
    pub fn push_filled(&mut self, buffer: &mut Vec<u8>) {
        if buffer.len() <= OWNED_VALUE_BYTES {
            self.push_found(buffer);
            return;
        }
        let next = self.spare.pop().unwrap_or_default();
        let value = std::mem::replace(buffer, next);
        self.answers.push(StoredAnswer::Owned(self.owned.len()));
        self.owned.push(value);
    }

    /// Lend out the buffers a read fills on its way here, which `put_buffers` hands back so they're kept for next time
    pub fn take_buffers(&mut self) -> ReadBuffers {
        std::mem::take(&mut self.buffers)
    }

    /// Keep lent buffers
    pub fn put_buffers(&mut self, buffers: ReadBuffers) {
        self.buffers = buffers;
    }

    /// Count the answers
    pub fn len(&self) -> usize {
        self.answers.len()
    }

    /// Check whether the list holds no answers
    pub fn is_empty(&self) -> bool {
        self.answers.is_empty()
    }

    /// Check any key's record failed its check
    pub fn has_corrupt(&self) -> bool {
        self.answers.iter().any(|answer| matches!(answer, StoredAnswer::Corrupt))
    }

    /// Count the answers that failed their check
    pub fn corrupt_count(&self) -> u64 {
        self.answers.iter().filter(|answer| matches!(answer, StoredAnswer::Corrupt)).count() as u64
    }

    /// Read the answer at an index
    pub fn answer(&self, index: usize) -> Option<ValueAnswer<'_>> {
        match self.answers.get(index)? {
            StoredAnswer::Found(span) => {
                let start = span.start.0 as usize;
                let end = start.checked_add(span.length.0 as usize)?;
                Some(ValueAnswer::Found(self.bytes.get(start..end)?))
            },
            StoredAnswer::Owned(at) => Some(ValueAnswer::Found(self.owned.get(*at)?)),
            StoredAnswer::Missing => Some(ValueAnswer::Missing),
            StoredAnswer::Corrupt => Some(ValueAnswer::Corrupt),
        }
    }

    /// Take the answer at an index out of the list: a long value's own buffer moves out with no copy, and a short one is copied.
    /// A long value taken twice comes back empty the second time
    pub fn take_answer(&mut self, index: usize) -> Option<TakenAnswer> {
        match *self.answers.get(index)? {
            StoredAnswer::Owned(at) => {
                let mut value = std::mem::take(self.owned.get_mut(at)?);
                // a reused list may have read a short long value into a buffer far larger, which a caller keeping it would hold
                if value.capacity() > 2 * value.len() {
                    value.shrink_to_fit();
                }
                Some(TakenAnswer::Found(value))
            },
            StoredAnswer::Found(_span) => match self.answer(index)? {
                ValueAnswer::Found(value) => Some(TakenAnswer::Found(value.to_vec())),
                ValueAnswer::Missing | ValueAnswer::Corrupt => None,
            },
            StoredAnswer::Missing => Some(TakenAnswer::Missing),
            StoredAnswer::Corrupt => Some(TakenAnswer::Corrupt),
        }
    }
}
