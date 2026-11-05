//! The answers to a batched get, kept in buffers the caller reuses

use crate::units::ByteCount;

/// Where one found value sits in the list's bytes
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValueSpan {
    pub start: ByteCount,
    pub length: ByteCount,
}

/// How one key was answered
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoredAnswer {
    /// The key has a value
    Found(ValueSpan),
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

/// One answer per key, in the order of the keys asked for
#[derive(Debug, Default)]
pub struct ValueList {
    bytes: Vec<u8>,
    answers: Vec<StoredAnswer>,
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
    }

    /// Add the answer for a key with no value
    pub fn push_missing(&mut self) {
        self.answers.push(StoredAnswer::Missing);
    }

    /// Add the answer for a key whose record failed its check
    pub fn push_corrupt(&mut self) {
        self.answers.push(StoredAnswer::Corrupt);
    }

    /// Add the answer for a key with a value
    pub fn push_found(&mut self, value: &[u8]) {
        let span = ValueSpan {
            start: ByteCount(self.bytes.len() as u64),
            length: ByteCount(value.len() as u64),
        };
        self.bytes.extend_from_slice(value);
        self.answers.push(StoredAnswer::Found(span));
    }

    /// Count the answers
    pub fn len(&self) -> usize {
        self.answers.len()
    }

    /// Check whether the list holds no answers
    pub fn is_empty(&self) -> bool {
        self.answers.is_empty()
    }

    /// Read the answer at an index
    pub fn answer(&self, index: usize) -> Option<ValueAnswer<'_>> {
        match self.answers.get(index)? {
            StoredAnswer::Found(span) => {
                let start = span.start.0 as usize;
                let end = start.checked_add(span.length.0 as usize)?;
                Some(ValueAnswer::Found(self.bytes.get(start..end)?))
            },
            StoredAnswer::Missing => Some(ValueAnswer::Missing),
            StoredAnswer::Corrupt => Some(ValueAnswer::Corrupt),
        }
    }
}
