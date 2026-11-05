//! The answers to a batched get, kept in buffers the caller reuses

use crate::units::ByteCount;

/// Where one found value sits in the list's bytes
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValueSpan {
    pub start: ByteCount,
    pub length: ByteCount,
}

/// One answer per key, in the order of the keys asked for
#[derive(Debug, Default)]
pub struct ValueList {
    bytes: Vec<u8>,
    spans: Vec<Option<ValueSpan>>,
}

impl ValueList {
    /// Start an empty list
    pub fn new() -> Self {
        Self::default()
    }

    /// Forget every answer and keep the buffers
    pub fn clear(&mut self) {
        self.bytes.clear();
        self.spans.clear();
    }

    /// Add the answer for a key with no value
    pub fn push_missing(&mut self) {
        self.spans.push(None);
    }

    /// Add the answer for a key with a value
    pub fn push_found(&mut self, value: &[u8]) {
        let span = ValueSpan {
            start: ByteCount(self.bytes.len() as u64),
            length: ByteCount(value.len() as u64),
        };
        self.bytes.extend_from_slice(value);
        self.spans.push(Some(span));
    }

    /// Count the answers
    pub fn len(&self) -> usize {
        self.spans.len()
    }

    /// Check whether the list holds no answers
    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    /// Read the answer at an index
    pub fn value(&self, index: usize) -> Option<&[u8]> {
        let span = (*self.spans.get(index)?)?;
        let start = span.start.0 as usize;
        let end = start.checked_add(span.length.0 as usize)?;
        self.bytes.get(start..end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // answers come back in order, and clear keeps nothing
    #[test]
    fn answers() {
        let mut list = ValueList::new();

        list.push_found(b"first");
        list.push_missing();
        list.push_found(b"");

        assert_eq!(list.len(), 3);
        assert_eq!(list.value(0), Some(&b"first"[..]));
        assert_eq!(list.value(1), None);
        assert_eq!(list.value(2), Some(&b""[..]));
        assert_eq!(list.value(3), None);

        list.clear();
        assert!(list.is_empty());
    }
}
