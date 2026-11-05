//! A set of writes applied whole, kept in buffers the caller reuses

use crate::column_id::ColumnId;
use crate::units::ByteCount;

/// What one entry in a batch does
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchKind {
    /// Write a value
    Put,
    /// Write a grave
    Delete,
    /// Remove the keys in a range
    DeleteRange,
}

/// One entry's place in the batch's bytes
#[derive(Clone, Copy, Debug)]
pub struct BatchEntry {
    pub kind: BatchKind,
    pub column: ColumnId,
    pub start: ByteCount,
    pub first_length: ByteCount,
    pub second_length: ByteCount,
}

/// One operation read back from a batch
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchOperation<'batch> {
    /// Write a value
    Put {
        column: ColumnId,
        key: &'batch [u8],
        value: &'batch [u8],
    },
    /// Write a grave
    Delete { column: ColumnId, key: &'batch [u8] },
    /// Remove every key in [start, end)
    DeleteRange {
        column: ColumnId,
        start: &'batch [u8],
        end: &'batch [u8],
    },
}

/// Puts, deletes and range deletes on columns of one volume, applied in order
#[derive(Debug, Default)]
pub struct WriteBatch {
    bytes: Vec<u8>,
    entries: Vec<BatchEntry>,
}

impl WriteBatch {
    /// Start an empty batch
    pub fn new() -> Self {
        Self::default()
    }

    /// Forget every entry and keep the buffers
    pub fn clear(&mut self) {
        self.bytes.clear();
        self.entries.clear();
    }

    /// Add a put
    pub fn put(&mut self, column: ColumnId, key: &[u8], value: &[u8]) {
        self.push(BatchKind::Put, column, key, value);
    }

    /// Add a delete
    pub fn delete(&mut self, column: ColumnId, key: &[u8]) {
        self.push(BatchKind::Delete, column, key, &[]);
    }

    /// Add a range delete of [start, end)
    pub fn delete_range(&mut self, column: ColumnId, start: &[u8], end: &[u8]) {
        self.push(BatchKind::DeleteRange, column, start, end);
    }

    /// Count the entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check whether the batch holds no entries
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Read the bytes every entry carries in total
    pub fn byte_length(&self) -> ByteCount {
        ByteCount(self.bytes.len() as u64)
    }

    /// Read the entry at an index
    pub fn operation(&self, index: usize) -> Option<BatchOperation<'_>> {
        let entry = self.entries.get(index)?;
        let start = entry.start.0 as usize;
        let middle = start.checked_add(entry.first_length.0 as usize)?;
        let end = middle.checked_add(entry.second_length.0 as usize)?;
        let first = self.bytes.get(start..middle)?;
        let second = self.bytes.get(middle..end)?;

        Some(match entry.kind {
            BatchKind::Put => BatchOperation::Put {
                column: entry.column,
                key: first,
                value: second,
            },
            BatchKind::Delete => BatchOperation::Delete {
                column: entry.column,
                key: first,
            },
            BatchKind::DeleteRange => BatchOperation::DeleteRange {
                column: entry.column,
                start: first,
                end: second,
            },
        })
    }

    /// Append one entry's bytes and its place
    pub fn push(&mut self, kind: BatchKind, column: ColumnId, first: &[u8], second: &[u8]) {
        let start = ByteCount(self.bytes.len() as u64);
        self.bytes.extend_from_slice(first);
        self.bytes.extend_from_slice(second);
        self.entries.push(BatchEntry {
            kind,
            column,
            start,
            first_length: ByteCount(first.len() as u64),
            second_length: ByteCount(second.len() as u64),
        });
    }
}
