//! Write batch operations for atomic writes across column families

use std::borrow::Cow;

/// A batch of write operations (Put/Delete) to be applied atomically
#[derive(Debug, Clone, Default)]
pub struct WriteBatch {
    ops: Vec<BatchOp>,
}

/// The column family an operation names, static for a column constant and owned
/// for a name computed at runtime
pub type ColumnName = Cow<'static, str>;

#[derive(Debug, Clone)]
pub enum BatchOp {
    Put {
        cf: ColumnName,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        cf: ColumnName,
        key: Vec<u8>,
    },
}

impl BatchOp {
    /// Column family this operation targets
    pub fn cf(&self) -> &str {
        match self {
            BatchOp::Put { cf, .. } => cf,
            BatchOp::Delete { cf, .. } => cf,
        }
    }
}

impl WriteBatch {
    /// Create a new empty write batch
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    /// Add a Put operation to the batch
    pub fn put(&mut self, cf: &'static str, key: &[u8], value: &[u8]) {
        self.put_owned(cf, key.to_vec(), value.to_vec());
    }

    /// Add a Put operation whose family name the caller already holds
    pub fn put_named(&mut self, cf: ColumnName, key: Vec<u8>, value: Vec<u8>) {
        self.ops.push(BatchOp::Put { cf, key, value });
    }

    /// Add a Put operation, taking ownership of key and value
    ///
    /// The borrowing form copies the whole value, so freshly serialized bytes
    /// belong here.
    pub fn put_owned(&mut self, cf: &'static str, key: Vec<u8>, value: Vec<u8>) {
        self.ops.push(BatchOp::Put {
            cf: Cow::Borrowed(cf),
            key,
            value,
        });
    }

    /// Add a Delete operation to the batch
    pub fn delete(&mut self, cf: &'static str, key: &[u8]) {
        self.delete_owned(cf, key.to_vec());
    }

    /// Add a Delete operation whose family name the caller already holds
    pub fn delete_named(&mut self, cf: ColumnName, key: Vec<u8>) {
        self.ops.push(BatchOp::Delete { cf, key });
    }

    /// Add a Delete operation, taking ownership of the key
    pub fn delete_owned(&mut self, cf: &'static str, key: Vec<u8>) {
        self.ops.push(BatchOp::Delete {
            cf: Cow::Borrowed(cf),
            key,
        });
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Get the number of operations in the batch
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Get an iterator over the operations
    pub fn iter(&self) -> impl Iterator<Item = &BatchOp> {
        self.ops.iter()
    }
}

/// Consuming iteration that hands each staged payload over without a copy
impl IntoIterator for WriteBatch {
    type Item = BatchOp;
    type IntoIter = std::vec::IntoIter<BatchOp>;

    fn into_iter(self) -> Self::IntoIter {
        self.ops.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_ops() {
        let mut batch = WriteBatch::new();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);

        batch.put("cf1", b"key1", b"value1");
        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 1);

        batch.delete("cf2", b"key2");
        assert_eq!(batch.len(), 2);

        batch.put("cf1", b"key3", b"value3");
        assert_eq!(batch.len(), 3);
    }
}
