//! In-memory implementation of the Store trait for testing

use std::collections::HashMap;
use std::sync::RwLock;

use reel_core::{batch::BatchOp, Direction, Result, Store, StoreIter, Value, WriteBatch};

/// One column family's keys and the values they hold
type ColumnData = HashMap<Vec<u8>, Vec<u8>>;

/// In-memory key-value store using HashMap
///
/// Thread-safe through an RwLock, with column families created on first write.
pub struct MemoryStore {
    data: RwLock<HashMap<String, ColumnData>>,
}

impl MemoryStore {
    /// Create a new empty in-memory store
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    /// Total byte size of all keys and values across all column families.
    pub fn total_size_bytes(&self) -> usize {
        let data = self.data.read().unwrap();
        data.values()
            .flat_map(|cf| cf.iter())
            .map(|(k, v)| k.len() + v.len())
            .sum()
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Store for MemoryStore {
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Value>> {
        let data = self.data.read().unwrap();
        Ok(data
            .get(cf)
            .and_then(|cf_data| cf_data.get(key))
            .cloned()
            .map(Value::new))
    }

    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let mut data = self.data.write().unwrap();
        data.entry(cf.to_string())
            .or_default()
            .insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&self, cf: &str, key: &[u8]) -> Result<()> {
        let mut data = self.data.write().unwrap();
        if let Some(cf_data) = data.get_mut(cf) {
            cf_data.remove(key);
        }
        Ok(())
    }

    fn contains(&self, cf: &str, key: &[u8]) -> Result<bool> {
        let data = self.data.read().unwrap();
        Ok(data
            .get(cf)
            .map(|cf_data| cf_data.contains_key(key))
            .unwrap_or(false))
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        let mut data = self.data.write().unwrap();

        for op in batch.iter() {
            match op {
                BatchOp::Put { cf, key, value } => {
                    data.entry(cf.to_string())
                        .or_default()
                        .insert(key.clone(), value.clone());
                }
                BatchOp::Delete { cf, key } => {
                    if let Some(cf_data) = data.get_mut(cf.as_ref()) {
                        cf_data.remove(key);
                    }
                }
            }
        }

        Ok(())
    }

    fn iter(&self, cf: &str) -> Result<StoreIter<'_>> {
        let data = self.data.read().unwrap();
        let mut entries: Vec<_> = data
            .get(cf)
            .map(|cf_data| {
                cf_data
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(Box::new(
            entries
                .into_iter()
                .map(|(key, value)| (key, Value::new(value))),
        ) as StoreIter<'_>)
    }

    fn count_prefix(&self, cf: &str, prefix: &[u8]) -> Result<u64> {
        // Counting in place, so neither the keys nor the values are cloned.
        let data = self.data.read().unwrap();
        let count = data
            .get(cf)
            .map(|cf_data| cf_data.keys().filter(|key| key.starts_with(prefix)).count() as u64)
            .unwrap_or(0);

        Ok(count)
    }

    fn bytes_prefix(&self, cf: &str, prefix: &[u8]) -> Result<Option<u64>> {
        // Summed in place: the values are already in memory, so nothing is faulted
        // in to weigh them.
        let data = self.data.read().unwrap();
        let bytes = data
            .get(cf)
            .map(|cf_data| {
                cf_data
                    .iter()
                    .filter(|(key, _)| key.starts_with(prefix))
                    .map(|(_, value)| value.len() as u64)
                    .sum()
            })
            .unwrap_or(0);

        Ok(Some(bytes))
    }

    fn iter_prefix(&self, cf: &str, prefix: &[u8]) -> Result<StoreIter<'_>> {
        let data = self.data.read().unwrap();
        let prefix = prefix.to_vec();
        let mut entries: Vec<_> = data
            .get(cf)
            .map(|cf_data| {
                cf_data
                    .iter()
                    .filter(|(k, _)| k.starts_with(&prefix))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(Box::new(
            entries
                .into_iter()
                .map(|(key, value)| (key, Value::new(value))),
        ) as StoreIter<'_>)
    }

    fn iter_from(&self, cf: &str, start: &[u8], direction: Direction) -> Result<StoreIter<'_>> {
        let data = self.data.read().unwrap();
        let start = start.to_vec();
        let mut entries: Vec<_> = data
            .get(cf)
            .map(|cf_data| {
                cf_data
                    .iter()
                    .filter(|(k, _)| match direction {
                        Direction::Asc => k.as_slice() >= start.as_slice(),
                        Direction::Desc => k.as_slice() <= start.as_slice(),
                    })
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();

        match direction {
            Direction::Asc => entries.sort_by(|a, b| a.0.cmp(&b.0)),
            Direction::Desc => entries.sort_by(|a, b| b.0.cmp(&a.0)),
        }
        Ok(Box::new(
            entries
                .into_iter()
                .map(|(key, value)| (key, Value::new(value))),
        ) as StoreIter<'_>)
    }

    fn iter_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<StoreIter<'_>> {
        let data = self.data.read().unwrap();
        let start = start.to_vec();
        let end = end.to_vec();
        let mut entries: Vec<_> = data
            .get(cf)
            .map(|cf_data| {
                cf_data
                    .iter()
                    .filter(|(k, _)| {
                        k.as_slice() >= start.as_slice() && k.as_slice() < end.as_slice()
                    })
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(Box::new(
            entries
                .into_iter()
                .map(|(key, value)| (key, Value::new(value))),
        ) as StoreIter<'_>)
    }

    fn actual_size_bytes(&self) -> Result<u64> {
        Ok(self.total_size_bytes() as u64)
    }

    fn available_disk_bytes(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    fn live_data_size_bytes(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    fn key_count_estimate(&self, _cf: &str) -> Result<Option<u64>> {
        Ok(None)
    }

    fn reclaim_space(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_empty() {
        let store = MemoryStore::new();
        assert!(store.get("test", b"key").unwrap().is_none());
        assert!(!store.contains("test", b"key").unwrap());
    }

    #[test]
    fn put_get() {
        let store = MemoryStore::new();

        store.put("test", b"key", b"value").unwrap();

        let result = store.get("test", b"key").unwrap();
        assert_eq!(result, Some(Value::new(b"value".to_vec())));
    }

    #[test]
    fn put_overwrites() {
        let store = MemoryStore::new();

        store.put("test", b"key", b"value1").unwrap();
        store.put("test", b"key", b"value2").unwrap();

        let result = store.get("test", b"key").unwrap();
        assert_eq!(result, Some(Value::new(b"value2".to_vec())));
    }

    #[test]
    fn delete() {
        let store = MemoryStore::new();

        store.put("test", b"key", b"value").unwrap();
        assert!(store.contains("test", b"key").unwrap());

        store.delete("test", b"key").unwrap();
        assert!(!store.contains("test", b"key").unwrap());
        assert_eq!(store.get("test", b"key").unwrap(), None);
    }

    #[test]
    fn delete_nonexistent() {
        let store = MemoryStore::new();

        store.delete("test", b"nonexistent").unwrap();
    }

    #[test]
    fn multi_cf() {
        let store = MemoryStore::new();

        store.put("cf1", b"key", b"value1").unwrap();
        store.put("cf2", b"key", b"value2").unwrap();
        store.put("cf3", b"key", b"value3").unwrap();

        assert_eq!(
            store.get("cf1", b"key").unwrap(),
            Some(Value::new(b"value1".to_vec()))
        );
        assert_eq!(
            store.get("cf2", b"key").unwrap(),
            Some(Value::new(b"value2".to_vec()))
        );
        assert_eq!(
            store.get("cf3", b"key").unwrap(),
            Some(Value::new(b"value3".to_vec()))
        );

        store.delete("cf2", b"key").unwrap();
        assert_eq!(
            store.get("cf1", b"key").unwrap(),
            Some(Value::new(b"value1".to_vec()))
        );
        assert_eq!(store.get("cf2", b"key").unwrap(), None);
        assert_eq!(
            store.get("cf3", b"key").unwrap(),
            Some(Value::new(b"value3".to_vec()))
        );
    }

    #[test]
    fn binary_data() {
        let store = MemoryStore::new();

        let key = vec![0u8, 1, 2, 255, 254];
        let value = vec![10u8, 20, 30, 200, 100];

        store.put("test", &key, &value).unwrap();
        assert_eq!(store.get("test", &key).unwrap(), Some(Value::new(value)));
    }

    #[test]
    fn batch_empty() {
        let store = MemoryStore::new();
        let batch = WriteBatch::new();

        store.write_batch(batch).unwrap();
    }

    #[test]
    fn batch_atomic() {
        let store = MemoryStore::new();

        store.put("test", b"key1", b"old1").unwrap();
        store.put("test", b"key2", b"old2").unwrap();

        let mut batch = WriteBatch::new();
        batch.put("test", b"key1", b"new1");
        batch.put("test", b"key3", b"new3");
        batch.delete("test", b"key2");

        store.write_batch(batch).unwrap();

        assert_eq!(
            store.get("test", b"key1").unwrap(),
            Some(Value::new(b"new1".to_vec()))
        );
        assert_eq!(store.get("test", b"key2").unwrap(), None);
        assert_eq!(
            store.get("test", b"key3").unwrap(),
            Some(Value::new(b"new3".to_vec()))
        );
    }

    #[test]
    fn batch_multi_cf() {
        let store = MemoryStore::new();

        let mut batch = WriteBatch::new();
        batch.put("cf1", b"key", b"value1");
        batch.put("cf2", b"key", b"value2");
        batch.delete("cf3", b"key");

        store.write_batch(batch).unwrap();

        assert_eq!(
            store.get("cf1", b"key").unwrap(),
            Some(Value::new(b"value1".to_vec()))
        );
        assert_eq!(
            store.get("cf2", b"key").unwrap(),
            Some(Value::new(b"value2".to_vec()))
        );
        assert_eq!(store.get("cf3", b"key").unwrap(), None);
    }

    #[test]
    fn iter() {
        let store = MemoryStore::new();

        store.put("test", b"c", b"3").unwrap();
        store.put("test", b"a", b"1").unwrap();
        store.put("test", b"b", b"2").unwrap();

        let entries: Vec<_> = store.iter("test").unwrap().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, b"a".to_vec());
        assert_eq!(&*entries[0].1, b"1");
        assert_eq!(entries[1].0, b"b".to_vec());
        assert_eq!(&*entries[1].1, b"2");
        assert_eq!(entries[2].0, b"c".to_vec());
        assert_eq!(&*entries[2].1, b"3");
    }

    #[test]
    fn iter_prefix() {
        let store = MemoryStore::new();

        store.put("test", b"user:1", b"alice").unwrap();
        store.put("test", b"user:2", b"bob").unwrap();
        store.put("test", b"post:1", b"hello").unwrap();
        store.put("test", b"user:3", b"charlie").unwrap();

        let users: Vec<_> = store.iter_prefix("test", b"user:").unwrap().collect();
        assert_eq!(users.len(), 3);
        assert_eq!(users[0].1, b"alice".to_vec());
        assert_eq!(users[1].1, b"bob".to_vec());
        assert_eq!(users[2].1, b"charlie".to_vec());

        let posts: Vec<_> = store.iter_prefix("test", b"post:").unwrap().collect();
        assert_eq!(posts.len(), 1);
    }

    #[test]
    fn iter_from() {
        let store = MemoryStore::new();

        store.put("test", b"a", b"1").unwrap();
        store.put("test", b"b", b"2").unwrap();
        store.put("test", b"c", b"3").unwrap();
        store.put("test", b"d", b"4").unwrap();

        let asc: Vec<_> = store
            .iter_from("test", b"b", Direction::Asc)
            .unwrap()
            .collect();
        assert_eq!(asc.len(), 3);
        assert_eq!(asc[0].0, b"b".to_vec());
        assert_eq!(asc[1].0, b"c".to_vec());
        assert_eq!(asc[2].0, b"d".to_vec());

        let desc: Vec<_> = store
            .iter_from("test", b"c", Direction::Desc)
            .unwrap()
            .collect();
        assert_eq!(desc.len(), 3);
        assert_eq!(desc[0].0, b"c".to_vec());
        assert_eq!(desc[1].0, b"b".to_vec());
        assert_eq!(desc[2].0, b"a".to_vec());
    }

    #[test]
    fn iter_range() {
        let store = MemoryStore::new();

        store.put("test", b"a", b"1").unwrap();
        store.put("test", b"b", b"2").unwrap();
        store.put("test", b"c", b"3").unwrap();
        store.put("test", b"d", b"4").unwrap();

        let range: Vec<_> = store.iter_range("test", b"b", b"d").unwrap().collect();
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].0, b"b".to_vec());
        assert_eq!(range[1].0, b"c".to_vec());
    }

    #[test]
    fn concurrent() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(MemoryStore::new());
        let mut handles = vec![];

        for i in 0..10 {
            let store_clone = Arc::clone(&store);
            let handle = thread::spawn(move || {
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                store_clone
                    .put("test", key.as_bytes(), value.as_bytes())
                    .unwrap();
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for i in 0..10 {
            let key = format!("key{}", i);
            let expected_value = format!("value{}", i);
            assert_eq!(
                store.get("test", key.as_bytes()).unwrap(),
                Some(Value::new(expected_value.as_bytes().to_vec()))
            );
        }
    }
}
