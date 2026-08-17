//! One page of keys taken off the index, with the size each one resolves to
//!
//! The page carries where every key's record sits, because the index had the entry
//! in hand when it copied the key out and asking again costs a lock and a descent
//! per record. Keys are packed end to end at the column's width, so a page is one
//! allocation rather than one per key.

use std::sync::Arc;

use crate::index::entry::Entry;

/// A run of keys in index order and, when asked for, where each one's record sits
#[derive(Debug, Default)]
pub struct KeyPage {
    /// Every key in the page, packed end to end at the column's width
    keys: Vec<u8>,

    /// Where each key's record sits, empty on a keys-only page
    found: Vec<Entry>,

    /// The value for a key whose column carries it resident, empty otherwise
    carried: Vec<Option<Arc<[u8]>>>,

    /// Stride the keys were packed at, meaningful only while the ends are empty
    width: usize,

    /// Where each key ends, filled only once a page holds two lengths
    ends: Vec<u32>,

    /// How many keys the page holds, which the packed bytes alone cannot say
    count: usize,

    /// Whether a caller will read the entries, so a keys-only walk skips them
    keeps_found: bool,

    /// Whether a caller will read the carried values, so a fill can skip them
    keeps_carried: bool,
}

impl KeyPage {
    /// A page that carries where each key's record is, for a walk that reads them
    pub fn with_lens() -> KeyPage {
        KeyPage {
            keeps_found: true,
            keeps_carried: true,
            ..KeyPage::default()
        }
    }

    /// The same page without the values a carrying column keeps beside its entries
    ///
    /// Saves a carrying column a map lookup and a refcount per key when the walk
    /// reads where a record sits and never its bytes.
    pub fn entries_only() -> KeyPage {
        KeyPage {
            keeps_found: true,
            keeps_carried: false,
            ..KeyPage::default()
        }
    }

    /// Whether a fill should look up the value a carrying column holds for a key
    pub fn keeps_carried(&self) -> bool {
        self.keeps_carried
    }

    /// Drop the page's contents, keeping its allocations for the next fill
    pub fn clear(&mut self) {
        self.keys.clear();
        self.ends.clear();
        self.found.clear();
        self.carried.clear();
        self.width = 0;
        self.count = 0;
    }

    /// Add one key and the payload length its record holds
    ///
    /// The page takes its stride from whichever key is added first.
    pub fn push(&mut self, key: &[u8], found: Entry) {
        self.push_carried(key, found, None);
    }

    /// Add one key with the value its column carries beside the index
    pub fn push_carried(&mut self, key: &[u8], found: Entry, carried: Option<Arc<[u8]>>) {
        // A page of one width is cut by arithmetic. A second width ends that, so
        // the ends are filled in for the keys already packed and kept from then on.
        if self.ends.is_empty() && self.count > 0 && key.len() != self.width {
            self.ends
                .extend((1..=self.count).map(|at| (at * self.width) as u32));
        }
        if self.count == 0 {
            self.width = key.len();
        }
        self.keys.extend_from_slice(key);
        if !self.ends.is_empty() {
            self.ends.push(self.keys.len() as u32);
        }
        if self.keeps_found {
            self.found.push(found);
        }
        if self.keeps_carried {
            self.carried.push(carried);
        }
        self.count += 1;
    }

    /// Take the carried value at a position, leaving nothing behind
    pub fn take_carried(&mut self, at: usize) -> Option<Arc<[u8]>> {
        self.carried.get_mut(at).and_then(Option::take)
    }

    /// How many keys the page holds
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The key at a position, copied out at the page's width
    pub fn key_at(&self, at: usize) -> Vec<u8> {
        self.key_ref(at).unwrap_or_default().to_vec()
    }

    /// The key at a position without copying it, for a reader that only compares
    pub fn key_ref(&self, at: usize) -> Option<&[u8]> {
        if at >= self.count {
            return None;
        }
        if self.ends.is_empty() {
            return self.keys.get(at * self.width..(at + 1) * self.width);
        }
        let start = match at {
            0 => 0,
            _ => *self.ends.get(at - 1)? as usize,
        };
        self.keys.get(start..*self.ends.get(at)? as usize)
    }

    /// Payload bytes the key at a position resolves to, or zero on a keys-only page
    pub fn len_at(&self, at: usize) -> u64 {
        self.found_at(at)
            .map(|found| u64::from(found.loc.len))
            .unwrap_or(0)
    }

    /// Where the key at a position resolves to, on a page that kept its entries
    pub fn found_at(&self, at: usize) -> Option<Entry> {
        self.found.get(at).copied()
    }
}
