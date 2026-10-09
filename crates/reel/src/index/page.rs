//! One page of keys taken off the index, with the size each one resolves to

use crate::index::entry::Entry;

/// A run of keys in index order and, when asked for, where each one's record sits
#[derive(Debug, Default)]
pub struct KeyPage {
    /// Every key in the page, packed end to end at the column's width
    keys: Vec<u8>,

    /// Where each key's record sits, empty on a keys-only page
    found: Vec<Entry>,

    /// The keys' packing stride, meaningful only while `ends` is empty
    width: usize,

    /// Where each key ends, filled only once a page holds two lengths
    ends: Vec<u32>,

    /// How many keys the page holds, which the packed bytes alone cannot say
    count: usize,

    /// Whether a caller will read the entries, so a keys-only walk skips them
    keeps_found: bool,

    /// Whether graves and covered entries come out too, for a merge that checks them itself
    keeps_graves: bool,
}

impl KeyPage {
    /// A page that keeps each key's entry, for a walk that reads the records
    pub fn with_lens() -> KeyPage {
        KeyPage {
            keeps_found: true,
            ..KeyPage::default()
        }
    }

    /// Whether the walk filling this page reads the records it points at
    pub fn keeps_found(&self) -> bool {
        self.keeps_found
    }

    /// The map's half of a merge, keeping graves and covered entries to drop deleted sealed keys
    pub fn merging() -> KeyPage {
        KeyPage {
            keeps_found: true,
            keeps_graves: true,
            ..KeyPage::default()
        }
    }

    /// Whether the page holds graves and covered entries for its reader to check
    pub fn keeps_graves(&self) -> bool {
        self.keeps_graves
    }

    /// Drop the page's contents, keeping its allocations for the next fill
    pub fn clear(&mut self) {
        self.keys.clear();
        self.ends.clear();
        self.found.clear();
        self.width = 0;
        self.count = 0;
    }

    /// Reserve room for this many more keys at a width, so a fill never regrows its buffers
    pub fn reserve(&mut self, count: usize, width: usize) {
        self.keys.reserve(count * width);
        if self.keeps_found {
            self.found.reserve(count);
        }
    }

    /// Add one key and its entry, taking the stride from the first key
    pub fn push(&mut self, key: &[u8], found: Entry) {
        // A second width switches the page to explicit ends, backfilled for the keys so far
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
        self.count += 1;
    }

    /// Add a run of same-width keys and their entries, false with nothing added on a width mismatch
    pub fn push_packed(&mut self, keys: &[u8], width: usize, found: &[Entry]) -> bool {
        if found.is_empty() {
            return true;
        }
        if !self.ends.is_empty() || (self.count > 0 && width != self.width) || width == 0 {
            return false;
        }
        if self.count == 0 {
            self.width = width;
        }
        self.keys.extend_from_slice(keys);
        if self.keeps_found {
            self.found.extend_from_slice(found);
        }
        self.count += found.len();
        true
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

    /// The payload length the key at a position resolves to, or zero on a keys-only page
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
