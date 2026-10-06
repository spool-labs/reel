//! One page of keys taken off the index, with the size each one resolves to
//!
//! The page carries where every key's record sits, because the index had the entry
//! in hand when it copied the key out and asking again costs a lock and a descent
//! per record. Keys are packed end to end at the column's width, so a page is one
//! allocation rather than one per key.

use reel_core::Value;

use crate::index::entry::Entry;

/// A run of keys in index order and, when asked for, where each one's record sits
#[derive(Debug, Default)]
pub struct KeyPage {
    /// Every key in the page, packed end to end at the column's width
    keys: Vec<u8>,

    /// Where each key's record sits, empty on a keys-only page
    found: Vec<Entry>,

    /// Stride the keys were packed at, meaningful only while the ends are empty
    width: usize,

    /// Where each key ends, filled only once a page holds two lengths
    ends: Vec<u32>,

    /// How many keys the page holds, which the packed bytes alone cannot say
    count: usize,

    /// Whether a caller will read the entries, so a keys-only walk skips them
    keeps_found: bool,

    /// Whether the caller reads every payload, so a walk that met them may hand them over
    reads_payloads: bool,

    /// Whether graves and covered entries come out too, for a merge that judges them itself
    keeps_graves: bool,

    /// Payloads a walk read with their keys, one place a key once the first arrives
    payloads: Vec<Option<Value>>,
}

impl KeyPage {
    /// A page that carries where each key's record is, for a walk that reads them
    pub fn with_lens() -> KeyPage {
        KeyPage {
            keeps_found: true,
            ..KeyPage::default()
        }
    }

    /// A page for a caller that reads every payload, so a walk that reads a record whole hands it over
    pub fn reading() -> KeyPage {
        KeyPage {
            keeps_found: true,
            reads_payloads: true,
            ..KeyPage::default()
        }
    }

    /// Whether the walk filling this page reads the records it points at
    pub fn keeps_found(&self) -> bool {
        self.keeps_found
    }

    /// Whether the caller reads every payload, so a payload read beside its key is worth keeping
    pub fn reads_payloads(&self) -> bool {
        self.reads_payloads
    }

    /// The map's half of a merge, graves and covered entries included
    ///
    /// The merge drops a sealed key the map has deleted, so it has to see the grave.
    pub fn merging() -> KeyPage {
        KeyPage {
            keeps_found: true,
            keeps_graves: true,
            ..KeyPage::default()
        }
    }

    /// Whether the page carries graves and covered entries for its reader to judge
    pub fn keeps_graves(&self) -> bool {
        self.keeps_graves
    }

    /// Drop the page's contents, keeping its allocations for the next fill
    pub fn clear(&mut self) {
        self.keys.clear();
        self.ends.clear();
        self.found.clear();
        self.payloads.clear();
        self.width = 0;
        self.count = 0;
    }

    /// Add one key, its entry, and the payload a walk already read for it
    pub fn push_read(&mut self, key: &[u8], found: Entry, payload: Option<Value>) {
        let Some(payload) = payload.filter(|_| self.reads_payloads) else {
            self.push(key, found);
            return;
        };
        // The keys before this one came with no payload, and an empty place each keeps the
        // two lists in step.
        self.payloads.resize_with(self.count, || None);
        self.push(key, found);
        self.payloads.push(Some(payload));
    }

    /// The payload a walk read beside the key at a position, handed over once
    pub fn take_payload(&mut self, at: usize) -> Option<Value> {
        self.payloads.get_mut(at).and_then(Option::take)
    }

    /// Room for this many more keys at a width, so a fill never regrows its buffers
    pub fn reserve(&mut self, count: usize, width: usize) {
        self.keys.reserve(count * width);
        if self.keeps_found {
            self.found.reserve(count);
        }
        if self.reads_payloads {
            self.payloads.reserve(count);
        }
    }

    /// Add one key and the payload length its record holds
    ///
    /// The page takes its stride from whichever key is added first.
    pub fn push(&mut self, key: &[u8], found: Entry) {
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
        self.count += 1;
    }

    /// Add a run of keys packed at one width, with each key's entry, in one copy each
    ///
    /// False, with nothing added, when the run's width differs from the page's.
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
