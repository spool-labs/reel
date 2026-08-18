//! One shard's keys in an open-addressed table instead of a tree
//!
//! Three parallel arrays and no nodes, so a slot costs exactly its control byte, its
//! key and its value. What it gives up is order, so the walks gather and sort.
//!
//! Probing is linear from a home slot and stops at the first empty one, and a delete
//! shifts the run back over the hole rather than leaving a tombstone, so a chain is
//! always contiguous from its home. Capacity is any size rather than a power of two,
//! the slot coming from a multiply into the capacity instead of a mask, so a table
//! built from a run of known length is sized once at its load factor.
//!
//! The whole key is the map key and every hit compares it. The control byte holds
//! seven bits of the hash and is a probe hint and nothing else, since a lossy key
//! must never be the thing a slot is claimed by.

use std::ops::Bound;

/// Control byte of a slot holding nothing
///
/// Every occupied slot holds a seven bit hash fragment, so the high bit is free to
/// mean empty and no fragment can collide with it.
const EMPTY: u8 = 0xFF;

/// Slots a table takes the first time something is put in it
///
/// A column sharded on two key bytes is 65,536 tables and a volume fills a handful,
/// so an untouched table allocates nothing and a barely touched one allocates little.
const MIN_SLOTS: usize = 8;

/// Numerator of the load factor a table is sized to and grown at
const LOAD_HELD: usize = 7;

/// Denominator of the same load factor
const LOAD_SLOTS: usize = 8;

/// Bytes of control a slot carries beside its key and its value
pub const CONTROL_BYTES: u64 = 1;

/// Slots that hold this many keys at the load factor, rounded up
///
/// Exact rather than rounded to a power of two, which is what the multiply-shift
/// index buys.
pub fn slots_for(keys: usize) -> usize {
    keys.saturating_mul(LOAD_SLOTS)
        .div_ceil(LOAD_HELD)
        .max(MIN_SLOTS)
}

/// Bytes a key costs beyond itself and its value, at the load factor
///
/// The control byte plus the share of the empty slots that key is holding open.
/// Const, since the index reports it per column shape rather than per table.
pub const fn overhead_per_key(key_width: u64, value_width: u64) -> u64 {
    let slot = key_width + value_width + CONTROL_BYTES;
    slot * LOAD_SLOTS as u64 / LOAD_HELD as u64 - key_width - value_width
}

/// Where a key sits, or where it would go if it were put in
enum Site {
    /// The slot already holding the key
    Held(usize),

    /// The first empty slot on the key's own chain
    Free(usize),
}

/// One shard's keys, open addressed at a declared key width
///
/// Only a fixed width can take this, since the key is stored in place. The value is
/// whatever the shard keeps beside a key and must have a default, because an empty
/// slot holds one rather than a hole the code has to remember not to read.
pub struct OpenTable<const N: usize, Value> {
    /// One byte a slot: the empty marker, or seven bits of the key's hash
    control: Vec<u8>,

    /// The keys, meaningful only where the control byte says the slot is taken
    keys: Vec<[u8; N]>,

    /// The values, parallel to the keys and defaulted where a slot is empty
    values: Vec<Value>,

    /// Keys the table holds, which is what the load factor is measured against
    held: usize,
}

impl<const N: usize, Value> Default for OpenTable<N, Value> {
    /// A table holding nothing and owning nothing
    fn default() -> OpenTable<N, Value> {
        OpenTable {
            control: Vec::new(),
            keys: Vec::new(),
            values: Vec::new(),
            held: 0,
        }
    }
}

impl<const N: usize, Value: Default> OpenTable<N, Value> {
    /// An empty table, which allocates when something is first put in it
    pub fn new() -> OpenTable<N, Value> {
        OpenTable::default()
    }

    /// A table sized for a known number of keys before any of them arrive
    ///
    /// The install path: the key count is known at open, so the table is sized once
    /// at its load factor rather than doubled into it.
    pub fn with_keys(keys: usize) -> OpenTable<N, Value> {
        let mut table = OpenTable::default();
        if keys > 0 {
            table.resize(slots_for(keys));
        }
        table
    }

    /// Slots the table holds, filled and empty
    pub fn slots(&self) -> usize {
        self.control.len()
    }

    /// Keys the table holds
    pub fn len(&self) -> usize {
        self.held
    }

    /// Whether the table holds no keys at all
    pub fn is_empty(&self) -> bool {
        self.held == 0
    }

    /// What is held for a key
    pub fn get(&self, key: &[u8; N]) -> Option<&Value> {
        match self.site(key) {
            Site::Held(at) => Some(&self.values[at]),
            Site::Free(_) => None,
        }
    }

    /// Whether a key is held at all
    pub fn contains_key(&self, key: &[u8; N]) -> bool {
        matches!(self.site(key), Site::Held(_))
    }

    /// Put a value in, handing back the one it displaced
    pub fn insert(&mut self, key: [u8; N], value: Value) -> Option<Value> {
        let free = match self.site(&key) {
            Site::Held(at) => return Some(std::mem::replace(&mut self.values[at], value)),
            // The probe already walked this key's chain to its end, so the slot it
            // stopped on is where the key goes. Only a growth moves it, and only a key
            // the table does not hold can force one, so the room is asked about here.
            Site::Free(at) if !self.is_crowded() => at,
            Site::Free(_) => {
                self.resize(self.grown());
                match self.site(&key) {
                    Site::Held(at) => return Some(std::mem::replace(&mut self.values[at], value)),
                    Site::Free(at) => at,
                }
            }
        };
        self.control[free] = fragment(hash_of(&key));
        self.keys[free] = key;
        self.values[free] = value;
        self.held += 1;
        None
    }

    /// Take a key out, handing back what it held
    pub fn remove(&mut self, key: &[u8; N]) -> Option<Value> {
        match self.site(key) {
            Site::Held(at) => Some(self.erase(at)),
            Site::Free(_) => None,
        }
    }

    /// Drop every key, keeping the room already taken
    pub fn clear(&mut self) {
        for at in 0..self.control.len() {
            if self.control[at] != EMPTY {
                self.control[at] = EMPTY;
                self.values[at] = Value::default();
            }
        }
        self.held = 0;
    }

    /// Take a run of pairs in one pass, sizing the table for it where it is empty
    ///
    /// Repeats are allowed and the last one wins. A table that already holds
    /// something takes them a key at a time, since sizing for the run would ignore
    /// what is already in the slots. An empty table is sized to the run exactly,
    /// downward as readily as upward, so a reinstalled shard that lost most of its
    /// keys stops paying for them.
    pub fn absorb(&mut self, run: Vec<([u8; N], Value)>) {
        if self.held == 0 && !run.is_empty() && self.slots() != slots_for(run.len()) {
            *self = OpenTable::with_keys(run.len());
        }
        for (key, value) in run {
            self.insert(key, value);
        }
    }

    /// Give back the room a run of deletes left behind, where enough of it has gone
    ///
    /// A delete leaves room rather than something a search steps over, but a table
    /// that fell to a quarter of its load factor holds more than three times what its
    /// keys need. The guard keeps a table near its load factor from rehashing on
    /// every pass.
    pub fn pack(&mut self) {
        if self.slots() <= MIN_SLOTS || self.held * 4 >= self.slots() {
            return;
        }
        self.resize(slots_for(self.held));
    }

    /// Every pair the table holds, in key order
    ///
    /// Gathered and sorted, since an open-addressed table has no order to read off.
    /// A column taking this shape pays for its footprint here.
    pub fn sorted(&self) -> Vec<(&[u8; N], &Value)> {
        let mut held: Vec<(&[u8; N], &Value)> = Vec::with_capacity(self.held);
        for at in 0..self.control.len() {
            if self.control[at] != EMPTY {
                held.push((&self.keys[at], &self.values[at]));
            }
        }
        held.sort_unstable_by(|left, right| left.0.cmp(right.0));
        held
    }

    /// Every pair inside a span, in key order
    pub fn sorted_span(
        &self,
        low: Bound<&[u8; N]>,
        high: Bound<&[u8; N]>,
    ) -> Vec<(&[u8; N], &Value)> {
        let mut held: Vec<(&[u8; N], &Value)> = Vec::new();
        for at in 0..self.control.len() {
            if self.control[at] == EMPTY {
                continue;
            }
            if within(&self.keys[at], low, high) {
                held.push((&self.keys[at], &self.values[at]));
            }
        }
        held.sort_unstable_by(|left, right| left.0.cmp(right.0));
        held
    }

    /// Whether one more key would put the table past its load factor
    ///
    /// True of a table with no slots at all, which is how the first put allocates.
    fn is_crowded(&self) -> bool {
        (self.held + 1) * LOAD_SLOTS > self.slots() * LOAD_HELD
    }

    /// Slots to grow to, which is half again as many rather than twice as many
    ///
    /// Doubling would leave a grown table sitting near half load, where half again
    /// keeps one between four sevenths and seven eighths full. The install path does
    /// not come through here at all.
    fn grown(&self) -> usize {
        slots_for(self.held + 1).max(self.slots() + self.slots() / 2)
    }

    /// Where a key sits, or the empty slot its chain ends at
    ///
    /// The walk stops at the first empty slot because a chain is contiguous from its
    /// home, so an empty slot on it means the key is not in the table.
    fn site(&self, key: &[u8; N]) -> Site {
        let slots = self.slots();
        // A table nothing has been put in has no slot to name, and the caller that
        // sees this makes room and asks again.
        if slots == 0 {
            return Site::Free(0);
        }
        let hash = hash_of(key);
        let want = fragment(hash);
        let mut at = home(hash, slots);
        loop {
            let control = self.control[at];
            if control == EMPTY {
                return Site::Free(at);
            }
            // The fragment turns most of the wrong slots away without touching the key
            // array. Nothing is ever resolved on the fragment alone.
            if control == want && self.keys[at] == *key {
                return Site::Held(at);
            }
            at = step(at, slots);
        }
    }

    /// Empty one slot, shifting the rest of its run back over the hole
    ///
    /// Knuth's deletion for linear probing. An entry moves into the hole exactly when
    /// the hole is nearer its home than the slot it is in, which is what keeps every
    /// chain contiguous. The walk terminates because the load factor leaves at least
    /// one slot empty and the walk stops at the first one.
    fn erase(&mut self, at: usize) -> Value {
        let slots = self.slots();
        let taken = std::mem::take(&mut self.values[at]);
        let mut hole = at;
        let mut probe = step(at, slots);
        while self.control[probe] != EMPTY {
            let from = home(hash_of(&self.keys[probe]), slots);
            if reach(from, hole, slots) < reach(from, probe, slots) {
                self.control[hole] = self.control[probe];
                self.keys[hole] = self.keys[probe];
                self.values[hole] = std::mem::take(&mut self.values[probe]);
                hole = probe;
            }
            probe = step(probe, slots);
        }
        self.control[hole] = EMPTY;
        self.held -= 1;
        taken
    }

    /// Build a table of this many slots and put every held key back in it
    fn resize(&mut self, slots: usize) {
        let mut control = vec![EMPTY; slots];
        let mut keys = vec![[0u8; N]; slots];
        let mut values: Vec<Value> = Vec::with_capacity(slots);
        values.resize_with(slots, Value::default);

        for at in 0..self.control.len() {
            if self.control[at] == EMPTY {
                continue;
            }
            let key = self.keys[at];
            let hash = hash_of(&key);
            let mut to = home(hash, slots);
            while control[to] != EMPTY {
                to = step(to, slots);
            }
            control[to] = fragment(hash);
            keys[to] = key;
            values[to] = std::mem::take(&mut self.values[at]);
        }

        self.control = control;
        self.keys = keys;
        self.values = values;
    }
}

/// Whether a key falls inside a pair of bounds
fn within<const N: usize>(key: &[u8; N], low: Bound<&[u8; N]>, high: Bound<&[u8; N]>) -> bool {
    let above = match low {
        Bound::Unbounded => true,
        Bound::Included(edge) => key >= edge,
        Bound::Excluded(edge) => key > edge,
    };
    let below = match high {
        Bound::Unbounded => true,
        Bound::Included(edge) => key <= edge,
        Bound::Excluded(edge) => key < edge,
    };
    above && below
}

/// The slot a hash claims first, across a capacity that is not a power of two
///
/// The high half of a 64 by 64 multiply, which spreads a uniform hash evenly over any
/// capacity for one multiply. Masking would be a cycle cheaper and would force the
/// capacity to a power of two, which is the doubling the load factor avoids.
fn home(hash: u64, slots: usize) -> usize {
    ((hash as u128 * slots as u128) >> 64) as usize
}

/// The next slot on a chain, wrapping at the end of the table
fn step(at: usize, slots: usize) -> usize {
    match at + 1 == slots {
        true => 0,
        false => at + 1,
    }
}

/// Slots walked forward from one place to another, wrapping at the end
fn reach(from: usize, to: usize, slots: usize) -> usize {
    match to >= from {
        true => to - from,
        false => slots - from + to,
    }
}

/// The seven bits of a hash a control byte carries
///
/// Taken from the low bits, since `home` reads the high ones, so a slot and its
/// fragment do not repeat each other.
fn fragment(hash: u64) -> u8 {
    (hash & 0x7F) as u8
}

/// One 64 bit hash of a key
///
/// Mixed rather than taken raw, since a column is free to declare an open shape over
/// structured keys and passing those through would pile a shard onto one chain.
fn hash_of(key: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for chunk in key.chunks(8) {
        let mut word = [0u8; 8];
        word[..chunk.len()].copy_from_slice(chunk);
        hash ^= u64::from_le_bytes(word);
        hash = hash.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        hash ^= hash >> 31;
    }
    // A final avalanche, so the high bits the home slot reads and the low bits the
    // fragment reads both depend on every byte of the key.
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 29;
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::format::loc::{Loc, SegmentId};
    use crate::format::lsn::Lsn;
    use crate::index::entry::Entry;

    /// Bytes one slot of a state-shaped key and the shipped entry occupies
    const SLOT_BYTES: usize = 32 + std::mem::size_of::<Entry>() + CONTROL_BYTES as usize;

    fn key(at: u64) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&at.to_be_bytes());
        bytes
    }

    /// A signature-shaped key, uniform from its first byte and wider than a slot
    fn signature(at: u64) -> [u8; 72] {
        let mut bytes = [0u8; 72];
        let mut state = at.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xa5a5_a5a5_a5a5_a5a5;
        for chunk in bytes[..64].chunks_mut(8) {
            state ^= state >> 33;
            state = state.wrapping_mul(0xff51_afd7_ed55_8ccd);
            chunk.copy_from_slice(&state.to_le_bytes());
        }
        bytes[64..].copy_from_slice(&at.to_be_bytes());
        bytes
    }

    /// Keys whose home slot is the same in a table of this many slots
    ///
    /// Found by trial rather than constructed, since a constructed collision would
    /// test the arithmetic instead of the table.
    fn sharing_a_home<const N: usize>(
        slots: usize,
        want: usize,
        draw: fn(u64) -> [u8; N],
    ) -> Vec<[u8; N]> {
        let target = home(hash_of(&draw(0)), slots);
        let mut found = vec![draw(0)];
        let mut at = 1u64;
        while found.len() < want {
            let candidate = draw(at);
            if home(hash_of(&candidate), slots) == target {
                found.push(candidate);
            }
            at += 1;
        }
        found
    }

    /// Candidates the fragment search draws from before it gives up
    ///
    /// Only here so a search that cannot succeed fails instead of hanging.
    const FRAGMENT_DRAWS: u64 = 1_000_000;

    /// Keys agreeing on their home slot and on their control byte both
    ///
    /// The fragment is the only thing a probe reads before the key itself, so keys
    /// that defeat it leave the whole key as the only thing telling them apart.
    fn sharing_a_fragment<const N: usize>(
        slots: usize,
        want: usize,
        draw: fn(u64) -> [u8; N],
    ) -> Vec<[u8; N]> {
        let first = hash_of(&draw(0));
        let target = (home(first, slots), fragment(first));
        let mut found = vec![draw(0)];
        for at in 1..FRAGMENT_DRAWS {
            let hash = hash_of(&draw(at));
            if (home(hash, slots), fragment(hash)) == target {
                found.push(draw(at));
            }
            if found.len() == want {
                break;
            }
        }

        assert_eq!(
            found.len(),
            want,
            "the draw ran out before {want} keys agreed"
        );
        found
    }

    // a table serves back every key put into it
    #[test]
    fn puts_resolve() {
        let mut table: OpenTable<32, u64> = OpenTable::new();

        for at in 0..2_000u64 {
            table.insert(key(at), at);
        }

        assert_eq!(table.len(), 2_000);
        for at in 0..2_000u64 {
            assert_eq!(table.get(&key(at)), Some(&at));
        }
        assert!(table.get(&key(2_000)).is_none());
    }

    // an overwrite replaces the value and takes no new slot
    #[test]
    fn overwrite_keeps_one_slot() {
        let mut table: OpenTable<32, u64> = OpenTable::new();
        table.insert(key(1), 10);

        let displaced = table.insert(key(1), 20);

        assert_eq!(displaced, Some(10));
        assert_eq!(table.len(), 1);
        assert_eq!(table.get(&key(1)), Some(&20));
    }

    // keys landing on one home slot each resolve to their own value, since the whole
    // key claims a slot and a shared home is a longer chain rather than a lost key
    #[test]
    fn shared_home_resolves() {
        let mut table: OpenTable<32, u64> = OpenTable::with_keys(64);
        let clashing = sharing_a_home(table.slots(), 6, key);

        for (at, key) in clashing.iter().enumerate() {
            table.insert(*key, at as u64);
        }

        assert_eq!(table.len(), clashing.len());
        for (at, key) in clashing.iter().enumerate() {
            assert_eq!(
                table.get(key),
                Some(&(at as u64)),
                "key {at} of a shared chain"
            );
        }
    }

    // keys agreeing on their home slot and their fragment still resolve apart, since
    // a table that stopped at the fragment would hand one of them the other's value
    #[test]
    fn shared_fragment_resolves() {
        let mut table: OpenTable<32, u64> = OpenTable::with_keys(64);
        let clashing = sharing_a_fragment(table.slots(), 3, key);

        for (at, key) in clashing.iter().enumerate() {
            table.insert(*key, at as u64);
        }

        assert_eq!(table.len(), clashing.len());
        for (at, key) in clashing.iter().enumerate() {
            assert_eq!(
                table.get(key),
                Some(&(at as u64)),
                "key {at} of a shared fragment"
            );
        }
        assert_eq!(table.remove(&clashing[0]), Some(0));
        assert!(table.get(&clashing[0]).is_none());
        for (at, key) in clashing.iter().enumerate().skip(1) {
            assert_eq!(
                table.get(key),
                Some(&(at as u64)),
                "key {at} after the delete"
            );
        }
    }

    // a delete out of the middle of a chain leaves the rest of it reachable
    #[test]
    fn delete_keeps_the_chain() {
        let mut table: OpenTable<32, u64> = OpenTable::with_keys(64);
        let clashing = sharing_a_home(table.slots(), 6, key);
        for (at, key) in clashing.iter().enumerate() {
            table.insert(*key, at as u64);
        }

        assert_eq!(table.remove(&clashing[2]), Some(2));

        assert_eq!(table.len(), clashing.len() - 1);
        assert!(table.get(&clashing[2]).is_none());
        for (at, key) in clashing.iter().enumerate().filter(|(at, _)| *at != 2) {
            assert_eq!(
                table.get(key),
                Some(&(at as u64)),
                "key {at} after the delete"
            );
        }
    }

    // a signature-wide table serves back every key put into it
    #[test]
    fn wide_puts_resolve() {
        let mut table: OpenTable<72, u64> = OpenTable::new();

        for at in 0..2_000u64 {
            table.insert(signature(at), at);
        }

        assert_eq!(table.len(), 2_000);
        for at in 0..2_000u64 {
            assert_eq!(table.get(&signature(at)), Some(&at));
        }
        assert!(table.get(&signature(2_000)).is_none());
    }

    // and tells apart signature-wide keys that agree on their home and their fragment
    #[test]
    fn wide_shared_fragment_resolves() {
        let mut table: OpenTable<72, u64> = OpenTable::with_keys(64);
        let clashing = sharing_a_fragment(table.slots(), 3, signature);

        for (at, key) in clashing.iter().enumerate() {
            table.insert(*key, at as u64);
        }

        assert_eq!(table.len(), clashing.len());
        for (at, key) in clashing.iter().enumerate() {
            assert_eq!(
                table.get(key),
                Some(&(at as u64)),
                "key {at} of a shared fragment"
            );
        }
        assert_eq!(table.remove(&clashing[0]), Some(0));
        assert!(table.get(&clashing[0]).is_none());
        for (at, key) in clashing.iter().enumerate().skip(1) {
            assert_eq!(
                table.get(key),
                Some(&(at as u64)),
                "key {at} after the delete"
            );
        }
    }

    // a delete out of a signature-wide chain leaves the rest of it reachable
    #[test]
    fn wide_delete_keeps_the_chain() {
        let mut table: OpenTable<72, u64> = OpenTable::with_keys(64);
        let clashing = sharing_a_home(table.slots(), 6, signature);
        for (at, key) in clashing.iter().enumerate() {
            table.insert(*key, at as u64);
        }

        assert_eq!(table.remove(&clashing[2]), Some(2));

        assert_eq!(table.len(), clashing.len() - 1);
        assert!(table.get(&clashing[2]).is_none());
        for (at, key) in clashing.iter().enumerate().filter(|(at, _)| *at != 2) {
            assert_eq!(
                table.get(key),
                Some(&(at as u64)),
                "key {at} after the delete"
            );
        }
    }

    // a signature-wide span serves the keys inside its bounds, in order
    #[test]
    fn wide_span_holds_its_bounds() {
        let mut table: OpenTable<72, u64> = OpenTable::new();
        let mut ordered: Vec<[u8; 72]> = (0..100u64).map(signature).collect();
        for (at, key) in ordered.iter().enumerate() {
            table.insert(*key, at as u64);
        }
        ordered.sort_unstable();

        let held: Vec<[u8; 72]> = table
            .sorted_span(Bound::Included(&ordered[10]), Bound::Excluded(&ordered[20]))
            .into_iter()
            .map(|(key, _)| *key)
            .collect();

        assert_eq!(held, ordered[10..20].to_vec());
    }

    // a table never holds more than seven eighths of its slots
    #[test]
    fn load_stays_under() {
        let mut table: OpenTable<32, u64> = OpenTable::new();

        for at in 0..5_000u64 {
            table.insert(key(at), at);
            assert!(
                table.len() * LOAD_SLOTS <= table.slots() * LOAD_HELD,
                "{} keys in {} slots",
                table.len(),
                table.slots(),
            );
        }
    }

    // a run absorbed into an empty table sizes it once and never grows it
    #[test]
    fn absorb_sizes_once() {
        let mut table: OpenTable<32, u64> = OpenTable::new();
        let run: Vec<([u8; 32], u64)> = (0..4_000u64).map(|at| (key(at), at)).collect();

        table.absorb(run);
        let slots = table.slots();

        assert_eq!(table.len(), 4_000);
        assert_eq!(slots, slots_for(4_000));
        assert!(
            slots < 4_000 * 2,
            "sized into a doubling instead of a load factor"
        );
        for at in 0..4_000u64 {
            assert_eq!(table.get(&key(at)), Some(&at));
        }
    }

    // a run absorbed into an emptied table sizes down to what the run holds, since a
    // clear keeps the room and the reinstall would otherwise pay for keys it lost
    #[test]
    fn absorb_sizes_down() {
        let mut table: OpenTable<32, u64> = OpenTable::new();
        for at in 0..8_000u64 {
            table.insert(key(at), at);
        }
        let filled = table.slots();
        table.clear();

        table.absorb((0..100u64).map(|at| (key(at), at)).collect());

        assert_eq!(
            table.slots(),
            slots_for(100),
            "{filled} slots kept for 100 keys"
        );
        assert_eq!(table.len(), 100);
        for at in 0..100u64 {
            assert_eq!(table.get(&key(at)), Some(&at));
        }
    }

    // a run of repeats absorbs the way one key at a time would, last one winning
    #[test]
    fn absorb_takes_the_last() {
        let mut table: OpenTable<32, u64> = OpenTable::new();

        table.absorb(vec![(key(1), 10), (key(1), 20), (key(2), 30)]);

        assert_eq!(table.len(), 2);
        assert_eq!(table.get(&key(1)), Some(&20));
    }

    // a packed table gives its room back and still holds every survivor
    #[test]
    fn pack_gives_room_back() {
        let mut table: OpenTable<32, u64> = OpenTable::new();
        for at in 0..4_000u64 {
            table.insert(key(at), at);
        }
        let filled = table.slots();
        for at in 0..3_900u64 {
            table.remove(&key(at));
        }

        table.pack();

        assert!(
            table.slots() < filled / 4,
            "{} slots against {filled}",
            table.slots()
        );
        assert_eq!(table.len(), 100);
        for at in 3_900..4_000u64 {
            assert_eq!(table.get(&key(at)), Some(&at), "key {at} after the pack");
        }
    }

    // a table still near its load factor is left alone
    #[test]
    fn pack_leaves_a_full_table() {
        let mut table: OpenTable<32, u64> = OpenTable::new();
        for at in 0..4_000u64 {
            table.insert(key(at), at);
        }
        let filled = table.slots();

        table.pack();

        assert_eq!(table.slots(), filled);
    }

    // a cleared table serves nothing and keeps its room
    #[test]
    fn clear_keeps_room() {
        let mut table: OpenTable<32, u64> = OpenTable::new();
        for at in 0..500u64 {
            table.insert(key(at), at);
        }
        let slots = table.slots();

        table.clear();

        assert_eq!(table.len(), 0);
        assert_eq!(table.slots(), slots);
        assert!(table.get(&key(1)).is_none());
    }

    // a span serves the keys inside its bounds and no others, in order
    #[test]
    fn span_holds_its_bounds() {
        let mut table: OpenTable<32, u64> = OpenTable::new();
        for at in 0..100u64 {
            table.insert(key(at), at);
        }

        let held: Vec<u64> = table
            .sorted_span(Bound::Included(&key(10)), Bound::Excluded(&key(20)))
            .into_iter()
            .map(|(_, at)| *at)
            .collect();

        assert_eq!(held, (10..20).collect::<Vec<u64>>());
    }

    // the slot count for a key count sits at the load factor, not at a power of two
    #[test]
    fn slots_follow_the_load() {
        assert_eq!(slots_for(0), MIN_SLOTS);
        assert_eq!(slots_for(7), MIN_SLOTS);
        assert_eq!(slots_for(1_000_000), 1_142_858);
        assert!(slots_for(1_000_000) * LOAD_HELD >= 1_000_000 * LOAD_SLOTS);
    }

    // a key's cost is its control byte and its share of the empty slots
    #[test]
    fn overhead_is_the_slack() {
        assert_eq!(overhead_per_key(32, 24), 9);
        assert_eq!(overhead_per_key(34, 24), 9);
        assert_eq!(overhead_per_key(72, 24), 14);
        assert_eq!(overhead_per_key(108, 24), 20);
    }

    // a sized table holds a state-shaped key for what the arithmetic says it does,
    // asked of the structure rather than weighed off the allocator
    #[test]
    fn slot_bytes_a_key() {
        let count = 262_144usize;
        let mut table: OpenTable<32, Entry> = OpenTable::with_keys(count);
        for at in 0..count as u64 {
            table.insert(
                key(at),
                Entry::new(Loc::new(SegmentId(1), at as u32, 200), Lsn(at)),
            );
        }

        let per_key = table.slots() * SLOT_BYTES / table.len();

        assert_eq!(table.len(), count);
        assert_eq!(per_key, 65, "{} slots for {count} keys", table.slots());
    }

    // and a signature-wide key for what its own slot says, which is the wider one
    #[test]
    fn wide_slot_bytes_a_key() {
        let count = 262_144usize;
        let mut table: OpenTable<72, Entry> = OpenTable::with_keys(count);
        for at in 0..count as u64 {
            table.insert(
                signature(at),
                Entry::new(Loc::new(SegmentId(1), at as u32, 200), Lsn(at)),
            );
        }

        let wide_slot = 72 + std::mem::size_of::<Entry>() + CONTROL_BYTES as usize;
        let per_key = table.slots() * wide_slot / table.len();

        assert_eq!(table.len(), count);
        assert_eq!(per_key, 110, "{} slots for {count} keys", table.slots());
    }
}
