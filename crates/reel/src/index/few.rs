//! A shard map that holds a few keys in two sorted vectors and grows into a tree past them

use std::borrow::Borrow;
use std::ops::Bound;

use crate::index::column::{IndexKey, ShardMap};
use crate::index::tbtreemap::{TBTreeMap, TreeKey};

/// A shard holds this many keys in its vectors before it grows into a tree
pub const FEW_KEYS: usize = 8;

/// A shard's keys, in sorted vectors while few and in the column's tree once more
pub enum Few<K, V, M> {
    /// Keys in order beside their values, each pair at the same place
    Few { keys: Vec<K>, vals: Vec<V> },

    /// The column's own map, once the shard outgrew the vectors
    Tree(M),
}

impl<K, V, M> Default for Few<K, V, M> {
    fn default() -> Self {
        Few::Few {
            keys: Vec::new(),
            vals: Vec::new(),
        }
    }
}

/// One of two iterators, so both shapes walk behind one type
pub enum Either<A, B> {
    Left(A),
    Right(B),
}

impl<T, A: Iterator<Item = T>, B: Iterator<Item = T>> Iterator for Either<A, B> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        match self {
            Either::Left(iter) => iter.next(),
            Either::Right(iter) => iter.next(),
        }
    }
}

/// Where a key sits among sorted keys, or where it would go
fn place<K: Borrow<[u8]>>(keys: &[K], key: &[u8]) -> std::result::Result<usize, usize> {
    keys.binary_search_by(|held| held.borrow().cmp(key))
}

/// The first place at or past a low bound
fn from_low<K: Ord>(keys: &[K], low: Bound<&K>) -> usize {
    match low {
        Bound::Unbounded => 0,
        Bound::Included(low) => keys.partition_point(|key| key < low),
        Bound::Excluded(low) => keys.partition_point(|key| key <= low),
    }
}

/// The place just past a high bound
fn to_high<K: Ord>(keys: &[K], high: Bound<&K>) -> usize {
    match high {
        Bound::Unbounded => keys.len(),
        Bound::Included(high) => keys.partition_point(|key| key <= high),
        Bound::Excluded(high) => keys.partition_point(|key| key < high),
    }
}

impl<K: IndexKey, V: 'static, M: ShardMap<K, V>> Few<K, V, M> {
    /// Move the pairs into the column's map, for a shard that outgrew its vectors
    fn grow(keys: &mut Vec<K>, vals: &mut Vec<V>) -> M {
        let mut tree = M::default();
        for (key, val) in std::mem::take(keys).into_iter().zip(std::mem::take(vals)) {
            tree.put(key, val);
        }
        tree
    }

    /// Bring a tree that shrank back into vectors sized to what it holds
    fn shrink(&mut self) {
        let Few::Tree(tree) = self else {
            return;
        };
        if tree.count() > FEW_KEYS / 2 {
            return;
        }
        let mut taken: Vec<K> = Vec::with_capacity(tree.count());
        taken.extend(tree.walk().map(|(key, _)| key.clone()));
        let mut keys = Vec::with_capacity(taken.len());
        let mut vals = Vec::with_capacity(taken.len());
        for key in taken {
            if let Some(val) = tree.take(key.borrow()) {
                keys.push(key);
                vals.push(val);
            }
        }
        *self = Few::Few { keys, vals };
    }
}

impl<K: IndexKey, V: 'static, M: ShardMap<K, V>> ShardMap<K, V> for Few<K, V, M> {
    fn put(&mut self, key: K, val: V) -> Option<V> {
        match self {
            Few::Tree(tree) => tree.put(key, val),
            Few::Few { keys, vals } => match place(keys, key.borrow()) {
                Ok(at) => Some(std::mem::replace(&mut vals[at], val)),
                Err(_) if keys.len() == FEW_KEYS => {
                    let mut tree = Self::grow(keys, vals);
                    tree.put(key, val);
                    *self = Few::Tree(tree);
                    None
                }
                Err(at) => {
                    keys.insert(at, key);
                    vals.insert(at, val);
                    None
                }
            },
        }
    }

    fn at(&self, key: &[u8]) -> Option<&V> {
        match self {
            Few::Tree(tree) => tree.at(key),
            Few::Few { keys, vals } => place(keys, key).ok().map(|at| &vals[at]),
        }
    }

    fn at_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        match self {
            Few::Tree(tree) => tree.at_mut(key),
            Few::Few { keys, vals } => place(keys, key).ok().map(|at| &mut vals[at]),
        }
    }

    fn holds(&self, key: &[u8]) -> bool {
        self.at(key).is_some()
    }

    fn take(&mut self, key: &[u8]) -> Option<V> {
        match self {
            Few::Tree(tree) => tree.take(key),
            Few::Few { keys, vals } => {
                let at = place(keys, key).ok()?;
                keys.remove(at);
                Some(vals.remove(at))
            }
        }
    }

    fn count(&self) -> usize {
        match self {
            Few::Tree(tree) => tree.count(),
            Few::Few { keys, .. } => keys.len(),
        }
    }

    fn vacant(&self) -> bool {
        self.count() == 0
    }

    fn heap_bytes(&self) -> u64 {
        match self {
            Few::Tree(tree) => tree.heap_bytes(),
            Few::Few { keys, vals } => {
                (keys.capacity() * std::mem::size_of::<K>()
                    + vals.capacity() * std::mem::size_of::<V>()) as u64
            }
        }
    }

    fn empty(&mut self) {
        match self {
            Few::Tree(tree) => tree.empty(),
            Few::Few { keys, vals } => {
                keys.clear();
                vals.clear();
            }
        }
    }

    fn walk(&self) -> impl Iterator<Item = (&K, &V)> {
        match self {
            Few::Tree(tree) => Either::Left(tree.walk()),
            Few::Few { keys, vals } => Either::Right(keys.iter().zip(vals.iter())),
        }
    }

    fn span<'a>(
        &'a self,
        low: Bound<&K>,
        high: Bound<&'a K>,
    ) -> impl Iterator<Item = (&'a K, &'a V)> {
        match self {
            Few::Tree(tree) => Either::Left(tree.span(low, high)),
            Few::Few { keys, vals } => {
                let from = from_low(keys, low);
                let to = to_high(keys, high).max(from);
                Either::Right(keys[from..to].iter().zip(vals[from..to].iter()))
            }
        }
    }

    fn span_back<'a>(
        &'a self,
        low: Bound<&'a K>,
        high: Bound<&K>,
    ) -> impl Iterator<Item = (&'a K, &'a V)> {
        match self {
            Few::Tree(tree) => Either::Left(tree.span_back(low, high)),
            Few::Few { keys, vals } => {
                let from = from_low(keys, low);
                let to = to_high(keys, high).max(from);
                Either::Right(keys[from..to].iter().zip(vals[from..to].iter()).rev())
            }
        }
    }

    fn span_runs<'a>(
        &'a self,
        low: Bound<&'a K>,
    ) -> Box<dyn Iterator<Item = (&'a [K], &'a [V])> + 'a> {
        match self {
            Few::Tree(tree) => tree.span_runs(low),
            Few::Few { keys, vals } => {
                let from = from_low(keys, low);
                let run = (&keys[from..], &vals[from..]);
                Box::new(std::iter::once(run).filter(|(keys, _)| !keys.is_empty()))
            }
        }
    }

    fn at_many<'a>(&'a self, keys: &[K], out: &mut Vec<Option<&'a V>>) {
        match self {
            Few::Tree(tree) => tree.at_many(keys, out),
            Few::Few { .. } => {
                out.clear();
                for key in keys {
                    out.push(self.at(key.borrow()));
                }
            }
        }
    }

    fn sweep(&mut self, mut keep: impl FnMut(&K, &V) -> bool) -> usize {
        match self {
            Few::Tree(tree) => tree.sweep(keep),
            Few::Few { keys, vals } => {
                let mut to = 0;
                for at in 0..keys.len() {
                    if keep(&keys[at], &vals[at]) {
                        keys.swap(to, at);
                        vals.swap(to, at);
                        to += 1;
                    }
                }
                let taken = keys.len() - to;
                keys.truncate(to);
                vals.truncate(to);
                taken
            }
        }
    }

    fn fit(&mut self) {
        match self {
            Few::Tree(tree) => tree.fit(),
            Few::Few { keys, vals } => {
                keys.shrink_to_fit();
                vals.shrink_to_fit();
            }
        }
        self.shrink();
    }

    fn pack_owed(&mut self) {
        if let Few::Tree(tree) = self {
            tree.pack_owed();
        }
        self.shrink();
    }

    fn lead_tie_rate(&self) -> Option<f64> {
        match self {
            Few::Tree(tree) => tree.lead_tie_rate(),
            Few::Few { .. } => None,
        }
    }
}

impl<K: TreeKey, const B: usize, V: Default> Few<K, V, TBTreeMap<K, B, V>> {
    /// How many leaves hold the keys, the vectors counting as one while they hold any
    pub fn leaf_count(&self) -> usize {
        match self {
            Few::Tree(tree) => tree.leaf_count(),
            Few::Few { keys, .. } => usize::from(!keys.is_empty()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::RangeBounds;

    use super::*;

    type Map = Few<[u8; 4], u32, TBTreeMap<[u8; 4], 16, u32>>;

    fn key(n: u32) -> [u8; 4] {
        n.to_be_bytes()
    }

    /// Sorted pairs, the order every walk must give back
    #[derive(Default)]
    struct Model(Vec<([u8; 4], u32)>);

    impl Model {
        fn put(&mut self, key: [u8; 4], val: u32) -> Option<u32> {
            match self.0.binary_search_by_key(&key, |(held, _)| *held) {
                Ok(at) => Some(std::mem::replace(&mut self.0[at].1, val)),
                Err(at) => {
                    self.0.insert(at, (key, val));
                    None
                }
            }
        }

        fn take(&mut self, key: [u8; 4]) -> Option<u32> {
            let at = self.0.binary_search_by_key(&key, |(held, _)| *held).ok()?;
            Some(self.0.remove(at).1)
        }

        fn span(&self, low: Bound<&[u8; 4]>, high: Bound<&[u8; 4]>) -> Vec<u32> {
            self.0
                .iter()
                .filter(|(key, _)| (low, high).contains(key))
                .map(|(_, val)| *val)
                .collect()
        }
    }

    fn walked(map: &Map) -> Vec<([u8; 4], u32)> {
        map.walk().map(|(key, val)| (*key, *val)).collect()
    }

    // a shard answers the same before and after it grows into a tree and shrinks back
    #[test]
    fn grows_and_shrinks_in_order() {
        let mut map = Map::default();
        let mut model = Model::default();
        for n in (0..40u32).rev() {
            assert_eq!(map.put(key(n * 3), n), model.put(key(n * 3), n));
            assert_eq!(matches!(map, Few::Few { .. }), model.0.len() <= FEW_KEYS);
        }
        assert_eq!(walked(&map), model.0);

        for n in 0..38u32 {
            assert_eq!(map.take(&key(n * 3)), model.take(key(n * 3)));
        }
        map.pack_owed();
        assert!(matches!(map, Few::Few { .. }));
        assert_eq!(walked(&map), model.0);
        for n in 0..40u32 {
            assert_eq!(
                map.at(&key(n * 3)).copied(),
                model
                    .span(Bound::Included(&key(n * 3)), Bound::Included(&key(n * 3)))
                    .first()
                    .copied()
            );
        }
    }

    // spans over the vectors match the same spans over sorted pairs
    #[test]
    fn spans_match_sorted_pairs() {
        let mut map = Map::default();
        let mut model = Model::default();
        for n in [5u32, 1, 9, 3, 7] {
            map.put(key(n), n);
            model.put(key(n), n);
        }
        let bounds = [
            Bound::Unbounded,
            Bound::Included(key(3)),
            Bound::Excluded(key(3)),
            Bound::Included(key(4)),
            Bound::Excluded(key(9)),
        ];
        for low in &bounds {
            for high in &bounds {
                let (low, high) = (low.as_ref(), high.as_ref());
                let want = model.span(low, high);
                let up: Vec<u32> = map.span(low, high).map(|(_, val)| *val).collect();
                let mut down: Vec<u32> = map.span_back(low, high).map(|(_, val)| *val).collect();
                down.reverse();
                assert_eq!(up, want, "span {low:?}..{high:?}");
                assert_eq!(down, want, "span back {low:?}..{high:?}");
            }
            let want = model.span(low.as_ref(), Bound::Unbounded);
            let runs: Vec<u32> = map
                .span_runs(low.as_ref())
                .flat_map(|(_, vals)| vals.iter().copied())
                .collect();
            assert_eq!(runs, want, "runs from {low:?}");
        }
    }

    // two keys cost a fraction of one tree leaf
    #[test]
    fn a_few_keys_weigh_little() {
        let mut few = Map::default();
        let mut tree = TBTreeMap::<[u8; 4], 16, u32>::default();
        for n in 0..2u32 {
            few.put(key(n), n);
            ShardMap::put(&mut tree, key(n), n);
        }
        assert!(few.heap_bytes() * 4 < ShardMap::heap_bytes(&tree));
    }
}
