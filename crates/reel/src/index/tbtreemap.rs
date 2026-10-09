//! `TBTreeMap`: a B+ tree with arena nodes searched on an eight-byte lead per key

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::ops::Bound;

const NONE: u32 = u32::MAX;

/// A key the tree can order and search by one leading word
pub trait TreeKey: Ord + Clone + Borrow<Self::Probe> + Sized {
    /// What a lookup takes: the key itself, or the bytes for a key that owns them
    type Probe: Ord + ?Sized;

    /// How a node derives its search word: `Whole` for numbers, `Shared` for bytes
    type Window: LeadWindow<Self>;

    /// A placeholder key for an unused slot, which nothing reads
    fn filler() -> Self;

    /// The leading bytes as an integer, which must agree with `Ord` wherever two leads differ
    fn head(probe: &Self::Probe) -> u64;

    /// Open a slot in a node's keys, moving the keys right of it up one
    fn open(keys: &mut [Self], slot: usize, len: usize) {
        keys[slot..=len].rotate_right(1);
    }

    /// Close a key's slot, moving the keys right of it down so the leaving key ends at the tail
    fn close(keys: &mut [Self], slot: usize, len: usize) {
        keys[slot..len].rotate_left(1);
    }

    /// Move a run of keys into a fresh node, leaving fillers where they were
    fn hand_over(from: &mut [Self], to: &mut [Self], at: usize, count: usize) {
        for step in 0..count {
            to[step] = std::mem::replace(&mut from[at + step], Self::filler());
        }
    }

    /// A separator between `left` and a greater `right`, and whether it must be kept whole
    fn separator(left: &Self, right: &Self) -> (Self, bool);
}

/// The edge a probe takes when a node's window puts it outside the node
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Place {
    /// Under everything the node holds, so the answer is its left edge
    Below,

    /// Over everything the node holds, so the answer is its right edge
    Above,
}

/// How a node turns a probe into its search word, which must be monotone within the node
pub trait LeadWindow<K: TreeKey>: Clone + Default {
    /// The search word for an entry the node holds, used when it rebuilds its leads
    fn lead(&self, probe: &K::Probe) -> u64;

    /// The search word for a probe, or the edge a probe outside the node takes
    fn word(&self, probe: &K::Probe) -> Result<u64, Place>;

    /// Retune to the node's entries, returning whether the leads must be rebuilt
    fn tune<'a>(&mut self, held: impl Iterator<Item = &'a K::Probe>) -> bool
    where
        K::Probe: 'a;
}

/// The window for numbers, which reads the key's own leading bytes
#[derive(Clone, Default)]
pub struct Whole;

impl<K: TreeKey> LeadWindow<K> for Whole {
    fn lead(&self, probe: &K::Probe) -> u64 {
        K::head(probe)
    }

    fn word(&self, probe: &K::Probe) -> Result<u64, Place> {
        Ok(K::head(probe))
    }

    fn tune<'a>(&mut self, _held: impl Iterator<Item = &'a K::Probe>) -> bool
    where
        K::Probe: 'a,
    {
        false
    }
}

/// A node holds up to this many shared prefix bytes inline and reads its lead past them
pub const SHARED_CAP: usize = 128;

/// A window that reads the lead past the prefix every entry in the node shares
#[derive(Clone)]
pub struct Shared<const CAP: usize = SHARED_CAP> {
    /// How many bytes of `pre` the node's entries agree on, zero or at least eight
    off: u16,

    /// The agreed bytes themselves, held inline so a placement chases no pointer
    pre: [u8; CAP],
}

/// An empty window reads the lead from the front of the key, the same as `TreeKey::head`
impl<const CAP: usize> Default for Shared<CAP> {
    fn default() -> Shared<CAP> {
        Shared {
            off: 0,
            pre: [0u8; CAP],
        }
    }
}

impl<const CAP: usize> Shared<CAP> {
    /// The eight bytes at the window, zero padded where the key runs out under it
    fn at_window(&self, probe: &[u8]) -> u64 {
        let at = self.off as usize;
        let mut wide = [0u8; 8];
        if at + 8 <= probe.len() {
            wide.copy_from_slice(&probe[at..at + 8]);
        } else if at < probe.len() {
            let take = probe.len() - at;
            wide[..take].copy_from_slice(&probe[at..]);
        }
        u64::from_be_bytes(wide)
    }

    /// Whether a probe holds the node's agreed bytes, or the edge it takes if it does not
    fn inside(&self, probe: &[u8]) -> Result<(), Place> {
        let at = self.off as usize;
        if at == 0 {
            return Ok(());
        }
        let cut = at.min(probe.len());
        match against(&probe[..cut], &self.pre[..cut]) {
            Ordering::Less => Err(Place::Below),
            Ordering::Greater => Err(Place::Above),
            // A probe shorter than the shared bytes is a prefix of every key here and sorts below
            Ordering::Equal if probe.len() < at => Err(Place::Below),
            Ordering::Equal => Ok(()),
        }
    }
}

impl<K: TreeKey, const CAP: usize> LeadWindow<K> for Shared<CAP>
where
    K::Probe: AsRef<[u8]>,
{
    fn lead(&self, probe: &K::Probe) -> u64 {
        self.at_window(probe.as_ref())
    }

    fn word(&self, probe: &K::Probe) -> Result<u64, Place> {
        let probe = probe.as_ref();
        self.inside(probe)?;
        Ok(self.at_window(probe))
    }

    fn tune<'a>(&mut self, held: impl Iterator<Item = &'a K::Probe>) -> bool
    where
        K::Probe: 'a,
    {
        let mut held = held.map(AsRef::as_ref);
        let Some(first) = held.next() else {
            return false;
        };
        let mut shared = first.len().min(CAP);
        for next in held {
            shared = shared.min(agreed::<CAP>(first, next));
            if shared < 8 {
                break;
            }
        }
        // A shared run under eight bytes adds nothing the lead does not already read
        let shared = match shared < 8 {
            true => 0,
            false => shared,
        };
        // Compare the bytes too, since a refilled node can share as many bytes but other ones
        if shared == self.off as usize && self.pre[..shared] == first[..shared] {
            return false;
        }
        self.off = shared as u16;
        self.pre[..shared].copy_from_slice(&first[..shared]);
        true
    }
}

/// How a probe's leading bytes compare with a node's window, a word at a time
fn against(probe: &[u8], pre: &[u8]) -> Ordering {
    let mut done = 0;
    while done + 8 <= probe.len() {
        let held = u64::from_be_bytes(probe[done..done + 8].try_into().expect("eight bytes"));
        let want = u64::from_be_bytes(pre[done..done + 8].try_into().expect("eight bytes"));
        if held != want {
            return held.cmp(&want);
        }
        done += 8;
    }
    for (held, want) in probe[done..].iter().zip(&pre[done..]) {
        if held != want {
            return held.cmp(want);
        }
    }
    Ordering::Equal
}

/// How many leading bytes two probes share, capped at `CAP`
fn agreed<const CAP: usize>(left: &[u8], right: &[u8]) -> usize {
    let cut = left.len().min(right.len()).min(CAP);
    left[..cut]
        .iter()
        .zip(&right[..cut])
        .take_while(|(a, b)| a == b)
        .count()
}

/// A fixed-width key held inline, its window capped at the key's width
impl<const N: usize> TreeKey for [u8; N] {
    type Probe = [u8; N];
    type Window = Shared<N>;

    fn filler() -> [u8; N] {
        [0u8; N]
    }

    fn open(keys: &mut [Self], slot: usize, len: usize) {
        keys.copy_within(slot..len, slot + 1);
    }

    fn close(keys: &mut [Self], slot: usize, len: usize) {
        keys.copy_within(slot + 1..len, slot);
    }

    fn hand_over(from: &mut [Self], to: &mut [Self], at: usize, count: usize) {
        to[..count].copy_from_slice(&from[at..at + count]);
    }

    fn head(probe: &[u8; N]) -> u64 {
        let mut wide = [0u8; 8];
        let take = if N < 8 { N } else { 8 };
        wide[..take].copy_from_slice(&probe[..take]);
        u64::from_be_bytes(wide)
    }

    fn separator(left: &Self, right: &Self) -> (Self, bool) {
        // Cut `right` just past the first byte that differs from `left`, zero padding the rest
        let differs = left
            .iter()
            .zip(right.iter())
            .position(|(low, high)| low != high);
        let mut cut = [0u8; N];
        let take = differs.map_or(N, |at| at + 1);
        cut[..take].copy_from_slice(&right[..take]);
        // Always kept whole, since a retuning node rebuilds its leads from the separators
        (cut, true)
    }
}

/// A heap-held key probed as its bytes, with separators cut short and always kept whole
impl TreeKey for Box<[u8]> {
    type Probe = [u8];
    type Window = Shared;

    fn filler() -> Box<[u8]> {
        Box::from([].as_slice())
    }

    fn head(probe: &[u8]) -> u64 {
        let mut wide = [0u8; 8];
        let take = probe.len().min(8);
        wide[..take].copy_from_slice(&probe[..take]);
        u64::from_be_bytes(wide)
    }

    fn separator(left: &Self, right: &Self) -> (Self, bool) {
        // If `left` is a prefix of `right`, the first byte past `left` clears it
        let differs = left
            .iter()
            .zip(right.iter())
            .position(|(low, high)| low != high);
        let take = differs.map_or(left.len(), |at| at) + 1;
        (Box::from(&right[..take.min(right.len())]), true)
    }
}

/// A number is its own lead, which is the whole key in one compare
impl TreeKey for u64 {
    type Probe = u64;
    type Window = Whole;

    fn filler() -> u64 {
        0
    }

    fn head(probe: &u64) -> u64 {
        *probe
    }

    fn separator(_left: &Self, right: &Self) -> (Self, bool) {
        (*right, false)
    }
}

impl TreeKey for u32 {
    type Probe = u32;
    type Window = Whole;

    fn filler() -> u32 {
        0
    }

    fn head(probe: &u32) -> u64 {
        *probe as u64
    }

    fn separator(_left: &Self, right: &Self) -> (Self, bool) {
        (*right, false)
    }
}

/// Each node holds about this many bytes of key, which sets a column's node width
pub const NODE_BUDGET: usize = 1024;

/// The narrowest node the budget may ask for
pub const MIN_NODE_WIDTH: usize = 16;

/// The widest node the budget may ask for
pub const MAX_NODE_WIDTH: usize = 64;

/// How many keys a node holds for a key of `key_bytes`, clamped between the width bounds
pub const fn node_width(key_bytes: usize) -> usize {
    // A column may declare a zero width key, so floor the divisor at one
    let key = match key_bytes {
        0 => 1,
        held => held,
    };
    match NODE_BUDGET / key {
        narrow if narrow < MIN_NODE_WIDTH => MIN_NODE_WIDTH,
        wide if wide > MAX_NODE_WIDTH => MAX_NODE_WIDTH,
        held => held,
    }
}

/// Node width for trees keyed by one of the crate's own word-sized scalars
pub const NODE_WIDTH: usize = node_width(size_of::<u64>());

/// A batched descent keeps this many keys in flight at once
const LANES: usize = 16;

thread_local! {
    /// Each thread keeps its sorted batched descent stack here between calls
    static DESCENT_STACK: std::cell::Cell<Vec<(u32, usize, usize)>> =
        const { std::cell::Cell::new(Vec::new()) };
}

/// This thread's descent stack, returned on drop however the descent ends
struct HeldDescent(Vec<(u32, usize, usize)>);

impl HeldDescent {
    fn take() -> HeldDescent {
        HeldDescent(DESCENT_STACK.with(std::cell::Cell::take))
    }
}

impl Drop for HeldDescent {
    fn drop(&mut self) {
        let mut held = std::mem::take(&mut self.0);
        held.clear();
        DESCENT_STACK.with(|spare| spare.set(held));
    }
}

/// How many leads fall below `want`, using the widest scan this x86 core has
#[cfg(target_arch = "x86_64")]
fn count_below(leads: &[u64], want: u64) -> usize {
    match backend() {
        // SAFETY: each arm runs only where `backend` found its feature, and loads stay in bounds
        Scan::Avx512 => unsafe { count_avx512(leads, want) },
        Scan::Avx2 => unsafe { count_avx2(leads, want) },
        Scan::Scalar => count_scalar(leads, want),
    }
}

/// Which scan the processor supports
#[cfg(target_arch = "x86_64")]
#[derive(Clone, Copy, PartialEq)]
enum Scan {
    Avx512,
    Avx2,
    Scalar,
}

#[cfg(target_arch = "x86_64")]
fn backend() -> Scan {
    use std::sync::OnceLock;
    static CHOSEN: OnceLock<Scan> = OnceLock::new();
    *CHOSEN.get_or_init(|| {
        // `REEL_SCAN` forces a narrower arm so one box can test them all
        match std::env::var("REEL_SCAN").ok().as_deref() {
            Some("avx2") => return Scan::Avx2,
            Some("scalar") => return Scan::Scalar,
            _ => {}
        }
        if is_x86_feature_detected!("avx512f") {
            Scan::Avx512
        } else if is_x86_feature_detected!("avx2") {
            Scan::Avx2
        } else {
            Scan::Scalar
        }
    })
}

/// Eight lanes a step, counting the bits of the compare mask
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn count_avx512(leads: &[u64], want: u64) -> usize {
    use std::arch::x86_64::*;

    let wanted = _mm512_set1_epi64(want as i64);
    let mut below = 0usize;
    let mut lanes = leads.chunks_exact(8);
    for lane in &mut lanes {
        let held = _mm512_loadu_si512(lane.as_ptr() as *const __m512i);
        below += _mm512_cmplt_epu64_mask(held, wanted).count_ones() as usize;
    }
    below + count_scalar(lanes.remainder(), want)
}

/// Four lanes a step, biased into signed space because there is no unsigned form
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn count_avx2(leads: &[u64], want: u64) -> usize {
    use std::arch::x86_64::*;

    // Flipping the high bit maps unsigned order onto the signed compare
    let bias = _mm256_set1_epi64x(i64::MIN);
    let wanted = _mm256_xor_si256(_mm256_set1_epi64x(want as i64), bias);
    let mut below = 0usize;
    let mut lanes = leads.chunks_exact(4);
    for lane in &mut lanes {
        let held = _mm256_xor_si256(_mm256_loadu_si256(lane.as_ptr() as *const __m256i), bias);
        // Greater-than with swapped operands is the less-than we want
        let mask = _mm256_cmpgt_epi64(wanted, held);
        below += (_mm256_movemask_pd(_mm256_castsi256_pd(mask)) as u32).count_ones() as usize;
    }
    below + count_scalar(lanes.remainder(), want)
}

/// The same count in plain Rust, which the optimiser widens on its own
fn count_scalar(leads: &[u64], want: u64) -> usize {
    leads.iter().filter(|held| **held < want).count()
}

/// The same count on targets without the hand-written arms
#[cfg(not(target_arch = "x86_64"))]
fn count_below(leads: &[u64], want: u64) -> usize {
    count_scalar(leads, want)
}

/// The three scans, callable directly so a test can compare them
#[cfg(target_arch = "x86_64")]
pub mod scans {
    /// The widest form, where the processor has it
    ///
    /// # Safety
    /// The caller must have found `avx512f` before calling this.
    pub unsafe fn avx512(leads: &[u64], want: u64) -> usize {
        unsafe { super::count_avx512(leads, want) }
    }

    /// The four lane form
    ///
    /// # Safety
    /// The caller must have found `avx2` before calling this.
    pub unsafe fn avx2(leads: &[u64], want: u64) -> usize {
        unsafe { super::count_avx2(leads, want) }
    }

    /// The form every machine has
    pub fn scalar(leads: &[u64], want: u64) -> usize {
        super::count_scalar(leads, want)
    }
}

/// The only scan on targets other than x86_64
#[cfg(not(target_arch = "x86_64"))]
pub mod scans {
    /// The form this build uses
    pub fn scalar(leads: &[u64], want: u64) -> usize {
        super::count_scalar(leads, want)
    }
}

/// The scan this process counts leads with, to spot a box that fell back quietly
pub fn scan_backend() -> &'static str {
    #[cfg(target_arch = "x86_64")]
    return match backend() {
        Scan::Avx512 => "avx512f",
        Scan::Avx2 => "avx2",
        Scan::Scalar => "scalar",
    };
    #[cfg(not(target_arch = "x86_64"))]
    return "scalar";
}

/// Whether a run is in key order, comparing leads first and whole keys only on a tie
fn ordered<K: TreeKey>(keys: &[K]) -> bool {
    let Some(first) = keys.first() else {
        return true;
    };
    let mut held = K::head(first.borrow());
    for pair in keys.windows(2) {
        let next = K::head(pair[1].borrow());
        if held > next || (held == next && pair[0] > pair[1]) {
            return false;
        }
        held = next;
    }
    true
}

/// The tag bit set on a child index that points at an inner node
const INNER: u32 = 1 << 31;

fn is_inner(at: u32) -> bool {
    at & INNER != 0
}

fn slot_of(at: u32) -> usize {
    (at & !INNER) as usize
}

/// Where a key sits among a node's keys, or where it would go, found by counting leads
fn seek<K: TreeKey>(
    win: &K::Window,
    leads: &[u64],
    keys: &[K],
    len: usize,
    probe: &K::Probe,
) -> Result<usize, usize> {
    let want = match win.word(probe) {
        Ok(want) => want,
        Err(Place::Below) => return Err(0),
        Err(Place::Above) => return Err(len),
    };
    let mut at = count_below(&leads[..len], want);
    while at < len && leads[at] == want {
        match keys[at].borrow().cmp(probe) {
            Ordering::Less => at += 1,
            Ordering::Equal => return Ok(at),
            Ordering::Greater => return Err(at),
        }
    }
    Err(at)
}

/// `repr(C)` keeps the window on the same cache line as the length
#[repr(C)]
struct Leaf<K: TreeKey, const B: usize, V: Default> {
    len: usize,
    win: K::Window,
    lead: [u64; B],
    keys: [K; B],
    vals: [V; B],
    next: u32,
    prev: u32,
}

/// An inner node, laid out like `Leaf` so the window shares the length's cache line
#[repr(C)]
struct Inner<K: TreeKey, const B: usize> {
    /// How many separators the node routes by
    len: usize,

    /// How this node turns a probe into its search word
    win: K::Window,

    /// The lead of each separator, which a descent counts over
    lead: [u64; B],

    /// The children a descent routes into
    kids: [u32; B],

    /// Whole separators for the slots whose lead cannot route alone, in slot order
    spill: Vec<(u8, K)>,
}

/// Where a slot's separator would sit in a run held in slot order
fn spill_from<K: TreeKey>(spill: &[(u8, K)], slot: usize) -> usize {
    spill.partition_point(|(held, _)| (*held as usize) < slot)
}

/// The separator at one slot, moving a cursor along a run walked in slot order
fn spill_at<'a, K: TreeKey>(
    spill: &'a [(u8, K)],
    cursor: &mut usize,
    slot: usize,
) -> Option<&'a K> {
    while *cursor < spill.len() && (spill[*cursor].0 as usize) < slot {
        *cursor += 1;
    }
    match spill.get(*cursor) {
        Some((held, key)) if *held as usize == slot => Some(key),
        _ => None,
    }
}

/// The separator at one slot, found by search without a cursor
fn spill_of<K: TreeKey>(spill: &[(u8, K)], slot: usize) -> Option<&K> {
    let at = spill_from(spill, slot);
    match spill.get(at) {
        Some((held, key)) if *held as usize == slot => Some(key),
        _ => None,
    }
}

/// Put a separator in at its slot, keeping the run in slot order
fn put_spill<K: TreeKey>(spill: &mut Vec<(u8, K)>, slot: usize, key: K) {
    let at = spill_from(spill, slot);
    spill.insert(at, (slot as u8, key));
}

/// Take a separator out, for a lift that moves it up a level
fn take_spill<K: TreeKey>(spill: &mut Vec<(u8, K)>, slot: usize) -> Option<K> {
    let at = spill_from(spill, slot);
    match spill.get(at) {
        Some((held, _)) if *held as usize == slot => Some(spill.remove(at).1),
        _ => None,
    }
}

/// Move the spills right of the cut into a fresh node, rebased past it
fn split_spill<K: TreeKey>(spill: &mut Vec<(u8, K)>, half: usize) -> Vec<(u8, K)> {
    let mut moved = Vec::new();
    let mut kept = Vec::new();
    for (slot, key) in spill.drain(..) {
        match (slot as usize) > half {
            true => moved.push((slot - half as u8 - 1, key)),
            false => kept.push((slot, key)),
        }
    }
    *spill = kept;
    moved
}

/// Open a separator slot, stepping the spills at or right of it along
fn shift_spill<K: TreeKey>(spill: &mut [(u8, K)], slot: usize) {
    for (held, _) in spill.iter_mut() {
        if *held as usize >= slot {
            *held += 1;
        }
    }
}

/// The child a key descends into, where a key equal to a separator goes right
fn inner_seek<K: TreeKey, const B: usize>(inner: &Inner<K, B>, probe: &K::Probe) -> usize {
    // A node with no separators has one child and an untuned window
    if inner.len == 0 {
        return 0;
    }
    let want = match inner.win.word(probe) {
        Ok(want) => want,
        Err(Place::Below) => return 0,
        Err(Place::Above) => return inner.len,
    };
    let mut at = count_below(&inner.lead[..inner.len], want);
    // One cursor walks the spill once across the whole tied run
    let mut cursor = 0;
    while at < inner.len && inner.lead[at] == want {
        match spill_at(&inner.spill, &mut cursor, at) {
            Some(full) if full.borrow() > probe => break,
            _ => at += 1,
        }
    }
    at
}

/// A probe's place in one node's order, ranking probes outside the window at the edges
fn rank<K: TreeKey>(win: &K::Window, probe: &K::Probe) -> (u8, u64) {
    match win.word(probe) {
        Ok(lead) => (1, lead),
        Err(Place::Below) => (0, 0),
        Err(Place::Above) => (2, u64::MAX),
    }
}

/// A B+ tree with its nodes held in two arenas
pub struct TBTreeMap<K: TreeKey, const B: usize, V: Default> {
    leaves: Vec<Leaf<K, B, V>>,
    inners: Vec<Inner<K, B>>,
    root: u32,
    first: u32,
    len: usize,
}

/// The index of the node just pushed, debug-checked against the inner tag bit
fn arena_index(len: usize) -> u32 {
    debug_assert!(
        len < INNER as usize,
        "the arena reached the tag bit at {len} nodes"
    );
    len as u32 - 1
}

fn empty_leaf<K: TreeKey, const B: usize, V: Default>() -> Leaf<K, B, V> {
    Leaf {
        len: 0,
        win: K::Window::default(),
        lead: [0u64; B],
        keys: std::array::from_fn(|_| K::filler()),
        vals: std::array::from_fn(|_| V::default()),
        next: NONE,
        prev: NONE,
    }
}

fn empty_inner<K: TreeKey, const B: usize>() -> Inner<K, B> {
    Inner {
        len: 0,
        win: K::Window::default(),
        lead: [0u64; B],
        kids: [NONE; B],
        spill: Vec::new(),
    }
}

impl<K: TreeKey, const B: usize, V: Default> Leaf<K, B, V> {
    /// Retune the window to the keys held and redo the leads it moved
    fn retune(&mut self) {
        if !self
            .win
            .tune(self.keys[..self.len].iter().map(Borrow::borrow))
        {
            return;
        }
        for slot in 0..self.len {
            self.lead[slot] = self.win.lead(self.keys[slot].borrow());
        }
    }
}

impl<K: TreeKey, const B: usize> Inner<K, B> {
    /// Retune the window to the separators, which a retuning node always keeps whole
    fn retune(&mut self) {
        if !self
            .win
            .tune(self.spill.iter().map(|(_, key)| key.borrow()))
        {
            return;
        }
        debug_assert_eq!(
            self.spill.len(),
            self.len,
            "a node that retunes must hold every separator it routes by",
        );
        for slot in 0..self.len {
            let held = spill_of(&self.spill, slot).expect("a retuning node holds every separator");
            self.lead[slot] = self.win.lead(held.borrow());
        }
    }
}

/// Prints key and leaf counts, which say whether a repack is owed
impl<K: TreeKey, const B: usize, V: Default> std::fmt::Debug for TBTreeMap<K, B, V> {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            out,
            "TBTreeMap({} keys, {} leaves)",
            self.len,
            self.leaves.len()
        )
    }
}

impl<K: TreeKey, const B: usize, V: Default> Default for TBTreeMap<K, B, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: TreeKey, const B: usize, V: Default> TBTreeMap<K, B, V> {
    /// `B` must be at least two to split and at most 256 to fit a spill slot's `u8`
    const WIDE_ENOUGH: () = assert!(
        B >= 2 && B <= 256,
        "a node width below two cannot split, one past 256 cannot spill"
    );

    /// An empty tree, holding no node at all until something is put in it
    pub fn new() -> TBTreeMap<K, B, V> {
        let () = Self::WIDE_ENOUGH;
        TBTreeMap {
            leaves: Vec::new(),
            inners: Vec::new(),
            root: NONE,
            first: NONE,
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the map holds nothing
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get(&self, probe: &K::Probe) -> Option<&V> {
        let mut at = self.root;
        if at == NONE {
            return None;
        }
        while is_inner(at) {
            let inner = &self.inners[slot_of(at)];
            at = inner.kids[inner_seek(inner, probe)];
        }
        let leaf = &self.leaves[slot_of(at)];
        match seek(&leaf.win, &leaf.lead, &leaf.keys, leaf.len, probe) {
            Ok(found) => Some(&leaf.vals[found]),
            Err(_) => None,
        }
    }

    /// A mutable reference to a key's value, for changing it in place
    pub fn get_mut(&mut self, probe: &K::Probe) -> Option<&mut V> {
        let mut at = self.root;
        if at == NONE {
            return None;
        }
        while is_inner(at) {
            let inner = &self.inners[slot_of(at)];
            at = inner.kids[inner_seek(inner, probe)];
        }
        let leaf = &mut self.leaves[slot_of(at)];
        match seek(&leaf.win, &leaf.lead, &leaf.keys, leaf.len, probe) {
            Ok(found) => Some(&mut leaf.vals[found]),
            Err(_) => None,
        }
    }

    fn full(&self, at: u32) -> bool {
        match is_inner(at) {
            true => self.inners[slot_of(at)].len == B - 1,
            false => self.leaves[slot_of(at)].len == B,
        }
    }

    /// Split a full child of `parent` at `slot`, lifting one separator up
    fn split_child(&mut self, parent: usize, slot: usize, appending: bool) {
        let child = self.inners[parent].kids[slot];

        let (lift_lead, lift_spill, fresh) = if is_inner(child) {
            let at = slot_of(child);
            let half = self.inners[at].len / 2;
            // The middle separator moves up a level, with its spilled key
            let lift_lead = self.inners[at].lead[half];
            let lift_spill = take_spill(&mut self.inners[at].spill, half);
            let mut right: Inner<K, B> = empty_inner();
            right.len = self.inners[at].len - half - 1;
            // The right half starts with the left's window, which matches the copied leads
            right.win = self.inners[at].win.clone();
            right.lead[..right.len]
                .copy_from_slice(&self.inners[at].lead[half + 1..self.inners[at].len]);
            let kid_count = right.len + 1;
            right.kids[..kid_count]
                .copy_from_slice(&self.inners[at].kids[half + 1..self.inners[at].len + 1]);
            right.spill = split_spill(&mut self.inners[at].spill, half);
            self.inners[at].len = half;
            right.retune();
            self.inners[at].retune();
            self.inners.push(right);
            (
                lift_lead,
                lift_spill,
                INNER | arena_index(self.inners.len()),
            )
        } else {
            let at = slot_of(child);
            // An append splits off only the last key so ascending inserts keep leaves nearly full
            let half = match appending {
                true => self.leaves[at].len - 1,
                false => self.leaves[at].len / 2,
            };
            let mut right: Leaf<K, B, V> = empty_leaf();
            right.len = self.leaves[at].len - half;
            right.win = self.leaves[at].win.clone();
            right.lead[..right.len]
                .copy_from_slice(&self.leaves[at].lead[half..self.leaves[at].len]);
            // Keys and values move, since either may own bytes
            K::hand_over(&mut self.leaves[at].keys, &mut right.keys, half, right.len);
            for step in 0..right.len {
                right.vals[step] = std::mem::take(&mut self.leaves[at].vals[half + step]);
            }
            right.next = self.leaves[at].next;
            right.prev = at as u32;
            self.leaves[at].len = half;
            // Cut a fresh separator at the boundary, since a B+ leaf keeps its keys
            let (lift_key, lift_whole) =
                K::separator(&self.leaves[at].keys[half - 1], &right.keys[0]);
            let lift_lead = K::head(lift_key.borrow());
            let lift_spill = lift_whole.then_some(lift_key);
            right.retune();
            self.leaves[at].retune();
            self.leaves.push(right);
            let right_at = arena_index(self.leaves.len());
            self.leaves[at].next = right_at;
            let after = self.leaves[right_at as usize].next;
            if after != NONE {
                self.leaves[after as usize].prev = right_at;
            }
            (lift_lead, lift_spill, right_at)
        };

        let inner = &mut self.inners[parent];
        inner.lead.copy_within(slot..inner.len, slot + 1);
        inner.kids.copy_within(slot + 1..inner.len + 1, slot + 2);
        shift_spill(&mut inner.spill, slot);
        // Re-lead a spilled separator under this node's window, and retune if it falls outside
        let lifted = lift_spill
            .as_ref()
            .map(|full| inner.win.word(full.borrow()));
        inner.lead[slot] = match lifted {
            Some(word) => word.unwrap_or(0),
            None => lift_lead,
        };
        let broke = matches!(lifted, Some(Err(_)));
        if let Some(full) = lift_spill {
            put_spill(&mut inner.spill, slot, full);
        }
        inner.kids[slot + 1] = fresh;
        inner.len += 1;
        if broke {
            inner.retune();
        }
    }

    /// Put a key in, handing back what it displaced
    pub fn insert(&mut self, key: K, val: V) -> Option<V> {
        if self.root == NONE {
            self.leaves.push(empty_leaf());
            self.root = arena_index(self.leaves.len());
            self.first = self.root;
        }
        if self.full(self.root) {
            let mut fresh: Inner<K, B> = empty_inner();
            fresh.kids[0] = self.root;
            self.inners.push(fresh);
            let root_at = arena_index(self.inners.len()) as usize;
            self.root = INNER | root_at as u32;
            self.split_child(root_at, 0, false);
        }

        let mut at = self.root;
        while is_inner(at) {
            let parent = slot_of(at);
            let inner = &self.inners[parent];
            let down = inner_seek(inner, key.borrow());
            let child = inner.kids[down];
            if self.full(child) {
                // Only a key past the last key of the last leaf counts as an append
                let appending = !is_inner(child) && {
                    let leaf = &self.leaves[slot_of(child)];
                    leaf.next == NONE && leaf.len > 0 && key > leaf.keys[leaf.len - 1]
                };
                self.split_child(parent, down, appending);
                let inner = &self.inners[parent];
                at = inner.kids[inner_seek(inner, key.borrow())];
            } else {
                at = child;
            }
        }

        let leaf = &mut self.leaves[slot_of(at)];
        match seek(&leaf.win, &leaf.lead, &leaf.keys, leaf.len, key.borrow()) {
            Ok(found) => Some(std::mem::replace(&mut leaf.vals[found], val)),
            Err(slot) => {
                // A key outside the window breaks what the leaf shares, so the leads are rebuilt
                let word = leaf.win.word(key.borrow());
                leaf.lead.copy_within(slot..leaf.len, slot + 1);
                // The shift moves the tail filler down to `slot`, where the new key replaces it
                K::open(&mut leaf.keys, slot, leaf.len);
                leaf.vals[slot..=leaf.len].rotate_right(1);
                leaf.lead[slot] = word.unwrap_or(0);
                leaf.keys[slot] = key;
                leaf.vals[slot] = val;
                leaf.len += 1;
                self.len += 1;
                if word.is_err() {
                    leaf.retune();
                }
                None
            }
        }
    }

    /// Build from key-ordered input in one pass with `fill` keys a leaf, the last repeat wins
    pub fn from_sorted<I: IntoIterator<Item = (K, V)>>(
        sorted: I,
        fill: usize,
    ) -> TBTreeMap<K, B, V> {
        let fill = fill.clamp(1, B);
        let sorted = sorted.into_iter();
        // Reserve up front, since each leaf is kilobytes wide and doubling would copy them
        let expected = sorted.size_hint().0.div_ceil(fill).max(1);
        let mut tree: TBTreeMap<K, B, V> = TBTreeMap {
            leaves: Vec::with_capacity(expected),
            inners: Vec::new(),
            root: NONE,
            first: NONE,
            len: 0,
        };

        for (key, val) in sorted {
            // A repeat overwrites the previous key's value, or the tree would hold the key twice
            if let Some(leaf) = tree.leaves.last_mut() {
                if leaf.len > 0 {
                    debug_assert!(
                        leaf.keys[leaf.len - 1] <= key,
                        "from_sorted was handed input out of order"
                    );
                    if leaf.keys[leaf.len - 1] == key {
                        leaf.vals[leaf.len - 1] = val;
                        continue;
                    }
                }
            }
            let fresh = match tree.leaves.last() {
                Some(leaf) => leaf.len == fill,
                None => true,
            };
            if fresh {
                if let Some(done) = tree.leaves.last_mut() {
                    done.retune();
                }
                tree.leaves.push(empty_leaf());
                let at = tree.leaves.len() - 1;
                if at > 0 {
                    tree.leaves[at - 1].next = at as u32;
                    tree.leaves[at].prev = (at - 1) as u32;
                }
            }
            let leaf = tree.leaves.last_mut().expect("a leaf was just made");
            // Tune once the leaf is closed, when what its keys share is known
            leaf.lead[leaf.len] = leaf.win.lead(key.borrow());
            leaf.keys[leaf.len] = key;
            leaf.vals[leaf.len] = val;
            leaf.len += 1;
            tree.len += 1;
        }

        if tree.leaves.is_empty() {
            return tree;
        }
        if let Some(done) = tree.leaves.last_mut() {
            done.retune();
        }
        tree.first = 0;

        // Each entry is a subtree's first leaf, last leaf and node index
        let mut level: Vec<(u32, u32, u32)> = (0..tree.leaves.len())
            .map(|at| (at as u32, at as u32, at as u32))
            .collect();

        while level.len() > 1 {
            let mut up: Vec<(u32, u32, u32)> = Vec::new();
            for run in level.chunks(B) {
                let mut inner: Inner<K, B> = empty_inner();
                inner.len = run.len() - 1;
                for (slot, (low, _, at)) in run.iter().enumerate() {
                    inner.kids[slot] = *at;
                    if slot > 0 {
                        let left = &tree.leaves[run[slot - 1].1 as usize];
                        let right = &tree.leaves[*low as usize];
                        let (sep, whole) = K::separator(&left.keys[left.len - 1], &right.keys[0]);
                        inner.lead[slot - 1] = K::head(sep.borrow());
                        if whole {
                            inner.spill.push((slot as u8 - 1, sep));
                        }
                    }
                }
                inner.retune();
                tree.inners.push(inner);
                up.push((
                    run[0].0,
                    run[run.len() - 1].1,
                    INNER | arena_index(tree.inners.len()),
                ));
            }
            level = up;
        }

        tree.root = level[0].2;
        tree
    }

    /// Pack the tree back to full leaves, dropping the room deletion left
    pub fn repack(&mut self, fill: usize) {
        if self.root == NONE {
            return;
        }
        let mut chain = Vec::with_capacity(self.leaves.len());
        let mut at = self.first;
        while at != NONE {
            chain.push(at as usize);
            at = self.leaves[at as usize].next;
        }
        let mut pairs: Vec<(K, V)> = Vec::with_capacity(self.len);
        for leaf_at in chain {
            let leaf = &mut self.leaves[leaf_at];
            for slot in 0..leaf.len {
                pairs.push((
                    std::mem::replace(&mut leaf.keys[slot], K::filler()),
                    std::mem::take(&mut leaf.vals[slot]),
                ));
            }
            leaf.len = 0;
        }
        *self = TBTreeMap::from_sorted(pairs, fill);
    }

    /// A key's value, inserting `val` first if the key is absent
    pub fn get_or_insert(&mut self, key: K, val: V) -> &mut V {
        if self.get(key.borrow()).is_none() {
            self.insert(key.clone(), val);
        }
        self.get_mut(key.borrow()).expect("the key was just put in")
    }

    /// Whether a key is held
    pub fn contains_key(&self, probe: &K::Probe) -> bool {
        self.get(probe).is_some()
    }

    /// Drop every key, keeping the arenas' capacity for the next fill
    pub fn clear(&mut self) {
        self.leaves.clear();
        self.inners.clear();
        self.root = NONE;
        self.first = NONE;
        self.len = 0;
    }

    /// Where a key sits, as the leaf holding it and the slot within
    fn seat(&self, probe: &K::Probe) -> Option<(u32, usize)> {
        let mut at = self.root;
        if at == NONE {
            return None;
        }
        while is_inner(at) {
            let inner = &self.inners[slot_of(at)];
            at = inner.kids[inner_seek(inner, probe)];
        }
        let leaf = &self.leaves[slot_of(at)];
        let slot = match seek(&leaf.win, &leaf.lead, &leaf.keys, leaf.len, probe) {
            Ok(found) => found,
            Err(step) => step,
        };
        Some((at, slot))
    }

    /// The rightmost leaf, which a backward walk with no bound starts at
    fn last_leaf(&self) -> Option<u32> {
        let mut at = self.root;
        if at == NONE {
            return None;
        }
        while is_inner(at) {
            let inner = &self.inners[slot_of(at)];
            at = inner.kids[inner.len];
        }
        Some(at)
    }

    /// Every pair inside a span in key order, from one descent and a walk along the leaves
    pub fn range<'a>(
        &'a self,
        low: Bound<&K>,
        high: Bound<&'a K>,
    ) -> impl Iterator<Item = (&'a K, &'a V)> {
        let (mut at, mut slot) = match low {
            Bound::Unbounded => (self.first, 0usize),
            Bound::Included(key) => self.seat(key.borrow()).unwrap_or((NONE, 0)),
            Bound::Excluded(key) => match self.seat(key.borrow()) {
                Some((at, slot)) => {
                    let leaf = &self.leaves[slot_of(at)];
                    // Past the leaf's length sit fillers, so check `slot < leaf.len` first
                    match slot < leaf.len && leaf.keys[slot] == *key {
                        true => (at, slot + 1),
                        false => (at, slot),
                    }
                }
                None => (NONE, 0),
            },
        };

        std::iter::from_fn(move || loop {
            if at == NONE {
                return None;
            }
            let leaf = &self.leaves[slot_of(at)];
            if slot >= leaf.len {
                at = leaf.next;
                slot = 0;
                continue;
            }
            let (key, val) = (&leaf.keys[slot], &leaf.vals[slot]);
            let inside = match high {
                Bound::Unbounded => true,
                Bound::Included(end) => key <= end,
                Bound::Excluded(end) => key < end,
            };
            if !inside {
                return None;
            }
            slot += 1;
            return Some((key, val));
        })
    }

    /// Every pair from a low bound on, a leaf's run at a time
    pub fn range_runs<'a>(&'a self, low: Bound<&K>) -> impl Iterator<Item = (&'a [K], &'a [V])> {
        let (mut at, mut slot) = match low {
            Bound::Unbounded => (self.first, 0usize),
            Bound::Included(key) => self.seat(key.borrow()).unwrap_or((NONE, 0)),
            Bound::Excluded(key) => match self.seat(key.borrow()) {
                Some((at, slot)) => {
                    let leaf = &self.leaves[slot_of(at)];
                    match slot < leaf.len && leaf.keys[slot] == *key {
                        true => (at, slot + 1),
                        false => (at, slot),
                    }
                }
                None => (NONE, 0),
            },
        };
        std::iter::from_fn(move || loop {
            if at == NONE {
                return None;
            }
            let leaf = &self.leaves[slot_of(at)];
            let from = slot;
            at = leaf.next;
            slot = 0;
            if from < leaf.len {
                return Some((&leaf.keys[from..leaf.len], &leaf.vals[from..leaf.len]));
            }
        })
    }

    /// The same span walked from its high end down
    pub fn range_back<'a>(
        &'a self,
        low: Bound<&'a K>,
        high: Bound<&K>,
    ) -> impl Iterator<Item = (&'a K, &'a V)> {
        let (mut at, mut slot) = match high {
            Bound::Unbounded => match self.last_leaf() {
                Some(at) => (at, self.leaves[slot_of(at)].len),
                None => (NONE, 0),
            },
            Bound::Included(key) => match self.seat(key.borrow()) {
                Some((at, slot)) => {
                    let leaf = &self.leaves[slot_of(at)];
                    match slot < leaf.len && leaf.keys[slot] == *key {
                        true => (at, slot + 1),
                        false => (at, slot),
                    }
                }
                None => (NONE, 0),
            },
            Bound::Excluded(key) => self.seat(key.borrow()).unwrap_or((NONE, 0)),
        };

        std::iter::from_fn(move || loop {
            if at == NONE {
                return None;
            }
            if slot == 0 {
                at = self.leaves[slot_of(at)].prev;
                slot = match at == NONE {
                    true => 0,
                    false => self.leaves[slot_of(at)].len,
                };
                continue;
            }
            slot -= 1;
            let leaf = &self.leaves[slot_of(at)];
            let (key, val) = (&leaf.keys[slot], &leaf.vals[slot]);
            let inside = match low {
                Bound::Unbounded => true,
                Bound::Included(end) => key >= end,
                Bound::Excluded(end) => key > end,
            };
            if !inside {
                return None;
            }
            return Some((key, val));
        })
    }

    /// Take a key out, leaving its leaf shorter with no borrow or merge
    pub fn remove(&mut self, probe: &K::Probe) -> Option<V> {
        let mut at = self.root;
        if at == NONE {
            return None;
        }
        while is_inner(at) {
            let inner = &self.inners[slot_of(at)];
            at = inner.kids[inner_seek(inner, probe)];
        }

        let leaf = &mut self.leaves[slot_of(at)];
        let found = seek(&leaf.win, &leaf.lead, &leaf.keys, leaf.len, probe).ok()?;
        leaf.lead.copy_within(found + 1..leaf.len, found);
        // Shift the leaving key and value to the tail and leave a filler there
        K::close(&mut leaf.keys, found, leaf.len);
        leaf.vals[found..leaf.len].rotate_left(1);
        drop(std::mem::replace(&mut leaf.keys[leaf.len - 1], K::filler()));
        let held = std::mem::take(&mut leaf.vals[leaf.len - 1]);
        leaf.len -= 1;
        self.len -= 1;
        Some(held)
    }

    /// Take a key out and repack once `repack_owed` says the room is worth taking back
    pub fn remove_packed(&mut self, probe: &K::Probe) -> Option<V> {
        let held = self.remove(probe)?;
        if self.repack_owed() {
            self.repack(B);
        }
        Some(held)
    }

    /// Live keys over leaf slots, where one means packed
    pub fn fill_factor(&self) -> f64 {
        match self.leaves.is_empty() {
            true => 1.0,
            false => self.len as f64 / (self.leaves.len() * B) as f64,
        }
    }

    /// How many leaves the tree occupies, emptied ones included
    pub fn leaf_count(&self) -> usize {
        self.leaves.len()
    }

    /// How many bytes the arenas and spills allocated, spare capacity included
    pub fn heap_bytes(&self) -> u64 {
        let spills: usize = self
            .inners
            .iter()
            .map(|inner| inner.spill.capacity() * std::mem::size_of::<(u8, K)>())
            .sum();
        (self.leaves.capacity() * std::mem::size_of::<Leaf<K, B, V>>()
            + self.inners.capacity() * std::mem::size_of::<Inner<K, B>>()
            + spills) as u64
    }

    /// Whether the tree is packed, counting leaves one key short as full
    pub fn is_packed(&self) -> bool {
        self.leaves.len() <= self.len.div_ceil(B - 1).max(1)
    }

    /// Whether the tree holds at least twice the leaves it needs, so a repack is worth it
    pub fn repack_owed(&self) -> bool {
        self.leaves.len() > 1 && self.leaves.len() >= 2 * self.len.div_ceil(B).max(1)
    }

    /// The share of held keys whose lead equals the lead before them, found by a walk
    pub fn tie_rate(&self) -> f64 {
        let mut tied = 0usize;
        let mut seen = 0usize;
        let mut at = self.first;
        while at != NONE {
            let leaf = &self.leaves[at as usize];
            for slot in 1..leaf.len {
                seen += 1;
                if leaf.lead[slot] == leaf.lead[slot - 1] {
                    tied += 1;
                }
            }
            at = leaf.next;
        }
        match seen {
            0 => 0.0,
            _ => tied as f64 / seen as f64,
        }
    }

    /// The lowest key held and what it holds
    pub fn first_key_value(&self) -> Option<(&K, &V)> {
        let mut at = self.first;
        while at != NONE {
            let leaf = &self.leaves[at as usize];
            if leaf.len > 0 {
                return Some((&leaf.keys[0], &leaf.vals[0]));
            }
            at = leaf.next;
        }
        None
    }

    /// Many keys at once, descending them in lockstep so their cache misses overlap
    pub fn get_many<'a>(&'a self, keys: &[K], out: &mut Vec<Option<&'a V>>) {
        out.clear();
        if self.root == NONE {
            out.resize(keys.len(), None);
            return;
        }
        for run in keys.chunks(LANES) {
            let mut at = [0u32; LANES];
            at[..run.len()].fill(self.root);

            let mut moving = true;
            while moving {
                moving = false;
                for slot in 0..run.len() {
                    if !is_inner(at[slot]) {
                        continue;
                    }
                    moving = true;
                    let inner = &self.inners[slot_of(at[slot])];
                    at[slot] = inner.kids[inner_seek(inner, run[slot].borrow())];
                    // Prefetch the next level now
                    self.touch(at[slot]);
                }
            }

            for slot in 0..run.len() {
                let leaf = &self.leaves[slot_of(at[slot])];
                out.push(
                    match seek(
                        &leaf.win,
                        &leaf.lead,
                        &leaf.keys,
                        leaf.len,
                        run[slot].borrow(),
                    ) {
                        Ok(found) => Some(&leaf.vals[found]),
                        Err(_) => None,
                    },
                );
            }
        }
    }

    /// A sorted batch, seeking each shared node once for the run beneath it
    pub fn get_many_sorted<'a>(&'a self, keys: &[K], out: &mut Vec<Option<&'a V>>) {
        if !ordered(keys) {
            return self.get_many(keys, out);
        }
        out.clear();
        out.resize(keys.len(), None);
        if keys.is_empty() || self.root == NONE {
            return;
        }

        // Reuse this thread's stack so a batch does not allocate
        let mut held = HeldDescent::take();
        let work = &mut held.0;
        work.push((self.root, 0usize, keys.len()));
        while let Some((at, low, high)) = work.pop() {
            if !is_inner(at) {
                let leaf = &self.leaves[slot_of(at)];
                for slot in low..high {
                    out[slot] = match seek(
                        &leaf.win,
                        &leaf.lead,
                        &leaf.keys,
                        leaf.len,
                        keys[slot].borrow(),
                    ) {
                        Ok(found) => Some(&leaf.vals[found]),
                        Err(_) => None,
                    };
                }
                continue;
            }

            let inner = &self.inners[slot_of(at)];
            let mut slot = low;
            for sep in 0..=inner.len {
                if slot >= high {
                    break;
                }
                let end = match sep < inner.len {
                    // Equal keys go right, and `rank` orders keys outside the window
                    true => {
                        // Look the separator up once for the whole run
                        let held = spill_of(&inner.spill, sep);
                        let mut walk = slot;
                        while walk < high && {
                            let want = rank::<K>(&inner.win, keys[walk].borrow());
                            want < (1, inner.lead[sep])
                                || (want == (1, inner.lead[sep])
                                    && held.is_some_and(|full| keys[walk] < *full))
                        } {
                            walk += 1;
                        }
                        walk
                    }
                    false => high,
                };
                if end > slot {
                    work.push((inner.kids[sep], slot, end));
                    self.touch(inner.kids[sep]);
                }
                slot = end;
            }
        }
    }

    /// Ask for a node's lead array early, without waiting on it
    #[inline(always)]
    fn touch(&self, at: u32) {
        let ptr = match is_inner(at) {
            true => self.inners[slot_of(at)].lead.as_ptr(),
            false => self.leaves[slot_of(at)].lead.as_ptr(),
        };
        crate::io::mapping::prefetch(ptr as *const u8);
    }

    /// Whole leaves in key order, for a caller that wants to run its own loop
    pub fn chunks(&self) -> impl Iterator<Item = (&[K], &[V])> {
        let mut at = self.first;
        std::iter::from_fn(move || {
            if at == NONE {
                return None;
            }
            let leaf = &self.leaves[at as usize];
            at = leaf.next;
            Some((&leaf.keys[..leaf.len], &leaf.vals[..leaf.len]))
        })
    }

    /// Every pair in key order, which on a B+ tree is a run along the leaves
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        let mut at = self.first;
        let mut slot = 0usize;
        std::iter::from_fn(move || loop {
            if at == NONE {
                return None;
            }
            let leaf = &self.leaves[at as usize];
            if slot < leaf.len {
                slot += 1;
                return Some((&leaf.keys[slot - 1], &leaf.vals[slot - 1]));
            }
            at = leaf.next;
            slot = 0;
        })
    }
}
