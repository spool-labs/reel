//! `TBTreeMap`: a B+ tree shaped for what a shard actually holds
//!
//! Nodes live in two arenas, leaves in one `Vec` and inner nodes in another, with a
//! child an index and a tag bit rather than a pointer. A node is searched on the
//! leading eight bytes of every key, kept as a `u64` beside them, with full keys
//! consulted only where those tie. The leaves are chained both ways, so an ordered
//! walk runs backwards at the speed it runs forwards, and a tree holds nothing until
//! something is put in it.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::ops::Bound;

const NONE: u32 = u32::MAX;

/// A key the tree can both order and discriminate in one word
///
/// Handing over the leading bytes as an integer is what lets a whole node be
/// searched with word compares, and generality is what that trades away. A key is
/// moved rather than copied, so one that owns its bytes is allowed here.
pub trait TreeKey: Ord + Clone + Borrow<Self::Probe> + Sized {
    /// What a lookup is given, which is the key itself where holding one is free
    ///
    /// A key that owns its bytes probes as the bytes, since the alternative is an
    /// allocation on every read of a key the reader may not even find.
    type Probe: Ord + ?Sized;

    /// Which word a node searches its lead array with
    ///
    /// `Whole` for a key whose leading bytes already discriminate, which is every
    /// fixed column. `Shared` for one carrying a bucket or a namespace in front,
    /// where the lead has to be read from past the bytes the node's keys share.
    type Window: LeadWindow<Self>;

    /// A key for a place a node has made but not filled
    ///
    /// Nothing reads them: every search and every walk is bounded by the node's own
    /// length rather than by the array's.
    fn filler() -> Self;

    /// The leading bytes as an integer, ordering the way the key itself does
    ///
    /// The contract is monotonicity: where two keys differ in their lead, the lead
    /// settles them the way `Ord` would, and where they tie the whole key is
    /// consulted. A lead that breaks that orders the tree wrongly.
    fn head(probe: &Self::Probe) -> u64;

    /// Open a slot in a node's keys, moving the run right of it along
    ///
    /// A move rather than a copy, since a key may own its bytes. A key that cannot
    /// own any overrides this with a memmove.
    fn open(keys: &mut [Self], slot: usize, len: usize) {
        keys[slot..=len].rotate_right(1);
    }

    /// Close the slot a key left, moving the run right of it down over it
    ///
    /// The leaving key ends at the tail, where the caller replaces it with a filler.
    fn close(keys: &mut [Self], slot: usize, len: usize) {
        keys[slot..len].rotate_left(1);
    }

    /// Move a run of keys into a fresh node, leaving fillers where they were
    fn hand_over(from: &mut [Self], to: &mut [Self], at: usize, count: usize) {
        for step in 0..count {
            to[step] = std::mem::replace(&mut from[at + step], Self::filler());
        }
    }

    /// A separator between two subtrees, and whether its lead can route alone
    ///
    /// Any value above everything left of a boundary and at or below everything right
    /// of it routes a descent exactly, so the shortest prefix of `right` that clears
    /// `left` is a separator. The flag says whether the eight bytes a lead holds reach
    /// the byte the boundary turned on; where they do not, the key rides along to
    /// settle the tie the lead cannot. `left` must sort strictly below `right`.
    fn separator(left: &Self, right: &Self) -> (Self, bool);
}

/// Where a probe sits against the bytes a node's entries have in common
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Place {
    /// Under everything the node holds, so the answer is its left edge
    Below,

    /// Carrying the shared bytes, so the lead array decides
    Inside,

    /// Over everything the node holds, so the answer is its right edge
    Above,
}

/// How one node turns a probe into the word its lead array is searched with
///
/// The lead array is sorted, so whatever this hands back has to be monotone in the
/// key order across the entries of that one node. Nothing is asked to be monotone
/// across nodes: a descent asks each node it lands on.
pub trait LeadWindow<K: TreeKey>: Clone + Default {
    /// The word this node's lead array is searched with, for a probe inside it
    fn lead(&self, probe: &K::Probe) -> u64;

    /// Whether the probe carries the bytes this node's entries share
    fn place(&self, probe: &K::Probe) -> Place;

    /// Bytes of key this window is reading its lead from past, which nothing needs
    fn skipped(&self) -> usize;

    /// Retune to what the node is holding, saying whether the leads must be redone
    ///
    /// Called where a node's contents change wholesale: a build, a split, and an
    /// insert that broke what the node had in common. The leads are rebuilt from the
    /// entries when this says so, so every entry a retuning node holds has to be
    /// recoverable from the node.
    fn tune<'a>(&mut self, held: impl Iterator<Item = &'a K::Probe>) -> bool
    where
        K::Probe: 'a;
}

/// The lead a key's own leading bytes make, which is what a fixed column takes
///
/// Zero sized, and every branch it decides folds away: a probe is always inside a
/// window that shares nothing, and a node that never retunes never rebuilds a lead.
#[derive(Clone, Default)]
pub struct Whole;

impl<K: TreeKey> LeadWindow<K> for Whole {
    fn lead(&self, probe: &K::Probe) -> u64 {
        K::head(probe)
    }

    fn place(&self, _probe: &K::Probe) -> Place {
        Place::Inside
    }

    fn skipped(&self) -> usize {
        0
    }

    fn tune<'a>(&mut self, _held: impl Iterator<Item = &'a K::Probe>) -> bool
    where
        K::Probe: 'a,
    {
        false
    }
}

/// Bytes of shared prefix a node holds inline to move its lead past
///
/// An object key is a thirty-two byte bucket address and then a name, so anything
/// under thirty-three leaves the lead reading bucket bytes and discriminating
/// nothing.
pub const SHARED_CAP: usize = 128;

/// The lead taken from past the bytes a node's own entries share
///
/// The one invariant: every entry in the node begins with `pre[..off]`. A probe that
/// does too is ordered against them by the eight bytes after it; one that does not is
/// below all of them or above all of them, decided by the same comparison that found
/// out, so nothing is left for the lead array to get wrong.
#[derive(Clone)]
pub struct Shared<const CAP: usize = SHARED_CAP> {
    /// Bytes of `pre` the node's entries are known to agree on
    off: u16,

    /// The agreed bytes themselves, held inline so a placement chases no pointer
    pre: [u8; CAP],
}

/// A window agreeing on nothing, which reads the lead from the front of the key
///
/// A fresh window has to hand back the same lead `TreeKey::head` does, or a bulk
/// build would fill its leads under one window and search them under another.
impl<const CAP: usize> Default for Shared<CAP> {
    fn default() -> Shared<CAP> {
        Shared {
            off: 0,
            pre: [0u8; CAP],
        }
    }
}

impl<K: TreeKey<Probe = [u8]>, const CAP: usize> LeadWindow<K> for Shared<CAP> {
    fn lead(&self, probe: &[u8]) -> u64 {
        let at = self.off as usize;
        let mut wide = [0u8; 8];
        if at < probe.len() {
            let take = (probe.len() - at).min(8);
            wide[..take].copy_from_slice(&probe[at..at + take]);
        }
        u64::from_be_bytes(wide)
    }

    fn place(&self, probe: &[u8]) -> Place {
        let at = self.off as usize;
        let cut = at.min(probe.len());
        match probe[..cut].cmp(&self.pre[..cut]) {
            Ordering::Less => Place::Below,
            Ordering::Greater => Place::Above,
            // A probe that runs out inside the shared bytes is a prefix of every
            // key the node holds, and a prefix sorts below what extends it.
            Ordering::Equal if probe.len() < at => Place::Below,
            Ordering::Equal => Place::Inside,
        }
    }

    fn skipped(&self) -> usize {
        self.off as usize
    }

    fn tune<'a>(&mut self, mut held: impl Iterator<Item = &'a [u8]>) -> bool {
        let Some(first) = held.next() else {
            return false;
        };
        let mut shared = first.len().min(CAP);
        for next in held {
            shared = shared.min(agreed::<CAP>(first, next));
            if shared == 0 {
                break;
            }
        }
        if shared == self.off as usize {
            return false;
        }
        self.off = shared as u16;
        self.pre[..shared].copy_from_slice(&first[..shared]);
        true
    }
}

/// Leading bytes two probes agree on, up to what a window will hold
fn agreed<const CAP: usize>(left: &[u8], right: &[u8]) -> usize {
    let cut = left.len().min(right.len()).min(CAP);
    left[..cut]
        .iter()
        .zip(&right[..cut])
        .take_while(|(a, b)| a == b)
        .count()
}

/// A key of the width its column declared, held inline
///
/// Short keys pad with zero, which keeps the order: the pad is the lowest byte, so a
/// key that is a prefix of another still sorts below it.
impl<const N: usize> TreeKey for [u8; N] {
    type Probe = [u8; N];
    type Window = Whole;

    fn filler() -> [u8; N] {
        [0u8; N]
    }

    /// One memmove, which is what a key held inline costs
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
        // The first byte the boundary's keys disagree on. Cutting there gives the
        // shortest prefix of `right` that still clears `left`, and zero padding keeps
        // it at or below `right`, which is the routing contract.
        let differs = left
            .iter()
            .zip(right.iter())
            .position(|(low, high)| low != high);
        let mut cut = [0u8; N];
        let take = differs.map_or(N, |at| at + 1);
        cut[..take].copy_from_slice(&right[..take]);
        (cut, take > 8)
    }
}

/// A name, held on the heap and probed as the bytes it holds
///
/// The separator is a truncation rather than a padding, since there is no width to
/// pad to, and it is always held whole beside its lead, which is what lets a node
/// that retunes rebuild its lead array from the separators it already keeps.
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
        // Where `left` is a prefix of `right` there is no disagreeing byte, and
        // the first byte past `left` is what clears it.
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

/// Bytes of key a node holds, which is where a column's width comes from
///
/// A node holds `B` whole keys beside the leads, so an insert shifts `B` times the
/// key's bytes and a tied run compares that many whole keys along the leaf. Both
/// scale with the product rather than the width, which is why a width taken at one
/// key size does not carry to another.
pub const NODE_BUDGET: usize = 1024;

/// The narrowest node the budget may ask for
///
/// Under sixteen the extra levels cost more than the bytes save.
pub const MIN_NODE_WIDTH: usize = 16;

/// The widest node the budget may ask for
///
/// A key of one word would take a hundred and twenty-eight on the budget alone, and
/// the returns are flat past sixty-four.
pub const MAX_NODE_WIDTH: usize = 64;

/// Keys a node holds, for a column whose keys are this many bytes
///
/// A kibibyte of key a node, held between the two bounds above.
pub const fn node_width(key_bytes: usize) -> usize {
    // A column may declare a zero width key, so the divisor is floored rather than
    // left to trap at compile time.
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

/// Node width for the trees the crate keys by one of its own scalars
///
/// A segment id, an lsn and a frontier's sequence number are a word or narrower,
/// which the budget takes to the ceiling.
pub const NODE_WIDTH: usize = node_width(size_of::<u64>());

/// Keys a batched descent keeps in flight at once
///
/// The line fill buffers cap how many misses a core can have outstanding, so this
/// is set where overlap stops being available rather than at what a caller asks for.
const LANES: usize = 16;

thread_local! {
    /// The stack one thread's sorted batched descents work down, kept between them
    ///
    /// A node number and the run beneath it, so nothing here is borrowed from a map
    /// and one thread's list serves every shard it descends.
    static DESCENT_STACK: std::cell::Cell<Vec<(u32, usize, usize)>> =
        const { std::cell::Cell::new(Vec::new()) };
}

/// This thread's descent stack, given back however the descent that took it ends
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

/// How many of a sorted lead array fall below the wanted one, on x86
///
/// The 512 bit form answers in the shape the question is asked, an unsigned compare
/// to a mask register a population count turns into the tally, where AVX2 has no
/// unsigned 64 bit compare and biases into signed space first. Chosen at runtime, so
/// one binary serves a fleet that is not all one generation.
#[cfg(target_arch = "x86_64")]
fn count_below(leads: &[u64], want: u64) -> usize {
    match backend() {
        // SAFETY: each arm runs only where the detection above found its
        // feature, and every load is bounded by the chunk iterator.
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
        // Detection takes the widest the processor has, so forcing the choice is
        // what lets one box exercise the narrower arms.
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

/// Eight lanes a step, the compare answering as a mask the tally counts
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

    // The high bit flipped turns an unsigned order into the signed one the
    // compare implements, which is exact rather than approximate.
    let bias = _mm256_set1_epi64x(i64::MIN);
    let wanted = _mm256_xor_si256(_mm256_set1_epi64x(want as i64), bias);
    let mut below = 0usize;
    let mut lanes = leads.chunks_exact(4);
    for lane in &mut lanes {
        let held = _mm256_xor_si256(_mm256_loadu_si256(lane.as_ptr() as *const __m256i), bias);
        // Greater-than with the operands swapped is the less-than wanted.
        let mask = _mm256_cmpgt_epi64(wanted, held);
        below += (_mm256_movemask_pd(_mm256_castsi256_pd(mask)) as u32).count_ones() as usize;
    }
    below + count_scalar(lanes.remainder(), want)
}

/// The same count, written so the optimiser is free to widen it
///
/// The idiomatic form on purpose: on aarch64 this compiles to eight lanes a step
/// against four accumulators, which is twice what a hand written NEON arm managed.
fn count_scalar(leads: &[u64], want: u64) -> usize {
    leads.iter().filter(|held| **held < want).count()
}

/// The same count, everywhere the hand arms are not carried
#[cfg(not(target_arch = "x86_64"))]
fn count_below(leads: &[u64], want: u64) -> usize {
    count_scalar(leads, want)
}

/// The three scans, callable directly, so a test can hold them against each other
///
/// `backend()` is a `OnceLock`, so a process gets one scan and cannot otherwise ask
/// the others what they would have said.
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

/// The one scan every other machine counts with
#[cfg(not(target_arch = "x86_64"))]
pub mod scans {
    /// The form this build uses, which is the only one it carries
    pub fn scalar(leads: &[u64], want: u64) -> usize {
        super::count_scalar(leads, want)
    }
}

/// Which scan this build counts leads with, for a box that fell back quietly
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

/// Whether a run is in key order, cheaply enough to ask on every call
///
/// Leads first, since two adjacent keys almost always differ inside their first
/// eight bytes, and the whole key only where they do not.
fn ordered<K: TreeKey>(keys: &[K]) -> bool {
    // A scalar loop rather than a vectorised one: the leads are not packed here,
    // they sit in keys `N` bytes apart, so there is no run to load.
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

/// Ask the machine for a line without waiting on it
#[inline(always)]
fn prefetch(ptr: *const u8) {
    // Inline asm because `core::arch::aarch64::_prefetch` is still unstable and
    // this crate builds on stable; the x86 intrinsic below is not.
    #[cfg(target_arch = "aarch64")]
    // SAFETY: a prefetch of any address is architecturally a hint and cannot
    // fault, and the pointer comes from a live arena slot regardless.
    unsafe {
        std::arch::asm!(
            "prfm pldl1keep, [{0}]",
            in(reg) ptr,
            options(nostack, readonly, preserves_flags)
        );
    }
    #[cfg(target_arch = "x86_64")]
    // SAFETY: as above, `_mm_prefetch` is a hint and never faults.
    unsafe {
        std::arch::x86_64::_mm_prefetch(ptr as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    let _ = ptr;
}

/// Set on a child index that names an inner node rather than a leaf
const INNER: u32 = 1 << 31;

fn is_inner(at: u32) -> bool {
    at & INNER != 0
}

fn slot_of(at: u32) -> usize {
    (at & !INNER) as usize
}

/// Where a key sits among a node's keys, or where it would go
///
/// A count rather than a bisect: bisecting the lead array costs data-dependent
/// branches no predictor can learn. Whole keys are read only across a tied run.
fn seek<K: TreeKey>(
    win: &K::Window,
    leads: &[u64],
    keys: &[K],
    len: usize,
    probe: &K::Probe,
) -> Result<usize, usize> {
    match win.place(probe) {
        Place::Below => return Err(0),
        Place::Above => return Err(len),
        Place::Inside => {}
    }
    let want = win.lead(probe);
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

struct Leaf<K: TreeKey, const B: usize, V: Default> {
    len: usize,
    win: K::Window,
    lead: [u64; B],
    keys: [K; B],
    vals: [V; B],
    next: u32,
    prev: u32,
}

struct Inner<K: TreeKey, const B: usize> {
    /// Separators the node routes by
    len: usize,

    /// How this node turns a probe into the word its leads are searched with
    win: K::Window,

    /// Leading bytes of each separator, the array a descent counts over
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

/// The separator at one slot, for a caller with no walk to carry a cursor for
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

/// The child a key descends into, counted from truncated separator leads
///
/// A separator's bytes past its lead are zeros, so across a tied lead it sorts at or
/// below every key sharing that lead and only a spilled separator has to be read. A
/// key equal to a separator belongs to its right.
fn inner_seek<K: TreeKey, const B: usize>(inner: &Inner<K, B>, probe: &K::Probe) -> usize {
    // A node dividing nothing routes everything the one way it can, and it is
    // also the one node whose window has no separator to have been tuned from.
    if inner.len == 0 {
        return 0;
    }
    match inner.win.place(probe) {
        Place::Below => return 0,
        Place::Above => return inner.len,
        Place::Inside => {}
    }
    let want = inner.win.lead(probe);
    let mut at = count_below(&inner.lead[..inner.len], want);
    // The cursor starts at the front and steps, so a whole tied walk costs one pass
    // over the spill rather than a search per slot.
    let mut cursor = 0;
    while at < inner.len && inner.lead[at] == want {
        match spill_at(&inner.spill, &mut cursor, at) {
            Some(full) if full.borrow() > probe => break,
            _ => at += 1,
        }
    }
    at
}

/// A probe's place in one node's order, as the pair a sorted run partitions on
///
/// The lead alone cannot order a probe the node's window puts outside itself, since
/// its bytes at the window are not comparable with the ones held.
fn rank<K: TreeKey>(win: &K::Window, probe: &K::Probe) -> (u8, u64) {
    match win.place(probe) {
        Place::Below => (0, 0),
        Place::Inside => (1, win.lead(probe)),
        Place::Above => (2, u64::MAX),
    }
}

/// A B+ tree over fixed width keys, nodes held in two arenas
pub struct TBTreeMap<K: TreeKey, const B: usize, V: Default> {
    leaves: Vec<Leaf<K, B, V>>,
    inners: Vec<Inner<K, B>>,
    root: u32,
    first: u32,
    len: usize,
}

/// A fresh node index, checked against the bit that tags an inner node
///
/// Leaf and inner indices share a `u32` with the top bit as the tag, so the arena's
/// ceiling is 2^31 nodes and nothing else checks it.
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
    ///
    /// A split does this on each half; an insert only where the arriving key broke
    /// what the rest agreed on.
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
    /// The same, over the separators, which a retuning key always holds whole
    ///
    /// A node that retunes rebuilds its leads out of its separators, so a separator
    /// held as a lead alone would leave a slot with nothing to rebuild from.
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

/// What the tree is holding, without asking its keys to be printable
///
/// Keys held and leaves occupied, since the two together say whether a repack is
/// owed, and a key's bytes are not what a surrounding `Debug` wants to see.
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
    /// A node holds at most `B` pairs and `B - 1` separators, so a width below two
    /// leaves no room to split; the ceiling is the spill slot's `u8`.
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

    /// What a key holds, to be changed in place rather than put back
    ///
    /// A counter beside a key is read, stepped and written again, and doing that
    /// through `insert` is a second descent for what the first one had.
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
            // The middle separator moves up a level, its spilled key with it:
            // it divided these halves down here and divides them from above.
            let lift_lead = self.inners[at].lead[half];
            let lift_spill = take_spill(&mut self.inners[at].spill, half);
            let mut right: Inner<K, B> = empty_inner();
            right.len = self.inners[at].len - half - 1;
            // The window rides across with the leads it was tuned for, so the
            // half arrives consistent and its own retune can only tighten it.
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
            // A half split would leave every left leaf at half fill forever under
            // ascending keys, so an append splits off only the tail.
            let half = match appending {
                true => self.leaves[at].len - 1,
                false => self.leaves[at].len / 2,
            };
            let mut right: Leaf<K, B, V> = empty_leaf();
            right.len = self.leaves[at].len - half;
            right.win = self.leaves[at].win.clone();
            right.lead[..right.len]
                .copy_from_slice(&self.leaves[at].lead[half..self.leaves[at].len]);
            // A key and a value are moved rather than copied: either may own
            // bytes, and nothing here is allowed to duplicate one.
            K::hand_over(&mut self.leaves[at].keys, &mut right.keys, half, right.len);
            for step in 0..right.len {
                right.vals[step] = std::mem::take(&mut self.leaves[at].vals[half + step]);
            }
            right.next = self.leaves[at].next;
            right.prev = at as u32;
            self.leaves[at].len = half;
            // A B+ leaf keeps its keys, so the separator is cut fresh at the
            // boundary: the shortest lead that clears the left half.
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
        // A lifted separator's lead was taken under whatever window cut it, and the
        // node taking it in reads its leads under its own, so where the two differ
        // the separator itself is what they are recomputed from.
        inner.lead[slot] = match &lift_spill {
            Some(full) => inner.win.lead(full.borrow()),
            None => lift_lead,
        };
        let broke = lift_spill
            .as_ref()
            .is_some_and(|full| inner.win.place(full.borrow()) != Place::Inside);
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
    ///
    /// The displaced value is what the index decides by: whether a write landed, lost
    /// to a newer version, or fell on a grave, and it puts the older one back when
    /// the newcomer loses.
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
                // An append is a key past the end of the whole tree, not past one
                // leaf's local end: a local test fires on every scattered insert at
                // a leaf's right edge and cuts off a leaf nothing ever fills.
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
                // A key the window puts outside itself is one the rest of the leaf
                // no longer agrees with, so the leads are owed a rebuild.
                let broke = leaf.win.place(key.borrow()) != Place::Inside;
                leaf.lead.copy_within(slot..leaf.len, slot + 1);
                // The tail slot holds a filler the shift carries down to `slot`,
                // where the arriving key replaces it, so nothing is cloned.
                K::open(&mut leaf.keys, slot, leaf.len);
                leaf.vals[slot..=leaf.len].rotate_right(1);
                leaf.lead[slot] = leaf.win.lead(key.borrow());
                leaf.keys[slot] = key;
                leaf.vals[slot] = val;
                leaf.len += 1;
                self.len += 1;
                if broke {
                    leaf.retune();
                }
                None
            }
        }
    }

    /// Build from sorted input in one pass, packing leaves to a target fill
    ///
    /// Driving a sorted run through `insert` pays a descent and a split per key to
    /// rediscover an order the input already had, where bottom up there are no splits
    /// at all. `fill` is how full a leaf is packed, full being densest and short
    /// leaving room for later inserts. The input must be in key order, which only a
    /// `debug_assert` says, and repeats are allowed with the last one winning.
    pub fn from_sorted<I: IntoIterator<Item = (K, V)>>(
        sorted: I,
        fill: usize,
    ) -> TBTreeMap<K, B, V> {
        let fill = fill.clamp(1, B);
        let sorted = sorted.into_iter();
        // A leaf is kilobytes wide, so growing the arena by doubling copies those
        // kilobytes again at every step.
        let expected = sorted.size_hint().0.div_ceil(fill).max(1);
        let mut tree: TBTreeMap<K, B, V> = TBTreeMap {
            leaves: Vec::with_capacity(expected),
            inners: Vec::new(),
            root: NONE,
            first: NONE,
            len: 0,
        };

        for (key, val) in sorted {
            // A repeat takes the previous key's value rather than a place of its own.
            // Without this the tree holds the key twice: `len` counts both, `get`
            // answers with the first, and `remove` leaves the other behind.
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
            // The lead goes in under the window the leaf has so far. Tuning happens
            // once the leaf is closed, since what its keys share is not known until
            // the last of them has arrived.
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

        // Each level names the level below by the leaves at its two ends rather than
        // by their keys, so a separator is cut where the keys already sit and nothing
        // is copied to carry a span upward.
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
    ///
    /// `remove` has no borrow and no merge, so a leaf keeps its place in the chain
    /// however few keys are left in it and an ordered walk pays for every one it
    /// steps over. One pass over the live pairs, already in order, and a bottom-up
    /// build with no splits. The whole tree is rebuilt at once, so a caller holding
    /// a lock holds it for the pass.
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

    /// What a key holds, put there first if it was holding nothing
    ///
    /// The tally shape: a book keyed by segment or by sequence number is read,
    /// stepped and written back.
    pub fn get_or_insert(&mut self, key: K, val: V) -> &mut V {
        if self.get(key.borrow()).is_none() {
            self.insert(key.clone(), val);
        }
        self.get_mut(key.borrow()).expect("the key was just put in")
    }

    /// Whether a key is held, without reaching for what it holds
    pub fn contains_key(&self, probe: &K::Probe) -> bool {
        self.get(probe).is_some()
    }

    /// Drop every key, keeping the arenas for what comes next
    ///
    /// The `Vec`s keep their capacity, so a shard cleared by a group drop takes its
    /// next fill without asking the allocator again.
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

    /// Every pair inside a span, in key order
    ///
    /// One descent to place the low bound and then a walk along the leaves, so a
    /// resumable sweep pays one descent per resumption rather than one per key.
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
                    // Bounded by the leaf's own length, not the array's: past the
                    // length sit fillers a remove or a split left behind.
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

    /// The same span walked from its high end down
    ///
    /// The leaves are chained both ways, so this is the forward walk with the links
    /// reversed. Walking forward and reversing would have to hold a whole shard to
    /// hand back the last few keys of it.
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

    /// Take a key out, leaving the leaf that held it shorter
    ///
    /// No borrow and no merge, which is a choice: this tree is rebuilt from sorted
    /// input at every install and every compaction, so the cheaper answer to a sparse
    /// tree is to rebuild it rather than carry merge logic down every delete.
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
        // The shift carries the leaving key and value to the tail, where taking them
        // leaves a filler behind. A key that owns bytes moves rather than copies.
        K::close(&mut leaf.keys, found, leaf.len);
        leaf.vals[found..leaf.len].rotate_left(1);
        drop(std::mem::replace(&mut leaf.keys[leaf.len - 1], K::filler()));
        let held = std::mem::take(&mut leaf.vals[leaf.len - 1]);
        leaf.len -= 1;
        self.len -= 1;
        Some(held)
    }

    /// Take a key out and pack behind it where the room is worth taking back
    ///
    /// `repack_owed` is the doubling guard, so the pass runs once per doubling of the
    /// room it would give back.
    pub fn remove_packed(&mut self, probe: &K::Probe) -> Option<V> {
        let held = self.remove(probe)?;
        if self.repack_owed() {
            self.repack(B);
        }
        Some(held)
    }

    /// Live keys against the room the leaves occupy, one being a packed tree
    ///
    /// A separator left behind by a delete still routes, so an emptied leaf keeps
    /// its place in the chain and its share of the footprint.
    pub fn fill_factor(&self) -> f64 {
        match self.leaves.is_empty() {
            true => 1.0,
            false => self.len as f64 / (self.leaves.len() * B) as f64,
        }
    }

    /// Leaves the tree occupies, emptied ones included
    pub fn leaf_count(&self) -> usize {
        self.leaves.len()
    }

    /// Whether packing would give back room worth the pass
    ///
    /// Fill alone cannot say: a shard holding three keys reads as badly under-filled,
    /// and packing it would move three keys into the same one leaf. What decides it
    /// is the leaves held against the leaves needed.
    pub fn repack_owed(&self) -> bool {
        self.leaves.len() > 1 && self.leaves.len() >= 2 * self.len.div_ceil(B).max(1)
    }

    /// The share of held keys whose lead is already taken by the key before them
    ///
    /// The lead is a bet that a few bytes separate most pairs: near zero where they
    /// do, climbing toward one where every slot in a node holds the same lead. Costs
    /// a walk, so it is asked rather than kept.
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

    /// Bytes of shared prefix the leaves are reading their leads from past
    ///
    /// The mean over the leaves, unweighted, since what is wanted is whether the
    /// windows moved. Zero on every fixed column, where there is nothing to move past.
    pub fn lead_skip(&self) -> f64 {
        let mut total = 0usize;
        let mut seen = 0usize;
        let mut at = self.first;
        while at != NONE {
            let leaf = &self.leaves[at as usize];
            if leaf.len > 0 {
                total += leaf.win.skipped();
                seen += 1;
            }
            at = leaf.next;
        }
        match seen {
            0 => 0.0,
            _ => total as f64 / seen as f64,
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

    /// The highest key held and what it holds
    pub fn last_key_value(&self) -> Option<(&K, &V)> {
        let mut at = self.last_leaf()?;
        while at != NONE {
            let leaf = &self.leaves[slot_of(at)];
            if leaf.len > 0 {
                return Some((&leaf.keys[leaf.len - 1], &leaf.vals[leaf.len - 1]));
            }
            at = leaf.prev;
        }
        None
    }

    /// Many keys at once, descending them in lockstep
    ///
    /// A single descent is a chain of dependent cache misses, one a level. Stepping a
    /// batch a level at a time issues every key's load for that level before waiting
    /// on any, so the misses overlap rather than queue. Fixed lanes rather than a
    /// vector per call, which would allocate on every single-key call.
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
                    // Start the next level's miss now rather than trusting the out
                    // of order window to reach it on the following pass.
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

    /// The same batched descent with the prefetch left out, to price it
    ///
    /// The control the prefetch is priced against, since the two are otherwise
    /// inseparable.
    pub fn get_many_cold<'a>(&'a self, keys: &[K], out: &mut Vec<Option<&'a V>>) {
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
    ///
    /// Neighbouring sorted keys descend through the same upper nodes, so this
    /// partitions the run against a node's separators in one pass and hands each
    /// child the run that belongs to it, turning the top levels from a cost per key
    /// into a cost per node. A run that is not sorted is answered by the lane batch,
    /// which needs no order.
    pub fn get_many_sorted<'a>(&'a self, keys: &[K], out: &mut Vec<Option<&'a V>>) {
        if !ordered(keys) {
            return self.get_many(keys, out);
        }
        out.clear();
        out.resize(keys.len(), None);
        if keys.is_empty() || self.root == NONE {
            return;
        }

        // The descent's own stack, kept by the thread: it holds node numbers and
        // positions and nothing borrowed, and a wide run grows it a level at a time,
        // which was an allocation or two per batch on top of the first.
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
                    // A key equal to the separator belongs to its right, the same
                    // rule the single key descent takes. An unspilled separator's
                    // bytes past the lead are zeros, which no key sharing the lead
                    // sorts under, and the rank carries what the lead cannot: a key
                    // the node's window puts outside itself.
                    true => {
                        // The separator belongs to the slot rather than to the key,
                        // so it is found once for the run instead of once per key.
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
        prefetch(ptr as *const u8);
    }

    /// Whole leaves in key order, for a caller that wants to run its own loop
    ///
    /// `iter` yields a pair at a time and pays the closure and its bounds checks on
    /// every one, where a B+ leaf is already a run to loop over tightly.
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
