//! Footer rows that carry what a key does not share with the row before it
//!
//! A footer holds a column's keys sorted, and sorted keys share their fronts, so a
//! row stores how much it shares with the row before it and only the rest. That
//! costs the arithmetic a flat run of fixed-width rows is binary searched by, so
//! every so many rows a restart carries its key whole; the restarts are a sorted
//! array to bisect, and a search lands in one small block and walks it. A footer
//! row's tail is its record's sequence, offset, length and flags, and inside a
//! restart block each of the numbers is kept as its difference from the row before.

use std::cmp::Ordering;

use crate::error::{ReelError, Result};
use crate::format::footer::ENTRY_TAIL_LEN;

/// Rows between restart points
///
/// Fewer restarts saves bytes and makes the walk after a seek longer.
pub const RESTART_INTERVAL: usize = 16;

/// Widest tail a row holds once read back, which sizes the buffer a row decodes into
const TAIL_CAP: usize = 32;

/// Where a footer entry tail keeps its offset, its length and its flags
const OFFSET_AT: usize = 8;
const LEN_AT: usize = 12;
const FLAGS_AT: usize = 16;

/// A footer entry tail is its sequence, offset, length and flags in that order
const _: () = assert!(ENTRY_TAIL_LEN == FLAGS_AT + 1 && ENTRY_TAIL_LEN <= TAIL_CAP);

/// How a row's tail lies behind its key
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Tail {
    /// A footer entry's sequence, offset, length and flags, the numbers kept as their
    /// difference from the row before in the same restart block
    #[default]
    Entry,

    /// This many bytes, as they stand
    Raw(usize),
}

impl Tail {
    /// Bytes a tail takes once read back
    pub fn len(self) -> usize {
        match self {
            Tail::Entry => ENTRY_TAIL_LEN,
            Tail::Raw(len) => len,
        }
    }
}

/// A tail read back whole
type Whole = [u8; TAIL_CAP];

/// One row read off the packed bytes
struct Row<'a> {
    /// Bytes of key the row shares with the row before it
    shared: usize,

    /// The rest of the key
    suffix: &'a [u8],

    /// The tail, whole, in the first `Tail::len` bytes
    tail: Whole,

    /// Where the next row starts
    next: usize,
}

/// A column's rows, each carrying only what it does not share with the one before
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PrefixRows {
    /// The rows themselves, in the shape they are written in
    packed: Vec<u8>,

    /// Where each restart row begins, which is what a search bisects
    restarts: Vec<u32>,

    /// Rows held, since a variable stride cannot divide it out of the length
    rows: usize,

    /// The last key appended, which the next append measures its share against
    last: Vec<u8>,

    /// The last tail appended, which the next append writes its numbers against
    last_tail: Whole,

    /// How every row's tail lies
    tail: Tail,
}

impl PrefixRows {
    pub fn new(tail: Tail) -> PrefixRows {
        PrefixRows {
            last_tail: [0; TAIL_CAP],
            tail,
            ..PrefixRows::default()
        }
    }

    /// An empty block with room for about this many bytes of rows, so an encode does not
    /// regrow its buffer a doubling at a time
    pub fn with_capacity(tail: Tail, bytes: usize) -> PrefixRows {
        PrefixRows {
            packed: Vec::with_capacity(bytes),
            ..PrefixRows::new(tail)
        }
    }

    /// Rows the block holds
    pub fn len(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Bytes the packed rows occupy, which is what the saving is measured on
    pub fn packed_len(&self) -> usize {
        self.packed.len()
    }

    /// Restart points held, one per `RESTART_INTERVAL` rows
    pub fn restarts(&self) -> usize {
        self.restarts.len()
    }

    /// Append one row, which must not sort before the row already at the end
    ///
    /// Ascending order is the encoding's premise: rows arriving out of order would
    /// each share almost nothing and the block would grow rather than shrink.
    pub fn push(&mut self, key: &[u8], tail: &[u8]) -> Result<()> {
        if self.rows > 0 && key < self.last.as_slice() {
            return Err(ReelError::Rejected(
                "a prefix-compressed row arrived out of order".to_string(),
            ));
        }
        if tail.len() != self.tail.len() {
            return Err(ReelError::Rejected(
                "a packed row's tail is not the width its rows declare".to_string(),
            ));
        }

        let restart = self.rows.is_multiple_of(RESTART_INTERVAL);
        if restart {
            self.restarts.push(self.packed.len() as u32);
            self.last_tail = [0; TAIL_CAP];
        }
        let shared = match restart {
            // A restart carries its key whole, so the array of them is searchable
            // without walking anything.
            true => 0,
            false => shared_prefix(&self.last, key),
        };

        let suffix = &key[shared..];
        put_varint(&mut self.packed, shared as u64);
        put_varint(&mut self.packed, suffix.len() as u64);
        self.packed.extend_from_slice(suffix);
        put_tail(&mut self.packed, self.tail, tail, &self.last_tail);

        self.last.clear();
        self.last.extend_from_slice(key);
        self.last_tail[..tail.len()].copy_from_slice(tail);
        self.rows += 1;
        Ok(())
    }

    /// One row, read against the tail of the row before it in its block
    fn row_at(&self, at: usize, before: &Whole) -> Result<Row<'_>> {
        read_row(&self.packed, at, self.tail, before)
    }

    /// The whole key of a restart row, which is the only key stored outright
    fn restart_key(&self, restart: usize) -> Result<&[u8]> {
        let row = self.row_at(self.restarts[restart] as usize, &[0; TAIL_CAP])?;
        match row.shared {
            0 => Ok(row.suffix),
            _ => Err(ReelError::Corruption(
                "a restart row does not carry its whole key".to_string(),
            )),
        }
    }

    /// The first row at or after a key, and the bytes behind it
    ///
    /// Bisects the restarts, then walks the one block that can hold the answer,
    /// comparing against the packed form rather than against rebuilt keys.
    pub fn seek(&self, target: &[u8]) -> Result<Option<Found>> {
        if self.rows == 0 {
            return Ok(None);
        }

        // The last restart at or below the target, whose block is the only one
        // that can hold the first row at or after it.
        let mut low = 0usize;
        let mut high = self.restarts.len();
        while low < high {
            let mid = (low + high) / 2;
            match self.restart_key(mid)?.cmp(target) {
                Ordering::Less => low = mid + 1,
                _ => high = mid,
            }
        }
        let block = low.saturating_sub(usize::from(low > 0 || low == self.restarts.len()));
        self.walk(block, target)
    }

    /// Walk one restart block for the first row at or after the target
    ///
    /// No case rebuilds a key: what the walk carries is how far the target matched
    /// the previous row, and a row's shared length against that decides it.
    fn walk(&self, block: usize, target: &[u8]) -> Result<Option<Found>> {
        let mut at = self.restarts[block] as usize;
        let mut index = block * RESTART_INTERVAL;
        let end = match self.restarts.get(block + 1) {
            Some(next) => *next as usize,
            None => self.packed.len(),
        };

        // How much of the target matched the key of the row just examined, which
        // is what lets the next row be decided without being rebuilt.
        let mut matched = 0usize;
        let mut order = Ordering::Less;
        let mut before = [0u8; TAIL_CAP];

        while at < end {
            let row = self.row_at(at, &before)?;
            let cmp = match row.shared.cmp(&matched) {
                // The row agrees with the previous key past where the target
                // stopped agreeing, so the previous comparison still decides.
                Ordering::Greater => order,
                // The row leaves the previous key first, so one byte decides.
                Ordering::Less => match row.suffix.first() {
                    Some(byte) => target[row.shared].cmp(byte),
                    None => Ordering::Greater,
                },
                // They diverge together, so the remainder decides it.
                Ordering::Equal => target[matched..].cmp(row.suffix),
            };

            if cmp != Ordering::Greater {
                return Ok(Some(Found {
                    index,
                    tail: row.tail[..self.tail.len()].to_vec(),
                }));
            }

            // Carry forward how far the target agrees with the row just passed,
            // which for a row the target sorts above is the whole of the row.
            matched = match row.shared.cmp(&matched) {
                Ordering::Equal => row.shared + shared_prefix(&target[matched..], row.suffix),
                Ordering::Less => row.shared,
                Ordering::Greater => matched,
            };
            order = cmp;
            before = row.tail;
            at = row.next;
            index += 1;
        }

        // Past the end of this block, so the answer is the next block's first row
        // when there is one.
        match self.restarts.get(block + 1) {
            Some(_) => self.walk(block + 1, target),
            None => Ok(None),
        }
    }

    /// Every key the block holds, rebuilt, which only a filter build asks for
    pub fn keys(&self) -> Result<Vec<Vec<u8>>> {
        let mut out = Vec::with_capacity(self.rows);
        let mut cursor = PrefixCursor::new(self);
        while cursor.advance()? {
            out.push(cursor.key().to_vec());
        }
        Ok(out)
    }
}

impl PrefixRows {
    /// The rows rebuilt whole, keys and tails packed back to back behind starts
    ///
    /// The shape every in-memory reader searches, so the prefix form stays an
    /// on-disk encoding only.
    pub fn unpacked(&self) -> Result<(Vec<u8>, Vec<u32>)> {
        // Prefix sharing and the tail's differences only remove bytes, so the encoded
        // length is a floor for the rebuilt one and seeding with it skips the early
        // doublings.
        let mut packed = Vec::with_capacity(self.packed.len());
        let mut starts = Vec::with_capacity(self.rows + 1);
        starts.push(0u32);
        let mut cursor = PrefixCursor::new(self);
        while cursor.advance()? {
            packed.extend_from_slice(cursor.key());
            packed.extend_from_slice(cursor.tail());
            starts.push(packed.len() as u32);
        }
        Ok((packed, starts))
    }
}

/// Where a seek landed and what the row carried
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Found {
    /// The row's position in the partition
    pub index: usize,

    /// The bytes stored behind the key
    pub tail: Vec<u8>,
}

fn shared_prefix(left: &[u8], right: &[u8]) -> usize {
    left.iter().zip(right).take_while(|(a, b)| a == b).count()
}

/// Append a number seven bits a byte, low bits first
fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Read a number `put_varint` wrote, stepping past it
fn get_varint(bytes: &[u8], at: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes
            .get(*at)
            .ok_or_else(|| ReelError::Corruption("a footer row number is truncated".to_string()))?;
        *at += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(ReelError::Corruption(
        "a footer row number runs past ten bytes".to_string(),
    ))
}

/// A signed difference folded so small ones of either sign stay small
fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn unzigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

fn u64_at(tail: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(tail[at..at + 8].try_into().expect("eight bytes"))
}

fn u32_at(tail: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(tail[at..at + 4].try_into().expect("four bytes"))
}

/// Append a tail, a footer entry's numbers as their differences from the row before
fn put_tail(out: &mut Vec<u8>, shape: Tail, tail: &[u8], before: &Whole) {
    match shape {
        Tail::Raw(_) => out.extend_from_slice(tail),
        Tail::Entry => {
            out.push(tail[FLAGS_AT]);
            put_varint(out, u64::from(u32_at(tail, LEN_AT)));
            let offset = i64::from(u32_at(tail, OFFSET_AT)) - i64::from(u32_at(before, OFFSET_AT));
            put_varint(out, zigzag(offset));
            let lsn = u64_at(tail, 0).wrapping_sub(u64_at(before, 0)) as i64;
            put_varint(out, zigzag(lsn));
        }
    }
}

/// Read a tail `put_tail` wrote back whole, stepping past it
fn get_tail(bytes: &[u8], at: &mut usize, shape: Tail, before: &Whole) -> Result<Whole> {
    let truncated = || ReelError::Corruption("footer row tail is truncated".to_string());
    let mut tail = [0u8; TAIL_CAP];
    match shape {
        Tail::Raw(len) => {
            let raw = bytes.get(*at..*at + len).ok_or_else(truncated)?;
            tail[..len].copy_from_slice(raw);
            *at += len;
        }
        Tail::Entry => {
            tail[FLAGS_AT] = *bytes.get(*at).ok_or_else(truncated)?;
            *at += 1;
            let len = u32::try_from(get_varint(bytes, at)?).map_err(|_| {
                ReelError::Corruption("a footer row length is past four bytes".to_string())
            })?;
            let offset = i64::from(u32_at(before, OFFSET_AT)) + unzigzag(get_varint(bytes, at)?);
            let offset = u32::try_from(offset).map_err(|_| {
                ReelError::Corruption("a footer row offset is out of range".to_string())
            })?;
            let lsn = u64_at(before, 0).wrapping_add(unzigzag(get_varint(bytes, at)?) as u64);
            tail[..8].copy_from_slice(&lsn.to_le_bytes());
            tail[OFFSET_AT..LEN_AT].copy_from_slice(&offset.to_le_bytes());
            tail[LEN_AT..FLAGS_AT].copy_from_slice(&len.to_le_bytes());
        }
    }
    Ok(tail)
}

/// One row off packed bytes, its tail read against the row before it in its block
fn read_row<'a>(bytes: &'a [u8], at: usize, shape: Tail, before: &Whole) -> Result<Row<'a>> {
    let mut next = at;
    let shared = get_varint(bytes, &mut next)? as usize;
    let suffix_len = get_varint(bytes, &mut next)? as usize;
    let suffix = bytes
        .get(next..next + suffix_len)
        .ok_or_else(|| ReelError::Corruption("footer row suffix is truncated".to_string()))?;
    next += suffix_len;
    let tail = get_tail(bytes, &mut next, shape, before)?;
    Ok(Row {
        shared,
        suffix,
        tail,
        next,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const TAIL: Tail = Tail::Raw(4);

    fn built(keys: &[&[u8]]) -> PrefixRows {
        let mut rows = PrefixRows::new(TAIL);
        for (at, key) in keys.iter().enumerate() {
            rows.push(key, &(at as u32).to_le_bytes()).expect("push");
        }
        rows
    }

    // a seek for a key between two rows lands on the one after it
    #[test]
    fn a_seek_between_rows_lands_after() {
        let keys: Vec<&[u8]> = vec![b"aa", b"cc", b"ee", b"gg"];
        let rows = built(&keys);

        for (asked, want) in [
            (b"a".as_slice(), 0usize),
            (b"ab", 1),
            (b"cc", 1),
            (b"dd", 2),
            (b"ef", 3),
        ] {
            let found = rows.seek(asked).expect("seek").expect("present");
            assert_eq!(
                found.index,
                want,
                "seeking {:?}",
                String::from_utf8_lossy(asked)
            );
        }
        assert!(rows.seek(b"zz").expect("seek").is_none(), "past the end");
    }

    // the cases the incremental comparison is built out of, one at a time
    #[test]
    fn every_comparison_arm_answers_correctly() {
        let keys: Vec<&[u8]> = vec![
            b"aaaa0", b"aaaa1", b"aaaa2", b"aaab0", b"aaac0", b"aab00", b"abbbb", b"b0000",
        ];
        let rows = built(&keys);

        // A model that rebuilds and compares, sharing nothing with the walk
        for asked in [
            b"aaaa0".as_slice(),
            b"aaaa15",
            b"aaab",
            b"aaab0",
            b"aaaz",
            b"aab",
            b"aab001",
            b"abbba",
            b"abbbb",
            b"abbbc",
            b"b",
            b"b0000",
        ] {
            let wanted = keys.iter().position(|key| *key >= asked);
            let found = rows.seek(asked).expect("seek").map(|row| row.index);
            assert_eq!(
                found,
                wanted,
                "seeking {:?}",
                String::from_utf8_lossy(asked)
            );
        }
    }

    // rows out of order are refused rather than encoded badly
    #[test]
    fn an_unsorted_row_is_refused() {
        let mut rows = PrefixRows::new(TAIL);
        rows.push(b"bbb", &[0u8; 4]).expect("first");
        assert!(rows.push(b"aaa", &[0u8; 4]).is_err());
    }

    // a key that is a prefix of the next one packs and seeks correctly
    #[test]
    fn a_prefix_key_sits_before_what_extends_it() {
        let keys: Vec<&[u8]> = vec![b"photos", b"photos/", b"photos/a", b"photosx"];
        let rows = built(&keys);

        assert_eq!(rows.keys().expect("keys"), keys);
        for (at, key) in keys.iter().enumerate() {
            assert_eq!(rows.seek(key).expect("seek").expect("present").index, at);
        }
    }
}

/// A walk over the rows, holding the key it sits on
///
/// A compressed row is a length and a difference, so the key it stands for exists
/// only once something has added it up. Stepping truncates the held key to what
/// the row shares and appends what it does not, so a walk copies the packed suffix
/// bytes rather than the sum of the keys.
pub struct PrefixCursor<'a> {
    /// The rows being walked
    rows: &'a PrefixRows,

    /// Byte the next row starts at
    at: usize,

    /// Row the cursor is about to yield
    index: usize,

    /// The key of the row last yielded, which the next one is measured against
    key: Vec<u8>,

    /// The tail of the row last yielded, whole, which the next one is read against
    tail: Whole,
}

impl<'a> PrefixCursor<'a> {
    /// A cursor at the first row
    pub fn new(rows: &'a PrefixRows) -> PrefixCursor<'a> {
        PrefixCursor {
            rows,
            at: 0,
            index: 0,
            key: Vec::new(),
            tail: [0; TAIL_CAP],
        }
    }

    /// The row the cursor last yielded
    pub fn index(&self) -> usize {
        self.index.saturating_sub(1)
    }

    /// The key the cursor sits on, once it has yielded one
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// The tail of the row the cursor sits on, whole
    pub fn tail(&self) -> &[u8] {
        &self.tail[..self.rows.tail.len()]
    }

    /// Move to the next row, or report that there is not one
    ///
    /// Advance and read rather than yield, since returning the key from the same
    /// call would borrow the cursor for as long as the caller held it.
    pub fn advance(&mut self) -> Result<bool> {
        if self.index >= self.rows.rows {
            return Ok(false);
        }
        // A restart row holds its numbers whole, so it reads against nothing.
        if self.index.is_multiple_of(RESTART_INTERVAL) {
            self.tail = [0; TAIL_CAP];
        }
        let row = self.rows.row_at(self.at, &self.tail)?;
        if row.shared > self.key.len() {
            return Err(ReelError::Corruption(
                "a footer row shares more than the key before it holds".to_string(),
            ));
        }
        self.key.truncate(row.shared);
        self.key.extend_from_slice(row.suffix);
        self.tail = row.tail;
        self.at = row.next;
        self.index += 1;
        Ok(true)
    }
}

#[cfg(test)]
impl PrefixRows {
    /// A walk from the first row
    pub fn cursor(&self) -> PrefixCursor<'_> {
        PrefixCursor::new(self)
    }
}

#[cfg(test)]
mod cursor_tests {
    use super::*;

    const TAIL: Tail = Tail::Raw(4);

    fn paths(count: u32) -> Vec<Vec<u8>> {
        let mut keys: Vec<Vec<u8>> = (0..count)
            .map(|at| {
                format!(
                    "tenants/{:08x}/exports/2026/08/02/part-{at:05}.parquet",
                    at / 64
                )
                .into_bytes()
            })
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }

    fn built(keys: &[Vec<u8>]) -> PrefixRows {
        let mut rows = PrefixRows::new(TAIL);
        for (at, key) in keys.iter().enumerate() {
            rows.push(key, &(at as u32).to_le_bytes()).expect("push");
        }
        rows
    }

    // a walk yields every key in order, with the tail that was stored beside it
    #[test]
    fn a_walk_yields_every_row() {
        let keys = paths(200);
        let rows = built(&keys);

        let mut cursor = rows.cursor();
        let mut seen = 0usize;
        while cursor.advance().expect("step") {
            assert_eq!(cursor.key(), keys[seen].as_slice(), "row {seen}");
            assert_eq!(cursor.tail(), (seen as u32).to_le_bytes(), "tail {seen}");
            assert_eq!(cursor.index(), seen);
            seen += 1;
        }
        assert_eq!(seen, keys.len());
    }

    // the walk copies the difference between rows, not the keys
    #[test]
    fn a_walk_copies_the_difference_not_the_keys() {
        let keys = paths(400);
        let rows = built(&keys);

        let whole: usize = keys.iter().map(Vec::len).sum();
        let mut copied = 0usize;
        let mut cursor = rows.cursor();
        let mut before = 0usize;
        while cursor.advance().expect("step") {
            // What a step appended is the key past what survived the truncate,
            // which is exactly the row's suffix.
            let now = cursor.key().len();
            copied += now.saturating_sub(before.min(now));
            before = now;
        }

        assert!(
            copied * 4 < whole,
            "walk moved {copied} bytes against {whole} of keys, which is not a saving",
        );
    }

    // an empty block walks to nothing rather than erroring
    #[test]
    fn an_empty_block_yields_nothing() {
        let rows = PrefixRows::new(TAIL);
        let mut cursor = rows.cursor();
        assert!(!cursor.advance().expect("step"));
    }
}

/// A block's rows decoded, which is what stepping backwards needs
///
/// The row before is not reachable without adding up everything since the last
/// restart, so a descending walk decodes one restart block at a time.
struct Block {
    /// Each row's key and its tail, whole
    rows: Vec<(Vec<u8>, Whole)>,
}

impl PrefixRows {
    /// Decode one restart block, keys and tails
    fn block(&self, restart: usize) -> Result<Block> {
        let mut at = self.restarts[restart] as usize;
        let end = match self.restarts.get(restart + 1) {
            Some(next) => *next as usize,
            None => self.packed.len(),
        };

        let mut rows = Vec::with_capacity(RESTART_INTERVAL);
        let mut key: Vec<u8> = Vec::new();
        let mut before = [0u8; TAIL_CAP];
        while at < end {
            let row = self.row_at(at, &before)?;
            if row.shared > key.len() {
                return Err(ReelError::Corruption(
                    "a footer row shares more than the key before it holds".to_string(),
                ));
            }
            key.truncate(row.shared);
            key.extend_from_slice(row.suffix);
            rows.push((key.clone(), row.tail));
            before = row.tail;
            at = row.next;
        }
        Ok(Block { rows })
    }

    /// The last row at or before a key, which is where a descending walk starts
    pub fn seek_back(&self, target: &[u8]) -> Result<Option<usize>> {
        if self.rows == 0 {
            return Ok(None);
        }
        // The first row strictly after the target, then step back off it: one
        // search rather than two, and the forward one materialises no key.
        let after = match self.seek(target)? {
            Some(found) if self.key_of(found.index)? == target => found.index + 1,
            Some(found) => found.index,
            None => self.rows,
        };
        Ok(after.checked_sub(1))
    }

    /// One row of its restart block, key and tail
    fn row_of(&self, index: usize) -> Result<(Vec<u8>, Whole)> {
        let restart = index / RESTART_INTERVAL;
        if restart >= self.restarts.len() {
            return Err(ReelError::Corruption(
                "a footer row index is past the rows held".to_string(),
            ));
        }
        self.block(restart)?
            .rows
            .into_iter()
            .nth(index % RESTART_INTERVAL)
            .ok_or_else(|| {
                ReelError::Corruption("a footer row index is past its block".to_string())
            })
    }

    /// The key one row stands for, decoded from its restart block
    ///
    /// Costs the restart interval per call, so a walk holds its key and advances
    /// it instead of asking here per row.
    pub fn key_of(&self, index: usize) -> Result<Vec<u8>> {
        Ok(self.row_of(index)?.0)
    }

    /// The rows sharing the key at an index, as a half-open range
    ///
    /// A segment that overwrote its own record holds both versions under one key,
    /// and a walk takes the newest of the run and steps past the rest.
    pub fn run_at(&self, index: usize) -> Result<Option<(usize, usize)>> {
        if index >= self.rows {
            return Ok(None);
        }
        let key = self.key_of(index)?;

        let mut first = index;
        while first > 0 && self.key_of(first - 1)? == key {
            first -= 1;
        }
        let mut last = index;
        while last + 1 < self.rows && self.key_of(last + 1)? == key {
            last += 1;
        }
        Ok(Some((first, last)))
    }

    /// The tail stored behind one row, whole
    pub fn tail_of(&self, index: usize) -> Result<Vec<u8>> {
        Ok(self.row_of(index)?.1[..self.tail.len()].to_vec())
    }
}

#[cfg(test)]
mod backward_tests {
    use super::*;

    const TAIL: Tail = Tail::Raw(4);

    fn built(keys: &[&[u8]]) -> PrefixRows {
        let mut rows = PrefixRows::new(TAIL);
        for (at, key) in keys.iter().enumerate() {
            rows.push(key, &(at as u32).to_le_bytes()).expect("push");
        }
        rows
    }

    fn paths(count: u32) -> Vec<Vec<u8>> {
        let mut keys: Vec<Vec<u8>> = (0..count)
            .map(|at| {
                format!(
                    "tenants/{:08x}/exports/2026/08/02/part-{at:05}.parquet",
                    at / 64
                )
                .into_bytes()
            })
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }

    // a row's key can be recovered from its index alone
    #[test]
    fn a_row_index_resolves_to_its_key() {
        let keys = paths(200);
        let rows = {
            let mut rows = PrefixRows::new(TAIL);
            for (at, key) in keys.iter().enumerate() {
                rows.push(key, &(at as u32).to_le_bytes()).expect("push");
            }
            rows
        };

        for (at, key) in keys.iter().enumerate() {
            assert_eq!(&rows.key_of(at).expect("key"), key, "row {at}");
            assert_eq!(rows.tail_of(at).expect("tail"), (at as u32).to_le_bytes());
        }
    }

    // a descending seek lands on the last row at or before its key
    #[test]
    fn a_backward_seek_lands_at_or_before() {
        let keys: Vec<&[u8]> = vec![b"aa", b"cc", b"ee", b"gg"];
        let rows = built(&keys);

        for (asked, want) in [
            (b"aa".as_slice(), Some(0usize)),
            (b"ab", Some(0)),
            (b"cc", Some(1)),
            (b"dd", Some(1)),
            (b"gg", Some(3)),
            (b"zz", Some(3)),
        ] {
            assert_eq!(
                rows.seek_back(asked).expect("seek"),
                want,
                "seeking back from {:?}",
                String::from_utf8_lossy(asked),
            );
        }
        assert_eq!(
            rows.seek_back(b"a").expect("seek"),
            None,
            "before every row"
        );
    }

    // repeated keys form a run the walk can take whole
    #[test]
    fn a_repeated_key_is_one_run() {
        let keys: Vec<&[u8]> = vec![b"aa", b"cc", b"cc", b"cc", b"ee"];
        let rows = built(&keys);

        assert_eq!(rows.run_at(0).expect("run"), Some((0, 0)));
        for at in 1..=3 {
            assert_eq!(rows.run_at(at).expect("run"), Some((1, 3)), "from {at}");
        }
        assert_eq!(rows.run_at(4).expect("run"), Some((4, 4)));
        assert_eq!(rows.run_at(5).expect("run"), None);
    }

    // a run spanning a restart boundary is still one run
    #[test]
    fn a_run_crosses_a_restart() {
        let mut keys: Vec<&[u8]> = vec![b"same"; RESTART_INTERVAL + 4];
        keys.push(b"then");
        let rows = built(&keys);

        let span = rows.run_at(RESTART_INTERVAL).expect("run");
        assert_eq!(
            span,
            Some((0, RESTART_INTERVAL + 3)),
            "the run spans the restart"
        );
    }
}

/// Bytes the encoded form spends on its own shape past the rows
pub const TRAILER_LEN: usize = 8;

/// Rebuild one restart block's rows, keys and tails whole behind starts
///
/// The bytes are a cut of the packed rows between two restart offsets. The cut
/// has to open on a restart and end on a row boundary, and both are checked
/// rather than assumed, since the offsets came off a disk this code did not write.
pub fn unpack_block(bytes: &[u8], shape: Tail) -> Result<(Vec<u8>, Vec<u32>)> {
    let mut packed = Vec::with_capacity(bytes.len() * 2);
    let mut starts = vec![0u32];
    let mut key: Vec<u8> = Vec::new();
    let mut before = [0u8; TAIL_CAP];
    let mut at = 0usize;
    let mut index = 0usize;
    while at < bytes.len() {
        // Every restart row holds its numbers whole, so it reads against nothing.
        if index.is_multiple_of(RESTART_INTERVAL) {
            before = [0; TAIL_CAP];
        }
        let row = read_row(bytes, at, shape, &before)?;
        if at == 0 && row.shared != 0 {
            return Err(ReelError::Corruption(
                "a restart row does not carry its whole key".to_string(),
            ));
        }
        if row.shared > key.len() {
            return Err(ReelError::Corruption(
                "a footer row shares more than the key before it holds".to_string(),
            ));
        }
        key.truncate(row.shared);
        key.extend_from_slice(row.suffix);
        packed.extend_from_slice(&key);
        packed.extend_from_slice(&row.tail[..shape.len()]);
        starts.push(packed.len() as u32);
        before = row.tail;
        at = row.next;
        index += 1;
    }
    Ok((packed, starts))
}

impl PrefixRows {
    /// Bytes this block takes on disk
    pub fn encoded_len(&self) -> usize {
        self.packed.len() + self.restarts.len() * 4 + TRAILER_LEN
    }

    /// Write the block out: the rows, then the restarts, then their count
    ///
    /// The restarts trail the rows because a writer learns them as it goes, and
    /// the trailer at the very end is what lets a reader find them backwards.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.packed);
        for restart in &self.restarts {
            out.extend_from_slice(&restart.to_le_bytes());
        }
        out.extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.rows as u32).to_le_bytes());
    }

    /// The same bytes `encode` writes, grown onto the rows' own buffer with no copy
    pub fn into_encoded(self) -> Vec<u8> {
        let PrefixRows {
            mut packed,
            restarts,
            rows,
            ..
        } = self;
        packed.reserve(restarts.len() * 4 + TRAILER_LEN);
        for restart in &restarts {
            packed.extend_from_slice(&restart.to_le_bytes());
        }
        packed.extend_from_slice(&(restarts.len() as u32).to_le_bytes());
        packed.extend_from_slice(&(rows as u32).to_le_bytes());
        packed
    }

    /// Read a block back, checking that it describes itself consistently
    ///
    /// Everything a reader trusts comes off the bytes, so everything is checked:
    /// the trailer, the restart count, every offset, and the row count.
    pub fn decode(bytes: &[u8], tail: Tail) -> Result<PrefixRows> {
        let frame = Frame::read(bytes)?;
        let restarts = (0..frame.restarts)
            .map(|at| frame.restart(bytes, at))
            .collect();
        Ok(PrefixRows {
            packed: bytes[..frame.rows_end].to_vec(),
            restarts,
            rows: frame.rows,
            // Only an append measures against the last key and tail, and a decoded
            // block is not appended to.
            last: Vec::new(),
            last_tail: [0; TAIL_CAP],
            tail,
        })
    }
}

/// Where an encoded block's rows end and how many it says it holds, checked
struct Frame {
    /// Bytes of rows, which is where the restarts begin
    rows_end: usize,

    /// Restarts the trailer counts
    restarts: usize,

    /// Rows the trailer counts
    rows: usize,
}

impl Frame {
    /// Check the trailer and every restart offset against the bytes they sit in
    fn read(bytes: &[u8]) -> Result<Frame> {
        let trailer_at = bytes.len().checked_sub(TRAILER_LEN).ok_or_else(|| {
            ReelError::Corruption("a packed row block has no trailer".to_string())
        })?;
        let restarts =
            u32::from_le_bytes(bytes[trailer_at..trailer_at + 4].try_into().unwrap()) as usize;
        let rows = u32::from_le_bytes(bytes[trailer_at + 4..][..4].try_into().unwrap()) as usize;

        let rows_end = trailer_at.checked_sub(restarts * 4).ok_or_else(|| {
            ReelError::Corruption(
                "a packed row block claims more restarts than it holds".to_string(),
            )
        })?;
        // The rows and the restarts have to agree, or a search bisects an array
        // that does not describe the rows it is searching.
        if restarts != rows.div_ceil(RESTART_INTERVAL) {
            return Err(ReelError::Corruption(format!(
                "a packed row block holds {rows} rows under {restarts} restarts",
            )));
        }
        let frame = Frame {
            rows_end,
            restarts,
            rows,
        };
        for at in 0..restarts {
            if frame.restart(bytes, at) as usize > rows_end {
                return Err(ReelError::Corruption(
                    "a packed row restart points past the rows".to_string(),
                ));
            }
        }
        Ok(frame)
    }

    /// Where one restart row begins
    fn restart(&self, bytes: &[u8], at: usize) -> u32 {
        let from = self.rows_end + at * 4;
        u32::from_le_bytes(bytes[from..from + 4].try_into().unwrap())
    }
}

/// Rebuild an encoded block's rows whole, read where they lie
///
/// What `decode` and `unpacked` do together, with no copy of the rows first. Rows of
/// one width (`row_len` bytes each, key and tail) come back strided with no starts, in a
/// buffer sized exactly. Each restart offset has to land on the row it says it opens.
pub fn unpack(
    bytes: &[u8],
    tail: Tail,
    row_len: Option<usize>,
) -> Result<(Vec<u8>, Vec<u32>, usize)> {
    let frame = Frame::read(bytes)?;
    let rows_bytes = &bytes[..frame.rows_end];
    let mut packed = Vec::with_capacity(match row_len {
        Some(len) => len * frame.rows,
        None => rows_bytes.len() * 2,
    });
    let mut starts = match row_len {
        Some(_) => Vec::new(),
        None => {
            let mut starts = Vec::with_capacity(frame.rows + 1);
            starts.push(0u32);
            starts
        }
    };
    let mut key: Vec<u8> = Vec::new();
    let mut before = [0u8; TAIL_CAP];
    let mut at = 0usize;
    for index in 0..frame.rows {
        if index.is_multiple_of(RESTART_INTERVAL) {
            if frame.restart(bytes, index / RESTART_INTERVAL) as usize != at {
                return Err(ReelError::Corruption(
                    "a packed row restart does not open the row it counts".to_string(),
                ));
            }
            before = [0; TAIL_CAP];
        }
        let row = read_row(rows_bytes, at, tail, &before)?;
        if row.shared > key.len() || (index.is_multiple_of(RESTART_INTERVAL) && row.shared != 0) {
            return Err(ReelError::Corruption(
                "a footer row shares more than the key before it holds".to_string(),
            ));
        }
        key.truncate(row.shared);
        key.extend_from_slice(row.suffix);
        packed.extend_from_slice(&key);
        packed.extend_from_slice(&row.tail[..tail.len()]);
        if row_len.is_some_and(|len| packed.len() != (index + 1) * len) {
            return Err(ReelError::Corruption(
                "a packed row is not the width its partition strides by".to_string(),
            ));
        }
        if row_len.is_none() {
            starts.push(packed.len() as u32);
        }
        before = row.tail;
        at = row.next;
    }
    if at != frame.rows_end {
        return Err(ReelError::Corruption(
            "a packed row block holds bytes past its last row".to_string(),
        ));
    }
    Ok((packed, starts, frame.rows))
}

#[cfg(test)]
mod block_tests {
    use super::*;

    const TAIL: Tail = Tail::Raw(4);

    // every restart cut unpacks to exactly the keys the whole block holds
    #[test]
    fn a_cut_at_every_restart_unpacks_its_rows() {
        let mut keys: Vec<Vec<u8>> = (0..200u32)
            .map(|at| {
                format!(
                    "tenants/{:08x}/exports/2026/08/02/part-{at:05}.parquet",
                    at / 64
                )
                .into_bytes()
            })
            .collect();
        keys.sort();
        keys.dedup();
        let mut rows = PrefixRows::new(TAIL);
        for (at, key) in keys.iter().enumerate() {
            rows.push(key, &(at as u32).to_le_bytes()).expect("push");
        }

        let mut rebuilt = Vec::new();
        for block in 0..rows.restarts() {
            let start = rows.restarts[block] as usize;
            let end = match rows.restarts.get(block + 1) {
                Some(next) => *next as usize,
                None => rows.packed.len(),
            };
            let (packed, starts) = unpack_block(&rows.packed[start..end], TAIL).expect("unpack");
            for row in 0..starts.len() - 1 {
                let from = starts[row] as usize;
                let to = starts[row + 1] as usize - TAIL.len();
                rebuilt.push(packed[from..to].to_vec());
            }
        }
        assert_eq!(rebuilt, keys);
    }

    // a cut that opens mid-block is refused rather than misread
    #[test]
    fn a_cut_off_a_restart_is_refused() {
        let mut rows = PrefixRows::new(TAIL);
        rows.push(b"tenants/aa", &[0u8; 4]).expect("push");
        rows.push(b"tenants/ab", &[0u8; 4]).expect("push");

        let second = 2 + b"tenants/aa".len() + TAIL.len();
        assert!(unpack_block(&rows.packed[second..], TAIL).is_err());
    }
}

#[cfg(test)]
mod encoding_tests {
    use super::*;

    const TAIL: Tail = Tail::Raw(4);

    fn paths(count: u32) -> Vec<Vec<u8>> {
        let mut keys: Vec<Vec<u8>> = (0..count)
            .map(|at| {
                format!(
                    "tenants/{:08x}/exports/2026/08/02/part-{at:05}.parquet",
                    at / 64
                )
                .into_bytes()
            })
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }

    fn built(keys: &[Vec<u8>]) -> PrefixRows {
        let mut rows = PrefixRows::new(TAIL);
        for (at, key) in keys.iter().enumerate() {
            rows.push(key, &(at as u32).to_le_bytes()).expect("push");
        }
        rows
    }

    // a block written out and read back answers everything the original did
    #[test]
    fn a_block_survives_a_round_trip() {
        let keys = paths(200);
        let rows = built(&keys);

        let mut out = Vec::new();
        rows.encode(&mut out);
        assert_eq!(out.len(), rows.encoded_len());

        let back = PrefixRows::decode(&out, TAIL).expect("decode");
        assert_eq!(back.len(), rows.len());
        assert_eq!(back.restarts(), rows.restarts());
        assert_eq!(back.keys().expect("keys"), keys);

        for (at, key) in keys.iter().enumerate() {
            let found = back.seek(key).expect("seek").expect("present");
            assert_eq!(found.index, at);
            assert_eq!(found.tail, (at as u32).to_le_bytes());
        }
    }

    // an empty block round trips to an empty block
    #[test]
    fn an_empty_block_survives() {
        let rows = PrefixRows::new(TAIL);
        let mut out = Vec::new();
        rows.encode(&mut out);

        let back = PrefixRows::decode(&out, TAIL).expect("decode");
        assert!(back.is_empty());
        assert!(back.seek(b"anything").expect("seek").is_none());
    }

    // bytes that do not describe a block are refused rather than walked
    #[test]
    fn a_damaged_block_is_refused() {
        let keys = paths(64);
        let rows = built(&keys);
        let mut good = Vec::new();
        rows.encode(&mut good);

        assert!(PrefixRows::decode(&[], TAIL).is_err(), "no trailer at all");
        assert!(
            PrefixRows::decode(&good[..4], TAIL).is_err(),
            "truncated to nothing"
        );

        // A restart count larger than the block can hold
        let mut wrong = good.clone();
        let at = wrong.len() - TRAILER_LEN;
        wrong[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(
            PrefixRows::decode(&wrong, TAIL).is_err(),
            "impossible restart count"
        );

        // A row count the restarts cannot account for
        let mut mismatched = good.clone();
        let at = mismatched.len() - 4;
        mismatched[at..].copy_from_slice(&9999u32.to_le_bytes());
        assert!(
            PrefixRows::decode(&mismatched, TAIL).is_err(),
            "rows disagree with restarts"
        );
    }

    // the encoded block is smaller than the keys it stands for
    #[test]
    fn the_encoded_block_is_smaller_than_its_keys() {
        let keys = paths(400);
        let rows = built(&keys);

        let whole: usize = keys.iter().map(|key| key.len() + TAIL.len()).sum();
        assert!(
            rows.encoded_len() * 2 < whole,
            "encoded {} against {whole} whole",
            rows.encoded_len(),
        );
    }
}
