//! Footer rows that carry what a key does not share with the row before it
//!
//! A footer holds a column's keys sorted, and sorted keys share their fronts, so a
//! row stores how much it shares with the row before it and only the rest. That
//! costs the arithmetic a flat run of fixed-width rows is binary searched by, so
//! every so many rows a restart carries its key whole; the restarts are a sorted
//! array to bisect, and a search lands in one small block and walks it.

use std::cmp::Ordering;

use crate::error::{ReelError, Result};

/// Rows between restart points
///
/// Fewer restarts saves bytes and makes the walk after a seek longer.
pub const RESTART_INTERVAL: usize = 16;

/// Bytes a row spends saying how much it shares and how much it carries
const ROW_HEADER_LEN: usize = 4;

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
}

impl PrefixRows {
    pub fn new() -> PrefixRows {
        PrefixRows::default()
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

        let restart = self.rows.is_multiple_of(RESTART_INTERVAL);
        if restart {
            self.restarts.push(self.packed.len() as u32);
        }
        let shared = match restart {
            // A restart carries its key whole, so the array of them is searchable
            // without walking anything.
            true => 0,
            false => shared_prefix(&self.last, key),
        };

        let suffix = &key[shared..];
        self.packed
            .extend_from_slice(&(shared as u16).to_le_bytes());
        self.packed
            .extend_from_slice(&(suffix.len() as u16).to_le_bytes());
        self.packed.extend_from_slice(suffix);
        self.packed.extend_from_slice(tail);

        self.last.clear();
        self.last.extend_from_slice(key);
        self.rows += 1;
        Ok(())
    }

    /// One row's shared length, its suffix, and the bytes behind it
    fn row_at(&self, at: usize, tail_len: usize) -> Result<(usize, &[u8], &[u8])> {
        let header = self
            .packed
            .get(at..at + ROW_HEADER_LEN)
            .ok_or_else(|| ReelError::Corruption("footer row header is truncated".to_string()))?;
        let shared = u16::from_le_bytes([header[0], header[1]]) as usize;
        let suffix_len = u16::from_le_bytes([header[2], header[3]]) as usize;

        let from = at + ROW_HEADER_LEN;
        let suffix = self
            .packed
            .get(from..from + suffix_len)
            .ok_or_else(|| ReelError::Corruption("footer row suffix is truncated".to_string()))?;
        let tail = self
            .packed
            .get(from + suffix_len..from + suffix_len + tail_len)
            .ok_or_else(|| ReelError::Corruption("footer row tail is truncated".to_string()))?;
        Ok((shared, suffix, tail))
    }

    /// The whole key of a restart row, which is the only key stored outright
    fn restart_key(&self, restart: usize, tail_len: usize) -> Result<&[u8]> {
        let at = self.restarts[restart] as usize;
        let (shared, suffix, _) = self.row_at(at, tail_len)?;
        match shared {
            0 => Ok(suffix),
            _ => Err(ReelError::Corruption(
                "a restart row does not carry its whole key".to_string(),
            )),
        }
    }

    /// The first row at or after a key, and the bytes behind it
    ///
    /// Bisects the restarts, then walks the one block that can hold the answer,
    /// comparing against the packed form rather than against rebuilt keys.
    pub fn seek(&self, target: &[u8], tail_len: usize) -> Result<Option<Found>> {
        if self.rows == 0 {
            return Ok(None);
        }

        // The last restart at or below the target, whose block is the only one
        // that can hold the first row at or after it.
        let mut low = 0usize;
        let mut high = self.restarts.len();
        while low < high {
            let mid = (low + high) / 2;
            match self.restart_key(mid, tail_len)?.cmp(target) {
                Ordering::Less => low = mid + 1,
                _ => high = mid,
            }
        }
        let block = low.saturating_sub(usize::from(low > 0 || low == self.restarts.len()));
        self.walk(block, target, tail_len)
    }

    /// Walk one restart block for the first row at or after the target
    ///
    /// No case rebuilds a key: what the walk carries is how far the target matched
    /// the previous row, and a row's shared length against that decides it.
    fn walk(&self, block: usize, target: &[u8], tail_len: usize) -> Result<Option<Found>> {
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

        while at < end {
            let (shared, suffix, tail) = self.row_at(at, tail_len)?;
            let cmp = match shared.cmp(&matched) {
                // The row agrees with the previous key past where the target
                // stopped agreeing, so the previous comparison still decides.
                Ordering::Greater => order,
                // The row leaves the previous key first, so one byte decides.
                Ordering::Less => match suffix.first() {
                    Some(byte) => target[shared].cmp(byte),
                    None => Ordering::Greater,
                },
                // They diverge together, so the remainder decides it.
                Ordering::Equal => target[matched..].cmp(suffix),
            };

            if cmp != Ordering::Greater {
                return Ok(Some(Found {
                    index,
                    tail: tail.to_vec(),
                }));
            }

            // Carry forward how far the target agrees with the row just passed,
            // which for a row the target sorts above is the whole of the row.
            matched = match shared.cmp(&matched) {
                Ordering::Equal => shared + shared_prefix(&target[matched..], suffix),
                Ordering::Less => shared,
                Ordering::Greater => matched,
            };
            order = cmp;
            at += ROW_HEADER_LEN + suffix.len() + tail_len;
            index += 1;
        }

        // Past the end of this block, so the answer is the next block's first row
        // when there is one.
        match self.restarts.get(block + 1) {
            Some(_) => self.walk(block + 1, target, tail_len),
            None => Ok(None),
        }
    }

    /// Every key the block holds, rebuilt, which only a filter build asks for
    pub fn keys(&self, tail_len: usize) -> Result<Vec<Vec<u8>>> {
        let mut out = Vec::with_capacity(self.rows);
        let mut key: Vec<u8> = Vec::new();
        let mut at = 0usize;
        for _ in 0..self.rows {
            let (shared, suffix, _) = self.row_at(at, tail_len)?;
            key.truncate(shared);
            key.extend_from_slice(suffix);
            out.push(key.clone());
            at += ROW_HEADER_LEN + suffix.len() + tail_len;
        }
        Ok(out)
    }
}

impl PrefixRows {
    /// The rows rebuilt whole, keys and tails packed back to back behind starts
    ///
    /// The shape every in-memory reader searches, so the prefix form stays an
    /// on-disk encoding only.
    pub fn unpacked(&self, tail_len: usize) -> Result<(Vec<u8>, Vec<u32>)> {
        // Prefix sharing only removes bytes, so the encoded length is a floor for
        // the rebuilt one and seeding with it skips the early doublings.
        let mut packed = Vec::with_capacity(self.packed.len());
        let mut starts = Vec::with_capacity(self.rows + 1);
        starts.push(0u32);
        let mut cursor = PrefixCursor::new(self, tail_len);
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

#[cfg(test)]
mod tests {
    use super::*;

    const TAIL: usize = 4;

    fn built(keys: &[&[u8]]) -> PrefixRows {
        let mut rows = PrefixRows::new();
        for (at, key) in keys.iter().enumerate() {
            rows.push(key, &(at as u32).to_le_bytes()).expect("push");
        }
        rows
    }

    fn paths() -> Vec<Vec<u8>> {
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
        keys
    }

    // every key comes back out of the block it was packed into
    #[test]
    fn keys_survive_the_packing() {
        let keys = paths();
        let mut rows = PrefixRows::new();
        for (at, key) in keys.iter().enumerate() {
            rows.push(key, &(at as u32).to_le_bytes()).expect("push");
        }

        assert_eq!(rows.len(), keys.len());
        assert_eq!(rows.keys(TAIL).expect("keys"), keys);
    }

    // sharing a front is what the encoding is for, so it has to actually save
    #[test]
    fn a_shared_front_is_stored_once() {
        let keys = paths();
        let mut rows = PrefixRows::new();
        for key in &keys {
            rows.push(key, &[0u8; TAIL]).expect("push");
        }

        let whole: usize = keys.iter().map(|key| key.len() + TAIL).sum();
        let packed = rows.packed_len();
        assert!(
            packed * 2 < whole,
            "packed {packed} against {whole} whole, which is not a saving",
        );
    }

    // a seek finds the first row at or after the key it asked for
    #[test]
    fn a_seek_lands_on_the_first_row_at_or_after() {
        let keys = paths();
        let rows = built(&keys.iter().map(Vec::as_slice).collect::<Vec<_>>());

        for (at, key) in keys.iter().enumerate() {
            let found = rows.seek(key, TAIL).expect("seek").expect("present");
            assert_eq!(found.index, at, "seeking a key it holds");
        }
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
            let found = rows.seek(asked, TAIL).expect("seek").expect("present");
            assert_eq!(
                found.index,
                want,
                "seeking {:?}",
                String::from_utf8_lossy(asked)
            );
        }
        assert!(
            rows.seek(b"zz", TAIL).expect("seek").is_none(),
            "past the end"
        );
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
            let found = rows.seek(asked, TAIL).expect("seek").map(|row| row.index);
            assert_eq!(
                found,
                wanted,
                "seeking {:?}",
                String::from_utf8_lossy(asked)
            );
        }
    }

    // a block spanning many restarts is searched, not scanned
    #[test]
    fn restarts_carve_the_block_up() {
        let keys = paths();
        let rows = built(&keys.iter().map(Vec::as_slice).collect::<Vec<_>>());

        let wanted = keys.len().div_ceil(RESTART_INTERVAL);
        assert_eq!(rows.restarts(), wanted);
    }

    // rows out of order are refused rather than encoded badly
    #[test]
    fn an_unsorted_row_is_refused() {
        let mut rows = PrefixRows::new();
        rows.push(b"bbb", &[0u8; TAIL]).expect("first");
        assert!(rows.push(b"aaa", &[0u8; TAIL]).is_err());
    }

    // a key that is a prefix of the next one packs and seeks correctly
    #[test]
    fn a_prefix_key_sits_before_what_extends_it() {
        let keys: Vec<&[u8]> = vec![b"photos", b"photos/", b"photos/a", b"photosx"];
        let rows = built(&keys);

        assert_eq!(rows.keys(TAIL).expect("keys"), keys);
        for (at, key) in keys.iter().enumerate() {
            assert_eq!(
                rows.seek(key, TAIL).expect("seek").expect("present").index,
                at
            );
        }
    }

    // the tail rides with its key and comes back with it
    #[test]
    fn a_row_carries_its_tail() {
        let keys = paths();
        let rows = built(&keys.iter().map(Vec::as_slice).collect::<Vec<_>>());

        for (at, key) in keys.iter().enumerate() {
            let found = rows.seek(key, TAIL).expect("seek").expect("present");
            assert_eq!(found.tail, (at as u32).to_le_bytes(), "row {at}");
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

    /// Bytes every row carries behind its key
    tail_len: usize,

    /// Byte the next row starts at
    at: usize,

    /// Row the cursor is about to yield
    index: usize,

    /// The key of the row last yielded, which the next one is measured against
    key: Vec<u8>,

    /// Where the last yielded row's tail sits, so reading it borrows in place
    tail_at: usize,
}

impl<'a> PrefixCursor<'a> {
    /// A cursor at the first row
    pub fn new(rows: &'a PrefixRows, tail_len: usize) -> PrefixCursor<'a> {
        PrefixCursor {
            rows,
            tail_len,
            at: 0,
            index: 0,
            key: Vec::new(),
            tail_at: 0,
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

    /// The tail of the row the cursor sits on, borrowed in place since a tail is
    /// stored contiguously
    pub fn tail(&self) -> &[u8] {
        &self.rows.packed[self.tail_at..self.tail_at + self.tail_len]
    }

    /// Move to the next row, or report that there is not one
    ///
    /// Advance and read rather than yield, since returning the key from the same
    /// call would borrow the cursor for as long as the caller held it.
    pub fn advance(&mut self) -> Result<bool> {
        if self.index >= self.rows.rows {
            return Ok(false);
        }
        let (shared, suffix, _) = self.rows.row_at(self.at, self.tail_len)?;
        if shared > self.key.len() {
            return Err(ReelError::Corruption(
                "a footer row shares more than the key before it holds".to_string(),
            ));
        }
        self.key.truncate(shared);
        self.key.extend_from_slice(suffix);

        self.tail_at = self.at + ROW_HEADER_LEN + suffix.len();
        self.at = self.tail_at + self.tail_len;
        self.index += 1;
        Ok(true)
    }
}

impl PrefixRows {
    /// A walk from the first row
    pub fn cursor(&self, tail_len: usize) -> PrefixCursor<'_> {
        PrefixCursor::new(self, tail_len)
    }
}

#[cfg(test)]
mod cursor_tests {
    use super::*;

    const TAIL: usize = 4;

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
        let mut rows = PrefixRows::new();
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

        let mut cursor = rows.cursor(TAIL);
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
        let mut cursor = rows.cursor(TAIL);
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
        let rows = PrefixRows::new();
        let mut cursor = rows.cursor(TAIL);
        assert!(!cursor.advance().expect("step"));
    }

    // a walk crosses restart boundaries without losing its place
    #[test]
    fn a_walk_crosses_restarts() {
        let keys = paths(200);
        let rows = built(&keys);
        assert!(
            rows.restarts() > 4,
            "the corpus has to span several restarts"
        );

        let mut cursor = rows.cursor(TAIL);
        let mut walked: Vec<Vec<u8>> = Vec::new();
        while cursor.advance().expect("step") {
            walked.push(cursor.key().to_vec());
        }
        assert_eq!(walked, keys);
    }
}

/// A block's rows decoded, which is what stepping backwards needs
///
/// The row before is not reachable without adding up everything since the last
/// restart, so a descending walk decodes one restart block at a time.
struct Block {
    /// Each row's key and where its tail sits in the packed bytes
    rows: Vec<(Vec<u8>, usize)>,
}

impl PrefixRows {
    /// Decode one restart block, keys and tail positions
    fn block(&self, restart: usize, tail_len: usize) -> Result<Block> {
        let mut at = self.restarts[restart] as usize;
        let end = match self.restarts.get(restart + 1) {
            Some(next) => *next as usize,
            None => self.packed.len(),
        };

        let mut rows = Vec::with_capacity(RESTART_INTERVAL);
        let mut key: Vec<u8> = Vec::new();
        while at < end {
            let (shared, suffix, _) = self.row_at(at, tail_len)?;
            if shared > key.len() {
                return Err(ReelError::Corruption(
                    "a footer row shares more than the key before it holds".to_string(),
                ));
            }
            key.truncate(shared);
            key.extend_from_slice(suffix);
            let tail_at = at + ROW_HEADER_LEN + suffix.len();
            rows.push((key.clone(), tail_at));
            at = tail_at + tail_len;
        }
        Ok(Block { rows })
    }

    /// The last row at or before a key, which is where a descending walk starts
    pub fn seek_back(&self, target: &[u8], tail_len: usize) -> Result<Option<usize>> {
        if self.rows == 0 {
            return Ok(None);
        }
        // The first row strictly after the target, then step back off it: one
        // search rather than two, and the forward one materialises no key.
        let after = match self.seek(target, tail_len)? {
            Some(found) if self.key_of(found.index, tail_len)? == target => found.index + 1,
            Some(found) => found.index,
            None => self.rows,
        };
        Ok(after.checked_sub(1))
    }

    /// The key one row stands for, decoded from its restart block
    ///
    /// Costs the restart interval per call, so a walk holds its key and advances
    /// it instead of asking here per row.
    pub fn key_of(&self, index: usize, tail_len: usize) -> Result<Vec<u8>> {
        let restart = index / RESTART_INTERVAL;
        if restart >= self.restarts.len() {
            return Err(ReelError::Corruption(
                "a footer row index is past the rows held".to_string(),
            ));
        }
        let block = self.block(restart, tail_len)?;
        block
            .rows
            .get(index % RESTART_INTERVAL)
            .map(|(key, _)| key.clone())
            .ok_or_else(|| {
                ReelError::Corruption("a footer row index is past its block".to_string())
            })
    }

    /// The rows sharing the key at an index, as a half-open range
    ///
    /// A segment that overwrote its own record holds both versions under one key,
    /// and a walk takes the newest of the run and steps past the rest.
    pub fn run_at(&self, index: usize, tail_len: usize) -> Result<Option<(usize, usize)>> {
        if index >= self.rows {
            return Ok(None);
        }
        let key = self.key_of(index, tail_len)?;

        let mut first = index;
        while first > 0 && self.key_of(first - 1, tail_len)? == key {
            first -= 1;
        }
        let mut last = index;
        while last + 1 < self.rows && self.key_of(last + 1, tail_len)? == key {
            last += 1;
        }
        Ok(Some((first, last)))
    }

    /// The tail stored behind one row
    pub fn tail_of(&self, index: usize, tail_len: usize) -> Result<Vec<u8>> {
        let restart = index / RESTART_INTERVAL;
        let block = self.block(restart, tail_len)?;
        let (_, at) = block.rows.get(index % RESTART_INTERVAL).ok_or_else(|| {
            ReelError::Corruption("a footer row index is past its block".to_string())
        })?;
        Ok(self.packed[*at..*at + tail_len].to_vec())
    }
}

#[cfg(test)]
mod backward_tests {
    use super::*;

    const TAIL: usize = 4;

    fn built(keys: &[&[u8]]) -> PrefixRows {
        let mut rows = PrefixRows::new();
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
            let mut rows = PrefixRows::new();
            for (at, key) in keys.iter().enumerate() {
                rows.push(key, &(at as u32).to_le_bytes()).expect("push");
            }
            rows
        };

        for (at, key) in keys.iter().enumerate() {
            assert_eq!(&rows.key_of(at, TAIL).expect("key"), key, "row {at}");
            assert_eq!(
                rows.tail_of(at, TAIL).expect("tail"),
                (at as u32).to_le_bytes()
            );
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
                rows.seek_back(asked, TAIL).expect("seek"),
                want,
                "seeking back from {:?}",
                String::from_utf8_lossy(asked),
            );
        }
        assert_eq!(
            rows.seek_back(b"a", TAIL).expect("seek"),
            None,
            "before every row"
        );
    }

    // repeated keys form a run the walk can take whole
    #[test]
    fn a_repeated_key_is_one_run() {
        let keys: Vec<&[u8]> = vec![b"aa", b"cc", b"cc", b"cc", b"ee"];
        let rows = built(&keys);

        assert_eq!(rows.run_at(0, TAIL).expect("run"), Some((0, 0)));
        for at in 1..=3 {
            assert_eq!(
                rows.run_at(at, TAIL).expect("run"),
                Some((1, 3)),
                "from {at}"
            );
        }
        assert_eq!(rows.run_at(4, TAIL).expect("run"), Some((4, 4)));
        assert_eq!(rows.run_at(5, TAIL).expect("run"), None);
    }

    // a run spanning a restart boundary is still one run
    #[test]
    fn a_run_crosses_a_restart() {
        let mut keys: Vec<&[u8]> = vec![b"same"; RESTART_INTERVAL + 4];
        keys.push(b"then");
        let rows = built(&keys);

        let span = rows.run_at(RESTART_INTERVAL, TAIL).expect("run");
        assert_eq!(
            span,
            Some((0, RESTART_INTERVAL + 3)),
            "the run spans the restart"
        );
    }

    // walking backwards over the whole partition yields it reversed
    #[test]
    fn a_backward_walk_reverses_the_order() {
        let keys = paths(120);
        let rows = {
            let mut rows = PrefixRows::new();
            for (at, key) in keys.iter().enumerate() {
                rows.push(key, &(at as u32).to_le_bytes()).expect("push");
            }
            rows
        };

        let mut walked: Vec<Vec<u8>> = Vec::new();
        let mut at = rows.len();
        while let Some(index) = at.checked_sub(1) {
            walked.push(rows.key_of(index, TAIL).expect("key"));
            at = index;
        }
        walked.reverse();
        assert_eq!(walked, keys);
    }
}

/// Bytes the encoded form spends on its own shape past the rows
pub const TRAILER_LEN: usize = 8;

/// Rebuild one restart block's rows, keys and tails whole behind starts
///
/// The bytes are a cut of the packed rows between two restart offsets. The cut
/// has to open on a restart and end on a row boundary, and both are checked
/// rather than assumed, since the offsets came off a disk this code did not write.
pub fn unpack_block(bytes: &[u8], tail_len: usize) -> Result<(Vec<u8>, Vec<u32>)> {
    let mut packed = Vec::with_capacity(bytes.len() * 2);
    let mut starts = vec![0u32];
    let mut key: Vec<u8> = Vec::new();
    let mut at = 0usize;
    while at < bytes.len() {
        let header = bytes
            .get(at..at + ROW_HEADER_LEN)
            .ok_or_else(|| ReelError::Corruption("footer row header is truncated".to_string()))?;
        let shared = u16::from_le_bytes([header[0], header[1]]) as usize;
        let suffix_len = u16::from_le_bytes([header[2], header[3]]) as usize;
        if at == 0 && shared != 0 {
            return Err(ReelError::Corruption(
                "a restart row does not carry its whole key".to_string(),
            ));
        }
        if shared > key.len() {
            return Err(ReelError::Corruption(
                "a footer row shares more than the key before it holds".to_string(),
            ));
        }
        let from = at + ROW_HEADER_LEN;
        let suffix = bytes
            .get(from..from + suffix_len)
            .ok_or_else(|| ReelError::Corruption("footer row suffix is truncated".to_string()))?;
        let tail = bytes
            .get(from + suffix_len..from + suffix_len + tail_len)
            .ok_or_else(|| ReelError::Corruption("footer row tail is truncated".to_string()))?;
        key.truncate(shared);
        key.extend_from_slice(suffix);
        packed.extend_from_slice(&key);
        packed.extend_from_slice(tail);
        starts.push(packed.len() as u32);
        at = from + suffix_len + tail_len;
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

    /// Read a block back, checking that it describes itself consistently
    ///
    /// Everything a reader trusts comes off the bytes, so everything is checked:
    /// the trailer, the restart count, every offset, and the row count.
    pub fn decode(bytes: &[u8]) -> Result<PrefixRows> {
        let trailer_at = bytes.len().checked_sub(TRAILER_LEN).ok_or_else(|| {
            ReelError::Corruption("a packed row block has no trailer".to_string())
        })?;
        let restart_count =
            u32::from_le_bytes(bytes[trailer_at..trailer_at + 4].try_into().unwrap()) as usize;
        let rows = u32::from_le_bytes(bytes[trailer_at + 4..][..4].try_into().unwrap()) as usize;

        let restarts_at = trailer_at.checked_sub(restart_count * 4).ok_or_else(|| {
            ReelError::Corruption(
                "a packed row block claims more restarts than it holds".to_string(),
            )
        })?;

        let mut restarts = Vec::with_capacity(restart_count);
        for at in 0..restart_count {
            let from = restarts_at + at * 4;
            let offset = u32::from_le_bytes(bytes[from..from + 4].try_into().unwrap());
            if offset as usize > restarts_at {
                return Err(ReelError::Corruption(
                    "a packed row restart points past the rows".to_string(),
                ));
            }
            restarts.push(offset);
        }

        // The rows and the restarts have to agree, or a search bisects an array
        // that does not describe the rows it is searching.
        if restart_count != rows.div_ceil(RESTART_INTERVAL) {
            return Err(ReelError::Corruption(format!(
                "a packed row block holds {rows} rows under {restart_count} restarts",
            )));
        }

        Ok(PrefixRows {
            packed: bytes[..restarts_at].to_vec(),
            restarts,
            rows,
            // Only an append measures against the last key, and a decoded block
            // is not appended to.
            last: Vec::new(),
        })
    }
}

#[cfg(test)]
mod block_tests {
    use super::*;

    const TAIL: usize = 4;

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
        let mut rows = PrefixRows::new();
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
                let to = starts[row + 1] as usize - TAIL;
                rebuilt.push(packed[from..to].to_vec());
            }
        }
        assert_eq!(rebuilt, keys);
    }

    // a cut that opens mid-block is refused rather than misread
    #[test]
    fn a_cut_off_a_restart_is_refused() {
        let mut rows = PrefixRows::new();
        rows.push(b"tenants/aa", &[0u8; TAIL]).expect("push");
        rows.push(b"tenants/ab", &[0u8; TAIL]).expect("push");

        let second = ROW_HEADER_LEN + b"tenants/aa".len() + TAIL;
        assert!(unpack_block(&rows.packed[second..], TAIL).is_err());
    }
}

#[cfg(test)]
mod encoding_tests {
    use super::*;

    const TAIL: usize = 4;

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
        let mut rows = PrefixRows::new();
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

        let back = PrefixRows::decode(&out).expect("decode");
        assert_eq!(back.len(), rows.len());
        assert_eq!(back.restarts(), rows.restarts());
        assert_eq!(back.keys(TAIL).expect("keys"), keys);

        for (at, key) in keys.iter().enumerate() {
            let found = back.seek(key, TAIL).expect("seek").expect("present");
            assert_eq!(found.index, at);
            assert_eq!(found.tail, (at as u32).to_le_bytes());
        }
    }

    // an empty block round trips to an empty block
    #[test]
    fn an_empty_block_survives() {
        let rows = PrefixRows::new();
        let mut out = Vec::new();
        rows.encode(&mut out);

        let back = PrefixRows::decode(&out).expect("decode");
        assert!(back.is_empty());
        assert!(back.seek(b"anything", TAIL).expect("seek").is_none());
    }

    // bytes that do not describe a block are refused rather than walked
    #[test]
    fn a_damaged_block_is_refused() {
        let keys = paths(64);
        let rows = built(&keys);
        let mut good = Vec::new();
        rows.encode(&mut good);

        assert!(PrefixRows::decode(&[]).is_err(), "no trailer at all");
        assert!(
            PrefixRows::decode(&good[..4]).is_err(),
            "truncated to nothing"
        );

        // A restart count larger than the block can hold
        let mut wrong = good.clone();
        let at = wrong.len() - TRAILER_LEN;
        wrong[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(
            PrefixRows::decode(&wrong).is_err(),
            "impossible restart count"
        );

        // A row count the restarts cannot account for
        let mut mismatched = good.clone();
        let at = mismatched.len() - 4;
        mismatched[at..].copy_from_slice(&9999u32.to_le_bytes());
        assert!(
            PrefixRows::decode(&mismatched).is_err(),
            "rows disagree with restarts"
        );
    }

    // the encoded block is smaller than the keys it stands for
    #[test]
    fn the_encoded_block_is_smaller_than_its_keys() {
        let keys = paths(400);
        let rows = built(&keys);

        let whole: usize = keys.iter().map(|key| key.len() + TAIL).sum();
        assert!(
            rows.encoded_len() * 2 < whole,
            "encoded {} against {whole} whole",
            rows.encoded_len(),
        );
    }
}
