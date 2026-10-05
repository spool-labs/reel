//! One ordered run of a paged column, taken from the map and the footers at once
//!
//! A paged column holds its keys in two places: the map has what no footer covers
//! yet, and every sealed segment has a sorted run of its own. Both are already in
//! key order, so a playback is a merge rather than a sort. Three rules decide a
//! merged key: one the map holds wins outright, among footers the highest sequence
//! number wins since a segment number is not a version, and the map is asked about
//! every footer key before its row is believed.

use std::sync::Arc;

use std::ops::Bound;

use crate::error::Result;
use crate::format::column::{ColumnId, KeyBytes, MAX_KEY_LEN};
use crate::format::footer::{FooterPartition, FooterRow, SegmentFooter};
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::Lsn;
use crate::index::column::ColumnIndex;
use crate::index::entry::Entry;
use crate::index::page::KeyPage;
use crate::index::paged::{FooterSource, SealedRanges};

/// One paged column's three sources: its map, what it has sealed, and the footers
pub struct Paged<'a> {
    /// The column being played
    pub column: ColumnId,

    /// Its resident map, which holds what no footer covers yet
    pub index: &'a ColumnIndex,

    /// What each of its sealed segments covers, so most can be ruled out
    pub sealed: &'a SealedRanges,

    /// Where the footers themselves are read from
    pub footers: &'a dyn FooterSource,
}

/// Which way a playback crosses its column
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Way {
    /// Ascending from a lower bound
    Up,

    /// Descending from an upper bound
    Down,
}

/// One sealed segment's rows for one column, held for the length of a playback
struct Run {
    /// Segment the rows came from, which is where their records are
    segment: SegmentId,

    /// The whole footer, held so stepping a row costs no read
    footer: Arc<SegmentFooter>,

    /// Which of the footer's partitions holds this column
    partition: usize,
}

impl Run {
    fn rows(&self) -> &FooterPartition {
        &self.footer.partitions[self.partition]
    }
}

/// The key one cursor stands on: the rows sharing it, and its leading bytes
///
/// A segment that overwrote its own record holds both versions under one key in write
/// order, so the last row sharing a key is the live one, whichever end a playback
/// reaches it from.
#[derive(Clone, Copy)]
struct Head {
    /// The key's leading eight bytes, big-endian, which settle nearly every compare
    lead: u64,

    /// First and last row sharing the key
    first: usize,
    last: usize,

    /// Whether the cursor has passed the last row in its direction
    is_spent: bool,
}

impl Head {
    const SPENT: Head = Head {
        lead: 0,
        first: 0,
        last: 0,
        is_spent: true,
    };

    /// The head a cursor stepping one way has when it arrives at a row
    ///
    /// Stepping up arrives at the first row of a key and stepping down at the last, so
    /// only the far end of the run is looked for.
    fn at(rows: &FooterPartition, way: Way, row: usize) -> Head {
        let Some(key) = rows.key_at(row) else {
            return Head::SPENT;
        };
        let (mut first, mut last) = (row, row);
        match way {
            Way::Up => {
                while rows.key_at(last + 1) == Some(key) {
                    last += 1;
                }
            }
            Way::Down => {
                while first > 0 && rows.key_at(first - 1) == Some(key) {
                    first -= 1;
                }
            }
        }
        Head {
            lead: lead_of(key),
            first,
            last,
            is_spent: false,
        }
    }

    /// Where a cursor opened at a bound first stands
    ///
    /// A bound outside the rows' own range places the cursor at their near end with no
    /// search, which is every segment of a merged run but the one holding the bound.
    fn placed(rows: &FooterPartition, way: Way, from: Bound<&[u8]>) -> Head {
        let count = rows.len();
        let (Some(low), Some(high)) = (rows.key_at(0), count.checked_sub(1).and_then(|last| rows.key_at(last))) else {
            return Head::SPENT;
        };
        let row = match (way, from) {
            (Way::Up, Bound::Unbounded) => Some(0),
            (Way::Up, Bound::Included(key)) if key <= low => Some(0),
            (Way::Up, Bound::Included(key)) => Some(rows.lower_bound(key)),
            (Way::Up, Bound::Excluded(key)) if key < low => Some(0),
            (Way::Up, Bound::Excluded(key)) => Some(rows.upper_bound(key)),
            (Way::Down, Bound::Unbounded) => Some(count - 1),
            (Way::Down, Bound::Included(key)) if key >= high => Some(count - 1),
            (Way::Down, Bound::Included(key)) => rows.upper_bound(key).checked_sub(1),
            (Way::Down, Bound::Excluded(key)) if key > high => Some(count - 1),
            (Way::Down, Bound::Excluded(key)) => rows.lower_bound(key).checked_sub(1),
        };
        match row {
            Some(row) => Head::at(rows, way, row),
            None => Head::SPENT,
        }
    }
}

/// A key's leading eight bytes as one integer, zero filled past a short key
///
/// Two leads that differ order their keys as the bytes do, and equal ones leave the
/// rest of the keys to decide.
fn lead_of(key: &[u8]) -> u64 {
    let mut lead = [0u8; 8];
    let led = key.len().min(8);
    lead[..led].copy_from_slice(&key[..led]);
    u64::from_be_bytes(lead)
}

/// A cursor on every sealed run a playback reaches, played against each other in a loser tree
///
/// A step plays the cursor that moved up its own path, one compare a level, and the leads
/// settle nearly every compare without a key being read.
#[derive(Default)]
struct Sealed {
    runs: Vec<Run>,
    heads: Vec<Head>,

    /// The loser each node holds, and at 0 the cursor holding the next key
    tree: Vec<usize>,
}

impl Sealed {
    /// Play cursors already placed against each other
    fn new(runs: Vec<Run>, heads: Vec<Head>, way: Way) -> Sealed {
        let count = heads.len();
        let mut sealed = Sealed {
            runs,
            heads,
            tree: vec![0; count],
        };
        // Leaves stand past the nodes, and a node's children are the winners below it.
        let mut winners = vec![0usize; 2 * count];
        for leaf in 0..count {
            winners[count + leaf] = leaf;
        }
        for node in (1..count).rev() {
            let (left, right) = (winners[2 * node], winners[2 * node + 1]);
            let (won, lost) = match sealed.ahead(way, right, left) {
                true => (right, left),
                false => (left, right),
            };
            winners[node] = won;
            sealed.tree[node] = lost;
        }
        if count > 0 {
            sealed.tree[0] = winners[1];
        }
        sealed
    }

    /// The key the front cursor stands on, or nothing once every run is spent
    fn key(&self) -> Option<&[u8]> {
        self.key_of(*self.tree.first()?)
    }

    fn key_of(&self, at: usize) -> Option<&[u8]> {
        let head = &self.heads[at];
        match head.is_spent {
            true => None,
            false => self.runs[at].rows().key_at(head.first),
        }
    }

    /// The front cursor, when it stands on this key
    fn front_on(&self, key: &[u8]) -> Option<usize> {
        let at = *self.tree.first()?;
        (self.key_of(at) == Some(key)).then_some(at)
    }

    /// Step every cursor standing on a key past it, reading none of their rows
    fn skip(&mut self, way: Way, key: &[u8]) {
        while let Some(at) = self.front_on(key) {
            self.step(way, at);
        }
    }

    /// The newest row any cursor holds for a key, under a ceiling when one is given
    ///
    /// Every cursor standing on the key is stepped past it either way. The sequence
    /// number decides rather than the segment number, since a volume writing through
    /// several tails can land a rewrite in a lower-numbered segment.
    fn newest(&mut self, way: Way, key: &[u8], below: Option<Lsn>) -> Result<Option<(SegmentId, FooterRow)>> {
        let mut newest: Option<(SegmentId, FooterRow)> = None;
        while let Some(at) = self.front_on(key) {
            let found = self.runs[at].rows().row_at(self.heads[at].last)?;
            let segment = self.runs[at].segment;
            self.step(way, at);
            let is_under = below.is_none_or(|below| found.lsn < below);
            if is_under && newest.as_ref().is_none_or(|(_, newest)| newest.lsn < found.lsn) {
                newest = Some((segment, found));
            }
        }
        Ok(newest)
    }

    /// Move the front cursor off its key and play it up the tree
    fn step(&mut self, way: Way, at: usize) {
        let head = self.heads[at];
        let next = match way {
            Way::Up => Some(head.last + 1),
            Way::Down => head.first.checked_sub(1),
        };
        self.heads[at] = match next {
            Some(row) => Head::at(self.runs[at].rows(), way, row),
            None => Head::SPENT,
        };
        self.replay(way, at);
    }

    /// Play one leaf up to the root, leaving the new front at 0
    ///
    /// Only the front cursor may move, since every loser on its path was played
    /// against it and no other leaf's path says the same.
    fn replay(&mut self, way: Way, leaf: usize) {
        let mut winner = leaf;
        let mut node = (self.heads.len() + leaf) / 2;
        while node > 0 {
            let held = self.tree[node];
            if self.ahead(way, held, winner) {
                self.tree[node] = winner;
                winner = held;
            }
            node /= 2;
        }
        self.tree[0] = winner;
    }

    /// Whether one cursor's key comes before another's in the playback's order
    ///
    /// A spent cursor sorts behind every live one, so the tree empties from the front.
    fn ahead(&self, way: Way, left: usize, right: usize) -> bool {
        let (one, other) = (&self.heads[left], &self.heads[right]);
        if one.is_spent || other.is_spent {
            return !one.is_spent;
        }
        if one.lead != other.lead {
            return match way {
                Way::Up => one.lead < other.lead,
                Way::Down => one.lead > other.lead,
            };
        }
        match (self.key_of(left), self.key_of(right)) {
            (Some(one), Some(other)) => is_ahead(way, one, other),
            _ => false,
        }
    }
}

/// Where one playback has reached, and the sources it is reading to get there
///
/// Every page wants the same cursors standing where the last page left them, since
/// rebuilding them per page would cost a footer fetch per segment per page. Held by
/// the caller, since two playbacks of one column are two places in it.
pub struct PlaybackCursor {
    /// The column being played and the direction it is crossed in
    column: ColumnId,
    way: Way,

    /// Where the next page starts, or nothing once the playback has run out
    at: Option<Bound<KeyBytes>>,

    /// A cursor on every sealed segment the playback reaches into
    sealed: Sealed,

    /// The sealed set the cursors were opened against, so a change reopens them
    generation: Option<u64>,

    /// The map's half of the merge, kept so a page does not allocate one
    resident: KeyPage,
}

impl PlaybackCursor {
    /// A playback of one column from a bound, with nothing open yet
    pub fn new(column: ColumnId, way: Way, from: Bound<&[u8]>) -> Result<PlaybackCursor> {
        Ok(PlaybackCursor {
            column,
            way,
            at: Some(owned_bound(from)?),
            sealed: Sealed::default(),
            generation: None,
            resident: KeyPage::with_lens(),
        })
    }

    /// The column this playback crosses
    pub fn column(&self) -> ColumnId {
        self.column
    }

    /// Which way it crosses it
    pub fn way(&self) -> Way {
        self.way
    }

    /// Where the next page starts, or nothing once the playback is over
    pub fn at(&self) -> Option<Bound<KeyBytes>> {
        self.at.clone()
    }

    /// Whether the playback has run out of column to cross
    pub fn is_done(&self) -> bool {
        self.at.is_none()
    }

    /// Where the playback stands, enough to put it back if a page is abandoned
    ///
    /// A page filled without the publish barrier has already carried the playback
    /// past itself by the time the fill learns a batch landed under it.
    pub(crate) fn mark(&self) -> Option<Bound<KeyBytes>> {
        self.at.clone()
    }

    /// Put the playback back where a mark was taken, so its page can be filled again
    ///
    /// The open cursors are opened again rather than wound back, since they stand
    /// wherever the abandoned page left them and nothing here knows how far that was.
    pub(crate) fn rewind(&mut self, mark: &Option<Bound<KeyBytes>>) {
        self.at = mark.clone();
        self.generation = None;
    }

    /// Fill a page from the column's own map and carry the playback past it
    ///
    /// What a column with nothing sealed answers with, and the whole of what a
    /// resident volume ever does.
    pub fn page_resident(
        &mut self,
        index: &ColumnIndex,
        limit: usize,
        out: &mut KeyPage,
    ) -> Result<()> {
        out.clear();
        let Some(at) = self.at.as_ref() else {
            return Ok(());
        };
        resident_page(index, self.way, borrowed_bound(at), limit, out);
        self.advance(out, limit)
    }

    /// Carry the playback past the last key a page delivered
    ///
    /// A page that came back short is the end of the column, so the playback stops
    /// there rather than asking again for what is not coming.
    fn advance(&mut self, page: &KeyPage, wanted: usize) -> Result<()> {
        let last = match page.is_empty() {
            true => None,
            false => Some(KeyBytes::new(
                page.key_ref(page.len() - 1).unwrap_or_default(),
            )?),
        };
        self.resume_after(last, page.len(), wanted);
        Ok(())
    }

    /// Where the next run picks up, given what this one delivered
    ///
    /// A run that filled what was asked of it resumes strictly past its own last key;
    /// a short one is the end of the playback.
    fn resume_after(&mut self, last: Option<KeyBytes>, filled: usize, wanted: usize) {
        self.at = match last {
            Some(last) if filled == wanted && wanted > 0 => Some(Bound::Excluded(last)),
            Some(_) | None => None,
        };
    }

    /// Open the cursors, or reopen them if the sealed set has moved under the playback
    ///
    /// Reopening puts them where the playback has reached rather than where it began,
    /// so a segment sealing mid-playback is picked up without redelivering pages.
    fn open(&mut self, paged: &Paged<'_>) -> Result<()> {
        let generation = paged.sealed.generation();
        if self.generation == Some(generation) {
            return Ok(());
        }
        let Some(at) = self.at.as_ref() else {
            return Ok(());
        };
        self.sealed = paged.open_sealed(self.way, borrowed_bound(at))?;
        self.generation = Some(generation);
        Ok(())
    }
}

/// Fill a page with one merged run of a paged column's keys
///
/// The map's own page is taken first, since it both supplies keys and says how far
/// the run may reach. What comes back is a contiguous run in playback order.
pub fn merged_page(
    paged: &Paged<'_>,
    playback: &mut PlaybackCursor,
    limit: usize,
    out: &mut KeyPage,
) -> Result<()> {
    let index = paged.index;
    let way = playback.way;
    out.clear();
    if limit == 0 {
        return Ok(());
    }
    let Some(at) = playback.at.as_ref() else {
        return Ok(());
    };
    // The map's page is read before the footers open. A key the map hands to a footer
    // after this read is in the page, and one it handed over before sits in a segment
    // noted before that, which the opening counts.
    resident_page(index, way, borrowed_bound(at), limit, &mut playback.resident);
    playback.open(paged)?;

    let PlaybackCursor {
        sealed, resident, ..
    } = playback;
    // A page that came back full says nothing about the keys past its last, so the
    // merge stops there rather than emitting a footer key over an unread one.
    let edge = (resident.len() == limit)
        .then(|| resident.key_ref(limit - 1))
        .flatten();

    let mut taken = 0usize;
    let mut want = [0u8; MAX_KEY_LEN];

    while out.len() < limit {
        let Some(key) = next_key(resident.key_ref(taken), sealed, way, &mut want) else {
            break;
        };
        if edge.is_some_and(|edge| is_ahead(way, edge, key)) {
            break;
        }

        // The map's answer stands on its own, so the footers are stepped past this
        // key without their rows being decoded.
        if resident.key_ref(taken) == Some(key) {
            sealed.skip(way, key);
            // Dropping the key here would lose it for good, since the next page
            // starts after the last key this one emitted.
            let found = resident
                .found_at(taken)
                .expect("the merge's own page carries its entries");
            out.push(key, found);
            taken += 1;
            continue;
        }
        // A key missing from the page can still be one the map took since the page was
        // read. A grave drops it, and a put answers with its own entry.
        let newest = sealed.newest(way, key, None)?;
        if let Some(entry) = index.entry_or_grave(key) {
            if !entry.is_grave() && !index.is_covered_key(key, entry.lsn) {
                out.push(key, entry);
            }
            continue;
        }
        let Some((segment, found)) = newest else {
            continue;
        };
        if found.is_tombstone() || found.is_range_tombstone() {
            continue;
        }
        if index.is_covered_key(key, found.lsn) {
            continue;
        }
        out.push(
            key,
            Entry::new(Loc::new(segment, found.offset, found.len), found.lsn),
        );
    }

    playback.advance(out, limit)?;
    Ok(())
}

/// One page of a column's own map, in the playback's direction
pub fn resident_page(
    index: &ColumnIndex,
    way: Way,
    from: Bound<&[u8]>,
    limit: usize,
    out: &mut KeyPage,
) {
    match way {
        Way::Up => index.page(from, limit, out),
        Way::Down => index.page_back(from, limit, out),
    }
}

/// What one bounded release run covered
pub struct ReleaseRun {
    /// Key the next run resumes from, or nothing when the range is exhausted
    pub resume: Option<Vec<u8>>,

    /// Keys the run examined, the budget it spent
    pub examined: usize,
}

/// The footer rows a standing cover has taken, newest below the cover per key
///
/// Per key, the newest row below the cover is the one still booked live: rows under
/// it were settled by whichever overwrite shadowed them, and a row at or past the
/// cover was written after the delete. Bounded by keys examined rather than rows
/// kept, so a run of skips still moves the cursor.
pub fn release_rows(
    paged: &Paged<'_>,
    playback: &mut PlaybackCursor,
    until: Option<&[u8]>,
    below: Lsn,
    limit: usize,
    out: &mut Vec<(KeyBytes, Loc)>,
) -> Result<ReleaseRun> {
    let index = paged.index;
    out.clear();
    if playback.is_done() {
        return Ok(ReleaseRun {
            resume: None,
            examined: 0,
        });
    }
    playback.open(paged)?;
    let way = playback.way;
    let PlaybackCursor { sealed, .. } = playback;
    let mut want = [0u8; MAX_KEY_LEN];
    let mut examined = 0usize;
    let mut last_len = 0usize;

    while examined < limit {
        let Some(key) = next_key(None, sealed, way, &mut want) else {
            return Ok(ReleaseRun {
                resume: None,
                examined,
            });
        };
        if until.is_some_and(|until| key >= until) {
            return Ok(ReleaseRun {
                resume: None,
                examined,
            });
        }
        examined += 1;
        last_len = key.len();

        let Some((segment, found)) = sealed.newest(way, key, Some(below))? else {
            continue;
        };
        if found.is_tombstone() || found.is_range_tombstone() {
            continue;
        }
        match index.entry_or_grave(key) {
            Some(entry) if entry.lsn < below => continue,
            _ => {}
        }
        if index.covered_by_swept(key, found.lsn) {
            continue;
        }
        out.push((
            KeyBytes::new(key)?,
            Loc::new(segment, found.offset, found.len),
        ));
    }

    // The next run starts at the key straight after the last one examined, and
    // rebuilding the cursor from that bound is what makes a segment sealing between
    // runs safe: its rows below the bound belong to keys the map still holds.
    Ok(ReleaseRun {
        resume: successor(&want[..last_len]),
        examined,
    })
}

/// The immediate key after this one at its width, or nothing past the top
fn successor(key: &[u8]) -> Option<Vec<u8>> {
    let mut next = key.to_vec();
    for byte in next.iter_mut().rev() {
        match byte.checked_add(1) {
            Some(bumped) => {
                *byte = bumped;
                return Some(next);
            }
            None => *byte = 0,
        }
    }
    None
}

/// Copy a borrowed bound, for a playback that carries its place between pages
fn owned_bound(bound: Bound<&[u8]>) -> Result<Bound<KeyBytes>> {
    Ok(match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(key) => Bound::Included(KeyBytes::new(key)?),
        Bound::Excluded(key) => Bound::Excluded(KeyBytes::new(key)?),
    })
}

/// Borrow a carried bound back
fn borrowed_bound(bound: &Bound<KeyBytes>) -> Bound<&[u8]> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(key) => Bound::Included(key.as_slice()),
        Bound::Excluded(key) => Bound::Excluded(key.as_slice()),
    }
}

impl Paged<'_> {
    /// Open a cursor on every sealed segment whose keys reach into the playback
    fn open_sealed(&self, way: Way, from: Bound<&[u8]>) -> Result<Sealed> {
        let bound = match from {
            Bound::Unbounded => None,
            Bound::Included(key) | Bound::Excluded(key) => Some(key),
        };
        let reachable = match way {
            Way::Up => self.sealed.spanning(bound, None),
            Way::Down => self.sealed.spanning(None, bound),
        };

        let mut runs = Vec::with_capacity(reachable.len());
        let mut heads = Vec::with_capacity(reachable.len());
        for segment in reachable {
            // A segment retired between the range read and this one is simply gone,
            // and what it held has been copied on or was dead.
            let Some(footer) = self.footers.footer(segment)? else {
                continue;
            };
            let Some(partition) = footer.partitions.iter().position(|rows| rows.column == self.column) else {
                continue;
            };
            let head = Head::placed(&footer.partitions[partition], way, from);
            if head.is_spent {
                continue;
            }
            runs.push(Run { segment, footer, partition });
            heads.push(head);
        }
        Ok(Sealed::new(runs, heads, way))
    }
}

/// The next key in playback order across the map's page and every open cursor
///
/// Copied into the caller's buffer rather than borrowed from whichever source won,
/// since the merge steps those sources while it still has the key in hand.
fn next_key<'a>(
    resident: Option<&[u8]>,
    sealed: &Sealed,
    way: Way,
    want: &'a mut [u8; MAX_KEY_LEN],
) -> Option<&'a [u8]> {
    let sealed = sealed.key();
    let best = match (resident, sealed) {
        (Some(resident), Some(sealed)) => match is_ahead(way, sealed, resident) {
            true => sealed,
            false => resident,
        },
        (Some(only), None) | (None, Some(only)) => only,
        (None, None) => return None,
    };
    want[..best.len()].copy_from_slice(best);
    Some(&want[..best.len()])
}

/// Whether one key comes before another in the playback's own order
fn is_ahead(way: Way, key: &[u8], than: &[u8]) -> bool {
    match way {
        Way::Up => key < than,
        Way::Down => key > than,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::format::column::KeyWidth;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::config::IndexResidency;
    use crate::format::column::{Codec, ColumnSpec, RecordKey};
    use crate::format::footer::FooterEntry;
    use crate::format::lsn::Lsn;
    use crate::format::record::Flags;

    const COLUMN: ColumnId = ColumnId(1);

    /// Keys per page the fixture pages in, small enough to take several
    const PAGE: usize = 2;

    const SPEC: ColumnSpec = ColumnSpec {
        id: COLUMN,
        name: "flat",
        key_width: KeyWidth::Fixed(8),
        shard_bytes: 0,
        purge_mark: None,
        codec: Codec::None,
    };

    /// A footer source over hand-built footers that counts what it is asked for
    struct CountingFooters {
        footers: Vec<(SegmentId, Arc<SegmentFooter>)>,
        opened: AtomicUsize,
    }

    impl FooterSource for CountingFooters {
        fn footer(&self, segment: SegmentId) -> Result<Option<Arc<SegmentFooter>>> {
            self.opened.fetch_add(1, Ordering::Relaxed);
            Ok(self
                .footers
                .iter()
                .find(|(held, _)| *held == segment)
                .map(|(_, footer)| Arc::clone(footer)))
        }
    }

    /// A paged column with nothing resident and the footers it can be given
    struct Fixture {
        index: ColumnIndex,
        sealed: SealedRanges,
        footers: CountingFooters,
    }

    impl Fixture {
        fn new(footers: &[(SegmentId, &[u8])]) -> Fixture {
            Fixture {
                index: ColumnIndex::new(&SPEC, IndexResidency::Resident).expect("column"),
                sealed: SealedRanges::new(),
                footers: CountingFooters {
                    footers: footers
                        .iter()
                        .map(|(segment, keys)| (*segment, footer(keys)))
                        .collect(),
                    opened: AtomicUsize::new(0),
                },
            }
        }

        fn paged(&self) -> Paged<'_> {
            Paged {
                column: COLUMN,
                index: &self.index,
                sealed: &self.sealed,
                footers: &self.footers,
            }
        }

        /// Say a segment has sealed, covering an inclusive run of keys
        fn seal(&self, segment: SegmentId, low: u8, high: u8) {
            self.sealed.note(
                segment,
                KeyBytes::new(&[low; 8]).expect("low"),
                KeyBytes::new(&[high; 8]).expect("high"),
            );
        }

        fn opened(&self) -> usize {
            self.footers.opened.load(Ordering::Relaxed)
        }

        /// Take one page, and say which keys it carried
        fn page(&self, playback: &mut PlaybackCursor, page: &mut KeyPage) -> Vec<u8> {
            merged_page(&self.paged(), playback, PAGE, page).expect("page");
            (0..page.len()).map(|row| page.key_at(row)[0]).collect()
        }

        /// Playback what is left of a column, a page at a time
        fn drain(&self, playback: &mut PlaybackCursor) -> Vec<u8> {
            let mut page = KeyPage::with_lens();
            let mut seen = Vec::new();
            while !playback.is_done() {
                seen.extend(self.page(playback, &mut page));
            }
            seen
        }
    }

    fn key(byte: u8) -> RecordKey {
        RecordKey::from_bytes(COLUMN, &[byte; 8]).expect("key")
    }

    /// A sealed segment holding one row per key, sorted the way a seal leaves them
    fn footer(keys: &[u8]) -> Arc<SegmentFooter> {
        let rows: Vec<FooterEntry> = keys
            .iter()
            .enumerate()
            .map(|(at, byte)| FooterEntry::new(key(*byte), Lsn(at as u64 + 1), 0, 100, Flags::DATA))
            .collect();
        let mut footer = SegmentFooter::build(rows);
        let _ = footer.pack(0);
        Arc::new(footer)
    }

    fn playback() -> PlaybackCursor {
        PlaybackCursor::new(COLUMN, Way::Up, Bound::Unbounded).expect("playback")
    }

    // a playback opens each segment's footer once, however many pages it takes
    #[test]
    fn a_playback_opens_its_footers_once() {
        let fixture = Fixture::new(&[(SegmentId(1), &[1, 2, 3, 4]), (SegmentId(2), &[5, 6, 7, 8])]);
        fixture.seal(SegmentId(1), 1, 4);
        fixture.seal(SegmentId(2), 5, 8);

        let seen = fixture.drain(&mut playback());

        assert_eq!(seen, vec![1, 2, 3, 4, 5, 6, 7, 8], "four pages of two");
        assert_eq!(
            fixture.opened(),
            2,
            "one open per segment for the whole playback, not per page"
        );
    }

    // a segment sealed mid-playback is picked up, since the cursors are opened again
    #[test]
    fn a_seal_mid_walk_reopens_the_cursors() {
        let fixture = Fixture::new(&[(SegmentId(1), &[1, 2, 3, 4]), (SegmentId(2), &[5, 6])]);
        fixture.seal(SegmentId(1), 1, 4);

        let mut playback = playback();
        let mut page = KeyPage::with_lens();
        assert_eq!(fixture.page(&mut playback, &mut page), vec![1, 2]);

        // The second segment seals while the playback is partway through the first.
        fixture.seal(SegmentId(2), 5, 6);

        assert_eq!(
            fixture.drain(&mut playback),
            vec![3, 4, 5, 6],
            "what sealed mid-playback is still played"
        );
    }
}
