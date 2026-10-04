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

use reel_core::Value;

use crate::error::Result;
use crate::format::column::{ColumnId, KeyBytes, MAX_KEY_LEN};
use crate::format::footer::{FooterPartition, FooterRow, SegmentFooter};
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::Lsn;
use crate::index::column::ColumnIndex;
use crate::index::entry::Entry;
use crate::index::fastforward::{FastColumn, RowAsk, RowRead, WalkRow};
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

/// One sealed segment's rows for one column, stepped in the playback's own order
struct SegmentRows {
    /// Segment the rows came from, which is where their records are
    segment: SegmentId,

    /// The whole footer, held for the run so stepping a row costs no read
    footer: Arc<SegmentFooter>,

    /// Which of the footer's partitions holds this column
    partition: usize,

    /// Row the cursor sits on
    at: usize,

    /// Whether the cursor has passed the last row in its direction
    spent: bool,

    /// Direction the cursor steps in
    way: Way,
}

impl SegmentRows {
    /// Open a cursor on one segment's rows for a column, placed at a bound
    ///
    /// Nothing comes back when the segment holds no rows for the column, or when
    /// the bound is already past every row it does hold.
    fn open(
        segment: SegmentId,
        footer: Arc<SegmentFooter>,
        column: ColumnId,
        way: Way,
        from: Bound<&[u8]>,
    ) -> Option<SegmentRows> {
        let partition = footer
            .partitions
            .iter()
            .position(|partition| partition.column == column)?;
        let rows = &footer.partitions[partition];
        if rows.is_empty() {
            return None;
        }

        let at = match (way, from) {
            (Way::Up, Bound::Unbounded) => 0,
            (Way::Up, Bound::Included(key)) => rows.lower_bound(key),
            (Way::Up, Bound::Excluded(key)) => rows.upper_bound(key),
            (Way::Down, Bound::Unbounded) => rows.len() - 1,
            (Way::Down, Bound::Included(key)) => rows.upper_bound(key).checked_sub(1)?,
            (Way::Down, Bound::Excluded(key)) => rows.lower_bound(key).checked_sub(1)?,
        };
        if at >= rows.len() {
            return None;
        }

        Some(SegmentRows {
            segment,
            footer,
            partition,
            at,
            spent: false,
            way,
        })
    }

    fn rows(&self) -> &FooterPartition {
        &self.footer.partitions[self.partition]
    }

    /// The key the cursor sits on, or nothing once it has run out
    fn key(&self) -> Option<&[u8]> {
        match self.spent {
            true => None,
            false => self.rows().key_at(self.at),
        }
    }

    /// Take the newest row for the key the cursor sits on and step past its run
    ///
    /// A segment that overwrote its own record holds both versions under one key in
    /// write order, so the newest is the last of the run and stepping past the rest
    /// keeps a repeated key from being merged twice.
    fn take(&mut self) -> Result<Option<FooterRow>> {
        let Some((first, last)) = self.run() else {
            return Ok(None);
        };
        // The last of a run is the live one, whichever end the playback came at it from.
        let found = self.rows().row_at(last)?;
        self.step_past(first, last);
        Ok(Some(found))
    }

    /// Step past every row sharing the key the cursor sits on, reading none of them
    fn skip(&mut self) {
        match self.run() {
            Some((first, last)) => self.step_past(first, last),
            None => self.spent = true,
        }
    }

    /// The rows sharing the key the cursor sits on, or nothing once it has run out
    fn run(&self) -> Option<(usize, usize)> {
        match self.spent {
            true => None,
            false => self.rows().run(self.at),
        }
    }

    /// Move the cursor off a run, which is the one part of this the direction owns
    fn step_past(&mut self, first: usize, last: usize) {
        let next = match self.way {
            // Asking the row itself rather than the count, since the count is a
            // division by a stride the partition works out every time it is asked.
            Way::Up => self.rows().key_at(last + 1).map(|_| last + 1),
            Way::Down => first.checked_sub(1),
        };
        match next {
            Some(at) => self.at = at,
            None => self.spent = true,
        }
    }
}

/// The open cursors, ordered so the one holding the next key is to hand
///
/// Ordering costs the depth of a heap per key where a scan over every cursor costs
/// its width. The heap holds positions rather than keys, since the cursor a position
/// names already holds its key.
#[derive(Default)]
struct Front {
    /// Cursor positions, heap ordered by the key each one sits on
    order: Vec<usize>,
}

impl Front {
    /// Order every cursor that still holds a key
    fn new(rows: &[SegmentRows], way: Way) -> Front {
        let mut front = Front {
            order: (0..rows.len())
                .filter(|at| rows[*at].key().is_some())
                .collect(),
        };
        for at in (0..front.order.len() / 2).rev() {
            front.sift_down(rows, way, at);
        }
        front
    }

    /// The cursor holding the next key in playback order
    fn peek(&self) -> Option<usize> {
        self.order.first().copied()
    }

    /// Take the front cursor off, leaving the next one in its place
    fn pop(&mut self, rows: &[SegmentRows], way: Way) -> Option<usize> {
        let front = self.order.first().copied()?;
        let last = self.order.pop().expect("a heap that answered");
        if !self.order.is_empty() {
            self.order[0] = last;
            self.sift_down(rows, way, 0);
        }
        Some(front)
    }

    /// Put a cursor back once it has moved, unless it has run out
    fn push(&mut self, rows: &[SegmentRows], way: Way, at: usize) {
        if rows[at].key().is_none() {
            return;
        }
        self.order.push(at);
        self.sift_up(rows, way, self.order.len() - 1);
    }

    fn sift_up(&mut self, rows: &[SegmentRows], way: Way, mut at: usize) {
        while at > 0 {
            let parent = (at - 1) / 2;
            if !ahead_of(rows, way, self.order[at], self.order[parent]) {
                return;
            }
            self.order.swap(at, parent);
            at = parent;
        }
    }

    fn sift_down(&mut self, rows: &[SegmentRows], way: Way, mut at: usize) {
        loop {
            let mut first = at;
            for child in [at * 2 + 1, at * 2 + 2] {
                if child < self.order.len()
                    && ahead_of(rows, way, self.order[child], self.order[first])
                {
                    first = child;
                }
            }
            if first == at {
                return;
            }
            self.order.swap(at, first);
            at = first;
        }
    }
}

/// Whether one cursor's key comes before another's in the playback's own order
fn ahead_of(rows: &[SegmentRows], way: Way, left: usize, right: usize) -> bool {
    match (rows[left].key(), rows[right].key()) {
        (Some(left), Some(right)) => is_ahead(way, left, right),
        // A cursor that has run out sorts behind one that has not, so the heap
        // empties from the front rather than carrying spent cursors at its root.
        (Some(_), None) => true,
        (None, _) => false,
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

    /// One open cursor per sealed segment the playback reaches into
    rows: Vec<SegmentRows>,

    /// Those same cursors, ordered by the key each one sits on
    front: Front,

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
            rows: Vec::new(),
            front: Front::default(),
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
        self.rows = paged.open_rows(self.way, borrowed_bound(at))?;
        self.front = Front::new(&self.rows, self.way);
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
        rows,
        front,
        resident,
        ..
    } = playback;
    // A page that came back full says nothing about the keys past its last, so the
    // merge stops there rather than emitting a footer key over an unread one.
    let edge = (resident.len() == limit)
        .then(|| resident.key_ref(limit - 1))
        .flatten();

    let mut taken = 0usize;
    let mut want = [0u8; MAX_KEY_LEN];

    while out.len() < limit {
        let Some(key) = next_key(resident.key_ref(taken), rows, front, way, &mut want) else {
            break;
        };
        if edge.is_some_and(|edge| is_ahead(way, edge, key)) {
            break;
        }

        // The map's answer stands on its own, so the footers are stepped past this
        // key without their rows being decoded.
        if resident.key_ref(taken) == Some(key) {
            take_front(rows, front, way, key, |_, row| {
                row.skip();
                Ok(())
            })?;
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
        let newest = newest_row(rows, front, way, key)?;
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

/// Sealed rows an ordered page reads past what it still needs, for those it will drop
const ORDERED_SLACK: usize = 16;

/// Times one round of an ordered page looks again when a sealed slot left under its look,
/// before the footers answer
const ORDERED_TRIES: usize = 8;

/// How an ordered page fill came out
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Ordered {
    /// The page is filled and the playback moved past it
    Filled,

    /// The sealed index moved under every look of a round, or met a record only the
    /// checked path reads, so the footers fill this page
    Footers,
}

/// One sealed row an ordered page has read
struct SealedRow {
    /// Where the key its record holds sits in the round's packed keys, counted in keys
    at: usize,
    lsn: Lsn,
    len: u32,
    is_tombstone: bool,
    segment: SegmentId,
    offset: u32,

    /// The payload, when the page is for a caller that reads every one
    payload: Option<Value>,
}

/// One ordered page's walk: where its next round starts, and the buffers each round refills
struct Walk {
    /// The map is read past this bound, and a sealed row on the near side of it is dropped
    from: Bound<KeyBytes>,

    /// The lead the sealed walk resumes at, inclusive, or nothing to start at the near end
    ///
    /// Every sealed row on the near side of it has been merged by an earlier round.
    lead: Option<u64>,

    /// Whether a round has merged the last lead there is in the page's direction
    is_sealed_done: bool,

    /// The sealed rows a round's look found
    rows: Vec<WalkRow>,

    /// The keys their records hold, packed at the column's width
    keys: Vec<u8>,
}

/// What one round left the page to do
enum Round {
    /// The page has room and more of the column may be left
    More,

    /// The map and the sealed index both ran out
    End,

    /// The footers fill the page
    Footers,
}

/// The last key a round merged, from the map's page or from the sealed rows
#[derive(Clone, Copy)]
enum Merged {
    Map(usize),
    Sealed(usize),
}

/// Fill a page from the map and an ordered sealed index, leaving the footers closed
///
/// A page fills in rounds. Each round takes the map's page and a run of sealed rows back
/// to back, reads each row's record for its key, and merges the two. A record's key
/// settles a tie on the leading bytes, an older version beside a newer one, and a row of
/// the bound's own lead on the wrong side of it.
///
/// The map is read first, so a key handed from the map to the index is seen at least
/// once. A put moves a key the other way, and the index counts the slot it lets go, so a
/// round whose look saw a slot leave the shards it crossed looks again over half the
/// span. The look is memory alone, which keeps that window short, and the records are
/// read after it.
pub fn ordered_page(
    paged: &Paged<'_>,
    fast: &FastColumn,
    width: usize,
    playback: &mut PlaybackCursor,
    limit: usize,
    out: &mut KeyPage,
) -> Result<Ordered> {
    out.clear();
    let Some(from) = playback.at.clone() else {
        return Ok(Ordered::Filled);
    };
    let lead = match &from {
        Bound::Included(key) | Bound::Excluded(key) => Some(FastColumn::lead(key.as_slice())),
        Bound::Unbounded => None,
    };
    let mut walk = Walk {
        from,
        lead,
        is_sealed_done: false,
        rows: Vec::new(),
        keys: Vec::new(),
    };
    while out.len() < limit {
        match ordered_round(paged, fast, width, playback, &mut walk, limit, out)? {
            Round::More => {}
            Round::End => break,
            Round::Footers => return Ok(Ordered::Footers),
        }
    }
    playback.advance(out, limit)?;
    // The footers' cursors stand where the last footer page left them, so the next one
    // opens them again here.
    playback.generation = None;
    Ok(Ordered::Filled)
}

/// One round of an ordered page: one look at the map and the index, then the records it points at
fn ordered_round(
    paged: &Paged<'_>,
    fast: &FastColumn,
    width: usize,
    playback: &mut PlaybackCursor,
    walk: &mut Walk,
    limit: usize,
    out: &mut KeyPage,
) -> Result<Round> {
    let index = paged.index;
    let PlaybackCursor { way, resident, .. } = playback;
    let way = *way;
    let with_payloads = out.reads_payloads();
    // A payload read past the page's end is wasted, so a payload page takes no slack.
    let slack = match with_payloads {
        true => 0,
        false => ORDERED_SLACK,
    };
    let from = borrowed_bound(&walk.from);
    let (near, far) = match way {
        Way::Up => (0, u64::MAX),
        Way::Down => (u64::MAX, 0),
    };

    let mut span = limit - out.len();
    let mut looks = 0;
    // The last lead a walk cut short read, or nothing when the walk ran off the end
    let cut = loop {
        if looks == ORDERED_TRIES {
            return Ok(Round::Footers);
        }
        looks += 1;
        let before = fast.taken_all();
        resident_page(index, way, from, span, resident);
        walk.rows.clear();
        if !walk.is_sealed_done {
            fast.walk_lead(walk.lead, way, span + slack, &mut walk.rows);
        }
        let cut = (walk.rows.len() >= span + slack)
            .then(|| walk.rows.last().map(|row| row.lead))
            .flatten();
        if !fast.moved_between(&before, walk.lead.unwrap_or(near), cut.unwrap_or(far)) {
            break cut;
        }
        span = (span / 2).max(1);
    };
    // A full map page says nothing past its last key.
    let edge = (resident.len() == span)
        .then(|| resident.key_ref(span - 1))
        .flatten();
    let walked = walk.rows.last().map(|row| row.lead);

    let Some(mut sealed) = read_sealed(fast, paged.column, width, way, with_payloads, walk)? else {
        return Ok(Round::Footers);
    };
    let keys = walk.keys.as_slice();
    let key_of = |row: &SealedRow| &keys[row.at * width..(row.at + 1) * width];
    let mut next = 0usize;
    let mut taken = 0usize;
    let mut merged = None;
    let mut is_at_cut = false;
    while out.len() < limit {
        let mapped = resident.key_ref(taken);
        let from_map = match (mapped, sealed.get(next)) {
            (None, None) => break,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (Some(mapped), Some(row)) => mapped == key_of(row) || is_ahead(way, mapped, key_of(row)),
        };
        if from_map {
            let key = mapped.expect("a map key was chosen");
            // A map key past the last lead a cut walk read may have sealed rows before it
            // that no round has read yet.
            let lead = FastColumn::lead(key);
            let is_past = |last: u64| match way {
                Way::Up => lead > last,
                Way::Down => lead < last,
            };
            if cut.is_some_and(is_past) {
                is_at_cut = true;
                break;
            }
            if sealed.get(next).is_some_and(|row| key_of(row) == key) {
                next += 1;
            }
            let found = resident
                .found_at(taken)
                .expect("the map's page holds its entries");
            out.push(key, found);
            merged = Some(Merged::Map(taken));
            taken += 1;
            continue;
        }

        let row = &mut sealed[next];
        let key = key_of(row);
        if edge.is_some_and(|edge| is_ahead(way, edge, key)) {
            break;
        }
        merged = Some(Merged::Sealed(next));
        next += 1;
        // A key the map took since its page was read: a grave drops it, and a put
        // answers with its own entry.
        if let Some(entry) = index.entry_or_grave(key) {
            if !entry.is_grave() && !index.is_covered_key(key, entry.lsn) {
                out.push(key, entry);
            }
            continue;
        }
        if row.is_tombstone || index.is_covered_key(key, row.lsn) {
            continue;
        }
        let entry = Entry::new(Loc::new(row.segment, row.offset, row.len), row.lsn);
        out.push_read(key, entry, row.payload.take());
    }

    if out.len() >= limit {
        return Ok(Round::More);
    }
    // Short of a full page, a map page that was not full and a walk that ran off the end
    // read both to their ends in one look.
    if edge.is_none() && cut.is_none() {
        return Ok(Round::End);
    }
    let merged = merged.map(|merged| match merged {
        Merged::Map(at) => resident.key_ref(at).expect("a merged map key"),
        Merged::Sealed(at) => key_of(&sealed[at]),
    });
    let step = |last: u64| match way {
        Way::Up => last.checked_add(1),
        Way::Down => last.checked_sub(1),
    };
    let (lead, is_last) = match (is_at_cut, edge, walked) {
        // The map's page reached past the cut and every row walked was merged.
        (true, _, _) => (cut.and_then(step), true),
        // A full map page may end short of what the walk read, and a key handed over
        // between rounds could sit in that gap, so the next walk starts back at the last
        // key merged.
        (false, Some(_), _) => (merged.map(FastColumn::lead).or(walk.lead), false),
        (false, None, Some(last)) => (step(last), true),
        (false, None, None) => (walk.lead, false),
    };
    if let Some(merged) = merged {
        walk.from = Bound::Excluded(KeyBytes::new(merged)?);
    }
    walk.is_sealed_done |= is_last && lead.is_none();
    walk.lead = lead;
    Ok(Round::More)
}

/// Read the records a look's rows point at and order them by key, one row a key
///
/// The newest version of a key stands, a tie going to the newer segment, which is a
/// compaction copy of the other. A row whose segment is gone is taken out of the index,
/// since its record moved on or died. One only the checked path can read sends the page
/// to the footers. A page for a caller reading every payload reads each record whole,
/// so the payload comes in the same read.
fn read_sealed(
    fast: &FastColumn,
    column: ColumnId,
    width: usize,
    way: Way,
    with_payloads: bool,
    walk: &mut Walk,
) -> Result<Option<Vec<SealedRow>>> {
    let Walk { from, rows, keys, .. } = walk;
    let from = borrowed_bound(from);
    let asks: Vec<RowAsk> = rows
        .iter()
        .map(|row| RowAsk {
            segment: row.segment,
            offset: row.offset,
            bound: match with_payloads {
                true => row.bound,
                false => 0,
            },
        })
        .collect();
    let Some(reads) = fast.read_rows(column, width, &asks, keys)? else {
        return Ok(None);
    };
    let mut read = Vec::with_capacity(rows.len());
    for (at, (row, answer)) in rows.iter().zip(reads).enumerate() {
        match answer {
            RowRead::Found { head, value } => read.push(SealedRow {
                at,
                lsn: head.lsn,
                len: head.len,
                is_tombstone: head.is_tombstone,
                segment: row.segment,
                offset: row.offset,
                payload: value,
            }),
            // The record moved on and its source retired, so the slot points at nothing.
            RowRead::Gone => fast.forget_row(row),
            RowRead::Other => {}
            RowRead::Unsure => return Ok(None),
        }
    }
    let keys = keys.as_slice();
    let key_of = |row: &SealedRow| &keys[row.at * width..(row.at + 1) * width];
    read.retain(|row| match (way, from) {
        (_, Bound::Unbounded) => true,
        (Way::Up, Bound::Included(bound)) => key_of(row) >= bound,
        (Way::Up, Bound::Excluded(bound)) => key_of(row) > bound,
        (Way::Down, Bound::Included(bound)) => key_of(row) <= bound,
        (Way::Down, Bound::Excluded(bound)) => key_of(row) < bound,
    });
    // Newest version first within a key, so the dedup below keeps it.
    read.sort_unstable_by(|left, right| {
        let by_key = match way {
            Way::Up => key_of(left).cmp(key_of(right)),
            Way::Down => key_of(right).cmp(key_of(left)),
        };
        by_key.then((right.lsn, right.segment).cmp(&(left.lsn, left.segment)))
    });
    read.dedup_by(|later, kept| key_of(later) == key_of(kept));
    Ok(Some(read))
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
    let PlaybackCursor { rows, front, .. } = playback;
    let mut want = [0u8; MAX_KEY_LEN];
    let mut examined = 0usize;
    let mut last_len = 0usize;

    while examined < limit {
        let Some(key) = next_key(None, rows, front, way, &mut want) else {
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

        let Some((segment, found)) = newest_row_below(rows, front, way, key, below)? else {
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

/// The newest row below a floor any open cursor holds for a key
///
/// Every cursor sitting on the key is stepped past it either way, so a key is
/// visited once whatever the rows said.
fn newest_row_below(
    rows: &mut [SegmentRows],
    front: &mut Front,
    way: Way,
    key: &[u8],
    below: Lsn,
) -> Result<Option<(SegmentId, FooterRow)>> {
    let mut newest: Option<(SegmentId, FooterRow)> = None;
    take_front(rows, front, way, key, |segment, row| {
        if let Some(found) = row.take()? {
            if found.lsn < below
                && newest
                    .as_ref()
                    .is_none_or(|(_, newest)| newest.lsn < found.lsn)
            {
                newest = Some((segment, found));
            }
        }
        Ok(())
    })?;
    Ok(newest)
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
    fn open_rows(&self, way: Way, from: Bound<&[u8]>) -> Result<Vec<SegmentRows>> {
        let bound = match from {
            Bound::Unbounded => None,
            Bound::Included(key) | Bound::Excluded(key) => Some(key),
        };
        let reachable = match way {
            Way::Up => self.sealed.spanning(bound, None),
            Way::Down => self.sealed.spanning(None, bound),
        };

        let mut rows = Vec::with_capacity(reachable.len());
        for segment in reachable {
            // A segment retired between the range read and this one is simply gone,
            // and what it held has been copied on or was dead.
            let Some(footer) = self.footers.footer(segment)? else {
                continue;
            };
            if let Some(cursor) = SegmentRows::open(segment, footer, self.column, way, from) {
                rows.push(cursor);
            }
        }
        Ok(rows)
    }
}

/// The newest row any open cursor holds for a key, stepping every one of them past it
///
/// The sequence number decides it rather than the segment number, since a volume
/// writing through several tails can land a rewrite in a lower-numbered segment.
fn newest_row(
    rows: &mut [SegmentRows],
    front: &mut Front,
    way: Way,
    key: &[u8],
) -> Result<Option<(SegmentId, FooterRow)>> {
    let mut newest: Option<(SegmentId, FooterRow)> = None;
    take_front(rows, front, way, key, |segment, row| {
        if let Some(found) = row.take()? {
            if newest
                .as_ref()
                .is_none_or(|(_, newest)| newest.lsn < found.lsn)
            {
                newest = Some((segment, found));
            }
        }
        Ok(())
    })?;
    Ok(newest)
}

/// Visit every cursor sitting on a key, and put each back where it now belongs
///
/// Only the cursors holding the key are touched, which is what the ordering buys.
fn take_front(
    rows: &mut [SegmentRows],
    front: &mut Front,
    way: Way,
    key: &[u8],
    mut visit: impl FnMut(SegmentId, &mut SegmentRows) -> Result<()>,
) -> Result<()> {
    while front.peek().is_some_and(|at| rows[at].key() == Some(key)) {
        let at = front.pop(rows, way).expect("a front that answered");
        let segment = rows[at].segment;
        visit(segment, &mut rows[at])?;
        front.push(rows, way, at);
    }
    Ok(())
}

/// The next key in playback order across the map's page and every open cursor
///
/// Copied into the caller's buffer rather than borrowed from whichever source won,
/// since the merge steps those sources while it still has the key in hand.
fn next_key<'a>(
    resident: Option<&[u8]>,
    rows: &[SegmentRows],
    front: &Front,
    way: Way,
    want: &'a mut [u8; MAX_KEY_LEN],
) -> Option<&'a [u8]> {
    let sealed = front.peek().and_then(|at| rows[at].key());
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
    use crate::format::column::{Codec, ColumnSpec, MapShape, RecordKey};
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
        map_shape: MapShape::Tree,
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
