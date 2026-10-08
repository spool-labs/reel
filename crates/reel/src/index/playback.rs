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
use crate::format::loc::{Loc, SegmentId, SegmentIncarnation};
use crate::format::lsn::Lsn;
use crate::index::column::ColumnIndex;
use crate::index::counters::SegmentTable;
use crate::index::entry::Entry;
use crate::index::keyrun::{is_vanished, FooterRows, KeyRun, KeyRunSet, RowReader, RunColumn, RunRow, RunViews};
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

    /// Where the footers themselves are read from, held by count so a key run's reader outlives one page
    pub footers: &'a Arc<dyn FooterSource>,

    /// Walks keep their opened runs here while the sealed set stands
    pub runs: &'a WalkRuns,

    /// Merges write these key runs, and walks read them in place of the footers they cover
    pub key_runs: &'a KeyRunSet,

    /// The life each segment wears, which stamps a row's entry so a read can trust its place
    pub segments: &'a SegmentTable,
}

impl Paged<'_> {
    /// Where the walk's runs stand: the sealed set and the key runs, each only ever growing
    fn generation(&self) -> u64 {
        self.sealed
            .generation()
            .wrapping_add(self.key_runs.generation())
    }
}

/// A column keeps its opened runs in this many slots, so walks on different threads share none
const RUN_SLOTS: usize = 16;

/// Hands out each thread's run slot, once
static NEXT_RUN_SLOT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

thread_local! {
    /// This thread takes the same run slot in every column
    static RUN_SLOT: usize = NEXT_RUN_SLOT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % RUN_SLOTS;
}

/// One column's sealed runs, opened as of one generation of its sealed set
pub struct RunSet {
    generation: u64,
    runs: Vec<Run>,

    /// The life every segment the runs reach into wore when they were opened, by segment
    stamps: Vec<(SegmentId, SegmentIncarnation)>,
}

impl RunSet {
    /// The life a segment wore when the runs were opened, or none for one the runs do not reach
    fn stamp_of(&self, segment: SegmentId) -> SegmentIncarnation {
        match self
            .stamps
            .binary_search_by_key(&segment, |(held, _)| *held)
        {
            Ok(at) => self.stamps[at].1,
            Err(_) => SegmentIncarnation::NONE,
        }
    }
}

/// A slot of opened runs, on a cache line of its own
#[repr(align(64))]
#[derive(Default)]
struct RunSlot(std::sync::Mutex<Option<Arc<RunSet>>>);

/// One column's walks keep their opened runs here, one slot per thread, until its sealed set moves
#[derive(Default)]
pub struct WalkRuns {
    slots: [RunSlot; RUN_SLOTS],
}

impl WalkRuns {
    /// The runs as of a generation, from this thread's slot or opened afresh
    fn current(
        &self,
        generation: u64,
        open: impl FnOnce() -> Result<(Vec<Run>, Vec<(SegmentId, SegmentIncarnation)>)>,
    ) -> Result<Arc<RunSet>> {
        let slot = &self.slots[RUN_SLOT.with(|slot| *slot)];
        let mut held = crate::sync::lock(&slot.0);
        if let Some(set) = held.as_ref().filter(|set| set.generation == generation) {
            return Ok(Arc::clone(set));
        }
        let (runs, stamps) = open()?;
        let set = Arc::new(RunSet {
            generation,
            runs,
            stamps,
        });
        *held = Some(Arc::clone(&set));
        Ok(set)
    }

    /// Let go of every slot opened before a generation
    pub fn sweep(&self, generation: u64) {
        for slot in &self.slots {
            let mut held = crate::sync::lock(&slot.0);
            if held
                .as_ref()
                .is_some_and(|set| set.generation != generation)
            {
                *held = None;
            }
        }
    }
}

/// Which way a playback crosses its column
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Way {
    /// Ascending from a lower bound
    Up,

    /// Descending from an upper bound
    Down,
}

/// One sealed run's rows for one column, held while its sealed set stands
enum Run {
    /// A data segment's own footer, whose rows all point into that segment
    Footer {
        /// The segment holding these rows and their records
        segment: SegmentId,

        /// The whole footer, held so stepping a row costs no read
        footer: Arc<SegmentFooter>,

        /// Which of the footer's partitions holds this column
        partition: usize,

        /// The lead of every `LEAD_STRIDE`th row, so a search starts within a stride of its answer
        leads: Vec<u64>,
    },

    /// One column of a key run, each row pointing at a footer row, and what its readers share
    Keys {
        run: Arc<KeyRun>,
        column: usize,
        views: Arc<RunViews>,
    },
}

/// Rows between two sampled leads of a footer run
const LEAD_STRIDE: usize = 64;

/// The lead of every `LEAD_STRIDE`th row of a partition
fn sampled_leads(rows: &FooterPartition) -> Vec<u64> {
    (0..rows.len())
        .step_by(LEAD_STRIDE)
        .map(|row| rows.key_at(row).map_or(0, lead_of))
        .collect()
}

/// Narrow a key's row search to the strides the sampled leads cannot rule out
fn lead_window(leads: &[u64], key: &[u8], count: usize) -> (usize, usize) {
    let lead = lead_of(key);
    let below = leads.partition_point(|&held| held < lead);
    let past = leads.partition_point(|&held| held <= lead);
    (
        below.saturating_sub(1) * LEAD_STRIDE,
        (past * LEAD_STRIDE).min(count),
    )
}

/// A key run cursor's place, and the row it stands on when it stands on one, its key copied into the caller's buffer
type KeyRunPlace = (Head, Option<RunRow>);

/// Where a cursor opened at a bound first stands in a key run's column, and the key and row it stands on
fn key_run_head(
    run: &KeyRun,
    column: &RunColumn,
    way: Way,
    from: Bound<&[u8]>,
    rows: &mut FooterRows,
    key: &mut Vec<u8>,
) -> Result<KeyRunPlace> {
    let count = column.rows();
    let Some((low, high)) = column.key_range() else {
        return Ok((Head::SPENT, None));
    };
    let row = match (way, from) {
        (Way::Up, Bound::Included(key)) if key > high => None,
        (Way::Up, Bound::Excluded(key)) if key >= high => None,
        (Way::Down, Bound::Included(key)) if key < low => None,
        (Way::Down, Bound::Excluded(key)) if key <= low => None,
        (Way::Up, Bound::Unbounded) => Some(0),
        (Way::Down, Bound::Unbounded) => count.checked_sub(1),
        (Way::Up, Bound::Included(key)) => Some(run.seek(column, key, false, rows)?),
        (Way::Up, Bound::Excluded(key)) => Some(run.seek(column, key, true, rows)?),
        (Way::Down, Bound::Included(key)) => run.seek(column, key, true, rows)?.checked_sub(1),
        (Way::Down, Bound::Excluded(key)) => run.seek(column, key, false, rows)?.checked_sub(1),
    };
    match row {
        Some(row) => key_run_settle(run, column, way, row, rows, key),
        None => Ok((Head::SPENT, None)),
    }
}

/// The first row from `row` on, in the playback's direction, that reads through its footer
#[inline]
fn key_run_settle(
    run: &KeyRun,
    column: &RunColumn,
    way: Way,
    mut row: u64,
    rows: &mut FooterRows,
    key: &mut Vec<u8>,
) -> Result<KeyRunPlace> {
    while row < column.rows() {
        if let Some((found_key, found)) = rows.read(run.pointer(column, row))? {
            key.clear();
            key.extend_from_slice(found_key);
            return Ok((Head::on(key, row as usize), Some(found)));
        }
        row = match way {
            Way::Up => row + 1,
            Way::Down => match row.checked_sub(1) {
                Some(before) => before,
                None => break,
            },
        };
    }
    Ok((Head::SPENT, None))
}

/// Where one cursor stands: the rows sharing its key, and the key's leading bytes
#[derive(Clone, Copy)]
struct Head {
    /// The key's leading sixteen bytes, big-endian, which settle nearly every compare
    lead: u128,

    /// The key's length, which with the lead settles every compare of keys up to sixteen bytes
    len: usize,

    /// The first row sharing the key
    first: usize,
    /// The last row sharing the key, which is the live one since rows keep write order
    last: usize,

    /// Whether the cursor has passed the last row in its direction
    is_spent: bool,
}

impl Head {
    const SPENT: Head = Head {
        lead: 0,
        len: 0,
        first: 0,
        last: 0,
        is_spent: true,
    };

    /// The head of a cursor on a run's only row of a key
    fn on(key: &[u8], row: usize) -> Head {
        Head {
            lead: head_lead(key),
            len: key.len(),
            first: row,
            last: row,
            is_spent: false,
        }
    }

    /// The head of a cursor stepping onto a key's near end, so only the far end needs finding
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
            lead: head_lead(key),
            len: key.len(),
            first,
            last,
            is_spent: false,
        }
    }

    /// Where a cursor opened at a bound first stands
    fn placed(rows: &FooterPartition, leads: &[u64], way: Way, from: Bound<&[u8]>) -> Head {
        let count = rows.len();
        let (Some(low), Some(high)) = (
            rows.key_at(0),
            count.checked_sub(1).and_then(|last| rows.key_at(last)),
        ) else {
            return Head::SPENT;
        };
        let lower_bound = |key: &[u8]| {
            let (start, end) = lead_window(leads, key, count);
            rows.bound_within(key, start, end, false)
        };
        let upper_bound = |key: &[u8]| {
            let (start, end) = lead_window(leads, key, count);
            rows.bound_within(key, start, end, true)
        };
        let row = match (way, from) {
            (Way::Up, Bound::Included(key)) if key > high => None,
            (Way::Up, Bound::Excluded(key)) if key >= high => None,
            (Way::Down, Bound::Included(key)) if key < low => None,
            (Way::Down, Bound::Excluded(key)) if key <= low => None,
            (Way::Up, Bound::Unbounded) => Some(0),
            (Way::Up, Bound::Included(key)) if key <= low => Some(0),
            (Way::Up, Bound::Included(key)) => Some(lower_bound(key)),
            (Way::Up, Bound::Excluded(key)) if key < low => Some(0),
            (Way::Up, Bound::Excluded(key)) => Some(upper_bound(key)),
            (Way::Down, Bound::Unbounded) => Some(count - 1),
            (Way::Down, Bound::Included(key)) if key >= high => Some(count - 1),
            (Way::Down, Bound::Included(key)) => upper_bound(key).checked_sub(1),
            (Way::Down, Bound::Excluded(key)) if key > high => Some(count - 1),
            (Way::Down, Bound::Excluded(key)) => lower_bound(key).checked_sub(1),
        };
        match row {
            Some(row) => Head::at(rows, way, row),
            None => Head::SPENT,
        }
    }
}

/// A head holds this many leading key bytes
const LEAD_BYTES: usize = 16;

/// A key's leading sixteen bytes as an integer, which orders keys whenever two leads differ
#[inline]
fn head_lead(key: &[u8]) -> u128 {
    let mut lead = [0u8; LEAD_BYTES];
    let led = key.len().min(LEAD_BYTES);
    lead[..led].copy_from_slice(&key[..led]);
    u128::from_be_bytes(lead)
}

/// A key's leading eight bytes as an integer, which orders keys whenever two leads differ
fn lead_of(key: &[u8]) -> u64 {
    let mut lead = [0u8; 8];
    let led = key.len().min(8);
    lead[..led].copy_from_slice(&key[..led]);
    u64::from_be_bytes(lead)
}

/// A key run cursor's reader, and the key and row it stands on
struct Keyed {
    rows: FooterRows,
    key: Vec<u8>,
    row: RunRow,
}

/// A loser tree over one cursor on each sealed run within the playback's reach
#[derive(Default)]
struct Sealed {
    /// The opened runs, or nothing until the cursors open
    set: Option<Arc<RunSet>>,
    /// Which run each cursor reads
    at: Vec<u32>,
    /// Where each cursor stands
    heads: Vec<Head>,

    /// Each key run cursor's reader and the key and row it stands on, nothing for a footer cursor
    keyed: Vec<Option<Keyed>>,

    /// Each node's loser, and at 0 the cursor holding the next key
    tree: Vec<usize>,

    /// The winner below each node while the tree is played, kept for the next build
    winners: Vec<usize>,
}

impl Sealed {
    /// Play the cursors placed in `at` and `heads` against each other
    fn build(&mut self, way: Way) {
        let count = self.heads.len();
        self.tree.clear();
        self.tree.resize(count, 0);
        // Leaves stand past the nodes, and a node's children are the winners below it
        let mut winners = std::mem::take(&mut self.winners);
        winners.clear();
        winners.resize(2 * count, 0);
        for leaf in 0..count {
            winners[count + leaf] = leaf;
        }
        for node in (1..count).rev() {
            let (left, right) = (winners[2 * node], winners[2 * node + 1]);
            let (won, lost) = match self.ahead(way, right, left) {
                true => (right, left),
                false => (left, right),
            };
            winners[node] = won;
            self.tree[node] = lost;
        }
        if count > 0 {
            self.tree[0] = winners[1];
        }
        self.winners = winners;
    }

    /// Let go of the runs and forget every cursor, keeping the vectors for the next open
    fn clear(&mut self) {
        self.set = None;
        self.at.clear();
        self.heads.clear();
        self.keyed.clear();
        self.tree.clear();
    }

    /// The front cursor's key, or nothing once every run is spent
    fn key(&self) -> Option<&[u8]> {
        self.key_of(*self.tree.first()?)
    }

    /// Which run a cursor reads
    fn run(&self, at: usize) -> &Run {
        &self.set.as_ref().expect("cursors stand in a set").runs[self.at[at] as usize]
    }

    fn key_of(&self, at: usize) -> Option<&[u8]> {
        let head = &self.heads[at];
        if head.is_spent {
            return None;
        }
        match self.run(at) {
            Run::Footer {
                footer, partition, ..
            } => footer.partitions[*partition].key_at(head.first),
            Run::Keys { .. } => self.keyed[at].as_ref().map(|keyed| keyed.key.as_slice()),
        }
    }

    /// The front cursor, when it stands on this key
    #[inline]
    fn front_on(&self, key: &[u8], lead: u128) -> Option<usize> {
        let at = *self.tree.first()?;
        let head = &self.heads[at];
        // A lead that differs settles it without reading the cursor's key, and so does a short key's length
        if head.is_spent || head.lead != lead {
            return None;
        }
        if key.len() <= LEAD_BYTES {
            return (head.len == key.len()).then_some(at);
        }
        (self.key_of(at) == Some(key)).then_some(at)
    }

    /// Step every cursor standing on a key past it, reading none of their rows
    fn skip(&mut self, way: Way, key: &[u8]) -> Result<()> {
        let lead = head_lead(key);
        while let Some(at) = self.front_on(key, lead) {
            self.step(way, at)?;
        }
        Ok(())
    }

    /// Step every cursor past a key and return its newest row
    fn newest(
        &mut self,
        way: Way,
        key: &[u8],
        stands: impl Fn(SegmentId) -> bool,
    ) -> Result<Option<(SegmentId, FooterRow)>> {
        let mut newest: Option<(SegmentId, FooterRow)> = None;
        self.each_row(way, key, |segment, found| {
            // A rewrite keeps its record's number, so on a tie the row in a standing segment wins
            let is_newer = newest.as_ref().is_none_or(|(held, newest)| {
                newest.lsn < found.lsn
                    || (newest.lsn == found.lsn && !stands(*held) && stands(segment))
            });
            if is_newer {
                newest = Some((segment, found));
            }
        })?;
        Ok(newest)
    }

    /// Step every cursor past a key, handing each of its rows over with the segment its record is in
    fn each_row(
        &mut self,
        way: Way,
        key: &[u8],
        mut each: impl FnMut(SegmentId, FooterRow),
    ) -> Result<()> {
        let lead = head_lead(key);
        while let Some(at) = self.front_on(key, lead) {
            let head = self.heads[at];
            match self.run(at) {
                Run::Footer {
                    segment,
                    footer,
                    partition,
                    ..
                } => {
                    for row in head.first..=head.last {
                        each(*segment, footer.partitions[*partition].row_at(row)?);
                    }
                }
                Run::Keys { .. } => {
                    if let Some(keyed) = self.keyed[at].as_ref() {
                        let row = keyed.row;
                        let found = FooterRow {
                            lsn: row.lsn,
                            offset: row.loc.offset,
                            len: row.loc.len,
                            flags: row.flags,
                        };
                        each(row.loc.segment, found);
                    }
                }
            }
            self.step(way, at)?;
        }
        Ok(())
    }

    /// Move the front cursor off its key and play it up the tree
    fn step(&mut self, way: Way, at: usize) -> Result<()> {
        let head = self.heads[at];
        let next = match way {
            Way::Up => Some(head.last + 1),
            Way::Down => head.first.checked_sub(1),
        };
        let Sealed {
            set,
            at: runs_at,
            heads,
            keyed,
            ..
        } = self;
        let set = set.as_ref().expect("cursors stand in a set");
        let run = &set.runs[runs_at[at] as usize];
        heads[at] = match (next, run) {
            (None, _) => Head::SPENT,
            (
                Some(row),
                Run::Footer {
                    footer, partition, ..
                },
            ) => Head::at(&footer.partitions[*partition], way, row),
            (Some(row), Run::Keys { run, column, .. }) => {
                let column = &run.columns()[*column];
                let Some(keyed) = keyed[at].as_mut() else {
                    return Ok(());
                };
                let (head, standing) =
                    key_run_settle(run, column, way, row as u64, &mut keyed.rows, &mut keyed.key)?;
                if let Some(row) = standing {
                    keyed.row = row;
                }
                head
            }
        };
        self.replay(way, at);
        Ok(())
    }

    /// Play the front cursor's leaf up to the root, the one leaf that may move
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

    /// Whether one cursor's key comes before another's, a spent cursor sorting behind every live one
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
        // Two keys within the lead differ only in length, the shorter sorting first
        if one.len <= LEAD_BYTES && other.len <= LEAD_BYTES {
            return match way {
                Way::Up => one.len < other.len,
                Way::Down => one.len > other.len,
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

    /// The cursors on every sealed run within reach
    sealed: Sealed,

    /// The sealed set the cursors were opened against, so a change reopens them
    generation: Option<u64>,

    /// The map's half of the merge, kept so a page does not allocate one
    resident: KeyPage,
}

/// A finished playback hands these vectors to the next playback on its thread
pub struct CursorBuffers {
    sealed: Sealed,
    resident: KeyPage,
}

impl Default for CursorBuffers {
    fn default() -> CursorBuffers {
        CursorBuffers {
            sealed: Sealed::default(),
            resident: KeyPage::merging(),
        }
    }
}

impl PlaybackCursor {
    /// A playback of one column from a bound, with nothing open yet
    pub fn new(column: ColumnId, way: Way, from: Bound<&[u8]>) -> Result<PlaybackCursor> {
        PlaybackCursor::with_buffers(column, way, from, CursorBuffers::default())
    }

    /// The same playback, reusing the vectors of a finished one
    pub fn with_buffers(
        column: ColumnId,
        way: Way,
        from: Bound<&[u8]>,
        buffers: CursorBuffers,
    ) -> Result<PlaybackCursor> {
        let CursorBuffers {
            mut sealed,
            mut resident,
        } = buffers;
        sealed.clear();
        resident.clear();
        Ok(PlaybackCursor {
            column,
            way,
            at: Some(owned_bound(from)?),
            sealed,
            generation: None,
            resident,
        })
    }

    /// Empty this playback's vectors and hand them to the next one
    pub fn into_buffers(self) -> CursorBuffers {
        let PlaybackCursor {
            mut sealed,
            mut resident,
            ..
        } = self;
        sealed.clear();
        resident.clear();
        CursorBuffers { sealed, resident }
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
    /// What a column with nothing sealed answers with.
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
        let generation = paged.generation();
        if self.generation == Some(generation) {
            return Ok(());
        }
        let Some(at) = self.at.as_ref() else {
            return Ok(());
        };
        paged.open_sealed(self.way, borrowed_bound(at), &mut self.sealed)?;
        self.generation = Some(generation);
        Ok(())
    }
}

/// A page fill tries this many times when a segment a key run points into retires under it
const REFILLS: usize = 8;

/// Fill a page with one merged run of a paged column's keys, starting again when a segment retires under it
pub fn merged_page(
    paged: &Paged<'_>,
    playback: &mut PlaybackCursor,
    limit: usize,
    out: &mut KeyPage,
) -> Result<()> {
    let mut refills = 0;
    loop {
        match fill_merged_page(paged, playback, limit, out) {
            Err(error) if is_vanished(&error) && refills < REFILLS => {
                refills += 1;
                playback.generation = None;
                std::thread::yield_now();
            }
            outcome => return outcome,
        }
    }
}

/// One try at a merged page
///
/// The map's own page is taken first, since it both supplies keys and says how far
/// the run may reach. What comes back is a contiguous run in playback order.
fn fill_merged_page(
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
    // A hand-over moves a key from the map to a footer and compaction moves one back, so the map page stands only if the sealed set held still around it
    loop {
        playback.open(paged)?;
        let Some(at) = playback.at.as_ref() else {
            return Ok(());
        };
        resident_page(
            index,
            way,
            borrowed_bound(at),
            limit,
            &mut playback.resident,
        );
        if playback.generation == Some(paged.generation()) {
            break;
        }
    }

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
    // The next page resumes past the last key decided, grave or not, copied since a peek reuses `want`
    let mut resume = [0u8; MAX_KEY_LEN];
    let mut resume_len = 0usize;
    let mut exhausted = false;

    while out.len() < limit {
        let Some(key) = next_key(resident.key_ref(taken), sealed, way, &mut want) else {
            // A full map page may have keys past its edge, so only a short one ends the playback
            exhausted = resident.len() < limit;
            break;
        };
        if edge.is_some_and(|edge| is_ahead(way, edge, key)) {
            break;
        }
        resume[..key.len()].copy_from_slice(key);
        resume_len = key.len();

        // The map's entry or grave wins, so the sealed rows for this key are skipped unread
        if resident.key_ref(taken) == Some(key) {
            sealed.skip(way, key)?;
            let found = resident
                .found_at(taken)
                .expect("the merge's own page carries its entries");
            taken += 1;
            if !found.is_grave() && !index.is_covered_key(key, found.lsn) {
                out.push(key, found);
            }
            continue;
        }
        // The page is read under the publish barrier, so a key missing from it is not in the map
        let Some((segment, found)) =
            sealed.newest(way, key, |segment| paged.sealed.holds(segment))?
        else {
            continue;
        };
        if found.is_tombstone() || found.is_range_tombstone() {
            continue;
        }
        if index.is_covered_key(key, found.lsn) {
            continue;
        }
        let stamp = sealed
            .set
            .as_ref()
            .map_or(SegmentIncarnation::NONE, |set| set.stamp_of(segment));
        out.push(
            key,
            Entry::new(Loc::new(segment, found.offset, found.len), found.lsn).stamped(stamp),
        );
    }

    playback.at = match exhausted {
        true => None,
        false => Some(Bound::Excluded(KeyBytes::new(&resume[..resume_len])?)),
    };
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

/// Every standing data row a cover has taken, the budget counting keys examined so a run of skips still moves
pub fn release_rows(
    paged: &Paged<'_>,
    playback: &mut PlaybackCursor,
    until: Option<&[u8]>,
    below: Lsn,
    limit: usize,
    out: &mut Vec<(KeyBytes, Loc)>,
) -> Result<ReleaseRun> {
    let mut refills = 0;
    loop {
        match fill_release_rows(paged, playback, until, below, limit, out) {
            Err(error) if is_vanished(&error) && refills < REFILLS => {
                refills += 1;
                playback.generation = None;
                std::thread::yield_now();
            }
            outcome => return outcome,
        }
    }
}

/// One try at a release run
fn fill_release_rows(
    paged: &Paged<'_>,
    playback: &mut PlaybackCursor,
    until: Option<&[u8]>,
    below: Lsn,
    limit: usize,
    out: &mut Vec<(KeyBytes, Loc)>,
) -> Result<ReleaseRun> {
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
        let taken = KeyBytes::new(key)?;
        sealed.each_row(way, key, |segment, found| {
            if found.lsn < below && found.flags.is_data() && paged.sealed.holds(segment) {
                out.push((taken.clone(), Loc::new(segment, found.offset, found.len)));
            }
        })?;
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
    fn open_sealed(&self, way: Way, from: Bound<&[u8]>, sealed: &mut Sealed) -> Result<()> {
        let generation = self.generation();
        let set = self.runs.current(generation, || {
            let mut runs = Vec::new();
            // Key runs stand in for the segments they cover, so those footers stay shut
            let covered = self.key_runs.covered();
            // Every segment a row can name is stamped now, so a read later knows whether its place still stands
            let mut stamps: Vec<(SegmentId, SegmentIncarnation)> = covered
                .iter()
                .map(|segment| (*segment, self.segments.incarnation_of(*segment)))
                .collect();
            for segment in self.sealed.spanning(None, None) {
                if covered.contains(&segment) {
                    continue;
                }
                stamps.push((segment, self.segments.incarnation_of(segment)));
                // A segment retired since the range read is gone, its live rows already copied on
                let Some(footer) = self.footers.footer(segment)? else {
                    continue;
                };
                let Some(partition) = footer
                    .partitions
                    .iter()
                    .position(|rows| rows.column == self.column)
                else {
                    continue;
                };
                let leads = sampled_leads(&footer.partitions[partition]);
                runs.push(Run::Footer {
                    segment,
                    footer,
                    partition,
                    leads,
                });
            }
            for run in self.key_runs.runs() {
                if let Some(column) = run
                    .columns()
                    .iter()
                    .position(|held| held.column == self.column)
                {
                    // A segment retired before the open has its live records in the map or a newer footer
                    let views = Arc::new(RunViews::new(&run, &|segment| self.sealed.holds(segment)));
                    runs.push(Run::Keys { run, column, views });
                }
            }
            stamps.sort_unstable_by_key(|(segment, _)| *segment);
            stamps.dedup_by_key(|(segment, _)| *segment);
            Ok((runs, stamps))
        })?;

        sealed.clear();
        for (index, run) in set.runs.iter().enumerate() {
            let (head, keyed) = match run {
                Run::Footer {
                    footer,
                    partition,
                    leads,
                    ..
                } => (Head::placed(&footer.partitions[*partition], leads, way, from), None),
                Run::Keys { run, column, views } => {
                    let column = &run.columns()[*column];
                    let mut rows = FooterRows::sharing(
                        Arc::clone(self.footers),
                        Arc::clone(run),
                        self.column,
                        Arc::clone(views),
                    );
                    let mut key = Vec::new();
                    let (head, standing) = key_run_head(run, column, way, from, &mut rows, &mut key)?;
                    let keyed = standing.map(|row| Keyed { rows, key, row });
                    (head, keyed)
                }
            };
            if !head.is_spent {
                sealed.at.push(index as u32);
                sealed.heads.push(head);
                sealed.keyed.push(keyed);
            }
        }
        sealed.set = Some(set);
        sealed.build(way);
        Ok(())
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

    use crate::format::column::{Codec, ColumnSpec, RecordKey};
    use crate::format::footer::FooterEntry;
    use crate::format::lsn::Lsn;
    use crate::format::record::Flags;
    use crate::index::column::never_shadowed;

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
        footers: Arc<CountingFooters>,

        /// The same footers as the walk asks for them
        source: Arc<dyn FooterSource>,
        runs: WalkRuns,
        key_runs: KeyRunSet,
        segments: SegmentTable,
    }

    impl Fixture {
        fn new(footers: &[(SegmentId, &[u8])]) -> Fixture {
            let footers = Arc::new(CountingFooters {
                footers: footers
                    .iter()
                    .map(|(segment, keys)| (*segment, footer(keys)))
                    .collect(),
                opened: AtomicUsize::new(0),
            });
            let source: Arc<dyn FooterSource> = Arc::clone(&footers) as Arc<dyn FooterSource>;
            Fixture {
                index: ColumnIndex::new(&SPEC).expect("column"),
                sealed: SealedRanges::new(),
                footers,
                source,
                runs: WalkRuns::default(),
                key_runs: KeyRunSet::default(),
                segments: SegmentTable::new(),
            }
        }

        fn paged(&self) -> Paged<'_> {
            Paged {
                column: COLUMN,
                index: &self.index,
                sealed: &self.sealed,
                footers: &self.source,
                runs: &self.runs,
                key_runs: &self.key_runs,
                segments: &self.segments,
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

        /// Delete a key in the map, leaving a grave over whatever a sealed run holds for it
        fn bury(&self, byte: u8) {
            let segments = SegmentTable::new();
            self.index.remove(
                key(byte).as_slice(),
                Lsn(1000 + u64::from(byte)),
                Loc::new(SegmentId(9), 0, 0),
                &segments,
                &never_shadowed,
            );
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

    // a cursor opened at a bound lands on the same row whichever stride the bound falls in
    #[test]
    fn a_bound_lands_inside_its_stride() {
        let keys: Vec<u8> = (1..=200).collect();
        let fixture = Fixture::new(&[(SegmentId(1), &keys)]);
        fixture.seal(SegmentId(1), 1, 200);
        for from in [0u8, 1, 2, 63, 64, 65, 127, 128, 129, 199, 200] {
            let bound = [from; 8];
            let mut up = PlaybackCursor::new(COLUMN, Way::Up, Bound::Included(&bound)).expect("up");
            let want: Vec<u8> = (from.max(1)..=200).collect();
            assert_eq!(fixture.drain(&mut up), want, "up from {from}");
            let mut past =
                PlaybackCursor::new(COLUMN, Way::Up, Bound::Excluded(&bound)).expect("past");
            let want: Vec<u8> = (from + 1..=200).collect();
            assert_eq!(fixture.drain(&mut past), want, "up past {from}");
            let mut down =
                PlaybackCursor::new(COLUMN, Way::Down, Bound::Included(&bound)).expect("down");
            let want: Vec<u8> = (1..=from.min(200)).rev().collect();
            assert_eq!(fixture.drain(&mut down), want, "down from {from}");
        }
    }

    // a key the map deleted stays out of the merge, whatever a sealed run holds for it
    #[test]
    fn a_grave_hides_a_sealed_row() {
        let fixture = Fixture::new(&[(SegmentId(1), &[1, 2, 3, 4])]);
        fixture.seal(SegmentId(1), 1, 4);
        fixture.bury(2);

        assert_eq!(fixture.drain(&mut playback()), vec![1, 3, 4]);
    }

    // a page of graves alone leaves the playback open for the keys past it
    #[test]
    fn a_page_of_graves_does_not_end_the_playback() {
        let fixture = Fixture::new(&[(SegmentId(1), &[1, 2, 3, 4])]);
        fixture.seal(SegmentId(1), 1, 4);
        fixture.bury(1);
        fixture.bury(2);

        let mut playback = playback();
        let mut page = KeyPage::with_lens();
        assert_eq!(
            fixture.page(&mut playback, &mut page),
            Vec::<u8>::new(),
            "a page of graves is empty"
        );
        assert!(
            !playback.is_done(),
            "the playback stays open past a page of graves"
        );
        assert_eq!(fixture.drain(&mut playback), vec![3, 4]);
    }

    // a page that stops at its edge resumes before the sealed key it peeked, whose grave comes next
    #[test]
    fn a_page_edge_resumes_before_the_key_it_peeked() {
        let fixture = Fixture::new(&[(SegmentId(1), &[1, 2, 3, 4])]);
        fixture.seal(SegmentId(1), 1, 4);
        fixture.bury(1);
        fixture.bury(2);
        fixture.bury(3);

        assert_eq!(fixture.drain(&mut playback()), vec![4]);
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
