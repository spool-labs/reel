//! Writing the resident index down at a cue, so the next open reads it
//!
//! A cue seals every tail, so the volume below it is a set of immutable files and
//! the index over them is a value rather than a moving target. Only rows pointing
//! into segments the cue closed are written down; an open finds the rest for
//! itself. The walk drives the writer a page of keys at a time, since on a volume
//! worth running this on the whole value would be tens of gigabytes.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::Path;

use crate::compaction::pressure::PassSeat;
use crate::error::{ReelError, Result};
use crate::format::column::{ColumnId, KeyWidth};
use crate::format::loc::SegmentId;
use crate::format::lsn::Lsn;
use crate::index::page::KeyPage;
use crate::index::persisted::{
    PersistedColumn, PersistedIndex, PersistedSegment, PersistedWriter, VARIABLE_WIDTH,
};
use crate::reel::segment_number;

use super::{read_only, ReelStore};

/// Keys one page of the walk copies out of a column at a time
///
/// The walk takes the publish barrier once a page, and a shard with no order to read
/// off sorts itself for every page taken out of it, so this pays that once per shard.
const CAPTURE_PAGE: usize = 16 * 1024;

/// Sweep passes a checkpoint drives before it gives the volume back
///
/// Past this the volume is taking range deletes faster than the sweep drains them,
/// which is a refusal rather than a loop with no end.
const SWEEP_PASSES: usize = 256;

/// What one index checkpoint stood at, and how much work it saves an open
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexCheckpoint {
    /// Sequence number the index was written down as of
    pub at: Lsn,

    /// Segments the file speaks for, which is the set an open may skip reading
    pub segments: usize,

    /// Live keys written down, which is the per-key work the sweep would have paid
    pub keys: u64,
}

impl ReelStore {
    /// Write the resident index into the volume root, standing at a cue
    ///
    /// The next open reads it back instead of sweeping every footer, taking its rows
    /// for each segment still standing at the length recorded here. Nothing schedules
    /// this; a caller that wants the fast open runs it. Refused on a read-only volume
    /// and on a paging one.
    pub fn checkpoint_index(&self) -> Result<IndexCheckpoint> {
        if self.is_read_only {
            return Err(read_only());
        }
        if !self.keeps_index() {
            return Err(ReelError::Rejected(
                "this volume pages its sealed keys out to their footers, so there is \
                 no resident index to write down"
                    .to_string(),
            ));
        }
        // A standing cover has taken records the counters still book live, so the
        // sweep runs first and the stamps below name what the volume really holds.
        self.settle_covers()?;

        let cue = self.cue()?;
        // Read after the seal, so it is the line between what the cue can see and what
        // the volume writes next.
        let boundary = self.reel.shared().peek_segment();
        let taken = self.write_index(&self.root, cue.at(), boundary)?;
        drop(cue);
        Ok(taken)
    }

    /// Whether this volume keeps an index a later open can read back
    pub(super) fn keeps_index(&self) -> bool {
        !self.config.index.pages()
    }

    /// Walk the index over the segments a cue closed into a file under a root
    ///
    /// Two things have to hold for a row to mean what it says: no compaction pass may
    /// run across the walk, since a pass repoints an entry at a copy that is not
    /// durable yet, and nothing may be drawn and unpublished, since such a record is
    /// below the cue and outside the index. Both are refusals rather than waits.
    pub(super) fn write_index(
        &self,
        root: &Path,
        at: Lsn,
        boundary: SegmentId,
    ) -> Result<IndexCheckpoint> {
        let _seats = self.claim_compaction()?;
        let shared = self.reel.shared();
        if !shared.nothing_unpublished() {
            return Err(ReelError::Rejected(
                "a sequence number drawn before the cue has not published yet, so the \
                 index does not yet stand for everything below the cue"
                    .to_string(),
            ));
        }

        let closed = self.closed_segments(boundary)?;
        let head = self.head_of(at, &closed);
        let mut writer = PersistedWriter::try_new(&self.driver, root, &head)?;
        let walked = (|| -> Result<u64> {
            for column in &head.columns {
                writer.open_column(&self.driver, column.column, column.key_width)?;
                self.capture_column(&mut writer, column.column, &closed)?;
            }
            Ok(writer.keys())
        })();
        let keys = match walked {
            Ok(keys) => keys,
            Err(error) => {
                writer.abandon(&self.driver);
                return Err(error);
            }
        };
        writer.publish(&self.driver)?;
        Ok(IndexCheckpoint {
            at,
            segments: head.segments.len(),
            keys,
        })
    }

    /// The head of the file: what it stands at, its stamps, and its column table
    ///
    /// The walk does not quiesce writers, so a racing write can leave the dead
    /// accounting off by its own span. That skews the compaction trigger, never an
    /// answer: every row still names a durable record, and the scrub's recount
    /// settles the counters.
    fn head_of(&self, at: Lsn, closed: &BTreeMap<SegmentId, u64>) -> PersistedIndex {
        // One pass under the counter table's lock rather than two acquisitions of it
        // per segment.
        let stamps = self.index.segment_stamps();
        let mut segments = Vec::with_capacity(closed.len());
        for (segment, len) in closed {
            let stamp = stamps.get(segment).copied().unwrap_or_default();
            segments.push(PersistedSegment {
                segment: *segment,
                len: *len,
                dead: stamp.bytes.dead,
                held: stamp.bytes.held,
                held_lsn: stamp.bytes.held_lsn,
                min_lsn: stamp.min_lsn,
            });
        }

        let mut columns = Vec::with_capacity(self.index.columns().len());
        for spec in self.index.columns() {
            columns.push(PersistedColumn {
                column: spec.id,
                key_width: match spec.key_width {
                    KeyWidth::Fixed(width) => width,
                    KeyWidth::Variable => VARIABLE_WIDTH,
                },
            });
        }
        PersistedIndex {
            at,
            segments,
            columns,
        }
    }

    /// Write down every live key of one column that resolves into a closed segment
    ///
    /// A key resolving anywhere else is one an open finds for itself.
    fn capture_column(
        &self,
        writer: &mut PersistedWriter,
        column: ColumnId,
        closed: &BTreeMap<SegmentId, u64>,
    ) -> Result<()> {
        let mut page = KeyPage::entries_only();
        // One buffer for the resume bound rather than one per page.
        let mut from: Vec<u8> = Vec::new();
        let mut is_first = true;
        loop {
            let start = match is_first {
                true => Bound::Unbounded,
                false => Bound::Excluded(from.as_slice()),
            };
            self.index.page(column, start, CAPTURE_PAGE, &mut page)?;
            if page.is_empty() {
                return Ok(());
            }
            for at in 0..page.len() {
                let (Some(key), Some(entry)) = (page.key_ref(at), page.found_at(at)) else {
                    continue;
                };
                if !closed.contains_key(&entry.loc.segment) {
                    continue;
                }
                writer.row(&self.driver, key, entry.lsn, entry.loc)?;
            }
            if page.len() < CAPTURE_PAGE {
                return Ok(());
            }
            let last = page.key_ref(page.len() - 1).unwrap_or_default();
            from.clear();
            from.extend_from_slice(last);
            is_first = false;
        }
    }

    /// The sealed segments below the boundary, each with the length that proves it
    ///
    /// Read from the directories rather than the index's segment table, which books
    /// footprint per record while a segment whose records are all dead still has a
    /// file. Settled is the question asked of each, since it answers sealed, synced
    /// and past every hold in one.
    fn closed_segments(&self, boundary: SegmentId) -> Result<BTreeMap<SegmentId, u64>> {
        let shared = self.reel.shared();
        let mut closed = BTreeMap::new();
        for root in shared.volumes.roots() {
            for entry in self.driver.list_or_empty(root)? {
                let Some(number) = segment_number(&entry.name) else {
                    continue;
                };
                let segment = SegmentId(number);
                if number < boundary.as_u32() && shared.is_settled(segment) {
                    closed.insert(segment, entry.len);
                }
            }
        }
        Ok(closed)
    }

    /// Drive the lazy cover sweep to its end, or refuse a volume outrunning it
    fn settle_covers(&self) -> Result<()> {
        for _ in 0..SWEEP_PASSES {
            if !self.sweep_covers()? {
                return Ok(());
            }
        }
        Err(ReelError::Rejected(
            "range deletes are still owed their sweep, so an index written now would \
             name records the counters have not settled"
                .to_string(),
        ))
    }

    /// Take every place on the compaction plane, so no pass runs while the walk does
    fn claim_compaction(&self) -> Result<Vec<PassSeat<'_>>> {
        let width = self.compaction_plane.width();
        let mut seats = Vec::with_capacity(width);
        while let Some(seat) = self.compaction_plane.enter() {
            seats.push(seat);
            if seats.len() == width {
                return Ok(seats);
            }
        }
        Err(ReelError::Rejected(
            "a compaction pass is rewriting segments, so an index written now would \
             vouch for a segment it is about to retire"
                .to_string(),
        ))
    }
}
