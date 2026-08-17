//! What the resident index resolves a key to

use crate::format::column::{KeyBytes, RecordKey};
use crate::format::loc::{Loc, SegmentId, SegmentIncarnation};
use crate::format::lsn::Lsn;
use crate::format::record::HEADER_LEN;

/// Segment number no record can sit in, which is what tells a grave from an entry
///
/// Segments are numbered from one, so an entry pointing here points nowhere.
const NO_SEGMENT: SegmentId = SegmentId(0);

/// One resident index entry: where the live record is and which version it is
///
/// Twenty-four bytes, every one of them read on the path that resolves a key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Entry {
    /// Location of the live record within the reel
    pub loc: Loc,

    /// Sequence number of the version this entry resolves to
    pub lsn: Lsn,

    /// The life of the segment the location was learned under
    pub incarnation: SegmentIncarnation,
}

impl Default for Entry {
    /// A place in a node that has not been filled yet
    ///
    /// Deliberately the entry no read can mistake for a live one.
    fn default() -> Entry {
        Entry::new(Loc::new(SegmentId(0), 0, 0), Lsn::NONE)
    }
}

impl Entry {
    /// An entry pointing at a record written under a sequence number
    pub fn new(loc: Loc, lsn: Lsn) -> Entry {
        Entry {
            loc,
            lsn,
            incarnation: SegmentIncarnation::NONE,
        }
    }

    /// The same entry wearing the segment life it was resolved under
    pub fn stamped(self, incarnation: SegmentIncarnation) -> Entry {
        Entry {
            incarnation,
            ..self
        }
    }

    /// Repoint the entry at a rewritten copy of the same record
    ///
    /// The copy sits in another segment, so the stamp is the destination's and
    /// never the one this entry was wearing.
    pub fn moved_to(&self, loc: Loc, incarnation: SegmentIncarnation) -> Entry {
        Entry {
            loc,
            lsn: self.lsn,
            incarnation,
        }
    }

    /// A tombstone holding a key's place so a late older put cannot take it
    ///
    /// Writers publish in whatever order they finish, so the sequence number left
    /// on the key is what refuses a put that was drawn earlier and arrived later.
    /// This form remembers nothing about where the tombstone landed, which on a
    /// paged column is a grave nothing can ever retire.
    pub fn grave(lsn: Lsn) -> Entry {
        Entry::grave_from(lsn, NO_SEGMENT)
    }

    /// A grave that remembers which segment its tombstone record landed in
    ///
    /// A sealed footer still names the deleted record and is never told about the
    /// delete, so what ends a paged grave's job is its own tombstone reaching a
    /// footer. The segment rides in the offset a grave has no use for.
    pub fn grave_from(lsn: Lsn, tombstone: SegmentId) -> Entry {
        Entry::new(Loc::new(NO_SEGMENT, tombstone.as_u32(), 0), lsn)
    }

    /// The segment holding the tombstone that left this grave, if it named one
    ///
    /// Nothing comes back for a grave no tombstone record stands behind, which is
    /// one a paged column can never retire.
    pub fn grave_origin(&self) -> Option<SegmentId> {
        match self.is_grave() && self.loc.offset != NO_SEGMENT.as_u32() {
            true => Some(SegmentId(self.loc.offset)),
            false => None,
        }
    }

    /// Whether this holds a key's place rather than naming a record
    pub fn is_grave(&self) -> bool {
        self.loc.segment == NO_SEGMENT
    }

    /// On-disk footprint of the record, its header, key, and payload together
    pub fn span(&self, key_width: u16) -> u64 {
        span_of(key_width, self.loc.len)
    }
}

/// On-disk footprint of a record with this key width and payload length
pub fn span_of(key_width: u16, len: u32) -> u64 {
    HEADER_LEN as u64 + u64::from(key_width) + u64::from(len)
}

/// A half-open key range one tombstone covers, and the version it covers it at
///
/// A range tombstone is the one record whose effect is not settled by comparing
/// sequence numbers on a single key, so anything replaying the log out of order
/// keeps it and tests later records against it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeCover {
    /// Column and inclusive start of the range
    pub start: RecordKey,

    /// Exclusive end, or nothing when the range runs to the top of the column
    pub end: Option<KeyBytes>,

    /// Sequence number the delete was drawn at
    pub lsn: Lsn,
}

impl RangeCover {
    /// Whether this tombstone covers a key and was drawn after it was written
    pub fn covers(&self, key: &RecordKey, lsn: Lsn) -> bool {
        if key.column != self.start.column || lsn >= self.lsn {
            return false;
        }
        if key.key < self.start.key {
            return false;
        }
        match &self.end {
            Some(end) => key.key < *end,
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::format::column::ColumnId;
    use crate::format::loc::SegmentId;

    // a range covers only its own column, its own span, and older versions
    #[test]
    fn range_covers_its_own() {
        let column = ColumnId(1);
        let key = |byte: u8| RecordKey::from_bytes(column, &[byte; 8]).expect("key");
        let cover = RangeCover {
            start: key(2),
            end: Some(KeyBytes::new(&[4u8; 8]).expect("end")),
            lsn: Lsn(10),
        };

        assert!(cover.covers(&key(3), Lsn(9)));
        assert!(!cover.covers(&key(3), Lsn(10)), "a newer key survives");
        assert!(!cover.covers(&key(1), Lsn(9)), "below the start");
        assert!(!cover.covers(&key(5), Lsn(9)), "past the end");

        let other = RecordKey::from_bytes(ColumnId(2), &[3u8; 8]).expect("key");
        assert!(!cover.covers(&other, Lsn(9)), "another column");
    }

    // an unbounded range runs to the top of its column
    #[test]
    fn unbounded_range_covers_upward() {
        let column = ColumnId(1);
        let cover = RangeCover {
            start: RecordKey::from_bytes(column, &[2u8; 8]).expect("key"),
            end: None,
            lsn: Lsn(10),
        };

        assert!(cover.covers(
            &RecordKey::from_bytes(column, &[0xff; 8]).expect("key"),
            Lsn(1)
        ));
    }

    // an entry is the pointer, the version and the stamp, with no padding, and the
    // width is held to the number because every resident key pays it
    #[test]
    fn entry_width() {
        assert_eq!(std::mem::size_of::<Entry>(), 24);
    }

    // a rewritten copy keeps its version and takes the destination's stamp
    #[test]
    fn moved_keeps_its_version() {
        let entry = Entry::new(Loc::new(SegmentId(1), 0, 2), Lsn(5)).stamped(SegmentIncarnation(3));

        let moved = entry.moved_to(Loc::new(SegmentId(9), 64, 2), SegmentIncarnation(7));

        assert_eq!(moved.lsn, Lsn(5));
        assert_eq!(moved.loc.segment, SegmentId(9));
        assert_eq!(moved.incarnation, SegmentIncarnation(7));
    }

    // a fresh entry wears no stamp until one is put on it
    #[test]
    fn stamp_starts_empty() {
        let entry = Entry::new(Loc::new(SegmentId(1), 0, 8), Lsn(1));

        assert!(entry.incarnation.is_none());
        assert_eq!(
            entry.stamped(SegmentIncarnation(4)).incarnation,
            SegmentIncarnation(4)
        );
    }

    // a record's footprint counts its header, its key, and its payload
    #[test]
    fn span_counts_the_key() {
        let entry = Entry::new(Loc::new(SegmentId(1), 0, 1600), Lsn(1));

        assert_eq!(entry.span(34), HEADER_LEN as u64 + 34 + 1600);
        assert_eq!(span_of(0, 0), HEADER_LEN as u64);
    }
}
