//! What the resident index resolves a key to

use crate::format::column::{KeyBytes, RecordKey};
use crate::format::loc::{Loc, SegmentId, SegmentIncarnation};
use crate::format::lsn::Lsn;
use crate::format::record::RecordLayout;

/// No record sits in segment zero, so an entry pointing there is a grave
const NO_SEGMENT: SegmentId = SegmentId(0);

/// One resident index entry: where the live record is and which version it is
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Entry {
    /// Location of the live record within the reel
    pub loc: Loc,

    /// Sequence number of the version this entry resolves to
    pub lsn: Lsn,

    /// The segment's incarnation when the location was learned
    pub incarnation: SegmentIncarnation,
}

impl Default for Entry {
    /// A filler for an unused node slot, which no read can take for a live entry
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

    /// The same entry stamped with the segment incarnation it was resolved under
    pub fn stamped(self, incarnation: SegmentIncarnation) -> Entry {
        Entry {
            incarnation,
            ..self
        }
    }

    /// Repoint the entry at a rewritten copy of the same record, with the destination's stamp
    pub fn moved_to(&self, loc: Loc, incarnation: SegmentIncarnation) -> Entry {
        Entry {
            loc,
            lsn: self.lsn,
            incarnation,
        }
    }

    /// A grave holding a key's place against a late older put, with no tombstone segment
    pub fn grave(lsn: Lsn) -> Entry {
        Entry::grave_from(lsn, NO_SEGMENT)
    }

    /// A grave that remembers its tombstone's segment, kept in the unused offset
    pub fn grave_from(lsn: Lsn, tombstone: SegmentId) -> Entry {
        Entry::new(Loc::new(NO_SEGMENT, tombstone.as_u32(), 0), lsn)
    }

    /// The segment holding the tombstone behind this grave, if it has one
    pub fn grave_origin(&self) -> Option<SegmentId> {
        match self.is_grave() && self.loc.offset != NO_SEGMENT.as_u32() {
            true => Some(SegmentId(self.loc.offset)),
            false => None,
        }
    }

    /// Whether this holds a key's place with no record behind it
    pub fn is_grave(&self) -> bool {
        self.loc.segment == NO_SEGMENT
    }

    /// On-disk footprint of the record, keyless when small
    pub fn span(&self, key_width: u16) -> u64 {
        span_of(key_width, self.loc.len)
    }
}

/// On-disk footprint of a record with this key width and payload length, keyless when small
pub fn span_of(key_width: u16, len: u32) -> u64 {
    RecordLayout::KEYLESS.prefix_len(key_width as usize, len) as u64 + u64::from(len)
}

/// A range tombstone's half-open key range and the lsn it was drawn at
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeCover {
    /// Column and inclusive start of the range
    pub start: RecordKey,

    /// Exclusive end, or nothing when the range runs to the top of the column
    pub end: Option<KeyBytes>,

    /// The delete's sequence number
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

    // an entry is a pointer, a version and a stamp in 24 bytes with no padding
    #[test]
    fn entry_width() {
        assert_eq!(std::mem::size_of::<Entry>(), 24);
    }
}
