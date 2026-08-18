//! Resident index pointers into segment files

/// Identity of a segment file within one reel, numbered monotonically
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SegmentId(pub u32);

impl crate::index::tbtreemap::TreeKey for SegmentId {
    type Probe = SegmentId;
    type Window = crate::index::tbtreemap::Whole;

    fn filler() -> SegmentId {
        SegmentId(0)
    }

    /// A segment number is its own lead, so a node is searched in one compare
    fn head(probe: &SegmentId) -> u64 {
        probe.0 as u64
    }

    fn separator(_left: &SegmentId, right: &SegmentId) -> (SegmentId, bool) {
        (*right, false)
    }
}

impl SegmentId {
    /// Read the underlying segment number
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

/// One life of a segment in the live table, gone when its space goes
///
/// Retiring a segment or reusing its space must drop or advance the incarnation
/// before the bytes change, and a stamp is never reissued, so an index entry
/// whose stamp is current still names the record it was made from.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SegmentIncarnation(pub u32);

impl SegmentIncarnation {
    /// The stamp of no live segment, which no issued incarnation ever equals
    pub const NONE: SegmentIncarnation = SegmentIncarnation(0);

    /// Whether this stamp names no live segment
    pub fn is_none(self) -> bool {
        self == SegmentIncarnation::NONE
    }
}

/// A resident pointer to one record: which segment, where in it, and how long
///
/// The offset and length stay raw so the index packs densely.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Loc {
    /// Segment file the record lives in
    pub segment: SegmentId,

    /// Byte offset of the record header within the segment
    pub offset: u32,

    /// Payload length in bytes
    pub len: u32,
}

impl Loc {
    /// A pointer to a record header at an offset with a payload length
    pub fn new(segment: SegmentId, offset: u32, len: u32) -> Loc {
        Loc {
            segment,
            offset,
            len,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // a pointer keeps the fields it was built from
    #[test]
    fn keeps_fields() {
        let loc = Loc::new(SegmentId(7), 4096, 1600);

        assert_eq!(loc.segment, SegmentId(7));
        assert_eq!(loc.offset, 4096);
        assert_eq!(loc.len, 1600);
    }
}
