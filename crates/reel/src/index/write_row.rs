//! One row of an open segment

use reel_core::units::{ByteCount, FileOffset, WritePlace};

use crate::index::open_slots::OpenSlot;
use crate::row_kind::RowKind;
use crate::segment::segment_tail::SegmentNumber;

/// A row's place in the write order and where its record sits
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteRow {
    pub place: WritePlace,
    pub offset: FileOffset,
    pub length: ByteCount,
    pub kind: RowKind,
    pub slot: OpenSlot,
    pub segment: SegmentNumber,
    pub has_newer_sealed: bool,
}

/// What the write index says about a key
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteLookup {
    pub row: Option<WriteRow>,
    pub should_ask_footers: bool,
}
