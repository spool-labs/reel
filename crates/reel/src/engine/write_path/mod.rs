//! Puts, deletes and syncs

use std::io::{Error, ErrorKind};
use std::sync::Mutex;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, DeathMark, Nanoseconds, RowCount};

use crate::engine::open_segment::{
    OpenSegment, RESERVE_STEP_BYTES, RowQueue,
};
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::io::Io;
use crate::record::codec::Codec;

/// In-flight writes an open segment takes before the volume opens another
pub const WRITES_PER_OPEN: u32 = 2;
/// The job asks for a segment that takes writes as they come, with nothing claimed yet
pub const SPARE_REQUEST: ClaimRequest = ClaimRequest {
    bytes: ByteCount(0),
    rows: RowCount(0),
    is_alone: false,
    has_records: true,
    covers: 0,
    mark: None,
    has_deletes: false,
};
/// How many times a writer waits on the spot for a window to go out before it tries again
pub const WINDOW_SPINS: u32 = 2_000;
/// A writer waiting on a window spins this many rounds of spins, then pauses, since a sync holds the turn for milliseconds
pub const WINDOW_SPIN_ROUNDS: u32 = 4;
/// How long a writer waits for a seal to free the write index before it reports the failure
pub const INDEX_WAIT: Nanoseconds = Nanoseconds(60_000_000_000);

/// What a claim asks for
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClaimRequest {
    pub bytes: ByteCount,
    pub rows: RowCount,
    pub is_alone: bool,
    pub has_records: bool,
    pub covers: usize,
    pub mark: Option<DeathMark>,
    pub has_deletes: bool,
}

/// A value on its way to a record
#[derive(Clone, Copy, Debug)]
pub struct ValueWrite<'bytes> {
    pub column: ColumnId,
    pub key: &'bytes [u8],
    pub value: &'bytes [u8],
    pub codec: Codec,
    pub mark: Option<DeathMark>,
}

/// How an attempt to land a row ended
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LandOutcome {
    /// The row is in the index and the queue
    Landed,
    /// The index has no room, so nothing changed
    IndexFull,
}

/// Room held in a slot's filling group, given back when it drops, by which time a landed row has taken it
#[derive(Debug)]
pub struct RowRoom<'queue> {
    pub queue: &'queue Mutex<RowQueue>,
    pub size: usize,
}

impl Drop for RowRoom<'_> {
    fn drop(&mut self) {
        hold(self.queue).reserved -= self.size;
    }
}

/// Turn a backend failure into a store error, and a full disk into the volume's own error
pub fn store_error<Backend: Io>(volume: &Volume<Backend>, action: &'static str, source: Error) -> StoreError {
    if source.kind() == ErrorKind::StorageFull {
        return StoreError::VolumeFull { volume: volume.id };
    }
    StoreError::Io { action, source }
}

mod claims;
mod records;
mod sending;

pub use claims::lifetime_groups;
pub use sending::WritingTurn;

/// What a try to move a slot to its next window found
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowSwitch {
    /// The live window has room, or the slot has left this segment, so the next claim goes on
    HasRoom,
    /// The slot moved on, and the old window is waiting to go
    Switched,
    /// The other window is still going out
    OtherBusy,
    /// Too little of the segment is left for a window
    NoRoom,
}

/// A claim's choice of segment
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentPick {
    pub best: Option<usize>,
    pub wants_more: bool,
    /// The ceiling a segment opened for this claim takes
    pub ceiling: Option<DeathMark>,
    /// The segment to seal first, to make room for a group
    pub to_seal: Option<usize>,
    /// Every group is busy, so a new one waits for a seal
    pub must_wait: bool,
}


/// Refuse a value larger than a segment of its volume
pub fn check_value_size<Backend: Io>(volume: &Volume<Backend>, value: &[u8]) -> Result<(), StoreError> {
    let length = ByteCount(value.len() as u64);
    if length > volume.segment_bytes {
        return Err(StoreError::ValueTooLarge {
            length,
            limit: volume.segment_bytes,
        });
    }
    Ok(())
}

/// Refuse a write to a failed volume
pub fn check_writable<Backend: Io>(volume: &Volume<Backend>) -> Result<(), StoreError> {
    if volume.has_failed() {
        return Err(StoreError::VolumeFailed { volume: volume.id });
    }
    Ok(())
}

/// Refuse a wait that only a seal could end while the volume's seal stands stopped on a full table
pub fn check_seal_room<Backend: Io>(volume: &Volume<Backend>) -> Result<(), StoreError> {
    if volume.is_seal_stuck.load(Ordering::Acquire) {
        return Err(StoreError::TableFull { volume: volume.id });
    }
    Ok(())
}

/// Work out the end a segment's room grows to for a claim that ends at `end`: a step past what it holds, up to the segment's
/// bytes or the claim's end, whichever is further
pub fn reserve_target<Backend: Io>(segment: &OpenSegment<Backend::File>, volume: &Volume<Backend>, end: u64) -> u64 {
    let limit = volume.segment_bytes.0.max(end);
    (segment.reserved_end.0 + RESERVE_STEP_BYTES).max(end).min(limit)
}

