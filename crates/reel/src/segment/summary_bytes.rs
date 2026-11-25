//! The bytes of a summary's rows: on the heap for a copy open builds and drops, and in a room for a volume's own summary, where
//! memory follows the rows

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use reel_core::units::ByteCount;

use crate::pool_room::PoolRoom;

/// How much address space a room for rows takes past its plan, so rows that must stay can go past it as a list could
pub const ROOM_PAST_PLAN: usize = 2;

/// A summary's row bytes, with the calls a list of bytes takes
#[derive(Debug)]
pub enum SummaryBytes {
    Heap(Vec<u8>),
    Room(RoomRows),
}

/// Rows in a room: the bytes written, and the room the plan gave them
#[derive(Debug)]
pub struct RoomRows {
    room: Arc<PoolRoom>,
    length: usize,
    capacity: usize,
}

impl SummaryBytes {
    /// Start an empty list on the heap with room for this many bytes
    pub fn heap(capacity: usize) -> Self {
        Self::Heap(Vec::with_capacity(capacity))
    }

    /// Move these rows into a room planned for `plan` bytes, with room past it for rows that must stay. The rows come into
    /// memory now, and twice them comes in at open through the host check
    pub fn in_room(plan: usize, rows: &[u8]) -> std::io::Result<Self> {
        let room = PoolRoom::reserve(ByteCount((plan * ROOM_PAST_PLAN).max(1) as u64), ByteCount(rows.len() as u64))?;
        room.set_first_part(rows.len().saturating_mul(2));
        let mut bytes = Self::Room(RoomRows { room: Arc::new(room), length: 0, capacity: plan.max(rows.len()) });
        bytes.extend_from_slice(rows);
        Ok(bytes)
    }

    /// Read the room the rows live in, when they live in one
    pub fn room(&self) -> Option<&Arc<PoolRoom>> {
        match self {
            Self::Heap(_bytes) => None,
            Self::Room(rows) => Some(&rows.room),
        }
    }

    /// Count the bytes written
    pub fn len(&self) -> usize {
        match self {
            Self::Heap(bytes) => bytes.len(),
            Self::Room(rows) => rows.length,
        }
    }

    /// Check no byte is written
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Read the bytes the rows have room for
    pub fn capacity(&self) -> usize {
        match self {
            Self::Heap(bytes) => bytes.capacity(),
            Self::Room(rows) => rows.capacity,
        }
    }

    /// Read the bytes in memory for the rows
    pub fn heap_bytes(&self) -> usize {
        match self {
            Self::Heap(bytes) => bytes.capacity(),
            Self::Room(rows) => rows.room.committed(),
        }
    }

    /// Make room for this many more bytes than are written
    pub fn reserve(&mut self, additional: usize) {
        match self {
            Self::Heap(bytes) => bytes.reserve(additional),
            Self::Room(rows) => rows.capacity = rows.capacity.max(rows.length + additional),
        }
    }

    /// Make room for exactly this many more bytes than are written
    pub fn reserve_exact(&mut self, additional: usize) {
        match self {
            Self::Heap(bytes) => bytes.reserve_exact(additional),
            Self::Room(rows) => rows.capacity = rows.capacity.max(rows.length + additional),
        }
    }

    /// Set the length, bringing memory in for it in a room. Rows past what the room can hold move to the heap
    fn set_length(&mut self, length: usize, fill: u8) {
        match self {
            Self::Heap(bytes) => bytes.resize(length, fill),
            Self::Room(rows) if length > rows.room.reserved() => {
                let mut bytes = Vec::with_capacity(length.max(rows.capacity));
                bytes.extend_from_slice(rows.written());
                bytes.resize(length, fill);
                *self = Self::Heap(bytes);
            },
            Self::Room(rows) => {
                if length > rows.room.committed() {
                    // the job grows the room ahead of the rows, and a writer that outran it brings the next part in itself. A
                    // populate that fails leaves the pages to fault in as they are written
                    let _grown = rows.room.grow_to(length.max(rows.room.committed() + rows.room.step()));
                }
                if length > rows.length {
                    // SAFETY: the range lies in the room, past the rows written, and the writer's borrow is unique
                    unsafe { std::ptr::write_bytes(rows.room.base().add(rows.length), fill, length - rows.length) };
                }
                rows.length = length;
                rows.capacity = rows.capacity.max(length);
                rows.room.note_used(length);
            },
        }
    }

    /// Add one byte
    pub fn push(&mut self, byte: u8) {
        let length = self.len();
        self.set_length(length + 1, byte);
    }

    /// Add bytes
    pub fn extend_from_slice(&mut self, more: &[u8]) {
        let start = self.len();
        self.set_length(start + more.len(), 0);
        self[start..].copy_from_slice(more);
    }

    /// Set the length, writing `fill` past what was written
    pub fn resize(&mut self, length: usize, fill: u8) {
        self.set_length(length, fill);
    }

    /// Cut the rows to this many bytes
    pub fn truncate(&mut self, length: usize) {
        if length < self.len() {
            match self {
                Self::Heap(bytes) => bytes.truncate(length),
                Self::Room(rows) => {
                    rows.length = length;
                    rows.room.note_used(length);
                },
            }
        }
    }

    /// Drop every byte
    pub fn clear(&mut self) {
        self.truncate(0);
    }
}

impl RoomRows {
    /// Read the bytes written
    fn written(&self) -> &[u8] {
        // SAFETY: the bytes below the length were written and lie in the room's memory
        unsafe { std::slice::from_raw_parts(self.room.base(), self.length) }
    }
}

impl Deref for SummaryBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            Self::Heap(bytes) => bytes,
            Self::Room(rows) => rows.written(),
        }
    }
}

impl DerefMut for SummaryBytes {
    fn deref_mut(&mut self) -> &mut [u8] {
        match self {
            Self::Heap(bytes) => bytes,
            // SAFETY: as in written, and the writer's borrow is unique
            Self::Room(rows) => unsafe { std::slice::from_raw_parts_mut(rows.room.base(), rows.length) },
        }
    }
}
