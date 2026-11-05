//! Which open segments a reader counts

/// Most segments that can be open across a store
pub const MAX_OPEN_SLOTS: usize = 64;

/// The place of one open segment in the write index
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OpenSlot(pub u8);

/// A set of open segments, which seal changes in one atomic store
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct OpenSlots(pub u64);

impl OpenSlots {
    /// Build the set of one slot
    pub fn only(slot: OpenSlot) -> Self {
        Self(1 << slot.0)
    }

    /// Check whether a slot is in the set
    pub fn contains(self, slot: OpenSlot) -> bool {
        self.0 & (1 << slot.0) != 0
    }

    /// Add a slot
    pub fn with(self, slot: OpenSlot) -> Self {
        Self(self.0 | (1 << slot.0))
    }

    /// Remove a slot
    pub fn without(self, slot: OpenSlot) -> Self {
        Self(self.0 & !(1 << slot.0))
    }
}
