//! The death window a caller names when it places a write

/// A group of records the caller expects to die at about the same time
///
/// The number means nothing to the engine: it is the caller's own window
/// identifier, compared for equality and nothing else. Records carrying the same
/// band go to the same tail, so a segment holds one band's records and dies whole
/// when that window passes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Band(pub u64);

impl Band {
    /// The window this band names
    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}
