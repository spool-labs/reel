//! Plain wrappers for the numbers the engine passes around

/// A position in a file
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FileOffset(pub u64);

/// An amount of storage in bytes
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ByteCount(pub u64);

/// A reading of the monotonic clock
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Nanoseconds(pub u64);

/// The operating system's id for a process
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProcessId(pub u32);

/// A count of rows
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RowCount(pub u64);

/// A count of segments
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SegmentCount(pub u64);

/// One place in the store-wide write order
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WritePlace(pub u64);

/// One of a store's volumes, in the order the config declares them
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VolumeId(pub u8);

/// One of a volume's roots, in the order the config declares them
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RootId(pub u8);
