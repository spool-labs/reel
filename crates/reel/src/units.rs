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
