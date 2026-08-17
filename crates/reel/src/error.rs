//! Error type for the reel store engine

use thiserror::Error;

use reel_core::Error as StoreError;

/// Result alias for reel operations
pub type Result<T> = std::result::Result<T, ReelError>;

/// Failure modes of the reel bulk-volume engine
#[derive(Debug, Error)]
pub enum ReelError {
    /// Invalid or out-of-range configuration rejected at load
    #[error("invalid reel config: {0}")]
    Config(String),

    /// Underlying file I/O failure
    #[error("reel io error: {0}")]
    Io(#[from] std::io::Error),

    /// Record or footer failed its checksum or a bounds check
    #[error("reel corruption: {0}")]
    Corruption(String),

    /// The I/O backend broke its own contract, which is not data corruption
    #[error("reel backend fault: {0}")]
    Backend(String),

    /// An operation the volume cannot serve in the state it was opened in
    #[error("reel rejected the operation: {0}")]
    Rejected(String),

    /// A ranged read of a payload a codec produced, whose bytes have no offsets
    #[error("reel cannot serve a range of a coded payload: {0}")]
    CodedRange(String),

    /// Another writer already holds the reel ownership lock
    #[error("reel ownership lock held: {0}")]
    LockHeld(String),
}

impl ReelError {
    /// Whether this is the filesystem saying the thing simply is not there
    ///
    /// A directory that does not exist yet is an empty volume; one that cannot be
    /// read is a volume of unknown contents, and taking it for the first is how a
    /// store decides it holds nothing and starts overwriting.
    pub fn is_missing(&self) -> bool {
        match self {
            ReelError::Io(source) => source.kind() == std::io::ErrorKind::NotFound,
            _ => false,
        }
    }

    /// Whether this is the filesystem out of space
    ///
    /// The one refusal a draw survives by going elsewhere: a reel spanning several
    /// volumes retries a full one's draw on the next.
    pub fn is_full(&self) -> bool {
        match self {
            ReelError::Io(source) => source.kind() == std::io::ErrorKind::StorageFull,
            _ => false,
        }
    }

    /// Whether this is the filesystem refusing something it will never accept
    ///
    /// An O_DIRECT open comes back EINVAL on tmpfs and overlayfs, and EOPNOTSUPP on
    /// the filesystems that answer more precisely.
    pub fn is_unsupported(&self) -> bool {
        match self {
            ReelError::Io(source) => matches!(
                source.raw_os_error(),
                Some(libc::EINVAL) | Some(libc::EOPNOTSUPP)
            ),
            _ => false,
        }
    }
}

impl From<ReelError> for StoreError {
    fn from(error: ReelError) -> Self {
        match error {
            ReelError::Io(source) => StoreError::Io(source),
            ReelError::Config(message) => StoreError::Database(message),
            ReelError::Corruption(message) => StoreError::Database(message),
            ReelError::Backend(message) => StoreError::Database(message),
            ReelError::Rejected(message) => StoreError::Database(message),
            ReelError::CodedRange(message) => StoreError::Database(message),
            ReelError::LockHeld(message) => StoreError::Database(message),
        }
    }
}
