//! Which read errors mean the device lost bytes, as against the process doing something wrong

use std::io::{Error, ErrorKind, Result};

use reel_core::units::FileOffset;

use crate::io::{DeviceReads, IoFile};

/// Bytes of the smallest unit a device can lose
pub const SECTOR_BYTES: usize = 512;

/// Check an error says the bytes are gone, which is damage: a bad descriptor or no memory says nothing about them
pub fn is_device_error(error: &Error) -> bool {
    if error.kind() == ErrorKind::UnexpectedEof {
        return true;
    }
    matches!(
        error.raw_os_error(),
        Some(libc::EIO | libc::ENXIO | libc::ENODATA | libc::EBADMSG)
    )
}

/// Fill a buffer, or return false when the device can't give the bytes back
pub fn read_or_damaged<File: IoFile>(file: &File, at: FileOffset, buf: &mut [u8]) -> Result<bool> {
    match file.read_exact_at(at, buf) {
        Ok(()) => Ok(true),
        Err(error) if is_device_error(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Fill a buffer through a device's read path, or return false when the device can't give the bytes back
pub async fn read_or_damaged_on<File>(device: &impl DeviceReads<File>, file: &File, at: FileOffset, buf: &mut [u8]) -> Result<bool> {
    match device.read_exact(file, at, buf).await {
        Ok(()) => Ok(true),
        Err(error) if is_device_error(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

/// A sync that failed, which a retry can't trust: Linux can drop the dirty pages a failed sync held, and the next sync of the
/// file then reports them written
#[derive(Debug)]
pub struct SyncFailed(pub Error);

impl std::fmt::Display for SyncFailed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "a sync failed: {}", self.0)
    }
}

impl std::error::Error for SyncFailed {}

/// Mark a failed sync so no retry takes it for a passing write error, keeping its kind
pub fn sync_failed(error: Error) -> Error {
    Error::new(error.kind(), SyncFailed(error))
}

/// Check an error came from a failed sync
pub fn is_sync_failure(error: &Error) -> bool {
    error.get_ref().is_some_and(|inner| inner.is::<SyncFailed>())
}
