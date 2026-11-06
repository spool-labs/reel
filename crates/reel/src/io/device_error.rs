//! Which read errors mean the device lost bytes, as against the process doing something wrong

use std::io::{Error, ErrorKind, Result};

use reel_api::units::FileOffset;

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
