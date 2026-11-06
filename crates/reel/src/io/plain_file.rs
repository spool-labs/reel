//! An open file on the plain-syscall backend

use std::fs::File;
use std::io::{Error, ErrorKind, IoSlice, IoSliceMut, Result};
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, IntoRawFd, RawFd};

use reel_api::units::{ByteCount, FileOffset};

use crate::io::IoFile;

/// The size stat counts a file's blocks in, whatever the filesystem's own block is
pub const STAT_BLOCK_BYTES: u64 = 512;

/// A file reached through plain syscalls, closed only by `Io::close_file`
#[derive(Clone, Copy, Debug)]
pub struct PlainFile {
    descriptor: RawFd,
}

impl PlainFile {
    /// Read the descriptor the kernel knows this file by
    pub fn descriptor(&self) -> RawFd {
        self.descriptor
    }

    /// Take over an open file
    pub fn new(file: File) -> Self {
        Self {
            descriptor: file.into_raw_fd(),
        }
    }

    /// Close the file
    pub fn close(self) {
        // SAFETY: this handle owns the descriptor, and the engine closes each file once
        // a failed close leaves nothing to retry, since the descriptor is gone either way
        let _status = unsafe { libc::close(self.descriptor) };
    }

    /// Finish a short vectored write one part at a time
    pub fn write_remainder(&self, offset: FileOffset, parts: &[IoSlice], written: ByteCount) -> Result<()> {
        let mut skipped = written.0;
        let mut position = offset.0.saturating_add(written.0);
        for part in parts {
            let length = part.len() as u64;
            if skipped >= length {
                skipped -= length;
                continue;
            }

            let remainder = &part[skipped as usize..];
            let mut sent: u64 = 0;
            while sent < remainder.len() as u64 {
                let rest = [IoSlice::new(&remainder[sent as usize..])];
                let taken = write_vectored(self.descriptor, FileOffset(position + sent), &rest)?;
                if taken.0 == 0 {
                    return Err(Error::from(ErrorKind::WriteZero));
                }
                sent += taken.0;
            }
            position = position.saturating_add(remainder.len() as u64);
            skipped = 0;
        }
        Ok(())
    }

    /// Fill a buffer from an offset, however many reads that takes
    pub fn read_full(&self, offset: FileOffset, buf: &mut [u8]) -> Result<ByteCount> {
        let mut filled = 0;
        while filled < buf.len() {
            let position = offset.0.saturating_add(filled as u64);
            let mut parts = [IoSliceMut::new(&mut buf[filled..])];
            let read = read_vectored(self.descriptor, FileOffset(position), &mut parts)?;
            if read.0 == 0 {
                break;
            }
            filled += read.0 as usize;
        }
        Ok(ByteCount(filled as u64))
    }
}

impl IoFile for PlainFile {
    fn read_at(&self, offset: FileOffset, buf: &mut [u8]) -> Result<ByteCount> {
        self.read_full(offset, buf)
    }

    fn read_exact_at(&self, offset: FileOffset, buf: &mut [u8]) -> Result<()> {
        let read = self.read_full(offset, buf)?;
        if read.0 < buf.len() as u64 {
            return Err(Error::from(ErrorKind::UnexpectedEof));
        }
        Ok(())
    }

    fn read_parts_at(&self, offset: FileOffset, parts: &mut [IoSliceMut]) -> Result<()> {
        let read = read_vectored(self.descriptor, offset, parts)?;
        let mut skipped = read.0;
        let mut position = offset.0.saturating_add(read.0);
        for part in parts.iter_mut() {
            let length = part.len() as u64;
            if skipped >= length {
                skipped -= length;
                continue;
            }

            let remainder = &mut part[skipped as usize..];
            self.read_exact_at(FileOffset(position), remainder)?;
            position = position.saturating_add(remainder.len() as u64);
            skipped = 0;
        }
        Ok(())
    }

    fn write_at(&self, offset: FileOffset, parts: &[IoSlice]) -> Result<()> {
        let mut total: u64 = 0;
        for part in parts {
            total = total.saturating_add(part.len() as u64);
        }

        let written = write_vectored(self.descriptor, offset, parts)?;
        if written.0 == total {
            return Ok(());
        }
        self.write_remainder(offset, parts, written)
    }

    fn sync(&self) -> Result<()> {
        sync_descriptor(self.descriptor)
    }

    fn length(&self) -> Result<ByteCount> {
        Ok(ByteCount(descriptor_stats(self.descriptor)?.st_size as u64))
    }

    fn held_bytes(&self) -> Result<ByteCount> {
        Ok(ByteCount(descriptor_stats(self.descriptor)?.st_blocks as u64 * STAT_BLOCK_BYTES))
    }

    fn free_range(&self, offset: FileOffset, length: ByteCount) -> Result<()> {
        free_descriptor_range(self.descriptor, offset, length)
    }

    fn truncate(&self, length: ByteCount) -> Result<()> {
        // SAFETY: the descriptor is open, and no pointer crosses the call
        let status = unsafe { libc::ftruncate(self.descriptor, length.0 as libc::off_t) };
        if status != 0 {
            return Err(Error::last_os_error());
        }
        Ok(())
    }
}

/// Give a range's blocks back to the disk
#[cfg(target_os = "linux")]
pub fn free_descriptor_range(descriptor: RawFd, offset: FileOffset, length: ByteCount) -> Result<()> {
    let mode = libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE;
    // SAFETY: the descriptor is open, and no pointer crosses the call
    let status = unsafe { libc::fallocate(descriptor, mode, offset.0 as libc::off_t, length.0 as libc::off_t) };
    if status != 0 {
        return Err(Error::last_os_error());
    }
    Ok(())
}

/// Give a range's blocks back to the disk
#[cfg(not(target_os = "linux"))]
pub fn free_descriptor_range(descriptor: RawFd, offset: FileOffset, length: ByteCount) -> Result<()> {
    let mut range = libc::fpunchhole_t {
        fp_flags: 0,
        reserved: 0,
        fp_offset: offset.0 as libc::off_t,
        fp_length: length.0 as libc::off_t,
    };
    // SAFETY: the descriptor is open, and the range outlives the call
    let status = unsafe { libc::fcntl(descriptor, libc::F_PUNCHHOLE, &mut range) };
    if status != 0 {
        return Err(Error::last_os_error());
    }
    Ok(())
}

/// Make a file's writes reach the disk
#[cfg(target_os = "linux")]
pub fn sync_descriptor(descriptor: RawFd) -> Result<()> {
    // SAFETY: the descriptor is open, and no pointer crosses the call
    let status = unsafe { libc::fdatasync(descriptor) };
    if status != 0 {
        return Err(Error::last_os_error());
    }
    Ok(())
}

/// Make a file's writes reach the disk, which on macOS takes a full flush
#[cfg(target_os = "macos")]
pub fn sync_descriptor(descriptor: RawFd) -> Result<()> {
    // SAFETY: the descriptor is open, and no pointer crosses the call
    let status = unsafe { libc::fcntl(descriptor, libc::F_FULLFSYNC) };
    if status != 0 {
        return Err(Error::last_os_error());
    }
    Ok(())
}

/// Issue one positioned vectored read and return the bytes it brought
pub fn read_vectored(descriptor: RawFd, offset: FileOffset, parts: &mut [IoSliceMut]) -> Result<ByteCount> {
    let vectors = parts.as_ptr().cast::<libc::iovec>();
    let count = parts.len() as libc::c_int;
    let position = offset.0 as libc::off_t;

    loop {
        // SAFETY: IoSliceMut has the layout of iovec on unix, and the parts outlive the call
        let read = unsafe { libc::preadv(descriptor, vectors, count, position) };
        if read >= 0 {
            return Ok(ByteCount(read as u64));
        }

        let error = Error::last_os_error();
        if error.kind() != ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// Issue one positioned vectored write and return the bytes it took
pub fn write_vectored(descriptor: RawFd, offset: FileOffset, parts: &[IoSlice]) -> Result<ByteCount> {
    let vectors = parts.as_ptr().cast::<libc::iovec>();
    let count = parts.len() as libc::c_int;
    let position = offset.0 as libc::off_t;

    loop {
        // SAFETY: IoSlice has the layout of iovec on unix, and the parts outlive the call
        let written = unsafe { libc::pwritev(descriptor, vectors, count, position) };
        if written >= 0 {
            return Ok(ByteCount(written as u64));
        }

        let error = Error::last_os_error();
        if error.kind() != ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// Read a descriptor's stats
pub fn descriptor_stats(descriptor: RawFd) -> Result<libc::stat> {
    let mut stats = MaybeUninit::<libc::stat>::zeroed();
    // SAFETY: the descriptor is open and the pointer is valid for the call
    let status = unsafe { libc::fstat(descriptor, stats.as_mut_ptr()) };
    if status != 0 {
        return Err(Error::last_os_error());
    }

    // SAFETY: fstat filled the struct, and every field is a plain integer
    Ok(unsafe { stats.assume_init() })
}

/// Reserve a new file's space, so a full disk fails here and never during a write
#[cfg(target_os = "linux")]
pub fn reserve_space(file: &File, length: ByteCount) -> Result<()> {
    // SAFETY: the descriptor is open, and no pointer crosses the call
    let status = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, length.0 as libc::off_t) };
    if status != 0 {
        return Err(Error::last_os_error());
    }
    Ok(())
}

/// Reserve a new file's space, so a full disk fails here and never during a write
#[cfg(target_os = "macos")]
pub fn reserve_space(file: &File, length: ByteCount) -> Result<()> {
    let mut store = libc::fstore_t {
        fst_flags: libc::F_ALLOCATEALL,
        fst_posmode: libc::F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: length.0 as libc::off_t,
        fst_bytesalloc: 0,
    };

    // SAFETY: the descriptor is open, and the store outlives the call
    let status = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &raw mut store) };
    if status != 0 {
        return Err(Error::last_os_error());
    }
    file.set_len(length.0)
}
