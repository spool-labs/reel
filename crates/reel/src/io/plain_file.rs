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

    fn try_read_at(&self, offset: FileOffset, buf: &mut [u8]) -> Result<Option<ByteCount>> {
        read_no_wait(self.descriptor, offset, &mut [IoSliceMut::new(buf)])
    }

    fn try_read_parts_at(&self, offset: FileOffset, parts: &mut [IoSliceMut]) -> Result<Option<ByteCount>> {
        read_no_wait(self.descriptor, offset, parts)
    }

    fn start_writeback(&self, offset: FileOffset, length: ByteCount) -> Result<()> {
        start_descriptor_writeback(self.descriptor, offset, length)
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

    fn reserve_to(&self, end: ByteCount) -> Result<()> {
        reserve_descriptor_to(self.descriptor, end)
    }
}

/// Hold a file's space through an end, growing it there
#[cfg(all(target_os = "linux", not(reel_plain_unix)))]
pub fn reserve_descriptor_to(descriptor: RawFd, end: ByteCount) -> Result<()> {
    // SAFETY: the descriptor is open, and no pointer crosses the call
    let status = unsafe { libc::fallocate(descriptor, 0, 0, end.0 as libc::off_t) };
    if status != 0 {
        return Err(Error::last_os_error());
    }
    Ok(())
}

/// Hold a file's space through an end, growing it there
#[cfg(all(target_vendor = "apple", not(reel_plain_unix)))]
pub fn reserve_descriptor_to(descriptor: RawFd, end: ByteCount) -> Result<()> {
    let length = descriptor_stats(descriptor)?.st_size as u64;
    if end.0 <= length {
        return Ok(());
    }
    let mut store = libc::fstore_t {
        fst_flags: libc::F_ALLOCATEALL,
        fst_posmode: libc::F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: (end.0 - length) as libc::off_t,
        fst_bytesalloc: 0,
    };
    // SAFETY: the descriptor is open, and the store outlives the call
    let status = unsafe { libc::fcntl(descriptor, libc::F_PREALLOCATE, &raw mut store) };
    if status != 0 {
        return Err(Error::last_os_error());
    }
    // SAFETY: the descriptor is open, and no pointer crosses the call
    let status = unsafe { libc::ftruncate(descriptor, end.0 as libc::off_t) };
    if status != 0 {
        return Err(Error::last_os_error());
    }
    Ok(())
}

/// Grow a file through an end, on a unix with no call that reserves its blocks: a full disk then shows at a write
#[cfg(any(reel_plain_unix, not(any(target_os = "linux", target_vendor = "apple"))))]
pub fn reserve_descriptor_to(descriptor: RawFd, end: ByteCount) -> Result<()> {
    let length = descriptor_stats(descriptor)?.st_size as u64;
    if end.0 <= length {
        return Ok(());
    }
    // SAFETY: the descriptor is open, and no pointer crosses the call
    let status = unsafe { libc::ftruncate(descriptor, end.0 as libc::off_t) };
    if status != 0 {
        return Err(Error::last_os_error());
    }
    Ok(())
}

/// Give a range's blocks back to the disk
#[cfg(all(target_os = "linux", not(reel_plain_unix)))]
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
#[cfg(all(target_vendor = "apple", not(reel_plain_unix)))]
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

/// Say a range's blocks can't go back, on a unix with no hole punch, so space comes back only when a segment goes
#[cfg(any(reel_plain_unix, not(any(target_os = "linux", target_vendor = "apple"))))]
pub fn free_descriptor_range(_descriptor: RawFd, _offset: FileOffset, _length: ByteCount) -> Result<()> {
    Err(Error::from(ErrorKind::Unsupported))
}

/// Make a file's writes reach the disk
#[cfg(all(target_os = "linux", not(reel_plain_unix)))]
pub fn sync_descriptor(descriptor: RawFd) -> Result<()> {
    // SAFETY: the descriptor is open, and no pointer crosses the call
    let status = unsafe { libc::fdatasync(descriptor) };
    if status != 0 {
        return Err(Error::last_os_error());
    }
    Ok(())
}

/// Make a file's writes reach the disk, which on macOS takes a full flush
#[cfg(all(target_vendor = "apple", not(reel_plain_unix)))]
pub fn sync_descriptor(descriptor: RawFd) -> Result<()> {
    // SAFETY: the descriptor is open, and no pointer crosses the call
    let status = unsafe { libc::fcntl(descriptor, libc::F_FULLFSYNC) };
    if status != 0 {
        return Err(Error::last_os_error());
    }
    Ok(())
}

/// Make a file's writes reach the disk, on a unix with neither of the calls above
#[cfg(any(reel_plain_unix, not(any(target_os = "linux", target_vendor = "apple"))))]
pub fn sync_descriptor(descriptor: RawFd) -> Result<()> {
    // SAFETY: the descriptor is open, and no pointer crosses the call
    let status = unsafe { libc::fsync(descriptor) };
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
#[cfg(all(target_os = "linux", not(reel_plain_unix)))]
pub fn reserve_space(file: &File, length: ByteCount) -> Result<()> {
    // SAFETY: the descriptor is open, and no pointer crosses the call
    let status = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, length.0 as libc::off_t) };
    if status != 0 {
        return Err(Error::last_os_error());
    }
    Ok(())
}

/// Reserve a new file's space, so a full disk fails here and never during a write
#[cfg(all(target_vendor = "apple", not(reel_plain_unix)))]
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

/// Size a new file, on a unix with no call that reserves its blocks: a full disk then shows at a write
#[cfg(any(reel_plain_unix, not(any(target_os = "linux", target_vendor = "apple"))))]
pub fn reserve_space(file: &File, length: ByteCount) -> Result<()> {
    reserve_descriptor_to(file.as_raw_fd(), length)
}

/// Start a range's dirty pages for the device
#[cfg(target_os = "linux")]
pub fn start_descriptor_writeback(descriptor: RawFd, offset: FileOffset, length: ByteCount) -> Result<()> {
    // SAFETY: the descriptor is open, and no pointer crosses the call
    let status = unsafe { libc::sync_file_range(descriptor, offset.0 as i64, length.0 as i64, libc::SYNC_FILE_RANGE_WRITE) };
    if status != 0 {
        return Err(Error::last_os_error());
    }
    Ok(())
}

/// Leave a range to the system, which has no call that starts one without waiting
#[cfg(not(target_os = "linux"))]
pub fn start_descriptor_writeback(_descriptor: RawFd, _offset: FileOffset, _length: ByteCount) -> Result<()> {
    Ok(())
}

/// Read parts from what the kernel holds, and say nothing came when the read would block
#[cfg(target_os = "linux")]
pub fn read_no_wait(descriptor: RawFd, offset: FileOffset, parts: &mut [IoSliceMut<'_>]) -> Result<Option<ByteCount>> {
    let vectors = parts.as_ptr().cast::<libc::iovec>();
    // SAFETY: `IoSliceMut` matches `iovec`, and each part stays borrowed for the call
    let count = unsafe { libc::preadv2(descriptor, vectors, parts.len() as libc::c_int, offset.0 as i64, libc::RWF_NOWAIT) };
    if count >= 0 {
        return Ok(Some(ByteCount(count as u64)));
    }
    let error = Error::last_os_error();
    match error.raw_os_error() {
        // a filesystem with no such read answers as one that would block, and the read goes the long way
        Some(libc::EAGAIN | libc::EOPNOTSUPP) => Ok(None),
        _other => Err(error),
    }
}

/// Say every read would block, on a system with no read that refuses to wait
#[cfg(not(target_os = "linux"))]
pub fn read_no_wait(_descriptor: RawFd, _offset: FileOffset, _parts: &mut [IoSliceMut<'_>]) -> Result<Option<ByteCount>> {
    Ok(None)
}
