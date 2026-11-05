//! An open file on the plain-syscall backend

use std::fs::File;
use std::io::{Error, ErrorKind, IoSlice, Result};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;

use crate::io::IoFile;
use crate::units::{ByteCount, FileOffset};

/// A file reached through plain syscalls
#[derive(Debug)]
pub struct PlainFile {
    file: File,
}

impl PlainFile {
    /// Wrap an open file
    pub fn new(file: File) -> Self {
        Self { file }
    }

    /// Finish a short vectored write one part at a time
    pub fn write_remainder(&self, offset: FileOffset, parts: &[IoSlice], written: u64) -> Result<()> {
        let mut skipped = written;
        let mut position = offset.0.saturating_add(written);
        for part in parts {
            let length = part.len() as u64;
            if skipped >= length {
                skipped -= length;
                continue;
            }

            let remainder = &part[skipped as usize..];
            self.file.write_all_at(remainder, position)?;
            position = position.saturating_add(remainder.len() as u64);
            skipped = 0;
        }
        Ok(())
    }
}

impl IoFile for PlainFile {
    fn read_at(&self, offset: FileOffset, buf: &mut [u8]) -> Result<ByteCount> {
        let mut filled = 0;
        while filled < buf.len() {
            let position = offset.0.saturating_add(filled as u64);
            match self.file.read_at(&mut buf[filled..], position) {
                Ok(0) => break,
                Ok(count) => filled += count,
                Err(error) if error.kind() == ErrorKind::Interrupted => {},
                Err(error) => return Err(error),
            }
        }
        Ok(ByteCount(filled as u64))
    }

    fn read_exact_at(&self, offset: FileOffset, buf: &mut [u8]) -> Result<()> {
        self.file.read_exact_at(buf, offset.0)
    }

    fn write_at(&self, offset: FileOffset, parts: &[IoSlice]) -> Result<()> {
        let mut total: u64 = 0;
        for part in parts {
            total = total.saturating_add(part.len() as u64);
        }

        let written = write_vectored(&self.file, offset, parts)?;
        if written == total {
            return Ok(());
        }
        self.write_remainder(offset, parts, written)
    }

    fn sync(&self) -> Result<()> {
        self.file.sync_data()
    }

    fn length(&self) -> Result<ByteCount> {
        Ok(ByteCount(self.file.metadata()?.len()))
    }

    fn truncate(&self, length: ByteCount) -> Result<()> {
        self.file.set_len(length.0)
    }
}

/// Issue one positioned vectored write and return the bytes it took
pub fn write_vectored(file: &File, offset: FileOffset, parts: &[IoSlice]) -> Result<u64> {
    let descriptor = file.as_raw_fd();
    let vectors = parts.as_ptr().cast::<libc::iovec>();
    let count = parts.len() as libc::c_int;
    let position = offset.0 as libc::off_t;

    loop {
        // SAFETY: IoSlice has the layout of iovec on unix, and the parts outlive the call
        let written = unsafe { libc::pwritev(descriptor, vectors, count, position) };
        if written >= 0 {
            return Ok(written as u64);
        }

        let error = Error::last_os_error();
        if error.kind() != ErrorKind::Interrupted {
            return Err(error);
        }
    }
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
