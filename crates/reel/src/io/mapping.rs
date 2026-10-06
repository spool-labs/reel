//! Shared mappings of one segment file, read-only for readers and writable for a tail
//!
//! Taken once per segment on the first mapped read and unmapped when the last
//! handle drops, copying page-cache-warm bytes straight into the same pooled
//! buffers a pread would fill. A file that cannot be mapped reads through the
//! driver instead.
//!
//! The mapping reserves the whole span a segment may grow to, so a tail that keeps
//! growing after its first read stays mapped. Reads stop at the length the file was
//! last seen at, and a read past it looks at the file again before it gives up.

use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Ask the machine for a line without waiting on it
#[inline(always)]
pub(crate) fn prefetch(ptr: *const u8) {
    // Inline asm because `core::arch::aarch64::_prefetch` is still unstable and
    // this crate builds on stable; the x86 intrinsic below is not.
    #[cfg(target_arch = "aarch64")]
    // SAFETY: a prefetch of any address is architecturally a hint and cannot
    // fault, and the pointer comes from a live mapping or arena slot regardless.
    unsafe {
        std::arch::asm!(
            "prfm pldl1keep, [{0}]",
            in(reg) ptr,
            options(nostack, readonly, preserves_flags)
        );
    }
    #[cfg(target_arch = "x86_64")]
    // SAFETY: as above, `_mm_prefetch` is a hint and never faults.
    unsafe {
        std::arch::x86_64::_mm_prefetch(ptr as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    let _ = ptr;
}

/// One read-only mapping over a whole segment file
pub struct Mapping {
    base: *const u8,
    span: usize,
    seen: AtomicU64,
    path: PathBuf,
}

// Immutable shared memory over a file the format never cuts below its records:
// the only truncates release reservation blocks past the length or trim an
// aligned write's padding, both beyond what any record read touches. A read
// stops at a length the file has had, so no record read reaches past its end.
// Crossing threads is sound.
unsafe impl Send for Mapping {}
unsafe impl Sync for Mapping {}

impl Mapping {
    /// Map a file read-only over the span it may grow to, or nothing when it cannot be
    ///
    /// The span is the segment size, and a file already longer is mapped whole.
    /// Nothing rather than an error, since a volume whose files cannot be mapped
    /// falls back to the driver and stays correct. A mapping outlives the
    /// descriptor that made it, so the file closes on return.
    pub fn open(path: &Path, span: u64) -> Option<Mapping> {
        let file = File::open(path).ok()?;
        let len = file.metadata().ok()?.len();
        let span = span.max(len);
        if span == 0 || span > usize::MAX as u64 {
            return None;
        }
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                span as usize,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return None;
        }
        // Deliberately unadvised: MADV_RANDOM switches off fault-around, and a
        // record spanning many pages then faults a page at a time.
        Some(Mapping {
            base: base as *const u8,
            span: span as usize,
            seen: AtomicU64::new(len),
            path: path.to_path_buf(),
        })
    }

    /// The length the file was last seen at, which is as far as a read goes
    pub fn len(&self) -> u64 {
        self.seen.load(Ordering::Acquire)
    }

    /// Whether the file was empty when last seen
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The mapped bytes at an offset, or nothing when the span runs past the file
    ///
    /// A span past the length last seen looks at the file once more, since a tail
    /// grows after its first read. One the file still does not hold is the driver's.
    pub fn slice(&self, offset: u64, wanted: usize) -> Option<&[u8]> {
        let end = offset.checked_add(wanted as u64)?;
        if end > self.len() && (end > self.span as u64 || end > self.refresh()) {
            return None;
        }
        // In bounds of a live mapping, below a length the file has had.
        Some(unsafe { std::slice::from_raw_parts(self.base.add(offset as usize), wanted) })
    }

    /// Look at the file's length again and keep the longest one seen
    fn refresh(&self) -> u64 {
        let Ok(meta) = std::fs::metadata(&self.path) else {
            return self.len();
        };
        let len = meta.len().min(self.span as u64);
        self.seen.fetch_max(len, Ordering::AcqRel).max(len)
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // A failed unmap leaks address space, and a last drop has nobody to tell.
        unsafe { libc::munmap(self.base as *mut libc::c_void, self.span) };
    }
}

/// A writable shared mapping over a tail segment, which writers copy their records into
///
/// The file's length already covers the span, so every copy lands on bytes the file
/// holds, and the claim window has reserved the blocks under them, so a write fault
/// never has to allocate. A record copied in is in the page cache at once, where a
/// pread and a read mapping both find it.
pub struct WriteMapping {
    base: *mut u8,
    span: usize,
}

// Writers copy into ranges their claims keep apart, and nothing borrows the memory.
unsafe impl Send for WriteMapping {}
unsafe impl Sync for WriteMapping {}

impl WriteMapping {
    /// Map a file read-write over its first `span` bytes, or nothing when it cannot be
    pub fn open(path: &Path, span: u64) -> Option<WriteMapping> {
        let file = OpenOptions::new().read(true).write(true).open(path).ok()?;
        if span == 0 || span > usize::MAX as u64 || file.metadata().ok()?.len() < span {
            return None;
        }
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                span as usize,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return None;
        }
        Some(WriteMapping {
            base: base as *mut u8,
            span: span as usize,
        })
    }

    /// Copy bytes in at an offset, refusing a span past the mapping
    pub fn write(&self, offset: u64, bytes: &[u8]) -> bool {
        let Some(end) = offset.checked_add(bytes.len() as u64) else {
            return false;
        };
        if end > self.span as u64 {
            return false;
        }
        // In bounds of a live mapping, over a range only this writer's claim covers.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.base.add(offset as usize), bytes.len()) };
        true
    }
}

impl Drop for WriteMapping {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.base as *mut libc::c_void, self.span) };
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    // a mapping reads back the bytes the file holds, and refuses a span past the end
    #[test]
    fn maps_and_bounds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("segment");
        let mut file = File::create(&path).expect("create");
        file.write_all(&[7u8; 4096]).expect("write");
        file.write_all(&[9u8; 512]).expect("write");
        drop(file);

        let map = Mapping::open(&path, 0).expect("map");

        assert_eq!(map.len(), 4608);
        assert_eq!(map.slice(0, 4).expect("head"), &[7u8; 4]);
        assert_eq!(map.slice(4096, 512).expect("tail"), &[9u8; 512]);
        assert!(
            map.slice(4097, 512).is_none(),
            "a span past the end is the driver's"
        );
        assert!(map.slice(u64::MAX, 1).is_none());
    }

    // bytes copied into a write mapping read back through the file at once
    #[test]
    fn a_write_mapping_lands_in_the_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tail");
        File::create(&path).expect("create").set_len(8192).expect("size");

        let map = WriteMapping::open(&path, 8192).expect("map");
        assert!(map.write(4090, &[5u8; 12]), "a copy across a page edge");
        assert!(!map.write(8190, &[5u8; 4]), "a copy past the span is refused");
        let bytes = std::fs::read(&path).expect("read");
        assert_eq!(&bytes[4090..4102], &[5u8; 12]);
        assert!(bytes[..4090].iter().all(|byte| *byte == 0));
        assert!(WriteMapping::open(&path, 16384).is_none(), "a span past the file is refused");
    }

    // a path that does not open maps as nothing rather than an error
    #[test]
    fn absent_file_is_no_mapping() {
        assert!(Mapping::open(Path::new("/nonexistent/reel/segment"), 0).is_none());
    }

    // an empty file with no span maps as nothing, since there is nothing to serve
    #[test]
    fn empty_file_is_no_mapping() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("empty");
        File::create(&path).expect("create");

        assert!(Mapping::open(&path, 0).is_none());
    }

    // a file that grows after it was mapped serves its new bytes from the same mapping
    #[test]
    fn a_growing_file_stays_mapped() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tail");
        let mut file = File::create(&path).expect("create");
        file.write_all(&[7u8; 4096]).expect("write");

        let map = Mapping::open(&path, 1 << 20).expect("map");
        assert!(map.slice(4096, 512).is_none(), "nothing is there yet");

        file.write_all(&[9u8; 512]).expect("write");
        assert_eq!(map.slice(4096, 512).expect("grown"), &[9u8; 512]);
        assert_eq!(map.len(), 4608);
        assert!(
            map.slice(4608, 1).is_none(),
            "past the file is still the driver's"
        );
        assert!(
            map.slice((1 << 20) - 1, 2).is_none(),
            "past the span is the driver's"
        );
    }
}
