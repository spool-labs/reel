//! A read-only shared mapping of one segment file
//!
//! Taken once per segment on the first mapped read and unmapped when the last
//! handle drops, copying page-cache-warm bytes straight into the same pooled
//! buffers a pread would fill. A file that cannot be mapped reads through the
//! driver instead.
//!
//! The mapping reserves the whole span a segment may grow to, so a tail that keeps
//! growing after its first read stays mapped. Reads stop at the length the file was
//! last seen at, and a read past it looks at the file again before it gives up.

use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

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
