//! A read-only shared mapping of one segment file
//!
//! Taken once per segment on the first mapped read and unmapped when the last
//! handle drops, copying page-cache-warm bytes straight into the same pooled
//! buffers a pread would fill. A file that cannot be mapped reads through the
//! driver instead.

use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::path::Path;

/// One read-only mapping over a whole segment file
pub struct Mapping {
    base: *const u8,
    len: usize,
}

// Immutable shared memory over a file the format never cuts below its records:
// the only truncates release reservation blocks past the length or trim an
// aligned write's padding, both beyond what any record read touches. Crossing
// threads is sound.
unsafe impl Send for Mapping {}
unsafe impl Sync for Mapping {}

impl Mapping {
    /// Map a file read-only at its current length, or nothing when it cannot be
    ///
    /// Nothing rather than an error, since a volume whose files cannot be mapped
    /// falls back to the driver and stays correct. A mapping outlives the
    /// descriptor that made it, so the file closes on return.
    pub fn open(path: &Path) -> Option<Mapping> {
        let file = File::open(path).ok()?;
        let len = file.metadata().ok()?.len();
        if len == 0 || len > usize::MAX as u64 {
            return None;
        }
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len as usize,
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
            len: len as usize,
        })
    }

    /// Bytes the mapping covers, fixed at the length the file had when mapped
    pub fn len(&self) -> u64 {
        self.len as u64
    }

    /// Whether the mapping covers nothing at all
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The mapped bytes at an offset, or nothing when the span runs past the end
    ///
    /// Not an error: under chunked preallocation a record can land beyond where
    /// the file ended when the mapping was taken, and that read is the driver's.
    pub fn slice(&self, offset: u64, wanted: usize) -> Option<&[u8]> {
        let end = offset.checked_add(wanted as u64)?;
        if end > self.len as u64 {
            return None;
        }
        // In bounds of a live mapping over a file that is never truncated.
        Some(unsafe { std::slice::from_raw_parts(self.base.add(offset as usize), wanted) })
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // A failed unmap leaks address space, and a last drop has nobody to tell.
        unsafe { libc::munmap(self.base as *mut libc::c_void, self.len) };
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

        let map = Mapping::open(&path).expect("map");

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
        assert!(Mapping::open(Path::new("/nonexistent/reel/segment")).is_none());
    }

    // an empty file maps as nothing, since there is nothing to serve from it
    #[test]
    fn empty_file_is_no_mapping() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("empty");
        File::create(&path).expect("create");

        assert!(Mapping::open(&path).is_none());
    }
}
