//! Block-aligned buffers and spans for direct io, which takes whole blocks or nothing

use std::alloc::{alloc, dealloc, Layout};
use std::ptr::NonNull;

use crate::error::{ReelError, Result};
use crate::format::record::BLOCK;
use crate::io::op::ReadBuf;
use crate::io::posix_backend::STAGE_BYTES;

/// Every direct op's offset, length and buffer address sit on this boundary
pub const DIRECT_ALIGN: usize = BLOCK as usize;

/// The most bytes one direct op asks the device for
pub const DIRECT_REQUEST_BYTES: usize = STAGE_BYTES;

// The kernel refuses a request that does not divide into blocks.
const _: () = assert!(DIRECT_REQUEST_BYTES.is_multiple_of(DIRECT_ALIGN));

/// A buffered read faults in units of this many bytes
pub const PAGE_BYTES: usize = 4096;

// A block wider than a page would fetch bytes the buffered read does not.
const _: () = assert!(PAGE_BYTES.is_multiple_of(DIRECT_ALIGN));

/// Round an offset down to the block that contains it
pub fn align_down(offset: u64) -> u64 {
    offset & !(DIRECT_ALIGN as u64 - 1)
}

/// Round a length up to cover whole blocks
pub fn align_up(len: u64) -> u64 {
    let align = DIRECT_ALIGN as u64;
    len.saturating_add(align - 1) & !(align - 1)
}

/// The block-aligned span around a byte range, at most a block wider at each end
pub fn covering_span(offset: u64, len: u64) -> (u64, u64) {
    let start = align_down(offset);
    let end = align_up(offset.saturating_add(len));
    (start, end.saturating_sub(start))
}

/// The part of a covering read that holds the asked range, cut to what landed
pub fn wanted_window(filled: usize, skip: usize, wanted: usize) -> (usize, usize) {
    let from = filled.min(skip);
    let to = filled.min(skip.saturating_add(wanted));
    (from, to)
}

/// Copy a covering read's window into the buffer and commit exactly what was copied
pub fn cut_into(bytes: &[u8], buf: &mut ReadBuf) -> usize {
    let (ptr, room) = buf.as_mut_ptr();
    let taken = bytes.len().min(room);
    // Safety: taken fits the destination's room, and the commit covers exactly the copied bytes.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, taken);
        buf.commit(taken);
    }
    taken
}

/// Copy the window into a header buffer, then the rest into a payload buffer
pub fn cut_split_into(bytes: &[u8], head: &mut ReadBuf, body: &mut ReadBuf) -> usize {
    let split = bytes.len().min(head.wanted());
    cut_into(&bytes[..split], head) + cut_into(&bytes[split..], body)
}

/// A heap buffer whose address is on a block boundary, for a direct op
pub struct AlignedBuf {
    ptr: NonNull<u8>,
    len: usize,
}

// The buffer owns its allocation outright and hands out no interior references.
unsafe impl Send for AlignedBuf {}

impl AlignedBuf {
    /// Room for this many bytes, rounded up to whole blocks and zeroed
    pub fn new(len: usize) -> Result<AlignedBuf> {
        let buf = AlignedBuf::uninit(len)?;
        // Safety: the allocation is buf.len bytes, so zeroing it is in bounds.
        unsafe { std::ptr::write_bytes(buf.ptr.as_ptr(), 0, buf.len) };
        Ok(buf)
    }

    /// Room for this many bytes, rounded up to whole blocks and left unwritten
    pub fn uninit(len: usize) -> Result<AlignedBuf> {
        let len = align_up(len as u64) as usize;
        if len == 0 {
            return Err(ReelError::Backend(
                "a direct buffer cannot be empty".to_string(),
            ));
        }

        let layout = Layout::from_size_align(len, DIRECT_ALIGN).map_err(|error| {
            ReelError::Backend(format!(
                "a direct buffer of {len} bytes has no layout: {error}"
            ))
        })?;
        // Safety: the layout is non-zero and its alignment is a power of two.
        let raw = unsafe { alloc(layout) };
        let ptr = NonNull::new(raw).ok_or_else(|| {
            ReelError::Backend(format!(
                "a direct buffer of {len} bytes could not be allocated"
            ))
        })?;

        Ok(AlignedBuf { ptr, len })
    }

    /// The leading `count` bytes a read wrote into the buffer
    ///
    /// # Safety
    ///
    /// The count must come from a read, since the bytes past it are uninitialized
    pub unsafe fn filled(&self, count: usize) -> &[u8] {
        debug_assert!(count <= self.len, "a fill past the buffer was claimed");
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), count.min(self.len)) }
    }

    /// Zero the bytes past what a caller gathered, so a padded write stays clean
    pub fn zero_from(&mut self, at: usize) {
        if at >= self.len {
            return;
        }
        // Safety: at is inside the allocation and the run reaches exactly its end.
        unsafe { std::ptr::write_bytes(self.ptr.as_ptr().add(at), 0, self.len - at) };
    }

    /// Bytes the buffer holds, always a whole number of blocks
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the buffer holds nothing, which construction refuses
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The buffer as writable bytes, for gathering a write into it
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // Safety: the allocation is len bytes and the exclusive borrow rules out other reads.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    /// The address a syscall takes, which the allocation guarantees is aligned
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        if let Ok(layout) = Layout::from_size_align(self.len, DIRECT_ALIGN) {
            // Safety: the pointer came from alloc under this exact layout.
            unsafe { dealloc(self.ptr.as_ptr(), layout) };
        }
    }
}

impl std::fmt::Debug for AlignedBuf {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlignedBuf")
            .field("len", &self.len)
            .field("is_aligned", &is_block_aligned(self.as_ptr()))
            .finish()
    }
}

/// Whether a raw address sits on the boundary a direct op needs
pub fn is_block_aligned(address: *const u8) -> bool {
    (address as usize).is_multiple_of(DIRECT_ALIGN)
}

#[cfg(test)]
mod tests {
    use super::*;

    // a buffer's address lands on the boundary a direct op needs
    #[test]
    fn buffer_is_aligned() {
        for wanted in [1usize, 100, 4096, 4097, 1 << 20] {
            let buf = AlignedBuf::new(wanted).expect("aligned buffer");
            assert!(
                is_block_aligned(buf.as_ptr()),
                "{wanted} bytes landed off a block"
            );
            assert_eq!(
                buf.len() % DIRECT_ALIGN,
                0,
                "{wanted} bytes is not whole blocks"
            );
            assert!(buf.len() >= wanted, "{wanted} bytes did not fit");
        }
    }

    // a zeroed buffer reads back as zeros, so a padded write has no stale bytes
    #[test]
    fn buffer_starts_zeroed() {
        let buf = AlignedBuf::new(100).expect("aligned buffer");
        // Safety: new zeroed the whole allocation, so all of it is initialized.
        let bytes = unsafe { buf.filled(buf.len()) };
        assert!(bytes.iter().all(|byte| *byte == 0));
    }

    // zeroing from a mark clears the tail and leaves the gathered bytes alone
    #[test]
    fn zero_from_clears_only_the_tail() {
        let mut buf = AlignedBuf::uninit(100).expect("aligned buffer");
        buf.as_mut_slice()[..10].copy_from_slice(&[0xAB; 10]);
        buf.zero_from(10);

        // Safety: the write above and zero_from together initialized all of it.
        let bytes = unsafe { buf.filled(buf.len()) };
        assert!(
            bytes[..10].iter().all(|byte| *byte == 0xAB),
            "the gather was clobbered"
        );
        assert!(
            bytes[10..].iter().all(|byte| *byte == 0),
            "the tail kept stale bytes"
        );
    }

    // an empty buffer is refused
    #[test]
    fn empty_buffer_refused() {
        assert!(AlignedBuf::new(0).is_err());
    }

    // a covering span reaches whole blocks around the range and never cuts it short
    #[test]
    fn span_covers_the_range() {
        let cases = [
            (0u64, 100u64, 0u64, 4096u64),
            (100, 100, 0, 4096),
            (4096, 4096, 4096, 4096),
            (4095, 2, 0, 8192),
            (8192, 1, 8192, 4096),
        ];

        for (offset, len, want_start, want_span) in cases {
            let (start, span) = covering_span(offset, len);
            assert_eq!(start, want_start, "span for {offset}+{len} started wrong");
            assert_eq!(
                span, want_span,
                "span for {offset}+{len} was the wrong size"
            );
            assert!(
                start <= offset,
                "span for {offset}+{len} started past the range"
            );
            assert!(
                start + span >= offset + len,
                "span for {offset}+{len} ended before the range",
            );
            assert_eq!(start % DIRECT_ALIGN as u64, 0);
            assert_eq!(span % DIRECT_ALIGN as u64, 0);
        }
    }

    /// How many pages a buffered read of the range faults in, with readahead off
    fn pages_fetched(offset: u64, len: u64) -> u64 {
        let page = PAGE_BYTES as u64;
        let head = offset % page;
        head.saturating_add(len).div_ceil(page) * page
    }

    // a covering span never outruns the pages a buffered read of the range faults
    #[test]
    fn a_covering_span_never_exceeds_a_page_fetch() {
        for head in 0..PAGE_BYTES as u64 {
            for len in [1u64, 63, 512, 4000, 4096, 4097, 8192, 65_536] {
                let offset = 3 * PAGE_BYTES as u64 + head;
                let (_, span) = covering_span(offset, len);
                assert!(
                    span <= pages_fetched(offset, len),
                    "a span of {span} for {len} bytes at {offset} outran the page fetch",
                );
            }
        }
    }

    // a four kilobyte window lands in one block where it fits, two everywhere else
    #[test]
    fn a_page_window_covers_one_block_or_two() {
        let len = 4000u64;
        let mut one = 0u64;
        let mut two = 0u64;
        let mut bytes = 0u64;

        for head in 0..PAGE_BYTES as u64 {
            let (_, span) = covering_span(head, len);
            match span / DIRECT_ALIGN as u64 {
                1 => one += 1,
                2 => two += 1,
                other => panic!("a {len} byte window at {head} covered {other} blocks"),
            }
            bytes += span;
        }

        assert_eq!(one, 97, "one block fits a {len} byte window at 97 offsets");
        assert_eq!(two, 3999, "every other offset straddles two");
        assert_eq!(
            bytes / PAGE_BYTES as u64,
            8095,
            "the mean fetch is 8095 bytes"
        );
    }

    // the window is cut against what landed, so it never covers bytes the kernel skipped
    #[test]
    fn window_follows_the_read() {
        assert_eq!(
            wanted_window(4096, 100, 200),
            (100, 300),
            "the whole window landed"
        );
        assert_eq!(
            wanted_window(150, 100, 200),
            (100, 150),
            "the read stopped inside it"
        );
        assert_eq!(
            wanted_window(50, 100, 200),
            (50, 50),
            "the read stopped before it"
        );
        assert_eq!(wanted_window(0, 0, 200), (0, 0), "the read landed nothing");
    }

    // a cut takes the room the buffer has and commits exactly that
    #[test]
    fn a_cut_commits_what_it_copied() {
        let mut buf = ReadBuf::new(4);

        let taken = cut_into(&[1, 2, 3, 4, 5, 6], &mut buf);

        assert_eq!(taken, 4, "the buffer took only the room it had");
        assert_eq!(buf.filled(), 4);
        assert_eq!(buf.into_vec(), vec![1, 2, 3, 4]);
    }

    // a split cut fills the header first, and a short read leaves the payload empty
    #[test]
    fn a_split_cut_fills_the_head_first() {
        let mut head = ReadBuf::new(4);
        let mut body = ReadBuf::new(4);

        let taken = cut_split_into(&[1, 2, 3, 4, 5, 6], &mut head, &mut body);

        assert_eq!(taken, 6);
        assert_eq!(head.into_vec(), vec![1, 2, 3, 4]);
        assert_eq!(body.into_vec(), vec![5, 6]);

        let mut short_head = ReadBuf::new(4);
        let mut short_body = ReadBuf::new(4);

        let taken = cut_split_into(&[1, 2], &mut short_head, &mut short_body);

        assert_eq!(taken, 2);
        assert_eq!(short_head.into_vec(), vec![1, 2]);
        assert_eq!(
            short_body.filled(),
            0,
            "the payload took bytes the header wanted"
        );
    }

    // rounding a length up never shrinks it and always lands on a boundary
    #[test]
    fn rounding_is_a_superset() {
        for len in [0u64, 1, 4095, 4096, 4097] {
            let up = align_up(len);
            assert!(up >= len);
            assert_eq!(up % DIRECT_ALIGN as u64, 0);
        }
    }
}
