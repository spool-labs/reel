//! Asking the CPU to fetch memory ahead of a read, so several reads that each miss the cache can wait together

/// Bytes the CPU fetches at once
pub const LINE_BYTES: usize = 64;

/// Ask the CPU to bring a byte's line into its cache. A hint only: it never faults and never waits
#[inline(always)]
pub fn fetch(pointer: *const u8) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: a prefetch is a hint the CPU drops for an address it can't reach, so any address is sound
    unsafe {
        core::arch::x86_64::_mm_prefetch(pointer.cast(), core::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(target_arch = "aarch64")]
    // SAFETY: a prefetch is a hint the CPU drops for an address it can't reach, so any address is sound
    unsafe {
        core::arch::asm!("prfm pldl1keep, [{0}]", in(reg) pointer, options(nostack, readonly, preserves_flags));
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _hint = pointer;
}

/// Fetch the lines a span of bytes covers, from its start up to a number of bytes on
#[inline(always)]
pub fn fetch_span(bytes: &[u8], at: usize, length: usize) {
    let base = bytes.as_ptr();
    let end = at.saturating_add(length).min(bytes.len());
    let mut line = at / LINE_BYTES * LINE_BYTES;
    while line < end {
        fetch(base.wrapping_add(line));
        line += LINE_BYTES;
    }
}
