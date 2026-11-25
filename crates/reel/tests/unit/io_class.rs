//! The I/O class a maintenance thread takes and gives back

// the thread takes the idle class and gives it back, as a scrub step does around its reads
#[cfg(target_os = "linux")]
#[test]
fn the_idle_class_comes_and_goes() {
    let class = || {
        // SAFETY: gettid reads the calling thread's id
        let thread = unsafe { libc::gettid() };
        // SAFETY: the call reads one thread's I/O priority and touches no memory of ours
        (unsafe { libc::syscall(libc::SYS_ioprio_get, 1, libc::c_long::from(thread)) }) >> 13
    };
    reel::io::plain::set_thread_io_idle(true);
    assert_eq!(class(), 3, "the thread took no idle class");
    reel::io::plain::set_thread_io_idle(false);
    assert_ne!(class(), 3, "the thread kept the idle class");
}
