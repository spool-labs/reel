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

// a worker's I/O idles only while reclaim can wait
#[test]
fn reclaim_that_can_wait_idles() {
    use reel::engine::walk_job::is_io_idle_for;
    assert!(is_io_idle_for(false, false, false), "work no one waits on kept the default class");
    assert!(!is_io_idle_for(true, false, false), "a volume short of room left its merges idle");
    assert!(!is_io_idle_for(false, true, false), "a forced pick or a caller's rebuild went idle");
    assert!(!is_io_idle_for(false, false, true), "a held or refused writer left the merges idle");
}
