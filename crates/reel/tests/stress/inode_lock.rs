//! Whether the filesystem at REEL_LOCK_DIR lets concurrent writers into one file
//! Run with `cargo test -p tape-reel -r --test stress -- one_file_many --ignored --nocapture`

use std::os::unix::io::RawFd;
use std::time::Instant;

use tempfile::TempDir;

/// The probe sweeps these writer counts unless REEL_LOCK_THREADS is set
const DEFAULT_THREADS: &str = "1,2,4,8,16";

/// Each writer writes this many bytes per pass unless REEL_LOCK_BYTES is set
const DEFAULT_PER_THREAD: u64 = 64 * 1024 * 1024;

/// Size of one write, block aligned for the direct path
const BLOCK: u64 = 65_536;

/// A direct write's buffer needs this alignment
const ALIGN: usize = 4096;

fn env_list(name: &str, fallback: &str) -> Vec<u64> {
    std::env::var(name)
        .unwrap_or_else(|_| fallback.to_string())
        .split(',')
        .filter_map(|item| item.trim().parse().ok())
        .collect()
}

fn env_bytes(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(fallback)
}

/// How the file was opened for one cell
#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
    /// Through the page cache, which takes the inode exclusively on Linux
    Buffered,

    /// Straight to the device, which may take the inode shared
    Direct,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Mode::Buffered => "buffered",
            Mode::Direct => "direct",
        }
    }
}

/// A first pass writes unwritten extents, a rewrite writes allocated ones
#[derive(Clone, Copy, Eq, PartialEq)]
enum Pass {
    First,
    Rewrite,
}

impl Pass {
    fn label(self) -> &'static str {
        match self {
            Pass::First => "first",
            Pass::Rewrite => "rewrite",
        }
    }
}

/// A page aligned buffer, which a direct write needs and a buffered one tolerates
struct Aligned {
    raw: *mut libc::c_void,
    len: usize,
}

// Threads only pass the pointer to pwrite and never write through it, so sharing is sound
unsafe impl Send for Aligned {}
unsafe impl Sync for Aligned {}

impl Aligned {
    fn new(len: usize, fill: u8) -> Aligned {
        let mut raw: *mut libc::c_void = std::ptr::null_mut();
        let code = unsafe { libc::posix_memalign(&mut raw, ALIGN, len) };
        assert_eq!(code, 0, "could not allocate an aligned buffer");
        unsafe { std::ptr::write_bytes(raw as *mut u8, fill, len) };
        Aligned { raw, len }
    }
}

impl Drop for Aligned {
    fn drop(&mut self) {
        unsafe { libc::free(self.raw) };
    }
}

/// Open a file for writing, direct where the platform has it
fn open_for(path: &std::path::Path, mode: Mode) -> RawFd {
    let text = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).expect("path");
    #[allow(unused_mut)]
    let mut flags = libc::O_RDWR | libc::O_CREAT;
    #[cfg(target_os = "linux")]
    if mode == Mode::Direct {
        flags |= libc::O_DIRECT;
    }
    let fd = unsafe { libc::open(text.as_ptr(), flags, 0o644) };
    assert!(fd >= 0, "open failed: {}", std::io::Error::last_os_error());

    // macOS has no O_DIRECT, and F_NOCACHE locks differently, so a direct row here is a stand-in
    #[cfg(target_os = "macos")]
    if mode == Mode::Direct {
        unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) };
    }
    fd
}

/// Reserve the whole file so no write in the probe extends it
fn reserve(fd: RawFd, len: u64) {
    #[cfg(target_os = "linux")]
    {
        let code = unsafe { libc::fallocate(fd, 0, 0, len as libc::off_t) };
        assert_eq!(
            code,
            0,
            "fallocate failed: {}",
            std::io::Error::last_os_error()
        );
    }
    #[cfg(not(target_os = "linux"))]
    {
        let code = unsafe { libc::ftruncate(fd, len as libc::off_t) };
        assert_eq!(
            code,
            0,
            "ftruncate failed: {}",
            std::io::Error::last_os_error()
        );
    }
}

fn write_at(fd: RawFd, buffer: &Aligned, offset: u64) {
    let wrote = unsafe { libc::pwrite(fd, buffer.raw, buffer.len, offset as libc::off_t) };
    assert_eq!(
        wrote,
        buffer.len as isize,
        "pwrite: {}",
        std::io::Error::last_os_error()
    );
}

/// Runs one pass with strided writers and returns the seconds it took
fn run_pass(fd: RawFd, threads: u64, blocks_each: u64, buffer: &Aligned) -> f64 {
    let start = Instant::now();
    std::thread::scope(|scope| {
        for thread in 0..threads {
            scope.spawn(move || {
                for step in 0..blocks_each {
                    let block = step * threads + thread;
                    write_at(fd, buffer, block * BLOCK);
                }
            });
        }
    });
    start.elapsed().as_secs_f64()
}

/// The filesystem behind a path, so a result can be attributed to one
fn filesystem_of(path: &std::path::Path) -> String {
    #[cfg(target_os = "linux")]
    {
        let out = std::process::Command::new("stat")
            .arg("-f")
            .arg("-c")
            .arg("%T")
            .arg(path)
            .output();
        if let Ok(out) = out {
            if let Ok(text) = String::from_utf8(out.stdout) {
                let name = text.trim().to_string();
                if !name.is_empty() {
                    return name;
                }
            }
        }
    }
    let _ = path;
    "unknown".to_string()
}

// how far concurrent writers into one file scale, buffered against direct
#[test]
#[ignore = "kernel probe, run explicitly on the filesystem under test"]
fn one_file_many_writers() {
    // libtest leaves the test name line open, so start the table on a fresh line
    println!();
    let threads_sweep = env_list("REEL_LOCK_THREADS", DEFAULT_THREADS);
    let per_thread = env_bytes("REEL_LOCK_BYTES", DEFAULT_PER_THREAD);
    let temp = TempDir::new().expect("tempdir");
    let base = match std::env::var("REEL_LOCK_DIR") {
        Ok(dir) => std::path::PathBuf::from(dir),
        Err(_) => temp.path().to_path_buf(),
    };
    std::fs::create_dir_all(&base).expect("create dir");

    println!(
        "one file, many writers, {} MiB per writer per pass, {} blocks, dir {} on {}",
        per_thread / (1024 * 1024),
        BLOCK,
        base.display(),
        filesystem_of(&base),
    );
    if !cfg!(target_os = "linux") {
        println!(
            "warning: this platform has no O_DIRECT and no i_rwsem, so the direct rows \
             are not the experiment and nothing here transfers"
        );
    }
    println!(
        "\n{:>10} {:>9} {:>8} {:>12} {:>10}",
        "mode", "pass", "threads", "MB/s", "vs 1t"
    );

    let buffer = Aligned::new(BLOCK as usize, 0x5a);
    for mode in [Mode::Buffered, Mode::Direct] {
        for pass in [Pass::First, Pass::Rewrite] {
            let mut single: Option<f64> = None;
            for &threads in &threads_sweep {
                let blocks_each = (per_thread / BLOCK).max(1);
                let bytes = (blocks_each * threads * BLOCK) as f64;

                // Each cell gets its own file, so a rewrite only covers blocks this cell wrote
                let path = base.join(format!("probe-{}-{}-{threads}", mode.label(), pass.label()));
                let _ = std::fs::remove_file(&path);
                let fd = open_for(&path, mode);
                reserve(fd, blocks_each * threads * BLOCK);
                if pass == Pass::Rewrite {
                    run_pass(fd, threads, blocks_each, &buffer);
                }

                let secs = run_pass(fd, threads, blocks_each, &buffer);
                unsafe { libc::fsync(fd) };
                unsafe { libc::close(fd) };
                let _ = std::fs::remove_file(&path);

                let mbps = bytes / secs / 1e6;
                let scale = match single {
                    Some(one) => mbps / one,
                    None => {
                        single = Some(mbps);
                        1.0
                    }
                };
                println!(
                    "{:>10} {:>9} {threads:>8} {mbps:>12.0} {scale:>10.2}",
                    mode.label(),
                    pass.label(),
                );
            }
        }
    }

    println!(
        "\nreading this: a mode whose scaling stays near 1.00 took the inode \
         exclusively. A first pass that stays flat while its rewrite scales means \
         the shared path does not survive unwritten extents, which is the only \
         case an append-only log produces."
    );
}
