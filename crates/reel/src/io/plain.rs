//! The plain-syscall backend, used on every platform

use std::collections::hash_map::RandomState;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::hash::{BuildHasher, Hasher};
use std::hint::black_box;
use std::io::{Error, ErrorKind, Result};
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread::{self, Builder};
use std::time::{Duration, Instant};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use reel_core::maintenance_report::PriorityClass;
use reel_core::maintenance_report::WorkerPriority;
use reel_core::units::{ByteCount, FileOffset, Nanoseconds, ProcessId};

use crate::io::wake_line::WakeLine;
use crate::io::host_memory::{HostMemory, read_host_memory};
use crate::guard::hold;
use crate::io::{DiskSize, Io, IoFile};
use crate::io::job::Job;
use crate::io::lock_outcome::LockOutcome;
use crate::io::plain_file::{PlainFile, reserve_space, sync_descriptor};
use crate::io::plain_task::{PlainTask, TaskSignal, run_job};
#[cfg(not(target_os = "linux"))]
use crate::io::ring::NoRing;
#[cfg(target_os = "linux")]
use crate::io::uring::UringDriver;
use crate::pool::TOUCH_BYTE;

/// Width of the process id in a lock file, padded with spaces
pub const HOLDER_WIDTH: usize = 10;
/// The file open writes in a root to learn whether its filesystem gives blocks back
/// The mode a store's files take, which only the store's own user reads
pub const FILE_MODE: u32 = 0o600;
/// The mode a store's roots take
pub const DIRECTORY_MODE: u32 = 0o700;
pub const FREE_PROBE_NAME: &str = "free-probe";
/// Bytes of that file, two blocks of any filesystem reel runs on
pub const FREE_PROBE_BYTES: usize = 8 * 1024;
/// Layers of stacked devices a look at a disk goes down through
#[cfg(target_os = "linux")]
pub const MOST_STACK_DEPTH: usize = 8;
/// The nice value a maintenance worker runs at, the lowest normal one
#[cfg(target_os = "linux")]
pub const MAINTENANCE_NICE: i32 = 19;
/// The stack of a job thread, which holds a seal's and a merge's deepest calls with room to spare
pub const JOB_STACK_BYTES: ByteCount = ByteCount(1024 * 1024);
/// What a job's touch leaves alone: the guard page, the thread's own data that glibc keeps in the stack, and the frames
/// above the touch, about 15 KiB in a debug build. A kernel with 64 KiB pages takes 64 KiB for the guard alone
pub const STACK_SLACK_BYTES: usize = 128 * 1024;
/// The part of a job's stack written through at its start
pub const TOUCHED_STACK_BYTES: usize = JOB_STACK_BYTES.0 as usize - STACK_SLACK_BYTES;
/// Reads of a lock file before giving up on a holder writing its id
pub const HOLDER_READ_ATTEMPTS: u64 = 100;
/// Wait between reads of a lock file
pub const HOLDER_READ_WAIT: Duration = Duration::from_millis(1);

/// How long a waiting caller sleeps between looks
pub const PAUSE_WAIT: Duration = Duration::from_micros(100);

/// The backend that calls the operating system directly
#[derive(Clone, Debug)]
pub struct PlainIo {
    origin: Instant,
}

/// A held file lock, released when the file closes
#[derive(Debug)]
pub struct PlainLock {
    pub file: File,
}

impl PlainIo {
    /// Start a backend whose clock counts from now
    pub fn new() -> Self {
        Self { origin: Instant::now() }
    }
}

impl Default for PlainIo {
    fn default() -> Self {
        Self::new()
    }
}

impl Io for PlainIo {
    type File = PlainFile;
    type Lock = PlainLock;
    type Task = PlainTask;
    #[cfg(target_os = "linux")]
    type Ring = UringDriver;
    #[cfg(not(target_os = "linux"))]
    type Ring = NoRing;

    #[cfg(target_os = "linux")]
    fn open_ring(&self) -> Result<Option<UringDriver>> {
        // a kernel or a sandbox with no ring leaves the blocking pool, and the report says so
        Ok(UringDriver::open().ok())
    }

    #[cfg(not(target_os = "linux"))]
    fn open_ring(&self) -> Result<Option<NoRing>> {
        Ok(None)
    }

    fn create_file(&self, path: &Path, reserved: ByteCount) -> Result<PlainFile> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true).mode(FILE_MODE).custom_flags(libc::O_NOFOLLOW);
        let file = options.open(path)?;
        if reserved.0 == 0 {
            return Ok(PlainFile::new(file));
        }

        if let Err(error) = reserve_space(&file, reserved) {
            // a segment that failed its reservation must not be left for open to find
            let _removed = fs::remove_file(path);
            return Err(error);
        }
        Ok(PlainFile::new(file))
    }

    fn open_file(&self, path: &Path) -> Result<PlainFile> {
        let mut options = OpenOptions::new();
        options.read(true).write(true);
        Ok(PlainFile::new(options.open(path)?))
    }

    fn open_read_only(&self, path: &Path) -> Result<PlainFile> {
        Ok(PlainFile::new(File::open(path)?))
    }

    #[cfg(target_os = "linux")]
    fn open_direct(&self, path: &Path) -> Result<Option<PlainFile>> {
        Ok(Some(PlainFile::new(OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(path)?)))
    }

    #[cfg(target_vendor = "apple")]
    fn open_direct(&self, path: &Path) -> Result<Option<PlainFile>> {
        let file = File::open(path)?;
        // SAFETY: the descriptor is open, and no pointer crosses the call
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) } != 0 {
            return Err(Error::last_os_error());
        }
        Ok(Some(PlainFile::new(file)))
    }

    fn can_free_blocks(&self, root: &Path) -> bool {
        let path = root.join(FREE_PROBE_NAME);
        let probe = OpenOptions::new().read(true).write(true).create(true).truncate(true).mode(FILE_MODE).custom_flags(libc::O_NOFOLLOW).open(&path);
        let Ok(probe) = probe else {
            return false;
        };
        let is_written = probe.write_all_at(&[1; FREE_PROBE_BYTES], 0).is_ok();
        let file = PlainFile::new(probe);
        let is_freed = is_written && file.free_range(FileOffset(0), ByteCount(FREE_PROBE_BYTES as u64)).is_ok();
        file.close();
        // a probe left behind is the next open's to replace
        let _removed = fs::remove_file(&path);
        is_freed
    }

    fn close_file(&self, file: PlainFile) {
        file.close();
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        fs::remove_file(path)
    }

    fn rename_file(&self, from: &Path, to: &Path) -> Result<()> {
        fs::rename(from, to)
    }

    fn create_directory(&self, path: &Path) -> Result<()> {
        DirBuilder::new().recursive(true).mode(DIRECTORY_MODE).create(path)
    }

    fn sync_directory(&self, path: &Path) -> Result<()> {
        // the file sync's own call, since std's sync_all takes a full flush on Apple
        sync_descriptor(File::open(path)?.as_raw_fd())
    }

    fn list_directory(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                paths.push(entry.path());
            }
        }
        paths.sort();
        Ok(paths)
    }

    fn is_rotational(&self, path: &Path) -> Option<bool> {
        rotational_under(path)
    }

    fn free_space(&self, path: &Path) -> Result<ByteCount> {
        let stats = disk_stats(path)?;
        Ok(ByteCount(widen(stats.f_bavail).saturating_mul(widen(stats.f_frsize))))
    }

    fn capacity(&self, path: &Path) -> Result<Option<DiskSize>> {
        let stats = disk_stats(path)?;
        let bytes = ByteCount(widen(stats.f_blocks).saturating_mul(widen(stats.f_frsize)));
        Ok(Some(DiskSize { filesystem: widen(stats.f_fsid), bytes }))
    }

    fn host_memory(&self) -> HostMemory {
        read_host_memory()
    }

    fn lock(&self, path: &Path) -> Result<LockOutcome<PlainLock>> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false).mode(FILE_MODE).custom_flags(libc::O_NOFOLLOW);
        let file = options.open(path)?;

        // flock holds on every unix, where the standard library's try_lock holds only on some
        // SAFETY: the descriptor is open, and no pointer crosses the call
        let status = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if status != 0 {
            let error = Error::last_os_error();
            if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
                return Ok(LockOutcome::Refused {
                    holder: read_holder(&file)?,
                });
            }
            return Err(error);
        }
        // one padded write, so a reader never sees a cut-down id
        let holder = format!("{:>HOLDER_WIDTH$}", process::id());
        file.write_all_at(holder.as_bytes(), 0)?;
        Ok(LockOutcome::Held(PlainLock { file }))
    }

    fn now(&self) -> Nanoseconds {
        let elapsed = self.origin.elapsed();
        let nanoseconds = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        Nanoseconds(nanoseconds)
    }

    fn random_word(&self) -> u64 {
        // the record key keeps one store's records from checking under another's, so it comes from the kernel's random source, and
        // a kernel with no random source still needs a store id, for which the per-thread SipHash keys are the best left
        random_bytes().map_or_else(|| RandomState::new().build_hasher().finish(), u64::from_le_bytes)
    }

    fn pause_on(&self, line: &WakeLine, seen: u64) {
        line.wait(seen, PAUSE_WAIT);
    }

    fn pause(&self) {
        thread::sleep(PAUSE_WAIT);
    }

    fn job_stack_bytes(&self) -> ByteCount {
        JOB_STACK_BYTES
    }

    fn run_units(&self, threads: usize, units: usize, unit: &(dyn Fn(usize) + Sync)) {
        let threads = threads.min(units);
        if threads <= 1 {
            (0..units).for_each(unit);
            return;
        }
        // each thread takes the next unit as it frees up, so a slow unit holds up its own thread alone
        let next = AtomicUsize::new(0);
        let take = || loop {
            let index = next.fetch_add(1, Ordering::Relaxed);
            if index >= units {
                return;
            }
            unit(index);
        };
        thread::scope(|scope| {
            for _thread in 1..threads {
                // a thread that can't start leaves its units to the others, the caller among them
                let _spawned = Builder::new().name("reel-fill".to_string()).stack_size(JOB_STACK_BYTES.0 as usize).spawn_scoped(scope, take);
            }
            take();
        });
    }

    fn spawn_worker<Work: Job>(&self, name: &'static str, job: Work) -> Result<(PlainTask, WorkerPriority)> {
        let signal = Arc::new(TaskSignal::default());
        let thread_signal = Arc::clone(&signal);
        let thread_io = self.clone();
        let priority = Arc::new(Mutex::new(WorkerPriority::default()));
        let thread_priority = Arc::clone(&priority);
        let ready = Arc::new(Barrier::new(2));
        let thread_ready = Arc::clone(&ready);
        let thread = Builder::new()
            .name(name.to_string())
            .stack_size(JOB_STACK_BYTES.0 as usize)
            .spawn(move || {
                // the thread lowers its own priority, since a class set from outside would reach the whole process
                *hold(&thread_priority) = lower_priority();
                touch_stack();
                thread_ready.wait();
                run_job(job, &thread_signal, &thread_io);
            })?;
        ready.wait();
        let set = *hold(&priority);
        Ok((PlainTask::new(signal, thread), set))
    }

    fn spawn_job<Work: Job>(&self, name: &'static str, job: Work) -> Result<PlainTask> {
        let signal = Arc::new(TaskSignal::default());
        let thread_signal = Arc::clone(&signal);
        let thread_io = self.clone();
        // open isn't over until the stack is written, so it waits for the thread to get that far
        let ready = Arc::new(Barrier::new(2));
        let thread_ready = Arc::clone(&ready);
        let thread = Builder::new()
            .name(name.to_string())
            .stack_size(JOB_STACK_BYTES.0 as usize)
            .spawn(move || {
                touch_stack();
                thread_ready.wait();
                run_job(job, &thread_signal, &thread_io);
            })?;
        ready.wait();
        Ok(PlainTask::new(signal, thread))
    }
}

/// Write through the thread's stack now, so a job never faults one of its pages in later
#[inline(never)]
pub fn touch_stack() {
    let mut stack = [TOUCH_BYTE; TOUCHED_STACK_BYTES];
    black_box(&mut stack);
}

/// Read the holder's id, waiting out the moment a first holder takes to write it
pub fn read_holder(file: &File) -> Result<ProcessId> {
    let mut buf = [0; HOLDER_WIDTH];
    for _attempt in 0..HOLDER_READ_ATTEMPTS {
        let count = file.read_at(&mut buf, 0)?;
        if count == HOLDER_WIDTH {
            return parse_holder(&buf);
        }
        thread::sleep(HOLDER_READ_WAIT);
    }
    Err(Error::from(ErrorKind::InvalidData))
}

/// Parse a padded process id
pub fn parse_holder(buf: &[u8]) -> Result<ProcessId> {
    let text = String::from_utf8_lossy(buf);
    match text.trim().parse() {
        Ok(process) => Ok(ProcessId(process)),
        Err(_unreadable) => Err(Error::from(ErrorKind::InvalidData)),
    }
}

/// Widen a field whose width differs between platforms
pub fn widen<Field: Into<u64>>(field: Field) -> u64 {
    field.into()
}

/// Read the filesystem stats of the disk under a directory
pub fn disk_stats(path: &Path) -> Result<libc::statvfs> {
    let directory = File::open(path)?;
    let mut stats = MaybeUninit::<libc::statvfs>::zeroed();
    // SAFETY: the descriptor is open and the pointer is valid for the call
    let status = unsafe { libc::fstatvfs(directory.as_raw_fd(), stats.as_mut_ptr()) };
    if status != 0 {
        return Err(Error::last_os_error());
    }
    // SAFETY: fstatvfs filled the struct, and every field is a plain integer
    Ok(unsafe { stats.assume_init() })
}

/// Say whether any disk under a path spins, following a stacked device down to its slaves
#[cfg(target_os = "linux")]
pub fn rotational_under(path: &Path) -> Option<bool> {
    use std::os::linux::fs::MetadataExt;

    let device = fs::metadata(path).ok()?.st_dev();
    // the kernel's encoding of a device number
    let major = ((device >> 8) & 0xFFF) | ((device >> 32) & !0xFFF);
    let minor = (device & 0xFF) | ((device >> 12) & !0xFF);
    let block = fs::canonicalize(format!("/sys/dev/block/{major}:{minor}")).ok()?;
    block_spins(&block, 0)
}

/// Say whether a block device spins, for an operating system with no way to ask
#[cfg(not(target_os = "linux"))]
pub fn rotational_under(_path: &Path) -> Option<bool> {
    None
}

/// Check a block device or anything stacked under it spins. A partition asks its disk
#[cfg(target_os = "linux")]
pub fn block_spins(block: &Path, depth: usize) -> Option<bool> {
    if depth > MOST_STACK_DEPTH {
        return None;
    }
    let slaves: Vec<_> = fs::read_dir(block.join("slaves")).map(|entries| entries.flatten().map(|entry| entry.path()).collect()).unwrap_or_default();
    if !slaves.is_empty() {
        let mut known = None;
        for slave in slaves {
            let device = fs::canonicalize(slave).ok()?;
            match block_spins(&device, depth + 1) {
                Some(true) => return Some(true),
                Some(false) => known = Some(false),
                None => {},
            }
        }
        return known;
    }
    if block.join("partition").exists() {
        return block_spins(block.parent()?, depth + 1);
    }
    let flag = fs::read_to_string(block.join("queue/rotational")).ok()?;
    Some(flag.trim() == "1")
}

/// Put the calling thread in the batch class at nice 19, which callers preempt but which still gets a slice within milliseconds,
/// and read back what took
#[cfg(target_os = "linux")]
pub fn lower_priority() -> WorkerPriority {
    let param = libc::sched_param { sched_priority: 0 };
    // SAFETY: zero means the calling thread, and the parameter lives for the call
    unsafe { libc::sched_setscheduler(0, libc::SCHED_BATCH, &param) };
    // SAFETY: gettid reads the calling thread's id
    let thread = unsafe { libc::gettid() };
    // SAFETY: on Linux a thread id points at that thread alone for these calls
    unsafe { libc::setpriority(libc::PRIO_PROCESS, thread as libc::id_t, MAINTENANCE_NICE) };
    // SAFETY: as above, and each call reads a number
    let (policy, nice) = unsafe { (libc::sched_getscheduler(0), libc::getpriority(libc::PRIO_PROCESS, thread as libc::id_t)) };
    let class = match policy {
        libc::SCHED_BATCH => PriorityClass::Batch,
        libc::SCHED_OTHER => PriorityClass::Normal,
        _ => PriorityClass::Unknown,
    };
    WorkerPriority { class, nice, is_io_idle: take_idle_io(thread) }
}

/// The idle I/O class, and where ioprio_set reads a class in the priority value
#[cfg(target_os = "linux")]
const IOPRIO_CLASS_IDLE: libc::c_long = 3;
#[cfg(target_os = "linux")]
const IOPRIO_CLASS_SHIFT: libc::c_long = 13;
/// ioprio_set's target kind for one thread
#[cfg(target_os = "linux")]
const IOPRIO_WHO_PROCESS: libc::c_long = 1;

/// Put the thread's disk reads and writes in the idle class, so a disk scheduler with classes serves every caller's first, and say
/// whether the kernel took it
#[cfg(target_os = "linux")]
fn take_idle_io(thread: libc::pid_t) -> bool {
    let idle = IOPRIO_CLASS_IDLE << IOPRIO_CLASS_SHIFT;
    // SAFETY: the call sets one thread's I/O class and touches no memory of ours
    let _set = unsafe { libc::syscall(libc::SYS_ioprio_set, IOPRIO_WHO_PROCESS, libc::c_long::from(thread), idle) };
    // SAFETY: as above, and the call reads a number
    let taken = unsafe { libc::syscall(libc::SYS_ioprio_get, IOPRIO_WHO_PROCESS, libc::c_long::from(thread)) };
    taken >> IOPRIO_CLASS_SHIFT == IOPRIO_CLASS_IDLE
}

/// Put the calling thread in the utility QoS class, the nearest macOS has to a per-thread nice 19, since background throttles
/// its reads as an idle class would, and read back what took
#[cfg(target_os = "macos")]
pub fn lower_priority() -> WorkerPriority {
    // SAFETY: the call sets the calling thread's class and touches no memory of ours
    unsafe { libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_UTILITY, 0) };
    let (mut class, mut relative) = (0u32, 0);
    // SAFETY: the class is read as its plain number, so an unknown one can't make a bad enum, and both outs live for the call
    unsafe { libc::pthread_get_qos_class_np(libc::pthread_self(), (&raw mut class).cast(), &mut relative) };
    let class = if class == libc::qos_class_t::QOS_CLASS_UTILITY as u32 { PriorityClass::Utility } else { PriorityClass::Unknown };
    WorkerPriority { class, nice: relative, is_io_idle: false }
}

/// Leave the thread as it is where no lower class is known
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn lower_priority() -> WorkerPriority {
    WorkerPriority::default()
}

/// Read a word from the kernel's random source, where getentropy is always there
#[cfg(all(any(target_os = "linux", target_vendor = "apple"), not(reel_plain_unix)))]
pub fn random_bytes() -> Option<[u8; 8]> {
    let mut bytes = [0u8; 8];
    // SAFETY: the buffer is valid for its length, which is under the 256 bytes getentropy takes
    let status = unsafe { libc::getentropy(bytes.as_mut_ptr().cast::<libc::c_void>(), bytes.len()) };
    (status == 0).then_some(bytes)
}

/// Read a word from the kernel's random device, on a unix whose C library may predate getentropy
#[cfg(any(reel_plain_unix, not(any(target_os = "linux", target_vendor = "apple"))))]
pub fn random_bytes() -> Option<[u8; 8]> {
    use std::io::Read;

    let mut bytes = [0u8; 8];
    File::open("/dev/urandom").and_then(|mut device| device.read_exact(&mut bytes)).ok()?;
    Some(bytes)
}
