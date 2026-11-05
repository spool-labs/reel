//! The simulated backend: one seed drives the disk, the clock and the job order

use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use reel_api::units::{ByteCount, FileOffset, Nanoseconds, ProcessId};

use reel::guard::hold;
use reel::io::Io;
use reel::io::job::{Job, JobStep};
use reel::io::lock_outcome::LockOutcome;
use crate::count_pause::CountPause;
use crate::file_image::{BitMask, SECTOR_BYTES};
use crate::seeded_random::Seed;
use crate::simulated_disk::{FileSyncFault, InodeNumber, OperationCount, SimulatedDisk, SyncHook};
use crate::simulated_file::SimulatedFile;
use crate::simulated_task::{JobCount, JobIndex, JobSlot, JobWait, SimulatedTask};

/// The clock moves this far on every read
pub const CLOCK_TICK: Nanoseconds = Nanoseconds(1_000);

/// The clock moves this far each time a caller waits
pub const PAUSE_TICK: Nanoseconds = Nanoseconds(1_000_000);

/// A deterministic backend that a test drives by hand
#[derive(Clone)]
pub struct SimulatedIo {
    pub disk: Arc<Mutex<SimulatedDisk>>,
    pub slots: Arc<Mutex<Vec<JobSlot>>>,
    pub clock: Arc<AtomicU64>,
}

/// A held simulated lock, released when dropped
pub struct SimulatedLock {
    pub disk: Arc<Mutex<SimulatedDisk>>,
    pub path: PathBuf,
    pub process: ProcessId,
}

impl Drop for SimulatedLock {
    fn drop(&mut self) {
        let mut disk = hold(&self.disk);
        if disk.locks.get(&self.path) == Some(&self.process) {
            disk.locks.remove(&self.path);
        }
    }
}

impl SimulatedIo {
    /// Start an empty simulation from a seed
    pub fn new(seed: Seed) -> Self {
        Self {
            disk: Arc::new(Mutex::new(SimulatedDisk::new(seed))),
            slots: Arc::new(Mutex::new(Vec::new())),
            clock: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Declare a disk under a path, with a size in bytes
    pub fn add_disk(&self, root: &Path, capacity: ByteCount) {
        let mut disk = hold(&self.disk);
        disk.capacities.insert(root.to_path_buf(), capacity);
    }

    /// Kill the process: running jobs end, and the kernel keeps every write
    pub fn kill(&self) {
        hold(&self.slots).clear();
        hold(&self.disk).kill();
    }

    /// Cut the power: running jobs end, and unsynced changes each live or die
    pub fn power_cut(&self) {
        hold(&self.slots).clear();
        hold(&self.disk).power_cut();
    }

    /// Crash the process when this many more operations have run
    pub fn arm_crash(&self, operations: OperationCount) {
        hold(&self.disk).operations_until_crash = Some(operations);
    }

    /// Take back a crash that has not fired
    pub fn disarm_crash(&self) {
        hold(&self.disk).operations_until_crash = None;
    }

    /// Check whether an armed crash has fired
    pub fn is_crashed(&self) -> bool {
        hold(&self.disk).is_crashed
    }

    /// Read how many reads have run and how many bytes they returned
    pub fn read_totals(&self) -> (OperationCount, ByteCount) {
        let disk = hold(&self.disk);
        (disk.reads, disk.bytes_read)
    }

    /// Count the handles still open
    pub fn open_handle_count(&self) -> u64 {
        hold(&self.disk).open_handles.len() as u64
    }

    /// Count the uses of a closed handle and the second closes
    pub fn handle_faults(&self) -> OperationCount {
        hold(&self.disk).handle_faults
    }

    /// Make the next reads fail with an error a device never gives
    pub fn fail_reads(&self, count: OperationCount) {
        hold(&self.disk).failing_reads = count;
    }

    /// Make the next writes fail
    pub fn fail_writes(&self, count: OperationCount) {
        hold(&self.disk).failing_writes = count;
    }

    /// Fail one read after this many pass
    pub fn fail_reads_after(&self, reads: OperationCount) {
        hold(&self.disk).reads_before_failure = Some(reads);
    }

    /// Fail one write after this many pass
    pub fn fail_writes_after(&self, writes: OperationCount) {
        hold(&self.disk).writes_before_failure = Some(writes);
    }

    /// Make the next writes report a full disk
    pub fn fill_writes(&self, count: OperationCount) {
        hold(&self.disk).full_writes = count;
    }

    /// Make the next directory syncs fail
    pub fn fail_directory_syncs(&self, count: OperationCount) {
        hold(&self.disk).failing_directory_syncs = count;
    }

    /// Make the next file creates fail
    pub fn fail_creates(&self, count: OperationCount) {
        hold(&self.disk).failing_creates = count;
    }

    /// Run some work right after the next sync lands
    pub fn after_next_sync(&self, work: Box<dyn FnOnce() + Send>) {
        hold(&self.disk).after_sync = SyncHook(Some(work));
    }

    /// Make one sync of one kind of file fail, after that kind has synced `skip` times
    pub fn fail_file_sync(&self, extension: &'static str, skip: OperationCount) {
        hold(&self.disk).file_sync_fault = Some(FileSyncFault { extension, skip });
    }

    /// Take back an aimed sync failure that has not landed
    pub fn clear_file_sync(&self) {
        hold(&self.disk).file_sync_fault = None;
    }

    /// Make the next syncs fail
    pub fn fail_syncs(&self, count: OperationCount) {
        hold(&self.disk).failing_syncs = count;
    }

    /// Make the sector under an offset fail every read that touches it
    pub fn bad_sector(&self, path: &Path, offset: FileOffset) -> Result<()> {
        let mut disk = hold(&self.disk);
        let inode = disk.inode_of(path)?;
        if let Some(image) = disk.inodes.get_mut(&inode) {
            image.bad_sectors.insert(offset.0 / SECTOR_BYTES);
        }
        Ok(())
    }

    /// Flip bits of one byte of a file in place
    pub fn rot(&self, path: &Path, offset: FileOffset, mask: BitMask) -> Result<()> {
        let mut disk = hold(&self.disk);
        let inode = disk.inode_of(path)?;
        if let Some(image) = disk.inodes.get_mut(&inode) {
            image.rot(offset, mask);
        }
        Ok(())
    }

    /// Move the clock forward
    pub fn advance_clock(&self, elapsed: Nanoseconds) {
        self.clock.fetch_add(elapsed.0, Ordering::Relaxed);
    }

    /// Read the clock without moving it
    pub fn clock_reading(&self) -> Nanoseconds {
        Nanoseconds(self.clock.load(Ordering::Relaxed))
    }

    /// Draw a number below the bound from the simulation's seed
    pub fn draw(&self, bound: u64) -> u64 {
        hold(&self.disk).random.below(bound)
    }

    /// Count the jobs that have not ended
    pub fn live_jobs(&self) -> JobCount {
        let mut live = 0;
        for slot in hold(&self.slots).iter() {
            if slot.work.is_some() {
                live += 1;
            }
        }
        JobCount(live)
    }

    /// Run one step of the job at an index, whatever it waits for
    pub fn step_job(&self, index: JobIndex) -> Option<JobStep> {
        let pause = CountPause::new();
        let mut work = {
            let mut slots = hold(&self.slots);
            let slot = slots.get_mut(index.0)?;
            slot.is_woken = false;
            slot.work.take()?
        };

        // the job calls back into this backend, so nothing is locked while it runs
        // the job is engine code, and it counts
        drop(pause);
        let step = work.step();
        let _pause = CountPause::new();

        let mut slots = hold(&self.slots);
        if let Some(slot) = slots.get_mut(index.0) {
            slot.wait = match step {
                JobStep::Worked | JobStep::Finished => JobWait::Nothing,
                JobStep::Idle => JobWait::Wake,
                JobStep::IdleUntil { deadline } => JobWait::Clock { deadline },
            };
            if step != JobStep::Finished && !slot.is_stopped {
                slot.work = Some(work);
            }
        }
        Some(step)
    }

    /// Run one step of a runnable job the seed picks, if any job can run
    pub fn step_any(&self) -> Option<JobStep> {
        let pause = CountPause::new();
        let now = self.clock_reading();
        let mut runnable: Vec<JobIndex> = Vec::new();
        for (index, slot) in hold(&self.slots).iter().enumerate() {
            if slot.is_runnable(now) {
                runnable.push(JobIndex(index));
            }
        }

        let pick = self.draw(runnable.len() as u64) as usize;
        let index = *runnable.get(pick)?;
        drop(runnable);
        drop(pause);
        self.step_job(index)
    }

    /// Wrap an inode in a handle for the running process
    pub fn handle(&self, disk: &mut SimulatedDisk, inode: InodeNumber, is_writable: bool) -> SimulatedFile {
        SimulatedFile {
            disk: Arc::clone(&self.disk),
            inode,
            handle: disk.open_handle(inode),
            process: disk.process,
            is_writable,
        }
    }
}

impl Io for SimulatedIo {
    type File = SimulatedFile;
    type Lock = SimulatedLock;
    type Task = SimulatedTask;

    fn create_file(&self, path: &Path, reserved: ByteCount) -> Result<SimulatedFile> {
        let _pause = CountPause::new();
        let mut disk = hold(&self.disk);
        disk.enter()?;
        if disk.failing_creates.0 > 0 {
            disk.failing_creates = OperationCount(disk.failing_creates.0 - 1);
            return Err(Error::other("simulated create failure"));
        }
        let inode = disk.create(path, reserved)?;
        Ok(self.handle(&mut disk, inode, true))
    }

    fn open_file(&self, path: &Path) -> Result<SimulatedFile> {
        let _pause = CountPause::new();
        let mut disk = hold(&self.disk);
        disk.enter()?;
        let inode = disk.inode_of(path)?;
        Ok(self.handle(&mut disk, inode, true))
    }

    fn open_read_only(&self, path: &Path) -> Result<SimulatedFile> {
        let _pause = CountPause::new();
        let mut disk = hold(&self.disk);
        disk.enter()?;
        let inode = disk.inode_of(path)?;
        Ok(self.handle(&mut disk, inode, false))
    }

    fn close_file(&self, file: SimulatedFile) {
        let _pause = CountPause::new();
        hold(&self.disk).close_handle(file.handle);
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        let _pause = CountPause::new();
        let mut disk = hold(&self.disk);
        disk.enter()?;
        disk.unlink(path)
    }

    fn rename_file(&self, from: &Path, to: &Path) -> Result<()> {
        let _pause = CountPause::new();
        let mut disk = hold(&self.disk);
        disk.enter()?;
        disk.rename(from, to)
    }

    fn create_directory(&self, path: &Path) -> Result<()> {
        let _pause = CountPause::new();
        let mut disk = hold(&self.disk);
        disk.enter()?;
        for ancestor in path.ancestors() {
            if !disk.has_directory(ancestor) {
                disk.directories.push(ancestor.to_path_buf());
            }
        }
        Ok(())
    }

    fn sync_directory(&self, path: &Path) -> Result<()> {
        let _pause = CountPause::new();
        let mut disk = hold(&self.disk);
        disk.enter()?;
        if disk.failing_directory_syncs.0 > 0 {
            disk.failing_directory_syncs = OperationCount(disk.failing_directory_syncs.0 - 1);
            return Err(Error::other("simulated directory sync failure"));
        }
        disk.sync_directory(path);
        Ok(())
    }

    fn list_directory(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let _pause = CountPause::new();
        let mut disk = hold(&self.disk);
        disk.enter()?;
        if !disk.has_directory(path) {
            return Err(Error::from(ErrorKind::NotFound));
        }

        let mut paths = Vec::new();
        for name in disk.names.keys() {
            if name.parent() == Some(path) {
                paths.push(name.clone());
            }
        }
        Ok(paths)
    }

    fn free_space(&self, path: &Path) -> Result<ByteCount> {
        let _pause = CountPause::new();
        let mut disk = hold(&self.disk);
        disk.enter()?;
        Ok(disk.free_space(path))
    }

    fn lock(&self, path: &Path) -> Result<LockOutcome<SimulatedLock>> {
        let mut disk = hold(&self.disk);
        disk.enter()?;
        if let Some(holder) = disk.locks.get(path) {
            return Ok(LockOutcome::Refused { holder: *holder });
        }

        let process = disk.process;
        disk.locks.insert(path.to_path_buf(), process);
        Ok(LockOutcome::Held(SimulatedLock {
            disk: Arc::clone(&self.disk),
            path: path.to_path_buf(),
            process,
        }))
    }

    fn now(&self) -> Nanoseconds {
        let _pause = CountPause::new();
        let reading = self.clock.fetch_add(CLOCK_TICK.0, Ordering::Relaxed);
        Nanoseconds(reading)
    }

    fn random_word(&self) -> u64 {
        let _pause = CountPause::new();
        hold(&self.disk).random.next_word()
    }

    fn pause(&self) {
        // one thread drives a simulation, so a waiter runs the work it waits for
        self.advance_clock(PAUSE_TICK);
        self.step_any();
    }

    fn job_stack_bytes(&self) -> ByteCount {
        ByteCount(0)
    }

    fn spawn_job<Work: Job>(&self, name: &'static str, job: Work) -> Result<SimulatedTask> {
        let process = hold(&self.disk).process;
        let mut slots = hold(&self.slots);
        let index = JobIndex(slots.len());
        slots.push(JobSlot {
            name,
            process,
            work: Some(Box::new(job)),
            wait: JobWait::Nothing,
            is_woken: false,
            is_stopped: false,
        });
        Ok(SimulatedTask {
            slots: Arc::clone(&self.slots),
            index,
            process,
        })
    }
}
