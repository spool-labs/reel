//! The plain backend for a harness's pool arm, which holds the ring back. The engine has no such setting

use std::io::Result;
use std::path::{Path, PathBuf};

use reel::io::wake_line::WakeLine;
use reel::io::plain::PlainIo;
use reel::io::host_memory::HostMemory;
use reel::io::{DiskSize, Io};
use reel::io::job::Job;
use reel::io::lock_outcome::LockOutcome;
use reel_core::units::{ByteCount, Nanoseconds};

/// The plain backend, with or without its ring
#[derive(Clone, Debug)]
pub struct HarnessIo {
    plain: PlainIo,
    has_ring: bool,
}

impl HarnessIo {
    /// Start a backend that opens the ring wherever the kernel offers one, as a store does
    pub fn ringed() -> Self {
        Self { plain: PlainIo::new(), has_ring: true }
    }

    /// Start a backend whose reads that would block run on the blocking pool
    pub fn ringless() -> Self {
        Self { plain: PlainIo::new(), has_ring: false }
    }
}

impl Io for HarnessIo {
    type File = <PlainIo as Io>::File;
    type Lock = <PlainIo as Io>::Lock;
    type Ring = <PlainIo as Io>::Ring;
    type Task = <PlainIo as Io>::Task;

    fn enter_quiet(&self) -> Nanoseconds {
        Nanoseconds(0)
    }

    fn open_ring(&self) -> Result<Option<Self::Ring>> {
        if self.has_ring { self.plain.open_ring() } else { Ok(None) }
    }

    fn create_file(&self, path: &Path, reserved: ByteCount) -> Result<Self::File> {
        self.plain.create_file(path, reserved)
    }

    fn open_file(&self, path: &Path) -> Result<Self::File> {
        self.plain.open_file(path)
    }

    fn open_read_only(&self, path: &Path) -> Result<Self::File> {
        self.plain.open_read_only(path)
    }

    fn open_direct(&self, path: &Path) -> Result<Option<Self::File>> {
        self.plain.open_direct(path)
    }

    fn can_free_blocks(&self, root: &Path) -> bool {
        self.plain.can_free_blocks(root)
    }

    fn close_file(&self, file: Self::File) {
        self.plain.close_file(file);
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        self.plain.remove_file(path)
    }

    fn rename_file(&self, from: &Path, to: &Path) -> Result<()> {
        self.plain.rename_file(from, to)
    }

    fn create_directory(&self, path: &Path) -> Result<()> {
        self.plain.create_directory(path)
    }

    fn sync_directory(&self, path: &Path) -> Result<()> {
        self.plain.sync_directory(path)
    }

    fn list_directory(&self, path: &Path) -> Result<Vec<PathBuf>> {
        self.plain.list_directory(path)
    }

    fn free_space(&self, path: &Path) -> Result<ByteCount> {
        self.plain.free_space(path)
    }

    fn capacity(&self, path: &Path) -> Result<Option<DiskSize>> {
        self.plain.capacity(path)
    }

    fn set_io_idle(&self, is_idle: bool) {
        self.plain.set_io_idle(is_idle);
    }

    fn host_memory(&self) -> HostMemory {
        self.plain.host_memory()
    }

    fn has_io_classes(&self, path: &Path) -> Option<bool> {
        self.plain.has_io_classes(path)
    }

    fn watch_pressure(&self, wake: std::sync::Arc<dyn Fn() + Send + Sync>) -> Option<reel::io::host_memory::PressureWatch> {
        self.plain.watch_pressure(wake)
    }

    fn memory_pressure(&self) -> Option<u32> {
        self.plain.memory_pressure()
    }

    fn memory_limit_hits(&self) -> Option<u64> {
        self.plain.memory_limit_hits()
    }

    fn is_rotational(&self, path: &Path) -> Option<bool> {
        self.plain.is_rotational(path)
    }

    fn lock(&self, path: &Path) -> Result<LockOutcome<Self::Lock>> {
        self.plain.lock(path)
    }

    fn now(&self) -> Nanoseconds {
        self.plain.now()
    }

    fn random_word(&self) -> u64 {
        self.plain.random_word()
    }

    fn pause_on(&self, line: &WakeLine, seen: u64) {
        self.plain.pause_on(line, seen);
    }

    fn pause(&self) {
        self.plain.pause();
    }

    fn spawn_job<Work: Job>(&self, name: &'static str, job: Work) -> Result<Self::Task> {
        self.plain.spawn_job(name, job)
    }

    fn job_stack_bytes(&self) -> ByteCount {
        self.plain.job_stack_bytes()
    }
}
