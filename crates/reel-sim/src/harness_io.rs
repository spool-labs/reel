//! The plain backend for a harness's pool arm, which holds the ring back. The engine has no such setting

use std::io::Result;
use std::path::{Path, PathBuf};

use reel::io::plain::PlainIo;
use reel::io::Io;
use reel::io::job::Job;
use reel::io::lock_outcome::LockOutcome;
use reel_api::units::{ByteCount, Nanoseconds};

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
