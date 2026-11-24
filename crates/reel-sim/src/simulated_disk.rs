//! The simulated file system: names, inodes, locks, faults and capacity

use std::collections::BTreeMap;
use std::fmt::{self, Debug, Formatter};
use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use reel::io::DiskSize;
use reel::io::host_memory::HostMemory;
use reel_core::units::{ByteCount, ProcessId};

use crate::file_image::FileImage;
use crate::seeded_random::{Seed, SeededRandom};

/// The disk size used under a path with no declared disk
pub const UNBOUNDED_CAPACITY: ByteCount = ByteCount(1 << 50);
/// The host a simulation runs on: 64 GiB with 48 free and no control group limit, so admission always has room
pub const SIMULATED_HOST: HostMemory = HostMemory { total: ByteCount(64 << 30), available: ByteCount(48 << 30), group_room: None, group_limit: None };
/// Process ids in a simulation count up from here
pub const FIRST_PROCESS: ProcessId = ProcessId(1_000);
/// Each unsynced directory change survives a power cut one time in this many
pub const NAME_SURVIVAL_ODDS: u64 = 2;

/// The id of one file's bytes, apart from any name it has
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InodeNumber(pub u64);

/// The id of one open, which every copy of its handle shares
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HandleId(pub u64);

/// A count of operations on the simulated disk
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationCount(pub u64);

/// Work a test runs right after the next sync lands
#[derive(Default)]
pub struct SyncHook(pub Option<Box<dyn FnOnce() + Send>>);

impl Debug for SyncHook {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("SyncHook")
    }
}

/// Work a test runs at the start of every read
#[derive(Clone, Default)]
pub struct ReadHook(pub Option<Arc<dyn Fn() + Send + Sync>>);

impl Debug for ReadHook {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReadHook")
    }
}

/// A change to a directory that no directory sync has covered
#[derive(Clone, Debug)]
pub enum NameChange {
    /// A name now points at an inode
    Link { path: PathBuf, inode: InodeNumber },
    /// A name is gone
    Unlink { path: PathBuf },
}

/// A write failure aimed at the files whose name ends with a suffix, after some writes to them pass
#[derive(Clone, Copy, Debug)]
pub struct FileWriteFault {
    pub suffix: &'static str,
    pub skip: OperationCount,
}

/// A sync failure aimed at one kind of file, after that kind has synced so many times
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileSyncFault {
    pub extension: &'static str,
    pub skip: OperationCount,
}

/// The whole simulated file system
#[derive(Debug)]
pub struct SimulatedDisk {
    pub random: SeededRandom,
    pub process: ProcessId,
    pub names: BTreeMap<PathBuf, InodeNumber>,
    pub durable_names: BTreeMap<PathBuf, InodeNumber>,
    pub pending_names: Vec<NameChange>,
    pub directories: Vec<PathBuf>,
    pub inodes: BTreeMap<InodeNumber, FileImage>,
    pub next_inode: InodeNumber,
    pub locks: BTreeMap<PathBuf, ProcessId>,
    pub capacities: BTreeMap<PathBuf, ByteCount>,
    pub failing_syncs: OperationCount,
    pub file_sync_fault: Option<FileSyncFault>,
    pub file_write_fault: Option<FileWriteFault>,
    pub failing_writes: OperationCount,
    pub write_calls: OperationCount,
    pub writes_before_failure: Option<OperationCount>,
    pub reads_before_failure: Option<OperationCount>,
    pub reads_before_process_failure: Option<OperationCount>,
    pub failing_reads: OperationCount,
    pub full_writes: OperationCount,
    pub failing_creates: OperationCount,
    pub failing_removes: OperationCount,
    pub failing_directory_syncs: OperationCount,
    /// Directory syncs that pass before the failing ones start
    pub skipped_directory_syncs: OperationCount,
    pub after_sync: SyncHook,
    pub on_read: ReadHook,
    pub reads: OperationCount,
    pub writeback_bytes: u64,
    pub warm_bytes: usize,
    pub warm_tries: u64,
    pub refuses_no_wait: bool,
    /// What reads through a direct handle meet, so a test can refuse them or hand back a flipped byte while plain reads are clean
    pub direct_reads: DirectReads,
    pub tries: OperationCount,
    pub cold_tries: OperationCount,
    pub ring_reads: OperationCount,
    pub writes: OperationCount,
    pub bytes_read: ByteCount,
    pub operations_until_crash: Option<OperationCount>,
    pub is_crashed: bool,
    pub open_handles: BTreeMap<HandleId, InodeNumber>,
    pub next_handle: HandleId,
    pub handle_faults: OperationCount,
    pub refuses_free_blocks: bool,
    pub freed_bytes: ByteCount,
    pub host_memory: HostMemory,
    pub is_reserve_held: bool,
    pub held_reserves: u64,
}

impl SimulatedDisk {
    /// Start an empty disk from a seed
    pub fn new(seed: Seed) -> Self {
        Self {
            random: SeededRandom::new(seed),
            process: FIRST_PROCESS,
            names: BTreeMap::new(),
            durable_names: BTreeMap::new(),
            pending_names: Vec::new(),
            directories: Vec::new(),
            inodes: BTreeMap::new(),
            next_inode: InodeNumber::default(),
            locks: BTreeMap::new(),
            capacities: BTreeMap::new(),
            failing_syncs: OperationCount::default(),
            file_sync_fault: None,
            file_write_fault: None,
            failing_writes: OperationCount::default(),
            write_calls: OperationCount(0),
            writes_before_failure: None,
            reads_before_failure: None,
            reads_before_process_failure: None,
            failing_reads: OperationCount::default(),
            full_writes: OperationCount::default(),
            failing_creates: OperationCount::default(),
            failing_removes: OperationCount::default(),
            failing_directory_syncs: OperationCount::default(),
            skipped_directory_syncs: OperationCount::default(),
            after_sync: SyncHook::default(),
            on_read: ReadHook::default(),
            reads: OperationCount::default(),
            writeback_bytes: 0,
            warm_bytes: 0,
            warm_tries: 0,
            refuses_no_wait: false,
            direct_reads: DirectReads::Plain,
            tries: OperationCount::default(),
            cold_tries: OperationCount::default(),
            is_reserve_held: false,
            held_reserves: 0,
            ring_reads: OperationCount::default(),
            writes: OperationCount::default(),
            bytes_read: ByteCount::default(),
            operations_until_crash: None,
            is_crashed: false,
            open_handles: BTreeMap::new(),
            next_handle: HandleId::default(),
            handle_faults: OperationCount::default(),
            refuses_free_blocks: false,
            freed_bytes: ByteCount(0),
            host_memory: SIMULATED_HOST,
        }
    }

    /// Count one operation, and fail it once the process has crashed
    pub fn enter(&mut self) -> Result<()> {
        if let Some(remaining) = self.operations_until_crash {
            if remaining.0 == 0 {
                self.is_crashed = true;
                self.operations_until_crash = None;
            } else {
                self.operations_until_crash = Some(OperationCount(remaining.0 - 1));
            }
        }
        if self.is_crashed {
            return Err(Error::other("simulated process crashed"));
        }
        Ok(())
    }

    /// Find the inode behind a name
    pub fn inode_of(&self, path: &Path) -> Result<InodeNumber> {
        match self.names.get(path) {
            Some(inode) => Ok(*inode),
            None => Err(Error::from(ErrorKind::NotFound)),
        }
    }

    /// Check the path's parent exists
    pub fn check_parent(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent()
            && self.has_directory(parent)
        {
            return Ok(());
        }
        Err(Error::from(ErrorKind::NotFound))
    }

    /// Check whether a directory exists
    pub fn has_directory(&self, path: &Path) -> bool {
        for directory in &self.directories {
            if directory == path {
                return true;
            }
        }
        false
    }

    /// Make a new empty file under a name
    pub fn create(&mut self, path: &Path, reserved: ByteCount) -> Result<InodeNumber> {
        self.check_parent(path)?;
        if self.names.contains_key(path) {
            return Err(Error::from(ErrorKind::AlreadyExists));
        }
        if reserved > self.free_space(path) {
            return Err(Error::from(ErrorKind::StorageFull));
        }

        let inode = self.next_inode;
        self.next_inode = InodeNumber(inode.0 + 1);
        let mut image = FileImage {
            disk: self.disk_of(path),
            is_named: true,
            ..FileImage::default()
        };
        if reserved.0 > 0 {
            image.reserve(reserved);
        }
        self.inodes.insert(inode, image);
        self.names.insert(path.to_path_buf(), inode);
        self.pending_names.push(NameChange::Link {
            path: path.to_path_buf(),
            inode,
        });
        Ok(inode)
    }

    /// Take a name away, and free the bytes once no handle holds them
    pub fn unlink(&mut self, path: &Path) -> Result<()> {
        let inode = self.inode_of(path)?;
        self.names.remove(path);
        self.pending_names.push(NameChange::Unlink {
            path: path.to_path_buf(),
        });
        if let Some(image) = self.inodes.get_mut(&inode) {
            image.is_named = false;
        }
        self.collect(inode);
        Ok(())
    }

    /// Point a new name at a file and drop the old name
    pub fn rename(&mut self, from: &Path, to: &Path) -> Result<()> {
        let inode = self.inode_of(from)?;
        self.check_parent(to)?;

        if let Some(replaced) = self.names.insert(to.to_path_buf(), inode)
            && replaced != inode
            && let Some(image) = self.inodes.get_mut(&replaced)
        {
            image.is_named = false;
            self.collect(replaced);
        }
        self.names.remove(from);
        self.pending_names.push(NameChange::Link {
            path: to.to_path_buf(),
            inode,
        });
        self.pending_names.push(NameChange::Unlink {
            path: from.to_path_buf(),
        });
        Ok(())
    }

    /// Drop an inode once nothing can reach it, a power cut included
    pub fn collect(&mut self, inode: InodeNumber) {
        let is_held = match self.inodes.get(&inode) {
            Some(image) => image.is_held(),
            None => true,
        };
        if is_held {
            return;
        }

        for durable in self.durable_names.values() {
            if *durable == inode {
                return;
            }
        }
        self.inodes.remove(&inode);
    }

    /// Make a directory's pending changes survive a power cut
    pub fn sync_directory(&mut self, path: &Path) {
        let mut remaining = Vec::new();
        for change in self.pending_names.drain(..) {
            let is_inside = match &change {
                NameChange::Link { path: name, .. } => name.parent() == Some(path),
                NameChange::Unlink { path: name } => name.parent() == Some(path),
            };
            if is_inside {
                apply_name(&mut self.durable_names, &change);
            } else {
                remaining.push(change);
            }
        }
        self.pending_names = remaining;
    }

    /// Find the disk a path sits on
    pub fn disk_of(&self, path: &Path) -> Option<PathBuf> {
        let mut found: Option<PathBuf> = None;
        for root in self.capacities.keys() {
            if path.starts_with(root) {
                found = Some(root.clone());
            }
        }
        found
    }

    /// Add up the bytes a disk holds, for unlinked files with open handles too
    pub fn used_space(&self, root: &Path) -> ByteCount {
        let mut used: u64 = 0;
        for image in self.inodes.values() {
            if image.is_held() && image.disk.as_deref() == Some(root) {
                used = used.saturating_add(image.visible.len() as u64);
            }
        }
        ByteCount(used)
    }

    /// Read how much a disk can still take
    pub fn disk_room(&self, root: &Path) -> ByteCount {
        let capacity = match self.capacities.get(root) {
            Some(capacity) => *capacity,
            None => UNBOUNDED_CAPACITY,
        };
        ByteCount(capacity.0.saturating_sub(self.used_space(root).0))
    }

    /// Read the size of the disk under a path, with the disk's place among the declared ones as its filesystem, or nothing for a
    /// path on no declared disk
    pub fn disk_size(&self, path: &Path) -> Option<DiskSize> {
        let root = self.disk_of(path)?;
        let filesystem = self.capacities.keys().position(|declared| *declared == root)? as u64;
        Some(DiskSize { filesystem, bytes: *self.capacities.get(&root)? })
    }

    /// Read how much the disk under a path can still take
    pub fn free_space(&self, path: &Path) -> ByteCount {
        match self.disk_of(path) {
            Some(root) => self.disk_room(&root),
            None => UNBOUNDED_CAPACITY,
        }
    }

    /// Check a file can grow by some bytes on its disk
    pub fn has_room(&self, inode: InodeNumber, growth: ByteCount) -> bool {
        let Some(image) = self.inodes.get(&inode) else {
            return false;
        };
        match &image.disk {
            Some(root) => growth <= self.disk_room(root),
            None => true,
        }
    }

    /// Open a handle on an inode
    pub fn open_handle(&mut self, inode: InodeNumber) -> HandleId {
        let handle = self.next_handle;
        self.next_handle = HandleId(handle.0 + 1);
        self.open_handles.insert(handle, inode);
        if let Some(image) = self.inodes.get_mut(&inode) {
            image.handle_count += 1;
        }
        handle
    }

    /// Check an armed write failure lands on this file now, and spend it if so
    pub fn takes_write_fault(&mut self, inode: InodeNumber) -> bool {
        let Some(fault) = self.file_write_fault else {
            return false;
        };
        let is_aimed = self.names.iter().any(|(path, named)| *named == inode && path.to_str().is_some_and(|name| name.ends_with(fault.suffix)));
        if !is_aimed {
            return false;
        }
        if fault.skip.0 > 0 {
            self.file_write_fault = Some(FileWriteFault { suffix: fault.suffix, skip: OperationCount(fault.skip.0 - 1) });
            return false;
        }
        self.file_write_fault = None;
        true
    }

    /// Check an armed sync failure lands on this file now, and spend it if so
    pub fn takes_sync_fault(&mut self, inode: InodeNumber) -> bool {
        let Some(fault) = self.file_sync_fault else {
            return false;
        };
        let is_aimed = self.names.iter().any(|(path, named)| {
            *named == inode && path.extension().is_some_and(|extension| extension == fault.extension)
        });
        if !is_aimed {
            return false;
        }
        if fault.skip.0 > 0 {
            self.file_sync_fault = Some(FileSyncFault {
                extension: fault.extension,
                skip: OperationCount(fault.skip.0 - 1),
            });
            return false;
        }
        self.file_sync_fault = None;
        true
    }

    /// Count a try, and say how many bytes of a read the kernel holds
    pub fn take_try(&mut self, wanted: usize) -> usize {
        self.tries = OperationCount(self.tries.0 + 1);
        if self.warm_tries > 0 {
            self.warm_tries -= 1;
            return wanted;
        }
        let warm = self.warm_bytes.min(wanted);
        if warm < wanted {
            self.cold_tries = OperationCount(self.cold_tries.0 + 1);
        }
        warm
    }

    /// Close a handle, and count a second close as a fault
    pub fn close_handle(&mut self, handle: HandleId) {
        let Some(inode) = self.open_handles.remove(&handle) else {
            self.handle_faults = OperationCount(self.handle_faults.0 + 1);
            return;
        };
        if let Some(image) = self.inodes.get_mut(&inode) {
            image.handle_count = image.handle_count.saturating_sub(1);
        }
        self.collect(inode);
    }

    /// Close every handle, as the kernel does when a process ends
    pub fn close_all_handles(&mut self) {
        let handles = mem::take(&mut self.open_handles);
        for inode in handles.into_values() {
            if let Some(image) = self.inodes.get_mut(&inode) {
                image.handle_count = image.handle_count.saturating_sub(1);
            }
            self.collect(inode);
        }
    }

    /// End the running process: the kernel keeps every write and every lock is freed
    pub fn kill(&mut self) {
        self.close_all_handles();
        self.process = ProcessId(self.process.0 + 1);
        self.locks.clear();
        self.is_crashed = false;
        self.operations_until_crash = None;
    }

    /// Cut the power: only what a sync covered is sure to remain
    pub fn power_cut(&mut self) {
        self.kill();

        for change in mem::take(&mut self.pending_names) {
            if self.random.one_in(NAME_SURVIVAL_ODDS) {
                apply_name(&mut self.durable_names, &change);
            }
        }
        // a link can outlive its own unlink in this shuffle, and a real file system has no name for a freed inode
        let inodes = &self.inodes;
        self.durable_names.retain(|_path, inode| inodes.contains_key(inode));
        self.names.clone_from(&self.durable_names);

        for image in self.inodes.values_mut() {
            image.power_cut(&mut self.random);
            image.is_named = false;
        }
        for inode in self.names.values() {
            if let Some(image) = self.inodes.get_mut(inode) {
                image.is_named = true;
            }
        }
    }
}

/// Apply one directory change to a set of names
pub fn apply_name(names: &mut BTreeMap<PathBuf, InodeNumber>, change: &NameChange) {
    match change {
        NameChange::Link { path, inode } => {
            names.insert(path.clone(), *inode);
        },
        NameChange::Unlink { path } => {
            names.remove(path);
        },
    }
}

/// What a read through a direct handle meets
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DirectReads {
    /// The bytes on the disk, as a plain read gives
    #[default]
    Plain,
    /// A refusal, as a device whose blocks pass the alignment or a mount that takes the flag and not the read gives
    Refused,
    /// A device error, as a bad sector under a good copy in the page cache gives a direct read
    Broken,
    /// The disk's bytes with the middle one of each read flipped, as a disk copy rotted under a clean one in the page cache reads direct
    Flipped,
}
