//! The simulated file system: names, inodes, locks, faults and capacity

use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::path::{Path, PathBuf};

use reel_api::units::{ByteCount, FileOffset, ProcessId};

use crate::io::simulated::file_image::FileImage;
use crate::io::simulated::seeded_random::{Seed, SeededRandom};

/// The disk size used under a path with no declared disk
pub const UNBOUNDED_CAPACITY: ByteCount = ByteCount(1 << 50);
/// Process ids in a simulation count up from here
pub const FIRST_PROCESS: ProcessId = ProcessId(1_000);
/// Each unsynced directory change survives a power cut one time in this many
pub const NAME_SURVIVAL_ODDS: u64 = 2;

/// The id of one file's bytes, apart from any name it has
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InodeNumber(pub u64);

/// A count of operations on the simulated disk
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationCount(pub u64);

/// A change to a directory that no directory sync has covered
#[derive(Clone, Debug)]
pub enum NameChange {
    /// A name now points at an inode
    Link { path: PathBuf, inode: InodeNumber },
    /// A name is gone
    Unlink { path: PathBuf },
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
    pub reads: OperationCount,
    pub bytes_read: ByteCount,
    pub operations_until_crash: Option<OperationCount>,
    pub is_crashed: bool,
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
            reads: OperationCount::default(),
            bytes_read: ByteCount::default(),
            operations_until_crash: None,
            is_crashed: false,
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
            image.write(FileOffset(0), vec![0; reserved.0 as usize]);
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

    /// End the running process: the kernel keeps every write and every lock is freed
    pub fn kill(&mut self) {
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
