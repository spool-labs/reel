//! An open handle to a simulated file

use std::io::{Error, ErrorKind, IoSlice, IoSliceMut, Result};
use std::sync::{Arc, Mutex};

use reel_api::units::{ByteCount, FileOffset, ProcessId};

use crate::guard::hold;
use crate::io::IoFile;
use crate::io::simulated::simulated_disk::{InodeNumber, OperationCount, SimulatedDisk};

/// A handle that only works for the process that opened it
#[derive(Debug)]
pub struct SimulatedFile {
    pub disk: Arc<Mutex<SimulatedDisk>>,
    pub inode: InodeNumber,
    pub process: ProcessId,
    pub is_writable: bool,
}

impl SimulatedFile {
    /// Refuse a handle that outlived its process or a write through a read-only one
    pub fn check(&self, disk: &mut SimulatedDisk, is_write: bool) -> Result<()> {
        disk.enter()?;
        if disk.process != self.process {
            return Err(Error::other("simulated handle outlived its process"));
        }
        if is_write && !self.is_writable {
            return Err(Error::from(ErrorKind::PermissionDenied));
        }
        Ok(())
    }

    /// Leave the leading part of a write a crash stopped, since a kill can stop one part way
    pub fn tear(&self, disk: &mut SimulatedDisk, offset: FileOffset, mut bytes: Vec<u8>) {
        let kept = disk.random.below(bytes.len() as u64 + 1);
        bytes.truncate(kept as usize);

        let Some(image) = disk.inodes.get(&self.inode) else {
            return;
        };
        let end = offset.0.saturating_add(kept);
        let growth = end.saturating_sub(image.visible.len() as u64);
        if !disk.has_room(self.inode, ByteCount(growth)) {
            return;
        }
        if let Some(image) = disk.inodes.get_mut(&self.inode) {
            image.write(offset, bytes);
        }
    }
}

impl Drop for SimulatedFile {
    fn drop(&mut self) {
        let mut disk = hold(&self.disk);
        if let Some(image) = disk.inodes.get_mut(&self.inode) {
            image.handle_count = image.handle_count.saturating_sub(1);
        }
        disk.collect(self.inode);
    }
}

impl IoFile for SimulatedFile {
    fn read_at(&self, offset: FileOffset, buf: &mut [u8]) -> Result<ByteCount> {
        let mut disk = hold(&self.disk);
        self.check(&mut disk, false)?;

        let Some(image) = disk.inodes.get(&self.inode) else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        let start = image.visible.len().min(offset.0 as usize);
        let end = image.visible.len().min(start + buf.len());
        let count = end - start;
        buf[..count].copy_from_slice(&image.visible[start..end]);
        disk.reads = OperationCount(disk.reads.0 + 1);
        disk.bytes_read = ByteCount(disk.bytes_read.0 + count as u64);
        Ok(ByteCount(count as u64))
    }

    fn read_exact_at(&self, offset: FileOffset, buf: &mut [u8]) -> Result<()> {
        let count = self.read_at(offset, buf)?;
        if count.0 < buf.len() as u64 {
            return Err(Error::from(ErrorKind::UnexpectedEof));
        }
        Ok(())
    }

    fn read_parts_at(&self, offset: FileOffset, parts: &mut [IoSliceMut]) -> Result<()> {
        let mut disk = hold(&self.disk);
        self.check(&mut disk, false)?;

        let Some(image) = disk.inodes.get(&self.inode) else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        let mut start = offset.0 as usize;
        let mut total = 0;
        for part in parts.iter_mut() {
            let end = start + part.len();
            let Some(bytes) = image.visible.get(start..end) else {
                return Err(Error::from(ErrorKind::UnexpectedEof));
            };
            part.copy_from_slice(bytes);
            total += part.len() as u64;
            start = end;
        }

        disk.reads = OperationCount(disk.reads.0 + 1);
        disk.bytes_read = ByteCount(disk.bytes_read.0 + total);
        Ok(())
    }

    fn write_at(&self, offset: FileOffset, parts: &[IoSlice]) -> Result<()> {
        let mut bytes = Vec::new();
        for part in parts {
            bytes.extend_from_slice(part);
        }

        let mut disk = hold(&self.disk);
        let was_crashed = disk.is_crashed;
        if let Err(error) = self.check(&mut disk, true) {
            if !was_crashed && disk.is_crashed {
                self.tear(&mut disk, offset, bytes);
            }
            return Err(error);
        }

        let Some(image) = disk.inodes.get(&self.inode) else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        let end = offset.0.saturating_add(bytes.len() as u64);
        let growth = end.saturating_sub(image.visible.len() as u64);
        if growth > 0 && !disk.has_room(self.inode, ByteCount(growth)) {
            return Err(Error::from(ErrorKind::StorageFull));
        }

        if let Some(image) = disk.inodes.get_mut(&self.inode) {
            image.write(offset, bytes);
        }
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        let mut disk = hold(&self.disk);
        self.check(&mut disk, false)?;

        let is_failing = disk.failing_syncs.0 > 0;
        disk.failing_syncs = OperationCount(disk.failing_syncs.0.saturating_sub(1));
        let Some(image) = disk.inodes.get_mut(&self.inode) else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        if is_failing {
            image.fail_sync();
            return Err(Error::other("simulated sync failure"));
        }
        image.sync();
        Ok(())
    }

    fn length(&self) -> Result<ByteCount> {
        let mut disk = hold(&self.disk);
        self.check(&mut disk, false)?;
        match disk.inodes.get(&self.inode) {
            Some(image) => Ok(ByteCount(image.visible.len() as u64)),
            None => Err(Error::from(ErrorKind::NotFound)),
        }
    }

    fn truncate(&self, length: ByteCount) -> Result<()> {
        let mut disk = hold(&self.disk);
        self.check(&mut disk, true)?;
        match disk.inodes.get_mut(&self.inode) {
            Some(image) => {
                image.truncate(length);
                Ok(())
            },
            None => Err(Error::from(ErrorKind::NotFound)),
        }
    }
}
