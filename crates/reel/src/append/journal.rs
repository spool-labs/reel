//! An open segment's journal file: the rows of what has landed, written at the tail's sync points
//!
//! A write adds its rows to the pending groups as it lands. A flush writes them to the
//! file ahead of its syncs, so whatever a flush makes durable has its rows in the
//! journal. Writeback pacing writes them too, which bounds what a volume that never
//! syncs loses to the stretch since the last pace.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::format::journal::{journal_path, push_group, JournalRow};
use crate::io::op::{FileId, WriteBuf};
use crate::reel::segment::IoDriver;
use crate::sync::{lock, try_lock};

/// One open segment's journal
pub(super) struct Journal {
    driver: Arc<IoDriver>,
    path: PathBuf,

    /// Groups for records that have landed and are not in the file yet
    pending: Mutex<Vec<u8>>,

    /// The file and how far it is written, held across a write so groups land in order
    file: Mutex<JournalFile>,
}

struct JournalFile {
    id: Option<FileId>,
    written: u64,
}

impl Journal {
    /// Create the journal of a segment being drawn
    ///
    /// Made beside the segment and before the directory sync the segment takes, so
    /// that one sync covers both directory entries. A drawn number is new, and an open unlinks
    /// every journal whose segment is gone, so the file starts empty.
    pub(super) fn create(driver: &Arc<IoDriver>, segment_path: &Path) -> Result<Journal> {
        let path = journal_path(segment_path);
        let id = driver.open(&path, true)?;
        Ok(Journal::over(driver, path, Some(id), 0))
    }

    /// Take a journal up again with only the rows a reopen accepted
    ///
    /// The rows go down as one group in a fresh file renamed over the old one, so a
    /// crash in between leaves one journal or the other and never a group that lists a
    /// record the reopen dropped.
    pub(super) fn resume(driver: &Arc<IoDriver>, segment_path: &Path, rows: &[JournalRow]) -> Result<Journal> {
        let path = journal_path(segment_path);
        let fresh = path.with_extension("rows.part");
        let id = driver.open(&fresh, true)?;
        driver.truncate(id, 0)?;
        let mut bytes = Vec::new();
        if !rows.is_empty() {
            push_group(rows, &mut bytes);
        }
        let written = bytes.len() as u64;
        if written > 0 {
            driver.writev_all(id, 0, vec![WriteBuf::owned(bytes)])?;
        }
        driver.sync_data(id)?;
        driver.rename(&fresh, &path)?;
        if let Some(dir) = path.parent() {
            driver.sync_dir(dir)?;
        }
        Ok(Journal::over(driver, path, Some(id), written))
    }

    /// A journal with no file, for a tail that holds no segment yet
    pub(super) fn none(driver: &Arc<IoDriver>) -> Journal {
        Journal::over(driver, PathBuf::new(), None, 0)
    }

    fn over(driver: &Arc<IoDriver>, path: PathBuf, id: Option<FileId>, written: u64) -> Journal {
        Journal {
            driver: Arc::clone(driver),
            path,
            pending: Mutex::new(Vec::new()),
            file: Mutex::new(JournalFile { id, written }),
        }
    }

    /// Add the rows of one write that has landed, as one group
    pub(super) fn push(&self, rows: &[JournalRow]) {
        if rows.is_empty() {
            return;
        }
        push_group(rows, &mut lock(&self.pending));
    }

    /// Write every pending group to the file and sync it
    ///
    /// Both under the file's lock, so a seal that removes the journal waits for the sync
    /// and never closes the file under it.
    pub(super) fn sync_pending(&self) -> Result<()> {
        let mut file = lock(&self.file);
        self.write_locked(&mut file)?;
        match file.id {
            Some(id) => self.driver.sync_data(id),
            None => Ok(()),
        }
    }

    /// The same write, skipped while another caller holds the file
    pub(super) fn try_write_pending(&self) {
        if let Some(mut file) = try_lock(&self.file) {
            let _ = self.write_locked(&mut file);
        }
    }

    fn write_locked(&self, file: &mut JournalFile) -> Result<()> {
        let Some(id) = file.id else {
            return Ok(());
        };
        let bytes = std::mem::take(&mut *lock(&self.pending));
        if bytes.is_empty() {
            return Ok(());
        }
        let len = bytes.len() as u64;
        self.driver.writev_all(id, file.written, vec![WriteBuf::owned(bytes)])?;
        file.written += len;
        Ok(())
    }

    /// Close and unlink the journal, once the segment's footer lists everything it held
    pub(super) fn remove(&self) {
        let mut file = lock(&self.file);
        lock(&self.pending).clear();
        if let Some(id) = file.id.take() {
            let _ = self.driver.close(id);
            let _ = self.driver.unlink(&self.path);
        }
    }
}

impl Drop for Journal {
    /// Write what is pending on the way out, so a store dropped without a seal leaves its rows
    fn drop(&mut self) {
        let mut file = lock(&self.file);
        let _ = self.write_locked(&mut file);
        if let Some(id) = file.id.take() {
            let _ = self.driver.close(id);
        }
    }
}
