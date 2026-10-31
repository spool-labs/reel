//! The write-out pass, which puts every journal's rows down together and then the reel's cut

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockWriteGuard, Weak};

use crate::error::Result;
use crate::format::journal::{pack_cut, CUT_FILE};
use crate::format::lsn::{Lsn, LsnCounter};
use crate::io::op::{FileId, WriteBuf};
use crate::reel::segment::IoDriver;
use crate::sync::{lock, try_lock, write};

use super::journal::Journal;
use super::Active;

/// The reel's cut file and the cut it holds
struct CutFile {
    /// The open cut file, or none before the first pass writes it
    file: Option<FileId>,

    /// The cut the file holds, which a pass only raises
    written: Lsn,
}

/// Every tail and journal of a reel, so one pass writes all their rows at one point in call order
pub(crate) struct Writeout {
    /// The driver the cut file is written through
    driver: Arc<IoDriver>,

    /// Where the cut file sits, at the reel's root
    path: PathBuf,

    /// Held through a whole pass, so passes write their rows and cuts in turn
    cut: Mutex<CutFile>,

    /// Every tail's active segment, write-locked together so no record draws a number mid-cut
    tails: Mutex<Vec<Weak<RwLock<Active>>>>,

    /// Every journal that can still hold rows, active or waiting on its seal
    journals: Mutex<Vec<Weak<Journal>>>,
}

/// One journal's rows a pass took, to write once every tail is let go
struct Taken {
    journal: Arc<Journal>,
    rows: Vec<u8>,
}

/// One pass's hold on the cut file, taken before any tail's lock
pub(crate) struct Pass<'reel> {
    writeout: &'reel Writeout,
    cut: MutexGuard<'reel, CutFile>,
}

impl Writeout {
    pub(crate) fn new(root: &Path, driver: Arc<IoDriver>) -> Writeout {
        Writeout {
            driver,
            path: root.join(CUT_FILE),
            cut: Mutex::new(CutFile {
                file: None,
                written: Lsn::NONE,
            }),
            tails: Mutex::new(Vec::new()),
            journals: Mutex::new(Vec::new()),
        }
    }

    /// Start from the cut a reopen found, so no pass writes a lower one
    pub(crate) fn recover(&self, cut: Lsn) {
        let mut held = lock(&self.cut);
        held.written = held.written.max(cut);
    }

    /// The cut the file holds, or the one a reopen found
    pub(super) fn cut(&self) -> Lsn {
        lock(&self.cut).written
    }

    /// Count a tail in, so every pass stops its draws
    pub(super) fn enlist_tail(&self, active: &Arc<RwLock<Active>>) {
        lock(&self.tails).push(Arc::downgrade(active));
    }

    /// Count a journal in, so every pass writes its rows
    pub(super) fn enlist(&self, journal: &Arc<Journal>) {
        lock(&self.journals).push(Arc::downgrade(journal));
    }

    /// Wait for the cut file, for a caller that must take a tail's lock after it
    pub(super) fn begin(&self) -> Pass<'_> {
        Pass {
            writeout: self,
            cut: lock(&self.cut),
        }
    }

    /// Write every journal's rows and then the cut, returning the cut
    pub(super) fn write_out(&self, lsn: &LsnCounter) -> Result<Lsn> {
        self.begin().run(lsn, None)
    }

    /// The same pass, skipped when another one is running, since that one takes these rows too
    pub(super) fn try_write_out(&self, lsn: &LsnCounter) {
        let Some(cut) = try_lock(&self.cut) else {
            return;
        };
        // A failed write keeps its rows, so the next pass writes them or fails
        let _ = Pass {
            writeout: self,
            cut,
        }
        .run(lsn, None);
    }
}

impl Pass<'_> {
    /// Take every journal's rows at one point in call order, write them, then write the cut
    pub(super) fn run(mut self, lsn: &LsnCounter, held: Option<&RwLock<Active>>) -> Result<Lsn> {
        let tails = live(&self.writeout.tails);
        // A record draws its number under its tail's read lock and pushes its row before letting go
        let mut guards: Vec<RwLockWriteGuard<'_, Active>> = Vec::with_capacity(tails.len());
        for tail in &tails {
            let is_held = held.is_some_and(|held| std::ptr::eq(Arc::as_ptr(tail), held));
            if !is_held {
                guards.push(write(tail));
            }
        }
        // No draw is open on any tail, so every number below the counter has its row pushed
        let cut = Lsn(lsn.peek().as_u64().saturating_sub(1));
        let journals = live(&self.writeout.journals);
        let mut taken = Vec::with_capacity(journals.len());
        for journal in journals {
            let rows = journal.take_pending();
            taken.push(Taken { journal, rows });
        }
        drop(guards);

        let mut outcome = Ok(());
        for entry in taken {
            if let Err(error) = entry.journal.write_taken(entry.rows) {
                outcome = outcome.and(Err(error));
            }
        }
        outcome?;
        self.write_cut(cut)?;
        Ok(cut)
    }

    /// Write the cut to the reel's cut file, after every row at or below it is down
    fn write_cut(&mut self, cut: Lsn) -> Result<()> {
        if cut <= self.cut.written {
            return Ok(());
        }
        let driver = &self.writeout.driver;
        let file = match self.cut.file {
            Some(file) => file,
            None => {
                let file = driver.open(&self.writeout.path, true)?;
                self.cut.file = Some(file);
                file
            }
        };
        driver.writev_all(file, 0, vec![WriteBuf::owned(pack_cut(cut).to_vec())])?;
        self.cut.written = cut;
        Ok(())
    }
}

impl Drop for Writeout {
    fn drop(&mut self) {
        let held = self.cut.get_mut().unwrap_or_else(|held| held.into_inner());
        if let Some(file) = held.file.take() {
            let _ = self.driver.close(file);
        }
    }
}

/// Every entry still alive, dropping the ones that went
fn live<Held>(list: &Mutex<Vec<Weak<Held>>>) -> Vec<Arc<Held>> {
    let mut held = lock(list);
    held.retain(|entry| entry.strong_count() > 0);
    let mut alive = Vec::with_capacity(held.len());
    for entry in held.iter() {
        if let Some(entry) = entry.upgrade() {
            alive.push(entry);
        }
    }
    alive
}
