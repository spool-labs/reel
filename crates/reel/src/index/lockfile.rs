//! Per-reel ownership lock that makes a second writer fail loudly
//!
//! A writable open takes an exclusive advisory lock on a file in the volume, so a
//! second writer is rejected at open instead of interleaving two appenders. The lock
//! lives on the open file description, so it dies with the descriptor when the owner
//! exits or crashes. A read-only open takes no lock.

use std::fs::{File, OpenOptions, TryLockError};
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{ReelError, Result};

/// Where the kernel exposes the current boot identifier on Linux
const BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";

/// An exclusive advisory lock held for the lifetime of a writable reel
pub struct OwnershipLock {
    /// Held so the advisory lock outlives this value, never read
    _holder: File,
}

impl OwnershipLock {
    /// Take the lock on a reel's mount file, failing if another owner holds it
    pub fn try_acquire(path: &Path) -> Result<OwnershipLock> {
        let holder = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        acquire_exclusive(&holder, path)?;
        record_owner(&holder);
        Ok(OwnershipLock { _holder: holder })
    }
}

fn acquire_exclusive(holder: &File, path: &Path) -> Result<()> {
    match holder.try_lock() {
        Ok(()) => Ok(()),
        Err(TryLockError::WouldBlock) => Err(ReelError::LockHeld(held_message(path))),
        Err(TryLockError::Error(error)) => Err(ReelError::Io(error)),
    }
}

/// The contention error, carrying whatever the owner recorded about itself
///
/// The read races the owner rewriting the file, which can only stale the message,
/// never the verdict the kernel lock already gave.
fn held_message(path: &Path) -> String {
    let recorded = std::fs::read_to_string(path).unwrap_or_default();
    let recorded = recorded.trim().to_string();
    match recorded.is_empty() {
        true => format!("another owner already holds {}", path.display()),
        false => format!(
            "another owner already holds {} ({recorded})",
            path.display()
        ),
    }
}

fn record_owner(holder: &File) {
    let line = format!(
        "pid={} acquired_unix={} boot_id={} version={}\n",
        std::process::id(),
        unix_seconds(),
        boot_id(),
        env!("CARGO_PKG_VERSION"),
    );
    let mut writer = holder;
    let _ = writer.set_len(0);
    let _ = writer.write_all(line.as_bytes());
}

fn unix_seconds() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(elapsed) => elapsed.as_secs(),
        Err(_) => 0,
    }
}

fn boot_id() -> String {
    match std::fs::read_to_string(BOOT_ID_PATH) {
        Ok(value) => value.trim().to_string(),
        Err(_) => "unknown".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    // a first acquire succeeds and writes the owner diagnostics
    #[test]
    fn acquire_records_owner() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("reel-0007.mount");

        let _lock = OwnershipLock::try_acquire(&path).expect("acquire");

        let contents = std::fs::read_to_string(&path).expect("read");
        assert!(contents.starts_with("pid="));
        assert!(contents.contains("acquired_unix="));
        assert!(contents.contains("version="));
    }

    // a second acquire against a held lock fails loudly, naming the owner
    #[test]
    fn second_acquire_fails() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("reel-0007.mount");
        let _first = OwnershipLock::try_acquire(&path).expect("first");

        let second = OwnershipLock::try_acquire(&path);

        let held = second.err().expect("second must fail");
        let ReelError::LockHeld(message) = held else {
            panic!("expected LockHeld");
        };
        assert!(message.contains("pid="));
        assert!(message.contains("boot_id="));
    }

    // releasing the lock lets a later acquire take it again
    #[test]
    fn release_allows_reacquire() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("reel-0007.mount");

        {
            let _first = OwnershipLock::try_acquire(&path).expect("first");
        }
        let again = OwnershipLock::try_acquire(&path);

        assert!(again.is_ok());
    }
}
