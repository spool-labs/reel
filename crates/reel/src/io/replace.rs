//! Replacing a file whole, so a crash leaves the old file or the new one and never part of either

use std::io::{ErrorKind, IoSlice, Result};
use std::path::Path;

use reel_core::units::{ByteCount, FileOffset};

use crate::io::{Io, IoFile};

/// Replace a file with some bytes: write them to a fresh file beside it, sync that, and move it into place
pub fn replace_whole<Backend: Io>(io: &Backend, directory: &Path, new: &Path, target: &Path, parts: &[IoSlice<'_>]) -> Result<()> {
    // a save stopped part way leaves its new file, and the next save needs the name
    match io.remove_file(new) {
        Ok(()) => {},
        Err(error) if error.kind() == ErrorKind::NotFound => {},
        Err(error) => return Err(error),
    }
    let file = io.create_file(new, ByteCount(0))?;
    let written = file.write_at(FileOffset(0), parts);
    let synced = written.and_then(|()| file.sync());
    io.close_file(file);
    synced?;
    move_into_place(io, directory, new, target)
}

/// Give a new file, already written and synced, the target's name, and sync the directory so the name lasts
pub fn move_into_place<Backend: Io>(io: &Backend, directory: &Path, new: &Path, target: &Path) -> Result<()> {
    io.rename_file(new, target)?;
    io.sync_directory(directory)
}
