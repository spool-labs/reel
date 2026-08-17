//! A durable copy of the volume as it stood at a cue point
//!
//! An immutable file plus a hard link is a free copy: a cue seals every tail, so
//! every version at or below it sits in a sealed segment under a footer, the cue
//! floor stops compaction retiring what the copy needs while the link pass runs,
//! and recovery rebuilds the index from the segments alone. The result is a
//! directory that opens rather than an archive that needs restoring.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::error::{ReelError, Result};
use crate::format::loc::SegmentId;
use crate::format::lsn::Lsn;
use crate::reel::{segment_file_name, segment_number};

/// What a checkpoint stood at, and what it linked to get there
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    /// The sequence number every version in the copy is at or below
    pub at: Lsn,

    /// Segment files linked into the target, which is the size of the work
    pub segments: usize,
}

/// Sealed segments of a reel directory, up to but not including a boundary
///
/// The boundary is the number the next segment will take, read once the cue has
/// sealed every tail. Numbers climb, so everything below it sealed at or before the
/// cue. A file whose name is not a segment number is not a segment.
pub fn sealed_below(dir: &Path, boundary: SegmentId) -> Result<BTreeSet<SegmentId>> {
    let mut sealed = BTreeSet::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(number) = name.to_str().and_then(segment_number) else {
            continue;
        };
        if number < boundary.as_u32() {
            sealed.insert(SegmentId(number));
        }
    }
    Ok(sealed)
}

/// Link one sealed set into a staging directory beside the target
///
/// Hard links, so the copy shares every byte with the volume it came from: a sealed
/// inode cannot change, and it cannot go away while a link holds it. A segment gone
/// before its link is taken fails the whole checkpoint rather than being skipped,
/// since a copy with a hole in it would paper over a retire the cue floor should
/// have held off.
pub fn link_into(dir: &Path, staging: &Path, sealed: &BTreeSet<SegmentId>) -> Result<()> {
    for segment in sealed {
        let name = segment_file_name(*segment);
        let from = dir.join(&name);
        let to = staging.join(&name);
        if let Err(error) = std::fs::hard_link(&from, &to) {
            if error.kind() == std::io::ErrorKind::NotFound {
                return Err(ReelError::Rejected(format!(
                    "segment {} was retired while the checkpoint linked it, so the copy \
                     would be missing records the cue could still see",
                    segment.as_u32(),
                )));
            }
            return Err(ReelError::Io(error));
        }
    }
    Ok(())
}

/// The staging directory a checkpoint builds in before it is anything
///
/// A sibling of the target rather than a child, so the rename that publishes it
/// is within one directory and cannot cross a filesystem by accident.
pub fn staging_of(target: &Path) -> PathBuf {
    let mut name = target.as_os_str().to_owned();
    name.push(".tmp");
    PathBuf::from(name)
}

/// The target's name, refused when the reel would mistake it for its own
///
/// A multi-volume checkpoint carries this name into every live volume's root, so a
/// name the reel uses itself would be read back as part of the store beside it.
pub fn checkpoint_name(target: &Path) -> Result<&std::ffi::OsStr> {
    let Some(name) = target.file_name() else {
        return Err(ReelError::Rejected(format!(
            "{} has no name for a checkpoint to carry",
            target.display(),
        )));
    };
    let text = name.to_string_lossy();
    let reserved = segment_number(&text).is_some()
        || text == crate::reel::volumes::MANIFEST_NAME
        || text == crate::reel::volumes::MARKER_NAME
        || text == crate::index::persisted::PERSISTED_INDEX
        || text == crate::engine::LOCK_FILE;
    if reserved {
        return Err(ReelError::Rejected(format!(
            "{text} is a name the reel itself uses, so a checkpoint cannot carry it",
        )));
    }
    Ok(name)
}

/// Write the copy's manifest into the home staging, naming every piece
///
/// The copy is a store, so it proves itself the way the live one does, and both the
/// manifest and the markers are synced before anything publishes.
pub fn write_copy_manifest(staging: &Path, pieces: &[PathBuf]) -> Result<()> {
    durable_write(
        &staging.join(crate::reel::volumes::MANIFEST_NAME),
        &crate::reel::volumes::manifest_bytes(pieces),
    )
}

/// Write one piece's marker into its staging, naming its published path
pub fn write_copy_marker(staging: &Path, published: &Path) -> Result<()> {
    durable_write(
        &staging.join(crate::reel::volumes::MARKER_NAME),
        &crate::reel::volumes::marker_bytes(published),
    )
}

fn durable_write(path: &Path, bytes: &[u8]) -> Result<()> {
    std::fs::write(path, bytes)?;
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}
