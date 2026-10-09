//! A durable copy of the volume at a cue point, made by hard-linking its sealed segments

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

    /// How many segment files were linked into the target
    pub segments: usize,
}

/// Sealed segments of a reel directory, up to but not including a boundary
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

/// Hard-link one sealed set into a staging directory, failing if any segment is gone
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

/// The staging directory a checkpoint builds in, a sibling of the target
pub fn staging_of(target: &Path) -> PathBuf {
    let mut name = target.as_os_str().to_owned();
    name.push(".tmp");
    PathBuf::from(name)
}

/// The target's name, refused when the reel would mistake it for its own
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
        || text == crate::engine::LOCK_FILE;
    if reserved {
        return Err(ReelError::Rejected(format!(
            "{text} is a name the reel itself uses, so a checkpoint cannot carry it",
        )));
    }
    Ok(name)
}

/// Write the copy's manifest, listing every piece, durably into the home staging
pub fn write_copy_manifest(staging: &Path, pieces: &[PathBuf]) -> Result<()> {
    durable_write(
        &staging.join(crate::reel::volumes::MANIFEST_NAME),
        &crate::reel::volumes::manifest_bytes(pieces),
    )
}

/// Write one piece's marker into its staging, holding its published path
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
