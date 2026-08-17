//! The devices one reel places its segments across
//!
//! One reel owns a list of volume roots and places whole segments: ids stay
//! global, the index never encodes a path, and this table built at open is the
//! only thing that knows where a segment's file lives.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::VolumeClass;
use crate::error::{ReelError, Result};
use crate::format::loc::SegmentId;
use crate::io::op::WriteBuf;
use crate::reel::segment::IoDriver;
use crate::reel::segment_file_name;
use crate::sync::checked::{read, write, RwLock};

/// File on the first volume naming every root this reel spans
pub const MANIFEST_NAME: &str = "reel.volumes";

/// File on every root past the first, naming the root it was mounted as
///
/// What tells a fresh empty volume apart from an unmounted mountpoint. A
/// manifest-named root without its marker refuses the open, and a marker naming
/// some other path is two stores' volumes crossed.
pub const MARKER_NAME: &str = "reel.volume";

/// Segments of headroom a volume keeps to stay in the draw
///
/// The segment being drawn plus room for compaction to land output beside it,
/// since compaction is how a full volume gets space back.
pub const WATERMARK_SEGMENTS: u64 = 2;

/// A free-space reading no filesystem has answered yet, which reads as plenty
///
/// Erring toward plenty keeps a volume the machine cannot measure in the draw,
/// and the ENOSPC retry is what catches the ones that lied.
const UNKNOWN_FREE: u64 = u64::MAX;

/// The roots one reel places segments across, and which root holds which
pub struct Volumes {
    /// The volume roots, in the order the manifest records them, the first home
    roots: Vec<PathBuf>,

    /// Which root holds each segment, indexed by segment number
    table: RwLock<Vec<u8>>,

    /// What each root's filesystem will still hand out, refreshed per draw
    free: Vec<AtomicU64>,

    /// The tier each root serves, in root order, the first always fast
    classes: Vec<VolumeClass>,

    /// Which roots the operator declared dead, in root order, keeping their index
    dead: Vec<bool>,

    /// Free bytes under which a volume stops attracting draws
    watermark: u64,
}

impl Volumes {
    /// A volume set over these roots, the first of them home
    pub fn new(
        roots: Vec<PathBuf>,
        classes: Vec<VolumeClass>,
        dead: Vec<bool>,
        watermark: u64,
    ) -> Volumes {
        assert!(!roots.is_empty(), "a reel needs at least one volume");
        assert_eq!(roots.len(), classes.len(), "every root carries a class");
        assert_eq!(
            roots.len(),
            dead.len(),
            "every root answers whether it lives"
        );
        assert_eq!(
            classes[0],
            VolumeClass::Fast,
            "the reel root is the fast tier"
        );
        assert!(!dead[0], "the reel root cannot be declared dead");
        let free = roots.iter().map(|_| AtomicU64::new(UNKNOWN_FREE)).collect();
        Volumes {
            roots,
            table: RwLock::new(Vec::new()),
            free,
            classes,
            dead,
            watermark,
        }
    }

    /// Whether the operator declared this root dead
    pub fn is_dead(&self, at: usize) -> bool {
        self.dead.get(at).copied().unwrap_or(false)
    }

    /// The roots declared dead, for the open that reports what is degraded
    pub fn dead_roots(&self) -> Vec<&Path> {
        self.roots
            .iter()
            .enumerate()
            .filter(|(at, _)| self.dead[*at])
            .map(|(_, root)| root.as_path())
            .collect()
    }

    /// The first volume, where the lock and the manifest live
    pub fn first(&self) -> &Path {
        &self.roots[0]
    }

    /// Every root, in manifest order
    pub fn roots(&self) -> &[PathBuf] {
        &self.roots
    }

    /// How many volumes the reel spans
    pub fn len(&self) -> usize {
        self.roots.len()
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    /// Path of one segment's file, on whichever root holds it
    pub fn path_of(&self, id: SegmentId) -> PathBuf {
        self.root_at(self.root_of(id)).join(segment_file_name(id))
    }

    /// Which root a segment lives on
    pub fn root_of(&self, id: SegmentId) -> usize {
        read(&self.table)
            .get(id.as_u32() as usize)
            .copied()
            .unwrap_or(0) as usize
    }

    /// Record which root holds a segment, growing the table to reach it
    pub fn place(&self, id: SegmentId, root: usize) {
        debug_assert!(root < self.roots.len(), "a segment placed on no volume");
        let at = id.as_u32() as usize;
        let mut table = write(&self.table);
        if table.len() <= at {
            table.resize(at + 1, 0);
        }
        table[at] = root as u8;
    }

    /// The directory a segment's file sits in
    pub fn root_dir_of(&self, id: SegmentId) -> &Path {
        self.root_at(self.root_of(id))
    }

    /// The tier one root serves
    pub fn class_of(&self, at: usize) -> VolumeClass {
        self.classes.get(at).copied().unwrap_or_default()
    }

    /// Whether any living root serves the capacity tier
    pub fn has_capacity(&self) -> bool {
        self.classes
            .iter()
            .zip(&self.dead)
            .any(|(class, dead)| *class == VolumeClass::Capacity && !dead)
    }

    /// The root index of the nth living fast volume, which a tail pins to
    pub fn fast_at(&self, ordinal: usize) -> Option<usize> {
        self.classes
            .iter()
            .zip(&self.dead)
            .enumerate()
            .filter(|(_, (class, dead))| **class == VolumeClass::Fast && !**dead)
            .nth(ordinal)
            .map(|(at, _)| at)
    }

    /// The root a fresh segment lands on, within one tier
    ///
    /// A pinned tail draws on its own volume while that volume stands above the
    /// watermark, which keeps one sequential stream per device. Every other draw
    /// takes the volume in class with the most free space.
    pub fn draw(&self, pin: Option<usize>, class: VolumeClass) -> usize {
        if self.roots.len() == 1 {
            return 0;
        }
        self.refresh_free();
        if let Some(home) = pin {
            if self.free[home].load(Ordering::Relaxed) >= self.watermark {
                return home;
            }
        }
        self.pick(&[], class).unwrap_or(0)
    }

    /// The next root after a draw came back full, or nothing left in class
    ///
    /// The readings were refreshed by the draw that failed, and a retry is racing
    /// ENOSPC either way, so this does not ask the filesystem again.
    pub fn draw_past(&self, ruled_out: &[usize], class: VolumeClass) -> Option<usize> {
        self.pick(ruled_out, class)
    }

    /// The most free root in class above the watermark, else in class at all
    ///
    /// Below the watermark placement is degraded, not refused. The class boundary
    /// still holds: a full fast tier never spills fresh writes onto spindles priced
    /// for bytes at rest. Ties keep the lowest index.
    fn pick(&self, ruled_out: &[usize], class: VolumeClass) -> Option<usize> {
        let mut best: Option<(usize, u64)> = None;
        let mut afloat: Option<(usize, u64)> = None;
        for at in 0..self.roots.len() {
            if self.classes[at] != class || self.dead[at] || ruled_out.contains(&at) {
                continue;
            }
            let free = self.free[at].load(Ordering::Relaxed);
            if best.is_none_or(|(_, held)| free > held) {
                best = Some((at, free));
            }
            if free >= self.watermark && afloat.is_none_or(|(_, held)| free > held) {
                afloat = Some((at, free));
            }
        }
        afloat.or(best).map(|(at, _)| at)
    }

    /// Ask every root's filesystem what it will still hand out
    ///
    /// A filesystem that cannot answer leaves the last reading standing.
    fn refresh_free(&self) {
        for (at, root) in self.roots.iter().enumerate() {
            if let Some(bytes) = crate::reel::bias::available_bytes(root) {
                self.free[at].store(bytes, Ordering::Relaxed);
            }
        }
    }

    fn root_at(&self, at: usize) -> &Path {
        // A table byte can only name a root place() checked, but a table from a
        // manifestless future degrades to home rather than panicking.
        self.roots
            .get(at)
            .map(PathBuf::as_path)
            .unwrap_or_else(|| self.first())
    }
}

/// What the manifest says, compared against what the config gave
///
/// The manifest is authoritative about presence and order: the placement table
/// stores root indices, so a reordered list would silently remap every placed
/// segment. New roots append, which is how a drive is added, and anything else
/// refuses rather than reading a typo as loss.
pub fn reconcile_manifest(stored: &str, roots: &[PathBuf]) -> Result<()> {
    let named: Vec<&str> = stored
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    if named.len() > roots.len() {
        return Err(ReelError::Config(format!(
            "the volume manifest names {} roots and the configuration {}; \
             a missing mount refuses rather than reading as loss",
            named.len(),
            roots.len(),
        )));
    }
    for (at, name) in named.iter().enumerate() {
        if roots[at].as_os_str() != std::ffi::OsStr::new(name) {
            return Err(ReelError::Config(format!(
                "volume {at} is {} in the manifest and {} in the configuration; \
                 the placement table is indexed by that order and will not guess",
                name,
                roots[at].display(),
            )));
        }
    }
    Ok(())
}

/// Read the manifest, verify it against the roots, and keep it current
///
/// Written on the first open and rewritten whenever the list grows, with every
/// named root proving it is mounted through its marker. A read-only open verifies
/// without writing, since a reader holds no lock.
pub fn ensure_manifest(
    driver: &IoDriver,
    roots: &[PathBuf],
    dead: &[bool],
    read_only: bool,
) -> Result<()> {
    let home = &roots[0];
    let path = home.join(MANIFEST_NAME);
    let held = driver
        .list_or_empty(home)?
        .into_iter()
        .find(|entry| entry.name == MANIFEST_NAME)
        .map(|entry| entry.len);

    let wanted = manifest_bytes(roots);
    let mut named = 0usize;
    let mut current = false;
    if let Some(len) = held {
        let file = driver.open(&path, false)?;
        let outcome = driver.pread(file, 0, len);
        driver.close(file)?;
        let bytes = outcome?;
        let stored = String::from_utf8_lossy(&bytes).into_owned();
        reconcile_manifest(&stored, roots)?;
        named = stored
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count();
        current = bytes == wanted;
    } else if read_only || roots.len() == 1 {
        // Nothing stands to be verified and nothing is owed: a reader holds no
        // lock, and a single-volume store defers its manifest until it grows a
        // second root, which is when missing-mount protection starts protecting
        // anything.
        return Ok(());
    }

    // Every root the manifest names answers for itself on every open: a missing
    // mount is an empty directory in the right place, and only the marker tells it
    // from a fresh volume. A root declared dead is excused.
    for at in 1..named.min(roots.len()) {
        if !dead[at] {
            verify_marker(driver, &roots[at])?;
        }
    }
    if read_only || current {
        return Ok(());
    }

    // A root joining now takes its marker before the manifest names it, so a crash
    // between the two leaves an unnamed root the next open re-adds rather than a
    // named root that cannot prove itself.
    for at in named.max(1)..roots.len() {
        if !dead[at] {
            write_marker(driver, &roots[at])?;
        }
    }

    // The reconcile only lets the list grow, so writing from zero never leaves a
    // stale tail behind the new bytes. Best effort: a full device must still open,
    // since opening is how compaction gets the room back.
    let written = (|| -> Result<()> {
        let file = driver.open(&path, true)?;
        let outcome = (|| -> Result<()> {
            driver.writev_all(file, 0, vec![WriteBuf::owned(wanted)])?;
            driver.sync_full(file)
        })();
        driver.close(file)?;
        outcome?;
        driver.sync_dir(home)
    })();
    if let Err(error) = written {
        tracing::warn!("the volume manifest did not land and will be retried next open: {error}");
    }
    Ok(())
}

/// Prove a manifest-named root is the volume it claims to be
fn verify_marker(driver: &IoDriver, root: &Path) -> Result<()> {
    let path = root.join(MARKER_NAME);
    let file = match driver.open(&path, false) {
        Ok(file) => file,
        Err(error) if error.is_missing() => {
            return Err(ReelError::Config(format!(
                "volume {} carries no marker, so it is unmounted or was never \
                 initialized; only a volume declared dead opens without one",
                root.display(),
            )));
        }
        Err(error) => return Err(error),
    };
    let outcome = (|| {
        let len = driver.length(file)?;
        driver.pread(file, 0, len)
    })();
    driver.close(file)?;
    let bytes = outcome?;
    let held = String::from_utf8_lossy(&bytes);
    if held.trim() != root.as_os_str().to_string_lossy() {
        return Err(ReelError::Config(format!(
            "volume {} carries a marker naming {}, which is another store's \
             volume standing where this one should be",
            root.display(),
            held.trim(),
        )));
    }
    Ok(())
}

/// The marker's bytes for a root, which are the root's own name
pub(crate) fn marker_bytes(root: &Path) -> Vec<u8> {
    let mut bytes = root.as_os_str().to_string_lossy().into_owned().into_bytes();
    bytes.push(b'\n');
    bytes
}

/// Stamp a joining root with its own name, durably, before the manifest grows
fn write_marker(driver: &IoDriver, root: &Path) -> Result<()> {
    let path = root.join(MARKER_NAME);
    let bytes = marker_bytes(root);
    let file = driver.open(&path, true)?;
    let outcome = (|| -> Result<()> {
        driver.writev_all(file, 0, vec![WriteBuf::owned(bytes)])?;
        driver.sync_full(file)
    })();
    driver.close(file)?;
    outcome?;
    driver.sync_dir(root)
}

/// The manifest's bytes for a root list
pub fn manifest_bytes(roots: &[PathBuf]) -> Vec<u8> {
    let mut out = String::new();
    for root in roots {
        out.push_str(&root.to_string_lossy());
        out.push('\n');
    }
    out.into_bytes()
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    /// A two-root fast set on fake paths, so the readings stay what a test stores
    fn two(watermark: u64) -> Volumes {
        Volumes::new(
            vec![PathBuf::from("/a"), PathBuf::from("/b")],
            vec![VolumeClass::Fast, VolumeClass::Fast],
            vec![false, false],
            watermark,
        )
    }

    /// A fast home with a capacity and a fast extra, fake paths as above
    fn mixed(watermark: u64) -> Volumes {
        Volumes::new(
            vec![
                PathBuf::from("/a"),
                PathBuf::from("/b"),
                PathBuf::from("/c"),
            ],
            vec![VolumeClass::Fast, VolumeClass::Capacity, VolumeClass::Fast],
            vec![false, false, false],
            watermark,
        )
    }

    /// The mixed set with its last fast volume declared dead
    fn wounded(watermark: u64) -> Volumes {
        Volumes::new(
            vec![
                PathBuf::from("/a"),
                PathBuf::from("/b"),
                PathBuf::from("/c"),
            ],
            vec![VolumeClass::Fast, VolumeClass::Capacity, VolumeClass::Fast],
            vec![false, false, true],
            watermark,
        )
    }

    // an unplaced segment is on the first root, which is the one-element case
    #[test]
    fn unplaced_segments_are_home() {
        let volumes = two(0);
        assert_eq!(
            volumes.path_of(SegmentId(7)),
            PathBuf::from("/a/000007.reel")
        );
    }

    // a placed segment resolves to its own root and nobody else moves
    #[test]
    fn placement_routes_one_segment() {
        let volumes = two(0);
        volumes.place(SegmentId(9), 1);
        assert_eq!(
            volumes.path_of(SegmentId(9)),
            PathBuf::from("/b/000009.reel")
        );
        assert_eq!(
            volumes.path_of(SegmentId(8)),
            PathBuf::from("/a/000008.reel")
        );
        assert_eq!(
            volumes.path_of(SegmentId(10)),
            PathBuf::from("/a/000010.reel")
        );
    }

    // a pinned tail stays home above the watermark, whatever is freer elsewhere
    #[test]
    fn a_pinned_tail_draws_its_own_volume() {
        let volumes = two(100);
        volumes.free[0].store(150, Ordering::Relaxed);
        volumes.free[1].store(9_000, Ordering::Relaxed);
        assert_eq!(volumes.draw(Some(0), VolumeClass::Fast), 0);
    }

    // a pinned volume under the watermark degrades to the most free one
    #[test]
    fn a_low_volume_stops_attracting_its_tail() {
        let volumes = two(100);
        volumes.free[0].store(50, Ordering::Relaxed);
        volumes.free[1].store(9_000, Ordering::Relaxed);
        assert_eq!(volumes.draw(Some(0), VolumeClass::Fast), 1);
    }

    // an unpinned draw takes the most free volume, and ties keep the lowest index
    #[test]
    fn an_unpinned_draw_takes_the_most_free() {
        let volumes = two(100);
        volumes.free[0].store(500, Ordering::Relaxed);
        volumes.free[1].store(9_000, Ordering::Relaxed);
        assert_eq!(volumes.draw(None, VolumeClass::Fast), 1);
        volumes.free[0].store(9_000, Ordering::Relaxed);
        assert_eq!(volumes.draw(None, VolumeClass::Fast), 0);
    }

    // every volume under water still places, on whichever has the most room
    #[test]
    fn a_sinking_store_still_places() {
        let volumes = two(1_000);
        volumes.free[0].store(10, Ordering::Relaxed);
        volumes.free[1].store(20, Ordering::Relaxed);
        assert_eq!(volumes.draw(Some(0), VolumeClass::Fast), 1);
        assert_eq!(volumes.draw(None, VolumeClass::Fast), 1);
    }

    // an ENOSPC retry rules volumes out one by one and runs out honestly
    #[test]
    fn a_retry_runs_out_of_volumes() {
        let volumes = two(100);
        volumes.free[0].store(5_000, Ordering::Relaxed);
        volumes.free[1].store(9_000, Ordering::Relaxed);
        assert_eq!(volumes.draw_past(&[1], VolumeClass::Fast), Some(0));
        assert_eq!(volumes.draw_past(&[0, 1], VolumeClass::Fast), None);
    }

    // a fast draw never lands on capacity, however free the spindles are
    #[test]
    fn the_class_boundary_holds_both_ways() {
        let volumes = mixed(100);
        volumes.free[0].store(200, Ordering::Relaxed);
        volumes.free[1].store(1_000_000, Ordering::Relaxed);
        volumes.free[2].store(300, Ordering::Relaxed);
        assert_eq!(volumes.draw(None, VolumeClass::Fast), 2);
        assert_eq!(volumes.draw(None, VolumeClass::Capacity), 1);
        // A full fast tier fails in class rather than spilling onto spindles.
        assert_eq!(volumes.draw_past(&[0, 2], VolumeClass::Fast), None);
    }

    // tails pin over the fast volumes alone, in root order
    #[test]
    fn pins_skip_the_capacity_tier() {
        let volumes = mixed(100);
        assert_eq!(volumes.fast_at(0), Some(0));
        assert_eq!(volumes.fast_at(1), Some(2));
        assert_eq!(volumes.fast_at(2), None);
    }

    // a dead root leaves every scan, draw, and pin, and keeps its index
    #[test]
    fn a_dead_root_attracts_nothing() {
        let volumes = wounded(100);
        volumes.free[2].store(1_000_000, Ordering::Relaxed);
        assert_eq!(volumes.fast_at(0), Some(0));
        assert_eq!(volumes.fast_at(1), None);
        assert_eq!(volumes.draw(None, VolumeClass::Fast), 0);
        assert_eq!(volumes.draw_past(&[0], VolumeClass::Fast), None);
        assert_eq!(volumes.dead_roots(), vec![Path::new("/c")]);
        // Its placements still resolve, which is what keeps the table honest.
        volumes.place(SegmentId(4), 2);
        assert_eq!(
            volumes.path_of(SegmentId(4)),
            PathBuf::from("/c/000004.reel")
        );
    }

    // a store whose only capacity volume died stops offering the tier
    #[test]
    fn a_dead_capacity_tier_is_no_tier() {
        let volumes = Volumes::new(
            vec![PathBuf::from("/a"), PathBuf::from("/b")],
            vec![VolumeClass::Fast, VolumeClass::Capacity],
            vec![false, true],
            100,
        );
        assert!(!volumes.has_capacity());
    }

    // a manifest naming a root the config dropped or moved refuses the open
    #[test]
    fn a_missing_mount_refuses() {
        let roots = vec![PathBuf::from("/a")];
        assert!(reconcile_manifest("/a\n/b\n", &roots).is_err());
        assert!(reconcile_manifest("/a\n", &roots).is_ok());
        let grown = vec![PathBuf::from("/a"), PathBuf::from("/b")];
        assert!(reconcile_manifest("/a\n", &grown).is_ok());
        // Order is the placement table's index, so a reorder is a refusal too.
        let swapped = vec![PathBuf::from("/b"), PathBuf::from("/a")];
        assert!(reconcile_manifest("/a\n/b\n", &swapped).is_err());
    }
}
