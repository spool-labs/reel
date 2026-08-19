//! What this machine argues a volume's knobs should be, beside the configured ones
//!
//! The one report that asks the machine under a root rather than the volume on
//! it, so it answers where nothing has been written yet and opens nothing.

use std::path::Path;

use crate::config::{IoBackend, ReelConfig, DEFAULT_FD_CACHE};
use crate::reel::bias::{access_ranges, MachineFacts, Plane, RingAvailability};
use crate::units::ByteCount;

/// The machine's facts beside the knobs a configuration asks for
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct DoctorReport {
    /// The directory the facts were read under
    pub root: String,

    /// Memory this machine has, where it says
    pub memory_bytes: Option<u64>,

    /// The filesystem's capacity under the root
    pub capacity_bytes: Option<u64>,

    /// What is already on that filesystem
    pub occupied_bytes: Option<u64>,

    /// The block size the device reads and writes in
    pub logical_block_bytes: Option<u64>,

    /// Whether the drive is spinning, where it says
    pub is_rotational: Option<bool>,

    /// Independent access ranges the drive declares, one actuator apiece
    pub actuator_ranges: usize,

    /// Open files this process may hold, where there is a limit
    pub open_file_limit: Option<u64>,

    /// Whether io_uring is reachable here, and what stops it where it is not
    pub ring: String,

    /// Bytes the tails hold open while the volume is idle
    pub idle_reservation_bytes: u64,

    /// Why the verdict chose the plane it chose
    pub because: String,

    /// The plane the configuration opens on
    pub configured_plane: String,

    /// The plane this machine argues for
    pub verdict_plane: String,

    /// The record size the configuration maps above, absent where it maps none
    pub configured_map_above: Option<u64>,

    /// The record size this machine argues for mapping above
    pub verdict_map_above: Option<u64>,

    /// Why the verdict chose that mapping floor
    pub map_because: String,

    /// Whether the configuration reads ranges of a record
    pub configured_ranged_reads: String,

    /// Whether this machine argues for ranged reads
    pub verdict_ranged_reads: String,

    /// Whether the configuration preallocates a segment before writing it
    pub configured_preallocate: String,

    /// Whether this machine argues for preallocation
    pub verdict_preallocate: String,

    /// Open segment files the shipped default caches
    pub shipped_fd_cache: u64,

    /// Open segment files this machine argues for caching
    pub verdict_fd_cache: u64,
}

/// Read this machine's facts under a root and weigh them against a configuration
///
/// The root need not hold a volume: nothing is opened, and the facts are the
/// filesystem's and the kernel's.
pub fn doctor(root: &Path, config: &ReelConfig) -> DoctorReport {
    let facts = MachineFacts::read(root);
    let reservation = config.segment_bytes.to_bytes() * config.tail_count() as u64;
    let verdict = facts.verdict(reservation);
    let spans = access_ranges(root);
    DoctorReport {
        root: root.display().to_string(),
        memory_bytes: facts.memory_bytes,
        capacity_bytes: facts.volume_capacity_bytes,
        occupied_bytes: facts.volume_bytes,
        logical_block_bytes: facts.logical_block_bytes,
        is_rotational: facts.is_rotational,
        actuator_ranges: spans.len(),
        open_file_limit: facts.open_file_limit,
        ring: ring_label(facts.ring).to_string(),
        idle_reservation_bytes: reservation,
        because: verdict.because.to_string(),
        configured_plane: format!("{:?}", configured_plane(config.io_backend)),
        verdict_plane: format!("{:?}", verdict.plane),
        configured_map_above: config.map_above.map(ByteCount::to_bytes),
        verdict_map_above: verdict.map_above.map(ByteCount::to_bytes),
        map_because: verdict.map_because.to_string(),
        configured_ranged_reads: format!("{:?}", config.ranged_reads),
        verdict_ranged_reads: format!("{:?}", verdict.ranged_reads),
        configured_preallocate: format!("{:?}", config.preallocate),
        verdict_preallocate: format!("{:?}", verdict.preallocate),
        shipped_fd_cache: DEFAULT_FD_CACHE,
        verdict_fd_cache: verdict.fd_cache,
    }
}

/// The plane a configured backend actually opens on
fn configured_plane(backend: IoBackend) -> Plane {
    match backend.is_direct() {
        true => Plane::Direct,
        false => Plane::Buffered,
    }
}

fn ring_label(ring: RingAvailability) -> &'static str {
    match ring {
        RingAvailability::Available => "available",
        RingAvailability::Unsupported => "no io_uring in this kernel",
        RingAvailability::Denied => "refused, by policy or sandbox",
        RingAvailability::NotLinux => "not linux",
    }
}
