//! Compares a configuration's knobs with what this machine suggests, without opening a volume

use std::path::Path;

use crate::config::{IoBackend, ReelConfig, DEFAULT_FD_CACHE};
use crate::reel::bias::{access_ranges, MachineFacts, Plane, RingAvailability};
use crate::report::doc::{Column, Doc, Note, Row, Table, Tone};
use crate::report::fmt;
use crate::report::render::Report;
use crate::units::ByteCount;

/// The machine's facts beside the knobs a configuration asks for
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct DoctorReport {
    /// The directory the facts were read under
    pub root: String,

    /// Memory this machine has, if known
    pub memory_bytes: Option<u64>,

    /// The filesystem's capacity under the root
    pub capacity_bytes: Option<u64>,

    /// What is already on that filesystem
    pub occupied_bytes: Option<u64>,

    /// The block size the device reads and writes in
    pub logical_block_bytes: Option<u64>,

    /// Whether the drive is spinning, if known
    pub is_rotational: Option<bool>,

    /// Independent access ranges the drive declares, one per actuator
    pub actuator_ranges: usize,

    /// Open files this process may hold, if limited
    pub open_file_limit: Option<u64>,

    /// Whether io_uring is available here, or what blocks it
    pub ring: String,

    /// Bytes the tails hold open while the volume is idle
    pub idle_reservation_bytes: u64,

    /// Why the verdict chose its plane
    pub because: String,

    /// The plane the configuration opens on
    pub configured_plane: String,

    /// The plane this machine suggests
    pub verdict_plane: String,

    /// The record size the configuration maps above, if any
    pub configured_map_above: Option<u64>,

    /// The record size this machine suggests mapping above
    pub verdict_map_above: Option<u64>,

    /// Why the verdict chose that mapping floor
    pub map_because: String,

    /// Open segment files the shipped default caches
    pub shipped_fd_cache: u64,

    /// Open segment files this machine suggests caching
    pub verdict_fd_cache: u64,
}

/// Read this machine's facts under a root and compare them with a configuration
pub fn doctor(root: &Path, config: &ReelConfig) -> DoctorReport {
    let facts = MachineFacts::read(root);
    let reservation = config.segment_bytes.to_bytes() * config.tail_count() as u64;
    let verdict = facts.verdict();
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
        shipped_fd_cache: DEFAULT_FD_CACHE,
        verdict_fd_cache: verdict.fd_cache,
    }
}

/// One knob's configured value beside this machine's choice
struct Knob {
    /// The knob's label
    name: &'static str,

    /// The configured value
    configured: String,

    /// This machine's choice
    chosen: String,

    /// The verdict's reason, if it gave one
    because: Option<String>,
}

impl Knob {
    fn disagrees(&self) -> bool {
        self.configured != self.chosen
    }
}

impl Report for DoctorReport {
    fn doc(&self) -> Doc {
        let knobs = self.knobs();
        let disagreeing = knobs.iter().filter(|knob| knob.disagrees()).count();

        let mut table = Table::new([
            Column::left("knob"),
            Column::left("configured"),
            Column::left("this machine"),
        ]);
        for knob in &knobs {
            let cells = Row::new([
                knob.name.to_string(),
                knob.configured.clone(),
                knob.chosen.clone(),
            ]);
            table = table.row(match knob.disagrees() {
                true => cells.note("disagrees").toned(Tone::Warn),
                false => cells,
            });
        }

        // Each reason is prefixed with the knob it explains
        let notes: Vec<Note> = knobs
            .iter()
            .filter_map(|knob| {
                knob.because
                    .as_ref()
                    .map(|because| Note::new(format!("{}: {because}", knob.name)))
            })
            .collect();

        Doc::new()
            .head(fmt::volume_name(&self.root))
            .head("doctor")
            .head(fmt::maybe_bytes(self.memory_bytes) + " memory")
            .head(format!("io_uring {}", self.ring))
            .verdict(
                match disagreeing {
                    0 => Tone::Good,
                    _ => Tone::Warn,
                },
                match disagreeing {
                    0 => format!("{} of {} knobs agree", knobs.len(), knobs.len()),
                    _ => format!("{disagreeing} of {} knobs disagree", knobs.len()),
                },
                format!(
                    "idle reservation {} under the shipped default",
                    fmt::bytes(self.idle_reservation_bytes),
                ),
            )
            .facts(self.facts())
            .table(table)
            .notes("why", Tone::Plain, notes)
            .footer(["machine-readable: -o json"])
    }
}

impl DoctorReport {
    fn facts(&self) -> Vec<(String, String)> {
        vec![
            ("root".to_string(), self.root.clone()),
            (
                "filesystem".to_string(),
                match (self.capacity_bytes, self.occupied_bytes) {
                    (Some(capacity), Some(occupied)) => format!(
                        "{}, {} occupied",
                        fmt::bytes(capacity),
                        fmt::bytes(occupied)
                    ),
                    _ => fmt::maybe_bytes(self.capacity_bytes),
                },
            ),
            (
                "logical block".to_string(),
                fmt::maybe_bytes(self.logical_block_bytes),
            ),
            (
                "rotational".to_string(),
                match self.is_rotational {
                    Some(true) => "yes".to_string(),
                    Some(false) => "no".to_string(),
                    None => "unknown".to_string(),
                },
            ),
            (
                "actuators".to_string(),
                match self.actuator_ranges {
                    0 => "one, or the drive does not say".to_string(),
                    ranges => format!("{ranges} ranges"),
                },
            ),
            (
                "open file limit".to_string(),
                match self.open_file_limit {
                    Some(limit) => limit.to_string(),
                    None => "unlimited".to_string(),
                },
            ),
        ]
    }

    /// Each knob's configured value beside this machine's choice
    fn knobs(&self) -> [Knob; 3] {
        [
            Knob {
                name: "plane",
                configured: self.configured_plane.clone(),
                chosen: self.verdict_plane.clone(),
                because: Some(self.because.clone()),
            },
            Knob {
                name: "map above",
                configured: floor_label(self.configured_map_above),
                // A direct plane has no mapping floor, so a configured mapping there is refused
                chosen: match self.configured_map_above.is_some()
                    && self.verdict_map_above.is_none()
                {
                    true => "refused".to_string(),
                    false => floor_label(self.verdict_map_above),
                },
                because: Some(self.map_because.clone()),
            },
            Knob {
                name: "fd cache",
                configured: self.shipped_fd_cache.to_string(),
                chosen: self.verdict_fd_cache.to_string(),
                because: None,
            },
        ]
    }
}

/// A mapping floor as text, or off when the volume maps nothing
fn floor_label(floor: Option<u64>) -> String {
    match floor {
        Some(bytes) => fmt::bytes(bytes),
        None => "off".to_string(),
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
