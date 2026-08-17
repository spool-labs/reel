//! What the machine says about itself, read once when a volume opens
//!
//! Nothing here measures: every fact is a file the kernel already wrote or a stat
//! the volume already needs. Every field is optional and every failure is silent,
//! so a volume never fails to open over a fact it wanted for a log line. The pass
//! logs a verdict and acts on nothing.

use std::path::Path;

use crate::config::{Preallocate, RangedReads, DEFAULT_FD_CACHE};
use crate::units::ByteCount;

/// Facts about the machine and the device a volume sits on
///
/// Absent means the platform did not offer it, which is not the same as zero.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MachineFacts {
    /// Total usable memory, which bounds what a working set can stay warm in
    pub memory_bytes: Option<u64>,

    /// Capacity of the filesystem holding the volume, a proxy for its ceiling
    pub volume_capacity_bytes: Option<u64>,

    /// What the volume already occupies, summed from its segment files
    pub volume_bytes: Option<u64>,

    /// Bytes the device takes or refuses, which direct io has to be framed on
    pub logical_block_bytes: Option<u64>,

    /// Whether the device seeks
    pub is_rotational: Option<bool>,

    /// How many actuators the device seeks with, when it says
    pub access_ranges: Option<u64>,

    /// Readahead the device is configured for, which sizes a cold fault's window
    pub readahead_bytes: Option<u64>,

    /// Descriptors this process may hold at once
    pub open_file_limit: Option<u64>,

    /// Whether a ring can be set up here, and if not why not
    pub ring: RingAvailability,
}

/// Why a ring is or is not available, which the probe's errno already knows
///
/// A kernel that cannot and a policy that will not are different problems for an
/// operator, and collapsing both to "unavailable" makes the second unfindable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RingAvailability {
    /// `io_uring_setup` succeeded
    Available,
    /// The syscall is absent, so the kernel has no io_uring
    Unsupported,
    /// Refused, which is `kernel.io_uring_disabled` or a seccomp policy
    Denied,
    /// Not probed, because this platform has no ring to probe for
    #[default]
    NotLinux,
}

/// The span of the device one actuator serves, in 512 byte sectors
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AccessRange {
    /// First sector this actuator reaches
    pub sector: u64,

    /// How many sectors it reaches from there
    pub sectors: u64,
}

/// Which plane the bias pass would open a volume on
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Plane {
    /// Let the kernel hold the pages, which wins whenever the set stays warm
    Buffered,
    /// Go around the page cache, which wins once the set cannot stay warm
    Direct,
}

/// What the pass would choose, and why
///
/// Durability settings are deliberately absent: they are a promise about what a
/// crash may cost, not a fit to hardware.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Verdict {
    /// Whether the volume's descriptors go around the page cache
    pub plane: Plane,

    /// Smallest record a mapping is worth, absent where a mapping is not worth having
    pub map_above: Option<ByteCount>,

    /// The plane a window of a large record is read on, which follows the volume's
    pub ranged_reads: RangedReads,

    /// Chunk where the idle reservation is a large share of the filesystem
    pub preallocate: Preallocate,

    /// Sealed descriptors the reader cache may hold under this process's limit
    pub fd_cache: u64,

    /// The sentence a disagreement gets logged with
    pub because: &'static str,

    /// Why the mapping is advised or withheld, which is a separate argument
    pub map_because: &'static str,
}

/// Times larger than memory a volume's own contents must be before direct is picked
///
/// Not centred, because the errors are not the same size: wrongly direct gives up
/// an order of magnitude on a warm set, wrongly buffered a fraction on a cold one.
pub const DIRECT_AT_OCCUPANCY_RATIO: f64 = 1.5;

/// Share of a filesystem the idle reservation may take before it stops pre-writing
///
/// The default reservation is nothing on a large disk and absurd on a small one.
const RESERVATION_SHARE_OF_CAPACITY: u64 = 8;

/// Times the device's readahead a record must clear before a mapping pays
///
/// A cold mapped read pulls in a window around the record rather than the record,
/// so the floor sits where that toll has gone rather than where the warm gain
/// starts.
const MAP_AT_READAHEAD_MULTIPLE: u64 = 16;

/// The readahead assumed when the device will not say what its own is
const DEFAULT_READAHEAD_BYTES: u64 = 128 * 1024;

impl MachineFacts {
    /// Smallest record a mapping is worth on this machine
    fn mapping_floor(&self) -> ByteCount {
        let readahead = self.readahead_bytes.unwrap_or(DEFAULT_READAHEAD_BYTES);
        ByteCount::from_bytes(readahead.saturating_mul(MAP_AT_READAHEAD_MULTIPLE))
    }

    /// The floor a mapping is worth on this machine, and why
    ///
    /// Measured: mapped point reads win warm p50 by 13 to 25 percent, lose p99 by 1.7
    /// to 1.8x, and lose a cold read by up to 9x. So the gain is real only where the
    /// records stay resident, and the term that decides that is the filesystem rather
    /// than what has been written: a disk that cannot fit in memory serves cold records
    /// eventually however small its set is today. The bar is the plane's own, and a
    /// machine that will not say either term cannot claim it fits.
    fn mapping_for(&self, is_direct: bool) -> (Option<ByteCount>, &'static str) {
        if is_direct {
            return (
                None,
                "a direct volume holds no page cache for a mapping to read",
            );
        }
        match self.capacity_over_memory() {
            Some(ratio) if ratio < DIRECT_AT_OCCUPANCY_RATIO => (
                Some(self.mapping_floor()),
                "the filesystem is within reach of memory, so a mapped record stays resident",
            ),
            Some(_) => (
                None,
                "the filesystem is larger than memory, so a mapped read goes cold, which it loses by up to 9x",
            ),
            None => (
                None,
                "the machine will not say its filesystem against its memory, so a cold mapped read cannot be ruled out, and it loses by up to 9x",
            ),
        }
    }

    /// What plane these facts argue for
    pub fn verdict(&self, idle_reservation_bytes: u64) -> Verdict {
        // What the volume holds, not what the disk could take: a large disk
        // holding little argues for direct on a set that fits in memory.
        let (plane, because) = match self.occupied_over_memory() {
            None => (
                Plane::Buffered,
                "nothing written yet, so the plane that wins warm until a reopen sees otherwise",
            ),
            Some(ratio) if ratio >= DIRECT_AT_OCCUPANCY_RATIO => (
                Plane::Direct,
                "the volume holds more than memory, so its set cannot stay warm",
            ),
            Some(_) => (
                Plane::Buffered,
                "the volume fits within reach of memory, so its set may stay warm",
            ),
        };
        let is_direct = plane == Plane::Direct;
        let (map_above, map_because) = self.mapping_for(is_direct);

        Verdict {
            plane,
            map_above,
            ranged_reads: match is_direct {
                true => RangedReads::Direct,
                false => RangedReads::Cached,
            },
            preallocate: self.preallocate_for(idle_reservation_bytes),
            fd_cache: self.fd_cache_for(is_direct),
            because,
            map_because,
        }
    }

    /// Whether a whole segment may be pre-written at creation
    ///
    /// Full reserves real blocks before a byte is written, so it turns on the share
    /// of the filesystem that takes rather than the absolute size.
    fn preallocate_for(&self, idle_reservation_bytes: u64) -> Preallocate {
        let Some(capacity) = self.volume_capacity_bytes else {
            return Preallocate::Chunk;
        };
        match capacity / RESERVATION_SHARE_OF_CAPACITY >= idle_reservation_bytes {
            true => Preallocate::Full,
            false => Preallocate::Chunk,
        }
    }

    /// Descriptors the reader cache may hold without crowding the process
    ///
    /// A direct volume keeps a second descriptor per segment, and half the limit is
    /// left for tails, sockets and everything else the process opens.
    fn fd_cache_for(&self, is_direct: bool) -> u64 {
        let Some(limit) = self.open_file_limit else {
            return DEFAULT_FD_CACHE;
        };
        let per_segment = match is_direct {
            true => 2,
            false => 1,
        };
        DEFAULT_FD_CACHE.min(limit / 2 / per_segment)
    }

    /// Read what this machine will say, for a volume rooted at this path
    pub fn read(root: &Path) -> MachineFacts {
        MachineFacts {
            memory_bytes: memory_bytes(),
            volume_capacity_bytes: capacity_bytes(root),
            volume_bytes: occupied_bytes(root),
            logical_block_bytes: device_fact(root, "queue/logical_block_size")
                .and_then(|value| value.parse().ok()),
            is_rotational: device_fact(root, "queue/rotational")
                .and_then(|value| value.parse::<u8>().ok())
                .map(|value| value == 1),
            access_ranges: match access_ranges(root).len() as u64 {
                0 => None,
                count => Some(count),
            },
            readahead_bytes: device_fact(root, "queue/read_ahead_kb")
                .and_then(|value| value.parse::<u64>().ok())
                .map(|kb| kb * 1024),
            open_file_limit: open_file_limit(),
            ring: probe_ring(),
        }
    }

    /// How many times the filesystem's capacity exceeds memory
    pub fn capacity_over_memory(&self) -> Option<f64> {
        let capacity = self.volume_capacity_bytes?;
        let memory = self.memory_bytes?;
        (memory > 0).then(|| capacity as f64 / memory as f64)
    }

    /// How many times what the volume holds exceeds memory
    ///
    /// Absent on a volume with nothing in it, since zero is not evidence that the
    /// set is small.
    pub fn occupied_over_memory(&self) -> Option<f64> {
        let occupied = self.volume_bytes.filter(|bytes| *bytes > 0)?;
        let memory = self.memory_bytes?;
        (memory > 0).then(|| occupied as f64 / memory as f64)
    }
}

/// Descriptors this process may hold at once
fn open_file_limit() -> Option<u64> {
    // Safety: getrlimit writes into the struct it is handed and reads nothing else.
    let limit = unsafe {
        let mut limit: libc::rlimit = std::mem::zeroed();
        (libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0).then_some(limit)?
    };
    (limit.rlim_cur != libc::RLIM_INFINITY).then_some(limit.rlim_cur as u64)
}

/// Whether a ring sets up here, keeping the errno that says why not
#[cfg(target_os = "linux")]
fn probe_ring() -> RingAvailability {
    let mut params = [0u8; 256];
    // Safety: the kernel writes at most one io_uring_params into a buffer wider
    // than one, and the return is a descriptor this closes.
    let ring = unsafe {
        libc::syscall(
            libc::SYS_io_uring_setup,
            1 as libc::c_long,
            params.as_mut_ptr(),
        )
    };
    if ring >= 0 {
        // Safety: the syscall returned this descriptor and nothing else holds it.
        unsafe { libc::close(ring as libc::c_int) };
        return RingAvailability::Available;
    }
    match std::io::Error::last_os_error().raw_os_error() {
        Some(libc::ENOSYS) => RingAvailability::Unsupported,
        _ => RingAvailability::Denied,
    }
}

#[cfg(not(target_os = "linux"))]
fn probe_ring() -> RingAvailability {
    RingAvailability::NotLinux
}

/// Total memory, from the file the kernel keeps for the purpose
#[cfg(target_os = "linux")]
fn memory_bytes() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kib: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kib * 1024);
        }
    }
    None
}

/// Total memory, from the sysctl that carries it
#[cfg(target_os = "macos")]
fn memory_bytes() -> Option<u64> {
    let mut value = 0u64;
    let mut len = std::mem::size_of::<u64>();
    let name = c"hw.memsize";
    // Safety: the name is a nul-terminated literal and the kernel writes at most
    // len bytes into a u64 this owns.
    let ok = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            &mut value as *mut u64 as *mut libc::c_void,
            &mut len,
            std::ptr::null_mut(),
            0,
        ) == 0
    };
    ok.then_some(value)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn memory_bytes() -> Option<u64> {
    None
}

/// What the volume's own files already occupy
fn occupied_bytes(root: &Path) -> Option<u64> {
    let mut total = 0u64;
    for entry in std::fs::read_dir(root).ok()?.flatten() {
        if let Ok(meta) = entry.metadata() {
            if meta.is_file() {
                total = total.saturating_add(meta.len());
            }
        }
    }
    Some(total)
}

/// Capacity of the filesystem this path sits on
pub fn capacity_bytes(root: &Path) -> Option<u64> {
    stat_volume(root).map(|stats| stats.f_blocks as u64 * stats.f_frsize)
}

/// What the filesystem will still hand out, which is what ENOSPC counts down
///
/// Not the capacity less what the reel accounts for: preallocation, footers,
/// filesystem metadata and another tenant are all invisible to a sum over record
/// bytes and all visible here.
pub fn available_bytes(root: &Path) -> Option<u64> {
    stat_volume(root).map(|stats| stats.f_bavail as u64 * stats.f_frsize)
}

fn stat_volume(root: &Path) -> Option<libc::statvfs> {
    let path = std::ffi::CString::new(root.as_os_str().as_encoded_bytes()).ok()?;
    // Safety: statvfs writes into the struct it is handed and reads a path this
    // owns for the duration of the call.
    unsafe {
        let mut stats: libc::statvfs = std::mem::zeroed();
        (libc::statvfs(path.as_ptr(), &mut stats) == 0).then_some(stats)
    }
}

/// Where a device publishes one directory per actuator, from Linux 5.15
#[cfg(target_os = "linux")]
const RANGES: &str = "queue/independent_access_ranges";

/// The span each of the device's actuators serves, in device order
///
/// Empty on every ordinary drive, which reports no ranges at all rather than one
/// covering the whole device.
pub fn access_ranges(root: &Path) -> Vec<AccessRange> {
    match ranges_dir(root) {
        Some(at) => ranges_under(&at),
        None => Vec::new(),
    }
}

/// One actuator per numbered directory under a device's ranges, in device order
///
/// Anything the kernel puts beside the numbered directories is not an actuator, and
/// a range missing either half of its span is not one either.
fn ranges_under(at: &Path) -> Vec<AccessRange> {
    let Ok(entries) = std::fs::read_dir(at) else {
        return Vec::new();
    };

    let sectors =
        |at: &Path| -> Option<u64> { std::fs::read_to_string(at).ok()?.trim().parse().ok() };
    let mut ranges: Vec<(u64, AccessRange)> = entries
        .filter_map(|entry| {
            let at = entry.ok()?.path();
            let index = at.file_name()?.to_str()?.parse().ok()?;
            let range = AccessRange {
                sector: sectors(&at.join("sector"))?,
                sectors: sectors(&at.join("nr_sectors"))?,
            };
            Some((index, range))
        })
        .collect();
    // A directory read comes back unordered and a span list only reads in order.
    ranges.sort_by_key(|(index, _)| *index);
    ranges.into_iter().map(|(_, range)| range).collect()
}

/// Where the device holding this path publishes its actuators, from Linux 5.15
#[cfg(target_os = "linux")]
fn ranges_dir(root: &Path) -> Option<std::path::PathBuf> {
    use std::os::unix::fs::MetadataExt;

    let device = std::fs::metadata(root).ok()?.dev();
    let (major, minor) = (libc::major(device), libc::minor(device));
    let base = std::path::PathBuf::from(format!("/sys/dev/block/{major}:{minor}"));
    // A partition keeps its queue facts on the disk above it.
    [base.join(RANGES), base.join("..").join(RANGES)]
        .into_iter()
        .find(|at| at.is_dir())
}

#[cfg(not(target_os = "linux"))]
fn ranges_dir(_root: &Path) -> Option<std::path::PathBuf> {
    None
}

/// One `/sys/block` fact about the device holding this path
///
/// A partition does not carry the queue facts, so a miss walks up to the disk.
#[cfg(target_os = "linux")]
fn device_fact(root: &Path, leaf: &str) -> Option<String> {
    use std::os::unix::fs::MetadataExt;

    let device = std::fs::metadata(root).ok()?.dev();
    let (major, minor) = (libc::major(device), libc::minor(device));
    let base = std::path::PathBuf::from(format!("/sys/dev/block/{major}:{minor}"));

    let read = |at: &Path| std::fs::read_to_string(at.join(leaf)).ok();
    if let Some(found) = read(&base) {
        return Some(found.trim().to_string());
    }
    // A partition keeps its queue facts on the disk above it.
    read(&base.join("..")).map(|found| found.trim().to_string())
}

#[cfg(not(target_os = "linux"))]
fn device_fact(_root: &Path, _leaf: &str) -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // the pass answers for a real directory without measuring anything
    #[test]
    fn reads_what_the_platform_offers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let facts = MachineFacts::read(dir.path());

        // Capacity comes from statvfs, which every unix this builds on has.
        let capacity = facts.volume_capacity_bytes.expect("filesystem capacity");
        assert!(capacity > 0, "a mounted filesystem has capacity");

        if cfg!(target_os = "linux") {
            assert!(facts.memory_bytes.expect("meminfo") > 0);
        }
    }

    // a missing fact is absent rather than a zero standing in for one
    #[test]
    fn an_unreadable_root_answers_nothing_rather_than_zero() {
        let facts = MachineFacts::read(Path::new("/definitely/not/a/path"));

        assert_eq!(facts.volume_capacity_bytes, None);
        assert_eq!(facts.logical_block_bytes, None);
        assert_eq!(facts.is_rotational, None);
        assert_eq!(facts.access_ranges, None);
        assert!(access_ranges(Path::new("/definitely/not/a/path")).is_empty());
    }

    // two actuators read as two spans, in the order the device numbers them
    #[test]
    fn a_drive_that_reports_ranges_reads_as_its_spans() {
        const HALF: u64 = 15_628_053_168;
        let dir = tempfile::tempdir().expect("tempdir");
        for (name, sector) in [("1", HALF), ("0", 0)] {
            let range = dir.path().join(name);
            std::fs::create_dir(&range).expect("range");
            std::fs::write(range.join("sector"), format!("{sector}\n")).expect("sector");
            std::fs::write(range.join("nr_sectors"), format!("{HALF}\n")).expect("nr_sectors");
        }
        // Neither a file beside them nor a directory holding half a span is one.
        std::fs::write(dir.path().join("uevent"), "").expect("uevent");
        let partial = dir.path().join("2");
        std::fs::create_dir(&partial).expect("partial");
        std::fs::write(partial.join("sector"), "0\n").expect("sector");

        let spans = ranges_under(dir.path());

        assert_eq!(
            spans,
            vec![
                AccessRange {
                    sector: 0,
                    sectors: HALF
                },
                AccessRange {
                    sector: HALF,
                    sectors: HALF
                },
            ]
        );
    }

    // the count and the spans are one reading, on whatever drive this runs on
    #[test]
    fn the_actuator_count_is_the_spans_it_came_from() {
        let dir = tempfile::tempdir().expect("tempdir");
        let facts = MachineFacts::read(dir.path());
        let spans = access_ranges(dir.path());

        let counted = match spans.len() as u64 {
            0 => None,
            count => Some(count),
        };
        assert_eq!(facts.access_ranges, counted);
        for range in &spans {
            assert!(range.sectors > 0, "an actuator that reaches no sectors");
        }
    }

    // the ratio needs both terms and says so when it has one
    #[test]
    fn the_ratio_needs_both_terms() {
        let neither = MachineFacts::default();
        assert_eq!(neither.capacity_over_memory(), None);
        assert_eq!(neither.occupied_over_memory(), None);

        let one_term = MachineFacts {
            memory_bytes: Some(16),
            ..MachineFacts::default()
        };
        assert_eq!(one_term.capacity_over_memory(), None);

        let both = MachineFacts {
            memory_bytes: Some(16),
            volume_capacity_bytes: Some(64),
            ..MachineFacts::default()
        };
        assert_eq!(both.capacity_over_memory(), Some(4.0));
    }

    // a volume that fits within reach of memory keeps the warm plane
    #[test]
    fn a_small_volume_stays_buffered() {
        let facts = MachineFacts {
            memory_bytes: Some(64),
            volume_bytes: Some(64),
            ..MachineFacts::default()
        };

        assert_eq!(facts.verdict(0).plane, Plane::Buffered);
    }

    // the floor follows the device's own readahead
    #[test]
    fn the_mapping_floor_follows_readahead() {
        let facts = MachineFacts {
            memory_bytes: Some(64),
            volume_capacity_bytes: Some(64),
            volume_bytes: Some(1),
            readahead_bytes: Some(128 * 1024),
            ..MachineFacts::default()
        };

        assert_eq!(
            facts.verdict(0).map_above,
            Some(ByteCount::from_bytes(2 * 1024 * 1024)),
        );
    }

    // a device that will not say falls back rather than mapping everything
    #[test]
    fn an_unknown_readahead_still_names_a_floor() {
        let facts = MachineFacts {
            memory_bytes: Some(64),
            volume_capacity_bytes: Some(64),
            volume_bytes: Some(1),
            ..MachineFacts::default()
        };

        assert_eq!(
            facts.verdict(0).map_above,
            Some(ByteCount::from_bytes(DEFAULT_READAHEAD_BYTES * 16)),
        );
    }

    // a disk larger than memory is advised no mapping, whatever its set weighs today
    #[test]
    fn a_disk_past_memory_is_advised_no_mapping() {
        let roomy_disk = MachineFacts {
            memory_bytes: Some(64 << 30),
            volume_capacity_bytes: Some(4096u64 << 30),
            volume_bytes: Some(1 << 30),
            ..MachineFacts::default()
        };
        let verdict = roomy_disk.verdict(0);

        assert_eq!(
            verdict.plane,
            Plane::Buffered,
            "a small set still stays warm"
        );
        assert_eq!(
            verdict.map_above, None,
            "the disk cannot hold its records in memory"
        );
        assert!(
            verdict.map_because.contains("cold"),
            "{}",
            verdict.map_because
        );

        // The same machine with a disk memory could hold is advised the floor.
        let small_disk = MachineFacts {
            volume_capacity_bytes: Some(32 << 30),
            ..roomy_disk
        };
        assert!(small_disk.verdict(0).map_above.is_some());
    }

    // a machine that will not say its capacity cannot claim a mapping fits
    #[test]
    fn an_unknown_capacity_is_advised_no_mapping() {
        let facts = MachineFacts {
            memory_bytes: Some(64),
            volume_bytes: Some(1),
            ..MachineFacts::default()
        };
        let verdict = facts.verdict(0);

        assert_eq!(verdict.map_above, None);
        assert!(
            verdict.map_because.contains("will not say"),
            "{}",
            verdict.map_because
        );
    }

    // past the bar the set cannot stay warm and the plane flips
    #[test]
    fn a_volume_many_times_memory_goes_direct() {
        let facts = MachineFacts {
            memory_bytes: Some(64),
            volume_bytes: Some(64 * 2),
            ..MachineFacts::default()
        };

        assert_eq!(facts.verdict(0).plane, Plane::Direct);
    }

    // the bar is a floor rather than a midpoint, so just under it stays buffered
    #[test]
    fn the_bar_is_not_centred() {
        let under = MachineFacts {
            memory_bytes: Some(100),
            volume_bytes: Some(149),
            ..MachineFacts::default()
        };
        let over = MachineFacts {
            volume_bytes: Some(150),
            ..under
        };

        assert_eq!(under.verdict(0).plane, Plane::Buffered);
        assert_eq!(over.verdict(0).plane, Plane::Direct);
    }

    // a direct verdict says mappings are off, since validation refuses the pair
    #[test]
    fn a_direct_verdict_rules_out_mapping() {
        let direct = MachineFacts {
            memory_bytes: Some(64),
            volume_capacity_bytes: Some(64),
            volume_bytes: Some(64 * 2),
            ..MachineFacts::default()
        }
        .verdict(0);
        let buffered = MachineFacts {
            memory_bytes: Some(64),
            volume_capacity_bytes: Some(64),
            volume_bytes: Some(64),
            ..MachineFacts::default()
        }
        .verdict(0);

        assert_eq!(direct.plane, Plane::Direct);
        assert_eq!(direct.map_above, None, "direct refuses the pairing");
        assert!(
            direct.map_because.contains("direct"),
            "{}",
            direct.map_because
        );

        assert_eq!(buffered.plane, Plane::Buffered);
        assert!(
            buffered.map_above.is_some(),
            "a disk memory could hold names a floor"
        );
    }

    // the reader cache is sized under the process limit, doubled for direct
    #[test]
    fn the_fd_cache_fits_under_the_open_file_limit() {
        let tight = MachineFacts {
            open_file_limit: Some(256),
            ..MachineFacts::default()
        };
        assert_eq!(tight.verdict(0).fd_cache, 128, "half the limit, buffered");

        let direct = MachineFacts {
            memory_bytes: Some(1),
            volume_bytes: Some(64),
            open_file_limit: Some(256),
            ..MachineFacts::default()
        };
        assert_eq!(
            direct.verdict(0).fd_cache,
            64,
            "halved again, two per segment"
        );

        let roomy = MachineFacts {
            open_file_limit: Some(1_048_576),
            ..MachineFacts::default()
        };
        assert_eq!(
            roomy.verdict(0).fd_cache,
            DEFAULT_FD_CACHE,
            "never above the default"
        );
    }

    // a reservation that would claim a large share of the disk stops pre-writing
    #[test]
    fn a_small_disk_does_not_pre_write_whole_segments() {
        let facts = MachineFacts {
            volume_capacity_bytes: Some(64),
            ..MachineFacts::default()
        };

        assert_eq!(
            facts.verdict(8).preallocate,
            Preallocate::Full,
            "an eighth fits"
        );
        assert_eq!(
            facts.verdict(9).preallocate,
            Preallocate::Chunk,
            "past an eighth"
        );
    }

    // window reads follow the volume's own plane rather than diverging from it
    #[test]
    fn ranged_reads_follow_the_plane() {
        let direct = MachineFacts {
            memory_bytes: Some(1),
            volume_bytes: Some(64),
            ..MachineFacts::default()
        }
        .verdict(0);
        let buffered = MachineFacts {
            memory_bytes: Some(64),
            volume_bytes: Some(64),
            ..MachineFacts::default()
        }
        .verdict(0);

        assert_eq!(direct.ranged_reads, RangedReads::Direct);
        assert_eq!(buffered.ranged_reads, RangedReads::Cached);
    }

    // an unreadable root leaves no capacity, so it cannot claim a share of one
    #[test]
    fn no_capacity_does_not_pre_write() {
        let facts = MachineFacts::default();

        assert_eq!(facts.verdict(0).preallocate, Preallocate::Chunk);
    }

    // an empty volume is not evidence its set is small
    #[test]
    fn nothing_written_yet_keeps_the_warm_plane() {
        let fresh = MachineFacts {
            memory_bytes: Some(64),
            volume_bytes: Some(0),
            volume_capacity_bytes: Some(64 * 1000),
            ..MachineFacts::default()
        };

        assert_eq!(fresh.verdict(0).plane, Plane::Buffered);
        assert!(fresh.verdict(0).because.contains("nothing written"));
    }

    // no facts is not a reason to give up the plane that wins warm
    #[test]
    fn nothing_known_keeps_the_warm_plane() {
        let verdict = MachineFacts::default().verdict(0);

        assert_eq!(verdict.plane, Plane::Buffered);
        assert!(verdict.because.contains("nothing written"));
    }

    // memory of zero would divide rather than answer
    #[test]
    fn zero_memory_does_not_divide() {
        let facts = MachineFacts {
            memory_bytes: Some(0),
            volume_capacity_bytes: Some(64),
            ..MachineFacts::default()
        };

        assert_eq!(facts.capacity_over_memory(), None);
    }
}
