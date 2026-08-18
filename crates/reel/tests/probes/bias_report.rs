//! What the bias pass would choose on this machine, and where it disagrees
//!
//! The pass is a comparison before it is an actuator: it says what it would do, an
//! operator's config says what was done, and a disagreement is either a bug in the rule
//! or a misconfigured volume. Point `REEL_BIAS_DIR` at the volume under test; without
//! one it reports on a temporary directory, which measures the machine rather than the
//! disk the engine will run on.
//!
//! Opt-in. Run with:
//!   cargo test -p reel --test probes -- bias_report

use std::path::PathBuf;

use reel::reel::bias::{access_ranges, MachineFacts, Plane, RingAvailability};
use reel::{ByteCount, IoBackend, Preallocate, RangedReads, ReelConfig, DEFAULT_FD_CACHE};

/// How a floor reads in the report, where absent means the volume maps nothing
fn floor_label(floor: Option<ByteCount>) -> String {
    match floor {
        Some(bytes) => format!("{} bytes", bytes.to_bytes()),
        None => "off".to_string(),
    }
}

/// Directory the report reads its facts from
fn root() -> PathBuf {
    match std::env::var("REEL_BIAS_DIR") {
        Ok(path) => PathBuf::from(path),
        Err(_) => std::env::temp_dir(),
    }
}

fn bytes(value: Option<u64>) -> String {
    match value {
        None => "unknown".to_string(),
        Some(value) if value >= 1 << 30 => format!("{:.1} GiB", value as f64 / (1u64 << 30) as f64),
        Some(value) if value >= 1 << 20 => format!("{:.1} MiB", value as f64 / (1u64 << 20) as f64),
        Some(value) => format!("{value} B"),
    }
}

/// The plane a configured backend actually opens on
fn configured_plane(backend: IoBackend) -> Plane {
    match backend.is_direct() {
        true => Plane::Direct,
        false => Plane::Buffered,
    }
}

// what this machine argues for, beside what the config asks for
pub fn what_this_machine_argues_for() {
    let root = root();
    let config = ReelConfig::default();
    let facts = MachineFacts::read(&root);
    let reservation = config.segment_bytes.to_bytes() * config.active_tails.resolve_tails() as u64;
    let verdict = facts.verdict(reservation);

    println!();
    println!("root {}", root.display());
    println!();
    println!("{:<24}{}", "memory", bytes(facts.memory_bytes));
    println!("{:<24}{}", "filesystem", bytes(facts.volume_capacity_bytes));
    println!(
        "{:<24}{}",
        "capacity over memory",
        match facts.capacity_over_memory() {
            Some(ratio) => format!("{ratio:.1}x"),
            None => "unknown".to_string(),
        }
    );
    println!(
        "{:<24}{}",
        "logical block",
        bytes(facts.logical_block_bytes)
    );
    println!(
        "{:<24}{}",
        "rotational",
        match facts.is_rotational {
            Some(true) => "yes",
            Some(false) => "no",
            None => "unknown",
        }
    );
    println!(
        "{:<24}{}",
        "actuators",
        match access_ranges(&root).as_slice() {
            [] => "one, or the drive does not say".to_string(),
            spans => format!(
                "{} ranges: {}",
                spans.len(),
                spans
                    .iter()
                    .map(|range| format!("{}+{}", range.sector, range.sectors))
                    .collect::<Vec<_>>()
                    .join(" "),
            ),
        }
    );
    println!(
        "{:<24}{}",
        "open file limit",
        match facts.open_file_limit {
            Some(limit) => limit.to_string(),
            None => "unlimited".to_string(),
        }
    );
    println!(
        "{:<24}{}",
        "io_uring",
        match facts.ring {
            RingAvailability::Available => "available",
            RingAvailability::Unsupported => "no io_uring in this kernel",
            RingAvailability::Denied => "refused, by policy or sandbox",
            RingAvailability::NotLinux => "not linux",
        }
    );

    println!();
    println!(
        "idle reservation under the shipped default: {}",
        bytes(Some(reservation))
    );
    println!("because: {}", verdict.because);
    println!("mapping: {}", verdict.map_because);
    println!();

    let rows: [(&str, String, String); 4] = [
        (
            "plane",
            format!("{:?}", configured_plane(config.io_backend)),
            format!("{:?}", verdict.plane),
        ),
        (
            // A verdict names the floor a record has to clear, and a direct plane
            // names none at all, so a volume asking for a mapping there disagrees.
            "map above",
            floor_label(config.map_above),
            match config.map_above.is_some() && verdict.map_above.is_none() {
                true => "refused".to_string(),
                false => floor_label(verdict.map_above),
            },
        ),
        (
            "ranged reads",
            format!("{:?}", config.ranged_reads),
            format!("{:?}", verdict.ranged_reads),
        ),
        (
            "preallocate",
            format!("{:?}", config.preallocate),
            format!("{:?}", verdict.preallocate),
        ),
    ];

    println!("{:<18}{:<14}{:<14}", "knob", "configured", "verdict");
    for (knob, configured, chosen) in rows {
        let flag = match configured == chosen {
            true => "",
            false => "  <- disagrees",
        };
        println!("{knob:<18}{configured:<14}{chosen:<14}{flag}");
    }
    let cache_flag = match DEFAULT_FD_CACHE == verdict.fd_cache {
        true => "",
        false => "  <- disagrees",
    };
    println!(
        "{:<18}{:<14}{:<14}{}",
        "fd cache", DEFAULT_FD_CACHE, verdict.fd_cache, cache_flag
    );

    // A verdict that cannot be acted on is worse than none, so the pairing the engine
    // refuses is checked here rather than left to a volume that will not open.
    assert!(
        !(verdict.plane == Plane::Direct && verdict.map_above.is_some()),
        "a direct verdict that keeps mapped reads is refused at validation",
    );
    assert!(
        verdict.fd_cache > 0,
        "a reader cache of nothing would reopen every segment per read",
    );
}

// the rule answers the same way for the same facts, whatever the machine says
//
// The report above depends on where it runs, which is why it cannot check the rule.
pub fn the_rule_is_a_function_of_its_facts() {
    // The rule turns on what the volume holds rather than how large the disk is, so
    // both of these sit on the same 4 TiB disk and differ only in occupancy.
    let small = MachineFacts {
        memory_bytes: Some(64 << 30),
        volume_capacity_bytes: Some(4096u64 << 30),
        volume_bytes: Some(32 << 30),
        open_file_limit: Some(1024),
        ..MachineFacts::default()
    };
    let large = MachineFacts {
        volume_bytes: Some(4096u64 << 30),
        ..small
    };

    let reservation = ByteCount::gb(8).to_bytes();
    let small = small.verdict(reservation);
    let large = large.verdict(reservation);

    assert_eq!(small.plane, Plane::Buffered, "half of memory stays warm");
    assert_eq!(small.ranged_reads, RangedReads::Cached);
    // The disk is 64 times memory, so the set that fits today will not once it fills,
    // and a mapped read there loses cold by up to 9x.
    assert_eq!(small.map_above, None, "a 4 TiB disk was advised a mapping");
    assert!(small.map_because.contains("cold"), "{}", small.map_because);

    assert_eq!(large.plane, Plane::Direct, "sixty four times memory cannot");
    assert_eq!(large.ranged_reads, RangedReads::Direct);
    assert_eq!(large.map_above, None);

    // The same rule the other way: a disk memory could hold is advised the floor.
    let held = MachineFacts {
        memory_bytes: Some(64 << 30),
        volume_capacity_bytes: Some(32 << 30),
        volume_bytes: Some(8 << 30),
        open_file_limit: Some(1024),
        ..MachineFacts::default()
    }
    .verdict(reservation);
    assert_eq!(held.plane, Plane::Buffered);
    assert!(
        held.map_above.is_some(),
        "a disk under memory was refused a mapping"
    );

    // 8 GiB against a 4 TiB disk is far under the eighth that would chunk it.
    assert_eq!(small.preallocate, Preallocate::Full);
    assert_eq!(large.preallocate, Preallocate::Full);

    // 1024 descriptors, halved for headroom, halved again where direct doubles.
    assert_eq!(small.fd_cache, 256, "the default is under half of 1024");
    assert_eq!(large.fd_cache, 256);
}
