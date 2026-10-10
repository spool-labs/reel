//! Prints what the bias pass would choose on this machine and where the config disagrees
//! Run `cargo test -p tape-reel --test probes -- bias_report`, with `REEL_BIAS_DIR` at the volume

use std::path::PathBuf;

use reel::reel::bias::{access_ranges, MachineFacts, Plane, RingAvailability};
use reel::{ByteCount, IoBackend, ReelConfig, DEFAULT_FD_CACHE};

/// A floor's report label, where none means the volume maps nothing
fn floor_label(floor: Option<ByteCount>) -> String {
    match floor {
        Some(bytes) => format!("{} bytes", bytes.to_bytes()),
        None => "off".to_string(),
    }
}

/// The report reads its facts from this directory
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

/// The plane a configured backend opens on
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
    let verdict = facts.verdict();

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
    println!("because: {}", verdict.because);
    println!("mapping: {}", verdict.map_because);
    println!();

    let rows: [(&str, String, String); 2] = [
        (
            "plane",
            format!("{:?}", configured_plane(config.io_backend)),
            format!("{:?}", verdict.plane),
        ),
        (
            // A configured mapping disagrees with a verdict that has no map floor
            "map above",
            floor_label(config.map_above),
            match config.map_above.is_some() && verdict.map_above.is_none() {
                true => "refused".to_string(),
                false => floor_label(verdict.map_above),
            },
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

    // The verdict must be one the engine accepts, so check the pairing validation refuses
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
pub fn the_rule_is_a_function_of_its_facts() {
    // Both sit on the same 4 TiB disk and differ only in how much the volume holds
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

    let small = small.verdict();
    let large = large.verdict();

    assert_eq!(small.plane, Plane::Buffered, "half of memory stays warm");
    // The disk is 64 times memory, so the warm set will not fit once it fills
    assert_eq!(small.map_above, None, "a 4 TiB disk was advised a mapping");
    assert!(small.map_because.contains("cold"), "{}", small.map_because);

    assert_eq!(large.plane, Plane::Direct, "sixty four times memory cannot");
    assert_eq!(large.map_above, None);

    // The other way round, a disk that fits in memory is advised a map floor
    let held = MachineFacts {
        memory_bytes: Some(64 << 30),
        volume_capacity_bytes: Some(32 << 30),
        volume_bytes: Some(8 << 30),
        open_file_limit: Some(1024),
        ..MachineFacts::default()
    }
    .verdict();
    assert_eq!(held.plane, Plane::Buffered);
    assert!(
        held.map_above.is_some(),
        "a disk under memory was refused a mapping"
    );

    // Half the 1024 descriptor limit on either plane
    assert_eq!(small.fd_cache, 512, "half of 1024, buffered");
    assert_eq!(large.fd_cache, 512, "half of 1024, direct");
}
