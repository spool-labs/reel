//! A short hammer run on a temp dir, with kills, so every pull request runs the hammer

use std::process::Command;

fn hammer(extra: &[&str]) {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let mut args = vec![
        "run",
        "--backend",
        "mixed",
        "--segment-mib",
        "16",
        "--max-gib",
        "1",
        "--readers",
        "2",
        "--life",
        "8",
        "--kill-ratio",
        "0.6",
        "--duration",
        "40",
        "--tick",
        "5",
    ];
    args.extend_from_slice(extra);
    let output = Command::new(env!("CARGO_BIN_EXE_reel-hammer"))
        .args(&args)
        .arg("--dir")
        .arg(dir.path())
        .output()
        .expect("run the hammer");
    let journal = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "the hammer found a violation:\n{journal}"
    );
    assert!(
        journal.contains("no violation"),
        "the hammer ended early:\n{journal}"
    );
}

// reads through the store's own paths
#[test]
fn hammer_short() {
    hammer(&[]);
}

// reads through segment mappings
#[test]
fn hammer_short_mapped() {
    hammer(&["--map"]);
}
