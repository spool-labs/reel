//! Drive the built binary against a real volume
//!
//! The commands are exercised through the executable rather than by calling into
//! it, since argument parsing, volume opening and the column declaration are most
//! of what could break and none of them are reachable from a unit test. The
//! volume is written and closed before the tool opens it, because a writer holds
//! an ownership lock a second writer would be refused.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::process::Command;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, Preallocate, RecordKey,
    ReelConfig, ReelStore, SyncPolicy,
};
use tempfile::TempDir;

/// Records written into each volume before it is inspected
const RECORDS: u8 = 64;

/// Bytes in each record, sized so the writes seal a segment or two
const RECORD_BYTES: usize = 64 * 1024;

const RECORD_CF: &str = "records";

/// The columns the volume under test is written with, as any caller declares its own
const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: RECORD_CF,
    key_width: KeyWidth::Fixed(32),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// What one command run produced
struct Run {
    ok: bool,
    out: String,
    err: String,
}

/// One command run again as json, which is where the exact figures live
///
/// The text form is written for a reader and its wording is allowed to change;
/// the json is the report's data and is what a test should be pinned to. Only
/// the assertions that are actually about presentation read the text.
fn json(volume: &Path, args: &[&str]) -> serde_json::Value {
    let mut all = vec!["-o", "json"];
    all.extend_from_slice(args);
    let run = run(volume, &all);
    // Not asserted on the exit code: a sweep that finds a fault reports it and
    // exits nonzero, and its report is exactly the one a test wants to read.
    assert!(!run.out.is_empty(), "{all:?} answered nothing: {}", run.err);
    serde_json::from_str(&run.out)
        .unwrap_or_else(|error| panic!("{all:?} produced invalid json ({error}): {}", run.out))
}

/// A figure a report answers under this name
fn figure(report: &serde_json::Value, name: &str) -> u64 {
    report[name]
        .as_u64()
        .unwrap_or_else(|| panic!("no `{name}` figure in {report}"))
}

/// What a report says stands between its figures and what a reader would take
/// them for
fn caveats(report: &serde_json::Value) -> Vec<String> {
    report["caveats"]
        .as_array()
        .unwrap_or_else(|| panic!("no caveats in {report}"))
        .iter()
        .map(|caveat| caveat["what"].as_str().unwrap_or_default().to_string())
        .collect()
}

/// A table row by the name in its first cell, which is indented under its heading
fn row<'a>(out: &'a str, name: &str) -> &'a str {
    out.lines()
        .map(str::trim_start)
        .find(|line| line.starts_with(name))
        .unwrap_or_else(|| panic!("no row for {name} in:\n{out}"))
}

fn run(volume: &Path, args: &[&str]) -> Run {
    let output = Command::new(env!("CARGO_BIN_EXE_reel"))
        .arg(volume)
        .args(args)
        .output()
        .expect("run reel");
    Run {
        ok: output.status.success(),
        out: String::from_utf8_lossy(&output.stdout).to_string(),
        err: String::from_utf8_lossy(&output.stderr).to_string(),
    }
}

fn key(byte: u8) -> RecordKey {
    RecordKey::from_bytes(ColumnId(1), &[byte; 32]).expect("key")
}

/// A volume written and closed, ready for the tool to open
fn volume(dir: &TempDir) -> &Path {
    let config = ReelConfig {
        segment_bytes: ByteCount::mb(2),
        alloc_chunk: ByteCount::mb(1),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        ..ReelConfig::default()
    };
    let store = ReelStore::open(dir.path().to_path_buf(), config, COLUMNS).expect("open volume");
    fill(&store);
    store.close().expect("close volume");
    drop(store);
    dir.path()
}

/// Write enough records to seal a segment, then overwrite some to leave dead bytes
fn fill(store: &ReelStore) {
    for byte in 0..RECORDS {
        store
            .put(&key(byte), &vec![byte; RECORD_BYTES])
            .expect("put record");
    }
    for byte in 0..RECORDS / 4 {
        store
            .put(&key(byte), &vec![byte; RECORD_BYTES])
            .expect("overwrite record");
    }
    store.flush().expect("flush");
}

// the cue report answers on a written volume, segments and all
#[test]
fn cues_a_volume() {
    let dir = tempfile::tempdir().expect("tempdir");
    let volume = volume(&dir);

    let cue = run(volume, &["cue"]);
    assert!(cue.ok, "cue failed: {}", cue.err);
    assert!(
        cue.out
            .lines()
            .next()
            .is_some_and(|head| head.contains("seq ")),
        "the head should say where the sequence stands: {}",
        cue.out,
    );
    assert!(
        cue.out.contains(" dead"),
        "the verdict should lead with what compaction is owed: {}",
        cue.out,
    );
    assert!(
        cue.out.contains("segment"),
        "no segment header: {}",
        cue.out
    );
    assert!(
        cue.out.contains("none held in this process"),
        "cue points held by nothing should say so: {}",
        cue.out,
    );
}

// an undeclared volume says which figures it is not counting
#[test]
fn undeclared_columns_are_named_as_such() {
    let dir = tempfile::tempdir().expect("tempdir");
    let volume = volume(&dir);

    let cue = run(volume, &["cue"]);
    assert!(cue.ok, "cue failed: {}", cue.err);
    assert!(
        cue.out.contains("no columns declared"),
        "an empty declaration should be admitted: {}",
        cue.out,
    );
}

// a declared column is counted, and its name reaches the report
#[test]
fn declared_columns_are_counted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let volume = volume(&dir);

    // Paged, since only a paged open leaves the sealed spans standing to count.
    let cue = run(volume, &["--column", "records:1:32", "--paged", "cue"]);
    assert!(cue.ok, "cue failed: {}", cue.err);
    assert!(
        cue.out.contains("standing covers"),
        "no cover line: {}",
        cue.out
    );
    let row = row(&cue.out, RECORD_CF);
    let sealed: usize = row
        .split_whitespace()
        .last()
        .and_then(|count| count.parse().ok())
        .unwrap_or_else(|| panic!("no sealed count on the column row: {row}"));
    assert!(
        sealed > 0,
        "the open should leave the sealed spans standing: {row}"
    );
}

// the segment listing honours its limit
#[test]
fn limits_the_segment_listing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let volume = volume(&dir);

    let held = figure(&json(volume, &["cue"]), "total_segments") as usize;
    assert!(
        held > 1,
        "the writes should have filled more than one segment"
    );

    let all = run(volume, &["cue"]);
    assert!(all.ok, "cue failed: {}", all.err);
    assert!(
        all.out.contains(&format!("all {held} segments")),
        "a listing showing every segment should say so: {}",
        all.out,
    );

    let capped = run(volume, &["cue", "--limit", "1"]);
    assert!(capped.ok, "cue failed: {}", capped.err);
    assert!(
        capped.out.contains(&format!("1 of {held} segments")),
        "the limit should cap the listing and say what it capped: {}",
        capped.out,
    );
    assert_eq!(
        json(volume, &["cue", "--limit", "1"])["segments"]
            .as_array()
            .map(Vec::len),
        Some(1),
        "the limit should reach the data as well as the text",
    );
}

// the doctor reads the machine, on a root nothing has written
#[test]
fn doctor_reads_the_machine() {
    let dir = tempfile::tempdir().expect("tempdir");

    let doctor = run(dir.path(), &["doctor"]);
    assert!(doctor.ok, "doctor failed: {}", doctor.err);
    assert!(
        doctor.out.contains("this machine"),
        "no column for what the machine argues: {}",
        doctor.out
    );
    assert!(
        doctor.out.contains("plane:"),
        "the reason should be named beside the knob it explains: {}",
        doctor.out
    );
    assert!(
        doctor.out.contains("knobs"),
        "the verdict should weigh the knobs against each other: {}",
        doctor.out
    );
}

// a volume flag the tool cannot read is refused rather than ignored
#[test]
fn refuses_an_unknown_volume_tag() {
    let dir = tempfile::tempdir().expect("tempdir");
    let volume = volume(&dir);

    let cue = run(volume, &["--volume", "/mnt/other:warm", "cue"]);
    assert!(!cue.ok, "an unknown tier should be refused: {}", cue.out);
    assert!(
        cue.err.contains("warm"),
        "the refusal should name the tag: {}",
        cue.err
    );
}

// a resident stat counts what the volume actually holds
#[test]
fn stat_counts_the_live_records() {
    let dir = tempfile::tempdir().expect("tempdir");
    let volume = volume(&dir);

    let stat = run(volume, &["--column", "records:1:32", "stat"]);
    assert!(stat.ok, "stat failed: {}", stat.err);
    let row = row(&stat.out, RECORD_CF);
    // column, id, runs, records, bytes
    let records: u64 = row
        .split_whitespace()
        .nth(3)
        .and_then(|count| count.parse().ok())
        .unwrap_or_else(|| panic!("no record count on the column row: {row}"));
    assert_eq!(
        records,
        u64::from(RECORDS),
        "every key written once survives its overwrite: {row}",
    );
    // The overwrites left the versions they replaced behind.
    assert!(
        figure(
            &json(volume, &["--column", "records:1:32", "stat"]),
            "dead_bytes"
        ) > 0,
        "the overwrites should weigh something dead: {}",
        stat.out,
    );
}

// a paged stat says which numbers it cannot give rather than giving zero
#[test]
fn stat_paged_admits_what_it_cannot_count() {
    let dir = tempfile::tempdir().expect("tempdir");
    let volume = volume(&dir);

    let stat = run(volume, &["--column", "records:1:32", "--paged", "stat"]);
    assert!(stat.ok, "stat failed: {}", stat.err);
    let row = row(&stat.out, RECORD_CF);
    assert!(
        row.contains('-'),
        "an unanswerable count should be a dash: {row}"
    );
    // The caveat has to reach the data too: a consumer reading only the figures
    // would otherwise take a floor for the total.
    let caveats = caveats(&json(
        volume,
        &["--column", "records:1:32", "--paged", "stat"],
    ));
    assert!(
        caveats.iter().any(|caveat| caveat.contains("floors")),
        "a paged open owes the reader the floor caveat: {caveats:?}",
    );
    assert!(
        stat.out.contains("floors"),
        "and owes it in the text as well: {}",
        stat.out,
    );
}

// a sound volume verifies clean, and says how much it read
#[test]
fn verify_passes_a_sound_volume() {
    let dir = tempfile::tempdir().expect("tempdir");
    let volume = volume(&dir);

    let verify = run(volume, &["verify"]);
    assert!(verify.ok, "verify failed: {}", verify.err);
    assert!(
        verify.out.contains("CLEAN"),
        "no clean verdict: {}",
        verify.out
    );
    assert!(
        verify.err.is_empty(),
        "a redirected sweep should draw no progress: {:?}",
        verify.err,
    );

    let swept = json(volume, &["verify"]);
    assert!(
        swept["not_indexed"]
            .as_array()
            .is_some_and(|files| files.is_empty()),
        "every file should be one the index names: {swept}",
    );
    assert!(
        caveats(&swept).is_empty(),
        "a clean sweep of a whole volume owes the reader nothing: {swept}",
    );
    assert!(
        figure(&swept, "records") > 0,
        "a sweep of nothing is not a clean bill: {swept}",
    );
    // Every segment file on the root is swept, not just the ones still holding
    // a live key.
    assert_eq!(
        figure(&swept, "segments_swept"),
        segments(volume).len() as u64,
        "the sweep should cover every segment file: {swept}",
    );
}

// a flipped byte inside a record is caught, and fails the exit code
#[test]
fn verify_catches_a_flipped_byte() {
    let dir = tempfile::tempdir().expect("tempdir");
    let volume = volume(&dir);
    let target = segments(volume).into_iter().next().expect("a segment file");
    let len = std::fs::metadata(&target).expect("stat").len();
    flip(&target, len * 3 / 5);

    let verify = run(volume, &["verify"]);
    assert!(
        !verify.ok,
        "a flipped byte should fail the sweep: {}",
        verify.out
    );
    assert!(
        !verify.out.contains("CLEAN"),
        "a faulted sweep is not clean: {}",
        verify.out
    );
    assert!(
        verify.out.contains("FAULTS"),
        "the fault should be named, not only counted: {}",
        verify.out,
    );
    assert!(
        figure(&json(volume, &["verify"]), "faults") > 0,
        "the fault should be counted: {}",
        verify.out,
    );
}

// a truncated segment is caught, footer and all
#[test]
fn verify_catches_a_truncated_segment() {
    let dir = tempfile::tempdir().expect("tempdir");
    let volume = volume(&dir);
    let target = segments(volume).into_iter().next().expect("a segment file");
    let len = std::fs::metadata(&target).expect("stat").len();
    // Half of a segment lands inside a record, and takes the footer with it, so
    // the sweep falls back to walking and meets the torn record.
    std::fs::File::options()
        .write(true)
        .open(&target)
        .expect("open")
        .set_len(len / 2)
        .expect("truncate");

    let verify = run(volume, &["verify"]);
    assert!(
        !verify.ok,
        "a truncated segment should fail the sweep: {}",
        verify.out
    );
    assert!(
        figure(&json(volume, &["verify"]), "faults") > 0,
        "the fault should be counted: {}",
        verify.out,
    );
}

/// The volume's segment files, largest first
fn segments(volume: &Path) -> Vec<std::path::PathBuf> {
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(volume)
        .expect("read the volume")
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|suffix| suffix == "reel"))
        .collect();
    files.sort_by_key(|path| std::cmp::Reverse(std::fs::metadata(path).expect("stat").len()));
    files
}

/// Turn one byte of a file over, which is what a checksum is for
fn flip(path: &Path, at: u64) {
    let mut file = std::fs::File::options()
        .read(true)
        .write(true)
        .open(path)
        .expect("open");
    file.seek(SeekFrom::Start(at)).expect("seek");
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).expect("read");
    file.seek(SeekFrom::Start(at)).expect("seek");
    file.write_all(&[byte[0] ^ 0xFF]).expect("write");
}

// json output parses, for the commands a script would read
// spans stand only over a paged open, and say so on a resident one
#[test]
fn paged_spans() {
    let dir = tempfile::tempdir().expect("tempdir");
    let volume = volume(&dir);

    let resident = run(volume, &["--column", "records:1:32", "spans"]);
    assert!(resident.ok, "spans failed: {}", resident.err);
    assert!(
        resident.out.contains("records") && resident.out.contains("--paged"),
        "a resident open should name the open that answers: {}",
        resident.out
    );

    let paged = run(volume, &["--column", "records:1:32", "--paged", "spans"]);
    assert!(paged.ok, "paged spans failed: {}", paged.err);
    assert!(
        counted(&paged.out, "records") > 0,
        "a paged open should count the sealed segments: {}",
        paged.out
    );
}

// a checkpoint publishes a directory that opens as a volume of its own
#[test]
fn checkpoint_copy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let volume = volume(&dir);
    let target = dir.path().parent().expect("parent").join("copy");

    let taken = run(volume, &["--column", "records:1:32", "checkpoint"]);
    assert!(!taken.ok, "a checkpoint with no target should be refused");

    let target_arg = target.to_string_lossy().to_string();
    let taken = run(
        volume,
        &["--column", "records:1:32", "checkpoint", &target_arg],
    );
    assert!(taken.ok, "checkpoint failed: {}", taken.err);
    assert!(
        taken.out.contains("segments linked"),
        "the report should say what it linked: {}",
        taken.out
    );

    let copied = run(&target, &["--column", "records:1:32", "stat"]);
    assert!(
        copied.ok,
        "the copy should open as a volume: {}",
        copied.err
    );

    // Publishing over one is refused, so the second run leaves the first alone.
    let again = run(
        volume,
        &["--column", "records:1:32", "checkpoint", &target_arg],
    );
    assert!(
        !again.ok,
        "a second checkpoint should not publish over the first"
    );
    std::fs::remove_dir_all(&target).expect("remove the copy");
}

/// The first figure on the row a table labels with this name
fn counted(out: &str, label: &str) -> u64 {
    row(out, label)
        .split_whitespace()
        .nth(1)
        .and_then(|figure| figure.parse().ok())
        .unwrap_or_else(|| panic!("no figure on the {label} row in:\n{out}"))
}

#[test]
fn json_parses() {
    let dir = tempfile::tempdir().expect("tempdir");
    let volume = volume(&dir);

    for args in [
        vec!["-o", "json", "cue"],
        vec!["-o", "json", "--column", "records:1:32", "cue"],
        vec!["-o", "json", "--column", "records:1:32", "stat"],
        vec!["-o", "json", "--column", "records:1:32", "spans"],
        vec!["-o", "json", "verify"],
        vec!["-o", "json", "doctor"],
    ] {
        let run = run(volume, &args);
        assert!(run.ok, "{args:?} failed: {}", run.err);
        serde_json::from_str::<serde_json::Value>(&run.out).unwrap_or_else(|error| {
            panic!("{args:?} produced invalid json ({error}): {}", run.out)
        });
    }
}

// markdown carries the tables, for a pull request or a CI summary
#[test]
fn markdown_carries_the_tables() {
    let dir = tempfile::tempdir().expect("tempdir");
    let volume = volume(&dir);

    let stat = run(
        volume,
        &["-o", "markdown", "--column", "records:1:32", "stat"],
    );
    assert!(stat.ok, "markdown stat failed: {}", stat.err);
    assert!(
        stat.out.starts_with("## "),
        "the head should be a heading: {}",
        stat.out
    );
    assert!(
        stat.out
            .contains("| column | id | runs | records | bytes |"),
        "the column table should survive as a table: {}",
        stat.out,
    );
    assert!(
        stat.out.contains("| --- | ---: |"),
        "figures should be aligned right: {}",
        stat.out
    );
    assert!(
        stat.out.lines().any(|line| line.starts_with("**")),
        "the verdict should carry: {}",
        stat.out
    );
}

// nothing is dressed unless somebody asked for it or is watching
#[test]
fn text_is_plain_off_a_terminal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let volume = volume(&dir);

    // The test harness gives the child a pipe, which is the case that matters:
    // escape sequences in a captured log are noise a reader cannot turn off.
    for args in [vec!["cue"], vec!["--color", "never", "cue"]] {
        let cue = run(volume, &args);
        assert!(cue.ok, "{args:?} failed: {}", cue.err);
        assert!(
            !cue.out.contains('\x1b'),
            "{args:?} dressed a pipe: {:?}",
            cue.out,
        );
        assert!(
            !cue.out.contains('╭'),
            "{args:?} framed a pipe: {:?}",
            cue.out
        );
    }

    let asked = run(volume, &["--color", "always", "cue"]);
    assert!(asked.ok, "cue failed: {}", asked.err);
    assert!(
        asked.out.contains('\x1b') && asked.out.contains('╭'),
        "asking for colour should dress it anyway: {:?}",
        asked.out,
    );
}

// a caveat is a figure's own, and travels with it into the data
#[test]
fn caveats_travel_with_the_figures() {
    let dir = tempfile::tempdir().expect("tempdir");
    let volume = volume(&dir);

    let undeclared = json(volume, &["cue"]);
    let owed = caveats(&undeclared);
    assert!(
        owed.iter()
            .any(|caveat| caveat.contains("no columns declared")),
        "an undeclared open should say what it is not counting: {owed:?}",
    );
    assert!(
        undeclared["caveats"][0]["fix"].is_string(),
        "a caveat with an answer should name it: {undeclared}",
    );
    assert!(
        run(volume, &["cue"]).out.contains("NOT COUNTED"),
        "and should reach the text as its own block",
    );

    // Declaring the columns answers that one, so it stops being said.
    let declared = caveats(&json(
        volume,
        &["--column", "records:1:32", "--paged", "cue"],
    ));
    assert!(
        !declared
            .iter()
            .any(|caveat| caveat.contains("no columns declared")),
        "a declared open should not still be owed the declaration: {declared:?}",
    );
}
