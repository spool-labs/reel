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
        cue.out.contains("sequence"),
        "no sequence line: {}",
        cue.out
    );
    assert!(
        cue.out.contains("segment"),
        "no segment header: {}",
        cue.out
    );
    assert!(
        cue.out.contains("none in this process"),
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
    let row = cue
        .out
        .lines()
        .find(|line| line.starts_with(RECORD_CF))
        .unwrap_or_else(|| panic!("the declared column is missing: {}", cue.out));
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

    let all = run(volume, &["cue"]);
    assert!(all.ok, "cue failed: {}", all.err);
    let held = all
        .out
        .lines()
        .find_map(|line| line.strip_prefix("segments          "))
        .and_then(|count| count.trim().parse::<usize>().ok())
        .unwrap_or_else(|| panic!("no segment count: {}", all.out));
    assert!(
        held > 1,
        "the writes should have filled more than one segment"
    );
    assert!(
        all.out
            .contains(&format!("showing {held} of {held} segments")),
        "an unlimited listing shows every segment: {}",
        all.out,
    );

    let capped = run(volume, &["cue", "--limit", "1"]);
    assert!(capped.ok, "cue failed: {}", capped.err);
    assert!(
        capped
            .out
            .contains(&format!("showing 1 of {held} segments")),
        "the limit should cap the listing: {}",
        capped.out,
    );
}

// the doctor reads the machine, on a root nothing has written
#[test]
fn doctor_reads_the_machine() {
    let dir = tempfile::tempdir().expect("tempdir");

    let doctor = run(dir.path(), &["doctor"]);
    assert!(doctor.ok, "doctor failed: {}", doctor.err);
    assert!(
        doctor.out.contains("because:"),
        "no reason line: {}",
        doctor.out
    );
    assert!(
        doctor.out.contains("verdict"),
        "no verdict column: {}",
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
    let row = stat
        .out
        .lines()
        .find(|line| line.starts_with(RECORD_CF))
        .unwrap_or_else(|| panic!("the declared column is missing: {}", stat.out));
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
        !stat.out.contains("dead bytes        0 B"),
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
    let row = stat
        .out
        .lines()
        .find(|line| line.starts_with(RECORD_CF))
        .unwrap_or_else(|| panic!("the declared column is missing: {}", stat.out));
    assert!(
        row.contains('-'),
        "an unanswerable count should be a dash: {row}"
    );
    assert!(
        stat.out.contains("are floors"),
        "a paged open owes the reader the floor caveat: {}",
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
        verify.out.contains("clean, "),
        "no clean verdict: {}",
        verify.out
    );
    assert!(
        verify.out.contains("files not indexed 0"),
        "every file should be one the index names: {}",
        verify.out,
    );
    let records = swept(&verify.out, "records checked   ");
    assert!(
        records > 0,
        "a sweep of nothing is not a clean bill: {}",
        verify.out
    );
    // Every segment file on the root is swept, not just the ones still holding
    // a live key.
    let files = segments(volume).len();
    assert_eq!(
        swept(&verify.out, "segments swept    "),
        files as u64,
        "the sweep should cover every segment file: {}",
        verify.out,
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
        !verify.out.contains("clean, "),
        "a faulted sweep is not clean: {}",
        verify.out
    );
    assert!(
        swept(&verify.out, "faults            ") > 0,
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
        swept(&verify.out, "faults            ") > 0,
        "the fault should be counted: {}",
        verify.out,
    );
}

/// A headline figure out of a report
fn swept(out: &str, label: &str) -> u64 {
    out.lines()
        .find_map(|line| line.strip_prefix(label))
        .and_then(|count| count.trim().parse().ok())
        .unwrap_or_else(|| panic!("no `{label}` line: {out}"))
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
        resident.out.contains("records") && resident.out.contains("pass --paged"),
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
    out.lines()
        .find(|line| line.starts_with(label))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|figure| figure.parse().ok())
        .unwrap_or_else(|| panic!("no row for {label} in:\n{out}"))
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
