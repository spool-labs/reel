//! Hammer a reel volume on a real disk with a tape node's workload, until something breaks
//!
//! Each life is a child process that ends in a clean close or a kill -9. The supervisor
//! reopens the volume after each one and checks it holds a prefix of the calls that returned.

mod model;
mod stats;
mod worker;

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, ExitCode};
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

use clap::{Args, Parser, Subcommand};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IoBackend, KeyWidth, ReelConfig, ReelStore,
    SyncPolicy, ThreadBudget, MAP_EVERYTHING,
};
use reel_core::Store;

use crate::model::{
    check_prefix, header_of, read_log, show, Baseline, Held, Slot, Verdict, HEADER,
};

#[derive(Parser)]
#[command(about = "Hammer a reel volume with a tape node's workload, kills included")]
struct Cli {
    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand)]
enum Mode {
    /// Run lives until a violation or the time is up
    Run(Options),
    /// One life, started by `run`
    #[command(hide = true)]
    Life {
        #[arg(long)]
        index: u64,
        #[command(flatten)]
        options: Options,
    },
}

#[derive(Args, Clone)]
pub struct Options {
    /// Directory for the volume, op logs, metrics and any violation
    #[arg(long)]
    pub dir: PathBuf,
    /// Workload seed, drawn and printed when left at zero
    #[arg(long, default_value_t = 0)]
    pub seed: u64,
    /// posix, uring or mixed
    #[arg(long, default_value = "mixed")]
    pub backend: String,
    /// Append tails
    #[arg(long, default_value_t = 2)]
    pub tails: u32,
    /// Segment size in MiB
    #[arg(long, default_value_t = 256)]
    pub segment_mib: u64,
    /// MiB written between syncs, the node's default
    #[arg(long, default_value_t = 16)]
    pub sync_mib: u64,
    /// Live GiB above which the workload only expires tapes
    #[arg(long, default_value_t = 20)]
    pub max_gib: u64,
    /// Reader threads
    #[arg(long, default_value_t = 4)]
    pub readers: usize,
    /// Longest a life runs, in seconds
    #[arg(long, value_parser = seconds, default_value = "300")]
    pub life: Duration,
    /// Share of lives that end in kill -9 at a random moment
    #[arg(long, default_value_t = 0.5)]
    pub kill_ratio: f64,
    /// Seconds the whole run lasts, zero for no end
    #[arg(long, value_parser = seconds, default_value = "0")]
    pub duration: Duration,
    /// Seconds between metric lines
    #[arg(long, value_parser = seconds, default_value = "10")]
    pub tick: Duration,
    /// Serve reads from segment mappings, which the node leaves off
    #[arg(long)]
    pub map: bool,
}

fn seconds(raw: &str) -> Result<Duration, String> {
    raw.parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|error| error.to_string())
}

const fn plain(id: u8, name: &'static str) -> ColumnSpec {
    ColumnSpec {
        id: ColumnId(id),
        name,
        key_width: KeyWidth::Variable,
        shard_bytes: 0,
        purge_mark: None,
        codec: Codec::Lz4,
    }
}

const fn shaped(id: u8, name: &'static str, width: u16, shard_bytes: u8) -> ColumnSpec {
    ColumnSpec {
        id: ColumnId(id),
        name,
        key_width: KeyWidth::Fixed(width),
        shard_bytes,
        purge_mark: None,
        codec: Codec::Lz4,
    }
}

/// The columns a tape node declares, with its ids and shapes
const COLUMNS: [ColumnSpec; 29] = [
    plain(1, "meta"),
    plain(2, "tape"),
    shaped(3, "track", 32, 1),
    plain(4, "track_lookup"),
    shaped(5, "track_data", 32, 1),
    plain(6, "object_info"),
    plain(7, "object_metadata"),
    plain(8, "object_list"),
    plain(9, "sync_cursor"),
    plain(10, "gc"),
    plain(11, "spool_status"),
    shaped(12, "spool_pending_repair", 34, 2),
    shaped(13, "spool_pending_recovery", 34, 2),
    shaped(14, "slice", 34, 2),
    plain(21, "event_log"),
    plain(22, "vote_sig"),
    shaped(23, "snapshot_artifact", 24, 0),
    plain(24, "credential"),
    plain(25, "policy_rule"),
    plain(26, "auth_state"),
    plain(27, "audit_log"),
    plain(28, "ledger"),
    plain(29, "ledger_reservation"),
    plain(30, "s3_multipart_upload"),
    plain(31, "s3_multipart_part"),
    plain(32, "s3_multipart_part_data"),
    plain(33, "s3_pending_write"),
    plain(34, "s3_pending_write_data"),
    plain(35, "removal_log"),
];

pub fn columns() -> ColumnSet {
    &COLUMNS
}

pub fn column_name(column: u8) -> Result<&'static str, String> {
    COLUMNS
        .iter()
        .find(|spec| spec.id.as_u8() == column)
        .map(|spec| spec.name)
        .ok_or_else(|| format!("column {column} is not declared"))
}

static VOLUME: OnceLock<PathBuf> = OnceLock::new();

pub fn open_store(options: &Options) -> reel::Result<ReelStore> {
    let io_backend = match options.backend.as_str() {
        "posix" => IoBackend::Posix,
        "uring" => IoBackend::Uring,
        _ => IoBackend::Mixed,
    };
    let config = ReelConfig {
        segment_bytes: ByteCount::from_bytes(options.segment_mib << 20),
        sync: SyncPolicy::Bytes(ByteCount::from_bytes(options.sync_mib << 20)),
        map_above: match options.map {
            true => MAP_EVERYTHING,
            false => None,
        },
        io_backend,
        active_tails: ThreadBudget::threads(options.tails),
        ..ReelConfig::default()
    };
    let root = VOLUME.get_or_init(|| options.dir.join("volume")).clone();
    ReelStore::open(root, config, columns())
}

/// Every key the volume answers for, with its value's header
fn observe(options: &Options) -> Result<HashMap<Slot, Held>, String> {
    let store = open_store(options).map_err(|error| format!("reopen: {error}"))?;
    let mut seen = HashMap::new();
    for spec in columns() {
        let column = spec.id.as_u8();
        let keys = store
            .iter_keys_prefix(spec.name, b"")
            .map_err(|error| format!("keys of {}: {error}", spec.name))?;
        for key in keys {
            let slot = (column, key);
            let head = Store::get_range(&store, spec.name, &slot.1, 0, HEADER)
                .map_err(|error| format!("get_range: {error}"))?
                .ok_or_else(|| format!("{} is listed but reads as missing", show(&slot)))?;
            let held = header_of(column, &slot.1, &head)
                .map_err(|error| format!("{}: {error}", show(&slot)))?;
            seen.insert(slot, held);
        }
    }
    store
        .close()
        .map_err(|error| format!("close after the check: {error}"))?;
    Ok(seen)
}

/// The run's journal, printed and kept in the run dir
struct Journal {
    file: File,
}

impl Journal {
    fn say(&mut self, line: String) {
        println!("{line}");
        let _ = writeln!(self.file, "{line}");
    }
}

/// How one life ended
struct Ending {
    killed: bool,
    code: Option<i32>,
    seconds: f64,
}

fn live_once(options: &Options, life: u64, rng: &mut SmallRng) -> Result<Ending, String> {
    let longest = options.life.as_millis().max(3_001) as u64;
    let kill_at = rng
        .gen_bool(options.kill_ratio)
        .then(|| Duration::from_millis(rng.gen_range(3_000..longest)));
    let program = std::env::current_exe().map_err(|error| format!("own path: {error}"))?;
    let mut child = Command::new(program)
        .args(child_args(options, life))
        .spawn()
        .map_err(|error| format!("spawn life {life}: {error}"))?;
    let born = Instant::now();
    loop {
        if let Some(status) = child.try_wait().map_err(|error| format!("wait: {error}"))? {
            return Ok(Ending {
                killed: false,
                code: status.code(),
                seconds: born.elapsed().as_secs_f64(),
            });
        }
        if kill_at.is_some_and(|at| born.elapsed() >= at) {
            let _ = child.kill();
            let _ = child.wait();
            return Ok(Ending {
                killed: true,
                code: None,
                seconds: born.elapsed().as_secs_f64(),
            });
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn verify(options: &Options, life: u64, base: &Baseline, killed: bool) -> Result<Verdict, String> {
    let seen = observe(options)?;
    let ops = read_log(&options.dir.join(format!("life-{life}.ops")))?;
    check_prefix(base, &ops, &seen, !killed)
}

fn supervise(mut options: Options) -> Result<ExitCode, String> {
    if options.seed == 0 {
        options.seed = rand::random();
    }
    let dir = options.dir.clone();
    std::fs::create_dir_all(&dir).map_err(|error| format!("create {}: {error}", dir.display()))?;
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("hammer.log"))
        .map_err(|error| format!("hammer.log: {error}"))?;
    let mut journal = Journal { file };
    journal.say(format!(
        "seed {} backend {} tails {} segment {} MiB sync {} MiB readers {} life {} s kill ratio {} mapped {}",
        options.seed, options.backend, options.tails, options.segment_mib, options.sync_mib, options.readers, options.life.as_secs(), options.kill_ratio, options.map
    ));

    let mut rng = SmallRng::seed_from_u64(options.seed);
    let started = Instant::now();
    let baseline = dir.join("baseline.bin");
    let mut life = 0u64;
    while options.duration.is_zero() || started.elapsed() < options.duration {
        let base = Baseline::load(&baseline)?;
        let _ = std::fs::copy(&baseline, dir.join("baseline-prev.bin"));
        let ending = live_once(&options, life, &mut rng)?;
        let how = match ending.killed {
            true => "killed",
            false => "closed",
        };
        if !ending.killed && ending.code != Some(0) {
            journal.say(format!(
                "VIOLATION in life {life} (exit {:?}), see life-{life}.err, volume kept in {}",
                ending.code,
                dir.display()
            ));
            return Ok(ExitCode::FAILURE);
        }
        let check = Instant::now();
        match verify(&options, life, &base, ending.killed) {
            Ok(verdict) => {
                journal.say(format!(
                    "life {life} {how} after {:.1} s: {} of {} ops kept, {} lost, {} keys, check {:.1} s",
                    ending.seconds,
                    verdict.kept,
                    verdict.logged,
                    verdict.logged - verdict.kept,
                    verdict.next.held.len(),
                    check.elapsed().as_secs_f64()
                ));
                verdict.next.save(&baseline)?;
                let _ =
                    std::fs::remove_file(dir.join(format!("life-{}.ops", life.saturating_sub(2))));
            }
            Err(error) => {
                journal.say(format!("VIOLATION after life {life} ({how}): {error}. Volume, life-{life}.ops and baseline-prev.bin kept in {}", dir.display()));
                let _ = std::fs::write(dir.join("violation.txt"), &error);
                return Ok(ExitCode::FAILURE);
            }
        }
        life += 1;
    }
    journal.say(format!("done after {life} lives, no violation"));
    Ok(ExitCode::SUCCESS)
}

fn child_args(options: &Options, life: u64) -> Vec<String> {
    let pairs = [
        ("--index", life.to_string()),
        ("--dir", options.dir.display().to_string()),
        ("--seed", options.seed.to_string()),
        ("--backend", options.backend.clone()),
        ("--tails", options.tails.to_string()),
        ("--segment-mib", options.segment_mib.to_string()),
        ("--sync-mib", options.sync_mib.to_string()),
        ("--max-gib", options.max_gib.to_string()),
        ("--readers", options.readers.to_string()),
        ("--life", options.life.as_secs().to_string()),
        ("--tick", options.tick.as_secs().to_string()),
    ];
    let mut args = vec!["life".to_string()];
    for (flag, value) in pairs {
        args.push(flag.to_string());
        args.push(value);
    }
    if options.map {
        args.push("--map".to_string());
    }
    args
}

fn run_life(index: u64, options: &Options) -> ExitCode {
    let outcome = Baseline::load(&options.dir.join("baseline.bin"))
        .and_then(|base| worker::run(options, index, base));
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("life {index}: {error}");
            let _ = std::fs::write(options.dir.join(format!("life-{index}.err")), &error);
            ExitCode::from(2)
        }
    }
}

fn main() -> ExitCode {
    match Cli::parse().mode {
        Mode::Run(options) => supervise(options).unwrap_or_else(|error| {
            eprintln!("reel-hammer: {error}");
            ExitCode::FAILURE
        }),
        Mode::Life { index, options } => run_life(index, &options),
    }
}
