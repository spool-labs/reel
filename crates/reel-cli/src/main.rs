//! reel: cue up a volume and look inside it.
//!
//! Points at a volume root and asks the volume about itself: where its sequence
//! stands, what its segments weigh, the range deletes standing over them, the
//! cue points held. It also sweeps records against their checksums and takes a
//! durable copy. Every verb but the copy opens read-only and takes no ownership
//! lock, so it reads a volume something else is writing.
//!
//! A volume's columns are the declaration of whatever wrote it and no part of a
//! segment file names them, so the per column figures count only the columns
//! declared with `--column`. The reports live in the engine, so this binary is
//! argument parsing and a match arm per verb.

use std::error::Error;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;

use reel::report::render::{self, Render};
use reel::report::{checkpoint, cue, doctor, spans, spec, stat, verify};
use reel::{IndexResidency, ReelConfig, ReelStore};

type Fallible<T> = Result<T, Box<dyn Error>>;

/// Text (human-readable) or json output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
#[clap(rename_all = "lowercase")]
enum OutputFormat {
    #[default]
    Text,
    Json,
}

/// Print a report in the requested format.
fn emit<Report>(report: &Report, format: OutputFormat) -> Fallible<ExitCode>
where
    Report: Render + Serialize,
{
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(report)?),
        OutputFormat::Text => print!("{}", render::text(report)),
    }
    Ok(ExitCode::SUCCESS)
}

#[derive(Parser)]
#[command(
    name = "reel",
    about = "Cue up a reel volume and look inside it",
    version
)]
struct Cli {
    /// Volume root, holding the volume's segment files.
    #[arg(value_name = "VOLUME")]
    path: PathBuf,

    /// Further roots of the same volume set, in the order the set was written;
    /// repeatable. A bare path is a fast volume; append `:capacity` for the
    /// capacity tier or `:dead` for a drive declared dead. A set spanning
    /// several roots refuses to open without its full list.
    #[arg(long = "volume", value_name = "PATH[:capacity][:dead]")]
    volumes: Vec<String>,

    /// A column the volume was written with, as its name and the identifier its
    /// records are stamped with; repeatable. Append `:WIDTH` where every key in
    /// the column is that many bytes wide. Only declared columns are counted.
    #[arg(long = "column", value_name = "NAME:ID[:WIDTH]")]
    columns: Vec<String>,

    /// Leave the sealed keys in their footers instead of holding every live key
    /// in memory. What a volume larger than the memory here needs, and what
    /// counts the sealed segments standing over each column. It costs the exact
    /// figures: the dead bytes read as a floor and the per-column record counts
    /// fall to what the tails hold.
    #[arg(long)]
    paged: bool,

    /// Output format.
    #[arg(short, long, default_value = "text")]
    output: OutputFormat,

    #[command(subcommand)]
    command: Command,
}

/// What the tool was asked for
///
/// A verb either asks the volume, opening through `Cli::open`, or asks the
/// machine under it and opens nothing. Per-verb arguments ride as fields, so the
/// shared flags stay global and a new verb reshapes neither of these two.
#[derive(Subcommand)]
enum Command {
    /// Where the volume's sequence stands, what its segments weigh, and what a
    /// read can still reach back to.
    Cue {
        /// Maximum segments to list, fullest of dead first.
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// What each column holds and what the segments weigh, live against dead.
    Stat,
    /// Sealed segments standing over each column.
    ///
    /// What a lookup narrows its search with, and what a read at an older
    /// sequence number would need to find a version the map no longer holds.
    Spans,
    /// Sweep the volume's records against their checksums, reporting per segment.
    ///
    /// Exits nonzero if anything is unreadable or fails its checksum. Reads only:
    /// nothing is repaired and nothing is written.
    Verify {
        /// Maximum segments to list, the faulted ones first.
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Take a durable copy of the volume as it stands, into a new directory.
    ///
    /// The copy is hard links to sealed segments, so it costs metadata rather
    /// than bytes and shares them with the volume until compaction moves on.
    /// What comes back opens as a volume: restoring is pointing at it.
    Checkpoint {
        /// Directory to create. Must not exist.
        #[arg(value_name = "TARGET")]
        target: PathBuf,
    },
    /// What this machine argues the volume's knobs should be, beside the defaults.
    Doctor,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(code) => code,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: &Cli) -> Fallible<ExitCode> {
    match &cli.command {
        Command::Cue { limit } => emit(&cue::cue(&cli.open()?, *limit), cli.output),
        Command::Stat => emit(&stat::stat(&cli.open()?), cli.output),
        Command::Spans => emit(&spans::spans(&cli.open()?), cli.output),
        Command::Verify { limit } => sweep(cli, *limit),
        Command::Checkpoint { target } => copy(cli, target),
        // Reads the machine under the root rather than the volume on it, so it
        // answers where nothing has been written yet.
        Command::Doctor => emit(
            &doctor::doctor(&cli.path, &ReelConfig::default()),
            cli.output,
        ),
    }
}

/// Sweep the volume and let the findings be the exit code as well as the report
fn sweep(cli: &Cli, limit: usize) -> Fallible<ExitCode> {
    let swept = verify::verify(&cli.open_named()?, limit);
    let code = emit(&swept, cli.output)?;
    Ok(match swept.is_sound() {
        true => code,
        false => ExitCode::FAILURE,
    })
}

/// Take a durable copy, the one verb that writes and so the one that locks
fn copy(cli: &Cli, target: &Path) -> Fallible<ExitCode> {
    let taken = cli.open_primary()?.checkpoint(target)?;
    emit(&checkpoint::checkpoint(&taken, target), cli.output)
}

impl Cli {
    /// Open the volume the arguments name, read-only and without the lock
    ///
    /// Every verb that asks the volume rather than the machine opens through
    /// here, so adding one is a match arm and nothing else. Both flag sets are
    /// parsed before anything is opened, so a bad flag fails without touching a
    /// disk. A verb that wants one of the declared columns by name asks the
    /// opened store for it rather than re-reading the flags.
    ///
    /// Resident by default, because the numbers are the point: a paged rebuild
    /// attributes no bytes to the sealed segments it leaves in their footers, so
    /// the dead figures come back a floor. Paged answers the one thing resident
    /// cannot, the sealed segments standing over a column, and is what a volume
    /// larger than this machine's memory has to use.
    fn open(&self) -> Fallible<ReelStore> {
        Ok(ReelStore::open_read_only(
            self.path.clone(),
            self.config(match self.paged {
                true => IndexResidency::Paged,
                false => IndexResidency::Resident,
            })?,
            spec::columns(&self.columns)?,
        )?)
    }

    /// Open with every sealed segment named, whatever the flags asked for
    ///
    /// A resident open lists only the segments still holding a live key, so a
    /// segment whose every record has been superseded is absent from its table.
    /// A sweep reading that as the volume's segment list would call those files
    /// strangers. Paged names them all, and a sweep wants none of the resident
    /// figures anyway.
    fn open_named(&self) -> Fallible<ReelStore> {
        Ok(ReelStore::open_read_only(
            self.path.clone(),
            self.config(IndexResidency::Paged)?,
            spec::columns(&self.columns)?,
        )?)
    }

    /// Open for writing, which is what sealing a tail needs and what takes the lock
    fn open_primary(&self) -> Fallible<ReelStore> {
        Ok(ReelStore::open(
            self.path.clone(),
            self.config(match self.paged {
                true => IndexResidency::Paged,
                false => IndexResidency::Resident,
            })?,
            spec::columns(&self.columns)?,
        )?)
    }

    fn config(&self, index: IndexResidency) -> Fallible<ReelConfig> {
        Ok(ReelConfig {
            volumes: spec::volumes(&self.volumes)?,
            index,
            ..ReelConfig::default()
        })
    }
}
