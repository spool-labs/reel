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
//! argument parsing, a match arm per verb, and the two things the engine will
//! not do for itself: decide whether there is a terminal out there, and draw a
//! bar for the one verb slow enough to need one.

mod progress;
mod term;

use std::error::Error;
use std::io::{stderr, stdout, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;

use reel::report::render::{self, Report};
use reel::report::{checkpoint, cue, doctor, spans, spec, stat, verify};
use reel::{ReelConfig, ReelStore};

use term::ColorChoice;

type Fallible<T> = Result<T, Box<dyn Error>>;

/// How a report should be written out
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
#[clap(rename_all = "lowercase")]
enum OutputFormat {
    /// For a person at a terminal, framed and coloured where there is one
    #[default]
    Text,

    /// Every figure and every caveat as data, for a script or an agent
    Json,

    /// For a pull request, an issue, or a CI job summary
    Markdown,
}

/// Print a report in the requested format
///
/// Json is the complete record: it carries every row a listing truncates and
/// every caveat the text form renders as a note, so a consumer never has to
/// parse prose to learn that a figure is a floor.
fn emit<Model>(cli: &Cli, report: &Model) -> Fallible<ExitCode>
where
    Model: Report + Serialize,
{
    let out = stdout();
    match cli.output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(report)?),
        OutputFormat::Markdown => print!("{}", render::markdown(report)),
        OutputFormat::Text => {
            let style = term::style(cli.color, &out);
            let mut lock = out.lock();
            write!(lock, "{}", render::text(report, &style))?;
            lock.flush()?;
        }
    }
    Ok(ExitCode::SUCCESS)
}

#[derive(Parser)]
#[command(
    name = "reel",
    about = "Cue up a reel volume and look inside it",
    long_about = "Cue up a reel volume and look inside it.\n\n\
        Point at a volume root and ask the volume about itself: where its \
        sequence stands, what its segments weigh, what a read can still reach \
        back to. Every verb but `checkpoint` opens read-only and takes no \
        ownership lock, so it reads a volume something else is writing.\n\n\
        A volume's columns are the declaration of whatever wrote it, and no \
        part of a segment file names them, so the per-column figures count only \
        what `--column` declares.",
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

    /// Output format.
    #[arg(short, long, default_value = "text")]
    output: OutputFormat,

    /// Whether text output may be coloured and framed. Auto dresses a terminal
    /// and leaves a pipe, a file and a CI log plain. NO_COLOR is honoured.
    #[arg(long, value_name = "WHEN", default_value = "auto")]
    color: ColorChoice,

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
        /// Maximum segments to list, fullest of dead first. Zero lists them all.
        #[arg(long, default_value_t = 10)]
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
        /// Maximum segments to list, the faulted ones first. Zero lists them all.
        #[arg(long, default_value_t = 20)]
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
        Command::Cue { limit } => emit(cli, &cue::cue(&cli.open()?, *limit)),
        Command::Stat => emit(cli, &stat::stat(&cli.open()?)),
        Command::Spans => emit(cli, &spans::spans(&cli.open()?)),
        Command::Verify { limit } => sweep(cli, *limit),
        Command::Checkpoint { target } => copy(cli, target),
        // Reads the machine under the root rather than the volume on it, so it
        // answers where nothing has been written yet.
        Command::Doctor => emit(cli, &doctor::doctor(&cli.path, &ReelConfig::default())),
    }
}

/// Sweep the volume and let the findings be the exit code as well as the report
///
/// The sweep is the one verb that can take minutes, so it draws its progress
/// where somebody is watching. The bar is erased before the report is written,
/// which is what keeps it out of a terminal's scrollback as well as out of a
/// redirected stream.
fn sweep(cli: &Cli, limit: usize) -> Fallible<ExitCode> {
    let store = cli.open()?;
    let mut bar = progress::Bar::new("SWEPT", term::watch(cli.color, &stderr()));
    let swept = verify::verify_watched(&store, limit, &mut |swept| bar.show(swept.fraction()));
    bar.done();

    let code = emit(cli, &swept)?;
    Ok(match swept.is_sound() {
        true => code,
        false => ExitCode::FAILURE,
    })
}

/// Take a durable copy, the one verb that writes and so the one that locks
fn copy(cli: &Cli, target: &Path) -> Fallible<ExitCode> {
    let taken = cli.open_primary()?.checkpoint(target)?;
    emit(cli, &checkpoint::checkpoint(&taken, target))
}

impl Cli {
    /// Open the volume the arguments name, read-only and without the lock
    ///
    /// Every verb that asks the volume rather than the machine opens through
    /// here, so adding one is a match arm and nothing else. Both flag sets are
    /// parsed before anything is opened, so a bad flag fails without touching a
    /// disk. A verb that wants one of the declared columns by name asks the
    /// opened store for it rather than re-reading the flags.
    fn open(&self) -> Fallible<ReelStore> {
        Ok(ReelStore::open_read_only(
            self.path.clone(),
            self.config()?,
            spec::columns(&self.columns)?,
        )?)
    }

    /// Open for writing, which is what sealing a tail needs and what takes the lock
    fn open_primary(&self) -> Fallible<ReelStore> {
        Ok(ReelStore::open(
            self.path.clone(),
            self.config()?,
            spec::columns(&self.columns)?,
        )?)
    }

    fn config(&self) -> Fallible<ReelConfig> {
        Ok(ReelConfig {
            volumes: spec::volumes(&self.volumes)?,
            ..ReelConfig::default()
        })
    }
}
