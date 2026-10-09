//! reel: cue up a volume and look inside it
//! Every verb but checkpoint opens read-only without the ownership lock

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
        part of a segment file lists them, so the per-column figures count only \
        what `--column` declares.",
    version
)]
struct Cli {
    /// Volume root, holding the volume's segment files
    #[arg(value_name = "VOLUME")]
    path: PathBuf,

    /// Another root of the same set, repeatable, in written order, tagged `:capacity` or `:dead`
    #[arg(long = "volume", value_name = "PATH[:capacity][:dead]")]
    volumes: Vec<String>,

    /// A column the volume was written with, repeatable, and only declared columns are counted
    #[arg(long = "column", value_name = "NAME:ID[:WIDTH]")]
    columns: Vec<String>,

    /// Output format
    #[arg(short, long, default_value = "text")]
    output: OutputFormat,

    /// Whether to colour and frame text output, auto means a terminal only and honours NO_COLOR
    #[arg(long, value_name = "WHEN", default_value = "auto")]
    color: ColorChoice,

    #[command(subcommand)]
    command: Command,
}

/// What the tool was asked for
#[derive(Subcommand)]
enum Command {
    /// Where the sequence stands, what the segments weigh and how far back a read can reach
    Cue {
        /// Maximum segments to list, most dead first, or zero for all
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    /// What each column holds and what the segments weigh, live against dead
    Stat,
    /// Sealed segments standing over each column
    Spans,
    /// Check every record against its checksum, read-only, exiting nonzero on any fault
    Verify {
        /// Maximum segments to list, faulted first, or zero for all
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Take a durable copy into a new directory, as hard links to the sealed segments
    Checkpoint {
        /// Directory to create, which must not exist
        #[arg(value_name = "TARGET")]
        target: PathBuf,
    },
    /// What this machine suggests for the volume's knobs, beside the defaults
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
        // Reads the machine under the root, so it works before anything is written
        Command::Doctor => emit(cli, &doctor::doctor(&cli.path, &ReelConfig::default())),
    }
}

/// Sweep with a progress bar erased before the report, failing the exit code on any fault
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

/// Take a durable copy, the one verb that writes and takes the lock
fn copy(cli: &Cli, target: &Path) -> Fallible<ExitCode> {
    let taken = cli.open_primary()?.checkpoint(target)?;
    emit(cli, &checkpoint::checkpoint(&taken, target))
}

impl Cli {
    /// Open the volume read-only without the lock, parsing every flag before touching disk
    fn open(&self) -> Fallible<ReelStore> {
        Ok(ReelStore::open_read_only(
            self.path.clone(),
            self.config()?,
            spec::columns(&self.columns)?,
        )?)
    }

    /// Open for writing, which sealing a tail needs and which takes the lock
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
