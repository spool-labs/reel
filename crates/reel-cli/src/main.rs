//! reel: cue up a volume and look inside it.
//!
//! Points at a volume root and asks the volume about itself: where its sequence
//! stands, the segments it has written and what each weighs live against dead,
//! the range deletes still standing over them, and the cue points held. It also
//! sweeps a volume's records against their checksums. Opens read-only and takes
//! no ownership lock, so it reads a volume something else is writing, though a
//! sweep only means what it says on a volume nothing is appending to.
//!
//! A volume's columns are the declaration of whatever wrote it, and no part of a
//! segment file names them, so the figures that are counted per column are
//! counted only for the columns the caller declares with `--column`. The
//! segment, sequence, sweep and cue figures hold either way, since none of them
//! are a column's.

use std::error::Error;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;

use reel::format::footer::{SegmentFooter, NO_RECORD};
use reel::format::loc::SegmentId;
use reel::format::record::{RecordHeader, HEADER_LEN};
use reel::reel::bias::{access_ranges, MachineFacts, Plane, RingAvailability};
use reel::{
    segment_file_name, ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IndexResidency,
    IoBackend, KeyWidth, MapShape, ReelConfig, ReelStore, VolumeClass, VolumeSpec, MAX_KEY_LEN,
    SEGMENT_SUFFIX,
};

type Fallible<T> = Result<T, Box<dyn Error>>;

/// Text (human-readable) or json output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
#[clap(rename_all = "lowercase")]
enum OutputFormat {
    #[default]
    Text,
    Json,
}

/// A command result: serializable for json, printable for text.
trait CliOutput: Serialize {
    fn print_text(&self);
}

/// Print a command result in the requested format.
fn emit<T: CliOutput>(value: &T, format: OutputFormat) -> Fallible<()> {
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(value)?),
        OutputFormat::Text => value.print_text(),
    }
    Ok(())
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
    /// Sweep the volume's records against their checksums, reporting per segment.
    ///
    /// Exits nonzero if anything is unreadable or fails its checksum. Reads only:
    /// nothing is repaired and nothing is written.
    Verify {
        /// Maximum segments to list, the faulted ones first.
        #[arg(long, default_value_t = 50)]
        limit: usize,
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
    match cli.command {
        Command::Cue { limit } => {
            cue(&cli.open()?, limit, cli.paged, cli.output).map(|()| ExitCode::SUCCESS)
        }
        Command::Stat => stat(&cli.open()?, cli.paged, cli.output).map(|()| ExitCode::SUCCESS),
        // The only verb whose findings are the exit code as well as the report.
        Command::Verify { limit } => verify(&cli.open_named()?, limit, cli.output),
        // Reads the machine under the root rather than the volume on it, so it
        // answers where nothing has been written yet.
        Command::Doctor => doctor(&cli.path, cli.output).map(|()| ExitCode::SUCCESS),
    }
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
        self.open_with(match self.paged {
            true => IndexResidency::Paged,
            false => IndexResidency::Resident,
        })
    }

    /// Open with every sealed segment named, whatever the flags asked for
    ///
    /// A resident open lists only the segments still holding a live key, so a
    /// segment whose every record has been superseded is absent from its table.
    /// A sweep reading that as the volume's segment list would call those files
    /// strangers. Paged names them all, and a sweep wants none of the resident
    /// figures anyway.
    fn open_named(&self) -> Fallible<ReelStore> {
        self.open_with(IndexResidency::Paged)
    }

    fn open_with(&self, index: IndexResidency) -> Fallible<ReelStore> {
        let columns = parse_columns(&self.columns)?;
        let config = ReelConfig {
            volumes: parse_volumes(&self.volumes)?,
            index,
            ..ReelConfig::default()
        };
        Ok(ReelStore::open_read_only(
            self.path.clone(),
            config,
            columns,
        )?)
    }
}

/// Read the volume flags, a path each with its tags riding behind colons
fn parse_volumes(flags: &[String]) -> Fallible<Vec<VolumeSpec>> {
    flags.iter().map(|flag| parse_volume(flag)).collect()
}

fn parse_volume(flag: &str) -> Fallible<VolumeSpec> {
    let mut parts = flag.split(':');
    let path = parts.next().unwrap_or_default();
    if path.is_empty() {
        return Err("--volume needs a path".into());
    }
    let mut spec = VolumeSpec::fast(PathBuf::from(path));
    for tag in parts {
        match tag {
            "fast" => spec.class = VolumeClass::Fast,
            "capacity" => spec.class = VolumeClass::Capacity,
            "dead" => spec.dead = true,
            other => {
                return Err(format!("--volume tag `{other}` is not fast, capacity, or dead").into())
            }
        }
    }
    Ok(spec)
}

/// Read the column flags into the set the volume is opened over
///
/// The engine wants a set that outlives the store, and a set assembled from
/// arguments cannot be a constant, so what is parsed here is leaked. It lives
/// until the process ends either way.
fn parse_columns(flags: &[String]) -> Fallible<ColumnSet> {
    let specs = flags
        .iter()
        .map(|flag| parse_column(flag))
        .collect::<Fallible<Vec<ColumnSpec>>>()?;
    Ok(Vec::leak(specs))
}

fn parse_column(flag: &str) -> Fallible<ColumnSpec> {
    let parts: Vec<&str> = flag.split(':').collect();
    let (name, id, width) = match parts[..] {
        [name, id] => (name, id, None),
        [name, id, width] => (name, id, Some(width)),
        _ => return Err(format!("--column `{flag}` is not NAME:ID or NAME:ID:WIDTH").into()),
    };
    if name.is_empty() {
        return Err("--column needs a name".into());
    }
    let id: u8 = id
        .parse()
        .map_err(|_| format!("--column identifier `{id}` is not a byte"))?;
    let key_width = match width {
        None => KeyWidth::Variable,
        Some(width) => KeyWidth::Fixed(
            width
                .parse()
                .map_err(|_| format!("--column width `{width}` is not a key width"))?,
        ),
    };
    // The rest of a declaration shapes how records are written, and this one only
    // reads, so the report asks for the plainest column that can hold the keys.
    Ok(ColumnSpec {
        id: ColumnId(id),
        name: String::leak(name.to_string()),
        key_width,
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    })
}

/// Report where the volume stands: its sequence, its segments, its covers, its cues
fn cue(engine: &ReelStore, limit: usize, paged: bool, output: OutputFormat) -> Fallible<()> {
    let index = engine.index();
    let mut segments: Vec<SegmentRow> = index
        .segments_snapshot()
        .into_iter()
        .map(|(segment, bytes)| {
            let total = bytes.live + bytes.dead;
            SegmentRow {
                segment: segment.as_u32(),
                live: bytes.live,
                dead: bytes.dead,
                held: bytes.held,
                dead_fraction: match total {
                    0 => 0.0,
                    total => bytes.dead as f64 / total as f64,
                },
            }
        })
        .collect();
    let total_segments = segments.len();
    // Fullest of dead first, since that is the order compaction picks in and the
    // reason anyone lists segments by hand.
    segments.sort_by(|a, b| b.dead_fraction.total_cmp(&a.dead_fraction));
    segments.truncate(limit);

    let columns: Vec<ColumnRow> = engine
        .columns()
        .iter()
        .map(|spec| ColumnRow {
            column: spec.name.to_string(),
            id: spec.id.as_u8(),
            // A resident open resolves the sealed keys instead of leaving spans
            // over them, so it has no answer here rather than an answer of none.
            sealed_segments: paged.then(|| index.sealed_spans(spec.id)),
        })
        .collect();

    let cues = engine.cue_points();
    let held: Vec<HeldCue> = cues
        .held()
        .into_iter()
        .map(|(at, holders)| HeldCue {
            at: at.as_u64(),
            holders,
        })
        .collect();

    emit(
        &CueOut {
            volume: engine.root().display().to_string(),
            sequence: engine.sequence().as_u64(),
            floor: cues.floor().map(|at| at.as_u64()),
            total_segments,
            dead_bytes: engine.dead_bytes().to_bytes(),
            born_segments: engine.born_segments(),
            standing_covers: index.cover_count(),
            sweep_owed: index.has_pending_covers(),
            graves: index.grave_count(),
            held,
            segments,
            columns,
        },
        output,
    )
}

/// Report the operator numbers: what each column holds, what the segments weigh
fn stat(engine: &ReelStore, paged: bool, output: OutputFormat) -> Fallible<()> {
    let index = engine.index();
    let segments = index.segments_snapshot();
    let live: u64 = segments.iter().map(|(_, bytes)| bytes.live).sum();
    let dead: u64 = segments.iter().map(|(_, bytes)| bytes.dead).sum();
    let held: u64 = segments.iter().map(|(_, bytes)| bytes.held).sum();
    let columns = engine
        .columns()
        .iter()
        .map(|spec| {
            let totals = index.column(spec.id).map(|column| column.totals());
            StatColumn {
                column: spec.name.to_string(),
                id: spec.id.as_u8(),
                // Sealed segments standing over the column, which is what a
                // search the key filters do not rule out has to consider. Only a
                // paged open leaves them standing.
                runs: paged.then(|| index.sealed_spans(spec.id)),
                // A paged index holds the tails' keys and no others, so its
                // count would be a fraction of the column presented as the whole.
                records: (!paged).then(|| totals.as_ref().map_or(0, |totals| totals.count)),
                bytes: (!paged)
                    .then(|| totals.as_ref().map_or(0, |totals| totals.bytes.to_bytes())),
            }
        })
        .collect();
    emit(
        &StatOut {
            volume: engine.root().display().to_string(),
            sequence: engine.sequence().as_u64(),
            segments: segments.len(),
            live_bytes: live,
            dead_bytes: dead,
            tombstone_bytes: held,
            dead_share: match live + dead {
                0 => 0.0,
                total => dead as f64 / total as f64,
            },
            held_cues: engine.cue_points().held().len(),
            // Sealed segments this open attributed no bytes to, which is every
            // one of them under a paged open.
            born_segments: engine.born_segments(),
            columns,
        },
        output,
    )
}

/// Sweep every record the volume holds against its checksum
///
/// A sealed segment is swept through its footer, which names where each record it
/// indexes sits; a segment with no footer is walked record by record from the
/// start until the write frontier. Nothing here writes, and nothing is repaired.
fn verify(engine: &ReelStore, limit: usize, output: OutputFormat) -> Fallible<ExitCode> {
    let roots = roots(engine);
    // Driven by what is on the disk rather than by what the index remembers: a
    // file the index never named is exactly the file a sweep must not skip.
    let files = segment_files(&roots);
    let named: Vec<SegmentId> = engine
        .index()
        .segments_snapshot()
        .into_iter()
        .map(|(segment, _)| segment)
        .collect();
    let mut rows: Vec<VerifyRow> = files
        .iter()
        .map(|(segment, path)| sweep(engine, *segment, path, named.contains(segment)))
        .collect();
    rows.sort_by_key(|row| row.segment);
    let out = VerifyOut {
        volume: engine.root().display().to_string(),
        segments_swept: rows.len(),
        records: rows.iter().map(|row| row.records).sum(),
        bytes: rows.iter().map(|row| row.bytes).sum(),
        carried_rows: rows.iter().map(|row| row.carried).sum(),
        faults: rows.iter().map(|row| row.faults).sum(),
        // Reads the engine itself failed, which an open that swept the tails may
        // have counted before this sweep read a byte.
        unreadable_records: engine.unreadable_records(),
        // An open sets a file aside when its header names another format or
        // another segment, and it stays on disk under its own name. Its bytes
        // may sweep clean and still be bytes nothing will ever read.
        not_indexed: rows
            .iter()
            .filter(|row| !row.indexed)
            .map(|row| segment_file_name(SegmentId(row.segment)))
            .collect(),
        segments: {
            // The faulted ones first, since a sweep is run to find them.
            rows.sort_by(|a, b| b.faults.cmp(&a.faults).then(a.segment.cmp(&b.segment)));
            rows.truncate(limit);
            rows
        },
    };
    let sound = out.faults == 0 && out.unreadable_records == 0 && out.not_indexed.is_empty();
    emit(&out, output)?;
    Ok(match sound {
        true => ExitCode::SUCCESS,
        false => ExitCode::FAILURE,
    })
}

/// Every segment file on the volume's roots, in segment order
fn segment_files(roots: &[PathBuf]) -> Vec<(SegmentId, PathBuf)> {
    let mut files = Vec::new();
    for root in roots {
        let Ok(entries) = std::fs::read_dir(root) else {
            continue;
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            let Some(number) = name.strip_suffix(SEGMENT_SUFFIX) else {
                continue;
            };
            if let Ok(number) = number.parse::<u32>() {
                files.push((SegmentId(number), entry.path()));
            }
        }
    }
    files.sort();
    files
}

/// The roots a segment of this volume can sit under, in the volume's own order
fn roots(engine: &ReelStore) -> Vec<PathBuf> {
    std::iter::once(engine.root().to_path_buf())
        .chain(
            engine
                .config()
                .volumes
                .iter()
                .map(|volume| volume.path.clone()),
        )
        .collect()
}

/// Sweep one segment file, through its footer where it has one
fn sweep(engine: &ReelStore, segment: SegmentId, path: &Path, indexed: bool) -> VerifyRow {
    let mut row = VerifyRow::new(segment.as_u32());
    row.indexed = indexed;
    let name = segment_file_name(segment);
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) => return row.faulted(format!("{name} will not open: {error}")),
    };
    let len = match file.metadata() {
        Ok(data) => data.len(),
        Err(error) => return row.faulted(format!("{name} will not stat: {error}")),
    };
    // A footer says where every record it indexes sits, so a sealed segment is
    // swept through it. Without one there is no boundary between the records and
    // whatever follows them, so the segment is walked instead.
    match engine.segment_footer(segment) {
        Ok(Some(footer)) => {
            row.sealed = true;
            sweep_footer(&mut file, &footer, &mut row);
        }
        Ok(None) => walk(&mut file, len, &mut row),
        Err(error) => row.fault(format!("footer does not parse: {error}")),
    }
    row
}

/// Check every record a footer indexes, in the order they sit on disk
fn sweep_footer(file: &mut File, footer: &SegmentFooter, row: &mut VerifyRow) {
    let mut at: Vec<(u32, u16, u32)> = Vec::new();
    for entry in footer.entries() {
        match entry {
            // A row whose value lives in the footer alone has no record to read.
            Ok(entry) if entry.offset == NO_RECORD => row.carried += 1,
            Ok(entry) => at.push((entry.offset, entry.key.width(), entry.len)),
            Err(error) => row.fault(format!("footer row does not decode: {error}")),
        }
    }
    // Ascending, so a sweep of a spinning disk reads the file forwards.
    at.sort_unstable();
    for (offset, width, len) in at {
        let span = HEADER_LEN as u64 + u64::from(width) + u64::from(len);
        match check(file, u64::from(offset), span) {
            Checked::Sound(bytes) => row.sound(bytes),
            Checked::Fault(why) => row.fault(why),
            // A footer named the record, so unwritten space where it pointed is
            // the pointer being wrong rather than the end of anything.
            Checked::Frontier => row.fault(format!("record at {offset} is unwritten space")),
        }
    }
}

/// Walk a segment with no footer, record by record, up to its write frontier
fn walk(file: &mut File, len: u64, row: &mut VerifyRow) {
    let mut at = 0u64;
    while at + HEADER_LEN as u64 <= len {
        match check(file, at, (HEADER_LEN + MAX_KEY_LEN) as u64) {
            Checked::Sound(bytes) => {
                row.sound(bytes);
                at += bytes;
            }
            Checked::Fault(why) => {
                row.fault(why);
                return;
            }
            Checked::Frontier => return,
        }
    }
}

/// What one record's bytes came back as
enum Checked {
    /// The record checks out, and this is what it spans on disk
    Sound(u64),

    /// The record is not sound, and this says why
    Fault(String),

    /// Unwritten space, so a walk has reached the frontier and stops
    Frontier,
}

/// Read the record at this offset and check it against its own checksum
///
/// The hint is what to read before the header has said how long the record is:
/// a footer knows exactly, and a walk asks for a header and the widest key the
/// format admits.
fn check(file: &mut File, at: u64, hint: u64) -> Checked {
    let head = match read_at(file, at, hint) {
        Ok(head) => head,
        Err(error) => return Checked::Fault(format!("read at {at} failed: {error}")),
    };
    let header = match RecordHeader::unpack(&head) {
        Ok(header) => header,
        Err(error) => return Checked::Fault(format!("header at {at} does not parse: {error}")),
    };
    if header.is_unwritten() {
        return Checked::Frontier;
    }
    // A pad's fill is never written and never checksummed, so it is stepped over
    // rather than read.
    if header.flags.is_pad() {
        return Checked::Sound(header.span());
    }
    let payload = match header.has_payload() {
        false => Vec::new(),
        true => match read_at(file, at + header.prefix_len(), u64::from(header.length)) {
            Ok(payload) => payload,
            Err(error) => return Checked::Fault(format!("payload at {at} is short: {error}")),
        },
    };
    match header.verify(&payload) {
        true => Checked::Sound(header.span()),
        false => Checked::Fault(format!("record at {at} fails its checksum")),
    }
}

/// Read up to this many bytes from an offset, short at the end of the file
fn read_at(file: &mut File, at: u64, len: u64) -> std::io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(at))?;
    let mut bytes = Vec::new();
    file.take(len).read_to_end(&mut bytes)?;
    Ok(bytes)
}

/// Report what the bias pass reads off this machine and what it would choose
fn doctor(root: &Path, output: OutputFormat) -> Fallible<()> {
    let config = ReelConfig::default();
    let facts = MachineFacts::read(root);
    let reservation = config.segment_bytes.to_bytes() * config.tail_count() as u64;
    let verdict = facts.verdict(reservation);
    let spans = access_ranges(root);
    emit(
        &DoctorOut {
            root: root.display().to_string(),
            memory_bytes: facts.memory_bytes,
            capacity_bytes: facts.volume_capacity_bytes,
            occupied_bytes: facts.volume_bytes,
            logical_block_bytes: facts.logical_block_bytes,
            is_rotational: facts.is_rotational,
            actuator_ranges: spans.len(),
            open_file_limit: facts.open_file_limit,
            ring: ring_label(facts.ring).to_string(),
            idle_reservation_bytes: reservation,
            because: verdict.because.to_string(),
            configured_plane: format!("{:?}", configured_plane(config.io_backend)),
            verdict_plane: format!("{:?}", verdict.plane),
            configured_map_above: config.map_above.map(ByteCount::to_bytes),
            verdict_map_above: verdict.map_above.map(ByteCount::to_bytes),
            map_because: verdict.map_because.to_string(),
            configured_ranged_reads: format!("{:?}", config.ranged_reads),
            verdict_ranged_reads: format!("{:?}", verdict.ranged_reads),
            configured_preallocate: format!("{:?}", config.preallocate),
            verdict_preallocate: format!("{:?}", verdict.preallocate),
            shipped_fd_cache: reel::DEFAULT_FD_CACHE,
            verdict_fd_cache: verdict.fd_cache,
        },
        output,
    )
}

/// The plane a configured backend actually opens on
fn configured_plane(backend: IoBackend) -> Plane {
    match backend.is_direct() {
        true => Plane::Direct,
        false => Plane::Buffered,
    }
}

fn ring_label(ring: RingAvailability) -> &'static str {
    match ring {
        RingAvailability::Available => "available",
        RingAvailability::Unsupported => "no io_uring in this kernel",
        RingAvailability::Denied => "refused, by policy or sandbox",
        RingAvailability::NotLinux => "not linux",
    }
}

#[derive(Serialize)]
struct SegmentRow {
    segment: u32,
    live: u64,
    dead: u64,
    held: u64,
    dead_fraction: f64,
}

#[derive(Serialize)]
struct ColumnRow {
    column: String,
    id: u8,
    sealed_segments: Option<usize>,
}

#[derive(Serialize)]
struct HeldCue {
    at: u64,
    holders: usize,
}

/// Where the volume stands and what it is holding to get there
#[derive(Serialize)]
struct CueOut {
    volume: String,
    sequence: u64,
    floor: Option<u64>,
    total_segments: usize,
    dead_bytes: u64,
    born_segments: usize,
    standing_covers: u64,
    sweep_owed: bool,
    graves: u64,
    held: Vec<HeldCue>,
    segments: Vec<SegmentRow>,
    columns: Vec<ColumnRow>,
}

impl CliOutput for CueOut {
    fn print_text(&self) {
        println!("volume            {}", self.volume);
        println!("sequence          {}", self.sequence);
        println!("segments          {}", self.total_segments);
        println!("dead bytes        {}", fmt_bytes(self.dead_bytes));
        println!("born segments     {}", self.born_segments);
        println!(
            "reaches back to   {}",
            match self.floor {
                Some(at) => at.to_string(),
                None => "nothing older than the sequence above".to_string(),
            }
        );
        if self.held.is_empty() {
            // Cue points live in the process that took them, so a tool looking
            // in from outside sees none even while a writer holds several.
            println!("held              none in this process");
        }
        for row in &self.held {
            println!("held at {} by {}", row.at, row.holders);
        }

        println!();
        println!(
            "{:<8} {:>14} {:>14} {:>12} {:>7}",
            "segment", "live", "dead", "held", "dead%"
        );
        for row in &self.segments {
            println!(
                "{:<8} {:>14} {:>14} {:>12} {:>6.1}%",
                row.segment,
                row.live,
                row.dead,
                row.held,
                row.dead_fraction * 100.0,
            );
        }
        println!(
            "showing {} of {} segments",
            self.segments.len(),
            self.total_segments
        );

        println!();
        if self.columns.is_empty() {
            println!("no columns declared, so sealed spans and standing covers count nothing");
            println!("pass --column NAME:ID for each column the volume was written with");
            return;
        }
        println!("{:<24} {:>4} {:>16}", "column", "id", "sealed segments");
        for row in &self.columns {
            println!(
                "{:<24} {:>4} {:>16}",
                row.column,
                row.id,
                answered(row.sealed_segments),
            );
        }
        if self.columns.iter().all(|row| row.sealed_segments.is_none()) {
            println!("(sealed spans stand only over a paged open: pass --paged)");
        }

        println!();
        println!("standing covers   {}", self.standing_covers);
        println!("sweep owed        {}", self.sweep_owed);
        println!("graves            {}", self.graves);
        if self.sweep_owed {
            println!("\na cover is still owed its sweep, so the counters read as a floor");
        }
        if self.born_segments > 0 {
            println!("sealed segments a rebuild left uncounted, so totals are a floor");
        }
    }
}

/// What one column holds, as far as this open can say
#[derive(Serialize)]
struct StatColumn {
    column: String,
    id: u8,
    runs: Option<usize>,
    records: Option<u64>,
    bytes: Option<u64>,
}

/// The operator numbers for a volume
#[derive(Serialize)]
struct StatOut {
    volume: String,
    sequence: u64,
    segments: usize,
    live_bytes: u64,
    dead_bytes: u64,
    tombstone_bytes: u64,
    dead_share: f64,
    held_cues: usize,
    born_segments: usize,
    columns: Vec<StatColumn>,
}

impl CliOutput for StatOut {
    fn print_text(&self) {
        println!("volume            {}", self.volume);
        println!("sequence          {}", self.sequence);
        println!("segments          {}", self.segments);
        println!("live bytes        {}", fmt_bytes(self.live_bytes));
        println!("dead bytes        {}", fmt_bytes(self.dead_bytes));
        println!("dead share        {}", pct(self.dead_share));
        println!("tombstone bytes   {}", fmt_bytes(self.tombstone_bytes));
        println!("held cue points   {}", self.held_cues);
        // Every sealed segment is born under a paged open, and its bytes are in
        // no counter, so the live and dead figures above are floors rather than
        // the volume's totals. Saying so is the difference between a floor and a
        // wrong number.
        if self.born_segments > 0 {
            println!();
            println!(
                "{} sealed segments carry no attributed bytes, so the live, dead and",
                self.born_segments
            );
            println!("share figures above are floors; a resident open attributes them all");
        }

        println!();
        if self.columns.is_empty() {
            println!("no columns declared, so the per-column numbers count nothing");
            println!("pass --column NAME:ID for each column the volume was written with");
            return;
        }
        println!(
            "{:<24} {:>4} {:>6} {:>12} {:>12}",
            "column", "id", "runs", "records", "bytes",
        );
        for row in &self.columns {
            println!(
                "{:<24} {:>4} {:>6} {:>12} {:>12}",
                row.column,
                row.id,
                answered(row.runs),
                answered(row.records),
                answered(row.bytes.map(fmt_bytes)),
            );
        }
        println!();
        match self.columns.iter().all(|row| row.runs.is_none()) {
            true => println!("runs stand only over a paged open: pass --paged"),
            false => println!("record and byte counts need a resident open: drop --paged"),
        }
    }
}

/// One segment's sweep, and the first thing wrong with it
#[derive(Serialize)]
struct VerifyRow {
    segment: u32,
    sealed: bool,
    indexed: bool,
    records: u64,
    bytes: u64,
    carried: u64,
    faults: u64,
    fault: Option<String>,
}

impl VerifyRow {
    fn new(segment: u32) -> VerifyRow {
        VerifyRow {
            segment,
            sealed: false,
            indexed: false,
            records: 0,
            bytes: 0,
            carried: 0,
            faults: 0,
            fault: None,
        }
    }

    /// One sound record of this many bytes
    fn sound(&mut self, bytes: u64) {
        self.records += 1;
        self.bytes += bytes;
    }

    /// One fault, keeping the first as the one the report names
    fn fault(&mut self, why: String) {
        self.faults += 1;
        self.fault.get_or_insert(why);
    }

    /// A segment that could not be swept at all
    fn faulted(mut self, why: String) -> VerifyRow {
        self.fault(why);
        self
    }
}

/// What a sweep found, and what it did not look at
#[derive(Serialize)]
struct VerifyOut {
    volume: String,
    segments_swept: usize,
    records: u64,
    bytes: u64,
    carried_rows: u64,
    faults: u64,
    unreadable_records: u64,
    not_indexed: Vec<String>,
    segments: Vec<VerifyRow>,
}

impl CliOutput for VerifyOut {
    fn print_text(&self) {
        println!("volume            {}", self.volume);
        println!("segments swept    {}", self.segments_swept);
        println!("records checked   {}", self.records);
        println!("bytes checked     {}", fmt_bytes(self.bytes));
        println!("carried rows      {}", self.carried_rows);
        println!("faults            {}", self.faults);
        println!("unreadable reads  {}", self.unreadable_records);
        println!("files not indexed {}", self.not_indexed.len());

        println!();
        println!(
            "{:<8} {:<8} {:>12} {:>14} {:>8}",
            "segment", "kind", "records", "bytes", "faults"
        );
        for row in &self.segments {
            println!(
                "{:<8} {:<8} {:>12} {:>14} {:>8}",
                row.segment,
                match row.sealed {
                    true => "sealed",
                    false => "walked",
                },
                row.records,
                fmt_bytes(row.bytes),
                row.faults,
            );
        }
        for row in self.segments.iter().filter(|row| row.fault.is_some()) {
            println!(
                "segment {}: {}",
                row.segment,
                row.fault.as_deref().unwrap_or_default()
            );
        }
        if !self.not_indexed.is_empty() {
            println!();
            println!(
                "{} segment files the index does not name, so nothing reads them:",
                self.not_indexed.len()
            );
            for name in self.not_indexed.iter().take(8) {
                println!("  {name}");
            }
            println!("a volume whose files are all here has an unreadable format or a lost index");
        }

        println!();
        println!("checked: every sealed segment's footer decodes, every record a footer indexes");
        println!("matches its checksum, a segment with no footer is walked record by record to");
        println!("its write frontier, and every segment file on the roots is one the index names");
        println!("not checked: versions a footer no longer indexes, and whether the segments");
        println!("agree with each other");
        println!("nothing was repaired and nothing was written");
        if self.faults == 0 && self.unreadable_records == 0 && self.not_indexed.is_empty() {
            println!();
            match self.records {
                0 => println!("clean, with nothing to check"),
                records => println!("clean, {records} records"),
            }
        }
    }
}

/// The machine's facts beside the knobs the shipped default asks for
#[derive(Serialize)]
struct DoctorOut {
    root: String,
    memory_bytes: Option<u64>,
    capacity_bytes: Option<u64>,
    occupied_bytes: Option<u64>,
    logical_block_bytes: Option<u64>,
    is_rotational: Option<bool>,
    actuator_ranges: usize,
    open_file_limit: Option<u64>,
    ring: String,
    idle_reservation_bytes: u64,
    because: String,
    configured_plane: String,
    verdict_plane: String,
    configured_map_above: Option<u64>,
    verdict_map_above: Option<u64>,
    map_because: String,
    configured_ranged_reads: String,
    verdict_ranged_reads: String,
    configured_preallocate: String,
    verdict_preallocate: String,
    shipped_fd_cache: u64,
    verdict_fd_cache: u64,
}

impl CliOutput for DoctorOut {
    fn print_text(&self) {
        println!("root              {}", self.root);
        println!("memory            {}", fmt_option(self.memory_bytes));
        println!("filesystem        {}", fmt_option(self.capacity_bytes));
        println!("occupied          {}", fmt_option(self.occupied_bytes));
        println!("logical block     {}", fmt_option(self.logical_block_bytes));
        println!(
            "rotational        {}",
            match self.is_rotational {
                Some(true) => "yes",
                Some(false) => "no",
                None => "unknown",
            }
        );
        println!(
            "actuators         {}",
            match self.actuator_ranges {
                0 => "one, or the drive does not say".to_string(),
                ranges => format!("{ranges} ranges"),
            }
        );
        println!(
            "open file limit   {}",
            match self.open_file_limit {
                Some(limit) => limit.to_string(),
                None => "unlimited".to_string(),
            }
        );
        println!("io_uring          {}", self.ring);
        println!();
        println!(
            "idle reservation under the shipped default: {}",
            fmt_bytes(self.idle_reservation_bytes),
        );
        println!("because: {}", self.because);
        println!("mapping: {}", self.map_because);
        println!();
        println!("{:<18}{:<14}{:<14}", "knob", "configured", "verdict");
        for (knob, configured, chosen) in self.rows() {
            let flag = match configured == chosen {
                true => "",
                false => "  <- disagrees",
            };
            println!("{knob:<18}{configured:<14}{chosen:<14}{flag}");
        }
    }
}

impl DoctorOut {
    /// The knobs the default asks for beside the ones this machine argues for
    fn rows(&self) -> [(&'static str, String, String); 5] {
        [
            (
                "plane",
                self.configured_plane.clone(),
                self.verdict_plane.clone(),
            ),
            (
                // A verdict names the floor a record has to clear, and a direct
                // plane names none at all, so a volume asking for a mapping
                // there disagrees.
                "map above",
                floor_label(self.configured_map_above),
                match self.configured_map_above.is_some() && self.verdict_map_above.is_none() {
                    true => "refused".to_string(),
                    false => floor_label(self.verdict_map_above),
                },
            ),
            (
                "ranged reads",
                self.configured_ranged_reads.clone(),
                self.verdict_ranged_reads.clone(),
            ),
            (
                "preallocate",
                self.configured_preallocate.clone(),
                self.verdict_preallocate.clone(),
            ),
            (
                "fd cache",
                self.shipped_fd_cache.to_string(),
                self.verdict_fd_cache.to_string(),
            ),
        ]
    }
}

/// How a floor reads in the report, where absent means the volume maps nothing
fn floor_label(floor: Option<u64>) -> String {
    match floor {
        Some(bytes) => fmt_bytes(bytes),
        None => "off".to_string(),
    }
}

fn fmt_option(bytes: Option<u64>) -> String {
    match bytes {
        Some(bytes) => fmt_bytes(bytes),
        None => "unknown".to_string(),
    }
}

/// A figure this open could answer, or a dash where it could not
///
/// A cell the open cannot fill is not a zero, and printing one would be a wrong
/// answer where there is no answer.
fn answered<T: std::fmt::Display>(value: Option<T>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "-".to_string(),
    }
}

fn pct(fraction: f64) -> String {
    format!("{:.1}%", fraction * 100.0)
}

fn fmt_bytes(bytes: u64) -> String {
    match bytes {
        bytes if bytes >= 1 << 30 => format!("{:.1} GiB", bytes as f64 / (1u64 << 30) as f64),
        bytes if bytes >= 1 << 20 => format!("{:.1} MiB", bytes as f64 / (1u64 << 20) as f64),
        bytes if bytes >= 1 << 10 => format!("{:.1} KiB", bytes as f64 / (1u64 << 10) as f64),
        bytes => format!("{bytes} B"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // a volume flag reads its path and its tags
    #[test]
    fn parses_volume_flags() {
        let plain = parse_volume("/mnt/one").expect("plain");
        assert_eq!(plain.path, PathBuf::from("/mnt/one"));
        assert_eq!(plain.class, VolumeClass::Fast);
        assert!(!plain.dead);

        let tagged = parse_volume("/mnt/two:capacity:dead").expect("tagged");
        assert_eq!(tagged.class, VolumeClass::Capacity);
        assert!(tagged.dead);

        assert!(
            parse_volume("/mnt/three:warm").is_err(),
            "an unknown tier is refused"
        );
        assert!(parse_volume("").is_err(), "an empty path is refused");
    }

    // a column flag reads its name, identifier and key width
    #[test]
    fn parses_column_flags() {
        let varying = parse_column("records:3").expect("varying");
        assert_eq!(varying.name, "records");
        assert_eq!(varying.id, ColumnId(3));
        assert_eq!(varying.key_width, KeyWidth::Variable);

        let fixed = parse_column("records:3:32").expect("fixed");
        assert_eq!(fixed.key_width, KeyWidth::Fixed(32));

        assert!(parse_column("records").is_err(), "a bare name is refused");
        assert!(
            parse_column("records:wide").is_err(),
            "a nonnumeric identifier is refused"
        );
        assert!(
            parse_column("records:3:wide").is_err(),
            "a nonnumeric width is refused"
        );
    }
}
