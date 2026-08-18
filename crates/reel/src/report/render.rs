//! Text rendering of the reports, which is every format string in one file
//!
//! Rendering writes into anything that takes formatted text, so a caller that
//! would rather not build a string can hand over a locked stdout instead.

use std::fmt::Write;

use super::checkpoint::CheckpointReport;
use super::cue::CueReport;
use super::doctor::DoctorReport;
use super::spans::SpansReport;
use super::stat::StatReport;
use super::verify::VerifyReport;

/// A report that can say itself in text
pub trait Render {
    /// Write the report out, one line per line of the text form
    fn render<Sink>(&self, out: &mut Sink) -> std::fmt::Result
    where
        Sink: Write;
}

/// Render a report to its text form
pub fn text<Report: Render>(report: &Report) -> String {
    let mut out = String::new();
    // A String is the one sink that cannot fail, so there is nothing to report.
    let _ = report.render(&mut out);
    out
}

impl Render for CueReport {
    fn render<Sink>(&self, out: &mut Sink) -> std::fmt::Result
    where
        Sink: Write,
    {
        writeln!(out, "volume            {}", self.volume)?;
        writeln!(out, "sequence          {}", self.sequence)?;
        writeln!(out, "segments          {}", self.total_segments)?;
        writeln!(out, "dead bytes        {}", fmt_bytes(self.dead_bytes))?;
        writeln!(out, "born segments     {}", self.born_segments)?;
        writeln!(
            out,
            "reaches back to   {}",
            match self.floor {
                Some(at) => at.to_string(),
                None => "nothing older than the sequence above".to_string(),
            }
        )?;
        if self.held.is_empty() {
            // Cue points live in the process that took them, so a tool looking
            // in from outside sees none even while a writer holds several.
            writeln!(out, "held              none in this process")?;
        }
        for row in &self.held {
            writeln!(out, "held at {} by {}", row.at, row.holders)?;
        }

        writeln!(out)?;
        writeln!(
            out,
            "{:<8} {:>14} {:>14} {:>12} {:>7}",
            "segment", "live", "dead", "held", "dead%"
        )?;
        for row in &self.segments {
            writeln!(
                out,
                "{:<8} {:>14} {:>14} {:>12} {:>6.1}%",
                row.segment,
                row.live,
                row.dead,
                row.held,
                row.dead_fraction * 100.0,
            )?;
        }
        writeln!(
            out,
            "showing {} of {} segments",
            self.segments.len(),
            self.total_segments
        )?;

        writeln!(out)?;
        if self.columns.is_empty() {
            writeln!(
                out,
                "no columns declared, so sealed spans and standing covers count nothing"
            )?;
            writeln!(
                out,
                "pass --column NAME:ID for each column the volume was written with"
            )?;
            return Ok(());
        }
        writeln!(
            out,
            "{:<24} {:>4} {:>16}",
            "column", "id", "sealed segments"
        )?;
        for row in &self.columns {
            writeln!(
                out,
                "{:<24} {:>4} {:>16}",
                row.column,
                row.id,
                answered(row.sealed_segments),
            )?;
        }
        if self.columns.iter().all(|row| row.sealed_segments.is_none()) {
            writeln!(
                out,
                "(sealed spans stand only over a paged open: pass --paged)"
            )?;
        }

        writeln!(out)?;
        writeln!(out, "standing covers   {}", self.standing_covers)?;
        writeln!(out, "sweep owed        {}", self.sweep_owed)?;
        writeln!(out, "graves            {}", self.graves)?;
        if self.sweep_owed {
            writeln!(
                out,
                "\na cover is still owed its sweep, so the counters read as a floor"
            )?;
        }
        if self.born_segments > 0 {
            writeln!(
                out,
                "sealed segments a rebuild left uncounted, so totals are a floor"
            )?;
        }
        Ok(())
    }
}

impl Render for StatReport {
    fn render<Sink>(&self, out: &mut Sink) -> std::fmt::Result
    where
        Sink: Write,
    {
        writeln!(out, "volume            {}", self.volume)?;
        writeln!(out, "sequence          {}", self.sequence)?;
        writeln!(out, "segments          {}", self.segments)?;
        writeln!(out, "live bytes        {}", fmt_bytes(self.live_bytes))?;
        writeln!(out, "dead bytes        {}", fmt_bytes(self.dead_bytes))?;
        writeln!(out, "dead share        {}", pct(self.dead_share))?;
        writeln!(out, "tombstone bytes   {}", fmt_bytes(self.tombstone_bytes))?;
        writeln!(out, "held cue points   {}", self.held_cues)?;
        // Every sealed segment is born under a paged open, and its bytes are in
        // no counter, so the live and dead figures above are floors rather than
        // the volume's totals. Saying so is the difference between a floor and a
        // wrong number.
        if self.born_segments > 0 {
            writeln!(out)?;
            writeln!(
                out,
                "{} sealed segments carry no attributed bytes, so the live, dead and",
                self.born_segments
            )?;
            writeln!(
                out,
                "share figures above are floors; a resident open attributes them all"
            )?;
        }

        writeln!(out)?;
        if self.columns.is_empty() {
            writeln!(
                out,
                "no columns declared, so the per-column numbers count nothing"
            )?;
            writeln!(
                out,
                "pass --column NAME:ID for each column the volume was written with"
            )?;
            return Ok(());
        }
        writeln!(
            out,
            "{:<24} {:>4} {:>6} {:>12} {:>12}",
            "column", "id", "runs", "records", "bytes",
        )?;
        for row in &self.columns {
            writeln!(
                out,
                "{:<24} {:>4} {:>6} {:>12} {:>12}",
                row.column,
                row.id,
                answered(row.runs),
                answered(row.records),
                answered(row.bytes.map(fmt_bytes)),
            )?;
        }
        writeln!(out)?;
        match self.columns.iter().all(|row| row.runs.is_none()) {
            true => writeln!(out, "runs stand only over a paged open: pass --paged")?,
            false => writeln!(
                out,
                "record and byte counts need a resident open: drop --paged"
            )?,
        }
        Ok(())
    }
}

impl Render for SpansReport {
    fn render<Sink>(&self, out: &mut Sink) -> std::fmt::Result
    where
        Sink: Write,
    {
        writeln!(out, "volume            {}", self.volume)?;
        writeln!(out)?;
        if self.columns.is_empty() {
            writeln!(out, "no columns declared, so no spans are counted")?;
            writeln!(
                out,
                "pass --column NAME:ID for each column the volume was written with"
            )?;
            return Ok(());
        }
        writeln!(out, "{:<24} {:>16}", "column", "sealed segments")?;
        for row in &self.columns {
            writeln!(out, "{:<24} {:>16}", row.column, row.sealed_segments)?;
        }
        if !self.is_paged {
            writeln!(
                out,
                "(sealed spans stand only over a paged open: pass --paged)"
            )?;
        }
        Ok(())
    }
}

impl Render for VerifyReport {
    fn render<Sink>(&self, out: &mut Sink) -> std::fmt::Result
    where
        Sink: Write,
    {
        writeln!(out, "volume            {}", self.volume)?;
        writeln!(out, "segments swept    {}", self.segments_swept)?;
        writeln!(out, "records checked   {}", self.records)?;
        writeln!(out, "bytes checked     {}", fmt_bytes(self.bytes))?;
        writeln!(out, "carried rows      {}", self.carried_rows)?;
        writeln!(out, "faults            {}", self.faults)?;
        writeln!(out, "unreadable reads  {}", self.unreadable_records)?;
        writeln!(out, "files not indexed {}", self.not_indexed.len())?;

        writeln!(out)?;
        writeln!(
            out,
            "{:<8} {:<8} {:>12} {:>14} {:>8}",
            "segment", "kind", "records", "bytes", "faults"
        )?;
        for row in &self.segments {
            writeln!(
                out,
                "{:<8} {:<8} {:>12} {:>14} {:>8}",
                row.segment,
                match row.sealed {
                    true => "sealed",
                    false => "walked",
                },
                row.records,
                fmt_bytes(row.bytes),
                row.faults,
            )?;
        }
        for row in self.segments.iter().filter(|row| row.fault.is_some()) {
            writeln!(
                out,
                "segment {}: {}",
                row.segment,
                row.fault.as_deref().unwrap_or_default()
            )?;
        }
        if !self.not_indexed.is_empty() {
            writeln!(out)?;
            writeln!(
                out,
                "{} segment files the index does not name, so nothing reads them:",
                self.not_indexed.len()
            )?;
            for name in self.not_indexed.iter().take(8) {
                writeln!(out, "  {name}")?;
            }
            writeln!(
                out,
                "a volume whose files are all here has an unreadable format or a lost index"
            )?;
        }

        writeln!(out)?;
        writeln!(
            out,
            "checked: every sealed segment's footer decodes, every record a footer indexes"
        )?;
        writeln!(
            out,
            "matches its checksum, a segment with no footer is walked record by record to"
        )?;
        writeln!(
            out,
            "its write frontier, and every segment file on the roots is one the index names"
        )?;
        writeln!(
            out,
            "not checked: versions a footer no longer indexes, and whether the segments"
        )?;
        writeln!(out, "agree with each other")?;
        writeln!(out, "nothing was repaired and nothing was written")?;
        if self.is_sound() {
            writeln!(out)?;
            match self.records {
                0 => writeln!(out, "clean, with nothing to check")?,
                records => writeln!(out, "clean, {records} records")?,
            }
        }
        Ok(())
    }
}

impl Render for DoctorReport {
    fn render<Sink>(&self, out: &mut Sink) -> std::fmt::Result
    where
        Sink: Write,
    {
        writeln!(out, "root              {}", self.root)?;
        writeln!(out, "memory            {}", fmt_option(self.memory_bytes))?;
        writeln!(out, "filesystem        {}", fmt_option(self.capacity_bytes))?;
        writeln!(out, "occupied          {}", fmt_option(self.occupied_bytes))?;
        writeln!(
            out,
            "logical block     {}",
            fmt_option(self.logical_block_bytes)
        )?;
        writeln!(
            out,
            "rotational        {}",
            match self.is_rotational {
                Some(true) => "yes",
                Some(false) => "no",
                None => "unknown",
            }
        )?;
        writeln!(
            out,
            "actuators         {}",
            match self.actuator_ranges {
                0 => "one, or the drive does not say".to_string(),
                ranges => format!("{ranges} ranges"),
            }
        )?;
        writeln!(
            out,
            "open file limit   {}",
            match self.open_file_limit {
                Some(limit) => limit.to_string(),
                None => "unlimited".to_string(),
            }
        )?;
        writeln!(out, "io_uring          {}", self.ring)?;
        writeln!(out)?;
        writeln!(
            out,
            "idle reservation under the shipped default: {}",
            fmt_bytes(self.idle_reservation_bytes),
        )?;
        writeln!(out, "because: {}", self.because)?;
        writeln!(out, "mapping: {}", self.map_because)?;
        writeln!(out)?;
        writeln!(out, "{:<18}{:<14}{:<14}", "knob", "configured", "verdict")?;
        for (knob, configured, chosen) in knobs(self) {
            let flag = match configured == chosen {
                true => "",
                false => "  <- disagrees",
            };
            writeln!(out, "{knob:<18}{configured:<14}{chosen:<14}{flag}")?;
        }
        Ok(())
    }
}

impl Render for CheckpointReport {
    fn render<Sink>(&self, out: &mut Sink) -> std::fmt::Result
    where
        Sink: Write,
    {
        writeln!(out, "checkpoint        {}", self.target)?;
        writeln!(out, "sequence          {}", self.at)?;
        writeln!(out, "segments linked   {}", self.segments)?;
        writeln!(out)?;
        writeln!(
            out,
            "Open it as a volume to restore: there is no restore procedure."
        )?;
        Ok(())
    }
}

/// The knobs the configuration asks for beside the ones this machine argues for
fn knobs(report: &DoctorReport) -> [(&'static str, String, String); 5] {
    [
        (
            "plane",
            report.configured_plane.clone(),
            report.verdict_plane.clone(),
        ),
        (
            // A verdict names the floor a record has to clear, and a direct
            // plane names none at all, so a volume asking for a mapping there
            // disagrees.
            "map above",
            floor_label(report.configured_map_above),
            match report.configured_map_above.is_some() && report.verdict_map_above.is_none() {
                true => "refused".to_string(),
                false => floor_label(report.verdict_map_above),
            },
        ),
        (
            "ranged reads",
            report.configured_ranged_reads.clone(),
            report.verdict_ranged_reads.clone(),
        ),
        (
            "preallocate",
            report.configured_preallocate.clone(),
            report.verdict_preallocate.clone(),
        ),
        (
            "fd cache",
            report.shipped_fd_cache.to_string(),
            report.verdict_fd_cache.to_string(),
        ),
    ]
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
fn answered<Figure>(value: Option<Figure>) -> String
where
    Figure: std::fmt::Display,
{
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
