//! A report as a terminal reads it
//!
//! Head and verdict first, inside a frame where the frontend says there is a
//! terminal to draw one on, then the blocks in the order the report said them.
//! Column widths are measured off the content, so nothing here declares a layout
//! and a wider name widens its column rather than colliding with the next.

use std::fmt::Write;

use super::style::{pad, paint, width, Paint, Style};
use crate::report::doc::{Align, Block, Doc, Note, Notes, Table, Tone, Verdict};

/// Text a line is built from, kept beside the width it actually occupies
///
/// An escape sequence has no width, so a painted string cannot be measured for
/// padding. Building both forms at once is what keeps a coloured table's columns
/// standing where an uncoloured one's do.
#[derive(Default)]
struct Painted {
    plain: usize,
    rich: String,
}

impl Painted {
    /// Append text, painted in a role or left as it is
    fn push(&mut self, style: &Style, role: Option<Paint>, text: &str) {
        self.plain += width(text);
        match role {
            Some(role) => self.rich.push_str(&paint(style, role, text)),
            None => self.rich.push_str(text),
        }
    }

    /// Append unpainted spaces, which pad a line without colouring it
    fn space(&mut self, count: usize) {
        self.plain += count;
        for _ in 0..count {
            self.rich.push(' ');
        }
    }
}

/// The role a tone paints in
fn role(tone: Tone) -> Option<Paint> {
    match tone {
        Tone::Plain => None,
        Tone::Good => Some(Paint::Good),
        Tone::Warn => Some(Paint::Warn),
        Tone::Bad => Some(Paint::Bad),
    }
}

/// Render a whole report
pub fn render<Sink>(doc: &Doc, style: &Style, out: &mut Sink) -> std::fmt::Result
where
    Sink: Write,
{
    let banner = banner(doc, style);
    for line in &banner {
        writeln!(out, "{line}")?;
    }

    // A blank line between anything already written and whatever comes next,
    // which is the whole of how the blocks are held apart.
    let mut written = !banner.is_empty();
    for block in &doc.blocks {
        if written {
            writeln!(out)?;
        }
        written = true;
        match block {
            Block::Lines(lines) => lines_block(lines, style, out)?,
            Block::Facts(facts) => facts_block(facts, style, out)?,
            Block::Table(table) => table_block(table, style, out)?,
            Block::Notes(notes) => notes_block(notes, style, out)?,
            Block::Terms(terms) => terms_block(terms, style, out)?,
            Block::Footer(commands) => footer_block(commands, style, out)?,
        }
    }
    Ok(())
}

/// The head and the verdict, framed where the style allows it
fn banner(doc: &Doc, style: &Style) -> Vec<String> {
    let mut head: Vec<Painted> = Vec::new();
    if !doc.head.is_empty() {
        for line in wrap(&doc.head.join(" · "), style.frame_width()) {
            let mut row = Painted::default();
            row.push(style, Some(Paint::Dim), &line);
            head.push(row);
        }
    }
    let verdict: Vec<Painted> = doc
        .verdict
        .iter()
        .flat_map(|verdict| verdict_rows(verdict, style))
        .collect();
    match style.frames {
        true => frame(head, verdict),
        false => loose(head, verdict),
    }
}

/// The verdict as its own rows, the word carried heavier than the figures
fn verdict_rows(verdict: &Verdict, style: &Style) -> Vec<Painted> {
    let mut word = Painted::default();
    word.push(style, Some(Paint::Strong), &verdict.label);
    if let Some(role) = role(verdict.tone) {
        // The word is what carries, so it takes the colour and the weight
        // together where a terminal can show both.
        word.rich = paint(style, role, &word.rich);
    }
    // A verdict that is only a word is only a word: nothing is written after it
    // to hold a detail that was never given.
    if verdict.detail.is_empty() {
        return vec![word];
    }

    let indent = width(&verdict.label) + 1;
    let room = style.frame_width().saturating_sub(indent).max(20);
    let mut rows = Vec::new();
    for (at, line) in wrap(&format!("— {}", verdict.detail), room)
        .into_iter()
        .enumerate()
    {
        let mut row = match at {
            0 => std::mem::take(&mut word),
            _ => Painted::default(),
        };
        row.space(match at {
            0 => 1,
            _ => indent,
        });
        row.push(style, None, &line);
        rows.push(row);
    }
    rows
}

/// Head and verdict with a blank line between them, for a pipe or a file
fn loose(head: Vec<Painted>, verdict: Vec<Painted>) -> Vec<String> {
    let mut lines: Vec<String> = head.into_iter().map(|row| row.rich).collect();
    if !lines.is_empty() && !verdict.is_empty() {
        // A blank line between the two is the whole of the separation here,
        // since there is no frame to sit them in.
        lines.push(String::new());
    }
    lines.extend(verdict.into_iter().map(|row| row.rich));
    lines
}

/// Head and verdict inside a drawn frame, ruled apart from each other
fn frame(head: Vec<Painted>, verdict: Vec<Painted>) -> Vec<String> {
    if head.is_empty() && verdict.is_empty() {
        return Vec::new();
    }
    let inner = head
        .iter()
        .chain(verdict.iter())
        .map(|row| row.plain)
        .max()
        .unwrap_or(0);
    let rule = |left: &str, right: &str| format!("{left}{}{right}", "─".repeat(inner + 2));
    let sit = |row: Painted| {
        format!(
            "│ {}{} │",
            row.rich,
            " ".repeat(inner.saturating_sub(row.plain))
        )
    };

    let mut lines = vec![rule("╭", "╮")];
    let ruled = !head.is_empty() && !verdict.is_empty();
    lines.extend(head.into_iter().map(sit));
    if ruled {
        lines.push(rule("├", "┤"));
    }
    lines.extend(verdict.into_iter().map(sit));
    lines.push(rule("╰", "╯"));
    lines
}

fn lines_block<Sink>(lines: &[String], style: &Style, out: &mut Sink) -> std::fmt::Result
where
    Sink: Write,
{
    for line in lines {
        for wrapped in wrap(line, style.width) {
            writeln!(out, "{wrapped}")?;
        }
    }
    Ok(())
}

/// Labels down the left, values lined up past the longest of them
fn facts_block<Sink>(facts: &[(String, String)], style: &Style, out: &mut Sink) -> std::fmt::Result
where
    Sink: Write,
{
    let label_width = facts
        .iter()
        .map(|(label, _)| width(label))
        .max()
        .unwrap_or(0);
    for (label, value) in facts {
        let mut row = Painted::default();
        row.push(style, Some(Paint::Dim), &pad(label, label_width, true));
        row.space(2);
        row.push(style, None, value);
        writeln!(out, "{}", row.rich)?;
    }
    Ok(())
}

/// Two spaces of rail down the left of every table, so a table reads as a block
const RAIL: usize = 2;

/// Two spaces between columns, which is enough to separate and no more
const GAP: usize = 2;

fn table_block<Sink>(table: &Table, style: &Style, out: &mut Sink) -> std::fmt::Result
where
    Sink: Write,
{
    let widths = widths(table);

    // Nothing is padded out past the last thing on its line: trailing spaces are
    // invisible to a reader and noise to everything else that reads output. A
    // column of figures is exempt, since its padding leads rather than trails and
    // dropping it would leave the figures unaligned. Whether anything follows the
    // last cell is asked of each line rather than of the table, since a note on
    // one row is no reason to pad out the rows that carry none.
    let ragged = matches!(table.columns.last(), Some(column) if column.align == Align::Left);
    let last = table.columns.len().saturating_sub(1);

    let mut head = Painted::default();
    head.space(RAIL);
    for (at, column) in table.columns.iter().enumerate() {
        if at > 0 {
            head.space(GAP);
        }
        let cell = match ragged && at == last {
            true => column.head.clone(),
            false => pad(&column.head, widths[at], column.align == Align::Left),
        };
        head.push(style, Some(Paint::Dim), &cell);
    }
    writeln!(out, "{}", head.rich)?;

    for row in &table.rows {
        let mut line = Painted::default();
        line.space(RAIL);
        for (at, column) in table.columns.iter().enumerate() {
            if at > 0 {
                line.space(GAP);
            }
            let empty = String::new();
            let cell = row.cells.get(at).unwrap_or(&empty);
            let cell = match ragged && at == last && row.note.is_none() {
                true => cell.clone(),
                false => pad(cell, widths[at], column.align == Align::Left),
            };
            line.push(style, role(row.tone), &cell);
        }
        if let Some(note) = &row.note {
            line.space(GAP);
            let tone = match row.tone {
                // A note on a plain row is an aside; on a toned row it is the
                // reason the row is toned, so it keeps the row's colour.
                Tone::Plain => Some(Paint::Dim),
                tone => role(tone),
            };
            line.push(style, tone, &format!("← {note}"));
        }
        writeln!(out, "{}", line.rich)?;
    }

    if let Some(caption) = &table.caption {
        let mut line = Painted::default();
        line.space(RAIL);
        line.push(style, Some(Paint::Dim), caption);
        writeln!(out, "{}", line.rich)?;
    }
    Ok(())
}

/// Each column as wide as the widest of its heading and its cells
fn widths(table: &Table) -> Vec<usize> {
    let mut widths: Vec<usize> = table.columns.iter().map(|c| width(&c.head)).collect();
    for row in &table.rows {
        for (at, cell) in row.cells.iter().enumerate() {
            if let Some(slot) = widths.get_mut(at) {
                *slot = (*slot).max(width(cell));
            }
        }
    }
    widths
}

/// The bullet a finding is marked with, and the arrow its answer is marked with
const BULLET: &str = "●";
const ARROW: &str = "→";

fn notes_block<Sink>(notes: &Notes, style: &Style, out: &mut Sink) -> std::fmt::Result
where
    Sink: Write,
{
    let mut heading = Painted::default();
    heading.push(style, Some(Paint::Strong), &notes.label.to_uppercase());
    if let Some(role) = role(notes.tone) {
        heading.rich = paint(style, role, &heading.rich);
    }
    writeln!(out, "{}", heading.rich)?;
    for note in &notes.items {
        note_item(note, notes.tone, style, out)?;
    }
    Ok(())
}

fn note_item<Sink>(note: &Note, tone: Tone, style: &Style, out: &mut Sink) -> std::fmt::Result
where
    Sink: Write,
{
    for (at, line) in wrap(&note.what, style.width.saturating_sub(2))
        .into_iter()
        .enumerate()
    {
        let mut row = Painted::default();
        match at {
            0 => {
                row.push(style, role(tone), BULLET);
                row.space(1);
            }
            _ => row.space(2),
        }
        row.push(style, None, &line);
        writeln!(out, "{}", row.rich)?;
    }
    let Some(fix) = &note.fix else {
        return Ok(());
    };
    for (at, line) in wrap(fix, style.width.saturating_sub(4))
        .into_iter()
        .enumerate()
    {
        let mut row = Painted::default();
        row.space(2);
        match at {
            0 => {
                row.push(style, Some(Paint::Dim), ARROW);
                row.space(1);
            }
            _ => row.space(2),
        }
        row.push(style, Some(Paint::Dim), &line);
        writeln!(out, "{}", row.rich)?;
    }
    Ok(())
}

fn terms_block<Sink>(
    terms: &[(String, Vec<String>)],
    style: &Style,
    out: &mut Sink,
) -> std::fmt::Result
where
    Sink: Write,
{
    let label_width = terms.iter().map(|(term, _)| width(term)).max().unwrap_or(0);
    let room = style.width.saturating_sub(label_width + 2).max(20);
    for (term, items) in terms {
        for (at, line) in wrap(&items.join(" · "), room).into_iter().enumerate() {
            let mut row = Painted::default();
            match at {
                0 => row.push(
                    style,
                    Some(Paint::Dim),
                    &pad(&term.to_uppercase(), label_width, true),
                ),
                _ => row.space(label_width),
            }
            row.space(2);
            row.push(style, None, &line);
            writeln!(out, "{}", row.rich)?;
        }
    }
    Ok(())
}

/// What sits between two commands in a footer
const BETWEEN: &str = "   ·   ";

fn footer_block<Sink>(commands: &[String], style: &Style, out: &mut Sink) -> std::fmt::Result
where
    Sink: Write,
{
    // Packed whole rather than wrapped: half a command on each of two lines is
    // not a command anybody can copy.
    let mut line = String::new();
    for command in commands {
        let would = match line.is_empty() {
            true => width(command),
            false => width(&line) + width(BETWEEN) + width(command),
        };
        if would > style.width && !line.is_empty() {
            let mut row = Painted::default();
            row.push(style, Some(Paint::Dim), &line);
            writeln!(out, "{}", row.rich)?;
            line.clear();
        }
        if !line.is_empty() {
            line.push_str(BETWEEN);
        }
        line.push_str(command);
    }
    let mut row = Painted::default();
    row.push(style, Some(Paint::Dim), &line);
    writeln!(out, "{}", row.rich)?;
    Ok(())
}

/// Break text into lines no wider than a limit, at the spaces between words
///
/// A word longer than the limit is left whole and overhangs, since breaking a
/// path or a flag in the middle costs the reader more than the overhang does.
fn wrap(text: &str, limit: usize) -> Vec<String> {
    if text.is_empty() {
        return vec![String::new()];
    }
    let mut lines: Vec<String> = Vec::new();
    let mut line = String::new();
    for word in text.split(' ') {
        let would = match line.is_empty() {
            true => width(word),
            false => width(&line) + 1 + width(word),
        };
        match would <= limit || line.is_empty() {
            true => {
                if !line.is_empty() {
                    line.push(' ');
                }
                line.push_str(word);
            }
            false => {
                lines.push(std::mem::take(&mut line));
                line.push_str(word);
            }
        }
    }
    lines.push(line);
    lines
}
