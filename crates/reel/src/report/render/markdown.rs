//! A report as markdown, for a pull request, an issue, or a CI summary
//!
//! The same blocks the terminal renderer draws, said in the markup a review
//! surface renders: the head as a heading, tables as tables, and every caveat as
//! a bullet that survives the paste.

use std::fmt::Write;

use crate::report::doc::{Align, Block, Doc, Notes, Table, Tone};

/// Render a whole report as markdown
pub fn render<Sink>(doc: &Doc, out: &mut Sink) -> std::fmt::Result
where
    Sink: Write,
{
    if !doc.head.is_empty() {
        writeln!(out, "## {}", doc.head.join(" · "))?;
        writeln!(out)?;
    }
    if let Some(verdict) = &doc.verdict {
        let mark = match verdict.tone {
            Tone::Bad => "❌ ",
            Tone::Warn => "⚠️ ",
            _ => "",
        };
        match verdict.detail.is_empty() {
            true => writeln!(out, "{mark}**{}**", verdict.label)?,
            false => writeln!(out, "{mark}**{}** — {}", verdict.label, verdict.detail)?,
        }
        writeln!(out)?;
    }

    for (at, block) in doc.blocks.iter().enumerate() {
        if at > 0 {
            writeln!(out)?;
        }
        match block {
            Block::Lines(lines) => {
                for line in lines {
                    writeln!(out, "{line}  ")?;
                }
            }
            Block::Facts(facts) => {
                for (label, value) in facts {
                    writeln!(out, "- **{}** — {}", escape(label), escape(value))?;
                }
            }
            Block::Table(table) => table_block(table, out)?,
            Block::Notes(notes) => notes_block(notes, out)?,
            Block::Terms(terms) => {
                for (at, (term, items)) in terms.iter().enumerate() {
                    if at > 0 {
                        writeln!(out, ">")?;
                    }
                    writeln!(
                        out,
                        "> **{}** — {}",
                        escape(&capitalised(term)),
                        escape(&items.join("; ")),
                    )?;
                }
            }
            Block::Footer(commands) => {
                let commands: Vec<String> = commands.iter().map(|command| code(command)).collect();
                writeln!(out, "{}", commands.join(" · "))?;
            }
        }
    }
    Ok(())
}

fn table_block<Sink>(table: &Table, out: &mut Sink) -> std::fmt::Result
where
    Sink: Write,
{
    // A note hangs off a row's end in the terminal; here it earns a column, so
    // that a row carrying one stays a row rather than becoming loose prose.
    let noted = table.rows.iter().any(|row| row.note.is_some());
    let mut heads: Vec<String> = table.columns.iter().map(|c| escape(&c.head)).collect();
    let mut rules: Vec<&str> = table
        .columns
        .iter()
        .map(|c| match c.align {
            Align::Left => "---",
            Align::Right => "---:",
        })
        .collect();
    if noted {
        heads.push(String::new());
        rules.push("---");
    }
    writeln!(out, "| {} |", heads.join(" | "))?;
    writeln!(out, "| {} |", rules.join(" | "))?;

    for row in &table.rows {
        let mut cells: Vec<String> = Vec::new();
        for at in 0..table.columns.len() {
            let cell = row.cells.get(at).map(String::as_str).unwrap_or("");
            // The first cell names the row, so weighting that one is enough to
            // pick the row out; weighting all of them shouts the figures too.
            cells.push(match row.tone {
                Tone::Bad | Tone::Warn if at == 0 && !cell.trim().is_empty() => {
                    format!("**{}**", escape(cell))
                }
                _ => escape(cell),
            });
        }
        if noted {
            cells.push(match &row.note {
                Some(note) => escape(note),
                None => String::new(),
            });
        }
        writeln!(out, "| {} |", cells.join(" | "))?;
    }

    if let Some(caption) = &table.caption {
        writeln!(out)?;
        writeln!(out, "*{}*", escape(caption))?;
    }
    Ok(())
}

fn notes_block<Sink>(notes: &Notes, out: &mut Sink) -> std::fmt::Result
where
    Sink: Write,
{
    writeln!(out, "**{}**", escape(&capitalised(&notes.label)))?;
    writeln!(out)?;
    for note in &notes.items {
        writeln!(out, "- {}", escape(&note.what))?;
        if let Some(fix) = &note.fix {
            writeln!(out, "  - {}", code(fix))?;
        }
    }
    Ok(())
}

/// Text inside a code span, where nothing is escaped because nothing is read
///
/// A backslash is literal between backticks, so escaping a path or a flag on the
/// way in leaves the reader looking at the backslash. Only a backtick in the
/// content matters, and the fence grows past the longest run of them.
fn code(text: &str) -> String {
    let longest = text
        .split(|char| char != '`')
        .map(str::len)
        .max()
        .unwrap_or(0);
    let fence = "`".repeat(longest + 1);
    // A span whose content opens or closes with a backtick needs a space inside
    // the fence, which markdown strips back off.
    match text.starts_with('`') || text.ends_with('`') {
        true => format!("{fence} {text} {fence}"),
        false => format!("{fence}{text}{fence}"),
    }
}

/// A label as a heading in prose reads it, where a terminal would shout it
fn capitalised(label: &str) -> String {
    let mut chars = label.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

/// Blunt the characters that would otherwise close a cell or open a style
///
/// A path or a flag reaches here verbatim, and a pipe inside one would end its
/// cell three columns early.
fn escape(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace('*', "\\*")
        .replace('_', "\\_")
}
