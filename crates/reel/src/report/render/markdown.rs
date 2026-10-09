//! Markdown renderer for pull requests, issues and CI summaries

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
    // Notes get their own column so a noted row stays a table row
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
            // Only the first cell of a toned row is bolded, which is enough to mark the row
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

/// Wrap text unescaped in a code span, with a fence longer than any backtick run inside
fn code(text: &str) -> String {
    let longest = text
        .split(|char| char != '`')
        .map(str::len)
        .max()
        .unwrap_or(0);
    let fence = "`".repeat(longest + 1);
    // Content that starts or ends with a backtick needs a space inside the fence
    match text.starts_with('`') || text.ends_with('`') {
        true => format!("{fence} {text} {fence}"),
        false => format!("{fence}{text}{fence}"),
    }
}

/// A label with its first letter capitalised, for a markdown heading
fn capitalised(label: &str) -> String {
    let mut chars = label.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

/// Escape the characters that would close a cell or open a style
fn escape(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace('*', "\\*")
        .replace('_', "\\_")
}
