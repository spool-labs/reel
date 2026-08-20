//! Turning a report into the text somebody reads
//!
//! A report says itself once, as a `Doc`, and the renderers here are the only
//! places that decide what that looks like. Adding a format is a module beside
//! these two rather than another set of format strings per report, and a report
//! that grows a block gets it in every format at once.

use std::fmt::Write;

pub mod markdown;
pub mod style;
pub mod text;

pub use style::Style;

use super::doc::Doc;

/// A report that can say its own shape
pub trait Report {
    /// The report as blocks, before any format has been chosen
    fn doc(&self) -> Doc;
}

/// Render a report for a terminal
pub fn text<Model: Report>(report: &Model, style: &Style) -> String {
    let mut out = String::new();
    // A String is the one sink that cannot fail, so there is nothing to report.
    let _ = text::render(&report.doc(), style, &mut out);
    out
}

/// Render a report as markdown
pub fn markdown<Model: Report>(report: &Model) -> String {
    let mut out = String::new();
    let _ = markdown::render(&report.doc(), &mut out);
    out
}

/// Write a report straight into a sink, for a caller that would rather not build
/// a string
pub fn write<Model, Sink>(report: &Model, style: &Style, out: &mut Sink) -> std::fmt::Result
where
    Model: Report,
    Sink: Write,
{
    text::render(&report.doc(), style, out)
}

#[cfg(test)]
mod tests {
    use super::style::Style;
    use super::*;
    use crate::report::doc::{Column, Note, Row, Table, Tone};

    /// A report with one of everything, to render both ways
    fn sample() -> Doc {
        Doc::new()
            .head("vol")
            .head("verify")
            .verdict(Tone::Bad, "FAULTED", "2 faults across 1 segment")
            .facts([("records checked", "88"), ("bytes", "5.0 MiB")])
            .table(
                Table::new([Column::left("segment"), Column::right("bytes")])
                    .row(Row::new(["1", "1.9 MiB"]).toned(Tone::Bad).note("faulted"))
                    .row(Row::new(["10", "1.2 MiB"]))
                    .caption("2 of 9 segments"),
            )
            .table(
                // A left-hand last column with a note on one row and not the
                // other, which is the shape a knob table takes and the one the
                // trailing-space exemption has to get right.
                Table::new([Column::left("knob"), Column::left("verdict")])
                    .row(Row::new(["plane", "off"]))
                    .row(Row::new(["map above", "refused"]).note("disagrees")),
            )
            .notes(
                "not counted",
                Tone::Warn,
                vec![Note::new("no columns declared").fix("--column NAME:ID")],
            )
            .term("checked", ["footers decode", "checksums match"])
            .footer(["machine-readable: -o json"])
    }

    struct Rendered(Doc);

    impl Report for Rendered {
        fn doc(&self) -> Doc {
            self.0.clone()
        }
    }

    // a plain render writes nothing a terminal would have to interpret
    #[test]
    fn plain_is_plain() {
        let out = text(&Rendered(sample()), &Style::PLAIN);
        assert!(
            !out.contains('\x1b'),
            "escape sequences in a plain render: {out}"
        );
        assert!(!out.contains('╭'), "a frame in a plain render: {out}");
        assert!(out.contains("FAULTED"), "no verdict: {out}");
    }

    // no line is padded out past the last thing on it
    #[test]
    fn nothing_trails() {
        for style in [Style::PLAIN, Style::rich(80)] {
            let out = text(&Rendered(sample()), &style);
            for line in out.lines() {
                assert_eq!(line, line.trim_end(), "trailing space on {line:?}");
            }
        }
    }

    // a frame closes on every line, whatever the widest of them is
    #[test]
    fn a_frame_is_square() {
        let out = text(&Rendered(sample()), &Style::rich(80));
        let framed: Vec<&str> = out
            .lines()
            .filter(|line| {
                line.starts_with('╭')
                    || line.starts_with('│')
                    || line.starts_with('╰')
                    || line.starts_with('├')
            })
            .collect();
        assert!(framed.len() >= 4, "no frame drawn: {out}");
        let plain = |line: &str| {
            let mut out = String::new();
            let mut chars = line.chars();
            while let Some(char) = chars.next() {
                match char {
                    '\x1b' => {
                        for char in chars.by_ref() {
                            if char == 'm' {
                                break;
                            }
                        }
                    }
                    char => out.push(char),
                }
            }
            out
        };
        let widths: Vec<usize> = framed
            .iter()
            .map(|line| plain(line).chars().count())
            .collect();
        assert!(
            widths.windows(2).all(|pair| pair[0] == pair[1]),
            "the frame is ragged: {widths:?} in {out}",
        );
    }

    // a table's columns are measured from what is in them
    #[test]
    fn columns_fit_their_content() {
        let wide = Doc::new().table(
            Table::new([Column::left("id"), Column::right("n")])
                .row(Row::new(["a-very-long-identifier", "1"]))
                .row(Row::new(["b", "1000000"])),
        );
        let out = text(&Rendered(wide), &Style::PLAIN);
        let rows: Vec<&str> = out.lines().filter(|line| !line.is_empty()).collect();
        assert!(
            rows[1].contains("a-very-long-identifier"),
            "the long cell was clipped: {out}"
        );
        // A column of figures ends where every other row's does, whatever the
        // widths of the names beside them.
        let ends: Vec<usize> = rows.iter().map(|line| line.chars().count()).collect();
        assert!(
            ends.windows(2).all(|pair| pair[0] == pair[1]),
            "the figures do not line up: {ends:?} in {out}",
        );
    }

    // blocks are held apart from each other whether or not there is a head
    #[test]
    fn blocks_stay_apart() {
        let headless = Doc::new()
            .facts([("a", "1")])
            .table(Table::new([Column::left("x")]).row(Row::new(["y"])));
        let out = text(&Rendered(headless), &Style::PLAIN);
        assert_eq!(
            out.lines().filter(|line| line.is_empty()).count(),
            1,
            "no blank line between the blocks: {out:?}",
        );
    }

    // a verdict that is only a word is only a word
    #[test]
    fn a_bare_verdict_carries_nothing_after_it() {
        let bare = Doc::new().verdict(Tone::Good, "CLEAN", "");
        for style in [Style::PLAIN, Style::rich(80)] {
            let out = text(&Rendered(bare.clone()), &style);
            for line in out.lines() {
                assert_eq!(line, line.trim_end(), "trailing space on {line:?}");
            }
        }
    }

    // markdown keeps a pipe inside a cell from ending the cell
    #[test]
    fn markdown_escapes_its_cells() {
        let piped = Doc::new().table(Table::new([Column::left("path")]).row(Row::new(["a|b_c*d"])));
        let out = markdown(&Rendered(piped));
        assert!(out.contains(r"a\|b\_c\*d"), "an unescaped cell: {out}");
    }

    // a code span escapes nothing, since a backslash is literal inside one
    #[test]
    fn markdown_leaves_code_spans_alone() {
        let path = Doc::new()
            .notes(
                "not counted",
                Tone::Warn,
                vec![Note::new("nothing declared").fix("reel /srv/my_vol --column a*b cue")],
            )
            .footer(["reel /srv/my_vol cue"]);
        let out = markdown(&Rendered(path));
        assert!(
            !out.contains('\\'),
            "a backslash a reader would see inside backticks: {out}",
        );
        assert!(
            out.contains("`reel /srv/my_vol --column a*b cue`"),
            "the command should survive verbatim: {out}",
        );
    }

    // a backtick in the content widens the fence rather than closing it early
    #[test]
    fn markdown_fences_past_its_content() {
        let ticked = Doc::new().footer(["use `--paged`"]);
        let out = markdown(&Rendered(ticked));
        assert!(
            out.contains("`` use `--paged` ``"),
            "the fence did not clear the content: {out}",
        );
    }

    // every block a report can hold survives the trip into markdown
    #[test]
    fn markdown_carries_every_block() {
        let out = markdown(&Rendered(sample()));
        for wanted in [
            "## vol · verify",
            "**FAULTED**",
            "| segment | bytes |",
            "**Not counted**",
            "> **Checked**",
            "`machine-readable: -o json`",
        ] {
            assert!(out.contains(wanted), "no {wanted:?} in: {out}");
        }
    }
}
