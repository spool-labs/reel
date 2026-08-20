//! What kind of thing is on the other end of the output
//!
//! The engine renders into whatever style it is handed and reads no terminal of
//! its own, so deciding whether there is a person out there is this binary's
//! job. A pipe, a file and a CI log all get the plain form, because escape
//! sequences in a captured log are noise a reader cannot turn off.

use std::io::{IsTerminal, Stderr, Stdout};

use clap::ValueEnum;

use reel::report::Style;

/// Whether output may be dressed, where the caller wants a say
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
#[clap(rename_all = "lowercase")]
pub enum ColorChoice {
    /// Dressed when writing to a terminal, plain otherwise
    #[default]
    Auto,

    /// Dressed whatever it is writing to, for a pager that renders escapes
    Always,

    /// Never dressed
    Never,
}

impl ColorChoice {
    /// Whether this choice dresses output going to the given stream
    fn dresses(self, tty: bool) -> bool {
        match self {
            ColorChoice::Always => true,
            ColorChoice::Never => false,
            // NO_COLOR is honoured whatever its value, which is what the
            // convention asks: setting it at all is the request.
            ColorChoice::Auto => tty && std::env::var_os("NO_COLOR").is_none(),
        }
    }
}

/// The style a report going to standard output should be rendered in
///
/// Measured on the stream the report is written to, not on the other one: a
/// frame drawn to a wide stdout while stderr is redirected would otherwise be
/// sized from the fallback and sit narrow in the middle of the terminal.
pub fn style(choice: ColorChoice, out: &Stdout) -> Style {
    match choice.dresses(out.is_terminal()) {
        true => Style::rich(columns(libc::STDOUT_FILENO)),
        false => Style::PLAIN,
    }
}

/// What a progress bar on standard error may do
pub struct Watch {
    /// Whether it is drawn at all
    pub drawn: bool,

    /// Whether it may paint as well as move the cursor
    pub paint: bool,

    /// Columns it has to draw in
    pub width: usize,
}

/// What a progress bar on standard error is allowed to do
///
/// Progress goes to standard error so that a report piped into a file or into
/// `jq` arrives clean, and it is drawn only where somebody is actually watching:
/// a bar repaints in place, so a redirected stream collects every frame of it as
/// a line of its own. `--color always` is a request about colour and not about
/// that, so it cannot force a bar into a pipe the way it can force an escape
/// sequence into one.
///
/// Painting is asked separately, because moving the cursor and colouring are two
/// different requests: a bar that erases itself is how it stays out of the
/// scrollback, and `NO_COLOR` is about the colour it would otherwise paint with.
pub fn watch(choice: ColorChoice, err: &Stderr) -> Watch {
    let tty = err.is_terminal();
    Watch {
        drawn: tty && choice != ColorChoice::Never,
        paint: choice.dresses(tty),
        width: columns(libc::STDERR_FILENO),
    }
}

/// Columns a stream has, or the width to assume where it will not say
fn columns(stream: libc::c_int) -> usize {
    const ASSUMED: usize = 80;
    if let Some(columns) = winsize(stream) {
        return columns;
    }
    std::env::var("COLUMNS")
        .ok()
        .and_then(|columns| columns.parse().ok())
        .filter(|columns| *columns > 0)
        .unwrap_or(ASSUMED)
}

/// Ask the terminal behind a stream how wide it is
fn winsize(stream: libc::c_int) -> Option<usize> {
    let mut size: libc::winsize = unsafe { std::mem::zeroed() };
    let asked = unsafe { libc::ioctl(stream, libc::TIOCGWINSZ, &mut size) };
    match asked == 0 && size.ws_col > 0 {
        true => Some(size.ws_col as usize),
        false => None,
    }
}
