//! Decides whether output goes to a person, so pipes, files and CI logs stay plain

use std::io::{IsTerminal, Stderr, Stdout};

use clap::ValueEnum;

use reel::report::Style;

/// When output may be coloured and framed
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
#[clap(rename_all = "lowercase")]
pub enum ColorChoice {
    /// Dressed when writing to a terminal, plain otherwise
    #[default]
    Auto,

    /// Always dressed, for a pager that renders escapes
    Always,

    /// Never dressed
    Never,
}

impl ColorChoice {
    /// Whether to dress output, given whether the stream is a terminal
    fn dresses(self, tty: bool) -> bool {
        match self {
            ColorChoice::Always => true,
            ColorChoice::Never => false,
            // Setting NO_COLOR to any value turns colour off
            ColorChoice::Auto => tty && std::env::var_os("NO_COLOR").is_none(),
        }
    }
}

/// The style for a report on standard output, sized from standard output's width
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

/// Progress bar settings, drawn only on a terminal stderr unless --color never
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
