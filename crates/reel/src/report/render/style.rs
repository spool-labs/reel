//! What a renderer is allowed to dress its output with
//!
//! The engine reads no terminal and consults no environment: a frontend decides
//! whether it is talking to a person and hands the answer down. That keeps the
//! renderers pure, so a test can ask for the dressed form and the plain one from
//! the same report and compare them.

/// Whether a renderer may colour and frame, and how wide it may draw
#[derive(Clone, Copy, Debug)]
pub struct Style {
    /// Whether escape sequences may be written at all
    pub color: bool,

    /// Whether the head and verdict sit inside a drawn frame
    pub frames: bool,

    /// Columns available, which is what a frame is sized against
    pub width: usize,
}

impl Default for Style {
    fn default() -> Style {
        Style::PLAIN
    }
}

impl Style {
    /// No colour and no frame, which is what a pipe and a file want
    pub const PLAIN: Style = Style {
        color: false,
        frames: false,
        width: 80,
    };

    /// Dressed for a terminal of this width
    pub fn rich(width: usize) -> Style {
        Style {
            color: true,
            frames: true,
            width: width.max(40),
        }
    }

    /// The widest a drawn frame may be, leaving room for its own edges
    pub(super) fn frame_width(&self) -> usize {
        self.width.saturating_sub(4).max(20)
    }
}

/// The escape sequence a role is painted with, or nothing where colour is off
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Paint {
    /// Labels, units and anything the eye should slide over
    Dim,

    /// The answer, where the answer is the one hoped for
    Good,

    /// A caveat: true, but not the whole of it
    Warn,

    /// A fault
    Bad,

    /// The line that carries the most, drawn heavier than the rest
    Strong,
}

impl Paint {
    fn code(self) -> &'static str {
        match self {
            Paint::Dim => "\x1b[2m",
            Paint::Good => "\x1b[32m",
            Paint::Warn => "\x1b[33m",
            Paint::Bad => "\x1b[31m",
            Paint::Strong => "\x1b[1m",
        }
    }
}

/// Wrap text in a role's escape sequence, or hand it back where colour is off
pub(super) fn paint(style: &Style, role: Paint, text: &str) -> String {
    match style.color && !text.is_empty() {
        true => format!("{}{text}\x1b[0m", role.code()),
        false => text.to_string(),
    }
}

/// Columns a string occupies, which is its characters rather than its bytes
///
/// Every character these reports draw with is one column wide, so counting them
/// is the width. Counting bytes instead would over-measure the separators and
/// the box edges, and every table drawn beside one would sit crooked.
pub(super) fn width(text: &str) -> usize {
    text.chars().count()
}

/// Pad a string out to a width, on the side the cell sits against
pub(super) fn pad(text: &str, to: usize, left: bool) -> String {
    let short = to.saturating_sub(width(text));
    match left {
        true => format!("{text}{:short$}", "", short = short),
        false => format!("{:short$}{text}", "", short = short),
    }
}
