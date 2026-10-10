//! Colour, frame and width settings a frontend hands to the renderers

/// Whether a renderer may colour and frame, and how wide it may draw
#[derive(Clone, Copy, Debug)]
pub struct Style {
    /// Whether escape sequences may be written at all
    pub color: bool,

    /// Whether the head and verdict sit inside a drawn frame
    pub frames: bool,

    /// Terminal width in columns, which sizes the frame
    pub width: usize,
}

impl Default for Style {
    fn default() -> Style {
        Style::PLAIN
    }
}

impl Style {
    /// No colour and no frame, for a pipe or a file
    pub const PLAIN: Style = Style {
        color: false,
        frames: false,
        width: 80,
    };

    /// Colour and frames for a terminal of this width
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

/// The roles text can be painted in, each with its own escape sequence
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Paint {
    /// Labels, units and other text the eye should skip
    Dim,

    /// The answer that was hoped for
    Good,

    /// A caveat, true but partial
    Warn,

    /// A fault
    Bad,

    /// The line that matters most, drawn heavier than the rest
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

/// A string's width in columns, one per character
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
