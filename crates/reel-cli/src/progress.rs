//! A bar for the one verb that takes long enough to need one
//!
//! A sweep of a full volume is minutes of reading with nothing to show for it
//! until the end. The engine says how far it has got as often as it likes; what
//! is worth repainting, and whether there is anybody to repaint it for, is
//! decided here.
//!
//! It draws on standard error, so a report piped into a file or into `jq`
//! arrives with none of this in it, and it erases itself before the report is
//! written so nothing of the bar survives the run.

use std::io::{stderr, Write};
use std::time::{Duration, Instant};

use crate::term::Watch;

/// The ramp a bar fades out along, densest first
///
/// A hard edge between filled and empty reads as a boundary that means
/// something. Fading across a few cells says the frontier is where the reading
/// is, which is what it is.
const RAMP: [char; 3] = ['█', '▓', '▒'];

/// What the unfilled remainder is drawn with
const REST: char = '░';

/// A progress bar, or nothing at all where there is nobody watching
pub struct Bar {
    /// What the bar says is happening, in a word
    label: &'static str,

    /// When the work started, which is what the estimate is drawn from
    started: Instant,

    /// When the bar was last drawn, which is what paces the next draw
    painted: Option<Instant>,

    /// Cells across, bar and all
    width: usize,

    /// Whether anything is drawn at all
    drawn: bool,

    /// Whether the bar may paint, as against only moving the cursor
    ///
    /// Erasing is how the bar stays out of the scrollback and is not a colour, so
    /// it happens either way; the dim it would otherwise wear does not.
    paint: bool,
}

impl Bar {
    /// Repaint no faster than this, since nobody reads a bar faster
    const EVERY: Duration = Duration::from_millis(70);

    /// A bar that draws where somebody is watching and does nothing where not
    pub fn new(label: &'static str, watch: Watch) -> Bar {
        Bar {
            label,
            started: Instant::now(),
            painted: None,
            // Room for the two lines' own indent and a margin past their end,
            // so a bar never wraps into a second row of its own.
            width: watch.width.clamp(24, 100).saturating_sub(4),
            drawn: watch.drawn,
            paint: watch.paint,
        }
    }

    /// Text in a role, or the text alone where the bar may not paint
    fn dim(&self, text: &str) -> String {
        match self.paint {
            true => format!("\x1b[2m{text}\x1b[0m"),
            false => text.to_string(),
        }
    }

    /// Say how far along the work is, drawing if it is time to
    pub fn show(&mut self, fraction: f64) {
        if !self.drawn {
            return;
        }
        let now = Instant::now();
        if let Some(painted) = self.painted {
            if now.duration_since(painted) < Bar::EVERY {
                return;
            }
        }
        let _ = self.paint(fraction.clamp(0.0, 1.0), now);
    }

    /// Take the bar off the screen, leaving the report the only thing written
    pub fn done(&mut self) {
        if !self.drawn || self.painted.is_none() {
            return;
        }
        let mut err = stderr().lock();
        // Up over both lines and clear everything from there down, so a report
        // written next starts on a clean screen.
        let _ = write!(err, "\x1b[2A\x1b[0J");
        let _ = err.flush();
        self.painted = None;
    }

    fn paint(&mut self, fraction: f64, now: Instant) -> std::io::Result<()> {
        let mut err = stderr().lock();
        if self.painted.is_some() {
            write!(err, "\x1b[2A")?;
        }
        writeln!(err, "\x1b[2K  {}", self.bar(fraction))?;
        writeln!(err, "\x1b[2K  {}", self.legend(fraction, now))?;
        err.flush()?;
        self.painted = Some(now);
        Ok(())
    }

    /// The bar itself: solid behind the frontier, fading across it, faint ahead
    fn bar(&self, fraction: f64) -> String {
        let cells = self.width;
        let filled = (fraction * cells as f64).round() as usize;
        let mut bar = String::new();
        for at in 0..cells {
            bar.push(match filled.checked_sub(at) {
                // At or past the frontier there is nothing done yet.
                Some(0) | None => REST,
                // Solid well behind the frontier and thinning as it approaches,
                // so the eye lands on where the reading actually is.
                Some(ahead) => RAMP[RAMP.len() - ahead.min(RAMP.len())],
            });
        }
        // Dim ahead of the frontier and plain behind it, so the two halves read
        // apart on a terminal that has no colour to spare. Where the bar may not
        // paint, the ramp characters carry the difference on their own.
        let split = bar
            .char_indices()
            .nth(filled)
            .map(|(at, _)| at)
            .unwrap_or(bar.len());
        format!("{}{}", &bar[..split], self.dim(&bar[split..]))
    }

    /// The percentage on the left and what is left on the right
    fn legend(&self, fraction: f64, now: Instant) -> String {
        let done = format!("{:.0}% {}", fraction * 100.0, self.label);
        let left = match remaining(self.started, now, fraction) {
            Some(left) => format!("{} LEFT", span(left)),
            None => String::new(),
        };
        let gap = self
            .width
            .saturating_sub(done.chars().count() + left.chars().count())
            .max(1);
        self.dim(&format!("{done}{:gap$}{left}", "", gap = gap))
    }
}

/// What is left, from what the work has taken so far
///
/// Nothing is guessed from a standing start: until enough has been read for the
/// rate to mean anything, the estimate is withheld rather than invented.
fn remaining(started: Instant, now: Instant, fraction: f64) -> Option<Duration> {
    const ENOUGH: f64 = 0.02;
    if fraction <= ENOUGH || fraction >= 1.0 {
        return None;
    }
    let spent = now.duration_since(started).as_secs_f64();
    if spent < 0.5 {
        return None;
    }
    let left = Duration::from_secs_f64(spent / fraction - spent);
    // Under a second there is nothing worth saying, and "0S LEFT" standing over
    // a bar that is still moving reads as a stall rather than as an estimate.
    match left.as_secs() {
        0 => None,
        _ => Some(left),
    }
}

/// A duration as a reader waiting on it would say it
fn span(left: Duration) -> String {
    let seconds = left.as_secs();
    match seconds {
        seconds if seconds >= 3600 => format!("{}H {}M", seconds / 3600, (seconds % 3600) / 60),
        seconds if seconds >= 60 => format!("{}M {}S", seconds / 60, seconds % 60),
        seconds => format!("{seconds}S"),
    }
}
