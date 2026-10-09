//! Progress bar on standard error for the sweep, erased before the report is written

use std::io::{stderr, Write};
use std::time::{Duration, Instant};

use crate::term::Watch;

/// The cells a bar fades through at its frontier, densest first
const RAMP: [char; 3] = ['█', '▓', '▒'];

/// What the unfilled remainder is drawn with
const REST: char = '░';

/// A progress bar that draws nothing when nobody is watching
pub struct Bar {
    /// What the bar says is happening, in a word
    label: &'static str,

    /// When the work started, for the time estimate
    started: Instant,

    /// When the bar was last drawn, which paces the next draw
    painted: Option<Instant>,

    /// Width of the bar in cells
    width: usize,

    /// Whether anything is drawn at all
    drawn: bool,

    /// Whether the bar may use colour, which erasing does not need
    paint: bool,
}

impl Bar {
    /// The bar repaints at most this often
    const EVERY: Duration = Duration::from_millis(70);

    /// A bar that draws only where somebody is watching
    pub fn new(label: &'static str, watch: Watch) -> Bar {
        Bar {
            label,
            started: Instant::now(),
            painted: None,
            // Leave room for the indent and a margin so the bar never wraps
            width: watch.width.clamp(24, 100).saturating_sub(4),
            drawn: watch.drawn,
            paint: watch.paint,
        }
    }

    /// Dim the text, or leave it plain when the bar may not paint
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

    /// Erase the bar so the report is the only thing written
    pub fn done(&mut self) {
        if !self.drawn || self.painted.is_none() {
            return;
        }
        let mut err = stderr().lock();
        // Move up over both lines and clear to the end of the screen
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
                // At or past the frontier nothing is done yet
                Some(0) | None => REST,
                // Solid well behind the frontier, thinning as it nears it
                Some(ahead) => RAMP[RAMP.len() - ahead.min(RAMP.len())],
            });
        }
        // Plain behind the frontier and dim ahead of it
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

/// Time left at the current rate, or none until enough has been read to estimate
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
    // Under a second shows nothing, since "0S LEFT" on a moving bar reads as a stall
    match left.as_secs() {
        0 => None,
        _ => Some(left),
    }
}

/// A duration in hours, minutes or seconds
fn span(left: Duration) -> String {
    let seconds = left.as_secs();
    match seconds {
        seconds if seconds >= 3600 => format!("{}H {}M", seconds / 3600, (seconds % 3600) / 60),
        seconds if seconds >= 60 => format!("{}M {}S", seconds / 60, seconds % 60),
        seconds => format!("{seconds}S"),
    }
}
