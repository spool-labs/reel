//! Death marks: which lifetime group a marked record joins, so data that dies together sits together

use reel_core::units::DeathMark;

/// A record may wait past its death for this share of its remaining life, as one over this
pub const SLACK_DIVISOR: u64 = 4;
/// Most lifetime groups a volume keeps open. A group is one open segment
pub const MOST_LIFETIME_GROUPS: usize = 8;

/// The lowest and highest death mark a segment holds, which holds none when lowest is above highest
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarkRange {
    pub lowest: DeathMark,
    pub highest: DeathMark,
}

impl MarkRange {
    /// The range of a segment with no marks
    pub const NONE: Self = Self {
        lowest: DeathMark(u64::MAX),
        highest: DeathMark(0),
    };

    /// Check the range holds no marks
    pub fn is_empty(self) -> bool {
        self.lowest > self.highest
    }

    /// Read the highest mark, which is an open segment's ceiling after a crash
    pub fn highest(self) -> Option<DeathMark> {
        (!self.is_empty()).then_some(self.highest)
    }

    /// Widen the range over a mark
    pub fn add(&mut self, mark: DeathMark) {
        self.lowest = self.lowest.min(mark);
        self.highest = self.highest.max(mark);
    }

    /// Build the range over two ranges
    pub fn join(self, other: Self) -> Self {
        Self {
            lowest: self.lowest.min(other.lowest),
            highest: self.highest.max(other.highest),
        }
    }

    /// Check two segments may share a merge: both have no marks, or their marks overlap, so one output dies as they do
    pub fn shares_lifetime(self, other: Self) -> bool {
        if self.is_empty() || other.is_empty() {
            return self.is_empty() && other.is_empty();
        }
        self.lowest <= other.highest && other.lowest <= self.highest
    }
}

impl Default for MarkRange {
    fn default() -> Self {
        Self::NONE
    }
}

/// Read how long past its death a record may wait. A mark at or below the current one has no life left, so it has no slack
pub fn slack_of(mark: DeathMark, current: DeathMark) -> u64 {
    mark.0.saturating_sub(current.0) / SLACK_DIVISOR
}

/// Read the highest ceiling a mark may join
pub fn reach_of(mark: DeathMark, current: DeathMark) -> DeathMark {
    DeathMark(mark.0.saturating_add(slack_of(mark, current)))
}

/// Round a mark up on a power-of-two grid no coarser than its slack, so marks of nearby lifetimes share a ceiling
pub fn ceiling_of(mark: DeathMark, current: DeathMark) -> DeathMark {
    let slack = slack_of(mark, current);
    let grid = if slack == 0 { 1 } else { 1 << slack.ilog2() };
    DeathMark(mark.0.saturating_add(grid - 1) & !(grid - 1)).max(mark)
}
