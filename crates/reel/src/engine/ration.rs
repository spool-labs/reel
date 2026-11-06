//! The seal's ration: writers draw free index room in step with the walk, so a write waits for a seal's step and never for a
//! whole seal. A seal frees its rows only as it ends, so room a writer takes early has to last until then

use std::sync::atomic::{AtomicU64, Ordering};

use reel_api::units::{Nanoseconds, RowCount};

/// Eighths of the free room writers draw during the walk. The rest is held back from the start for the steps after it
pub const WALK_RATION_EIGHTHS: u64 = 7;
/// Rows the line stands ahead of the walk, so a batch of this many never waits for more than one step
pub const RATION_LEAD_ROWS: u64 = 1_024;
/// Marks a walk that hasn't ended
pub const NOT_ENDED: u64 = 0;
/// Times a writer looks at the line before it pauses
pub const RATION_SPINS: u32 = 256;

/// The room one volume's seal has rationed, which a seal fills in at its start and clears at its end
#[derive(Debug, Default)]
pub struct Ration {
    seal_rows: AtomicU64,
    start_rows: AtomicU64,
    free_rows: AtomicU64,
    walked_rows: AtomicU64,
    cleaned_rows: AtomicU64,
    walk_started: AtomicU64,
    walk_ended: AtomicU64,
    next_ticket: AtomicU64,
    serving: AtomicU64,
}

impl Ration {
    /// Start a seal of this many rows, with the index holding some rows and able to take some more. Two volumes sealing at
    /// once each count the same free room as theirs, so together they can admit past the index and fall back on its own wait
    pub fn start(&self, seal_rows: RowCount, held: RowCount, free: RowCount, now: Nanoseconds) {
        self.start_rows.store(held.0, Ordering::Relaxed);
        self.free_rows.store(free.0, Ordering::Relaxed);
        self.walked_rows.store(0, Ordering::Relaxed);
        self.cleaned_rows.store(0, Ordering::Relaxed);
        self.walk_started.store(now.0, Ordering::Relaxed);
        self.walk_ended.store(NOT_ENDED, Ordering::Relaxed);
        self.seal_rows.store(seal_rows.0, Ordering::Release);
    }

    /// End the ration, which leaves every write free to go
    pub fn clear(&self) {
        self.seal_rows.store(0, Ordering::Release);
    }

    /// Note how far the walk has come
    pub fn note_walked(&self, rows: RowCount) {
        self.walked_rows.store(rows.0, Ordering::Release);
    }

    /// Note the walk has ended, from when the share held back goes out on the clock
    pub fn note_walk_end(&self, now: Nanoseconds) {
        self.walk_ended.store(now.0, Ordering::Release);
    }

    /// Note how far the clean after the walk has come
    pub fn note_cleaned(&self, rows: RowCount) {
        self.cleaned_rows.store(rows.0, Ordering::Release);
    }

    /// Read the most rows the index may hold now, or nothing when no seal is rationing. The steps after the walk show no
    /// progress of their own, so the share held back for them goes out at the walk's pace, and the clean can only hurry it
    pub fn line(&self, now: impl Fn() -> Nanoseconds) -> Option<RowCount> {
        let seal_rows = self.seal_rows.load(Ordering::Acquire);
        if seal_rows == 0 {
            return None;
        }
        let free = self.free_rows.load(Ordering::Relaxed);
        let walked = self.walked_rows.load(Ordering::Acquire).saturating_add(RATION_LEAD_ROWS);
        let cleaned = self.cleaned_rows.load(Ordering::Acquire);
        let walk_share = free / 8 * WALK_RATION_EIGHTHS;
        let after_share = free - walk_share;
        let by_clean = share(after_share, cleaned, seal_rows);
        let by_clock = self.drawn_on_clock(walk_share, after_share, now);
        let drawn = share(walk_share, walked, seal_rows) + by_clean.max(by_clock);
        Some(RowCount(self.start_rows.load(Ordering::Relaxed).saturating_add(drawn)))
    }

    /// Work out what the clock has let out since the walk ended, at the rate the walk drew its own share
    pub fn drawn_on_clock(&self, walk_share: u64, after_share: u64, now: impl Fn() -> Nanoseconds) -> u64 {
        let ended = self.walk_ended.load(Ordering::Acquire);
        if ended == NOT_ENDED {
            return 0;
        }
        let walk_time = ended.saturating_sub(self.walk_started.load(Ordering::Relaxed)).max(1);
        let since = now().0.saturating_sub(ended);
        let drawn = u128::from(walk_share) * u128::from(since) / u128::from(walk_time);
        drawn.min(u128::from(after_share)) as u64
    }

    /// Check a write can go on ahead of any wait: nothing is rationed, or it fits the line with nobody in line
    pub fn lets_through(&self, held: RowCount, rows: RowCount, now: impl Fn() -> Nanoseconds) -> bool {
        match self.line(now) {
            Some(line) => !self.has_line_of_writers() && held.0 + rows.0 <= line.0,
            None => true,
        }
    }

    /// Check a writer holds a ticket nobody has served
    pub fn has_line_of_writers(&self) -> bool {
        self.serving.load(Ordering::Acquire) != self.next_ticket.load(Ordering::Acquire)
    }

    /// Take the next place in line, so a large batch isn't passed by the puts behind it
    pub fn take_ticket(&self) -> u64 {
        self.next_ticket.fetch_add(1, Ordering::AcqRel)
    }

    /// Check a ticket has reached the front of the line
    pub fn is_served(&self, ticket: u64) -> bool {
        self.serving.load(Ordering::Acquire) == ticket
    }

    /// Let the next ticket come to the front
    pub fn next_in_line(&self) {
        self.serving.fetch_add(1, Ordering::AcqRel);
    }
}

/// Work out the part of some room this far through some rows, and never more than the room
pub fn share(room: u64, done: u64, rows: u64) -> u64 {
    (u128::from(room) * u128::from(done.min(rows)) / u128::from(rows)) as u64
}
