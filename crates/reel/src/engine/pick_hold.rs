//! A volume's summary held for a pick, whose hold counts toward its kind's longest when it drops

use std::ops::{Deref, DerefMut};
use std::sync::MutexGuard;
use std::sync::atomic::Ordering;

use reel_core::maintenance_report::PickKind;
use reel_core::units::Nanoseconds;

use crate::engine::engine_state::EngineState;
use crate::guard::hold;
use crate::io::Io;
use crate::segment::summary::SummaryRows;

/// The held summary, the kind of pick holding it, and when the hold began
pub struct PickHold<'held, Backend: Io> {
    /// The summary under its lock
    rows: MutexGuard<'held, SummaryRows>,
    /// The state that keeps each kind's longest hold
    state: &'held EngineState<Backend>,
    /// The kind of pick
    kind: PickKind,
    /// When the lock was taken
    started: Nanoseconds,
}

impl<Backend: Io> EngineState<Backend> {
    /// Take a volume's summary for a pick of a kind
    pub fn hold_for_pick(&self, volume: usize, kind: PickKind) -> PickHold<'_, Backend> {
        let rows = hold(&self.volumes[volume].summary);
        PickHold { rows, state: self, kind, started: self.io.now() }
    }
}

impl<Backend: Io> Deref for PickHold<'_, Backend> {
    type Target = SummaryRows;

    fn deref(&self) -> &SummaryRows {
        &self.rows
    }
}

impl<Backend: Io> DerefMut for PickHold<'_, Backend> {
    fn deref_mut(&mut self) -> &mut SummaryRows {
        &mut self.rows
    }
}

impl<Backend: Io> Drop for PickHold<'_, Backend> {
    fn drop(&mut self) {
        let held = self.state.io.now().0.saturating_sub(self.started.0);
        self.state.pick_holds[self.kind.index()].fetch_max(held, Ordering::Relaxed);
    }
}
