//! A read at the point it took, for tests that move a job across it between the two steps

use reel::engine::read_path::{LatestRead, RecordWant};
use reel::engine::reel_store::ReelStore;
use reel::io::{InPlace, Io, in_place};
use reel_api::store_error::StoreError;

/// Answer a read at its point, or return nothing when a job crossed it and took what the point could see
pub fn answer<Backend: Io>(
    store: &ReelStore<Backend>,
    latest: &LatestRead<'_>,
    want: RecordWant,
    buf: &mut Vec<u8>,
) -> Option<Result<bool, StoreError>> {
    let answer = in_place(store.state.get_at(&InPlace, &latest.read, latest.handovers, want, buf));
    let is_bad = matches!(answer, Err(StoreError::Corruption { .. }));
    store.state.has_outlived(latest.counts, is_bad).then_some(answer)
}
