//! The index: per-column key maps, the spot index, segment counters, rebuild, and locks
//!
//! A reel keeps the record locations of its open tails in one ordered map per
//! column, guarded by sequence number and backed by per-segment reclaimable-byte
//! counters, and finds a sealed key through its footer and the spot index. On open
//! the maps are rebuilt from the tails and the spot index from the footers, and a
//! writable open takes an ownership lock so a second writer fails loudly.

pub mod column;
pub mod counters;
pub mod entry;
pub mod keyrun;
pub mod lockfile;
pub mod map;
pub mod page;
pub mod paged;
pub mod playback;
pub mod recovery;
pub mod spot;
pub mod tailer;
pub mod tbtreemap;

/// Each group's rows a chunk at a time, one chunk per group per round
///
/// A lock taken per chunk then sits free while the other groups take theirs, so a
/// put or a get waiting on it gets in. Taking the next chunk of the same group
/// straight away would win the lock back before the waiter wakes.
pub(crate) fn in_turns(
    groups: &[(usize, Vec<usize>)],
    chunk: usize,
) -> impl Iterator<Item = (usize, &[usize])> {
    let rounds = groups
        .iter()
        .map(|(_, ats)| ats.len().div_ceil(chunk))
        .max()
        .unwrap_or(0);
    (0..rounds).flat_map(move |round| {
        groups.iter().filter_map(move |(group, ats)| {
            let start = round * chunk;
            (start < ats.len()).then(|| (*group, &ats[start..(start + chunk).min(ats.len())]))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::in_turns;

    // every group gives up one chunk before any group gives a second
    #[test]
    fn groups_take_their_chunks_in_turns() {
        let groups = vec![(3, vec![0, 1, 2, 3, 4]), (7, vec![5, 6])];
        let turns: Vec<(usize, Vec<usize>)> = in_turns(&groups, 2)
            .map(|(group, chunk)| (group, chunk.to_vec()))
            .collect();
        assert_eq!(
            turns,
            vec![
                (3, vec![0, 1]),
                (7, vec![5, 6]),
                (3, vec![2, 3]),
                (3, vec![4]),
            ]
        );
    }
}
