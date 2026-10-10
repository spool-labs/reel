//! The index: per-column key maps, the spot index, segment counters, rebuild, and locks

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

/// Hand out each group's rows in slices of `chunk`, one per group a round, so waiters get the lock
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

    // every group gives up one slice before any group gives a second
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
