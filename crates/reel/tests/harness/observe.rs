//! Observable state extracted from any store over the harness columns
//!
//! Every backend is scanned through the store trait and reduced to the same snapshot:
//! an ordered dump of the record column, the global live count and bytes, and those
//! totals split per group. The dump is the ground truth a differential run compares
//! across backends, and the totals are what a crash recount checks the counters
//! against.

use std::collections::BTreeMap;

use reel_core::Store;

use crate::harness::wire::RECORDS_CF;

/// Live count and byte total for a group or the whole store
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Totals {
    /// Number of live records
    pub count: u64,

    /// Total live value bytes
    pub bytes: u64,
}

/// A comparable snapshot of everything a store serves on the harness columns
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Observation {
    /// Ordered key and value dump of the record column
    pub records: Vec<(Vec<u8>, Vec<u8>)>,

    /// Global live count and byte total
    pub global: Totals,

    /// Live count and byte total per group
    pub per_group: BTreeMap<u16, Totals>,

    /// Live count per group as the backend's own count path answers it
    pub counted: BTreeMap<u16, u64>,
}

/// Extract the observable state of a store over the harness columns
pub fn observe<Backend: Store>(store: &Backend) -> Observation {
    let records = dump(store, RECORDS_CF);

    let mut global = Totals::default();
    let mut per_group: BTreeMap<u16, Totals> = BTreeMap::new();
    for (key, value) in &records {
        let bytes = value.len() as u64;
        global.count += 1;
        global.bytes += bytes;
        let totals = per_group.entry(group_of(key)).or_default();
        totals.count += 1;
        totals.bytes += bytes;
    }

    let mut counted = BTreeMap::new();
    for (group, totals) in &per_group {
        let prefix = group.to_be_bytes();
        let count = store
            .count_prefix(RECORDS_CF, &prefix)
            .expect("count a group");
        // The failure names keys rather than only numbers: the walk and the dump take
        // the same keys from the same index and part company at the payload, so a key
        // in one and not the other is a key whose value the store could not produce.
        if count != totals.count {
            let walked = store
                .iter_keys_prefix(RECORDS_CF, &prefix)
                .expect("walk a group");
            let dumped: Vec<Vec<u8>> = records
                .iter()
                .map(|(key, _)| key.clone())
                .filter(|key| group_of(key) == *group)
                .collect();
            let missing: Vec<String> = walked
                .iter()
                .filter(|key| !dumped.contains(key))
                .map(|key| hex(key))
                .collect();
            panic!(
                "the count path disagrees with the dump on group {group}: counted {count}, dumped {}, \
                 walked {}, keys the walk has and the dump does not: {missing:?}",
                totals.count,
                walked.len(),
            );
        }
        counted.insert(*group, count);
    }
    let whole = store
        .count_prefix(RECORDS_CF, &[])
        .expect("count every group");
    // This is the one that catches a key whose group the dump never saw at all, since
    // the loop above only visits groups the dump already knows about.
    if whole != global.count {
        let walked = store
            .iter_keys_prefix(RECORDS_CF, &[])
            .expect("walk every group");
        let dumped: Vec<Vec<u8>> = records.iter().map(|(key, _)| key.clone()).collect();
        let missing: Vec<String> = walked
            .iter()
            .filter(|key| !dumped.contains(key))
            .map(|key| hex(key))
            .collect();
        panic!(
            "the count path disagrees with the dump over every group: counted {whole}, dumped {}, \
             walked {}, keys the walk has and the dump does not: {missing:?}",
            global.count,
            walked.len(),
        );
    }

    Observation {
        records,
        global,
        per_group,
        counted,
    }
}

/// Read one column into an ordered key and value dump
fn dump<Backend: Store>(store: &Backend, cf: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let iterator = store.iter(cf).expect("iterate a column");
    let mut out = Vec::new();
    for (key, value) in iterator {
        out.push((key, value.into_vec()));
    }
    out
}

/// The group a wire record key belongs to, from its big endian prefix
fn group_of(key: &[u8]) -> u16 {
    u16::from_be_bytes([key[0], key[1]])
}

/// A key as hex, for a failure that has to name one
fn hex(key: &[u8]) -> String {
    key.iter().map(|byte| format!("{byte:02x}")).collect()
}
