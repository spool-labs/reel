//! The open-addressed table against the map it stands in for, op for op
//!
//! A seeded stream of inserts, overwrites, removes and reads runs against OpenTable
//! and BTreeMap in lockstep, and every answer has to agree: the length after every
//! mutation, every point read, the ordered walk at every audit, and bounded spans
//! both ways. Order and deletion are the halves worth testing hardest. The table has
//! no order of its own and gathers for a walk, so a short or unsorted walk is a
//! listing losing rows silently. A chain is contiguous from its home slot and a
//! delete shifts the rest of the run back over the hole, so a wrong shift leaves a
//! live key at a slot no probe walks to.

use std::collections::BTreeMap;
use std::ops::Bound;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use reel::OpenTable;

/// The state-shaped key: thirty-two bytes, uniform, and far more of them than values
type Address = [u8; 32];

/// The record key shape, a two byte group and then an address
type Grouped = [u8; 34];

/// The history key shape, a signature and then the slot big endian
type Signed = [u8; 72];

const SEEDS: &[u64] = &[3, 41, 4173, 0x5eed];
const OPS: usize = 20_000;
const AUDIT_EVERY: usize = 512;

/// Distinct keys a stream draws from, small enough that overwrites and hits happen
const POOL: u64 = 4_096;

/// A uniform address, which is what the shape is for
fn address(at: u64) -> Address {
    let mut key = [0u8; 32];
    let mut state = at.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xa5a5_a5a5_a5a5_a5a5;
    for chunk in key.chunks_mut(8) {
        state ^= state >> 33;
        state = state.wrapping_mul(0xff51_afd7_ed55_8ccd);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    key
}

/// The same address behind a group, which is how the record column is keyed
///
/// Run beside the bare address because a group leads with bytes many keys share, and
/// a hash that let those dominate would pile a shard onto one chain.
fn grouped(at: u64) -> Grouped {
    let mut key = [0u8; 34];
    key[..2].copy_from_slice(&((at % 8) as u16).to_be_bytes());
    key[2..].copy_from_slice(&address(at));
    key
}

/// A signature with the slot behind it, which is how the history columns are keyed
///
/// Run beside the other two because the trailing slot is big endian, so every key in
/// the stream agrees on its last several bytes and only the leading signature tells
/// them apart.
fn signed(at: u64) -> Signed {
    let mut key = [0u8; 72];
    key[..32].copy_from_slice(&address(at));
    key[32..64].copy_from_slice(&address(at ^ 0x5eed));
    key[64..].copy_from_slice(&at.to_be_bytes());
    key
}

/// Run one seeded stream of mutations against both structures
fn stream<const N: usize>(seed: u64, draw: fn(u64) -> [u8; N]) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut table: OpenTable<N, u64> = OpenTable::new();
    let mut oracle: BTreeMap<[u8; N], u64> = BTreeMap::new();

    for step in 0..OPS {
        let key = draw(rng.gen_range(0..POOL));
        let value = step as u64;
        match rng.gen_range(0..3u32) {
            0 | 1 => assert_eq!(
                table.insert(key, value),
                oracle.insert(key, value),
                "seed {seed} step {step}: the put displaced something else",
            ),
            _ => assert_eq!(
                table.remove(&key),
                oracle.remove(&key),
                "seed {seed} step {step}: the delete took something else",
            ),
        }
        assert_eq!(table.len(), oracle.len(), "seed {seed} step {step}");

        if step % AUDIT_EVERY == 0 {
            audit(seed, step, &table, &oracle);
        }
    }
    audit(seed, OPS, &table, &oracle);
}

/// Check every read door against the oracle over whatever is held right now
fn audit<const N: usize>(
    seed: u64,
    step: usize,
    table: &OpenTable<N, u64>,
    oracle: &BTreeMap<[u8; N], u64>,
) {
    let walked: Vec<([u8; N], u64)> = table
        .sorted()
        .into_iter()
        .map(|(key, value)| (*key, *value))
        .collect();
    let wanted: Vec<([u8; N], u64)> = oracle.iter().map(|(key, value)| (*key, *value)).collect();
    assert_eq!(walked, wanted, "seed {seed} step {step}: the ordered walk");

    for (key, value) in oracle {
        assert_eq!(
            table.get(key),
            Some(value),
            "seed {seed} step {step}: a live key"
        );
    }

    // The span's edges are keys the oracle holds, rather than two random byte strings
    // that mostly bound nothing.
    if wanted.len() >= 4 {
        let low = wanted[wanted.len() / 4].0;
        let high = wanted[wanted.len() * 3 / 4].0;
        let inside: Vec<[u8; N]> = table
            .sorted_span(Bound::Included(&low), Bound::Excluded(&high))
            .into_iter()
            .map(|(key, _)| *key)
            .collect();
        let owed: Vec<[u8; N]> = oracle.range(low..high).map(|(key, _)| *key).collect();
        assert_eq!(inside, owed, "seed {seed} step {step}: a bounded span");

        let backward: Vec<[u8; N]> = table
            .sorted_span(Bound::Included(&low), Bound::Excluded(&high))
            .into_iter()
            .rev()
            .map(|(key, _)| *key)
            .collect();
        let mut reversed = owed.clone();
        reversed.reverse();
        assert_eq!(
            backward, reversed,
            "seed {seed} step {step}: the span backwards"
        );
    }
}

// the table answers what an ordered map does over a stream of uniform addresses
#[test]
fn addresses_agree() {
    for seed in SEEDS {
        stream::<32>(*seed, address);
    }
}

// and over keys that lead with bytes many of them share
#[test]
fn grouped_keys_agree() {
    for seed in SEEDS {
        stream::<34>(*seed, grouped);
    }
}

// and over signature-wide keys, which is the widest arm a column may declare
#[test]
fn signed_keys_agree() {
    for seed in SEEDS {
        stream::<72>(*seed, signed);
    }
}

// a run installed in bulk holds exactly what a key at a time would
//
// A rebuild hands each shard one sorted run and the table sizes itself for it, so
// this is the one load that never takes the growth path.
#[test]
fn absorbed_run_agrees() {
    let mut oracle: BTreeMap<Address, u64> = BTreeMap::new();
    for at in 0..50_000u64 {
        oracle.insert(address(at), at);
    }
    let run: Vec<(Address, u64)> = oracle.iter().map(|(key, value)| (*key, *value)).collect();

    let mut table: OpenTable<32, u64> = OpenTable::new();
    table.absorb(run);

    assert_eq!(table.len(), oracle.len());
    let walked: Vec<Address> = table.sorted().into_iter().map(|(key, _)| *key).collect();
    assert_eq!(walked, oracle.keys().copied().collect::<Vec<Address>>());
    assert!(
        table.slots() < oracle.len() * 3 / 2,
        "a sized install took {} slots for {} keys",
        table.slots(),
        oracle.len(),
    );
}

// deleting most of a filled table leaves every survivor reachable
//
// The chain walk stops at the first empty slot, and deleting three keys in four is
// what makes the holes outnumber the survivors.
#[test]
fn survivors_stay_reachable() {
    let mut table: OpenTable<32, u64> = OpenTable::new();
    for at in 0..20_000u64 {
        table.insert(address(at), at);
    }

    for at in 0..20_000u64 {
        if at % 4 != 0 {
            assert_eq!(table.remove(&address(at)), Some(at));
        }
    }

    assert_eq!(table.len(), 5_000);
    for at in (0..20_000u64).step_by(4) {
        assert_eq!(
            table.get(&address(at)),
            Some(&at),
            "key {at} went unreachable"
        );
    }
    for at in 0..20_000u64 {
        if at % 4 != 0 {
            assert!(table.get(&address(at)).is_none(), "key {at} came back");
        }
    }
}
