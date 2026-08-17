//! Seeded random op stream generator for the differential and crash suites
//!
//! A stream is a deterministic sequence drawn from a seed over a small key space, so
//! overwrites, deletes and group drops land on live keys often. The generator tracks
//! the live set as it emits, so an overwrite or delete always names a key the stream
//! created, and a fresh nonce makes each version distinct. The durable mix leaves out
//! the reopens and reads, so the crash suite can crash at raw io boundaries without a
//! mid stream reopen resetting the boundary count.

use std::collections::BTreeSet;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

/// Groups the generator draws from
const GROUPS: &[u16] = &[7, 8, 9];

/// Distinct addresses per group the generator draws from
const ADDRESS_SPACE: u8 = 12;

/// Smallest payload the generator emits
const MIN_LEN: usize = 1;

/// Largest payload the generator emits
const MAX_LEN: usize = 300;

/// Upper bound of the weighted op roll
const ROLL_SPACE: u32 = 100;

/// A roll below this emits a put
const PUT_CUTOFF: u32 = 45;

/// A roll below this emits an overwrite
const OVERWRITE_CUTOFF: u32 = 68;

/// A roll below this emits a delete
const DELETE_CUTOFF: u32 = 84;

/// A roll below this emits a group drop
const DROP_CUTOFF: u32 = 92;

/// Ways the differential tail of the roll splits across reopen, reads, and range delete
const QUERY_CHOICES: u32 = 5;

/// One operation in a generated stream
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamOp {
    /// Write a record at a group and address with a nonce distinguishing versions
    Put {
        group: u16,
        address: u8,
        len: usize,
        fill: u8,
    },

    /// Rewrite an existing record with a new payload and a fresh nonce
    Overwrite {
        group: u16,
        address: u8,
        len: usize,
        fill: u8,
    },

    /// Tombstone an existing record
    Delete { group: u16, address: u8 },

    /// Drop a whole group by its aligned key range
    DropGroup { group: u16 },

    /// Tombstone a half open address range inside one group
    DeleteRange { group: u16, lo: u8, hi: u8 },

    /// Seek from a group and address in a direction and read the ordered pairs
    IterFrom {
        group: u16,
        address: u8,
        descending: bool,
    },

    /// Read the ordered pairs inside a half open address range of one group
    IterRange { group: u16, lo: u8, hi: u8 },

    /// List the keys of one group without reading any payload
    IterKeysPrefix { group: u16 },

    /// Flush and reopen the persistent stores
    Reopen,
}

/// Generate a full stream including reopen boundaries
pub fn generate(seed: u64, length: usize) -> Vec<StreamOp> {
    build(seed, length, true)
}

/// Generate a stream of durable mutations with no reopen boundaries
pub fn generate_durable(seed: u64, length: usize) -> Vec<StreamOp> {
    build(seed, length, false)
}

fn build(seed: u64, length: usize, allow_reopen: bool) -> Vec<StreamOp> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut live: BTreeSet<(u16, u8)> = BTreeSet::new();
    let mut nonce: u8 = 0;

    let mut ops = Vec::with_capacity(length);
    for _ in 0..length {
        let roll = rng.gen_range(0..ROLL_SPACE);
        ops.push(choose(&mut rng, roll, &mut live, &mut nonce, allow_reopen));
    }
    ops
}

fn choose(
    rng: &mut SmallRng,
    roll: u32,
    live: &mut BTreeSet<(u16, u8)>,
    nonce: &mut u8,
    allow_reopen: bool,
) -> StreamOp {
    if roll < PUT_CUTOFF {
        emit_put(rng, live, nonce)
    } else if roll < OVERWRITE_CUTOFF {
        match emit_overwrite(rng, live, nonce) {
            Some(op) => op,
            None => emit_put(rng, live, nonce),
        }
    } else if roll < DELETE_CUTOFF {
        match emit_delete(rng, live) {
            Some(op) => op,
            None => emit_put(rng, live, nonce),
        }
    } else if roll < DROP_CUTOFF {
        emit_drop(rng, live)
    } else if allow_reopen {
        emit_query_or_reopen(rng, live)
    } else {
        emit_put(rng, live, nonce)
    }
}

fn emit_query_or_reopen(rng: &mut SmallRng, live: &mut BTreeSet<(u16, u8)>) -> StreamOp {
    match rng.gen_range(0..QUERY_CHOICES) {
        0 => StreamOp::Reopen,
        1 => emit_iter_from(rng),
        2 => emit_iter_range(rng),
        3 => emit_iter_keys_prefix(rng),
        _ => emit_delete_range(rng, live),
    }
}

fn emit_iter_from(rng: &mut SmallRng) -> StreamOp {
    StreamOp::IterFrom {
        group: pick_group(rng),
        address: rng.gen_range(0..ADDRESS_SPACE),
        descending: rng.gen_range(0..2u8) == 1,
    }
}

fn emit_iter_range(rng: &mut SmallRng) -> StreamOp {
    let group = pick_group(rng);
    let (lo, hi) = ordered_pair(rng);
    StreamOp::IterRange { group, lo, hi }
}

fn emit_iter_keys_prefix(rng: &mut SmallRng) -> StreamOp {
    StreamOp::IterKeysPrefix {
        group: pick_group(rng),
    }
}

fn emit_delete_range(rng: &mut SmallRng, live: &mut BTreeSet<(u16, u8)>) -> StreamOp {
    let group = pick_group(rng);
    let (lo, hi) = ordered_pair(rng);
    live.retain(|(held_group, held_address)| {
        !(*held_group == group && *held_address >= lo && *held_address < hi)
    });
    StreamOp::DeleteRange { group, lo, hi }
}

fn ordered_pair(rng: &mut SmallRng) -> (u8, u8) {
    let first = rng.gen_range(0..ADDRESS_SPACE);
    let second = rng.gen_range(0..ADDRESS_SPACE);
    (first.min(second), first.max(second))
}

fn emit_put(rng: &mut SmallRng, live: &mut BTreeSet<(u16, u8)>, nonce: &mut u8) -> StreamOp {
    let group = pick_group(rng);
    let address = rng.gen_range(0..ADDRESS_SPACE);
    live.insert((group, address));
    StreamOp::Put {
        group,
        address,
        len: rng.gen_range(MIN_LEN..=MAX_LEN),
        fill: next_nonce(nonce),
    }
}

fn emit_overwrite(
    rng: &mut SmallRng,
    live: &BTreeSet<(u16, u8)>,
    nonce: &mut u8,
) -> Option<StreamOp> {
    let (group, address) = pick_live(rng, live)?;
    Some(StreamOp::Overwrite {
        group,
        address,
        len: rng.gen_range(MIN_LEN..=MAX_LEN),
        fill: next_nonce(nonce),
    })
}

fn emit_delete(rng: &mut SmallRng, live: &mut BTreeSet<(u16, u8)>) -> Option<StreamOp> {
    let (group, address) = pick_live(rng, live)?;
    live.remove(&(group, address));
    Some(StreamOp::Delete { group, address })
}

fn emit_drop(rng: &mut SmallRng, live: &mut BTreeSet<(u16, u8)>) -> StreamOp {
    let group = pick_group(rng);
    live.retain(|(held, _)| *held != group);
    StreamOp::DropGroup { group }
}

fn pick_group(rng: &mut SmallRng) -> u16 {
    GROUPS[rng.gen_range(0..GROUPS.len())]
}

fn pick_live(rng: &mut SmallRng, live: &BTreeSet<(u16, u8)>) -> Option<(u16, u8)> {
    if live.is_empty() {
        return None;
    }
    live.iter().nth(rng.gen_range(0..live.len())).copied()
}

fn next_nonce(nonce: &mut u8) -> u8 {
    *nonce = nonce.wrapping_add(1);
    *nonce
}
