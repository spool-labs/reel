//! The engine itself, against the oracle, under guided mutation
//!
//! The other three targets ask whether a parser survives bad bytes. This one asks
//! whether the store is right, which is a different question and the one worth the
//! machine time. Every drawn op runs against a reel over the deterministic simulator
//! and against the in-memory oracle at once, and the fixture checks after each step
//! that everything both serve still agrees, across compaction, merges, index
//! checkpoints and reopens.
//!
//! The op stream is the same one the seeded differential suite drives, so the domain
//! stays in one place. What changes is who chooses the sequence: seeds walk where
//! they happen to fall, a guided run walks toward orderings nothing has reached.

#![no_main]

#[allow(dead_code)]
#[path = "../../crates/reel/tests/harness/mod.rs"]
mod harness;

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;

use reel::{ByteCount, Preallocate, ReelConfig, SyncPolicy, ThreadBudget};

use harness::fixture::Differential;
use harness::op_stream::{StreamOp, ADDRESS_SPACE, GROUPS, MAX_LEN, MIN_LEN};

/// Ops a case runs at most
///
/// A case opens a store and checks agreement after every step, so the run is dominated
/// by the checks rather than by the ops. Short enough to keep executions per second
/// somewhere a guided run can work with, long enough to reach a reopen.
const MAX_OPS: usize = 64;

/// Segment size that rolls a few times over a case, the differential suite's own
const SEGMENT_BYTES: u64 = 64 * 1024;

/// Space reserved ahead of the write head per allocation step
const ALLOC_CHUNK: u64 = 4 * 1024;

/// Tails the case drives, at four because that is what puts every insert through the
/// version guard
const TAILS: u32 = 4;

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT_BYTES),
        alloc_chunk: ByteCount::from_bytes(ALLOC_CHUNK),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(TAILS),
        ..ReelConfig::default()
    }
}

/// One op drawn inside the domain the generator emits
///
/// Drawn rather than derived, so the case bytes cannot ask for a key outside the
/// harness key space or a payload the fixture would not have written. A stream that
/// leaves the domain would diverge on the wire keys rather than on the engine, which
/// is not the question.
fn draw_op(u: &mut Unstructured) -> arbitrary::Result<StreamOp> {
    let group = GROUPS[usize::from(u8::arbitrary(u)?) % GROUPS.len()];
    let address = u8::arbitrary(u)? % ADDRESS_SPACE;
    let len = MIN_LEN + usize::from(u16::arbitrary(u)?) % (MAX_LEN - MIN_LEN + 1);
    let fill = u8::arbitrary(u)?;
    let first = u8::arbitrary(u)? % ADDRESS_SPACE;
    let second = u8::arbitrary(u)? % ADDRESS_SPACE;
    let (lo, hi) = (first.min(second), first.max(second));

    Ok(match u8::arbitrary(u)? % 9 {
        0 => StreamOp::Put {
            group,
            address,
            len,
            fill,
        },
        1 => StreamOp::Overwrite {
            group,
            address,
            len,
            fill,
        },
        2 => StreamOp::Delete { group, address },
        3 => StreamOp::DropGroup { group },
        4 => StreamOp::DeleteRange { group, lo, hi },
        5 => StreamOp::IterFrom {
            group,
            address,
            descending: bool::arbitrary(u)?,
        },
        6 => StreamOp::IterRange { group, lo, hi },
        7 => StreamOp::IterKeysPrefix { group },
        _ => StreamOp::Reopen,
    })
}

fuzz_target!(|case: &[u8]| {
    let mut u = Unstructured::new(case);
    let mut ops = Vec::new();
    while ops.len() < MAX_OPS && !u.is_empty() {
        match draw_op(&mut u) {
            Ok(op) => ops.push(op),
            Err(_) => break,
        }
    }
    if ops.is_empty() {
        return;
    }

    // The seed only names the simulator's fault plan, and the plan is quiet here: a
    // divergence must come from the ops, not from an injected fault. The crash suite
    // is where faults belong.
    Differential::open(0, config()).run_stream(&ops);
});
