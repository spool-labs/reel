//! Fuzzes the engine against the in-memory oracle with the differential suite's op stream

#![no_main]

#[allow(dead_code)]
#[path = "../../crates/reel/tests/harness/mod.rs"]
mod harness;

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;

use reel::{ByteCount, ReelConfig, SyncPolicy, ThreadBudget};

use harness::fixture::Differential;
use harness::op_stream::{StreamOp, ADDRESS_SPACE, GROUPS, MAX_LEN, MIN_LEN};

/// A case runs at most this many ops, enough to reach a reopen
const MAX_OPS: usize = 64;

/// The differential suite's segment size, which rolls a few times per case
const SEGMENT_BYTES: u64 = 64 * 1024;

/// Four tails, which puts every insert through the version guard
const TAILS: u32 = 4;

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT_BYTES),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(TAILS),
        ..ReelConfig::default()
    }
}

/// Draw one op inside the generator's key space and payload range
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

    // The seed only sets the fault plan, which is quiet here, so any divergence comes from the ops
    Differential::open(0, config()).run_stream(&ops);
});
