//! Shared differential and crash harness for the reel store
//!
//! Seeded op streams, the columns and wire keys every backend is driven with, a
//! comparable observation taken from any of them, and the crash enumeration over the
//! deterministic simulator.

pub mod fixture;
pub mod observe;
pub mod op_stream;
pub mod reel_harness;
pub mod wire;
