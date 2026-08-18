//! The resident index: per-column key maps, segment counters, rebuild, and locks
//!
//! A reel keeps its record locations in one ordered map per column, guarded by
//! sequence number and backed by per-segment reclaimable-byte counters. On open
//! the maps are rebuilt from the segment files, and a writable open takes an
//! ownership lock so a second writer fails loudly.

pub mod column;
pub mod counters;
pub mod entry;
pub mod lockfile;
pub mod map;
pub mod opentable;
pub mod page;
pub mod paged;
pub mod persisted;
pub mod playback;
pub mod recovery;
pub mod sealed_keys;
pub mod tailer;
pub mod tbtreemap;
