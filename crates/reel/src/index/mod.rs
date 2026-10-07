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
