//! The maintenance plane: dead-space compaction, tombstone trimming, and crc scrub
//!
//! Reclaiming a sealed segment's dead space means rewriting the live records that
//! remain into an active tail and retiring the old file, which the compactor does
//! under the pressure model that decides when a pass may run.

pub mod compactor;
pub mod merge;
pub mod pressure;
