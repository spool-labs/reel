//! Reel is an embedded key-value store for the Tapedrive storage node

pub mod config;
pub mod engine;
pub mod guard;
pub mod index;
pub mod io;
pub mod little_endian;
pub mod record;
pub mod pool;
pub mod row_kind;
pub mod rows;
pub mod segment;
pub mod superblock;
pub mod write_order;
