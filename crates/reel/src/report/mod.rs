//! Reports on a volume, as data rather than as printing
//!
//! Each verb answers a struct of rows and totals. Rendering them is `render`,
//! serialising them is serde under the `serde` feature, and a caller that wants
//! neither takes the struct. Nothing here opens a volume: the frontend decides
//! whether it wants the ownership lock and a resident or paged index.

pub mod checkpoint;
pub mod cue;
pub mod doctor;
pub mod render;
pub mod spans;
pub mod spec;
pub mod stat;
pub mod verify;
