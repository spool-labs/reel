//! Reports on a volume, as data rather than as printing
//!
//! Each verb answers a struct of rows and totals, and every figure it could not
//! count comes back beside a caveat saying so. Shaping one for a reader is
//! `doc`, drawing that shape is `render`, serialising the struct is serde under
//! the `serde` feature, and a caller that wants none of the three takes the
//! struct. Nothing here opens a volume: the frontend decides whether it wants the
//! ownership lock and a resident or paged index.

pub mod caveat;
pub mod checkpoint;
pub mod cue;
pub mod doc;
pub mod doctor;
pub mod fmt;
pub mod render;
pub mod spans;
pub mod spec;
pub mod stat;
pub mod verify;

pub use caveat::Caveat;
pub use doc::Doc;
pub use render::{Report, Style};
