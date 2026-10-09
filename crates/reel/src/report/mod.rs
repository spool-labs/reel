//! Reports on a volume as data, with a caveat for every figure they could not count

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
