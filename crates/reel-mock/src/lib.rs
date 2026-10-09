//! Test double for the store trait, not a backend choice
//!
//! Column families are hash maps under one lock over the whole store, and every
//! read copies its value. That is the shape an oracle wants and the wrong one for
//! production.

mod memory;

#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod typed_tests;

pub use memory::MemoryStore;
