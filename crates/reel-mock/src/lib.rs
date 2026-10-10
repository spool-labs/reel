//! In-memory test double for the store trait, with one lock over every column family

mod memory;

#[cfg(test)]
mod integration_tests;

pub use memory::MemoryStore;
