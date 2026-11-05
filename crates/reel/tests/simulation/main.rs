//! Deterministic simulation: the engine on a simulated disk, clock and scheduler
#![cfg(test)]

mod allocations;
mod counting;
mod damage;
mod io_contract;
mod place_limit;
mod records;
mod seed_sets;
mod write_index;
mod faults;
mod footers;
mod spot_index;
mod staged;
mod store;
