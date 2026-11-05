//! Deterministic simulation: the engine on a simulated disk, clock and scheduler
#![cfg(test)]

mod damage;
mod io_contract;
mod place_limit;
mod records;
mod write_index;
mod footers;
mod spot_index;
mod staged;
mod store;
