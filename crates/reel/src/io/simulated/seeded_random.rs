//! The random source behind every simulated choice

/// Added to the state at every draw
pub const STATE_STEP: u64 = 0x9E37_79B9_7F4A_7C15;
/// First mixing multiplier
pub const MIX_FIRST: u64 = 0xBF58_476D_1CE4_E5B9;
/// Second mixing multiplier
pub const MIX_SECOND: u64 = 0x94D0_49BB_1331_11EB;
/// First mixing shift
pub const SHIFT_FIRST: u32 = 30;
/// Second mixing shift
pub const SHIFT_SECOND: u32 = 27;
/// Last mixing shift
pub const SHIFT_LAST: u32 = 31;

/// A small generator whose whole output follows from its seed
#[derive(Clone, Debug)]
pub struct SeededRandom {
    state: u64,
}

impl SeededRandom {
    /// Start from a seed
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Draw the next word
    pub fn next_word(&mut self) -> u64 {
        self.state = self.state.wrapping_add(STATE_STEP);
        let mut word = self.state;
        word = (word ^ (word >> SHIFT_FIRST)).wrapping_mul(MIX_FIRST);
        word = (word ^ (word >> SHIFT_SECOND)).wrapping_mul(MIX_SECOND);
        word ^ (word >> SHIFT_LAST)
    }

    /// Draw a number below the bound, or zero for a zero bound
    pub fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            return 0;
        }
        self.next_word() % bound
    }

    /// Draw true once in every given number of times
    pub fn one_in(&mut self, odds: u64) -> bool {
        self.below(odds) == 0
    }
}
