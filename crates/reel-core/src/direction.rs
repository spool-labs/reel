//! The direction a scan walks

/// Bytewise key order, or its reverse
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Direction {
    /// Lowest key first
    Ascending,
    /// Highest key first
    Descending,
}
