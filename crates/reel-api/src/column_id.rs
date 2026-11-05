//! A column's id

/// One of up to 256 columns in a store
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ColumnId(pub u8);
