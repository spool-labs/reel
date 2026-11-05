//! What a row in the write order does

/// The stored byte of a value row
pub const VALUE_BYTE: u8 = 0;
/// The stored byte of a grave row
pub const GRAVE_BYTE: u8 = 1;
/// The stored byte of a range delete row
pub const RANGE_DELETE_BYTE: u8 = 2;

/// The kind of a row
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RowKind {
    /// A value
    Value,
    /// A grave, with no bytes behind it
    Grave,
    /// A range delete: the row's key starts the range, and its record holds the end
    RangeDelete,
}

impl RowKind {
    /// Read the byte that stores this kind
    pub fn as_byte(self) -> u8 {
        match self {
            RowKind::Value => VALUE_BYTE,
            RowKind::Grave => GRAVE_BYTE,
            RowKind::RangeDelete => RANGE_DELETE_BYTE,
        }
    }

    /// Read a stored kind back
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            VALUE_BYTE => Some(RowKind::Value),
            GRAVE_BYTE => Some(RowKind::Grave),
            RANGE_DELETE_BYTE => Some(RowKind::RangeDelete),
            _unknown => None,
        }
    }
}
