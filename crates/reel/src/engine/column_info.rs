//! What the engine keeps about each declared column

use reel_api::units::VolumeId;

use crate::config::KeyShape;
use crate::record::codec::Codec;
use crate::segment::row_block::{COMPLETE_BLOCK_BYTES, ROW_BLOCK_BYTES};

/// A column as the read and write paths need it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnInfo {
    pub key_shape: KeyShape,
    pub codec: Codec,
    pub volume: VolumeId,
    pub is_complete: bool,
}

impl ColumnInfo {
    /// Read how big the column's row blocks can get
    pub fn block_bytes(&self) -> usize {
        if self.is_complete { COMPLETE_BLOCK_BYTES } else { ROW_BLOCK_BYTES }
    }
}
