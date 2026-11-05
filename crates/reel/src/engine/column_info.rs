//! What the engine keeps about each declared column

use reel_api::units::VolumeId;

use crate::config::KeyShape;
use crate::record::codec::Codec;

/// A column as the read and write paths need it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnInfo {
    pub key_shape: KeyShape,
    pub codec: Codec,
    pub volume: VolumeId,
}
