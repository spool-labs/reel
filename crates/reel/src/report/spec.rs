//! Parses the volume and column spec strings so every frontend uses one spelling

use std::path::PathBuf;

use thiserror::Error;

use crate::config::{VolumeClass, VolumeSpec};
use crate::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth};

/// Result of reading one spec string
pub type SpecResult<T> = std::result::Result<T, SpecError>;

/// A spec string the grammar does not accept
#[derive(Debug, Error)]
pub enum SpecError {
    /// Nothing before the first colon of a volume spec
    #[error("a volume spec needs a path")]
    VolumePath,

    /// An unknown volume tag
    #[error("volume tag `{0}` is not fast, capacity, or dead")]
    VolumeTag(String),

    /// A column spec that is not two or three colon separated fields
    #[error("column spec `{0}` is not NAME:ID or NAME:ID:WIDTH")]
    ColumnShape(String),

    /// Nothing before the first colon of a column spec
    #[error("a column spec needs a name")]
    ColumnName,

    /// A column identifier that is not a byte
    #[error("column identifier `{0}` is not a byte")]
    ColumnId(String),

    /// A key width that is not a number of bytes
    #[error("column width `{0}` is not a key width")]
    ColumnWidth(String),
}

/// Read a list of volume specs, in the order the set was written
pub fn volumes<Spec: AsRef<str>>(specs: &[Spec]) -> SpecResult<Vec<VolumeSpec>> {
    let mut parsed = Vec::with_capacity(specs.len());
    for spec in specs {
        parsed.push(volume(spec.as_ref())?);
    }
    Ok(parsed)
}

/// Read one volume spec, a path followed by colon-separated tags
pub fn volume(spec: &str) -> SpecResult<VolumeSpec> {
    let mut parts = spec.split(':');
    let path = parts.next().unwrap_or_default();
    if path.is_empty() {
        return Err(SpecError::VolumePath);
    }

    let mut volume = VolumeSpec::fast(PathBuf::from(path));
    for tag in parts {
        match tag {
            "fast" => volume.class = VolumeClass::Fast,
            "capacity" => volume.class = VolumeClass::Capacity,
            "dead" => volume.dead = true,
            other => return Err(SpecError::VolumeTag(other.to_string())),
        }
    }
    Ok(volume)
}

/// Read a list of column specs into a leaked set that lives as long as the process
pub fn columns<Spec: AsRef<str>>(specs: &[Spec]) -> SpecResult<ColumnSet> {
    let mut parsed = Vec::with_capacity(specs.len());
    for spec in specs {
        parsed.push(column(spec.as_ref())?);
    }
    Ok(Vec::leak(parsed))
}

/// Read one `NAME:ID[:WIDTH]` column spec into the plainest column that holds the keys
pub fn column(spec: &str) -> SpecResult<ColumnSpec> {
    let parts: Vec<&str> = spec.split(':').collect();
    let (name, id, width) = match parts[..] {
        [name, id] => (name, id, None),
        [name, id, width] => (name, id, Some(width)),
        _ => return Err(SpecError::ColumnShape(spec.to_string())),
    };
    if name.is_empty() {
        return Err(SpecError::ColumnName);
    }

    let id: u8 = id
        .parse()
        .map_err(|_| SpecError::ColumnId(id.to_string()))?;
    let key_width = match width {
        None => KeyWidth::Variable,
        Some(width) => KeyWidth::Fixed(
            width
                .parse()
                .map_err(|_| SpecError::ColumnWidth(width.to_string()))?,
        ),
    };

    Ok(ColumnSpec {
        id: ColumnId(id),
        name: String::leak(name.to_string()),
        key_width,
        shard_bytes: 0,
        purge_mark: None,
        codec: Codec::None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // a volume spec reads its path and its tags
    #[test]
    fn volume_specs() {
        let plain = volume("/mnt/one").expect("plain");
        assert_eq!(plain.path, PathBuf::from("/mnt/one"));
        assert_eq!(plain.class, VolumeClass::Fast);
        assert!(!plain.dead);

        let tagged = volume("/mnt/two:capacity:dead").expect("tagged");
        assert_eq!(tagged.class, VolumeClass::Capacity);
        assert!(tagged.dead);

        assert!(volume("/mnt/three:warm").is_err());
        assert!(volume("").is_err());
    }

    // a column spec reads its name, identifier and key width
    #[test]
    fn column_specs() {
        let varying = column("records:3").expect("varying");
        assert_eq!(varying.name, "records");
        assert_eq!(varying.id, ColumnId(3));
        assert_eq!(varying.key_width, KeyWidth::Variable);

        let fixed = column("records:3:32").expect("fixed");
        assert_eq!(fixed.key_width, KeyWidth::Fixed(32));

        assert!(column("records").is_err());
        assert!(column("records:wide").is_err());
        assert!(column("records:3:wide").is_err());
    }

    // a list keeps the order it was given in
    #[test]
    fn spec_lists() {
        let set = volumes(&["/mnt/one", "/mnt/two:dead"]).expect("volumes");
        assert_eq!(set.len(), 2);
        assert!(!set[0].dead);
        assert!(set[1].dead);

        let declared = columns(&["records:3", "blobs:4:32"]).expect("columns");
        assert_eq!(declared.len(), 2);
        assert_eq!(declared[1].id, ColumnId(4));
    }
}
