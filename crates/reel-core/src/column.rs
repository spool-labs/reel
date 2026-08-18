//! Column family trait for defining typed columns

use wincode::{SchemaRead, SchemaWrite};

/// Trait for defining a typed column family
pub trait Column {
    /// Column family name, unique across every column of one store
    const CF_NAME: &'static str;

    /// Key type, serialized with wincode
    type Key: for<'de> SchemaRead<'de, Dst = Self::Key> + SchemaWrite<Src = Self::Key>;

    /// Value type, serialized with wincode
    type Value: for<'de> SchemaRead<'de, Dst = Self::Value> + SchemaWrite<Src = Self::Value>;
}
