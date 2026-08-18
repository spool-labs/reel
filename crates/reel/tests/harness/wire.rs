//! The columns the harness opens, the wire keys and values, and the mutation applier
//!
//! A record key is a group big endian then a thirty two byte id, and a value is
//! verbatim. Building the same bytes for every backend and applying each mutation
//! through the trait is what makes the reel and the memory oracle comparable.

use reel::{Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, RecordKey};
use reel_core::{Result as StoreResult, Store, WriteBatch};

use crate::harness::op_stream::StreamOp;

/// Bytes a wire record key occupies, a group big endian then an id
pub const RECORD_KEY_LEN: usize = GROUP_PREFIX_LEN + ID_LEN;

/// Bytes the group takes at the front of a record key
pub const GROUP_PREFIX_LEN: usize = 2;

/// Bytes an id occupies at the tail of a record key
pub const ID_LEN: usize = 32;

/// Bytes of length framing a stored value carries ahead of its payload
const VALUE_FRAME_LEN: usize = 0;

/// The record column, which every generated stream writes into
pub const RECORDS: ColumnId = ColumnId(1);

/// The blob column, wide payloads addressed by a bare id
pub const BLOB: ColumnId = ColumnId(2);

/// Family name the record column is addressed by through the store trait
pub const RECORDS_CF: &str = "records";

/// Family name the blob column is addressed by
pub const BLOB_CF: &str = "blob_data";

/// The columns every harness store is opened with
///
/// A column shards as far as its leading bytes let it: records lead with their group,
/// blobs lead with a uniform id that spreads on its own and take one shard. Neither
/// inlines, since both hold payloads past what an index entry can carry.
pub const TEST_COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: RECORDS,
        name: RECORDS_CF,
        key_width: KeyWidth::Fixed(RECORD_KEY_LEN as u16),
        shard_bytes: GROUP_PREFIX_LEN as u8,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
    ColumnSpec {
        id: BLOB,
        name: BLOB_CF,
        key_width: KeyWidth::Fixed(ID_LEN as u16),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
];

/// The record column key for a group and an id
///
/// Two big endian group bytes then the id, so the key space sorts by group.
pub fn record_key(group: u16, id: [u8; ID_LEN]) -> RecordKey {
    let mut bytes = [0u8; RECORD_KEY_LEN];
    bytes[..GROUP_PREFIX_LEN].copy_from_slice(&group.to_be_bytes());
    bytes[GROUP_PREFIX_LEN..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).unwrap_or_else(|_| RecordKey::none())
}

/// Build the wire record key for a group number and an address byte
pub fn wire_key(group: u16, address: u8) -> Vec<u8> {
    let mut key = Vec::with_capacity(RECORD_KEY_LEN);
    key.extend_from_slice(&group.to_be_bytes());
    key.extend_from_slice(&[address; ID_LEN]);
    key
}

/// Build a framed value, an eight byte length then the payload nonce bytes
pub fn framed_value(len: usize, fill: u8) -> Vec<u8> {
    let mut value = (len as u64).to_le_bytes().to_vec();
    value.resize(VALUE_FRAME_LEN + len, fill);
    value
}

/// The two byte big endian prefix that opens a group's key range
pub fn group_prefix(group: u16) -> Vec<u8> {
    group.to_be_bytes().to_vec()
}

/// Apply one mutation to a store through the trait
///
/// A reopen and the ordered read ops are handled by the fixture and are a no op here.
pub fn apply_mutation<Backend: Store>(store: &Backend, op: &StreamOp) -> StoreResult<()> {
    match op {
        StreamOp::Put {
            group,
            address,
            len,
            fill,
        }
        | StreamOp::Overwrite {
            group,
            address,
            len,
            fill,
        } => store.write_batch(write_batch(*group, *address, *len, *fill)),
        StreamOp::Delete { group, address } => store.write_batch(delete_batch(*group, *address)),
        StreamOp::DropGroup { group } => drop_group(store, *group),
        StreamOp::DeleteRange { group, lo, hi } => delete_subrange(store, *group, *lo, *hi),
        StreamOp::Reopen
        | StreamOp::IterFrom { .. }
        | StreamOp::IterRange { .. }
        | StreamOp::IterKeysPrefix { .. } => Ok(()),
    }
}

fn write_batch(group: u16, address: u8, len: usize, fill: u8) -> WriteBatch {
    let key = wire_key(group, address);
    let mut batch = WriteBatch::new();
    batch.put(RECORDS_CF, &key, &framed_value(len, fill));
    batch
}

fn delete_batch(group: u16, address: u8) -> WriteBatch {
    let key = wire_key(group, address);
    let mut batch = WriteBatch::new();
    batch.delete(RECORDS_CF, &key);
    batch
}

fn drop_group<Backend: Store>(store: &Backend, group: u16) -> StoreResult<()> {
    let start = group_prefix(group);
    let end = group_prefix(group + 1);
    store.delete_range(RECORDS_CF, &start, &end)
}

fn delete_subrange<Backend: Store>(store: &Backend, group: u16, lo: u8, hi: u8) -> StoreResult<()> {
    let start = wire_key(group, lo);
    let end = wire_key(group, hi);
    store.delete_range(RECORDS_CF, &start, &end)
}
