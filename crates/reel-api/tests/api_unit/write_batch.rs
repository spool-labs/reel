use reel_api::column_id::ColumnId;
use reel_api::units::ByteCount;
use reel_api::write_batch::*;

const COLUMN: ColumnId = ColumnId(3);

// entries read back in batch order with their own bytes
#[test]
fn batch_order() {
    let mut batch = WriteBatch::new();

    batch.put(COLUMN, b"key", b"value");
    batch.delete(COLUMN, b"gone");
    batch.delete_range(COLUMN, b"a", b"b");

    assert_eq!(batch.len(), 3);
    assert_eq!(
        batch.operation(0),
        Some(BatchOperation::Put {
            column: COLUMN,
            key: b"key",
            value: b"value",
        })
    );
    assert_eq!(
        batch.operation(1),
        Some(BatchOperation::Delete {
            column: COLUMN,
            key: b"gone",
        })
    );
    assert_eq!(
        batch.operation(2),
        Some(BatchOperation::DeleteRange {
            column: COLUMN,
            start: b"a",
            end: b"b",
        })
    );
    assert_eq!(batch.operation(3), None);
}

// clear empties the batch for reuse
#[test]
fn batch_reuse() {
    let mut batch = WriteBatch::new();
    batch.put(COLUMN, b"key", b"value");

    batch.clear();

    assert!(batch.is_empty());
    assert_eq!(batch.byte_length(), ByteCount(0));
}
