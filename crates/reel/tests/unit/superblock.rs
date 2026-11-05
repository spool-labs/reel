use reel::config::KeyShape;
use reel::record::record_check::RecordKey;
use reel::segment::segment_tail::SegmentNumber;
use reel::superblock::*;
use reel_api::column_id::ColumnId;

fn superblock() -> Superblock {
    Superblock {
        store_id: StoreId(5),
        record_key: RecordKey(6),
        revision: 2,
        next_segment: SegmentNumber(700),
        volumes: vec![
            StoredVolume {
                name: "meta".to_string(),
                roots: vec!["/mnt/nvme".to_string()],
            },
            StoredVolume {
                name: "bulk".to_string(),
                roots: vec!["/mnt/one".to_string(), "/mnt/two".to_string()],
            },
        ],
        columns: vec![
            StoredColumn {
                id: ColumnId(1),
                name: "track".to_string(),
                key_shape: KeyShape::Fixed { width: 32 },
                volume_name: "meta".to_string(),
            },
            StoredColumn {
                id: ColumnId(2),
                name: "slice".to_string(),
                key_shape: KeyShape::Variable { most: 98 },
                volume_name: "bulk".to_string(),
            },
        ],
        dead_roots: vec!["/mnt/gone".to_string()],
    }
}

// a superblock reads back field for field
#[test]
fn superblock_round() {
    let mut bytes = Vec::new();
    superblock().write_bytes(&mut bytes);

    assert_eq!(Superblock::from_bytes(&bytes), Some(superblock()));
}

// a flipped byte and a cut file are refused
#[test]
fn superblock_rot() {
    let mut bytes = Vec::new();
    superblock().write_bytes(&mut bytes);

    assert_eq!(Superblock::from_bytes(&bytes[..bytes.len() - 1]), None);
    bytes[12] ^= 1;
    assert_eq!(Superblock::from_bytes(&bytes), None);
}
