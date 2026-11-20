use reel::record::chunk_table::table_bytes;
use reel::record::codec::CHUNK_BYTES;
use reel::record::record_head::{PIECE_BYTES, chunk_count, head_bytes_for, plain_length, sums_bytes};
use reel::record::record_reader::*;
use reel_core::units::ByteCount;

// the split comes back from the record length for every value size, at each chunk and piece edge
#[test]
fn shape_math() {
    let mut values = vec![16 * 1024 * 1024 + 3, 64 * 1024 * 1024, 300 * 1024 * 1024];
    for chunks in [1, 2, 7] {
        for piece in [0, 1, PIECE_BYTES - 1, PIECE_BYTES, PIECE_BYTES + 1, CHUNK_BYTES - 1] {
            values.push(chunks * CHUNK_BYTES + piece + 1);
        }
    }
    for value in values {
        let value_length = ByteCount(value as u64);
        let chunks = chunk_count(value_length);
        let sums_at = head_bytes_for(chunks) as u64 + table_bytes(chunks).0;
        let front = sums_at + sums_bytes(value_length);

        let shape = written_shape(ByteCount(front + value as u64)).expect("shape");

        assert_eq!(shape.value_length, value_length, "{value} bytes");
        assert_eq!(shape.sums_at as u64, sums_at, "{value} bytes");
        assert_eq!(shape.front_length as u64, front, "{value} bytes");
        assert_eq!(plain_length(value_length).0, front + value as u64, "{value} bytes");
    }
}

// a buffer sized for a read with no fill keeps its length only once the read says it wrote every byte, and is empty when
// dropped before, as a failed read or a dropped read future leaves it
#[test]
fn an_unfilled_read_clears_unless_filled() {
    let mut buf = vec![7_u8; 3];
    drop(reel::record::record_reader::UnfilledRead::new(&mut buf, 4_096));
    assert!(buf.is_empty(), "a dropped read left {} bytes", buf.len());

    let mut unfilled = reel::record::record_reader::UnfilledRead::new(&mut buf, 4_096);
    unfilled.buf().fill(9);
    unfilled.filled();
    assert_eq!(buf.len(), 4_096);
    assert!(buf.iter().all(|byte| *byte == 9));
}
