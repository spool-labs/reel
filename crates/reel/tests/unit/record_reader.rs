use reel::record::chunk_table::table_bytes;
use reel::record::codec::CHUNK_BYTES;
use reel::record::record_head::{chunk_count, head_bytes_for};
use reel::record::record_reader::*;
use reel_api::units::ByteCount;

// the split comes back from the record length for every value size
#[test]
fn shape_math() {
    for value in [CHUNK_BYTES + 1, 2 * CHUNK_BYTES, 16 * 1024 * 1024 + 3, 300 * 1024 * 1024] {
        let value_length = ByteCount(value as u64);
        let chunks = chunk_count(value_length);
        let front = head_bytes_for(chunks) as u64 + table_bytes(chunks).0;

        let shape = written_shape(ByteCount(front + value as u64)).expect("shape");

        assert_eq!(shape.value_length, value_length);
        assert_eq!(shape.front_length as u64, front);
    }
}
