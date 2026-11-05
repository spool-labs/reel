use reel::segment::varint::*;

// numbers of every width read back with the bytes they took
#[test]
fn varint_round() {
    for number in [0, 1, 127, 128, 300, 1 << 32, u64::MAX] {
        let mut bytes = [0; MAX_VARINT_BYTES];

        let used = write_varint(&mut bytes, number).expect("fits");

        assert_eq!(read_varint(&bytes[..used]), Some((number, used)));
    }
}

// a cut number and a full buffer both report nothing
#[test]
fn varint_edges() {
    let mut bytes = [0; MAX_VARINT_BYTES];
    let used = write_varint(&mut bytes, u64::MAX).expect("fits");

    assert_eq!(read_varint(&bytes[..used - 1]), None);
    assert_eq!(write_varint(&mut bytes[..1], 300), None);
}
