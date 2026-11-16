use reel::record::codec::Codec;
use reel::record::record_head::*;
use reel_core::units::ByteCount;

// both codecs and both shapes survive the form byte, and an unknown byte is refused
#[test]
fn form_round() {
    for codec in [Codec::None, Codec::Lz4] {
        for is_chunked in [false, true] {
            let form = RecordForm { codec, is_chunked };
            assert_eq!(RecordForm::from_byte(form.as_byte()), Some(form));
        }
    }
    assert_eq!(RecordForm::from_byte(0x7F), None);
}

// one page's head stays small, and a value past the page adds one sum
#[test]
fn head_sizes() {
    let one_page = ByteCount(64 * 1024 * 1024);
    let two_pages = ByteCount(300 * 1024 * 1024);

    assert_eq!(chunked_head_bytes(one_page), PAGE_SUMS_AT + PAGE_SUM_BYTES);
    assert_eq!(chunked_head_bytes(two_pages), PAGE_SUMS_AT + 2 * PAGE_SUM_BYTES);
}
