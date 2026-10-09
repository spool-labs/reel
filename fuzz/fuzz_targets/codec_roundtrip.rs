//! Admission round-trips what it keeps and returns what it refuses untouched
//! Decode must refuse or return on any bytes at all

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

use reel::append::codec::{admit, decode};
use reel::format::column::Codec;

/// Every codec in the format, so a new one is fuzzed as soon as it is listed
const CODECS: &[Codec] = &[Codec::Lz4];

/// One admission the case asks for
#[derive(Arbitrary, Debug)]
struct Case<'a> {
    /// Index into `CODECS` for the column's codec
    codec: u8,

    /// The bytes offered to admission
    payload: &'a [u8],
}

fuzz_target!(|case: Case| {
    let codec = CODECS[usize::from(case.codec) % CODECS.len()];
    let logical = case.payload.to_vec();
    let (stored, byte) = admit(codec, logical.clone());

    match byte {
        0 => assert_eq!(stored, logical, "a refused payload was not handed back"),
        _ => {
            assert_eq!(byte, codec.as_byte(), "a keep stamped another codec's byte");
            assert_eq!(
                decode(byte, &stored).expect("bytes admission kept must decode"),
                logical,
                "a kept payload of {} bytes did not come back",
                logical.len(),
            );
        }
    }

    // Decode the stored form under every codec byte, as a crossed byte is how corruption looks
    for other in [0u8, Codec::Lz4.as_byte(), 0xff] {
        let _ = decode(other, &stored);
    }
    let _ = decode(byte, case.payload);
});
