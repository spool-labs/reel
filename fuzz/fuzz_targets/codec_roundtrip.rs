//! Admission and decode, as a contract rather than as two functions
//!
//! Three things must hold whatever the payload is. What admission kept must decode
//! back to exactly the bytes it was given. What admission refused must be handed back
//! untouched, since the caller stores whatever it returns. And decode must refuse or
//! return on any byte string at all, because the bytes it is handed at read time are
//! only as trustworthy as the checksum that let them through.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

use reel::append::codec::{admit, decode};
use reel::format::column::Codec;

/// Every codec the format names, drawn from so a second one is fuzzed on arrival
const CODECS: &[Codec] = &[Codec::Lz4];

/// One admission the case asks for
#[derive(Arbitrary, Debug)]
struct Case<'a> {
    /// Which of the codecs the column would have named
    codec: u8,

    /// The column's inline ceiling, which a small enough stored form is refused under
    inline_max: u16,

    /// The bytes offered to admission
    payload: &'a [u8],
}

fuzz_target!(|case: Case| {
    let codec = CODECS[usize::from(case.codec) % CODECS.len()];
    let logical = case.payload.to_vec();
    let (stored, byte) = admit(codec, case.inline_max, logical.clone());

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

    // The stored form under every byte, including the one that did not write it: a
    // crossed byte is what a corrupt header looks like, and it must read as damage.
    for other in [0u8, Codec::Lz4.as_byte(), 0xff] {
        let _ = decode(other, &stored);
    }
    let _ = decode(byte, case.payload);
});
