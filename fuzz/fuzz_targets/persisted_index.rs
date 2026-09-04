//! The checkpoint head, which the open path reads before anything else works
//!
//! This is the parser with the most to lose. It is read at open, off a file a crash
//! may have left half written, and everything after it trusts the counts it returns.
//! The target builds a real head rather than guessing at one, so the mutations land
//! on a structure the parser will actually walk into: pack a volume of the drawn
//! shape, then cut it short, flip bits in it, or hand over the raw case bytes.

#![no_main]

use arbitrary::{Arbitrary, Result, Unstructured};
use libfuzzer_sys::fuzz_target;

use reel::format::column::ColumnId;
use reel::format::loc::SegmentId;
use reel::format::lsn::Lsn;
use reel::index::persisted::{PersistedColumn, PersistedIndex, PersistedSegment};

/// Columns a drawn head names at most, past what any real volume declares
const MAX_COLUMNS: usize = 20;

/// Segments a drawn head names at most, enough that the stamp table is walked
const MAX_SEGMENTS: usize = 64;

/// Bit flips a case lands on one packed head at most
const MAX_WOUNDS: u8 = 8;

/// An optional sequence number that is never `Some(Lsn::NONE)`
///
/// Zero is how a stamp spells absent, so a `Some` holding it is not a value the store
/// can express: it packs as absent and reads back absent, and a roundtrip over one
/// would be failing the writer for a shape no writer produces.
fn optional_lsn(u: &mut Unstructured) -> Result<Option<Lsn>> {
    match bool::arbitrary(u)? {
        true => Ok(Some(Lsn(u64::arbitrary(u)?.max(1)))),
        false => Ok(None),
    }
}

/// A head of the drawn shape, filled from the case rather than from a generator
fn draw(u: &mut Unstructured) -> Result<PersistedIndex> {
    let columns = usize::from(u8::arbitrary(u)?) % (MAX_COLUMNS + 1);
    let segments = usize::from(u8::arbitrary(u)?) % (MAX_SEGMENTS + 1);
    Ok(PersistedIndex {
        at: Lsn(u64::arbitrary(u)?),
        columns: (0..columns)
            .map(|_| {
                Ok(PersistedColumn {
                    column: ColumnId(u8::arbitrary(u)?),
                    key_width: u16::arbitrary(u)?,
                })
            })
            .collect::<Result<_>>()?,
        segments: (0..segments)
            .map(|_| {
                Ok(PersistedSegment {
                    segment: SegmentId(u32::arbitrary(u)?),
                    len: u64::arbitrary(u)?,
                    dead: u64::arbitrary(u)?,
                    held: u64::arbitrary(u)?,
                    held_lsn: optional_lsn(u)?,
                    min_lsn: optional_lsn(u)?,
                })
            })
            .collect::<Result<_>>()?,
    })
}

fuzz_target!(|case: &[u8]| {
    let mut u = Unstructured::new(case);
    let Ok(head) = draw(&mut u) else {
        return;
    };
    let packed = head.pack();

    // What the writer wrote, the reader reads: the roundtrip is the property, and
    // everything below only has to refuse or return.
    assert_eq!(
        PersistedIndex::unpack(&packed).expect("a packed head parses"),
        head,
    );

    // A torn write leaves a prefix, and a prefix must never satisfy the parser.
    let cut = usize::arbitrary(&mut u).unwrap_or(0) % (packed.len() + 1);
    if cut < packed.len() {
        assert!(
            PersistedIndex::unpack(&packed[..cut]).is_err(),
            "a head of {} bytes cut to {cut} parsed",
            packed.len(),
        );
    }

    // A wound the checksum survives leaves a head lying about its own shape, which is
    // the case worth reaching. What comes back is not asserted, only that it comes.
    //
    // Counted rather than drawn until the case runs out: an exhausted `Unstructured`
    // keeps answering, with zeroes, so a loop that waits for it to refuse never ends.
    let wounds = u8::arbitrary(&mut u).unwrap_or(0) % MAX_WOUNDS;
    let mut wounded = packed;
    for _ in 0..wounds {
        if wounded.is_empty() {
            break;
        }
        let site = usize::arbitrary(&mut u).unwrap_or(0) % wounded.len();
        let bit = u8::arbitrary(&mut u).unwrap_or(0) % 8;
        wounded[site] ^= 1 << bit;
        let _ = PersistedIndex::unpack(&wounded);
    }
    let _ = PersistedIndex::unpack(case);
});
