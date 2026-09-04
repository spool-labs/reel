//! Property and corruption sweeps over the on-disk format
//!
//! Every parser in the format layer reads bytes the store did not necessarily write,
//! so it must refuse input rather than trust it and never take the process down
//! doing so. Two properties: anything that packs parses back identical, and nothing
//! panics. The corruption sweep does the same one level up, damaging a real durable
//! image and requiring the store to reopen and agree with itself however much of the
//! image survived. Randomness is seeded through the same generator the fault plans
//! use, so a failure names a seed that reproduces it exactly.

#[allow(dead_code)]
mod harness;

use std::path::PathBuf;
use std::sync::Arc;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use reel::append::codec::{admit, decode};
use reel::format::column::{Codec, ColumnId};
use reel::format::filter::Filter;
use reel::format::footer::{FooterEntry, SegmentFooter};
use reel::format::loc::SegmentId;
use reel::format::lsn::Lsn;
use reel::format::prefix::PrefixRows;
use reel::format::record::{BatchFrame, Flags, RecordHeader, HEADER_LEN};
use reel::format::segment_header::{SegmentHeader, SEGMENT_HEADER_LEN};
use reel::index::column::ColumnMark;
use reel::index::persisted::{PersistedColumn, PersistedIndex, PersistedSegment};
use reel::io::fault::FaultPlan;
use reel::io::sim_backend::{DurableImage, SimIo};
use reel::{ByteCount, Preallocate, RecordKey, ReelConfig, ReelStore, SyncPolicy, ThreadBudget};

use harness::observe::observe;
use harness::op_stream::StreamOp;
use harness::reel_harness::{assert_recount, ReelHarness};
use harness::wire::{
    apply_mutation, record_key, BLOB, ID_LEN, RECORDS, RECORD_KEY_LEN, TEST_COLUMNS,
};

/// Seeds every sweep draws its corpus from
const SEEDS: &[u64] = &[1, 7, 42, 1337];

/// Byte strings each parser is offered per seed
const CASES: usize = 400;

/// Longest random byte string a parser is offered
const MAX_CASE_LEN: usize = 512;

/// Every codec the format names, for the sweeps that must hold under each byte
///
/// One today. The sweeps read it rather than naming lz4, so a codec added behind the
/// byte is swept from the moment it exists rather than whenever somebody remembers.
const CODECS: &[Codec] = &[Codec::Lz4];

/// Longest payload the codec sweeps admit, well past the floor admission refuses under
const MAX_PAYLOAD_LEN: usize = 2048;

/// Damaged copies of the image each corruption seed produces
const IMAGES_PER_SEED: usize = 6;

/// Damage sites applied to one image
const WOUNDS: usize = 8;

/// Group the corruption fixture writes into
const GROUP: u16 = 7;

/// Records the corruption fixture writes
const FIXTURE_KEYS: u8 = 10;

/// Payload length each fixture record carries, wide enough to span blocks
const FIXTURE_LEN: usize = 5_000;

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(32 * 1024),
        alloc_chunk: ByteCount::from_bytes(8 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::EveryPut,
        active_tails: ThreadBudget::threads(1),
        ..ReelConfig::default()
    }
}

fn random_bytes(rng: &mut SmallRng, max: usize) -> Vec<u8> {
    let len = rng.gen_range(0..=max);
    (0..len).map(|_| rng.gen()).collect()
}

// no byte string, however malformed, takes a parser down
//
// Length prefixes and counts are the sharp part: a parser that trusts one indexes
// past its buffer. Every parser the crate exposes to bytes it did not write belongs
// here, not just the three the segment is made of: the checkpoint head is read at
// open, and a row block, a mark and a frame are each read off bytes a sweep found.
#[test]
fn parsers_never_panic_on_arbitrary_bytes() {
    for seed in SEEDS {
        let mut rng = SmallRng::seed_from_u64(*seed);
        for _ in 0..CASES {
            let bytes = random_bytes(&mut rng, MAX_CASE_LEN);

            let _ = RecordHeader::unpack(&bytes);
            let _ = SegmentFooter::parse(&bytes);
            let _ = SegmentHeader::unpack(&bytes);
            let _ = PersistedIndex::unpack(&bytes);
            let _ = PrefixRows::decode(&bytes);
            let _ = ColumnMark::unpack(&bytes);
            let _ = Filter::parse_region(&bytes, rng.gen_range(0..8));
            for codec in CODECS {
                let _ = decode(codec.as_byte(), &bytes);
            }

            // a frame reads a header beside its payload, so it is offered both: a
            // header these bytes really parse to where they do, and one packed to
            // claim a frame where they do not, since a random string almost never
            // carries the flag that gets past the first check.
            if let Ok(header) = RecordHeader::unpack(&bytes) {
                let _ = BatchFrame::unpack(&header, &bytes);
            }
            let declared = BatchFrame {
                count: rng.gen(),
                span: rng.gen(),
            };
            let _ = BatchFrame::unpack(&declared.header(), &bytes);
        }
    }
}

/// A checkpoint head standing for a volume of the drawn shape
///
/// Packed by the same writer the store uses, so what the sweeps below damage is a
/// real file rather than a guess at one.
fn persisted_index(rng: &mut SmallRng, columns: usize, segments: usize) -> PersistedIndex {
    PersistedIndex {
        at: Lsn(rng.gen()),
        columns: (0..columns)
            .map(|at| PersistedColumn {
                column: ColumnId(at as u8),
                key_width: rng.gen(),
            })
            .collect(),
        segments: (0..segments)
            .map(|at| PersistedSegment {
                segment: SegmentId(at as u32),
                len: rng.gen(),
                dead: rng.gen(),
                held: rng.gen(),
                held_lsn: rng.gen::<bool>().then(|| Lsn(rng.gen())),
                min_lsn: rng.gen::<bool>().then(|| Lsn(rng.gen())),
            })
            .collect(),
    }
}

// a checkpoint head cut short at any byte refuses rather than indexes past its end
//
// The random sweep above never gets through the magic, so it proves the front door
// and nothing behind it. A real head cut at every length walks the whole parser:
// the counts it reads are genuine, and every slice it takes off them is against a
// buffer that stops early. A short file is what a torn write leaves, so this is the
// damage the open path actually meets.
#[test]
fn a_truncated_checkpoint_head_refuses() {
    for seed in SEEDS {
        let mut rng = SmallRng::seed_from_u64(*seed);
        for shape in [(0usize, 0usize), (1, 1), (4, 9), (17, 40)] {
            let packed = persisted_index(&mut rng, shape.0, shape.1).pack();

            for cut in 0..packed.len() {
                assert!(
                    PersistedIndex::unpack(&packed[..cut]).is_err(),
                    "a head of {} bytes cut to {cut} parsed",
                    packed.len(),
                );
            }
            let whole = PersistedIndex::unpack(&packed).expect("a packed head parses");
            assert_eq!(whole.columns.len(), shape.0);
            assert_eq!(whole.segments.len(), shape.1);
        }
    }
}

// a wounded checkpoint head refuses or parses, and never takes the open path down
//
// A flipped bit is usually the checksum's to catch. The ones that are not are the
// point: a wound in the trailing bytes past what the counts claim, or one the crc
// happens to survive, leaves a head that lies about its own shape.
#[test]
fn a_wounded_checkpoint_head_never_panics() {
    for seed in SEEDS {
        let mut rng = SmallRng::seed_from_u64(*seed);
        for _ in 0..CASES {
            let columns = rng.gen_range(0..8);
            let segments = rng.gen_range(0..24);
            let mut packed = persisted_index(&mut rng, columns, segments).pack();

            for _ in 0..rng.gen_range(1..=4) {
                let at = rng.gen_range(0..packed.len());
                packed[at] ^= 1 << rng.gen_range(0..8);
            }
            let _ = PersistedIndex::unpack(&packed);
        }
    }
}

/// A payload as compressible as the draw makes it, from one repeated byte to a random
/// fill, so admission's keeps and both of its refusals all get drawn
fn payload_bytes(rng: &mut SmallRng, len: usize) -> Vec<u8> {
    let alphabet = rng.gen_range(1..=256usize);
    (0..len).map(|_| rng.gen_range(0..alphabet) as u8).collect()
}

// what admission stored, decode gives back exactly
//
// Lengths straddle the floor admission never tries under and the inline ceiling it
// refuses at, so a run covers both refusals beside the keeps rather than assuming
// them. A refusal must hand back the payload it was given, untouched.
#[test]
fn codec_payloads_roundtrip() {
    for seed in SEEDS {
        let mut rng = SmallRng::seed_from_u64(*seed);
        for _ in 0..CASES {
            let len = rng.gen_range(0..=MAX_PAYLOAD_LEN);
            let logical = payload_bytes(&mut rng, len);
            let inline_max = rng.gen_range(0..=128u16);
            for codec in CODECS {
                let (stored, byte) = admit(*codec, inline_max, logical.clone());
                match byte {
                    0 => assert_eq!(stored, logical, "a refused payload was not handed back"),
                    _ => assert_eq!(
                        decode(byte, &stored).expect("bytes admission kept must decode"),
                        logical,
                        "{codec:?} roundtrip at {} bytes",
                        logical.len()
                    ),
                }
            }
        }
    }
}

// a damaged stored payload refuses or returns bytes, and never takes the process down
//
// Random bytes almost never survive the logical length prefix, so the sweep that
// reaches a decoder starts from bytes a codec really wrote and wounds them: that is
// the shape corruption behind a checksum that happened to pass actually takes. What
// comes back is not asserted, since the block format carries no checksum of its own
// and a wounded frame may well decode to different bytes of the right length. The
// record checksum is what catches that, one level up.
#[test]
fn wounded_codec_payloads_never_panic() {
    for seed in SEEDS {
        let mut rng = SmallRng::seed_from_u64(*seed);
        for _ in 0..CASES {
            let len = rng.gen_range(MAX_CASE_LEN..=MAX_PAYLOAD_LEN);
            let logical = payload_bytes(&mut rng, len);
            for codec in CODECS {
                let (stored, byte) = admit(*codec, 0, logical.clone());
                if byte == 0 {
                    continue;
                }
                let mut wounded = stored.clone();
                for _ in 0..rng.gen_range(1..=4) {
                    let at = rng.gen_range(0..wounded.len());
                    wounded[at] ^= 1 << rng.gen_range(0..8);
                }
                let _ = decode(byte, &wounded);
                let _ = decode(byte, &wounded[..rng.gen_range(0..=wounded.len())]);
                let _ = decode(rng.gen(), &stored);
            }
        }
    }
}

/// A random key in one of the columns, at that column's own width
fn random_key(rng: &mut SmallRng) -> RecordKey {
    let (column, width) = match rng.gen_range(0..2u8) {
        0 => (RECORDS, RECORD_KEY_LEN),
        _ => (BLOB, ID_LEN),
    };
    let bytes: Vec<u8> = (0..width).map(|_| rng.gen()).collect();
    RecordKey::from_bytes(column, &bytes).expect("key")
}

// a packed record header parses back to exactly what packed it
#[test]
fn record_header_roundtrips() {
    for seed in SEEDS {
        let mut rng = SmallRng::seed_from_u64(*seed);
        for _ in 0..CASES {
            let key = random_key(&mut rng);
            let payload = random_bytes(&mut rng, MAX_CASE_LEN);
            let header = RecordHeader::data(key, Lsn(rng.gen()), &payload);

            let parsed =
                RecordHeader::unpack(header.pack().as_slice()).expect("a packed header parses");

            assert_eq!(parsed, header);
            assert!(
                parsed.verify(&payload),
                "a roundtripped header rejects its own payload"
            );
        }
    }
}

// a packed footer parses back to exactly what packed it, at every entry count
#[test]
fn footer_roundtrips() {
    for seed in SEEDS {
        let mut rng = SmallRng::seed_from_u64(*seed);
        for _ in 0..CASES / 8 {
            let count = rng.gen_range(0..24usize);
            let entries: Vec<FooterEntry> = (0..count)
                .map(|_| {
                    let key = random_key(&mut rng);
                    let len: u32 = rng.gen();
                    let flags = if len == 0 {
                        Flags::TOMBSTONE
                    } else {
                        Flags::DATA
                    };
                    FooterEntry::new(key, Lsn(rng.gen()), rng.gen(), len, flags)
                })
                .collect();
            let mut footer = SegmentFooter::build(entries);

            let packed = footer.pack(0).expect("pack");
            let parsed = SegmentFooter::parse(&packed).expect("a packed footer parses");

            assert_eq!(parsed, footer);
        }
    }
}

// a packed segment header parses back to exactly what packed it
#[test]
fn segment_header_roundtrips() {
    for seed in SEEDS {
        let mut rng = SmallRng::seed_from_u64(*seed);
        for _ in 0..CASES {
            let header = SegmentHeader {
                version: rng.gen(),
                segment: SegmentId(rng.gen()),
            };

            let parsed = SegmentHeader::unpack(&header.pack()).expect("a packed header parses");

            assert_eq!(parsed, header);
        }
    }
}

// a single flipped bit anywhere in a record is caught by its checksum
#[test]
fn every_record_bit_flip_is_caught() {
    let key = record_key(GROUP, [0x5a; ID_LEN]);
    let payload: Vec<u8> = (0..300u32).map(|byte| byte as u8).collect();
    let header = RecordHeader::data(key, Lsn(9), &payload);
    let mut record = header.pack().as_slice().to_vec();
    record.extend_from_slice(&payload);

    for at in 0..record.len() {
        for bit in 0..8u8 {
            let mut torn = record.clone();
            torn[at] ^= 1 << bit;
            let parsed = match RecordHeader::unpack(&torn) {
                Ok(parsed) => parsed,
                Err(_) => continue,
            };
            if parsed == header && torn[HEADER_LEN..] == payload[..] {
                continue;
            }
            assert!(
                !parsed.verify(&torn[HEADER_LEN..]),
                "a flipped bit at {at}:{bit} passed the checksum"
            );
        }
    }
}

/// A durable image of a volume holding a few multi block records
fn fixture_image() -> DurableImage {
    let sim = SimIo::new(FaultPlan::new(1));
    let store = ReelStore::open_with_io(
        PathBuf::from("/bulk"),
        config(),
        TEST_COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("open");
    for byte in 1..=FIXTURE_KEYS {
        let op = StreamOp::Put {
            group: GROUP,
            address: byte,
            len: FIXTURE_LEN,
            fill: byte,
        };
        apply_mutation(&store, &op).expect("fixture put");
    }
    store.flush().expect("flush");
    drop(store);
    sim.durable_image()
}

/// Damage an image the ways a device does: flipped bytes, zeroed runs, truncation
fn wound(image: &mut DurableImage, rng: &mut SmallRng) {
    for _ in 0..WOUNDS {
        if image.is_empty() {
            return;
        }
        let file = rng.gen_range(0..image.len());
        let bytes = &mut image[file].1;
        if bytes.is_empty() {
            continue;
        }
        let at = rng.gen_range(0..bytes.len());
        match rng.gen_range(0..3u8) {
            0 => bytes[at] ^= 1 << rng.gen_range(0..8u8),
            1 => {
                let end = (at + rng.gen_range(1..600usize)).min(bytes.len());
                bytes[at..end].fill(0);
            }
            _ => bytes.truncate(at),
        }
    }
}

// a store reopens and agrees with itself however its image was damaged
//
// What survives is not fixed, since the damage is arbitrary: opening at all, never
// panicking, and counters that match a scan of what it serves are.
#[test]
fn a_damaged_image_reopens_consistent() {
    let harness = ReelHarness::new(config());
    let clean = fixture_image();

    for seed in SEEDS {
        let mut rng = SmallRng::seed_from_u64(*seed);
        for round in 0..IMAGES_PER_SEED {
            let mut damaged = clean.clone();
            wound(&mut damaged, &mut rng);

            let reopened = harness.reopen(damaged);

            assert_recount(&reopened, *seed);
            for (_, value) in observe(&reopened).records {
                assert!(
                    !value.is_empty(),
                    "seed {seed} round {round} served an empty record"
                );
            }
        }
    }
}

// the frozen segment header payload keeps its width, since every file starts with it
#[test]
fn segment_header_width_is_frozen() {
    assert_eq!(
        SegmentHeader::new(SegmentId(1)).pack().len(),
        SEGMENT_HEADER_LEN
    );
}
