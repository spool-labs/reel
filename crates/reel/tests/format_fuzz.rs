//! Property and corruption sweeps over the on-disk format
//! Anything that packs parses back identical, and no parser panics on any bytes

#[allow(dead_code)]
mod harness;

use std::path::PathBuf;
use std::sync::Arc;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use reel::append::codec::{admit, decode};
use reel::format::column::Codec;
use reel::format::filter::Filter;
use reel::format::footer::{FooterEntry, SegmentFooter};
use reel::format::journal::read_groups;
use reel::format::loc::SegmentId;
use reel::format::lsn::Lsn;
use reel::format::prefix::{unpack, PrefixRows, Tail};
use reel::format::record::{CheckKey, Flags, RecordHeader, RecordLayout, HEADER_LEN};
use reel::format::segment_header::{SegmentHeader, SEGMENT_HEADER_LEN, SEGMENT_HEADER_SPAN};
use reel::index::column::ColumnMark;
use reel::io::fault::FaultPlan;
use reel::io::sim_backend::{DurableImage, SimIo};
use reel::{ByteCount, RecordKey, ReelConfig, ReelStore, SyncPolicy, ThreadBudget};

use harness::observe::observe;
use harness::op_stream::StreamOp;
use harness::reel_harness::{assert_recount, ReelHarness};
use harness::wire::{
    apply_mutation, record_key, BLOB, ID_LEN, RECORDS, RECORD_KEY_LEN, TEST_COLUMNS,
};

/// Every sweep draws its corpus from these seeds
const SEEDS: &[u64] = &[1, 7, 42, 1337];

/// Each parser gets this many byte strings per seed
const CASES: usize = 400;

/// The longest random byte string a parser gets
const MAX_CASE_LEN: usize = 512;

/// Every codec the format has, so a new codec is swept as soon as it exists
const CODECS: &[Codec] = &[Codec::Lz4];

/// The longest payload the codec sweeps admit, well past admission's floor
const MAX_PAYLOAD_LEN: usize = 2048;

/// Each corruption seed makes this many damaged copies of the image
const IMAGES_PER_SEED: usize = 6;

/// Each damaged image gets this many wounds
const WOUNDS: usize = 8;

/// The corruption fixture writes into this group
const GROUP: u16 = 7;

/// The corruption fixture writes this many records
const FIXTURE_KEYS: u8 = 10;

/// Payload length of each fixture record, wide enough to span blocks
const FIXTURE_LEN: usize = 5_000;

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(32 * 1024),
        sync: SyncPolicy::Bytes(ByteCount::from_bytes(0)),
        active_tails: ThreadBudget::threads(1),
        ..ReelConfig::default()
    }
}

fn random_bytes(rng: &mut SmallRng, max: usize) -> Vec<u8> {
    let len = rng.gen_range(0..=max);
    (0..len).map(|_| rng.gen()).collect()
}

// no byte string, however malformed, takes a parser down
#[test]
fn parsers_never_panic_on_arbitrary_bytes() {
    for seed in SEEDS {
        let mut rng = SmallRng::seed_from_u64(*seed);
        for _ in 0..CASES {
            let bytes = random_bytes(&mut rng, MAX_CASE_LEN);

            let _ = RecordHeader::unpack(&bytes);
            let _ = SegmentFooter::parse(&bytes);
            let _ = SegmentHeader::unpack(&bytes);
            let _ = PrefixRows::decode(&bytes, Tail::Entry);
            let _ = unpack(&bytes, Tail::Entry, None);
            let _ = unpack(&bytes, Tail::Entry, Some(33));
            let _ = ColumnMark::unpack(&bytes);
            let _ = Filter::parse_region(&bytes, rng.gen_range(0..8));
            for codec in CODECS {
                let _ = decode(codec.as_byte(), &bytes);
            }
            let _ = read_groups(&bytes);
        }
    }
}

/// A payload from one repeated byte up to random fill, so every admission outcome gets drawn
fn payload_bytes(rng: &mut SmallRng, len: usize) -> Vec<u8> {
    let alphabet = rng.gen_range(1..=256usize);
    (0..len).map(|_| rng.gen_range(0..alphabet) as u8).collect()
}

// what admission stored, decode gives back exactly
#[test]
fn codec_payloads_roundtrip() {
    for seed in SEEDS {
        let mut rng = SmallRng::seed_from_u64(*seed);
        for _ in 0..CASES {
            let len = rng.gen_range(0..=MAX_PAYLOAD_LEN);
            let logical = payload_bytes(&mut rng, len);
            for codec in CODECS {
                let (stored, byte) = admit(*codec, logical.clone());
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
#[test]
fn wounded_codec_payloads_never_panic() {
    for seed in SEEDS {
        let mut rng = SmallRng::seed_from_u64(*seed);
        for _ in 0..CASES {
            let len = rng.gen_range(MAX_CASE_LEN..=MAX_PAYLOAD_LEN);
            let logical = payload_bytes(&mut rng, len);
            for codec in CODECS {
                let (stored, byte) = admit(*codec, logical.clone());
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
                layout: match rng.gen::<bool>() {
                    true => RecordLayout::Keyless(CheckKey::from_bytes(rng.gen())),
                    false => RecordLayout::Keyed,
                },
                rows_at: rng.gen(),
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

// the segment header's frozen prefix keeps its width, since every file starts with it
#[test]
fn segment_header_width_is_frozen() {
    let packed = SegmentHeader::new(SegmentId(1)).pack();

    assert_eq!(packed.len(), SEGMENT_HEADER_SPAN);
    assert_eq!(
        SegmentHeader::unpack(&packed[..SEGMENT_HEADER_LEN])
            .expect("the prefix alone parses")
            .segment,
        SegmentId(1)
    );
}
