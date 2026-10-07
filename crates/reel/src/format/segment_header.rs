//! The frozen first record payload that makes every segment self describing

use crate::error::{ReelError, Result};
use crate::format::loc::SegmentId;
use crate::format::record::{read_u32_le, CheckKey, RecordLayout, CHECK_KEY_LEN};

/// The format version this build stamps into every new segment header
///
/// A build meeting a version it cannot read refuses the whole file here, rather
/// than truncating its walk at an unknown record kind and losing the tail silently.
pub const FORMAT_VERSION: u16 = 7;

const VERSION_LEN: usize = std::mem::size_of::<u16>();
const SEGMENT_LEN: usize = std::mem::size_of::<u32>();

const VERSION_AT: usize = 0;
const SEGMENT_AT: usize = VERSION_AT + VERSION_LEN;

/// Bytes the frozen segment header payload occupies
pub const SEGMENT_HEADER_LEN: usize = SEGMENT_AT + SEGMENT_LEN;

/// Where the record layout byte sits, behind the frozen prefix
const LAYOUT_AT: usize = SEGMENT_HEADER_LEN;

/// Where a keyless segment's check key sits, behind its layout byte
const CHECK_AT: usize = LAYOUT_AT + 1;

/// This build writes the frozen prefix, the record layout, then the check key
pub const SEGMENT_HEADER_SPAN: usize = CHECK_AT + CHECK_KEY_LEN;

/// The fixed payload carried by the first record of every segment
///
/// The prefix's layout never changes, so any build can identify any file ever written.
/// What follows is read where the payload reaches it and defaulted where it does not,
/// which is how the record layout joined a format already in the field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentHeader {
    /// Format version the segment was written under
    pub version: u16,

    /// Monotonic segment number within the reel
    pub segment: SegmentId,

    /// How the segment frames its records, which every reader of the file needs first
    pub layout: RecordLayout,
}

impl SegmentHeader {
    /// A header for a segment written under the current format version
    pub fn new(segment: SegmentId) -> SegmentHeader {
        SegmentHeader {
            version: FORMAT_VERSION,
            segment,
            layout: RecordLayout::Keyed,
        }
    }

    /// The same header for a segment whose records lie in this layout
    pub fn laid_out(self, layout: RecordLayout) -> SegmentHeader {
        SegmentHeader { layout, ..self }
    }

    /// Serialize to the on-disk payload bytes
    pub fn pack(self) -> [u8; SEGMENT_HEADER_SPAN] {
        let mut out = [0u8; SEGMENT_HEADER_SPAN];
        out[VERSION_AT..SEGMENT_AT].copy_from_slice(&self.version.to_le_bytes());
        out[SEGMENT_AT..SEGMENT_HEADER_LEN].copy_from_slice(&self.segment.as_u32().to_le_bytes());
        out[LAYOUT_AT] = self.layout.as_u8();
        if let RecordLayout::Keyless(check) = self.layout {
            out[CHECK_AT..SEGMENT_HEADER_SPAN].copy_from_slice(&check.to_bytes());
        }
        out
    }

    /// Parse the frozen prefix, tolerating a payload that stops before the layout
    pub fn unpack(bytes: &[u8]) -> Result<SegmentHeader> {
        if bytes.len() < SEGMENT_HEADER_LEN {
            return Err(ReelError::Corruption(
                "segment header payload is shorter than the frozen prefix".to_string(),
            ));
        }

        let version = read_u16_le(&bytes[VERSION_AT..SEGMENT_AT]);
        let segment = read_u32_le(&bytes[SEGMENT_AT..SEGMENT_HEADER_LEN]);

        let layout = match bytes.get(LAYOUT_AT) {
            None | Some(0) => RecordLayout::Keyed,
            Some(1) => {
                let check: [u8; CHECK_KEY_LEN] = bytes
                    .get(CHECK_AT..SEGMENT_HEADER_SPAN)
                    .and_then(|key| key.try_into().ok())
                    .ok_or_else(|| {
                        ReelError::Corruption(
                            "a keyless segment header holds no check key".to_string(),
                        )
                    })?;
                RecordLayout::Keyless(CheckKey::from_bytes(check))
            }
            Some(byte) => {
                return Err(ReelError::Corruption(format!(
                    "segment header holds record layout {byte}, which no writer stores"
                )))
            }
        };

        Ok(SegmentHeader {
            version,
            segment: SegmentId(segment),
            layout,
        })
    }
}

fn read_u16_le(bytes: &[u8]) -> u16 {
    let mut buf = [0u8; std::mem::size_of::<u16>()];
    buf.copy_from_slice(bytes);
    u16::from_le_bytes(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::format::lsn::Lsn;
    use crate::format::record::RecordHeader;

    const CHECK: CheckKey = CheckKey::from_bytes([0xc0; CHECK_KEY_LEN]);

    // the frozen layout pins exact bytes so offsets cannot drift
    #[test]
    fn frozen_layout() {
        let header = SegmentHeader {
            version: 2,
            segment: SegmentId(0x0302_0100),
            layout: RecordLayout::Keyless(CHECK),
        };

        let mut wanted = vec![0x02, 0x00, 0x00, 0x01, 0x02, 0x03, 0x01];
        wanted.extend_from_slice(&[0xc0; CHECK_KEY_LEN]);
        assert_eq!(header.pack().to_vec(), wanted);
        assert_eq!(SEGMENT_HEADER_LEN, VERSION_LEN + SEGMENT_LEN);
        assert_eq!(SEGMENT_HEADER_SPAN, SEGMENT_HEADER_LEN + 1 + CHECK_KEY_LEN);
    }

    // a keyless layout survives a round trip, and a payload without the layout byte reads keyed
    #[test]
    fn layout_roundtrip() {
        let header = SegmentHeader::new(SegmentId(9)).laid_out(RecordLayout::Keyless(CHECK));
        let packed = header.pack();

        let parsed = SegmentHeader::unpack(&packed).expect("unpack");
        let short = SegmentHeader::unpack(&packed[..LAYOUT_AT]).expect("unpack");

        assert_eq!(parsed.layout, RecordLayout::Keyless(CHECK));
        assert_eq!(short.layout, RecordLayout::Keyed);
    }

    // a keyless header cut before its check key refuses the file
    #[test]
    fn keyless_without_its_key_rejected() {
        let packed = SegmentHeader::new(SegmentId(9))
            .laid_out(RecordLayout::Keyless(CHECK))
            .pack();

        assert!(SegmentHeader::unpack(&packed[..SEGMENT_HEADER_SPAN - 1]).is_err());
    }

    // a layout byte no writer stores refuses the file
    #[test]
    fn unknown_layout_rejected() {
        let mut packed = SegmentHeader::new(SegmentId(9)).pack();
        packed[LAYOUT_AT] = 7;

        assert!(SegmentHeader::unpack(&packed).is_err());
    }

    // a future version may lengthen the payload and this build still reads it
    #[test]
    fn tolerates_longer_payload() {
        let header = SegmentHeader::new(SegmentId(4));
        let mut extended = header.pack().to_vec();
        extended.extend_from_slice(&[0xff; 16]);

        let parsed = SegmentHeader::unpack(&extended).expect("unpack");

        assert_eq!(parsed, header);
    }

    // a payload shorter than the frozen prefix is rejected
    #[test]
    fn short_payload_rejected() {
        let header = SegmentHeader::new(SegmentId(1));
        let packed = header.pack();

        assert!(SegmentHeader::unpack(&packed[..SEGMENT_HEADER_LEN - 1]).is_err());
        assert!(SegmentHeader::unpack(&[]).is_err());
    }

    // the payload rides a segment header record and verifies as its first record
    #[test]
    fn wraps_in_first_record() {
        let payload = SegmentHeader::new(SegmentId(2)).pack();
        let record = RecordHeader::segment_header(&payload);

        let parsed = RecordHeader::unpack(record.pack().as_slice()).expect("unpack");

        assert!(parsed.flags.is_segment_header());
        assert!(parsed.verify(&payload));
        assert_eq!(parsed.lsn, Lsn::NONE);

        let decoded = SegmentHeader::unpack(&payload).expect("decode");
        assert_eq!(decoded.segment, SegmentId(2));
    }
}
