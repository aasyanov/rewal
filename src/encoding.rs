//! Batch frame codec (v2 wire format) and reusable encoder buffer.
//!
//! ```text
//! ┌──────────────────────────────────────────────────┐
//! │ Magic        4 bytes   "EWAL"                    │
//! │ Version      2 bytes   (2)                       │
//! │ Flags        2 bytes   (bit 0 = compressed)      │
//! │ RecordCount  4 bytes                             │
//! │ FirstLSN     8 bytes                             │
//! │ BatchSize    4 bytes   (total frame incl. CRC)   │
//! ├──────────────────────────────────────────────────┤
//! │ Records region (possibly compressed):            │
//! │   PayloadLen  4B │ MetaLen 2B │ Meta │ Payload   │
//! ├──────────────────────────────────────────────────┤
//! │ CRC32C       4 bytes                             │
//! └──────────────────────────────────────────────────┘
//! ```

use crate::crc;
use crate::error::{Error, Result};
use crate::event::{Event, Lsn};
use crate::options::Compressor;

pub(crate) const BATCH_MAGIC: [u8; 4] = *b"EWAL";
pub(crate) const BATCH_VERSION: u16 = 2;
pub(crate) const HEADER_LEN: usize = 4 + 2 + 2 + 4 + 8 + 4; // 24
pub(crate) const TRAILER_LEN: usize = 4; // CRC32
pub(crate) const FRAME_OVERHEAD: usize = HEADER_LEN + TRAILER_LEN; // 28

const FLAG_COMPRESSED: u16 = 1 << 0;

// ── Size helpers ────────────────────────────────────────────────────────────

#[inline]
fn record_data_size(ev: &Event) -> usize {
    4 + 2 + ev.meta.as_ref().map_or(0, |m| m.len()) + ev.payload.len()
}

fn batch_frame_size(events: &[Event]) -> usize {
    FRAME_OVERHEAD + events.iter().map(record_data_size).sum::<usize>()
}

// ── Low-level encode / decode ───────────────────────────────────────────────

fn encode_records_region(dst: &mut [u8], events: &[Event]) -> usize {
    let base = dst.as_mut_ptr();
    let mut off = 0usize;
    for ev in events {
        let meta = ev.meta.as_deref().unwrap_or(&[]);
        let plen = ev.payload.len();
        let mlen = meta.len();
        unsafe {
            let p = base.add(off);
            (p as *mut u32).write_unaligned((plen as u32).to_le());
            (p.add(4) as *mut u16).write_unaligned((mlen as u16).to_le());
            std::ptr::copy_nonoverlapping(meta.as_ptr(), p.add(6), mlen);
            std::ptr::copy_nonoverlapping(ev.payload.as_ptr(), p.add(6 + mlen), plen);
        }
        off += 6 + mlen + plen;
    }
    off
}

/// Writes the 24-byte batch header in one shot via pointer writes.
/// Safety: caller must ensure `dst` has at least HEADER_LEN (24) bytes.
#[inline(always)]
fn write_header(dst: &mut [u8], flags: u16, count: u32, first_lsn: Lsn, total_size: u32) {
    debug_assert!(dst.len() >= HEADER_LEN);
    unsafe {
        let p = dst.as_mut_ptr();
        std::ptr::copy_nonoverlapping(BATCH_MAGIC.as_ptr(), p, 4);
        (p.add(4) as *mut u16).write_unaligned(BATCH_VERSION.to_le());
        (p.add(6) as *mut u16).write_unaligned(flags.to_le());
        (p.add(8) as *mut u32).write_unaligned(count.to_le());
        (p.add(12) as *mut u64).write_unaligned(first_lsn.to_le());
        (p.add(20) as *mut u32).write_unaligned(total_size.to_le());
    }
}

/// Encodes a complete batch frame into `dst`, returning the number of bytes
/// written. `dst` must be large enough to hold the frame (use
/// [`batch_frame_size`] to pre-compute).
pub fn encode_batch_frame(
    dst: &mut Vec<u8>,
    events: &[Event],
    first_lsn: Lsn,
    comp: Option<&dyn Compressor>,
) -> Result<usize> {
    let start = dst.len();
    let need = batch_frame_size(events);
    dst.resize(start + need, 0);

    let records_len = encode_records_region(&mut dst[start + HEADER_LEN..], events);

    let (flags, final_records_len) = if let Some(c) = comp {
        let records_data = &dst[start + HEADER_LEN..start + HEADER_LEN + records_len];
        let mut compressed = Vec::new();
        c.compress(records_data, &mut compressed)?;
        let clen = compressed.len();
        let total = HEADER_LEN + clen + TRAILER_LEN;
        unsafe {
            dst.set_len(start + total);
        }
        dst[start + HEADER_LEN..start + HEADER_LEN + clen].copy_from_slice(&compressed);
        (FLAG_COMPRESSED, clen)
    } else {
        (0u16, records_len)
    };

    let total_size = HEADER_LEN + final_records_len + TRAILER_LEN;

    write_header(
        &mut dst[start..],
        flags,
        events.len() as u32,
        first_lsn,
        total_size as u32,
    );

    let checksum = crc::checksum(&dst[start..start + HEADER_LEN + final_records_len]);
    let crc_off = start + HEADER_LEN + final_records_len;
    unsafe {
        (dst.as_mut_ptr().add(crc_off) as *mut u32).write_unaligned(checksum.to_le());
    }

    Ok(total_size)
}

/// Decodes one batch frame from `data` at byte offset `off`.
///
/// Returns `(events, next_offset)`. Event payloads and meta are copies when
/// compressed, or sub-slices of `data` converted to owned `Vec<u8>` (necessary
/// because events may outlive the mmap region during iteration).
pub fn decode_batch_frame(
    data: &[u8],
    off: usize,
    decomp: Option<&dyn Compressor>,
) -> Result<(Vec<Event>, usize)> {
    if off + HEADER_LEN > data.len() {
        return Err(Error::InvalidRecord);
    }

    let hdr = &data[off..];

    if hdr[..4] != BATCH_MAGIC {
        return Err(Error::InvalidRecord);
    }

    let version = read_u16(hdr, 4);
    if version != BATCH_VERSION {
        return Err(Error::InvalidRecord);
    }

    let flags = read_u16(hdr, 6);
    let count = read_u32(hdr, 8) as usize;
    let first_lsn = read_u64(hdr, 12);
    let total_size = read_u32(hdr, 20) as usize;

    let frame_end = off + total_size;
    if frame_end > data.len() || total_size < FRAME_OVERHEAD {
        return Err(Error::InvalidRecord);
    }

    let crc_off = frame_end - TRAILER_LEN;
    let stored_crc = read_u32(data, crc_off);
    let computed_crc = crc::checksum(&data[off..crc_off]);
    if stored_crc != computed_crc {
        return Err(Error::CrcMismatch);
    }

    let records_data = &data[off + HEADER_LEN..crc_off];

    let records_buf;
    let records: &[u8] = if flags & FLAG_COMPRESSED != 0 {
        let c = decomp.ok_or(Error::CompressorRequired)?;
        let mut decompressed = Vec::new();
        c.decompress(records_data, &mut decompressed)?;
        records_buf = decompressed;
        &records_buf
    } else {
        records_data
    };

    let rlen = records.len();
    let max_records = rlen / 6; // minimum 6 bytes per record header
    if count > max_records {
        return Err(Error::InvalidRecord);
    }

    let mut events = Vec::with_capacity(count);
    let mut r_off = 0usize;
    let mut lsn = first_lsn;

    for _ in 0..count {
        if r_off + 6 > rlen {
            return Err(Error::InvalidRecord);
        }
        let payload_len = read_u32(records, r_off) as usize;
        let meta_len = read_u16(records, r_off + 4) as usize;
        r_off += 6;

        let rec_end = r_off + meta_len + payload_len;
        if rec_end > rlen {
            return Err(Error::InvalidRecord);
        }

        let meta = if meta_len > 0 {
            Some(records[r_off..r_off + meta_len].to_vec())
        } else {
            None
        };
        r_off += meta_len;

        let payload = records[r_off..r_off + payload_len].to_vec();
        r_off += payload_len;

        events.push(Event { lsn, payload, meta });
        lsn += 1;
    }

    Ok((events, frame_end))
}

/// Decode only the header of a batch frame, returning (count, first_lsn, total_size).
/// Validates magic, version, bounds AND CRC. Used by recovery.
pub fn decode_batch_header(data: &[u8], off: usize) -> Result<(usize, Lsn, usize)> {
    let (count, first_lsn, total_size) = scan_batch_header(data, off)?;

    let frame_end = off + total_size;
    let crc_off = frame_end - TRAILER_LEN;
    let stored_crc = read_u32(data, crc_off);
    let computed_crc = crc::checksum(&data[off..crc_off]);
    if stored_crc != computed_crc {
        return Err(Error::CrcMismatch);
    }

    Ok((count, first_lsn, total_size))
}

/// Fast header-only scan: validates magic, version, and bounds but skips CRC.
/// Used for forward scanning where integrity was already verified, or where
/// speed is more important than detecting corruption (e.g. index building).
#[inline]
pub fn scan_batch_header(data: &[u8], off: usize) -> Result<(usize, Lsn, usize)> {
    if off + HEADER_LEN > data.len() {
        return Err(Error::InvalidRecord);
    }
    let hdr = &data[off..];
    if hdr[..4] != BATCH_MAGIC {
        return Err(Error::InvalidRecord);
    }
    if read_u16(hdr, 4) != BATCH_VERSION {
        return Err(Error::InvalidRecord);
    }
    let count = read_u32(hdr, 8) as usize;
    let first_lsn = read_u64(hdr, 12);
    let total_size = read_u32(hdr, 20) as usize;

    let frame_end = off + total_size;
    if frame_end > data.len() || total_size < FRAME_OVERHEAD {
        return Err(Error::InvalidRecord);
    }

    Ok((count, first_lsn, total_size))
}

#[inline(always)]
fn read_u16(buf: &[u8], off: usize) -> u16 {
    debug_assert!(off + 2 <= buf.len());
    u16::from_le(unsafe { (buf.as_ptr().add(off) as *const u16).read_unaligned() })
}

#[inline(always)]
fn read_u32(buf: &[u8], off: usize) -> u32 {
    debug_assert!(off + 4 <= buf.len());
    u32::from_le(unsafe { (buf.as_ptr().add(off) as *const u32).read_unaligned() })
}

#[inline(always)]
fn read_u64(buf: &[u8], off: usize) -> u64 {
    debug_assert!(off + 8 <= buf.len());
    u64::from_le(unsafe { (buf.as_ptr().add(off) as *const u64).read_unaligned() })
}

/// Borrowed event referencing data in a memory-mapped region.
/// Zero-copy: payload and meta are sub-slices of the mmap'd buffer.
/// Callers must copy if data must outlive the mmap lifetime.
pub struct BorrowedEvent<'a> {
    pub lsn: Lsn,
    pub payload: &'a [u8],
    pub meta: Option<&'a [u8]>,
}

impl BorrowedEvent<'_> {
    /// Converts to an owned Event (copies payload and meta to heap).
    #[inline]
    pub fn to_owned(&self) -> Event {
        Event {
            lsn: self.lsn,
            payload: self.payload.to_vec(),
            meta: self.meta.map(|m| m.to_vec()),
        }
    }
}

/// Zero-copy batch frame decoder. Returns borrowed events referencing the
/// input `data` slice directly. No heap allocation when uncompressed.
pub fn decode_batch_frame_borrowed<'a>(
    data: &'a [u8],
    off: usize,
    decomp: Option<&dyn Compressor>,
    decomp_buf: &'a mut Vec<u8>,
) -> Result<(Vec<BorrowedEvent<'a>>, usize)> {
    if off + HEADER_LEN > data.len() {
        return Err(Error::InvalidRecord);
    }

    let hdr = &data[off..];
    if hdr[..4] != BATCH_MAGIC {
        return Err(Error::InvalidRecord);
    }
    if read_u16(hdr, 4) != BATCH_VERSION {
        return Err(Error::InvalidRecord);
    }

    let flags = read_u16(hdr, 6);
    let count = read_u32(hdr, 8) as usize;
    let first_lsn = read_u64(hdr, 12);
    let total_size = read_u32(hdr, 20) as usize;

    let frame_end = off + total_size;
    if frame_end > data.len() || total_size < FRAME_OVERHEAD {
        return Err(Error::InvalidRecord);
    }

    let crc_off = frame_end - TRAILER_LEN;
    let stored_crc = read_u32(data, crc_off);
    let computed_crc = crc::checksum(&data[off..crc_off]);
    if stored_crc != computed_crc {
        return Err(Error::CrcMismatch);
    }

    let records_data = &data[off + HEADER_LEN..crc_off];

    let records: &[u8] = if flags & FLAG_COMPRESSED != 0 {
        let c = decomp.ok_or(Error::CompressorRequired)?;
        decomp_buf.clear();
        c.decompress(records_data, decomp_buf)?;
        decomp_buf.as_slice()
    } else {
        records_data
    };

    let rlen = records.len();
    let max_records = rlen / 6;
    if count > max_records {
        return Err(Error::InvalidRecord);
    }

    let mut events = Vec::with_capacity(count);
    let mut r_off = 0usize;
    let mut lsn = first_lsn;

    for _ in 0..count {
        if r_off + 6 > rlen {
            return Err(Error::InvalidRecord);
        }
        let payload_len = read_u32(records, r_off) as usize;
        let meta_len = read_u16(records, r_off + 4) as usize;
        r_off += 6;

        let rec_end = r_off + meta_len + payload_len;
        if rec_end > rlen {
            return Err(Error::InvalidRecord);
        }

        let meta = if meta_len > 0 {
            Some(&records[r_off..r_off + meta_len])
        } else {
            None
        };
        r_off += meta_len;

        let payload = &records[r_off..r_off + payload_len];
        r_off += payload_len;

        events.push(BorrowedEvent { lsn, payload, meta });
        lsn += 1;
    }

    Ok((events, frame_end))
}

/// Decodes all sequential batch frames from `data`, stopping at the first
/// invalid or truncated frame. Returns `(events, last_valid_offset, error)`.
pub fn decode_all_batches(
    data: &[u8],
    decomp: Option<&dyn Compressor>,
) -> (Vec<Event>, usize, Option<Error>) {
    let mut all = Vec::new();
    let mut off = 0;

    while off < data.len() {
        match decode_batch_frame(data, off, decomp) {
            Ok((events, next)) => {
                all.extend(events);
                off = next;
            }
            Err(e) => return (all, off, Some(e)),
        }
    }

    (all, off, None)
}

// ── Encoder (reusable buffer) ───────────────────────────────────────────────

/// Reusable buffer for encoding batch frames before writing to storage.
///
/// Not safe for concurrent use — owned by the writer thread.
pub(crate) struct Encoder {
    buf: Vec<u8>,
}

impl Encoder {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
        }
    }

    /// Discards buffered data, retaining allocated capacity.
    #[inline]
    pub fn reset(&mut self) {
        self.buf.clear();
    }

    /// Appends a complete batch frame to the encoder buffer.
    pub fn encode_batch(
        &mut self,
        events: &[Event],
        first_lsn: Lsn,
        comp: Option<&dyn Compressor>,
    ) -> Result<usize> {
        encode_batch_frame(&mut self.buf, events, first_lsn, comp)
    }

    /// Returns the buffered data.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_events(n: usize, payload_size: usize) -> Vec<Event> {
        (0..n)
            .map(|i| Event {
                lsn: 0,
                payload: vec![i as u8; payload_size],
                meta: None,
            })
            .collect()
    }

    #[test]
    fn round_trip_single_event() {
        let events = make_events(1, 32);
        let mut buf = Vec::new();
        let n = encode_batch_frame(&mut buf, &events, 1, None).unwrap();
        assert_eq!(buf.len(), n);

        let (decoded, next) = decode_batch_frame(&buf, 0, None).unwrap();
        assert_eq!(next, n);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].lsn, 1);
        assert_eq!(decoded[0].payload, events[0].payload);
        assert!(decoded[0].meta.is_none());
    }

    #[test]
    fn round_trip_batch() {
        let events: Vec<Event> = (0..10)
            .map(|i| Event {
                lsn: 0,
                payload: format!("event-{i}").into_bytes(),
                meta: if i % 2 == 0 {
                    Some(format!("meta-{i}").into_bytes())
                } else {
                    None
                },
            })
            .collect();

        let mut buf = Vec::new();
        encode_batch_frame(&mut buf, &events, 100, None).unwrap();

        let (decoded, _) = decode_batch_frame(&buf, 0, None).unwrap();
        assert_eq!(decoded.len(), 10);
        for (i, ev) in decoded.iter().enumerate() {
            assert_eq!(ev.lsn, 100 + i as u64);
            assert_eq!(ev.payload, format!("event-{i}").as_bytes());
            if i % 2 == 0 {
                assert_eq!(ev.meta.as_deref(), Some(format!("meta-{i}").as_bytes()));
            } else {
                assert!(ev.meta.is_none());
            }
        }
    }

    #[test]
    fn crc_mismatch_detected() {
        let events = make_events(1, 16);
        let mut buf = Vec::new();
        encode_batch_frame(&mut buf, &events, 1, None).unwrap();

        let last = buf.len() - 1;
        buf[last] ^= 0xFF;

        let err = decode_batch_frame(&buf, 0, None).unwrap_err();
        assert_eq!(err, Error::CrcMismatch);
    }

    #[test]
    fn truncated_frame() {
        let events = make_events(1, 16);
        let mut buf = Vec::new();
        encode_batch_frame(&mut buf, &events, 1, None).unwrap();

        let err = decode_batch_frame(&buf[..10], 0, None).unwrap_err();
        assert_eq!(err, Error::InvalidRecord);
    }

    #[test]
    fn invalid_magic() {
        let mut buf = vec![0u8; FRAME_OVERHEAD + 10];
        buf[0..4].copy_from_slice(b"XXXX");
        let err = decode_batch_frame(&buf, 0, None).unwrap_err();
        assert_eq!(err, Error::InvalidRecord);
    }

    #[test]
    fn zero_copy_round_trip() {
        let events: Vec<Event> = (0..5)
            .map(|i| Event {
                lsn: 0,
                payload: format!("payload-{i}").into_bytes(),
                meta: if i % 2 == 0 {
                    Some(format!("meta-{i}").into_bytes())
                } else {
                    None
                },
            })
            .collect();

        let mut buf = Vec::new();
        encode_batch_frame(&mut buf, &events, 10, None).unwrap();

        let mut decomp_buf = Vec::new();
        let (borrowed, next) = decode_batch_frame_borrowed(&buf, 0, None, &mut decomp_buf).unwrap();
        assert_eq!(next, buf.len());
        assert_eq!(borrowed.len(), 5);
        for (i, ev) in borrowed.iter().enumerate() {
            assert_eq!(ev.lsn, 10 + i as u64);
            assert_eq!(ev.payload, format!("payload-{i}").as_bytes());
            if i % 2 == 0 {
                assert_eq!(ev.meta, Some(format!("meta-{i}").as_bytes()));
            } else {
                assert!(ev.meta.is_none());
            }
        }
    }

    #[test]
    fn ewal_magic_bytes() {
        let events = make_events(1, 16);
        let mut buf = Vec::new();
        encode_batch_frame(&mut buf, &events, 1, None).unwrap();
        assert_eq!(&buf[0..4], b"EWAL");
    }

    #[test]
    fn encoder_reuse() {
        let mut enc = Encoder::new(1024);
        let events = make_events(5, 64);

        enc.encode_batch(&events, 1, None).unwrap();
        let first_len = enc.as_bytes().len();
        assert!(!enc.as_bytes().is_empty());

        enc.reset();
        assert_eq!(enc.as_bytes().len(), 0);

        enc.encode_batch(&events, 10, None).unwrap();
        assert_eq!(enc.as_bytes().len(), first_len);
    }

    #[test]
    fn multiple_frames_decode() {
        let mut buf = Vec::new();
        let e1 = make_events(3, 16);
        let e2 = make_events(2, 32);

        encode_batch_frame(&mut buf, &e1, 1, None).unwrap();
        encode_batch_frame(&mut buf, &e2, 4, None).unwrap();

        let (all, off, err) = decode_all_batches(&buf, None);
        assert!(err.is_none());
        assert_eq!(off, buf.len());
        assert_eq!(all.len(), 5);
        assert_eq!(all[0].lsn, 1);
        assert_eq!(all[3].lsn, 4);
    }

    #[test]
    fn frame_size_calculation() {
        let events = make_events(1, 128);
        let expected = FRAME_OVERHEAD + 4 + 2 + 128;
        assert_eq!(batch_frame_size(&events), expected);
    }
}
