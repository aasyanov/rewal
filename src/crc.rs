//! CRC-32C (Castagnoli) checksum with hardware acceleration.
//!
//! Uses SSE4.2 `_mm_crc32_*` intrinsics on x86_64 and `__crc32c*` on
//! aarch64 when available at runtime. Falls back to a slice-by-8
//! table-driven algorithm on all other targets.

const CASTAGNOLI: u32 = 0x82F6_3B78;

/// Returns the CRC-32C checksum of `data`.
#[inline]
pub fn checksum(data: &[u8]) -> u32 {
    update(0, data)
}

/// Feeds `data` into a running CRC-32C, returning the updated value.
#[inline]
pub fn update(crc: u32, data: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("sse4.2") {
            return unsafe { hw_x86::update(crc, data) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("crc") {
            return unsafe { hw_arm::update(crc, data) };
        }
    }
    sw::update(crc, data)
}

// ── x86_64 SSE4.2 ──────────────────────────────────────────────────────────

#[cfg(target_arch = "x86_64")]
mod hw_x86 {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{_mm_crc32_u64, _mm_crc32_u8};

    #[target_feature(enable = "sse4.2")]
    pub unsafe fn update(crc: u32, data: &[u8]) -> u32 {
        let mut h = crc as u64 ^ 0xFFFF_FFFF;
        let ptr = data.as_ptr();
        let len = data.len();
        let mut i = 0usize;

        let end8 = len & !7;
        while i < end8 {
            let word = (ptr.add(i) as *const u64).read_unaligned();
            h = _mm_crc32_u64(h, word);
            i += 8;
        }

        while i < len {
            h = _mm_crc32_u8(h as u32, *ptr.add(i)) as u64;
            i += 1;
        }

        (h as u32) ^ 0xFFFF_FFFF
    }
}

// ── aarch64 CRC ─────────────────────────────────────────────────────────────

#[cfg(target_arch = "aarch64")]
mod hw_arm {
    use std::arch::aarch64::{__crc32cb, __crc32cd, __crc32cw};

    #[target_feature(enable = "crc")]
    pub unsafe fn update(crc: u32, data: &[u8]) -> u32 {
        let mut h = crc ^ 0xFFFF_FFFF;
        let ptr = data.as_ptr();
        let len = data.len();
        let mut i = 0usize;

        let end8 = len & !7;
        while i < end8 {
            let word = (ptr.add(i) as *const u64).read_unaligned();
            h = __crc32cd(h, word);
            i += 8;
        }

        if i + 4 <= len {
            let word = (ptr.add(i) as *const u32).read_unaligned();
            h = __crc32cw(h, word);
            i += 4;
        }

        while i < len {
            h = __crc32cb(h, *ptr.add(i));
            i += 1;
        }

        h ^ 0xFFFF_FFFF
    }
}

// ── Software fallback (slice-by-8) ──────────────────────────────────────────

mod sw {
    use super::CASTAGNOLI;

    const fn make_tables() -> [[u32; 256]; 8] {
        let mut tables = [[0u32; 256]; 8];
        let mut i = 0u32;
        while i < 256 {
            let mut crc = i;
            let mut j = 0;
            while j < 8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ CASTAGNOLI;
                } else {
                    crc >>= 1;
                }
                j += 1;
            }
            tables[0][i as usize] = crc;
            i += 1;
        }

        let mut k = 1u32;
        while k < 8 {
            let mut i = 0u32;
            while i < 256 {
                tables[k as usize][i as usize] = (tables[(k - 1) as usize][i as usize] >> 8)
                    ^ tables[0][(tables[(k - 1) as usize][i as usize] & 0xFF) as usize];
                i += 1;
            }
            k += 1;
        }
        tables
    }

    pub(crate) static TABLES: [[u32; 256]; 8] = make_tables();

    pub fn update(crc: u32, data: &[u8]) -> u32 {
        let mut h = crc ^ 0xFFFF_FFFF;
        let ptr = data.as_ptr();
        let len = data.len();
        let mut i = 0usize;

        let end8 = len & !7;
        while i < end8 {
            unsafe {
                let lo = (ptr.add(i) as *const u32).read_unaligned().to_le() ^ h;
                let hi = (ptr.add(i + 4) as *const u32).read_unaligned().to_le();

                h = TABLES[7][(lo & 0xFF) as usize]
                    ^ TABLES[6][((lo >> 8) & 0xFF) as usize]
                    ^ TABLES[5][((lo >> 16) & 0xFF) as usize]
                    ^ TABLES[4][((lo >> 24) & 0xFF) as usize]
                    ^ TABLES[3][(hi & 0xFF) as usize]
                    ^ TABLES[2][((hi >> 8) & 0xFF) as usize]
                    ^ TABLES[1][((hi >> 16) & 0xFF) as usize]
                    ^ TABLES[0][((hi >> 24) & 0xFF) as usize];
            }
            i += 8;
        }

        while i < len {
            unsafe {
                h = (h >> 8) ^ TABLES[0][((h ^ *ptr.add(i) as u32) & 0xFF) as usize];
            }
            i += 1;
        }

        h ^ 0xFFFF_FFFF
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input() {
        assert_eq!(checksum(b""), 0x0000_0000);
    }

    #[test]
    fn known_vector_123456789() {
        // Standard CRC-32C test vector.
        assert_eq!(checksum(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn incremental_equals_oneshot() {
        let data = b"hello, world! this is a longer test string for CRC-32C.";
        let oneshot = checksum(data);
        let incremental = update(update(0, &data[..10]), &data[10..]);
        assert_eq!(oneshot, incremental);
    }

    #[test]
    fn software_matches_dispatch() {
        let data: Vec<u8> = (0..=255).collect();
        let via_dispatch = checksum(&data);
        let sw_direct = sw::update(0, &data);
        assert_eq!(via_dispatch, sw_direct);

        // Cross-check via raw table lookup
        let mut h = 0xFFFF_FFFFu32;
        for &b in &data {
            h = (h >> 8) ^ sw::TABLES[0][((h ^ b as u32) & 0xFF) as usize];
        }
        h ^= 0xFFFF_FFFF;
        assert_eq!(via_dispatch, h);
    }

    #[test]
    fn various_lengths() {
        for len in 0..=64 {
            let data: Vec<u8> = (0..len).map(|i| i as u8).collect();
            let a = checksum(&data);
            let b = sw::update(0, &data);
            assert_eq!(a, b, "mismatch at len={len}");
        }
    }

    #[test]
    fn alignment_independence() {
        let base: Vec<u8> = (0..100).collect();
        for offset in 0..8 {
            let slice = &base[offset..];
            let a = checksum(slice);
            let b = sw::update(0, slice);
            assert_eq!(a, b, "mismatch at offset={offset}");
        }
    }
}
