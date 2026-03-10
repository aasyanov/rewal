#![no_main]

use libfuzzer_sys::fuzz_target;
use std::io::Write;
use std::time::Duration;

#[derive(Debug, arbitrary::Arbitrary)]
struct Input {
    valid_records: u8,
    garbage: Vec<u8>,
}

fuzz_target!(|input: Input| {
    let valid_records = input.valid_records as usize;
    if valid_records == 0 || valid_records > 50 {
        return;
    }
    if input.garbage.len() > 100 {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fuzz_corrupt.wal");

    // Write valid records via WAL, then close
    {
        let wal = match rewal::Wal::open(&path)
            .sync_mode(rewal::SyncMode::Batch)
            .build()
        {
            Ok(w) => w,
            Err(_) => return,
        };

        for _ in 0..valid_records {
            if wal.append(b"valid").is_err() {
                return;
            }
        }
        let _ = wal.shutdown(Duration::from_secs(5));
    }

    // Append garbage after valid data
    if !input.garbage.is_empty() {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(&input.garbage).unwrap();
        f.sync_all().unwrap();
    }

    // Reopen and verify recovery
    let wal = match rewal::Wal::open(&path).build() {
        Ok(w) => w,
        Err(_) => return,
    };

    let mut count = 0usize;
    let _ = wal.replay(0, |_| {
        count += 1;
        Ok(())
    });

    assert_eq!(
        count, valid_records,
        "replayed {count}, want {valid_records}"
    );
    let _ = wal.shutdown(Duration::from_secs(5));
});
