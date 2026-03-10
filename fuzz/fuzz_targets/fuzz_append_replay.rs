#![no_main]

use libfuzzer_sys::fuzz_target;
use std::time::Duration;

#[derive(Debug, arbitrary::Arbitrary)]
struct Input {
    payload: Vec<u8>,
    count: u8,
}

fuzz_target!(|input: Input| {
    let count = input.count as usize;
    if count == 0 || count > 100 {
        return;
    }
    if input.payload.len() > 1024 {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fuzz.wal");

    let wal = match rewal::Wal::open(&path)
        .sync_mode(rewal::SyncMode::Batch)
        .build()
    {
        Ok(w) => w,
        Err(_) => return,
    };

    for _ in 0..count {
        if wal.append(input.payload.clone()).is_err() {
            return;
        }
    }
    let _ = wal.shutdown(Duration::from_secs(5));

    let wal2 = match rewal::Wal::open(&path).build() {
        Ok(w) => w,
        Err(_) => return,
    };

    let mut replayed = 0usize;
    let _ = wal2.replay(0, |_| {
        replayed += 1;
        Ok(())
    });

    assert_eq!(replayed, count, "replayed {replayed}, want {count}");
    let _ = wal2.shutdown(Duration::from_secs(5));
});
