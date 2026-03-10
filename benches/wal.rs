use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::time::Duration;

use rewal::{Backpressure, Batch, Event, SyncMode, Wal};

const PAYLOAD_SIZE: usize = 128;
const REPLAY_EVENTS: usize = 100_000;
const REPLAY_PAYLOAD_SIZE: usize = 256;

fn frame_size(payload_len: usize, count: usize) -> u64 {
    let record = 4 + 2 + payload_len;
    (28 + record * count) as u64
}

fn make_payload(size: usize) -> Vec<u8> {
    vec![0xAB; size]
}

fn tmp_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("rewal_bench_{name}_{}", std::process::id()))
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
}

// ── 1. AppendAsync — direct comparison with Go BenchmarkAppendAsync ─────────

fn bench_append_async(c: &mut Criterion) {
    let path = tmp_path("async");
    cleanup(&path);

    let mut group = c.benchmark_group("append_async");
    group.throughput(Throughput::Bytes(frame_size(PAYLOAD_SIZE, 1)));

    group.bench_function("128B", |b| {
        let wal = Wal::open(&path)
            .sync_mode(SyncMode::Never)
            .queue_capacity(16384)
            .build()
            .unwrap();
        let payload = make_payload(PAYLOAD_SIZE);

        b.iter(|| {
            wal.append(payload.clone()).unwrap();
        });

        wal.shutdown(Duration::from_secs(5)).unwrap();
        cleanup(&path);
    });
    group.finish();
}

// ── 2. AppendDurable — direct comparison with Go BenchmarkAppendDurable ─────

fn bench_append_durable(c: &mut Criterion) {
    let path = tmp_path("durable");
    cleanup(&path);

    let mut group = c.benchmark_group("append_durable");
    group.throughput(Throughput::Bytes(frame_size(PAYLOAD_SIZE, 1)));

    group.bench_function("128B", |b| {
        let wal = Wal::open(&path)
            .sync_mode(SyncMode::Batch)
            .queue_capacity(16384)
            .build()
            .unwrap();
        let payload = make_payload(PAYLOAD_SIZE);

        b.iter(|| {
            wal.append(payload.clone()).unwrap();
        });

        wal.shutdown(Duration::from_secs(5)).unwrap();
        cleanup(&path);
    });
    group.finish();
}

// ── 3. AppendBatch10 — direct comparison with Go BenchmarkAppendBatch10 ─────

fn bench_append_batch_10(c: &mut Criterion) {
    let path = tmp_path("batch10");
    cleanup(&path);

    let mut group = c.benchmark_group("append_batch_10");
    group.throughput(Throughput::Bytes(frame_size(PAYLOAD_SIZE, 10)));

    group.bench_function("10x128B", |b| {
        let wal = Wal::open(&path)
            .sync_mode(SyncMode::Never)
            .queue_capacity(16384)
            .build()
            .unwrap();
        let payload = make_payload(PAYLOAD_SIZE);

        b.iter(|| {
            let mut batch = Batch::with_capacity(10);
            for _ in 0..10 {
                batch.add(payload.clone());
            }
            wal.append_batch(batch).unwrap();
        });

        wal.shutdown(Duration::from_secs(5)).unwrap();
        cleanup(&path);
    });
    group.finish();
}

// ── 4. AppendBatch100 — direct comparison with Go BenchmarkAppendBatch100 ───

fn bench_append_batch_100(c: &mut Criterion) {
    let path = tmp_path("batch100");
    cleanup(&path);

    let mut group = c.benchmark_group("append_batch_100");
    group.throughput(Throughput::Bytes(frame_size(PAYLOAD_SIZE, 100)));

    group.bench_function("100x128B", |b| {
        let wal = Wal::open(&path)
            .sync_mode(SyncMode::Never)
            .queue_capacity(16384)
            .build()
            .unwrap();
        let payload = make_payload(PAYLOAD_SIZE);

        b.iter(|| {
            let mut batch = Batch::with_capacity(100);
            for _ in 0..100 {
                batch.add(payload.clone());
            }
            wal.append_batch(batch).unwrap();
        });

        wal.shutdown(Duration::from_secs(5)).unwrap();
        cleanup(&path);
    });
    group.finish();
}

// ── 5. AppendParallel — direct comparison with Go BenchmarkAppendParallel ───

fn bench_append_parallel(c: &mut Criterion) {
    let path = tmp_path("parallel");
    cleanup(&path);

    let mut group = c.benchmark_group("append_parallel");
    group.throughput(Throughput::Bytes(frame_size(PAYLOAD_SIZE, 1)));

    group.bench_function("4threads", |b| {
        let wal = Wal::open(&path)
            .sync_mode(SyncMode::Never)
            .queue_capacity(32768)
            .build()
            .unwrap();
        let wal = std::sync::Arc::new(wal);
        let payload = make_payload(PAYLOAD_SIZE);

        b.iter(|| {
            let mut handles = Vec::new();
            for _ in 0..4 {
                let w = wal.clone();
                let p = payload.clone();
                handles.push(std::thread::spawn(move || {
                    for _ in 0..100 {
                        w.append(p.clone()).unwrap();
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
        });

        wal.shutdown(Duration::from_secs(5)).unwrap();
        cleanup(&path);
    });
    group.finish();
}

// ── 6. Flush — direct comparison with Go BenchmarkFlush ─────────────────────

fn bench_flush(c: &mut Criterion) {
    let path = tmp_path("flush");
    cleanup(&path);

    let mut group = c.benchmark_group("flush");

    group.bench_function("append+flush", |b| {
        let wal = Wal::open(&path)
            .sync_mode(SyncMode::Never)
            .queue_capacity(16384)
            .build()
            .unwrap();
        let payload = make_payload(PAYLOAD_SIZE);

        b.iter(|| {
            wal.append(payload.clone()).unwrap();
            wal.flush().unwrap();
        });

        wal.shutdown(Duration::from_secs(5)).unwrap();
        cleanup(&path);
    });
    group.finish();
}

// ── 7. FlushSync — direct comparison with Go BenchmarkFlushSync ─────────────

fn bench_flush_sync(c: &mut Criterion) {
    let path = tmp_path("flush_sync");
    cleanup(&path);

    let mut group = c.benchmark_group("flush_sync");
    group.sample_size(20);

    group.bench_function("append+flush+sync", |b| {
        let wal = Wal::open(&path)
            .sync_mode(SyncMode::Never)
            .queue_capacity(16384)
            .build()
            .unwrap();
        let payload = make_payload(PAYLOAD_SIZE);

        b.iter(|| {
            wal.append(payload.clone()).unwrap();
            wal.flush().unwrap();
            wal.sync().unwrap();
        });

        wal.shutdown(Duration::from_secs(5)).unwrap();
        cleanup(&path);
    });
    group.finish();
}

// ── Replay/Iterator/Recovery helpers ────────────────────────────────────────

fn prepare_replay_file(path: &std::path::Path) {
    cleanup(path);
    let wal = Wal::open(path)
        .sync_mode(SyncMode::Batch)
        .queue_capacity(16384)
        .build()
        .unwrap();
    let payload = make_payload(REPLAY_PAYLOAD_SIZE);
    for _ in 0..(REPLAY_EVENTS / 100) {
        let mut batch = Batch::with_capacity(100);
        for _ in 0..100 {
            batch.add(payload.clone());
        }
        wal.append_batch(batch).unwrap();
    }
    wal.shutdown(Duration::from_secs(10)).unwrap();
}

// ── 8. Replay — direct comparison with Go BenchmarkReplay ───────────────────

fn bench_replay(c: &mut Criterion) {
    let path = tmp_path("replay");
    prepare_replay_file(&path);

    let mut group = c.benchmark_group("replay");
    group.sample_size(20);

    group.bench_function("100k_events", |b| {
        b.iter(|| {
            let wal = Wal::open(&path).build().unwrap();
            let mut count = 0u64;
            wal.replay(0, |_ev| {
                count += 1;
                Ok(())
            })
            .unwrap();
            assert_eq!(count, REPLAY_EVENTS as u64);
            wal.shutdown(Duration::from_secs(5)).unwrap();
        });
    });
    group.finish();

    cleanup(&path);
}

// ── 9. Iterator — direct comparison with Go BenchmarkIterator ───────────────

fn bench_iterator(c: &mut Criterion) {
    let path = tmp_path("iterator");
    prepare_replay_file(&path);

    let mut group = c.benchmark_group("iterator");
    group.sample_size(20);

    group.bench_function("100k_events_owned", |b| {
        b.iter(|| {
            let wal = Wal::open(&path).build().unwrap();
            let mut it = wal.iterator(0).unwrap();
            let mut count = 0u64;
            while let Ok(Some(_ev)) = it.next() {
                count += 1;
            }
            assert_eq!(count, REPLAY_EVENTS as u64);
            wal.shutdown(Duration::from_secs(5)).unwrap();
        });
    });

    group.bench_function("100k_events_borrowed", |b| {
        b.iter(|| {
            let wal = Wal::open(&path).build().unwrap();
            let mut it = wal.iterator(0).unwrap();
            let mut count = 0u64;
            while let Ok(Some(_ev)) = it.next_borrowed() {
                count += 1;
            }
            assert_eq!(count, REPLAY_EVENTS as u64);
            wal.shutdown(Duration::from_secs(5)).unwrap();
        });
    });
    group.finish();

    cleanup(&path);
}

// ── 10. EncodeBatch — direct comparison with Go BenchmarkEncodeBatch ────────

fn bench_encode_batch(c: &mut Criterion) {
    let payload = make_payload(PAYLOAD_SIZE);
    let events: Vec<Event> = (0..10)
        .map(|_| Event {
            lsn: 0,
            payload: payload.clone(),
            meta: None,
        })
        .collect();

    let mut group = c.benchmark_group("encode_batch");
    group.throughput(Throughput::Bytes(frame_size(PAYLOAD_SIZE, 10)));

    group.bench_function("10x128B", |b| {
        let mut buf = Vec::with_capacity(4096);
        b.iter(|| {
            buf.clear();
            rewal::encoding::encode_batch_frame(&mut buf, &events, 1, None).unwrap();
        });
    });
    group.finish();
}

// ── 11. DecodeBatch — direct comparison with Go BenchmarkDecodeBatch ────────

fn bench_decode_batch(c: &mut Criterion) {
    let payload = make_payload(PAYLOAD_SIZE);
    let events: Vec<Event> = (0..10)
        .map(|_| Event {
            lsn: 0,
            payload: payload.clone(),
            meta: None,
        })
        .collect();
    let mut buf = Vec::new();
    rewal::encoding::encode_batch_frame(&mut buf, &events, 1, None).unwrap();
    let frame_len = buf.len() as u64;

    let mut group = c.benchmark_group("decode_batch");
    group.throughput(Throughput::Bytes(frame_len));

    group.bench_function("10x128B_owned", |b| {
        b.iter(|| {
            rewal::encoding::decode_batch_frame(&buf, 0, None).unwrap();
        });
    });

    group.bench_function("10x128B_zero_copy", |b| {
        let mut decomp_buf = Vec::new();
        b.iter(|| {
            rewal::encoding::decode_batch_frame_borrowed(&buf, 0, None, &mut decomp_buf).unwrap();
        });
    });
    group.finish();
}

// ── 12. Recovery — direct comparison with Go BenchmarkRecovery ──────────────

fn bench_recovery(c: &mut Criterion) {
    let path = tmp_path("recovery");
    prepare_replay_file(&path);

    let mut group = c.benchmark_group("recovery");
    group.sample_size(20);

    group.bench_function("100k_events", |b| {
        b.iter(|| {
            let wal = Wal::open(&path).build().unwrap();
            assert_eq!(wal.last_lsn(), REPLAY_EVENTS as u64);
            wal.shutdown(Duration::from_secs(5)).unwrap();
        });
    });
    group.finish();

    cleanup(&path);
}

// ── 13. CRC32C — Rust advantage: bare intrinsic speed ───────────────────────

fn bench_crc32c(c: &mut Criterion) {
    let mut group = c.benchmark_group("crc32c");

    for &size in &[1024, 4096, 65536] {
        let data = vec![0x42u8; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(format!("{}B", size), |b| {
            b.iter(|| {
                std::hint::black_box(rewal::crc::checksum(&data));
            });
        });
    }
    group.finish();
}

// ── 14. Zero-copy append — Rust advantage: move semantics ───────────────────

fn bench_append_zero_copy(c: &mut Criterion) {
    let path = tmp_path("zerocopy");
    cleanup(&path);

    let mut group = c.benchmark_group("append_zero_copy");
    group.throughput(Throughput::Bytes(frame_size(PAYLOAD_SIZE, 1)));

    group.bench_function("move_vec", |b| {
        let wal = Wal::open(&path)
            .sync_mode(SyncMode::Never)
            .queue_capacity(16384)
            .build()
            .unwrap();

        b.iter(|| {
            let payload = vec![0xABu8; PAYLOAD_SIZE];
            wal.append(payload).unwrap();
        });

        wal.shutdown(Duration::from_secs(5)).unwrap();
        cleanup(&path);
    });
    group.finish();
}

// ── 15. Backpressure::Error — hot path without blocking ─────────────────────

fn bench_append_nonblocking(c: &mut Criterion) {
    let path = tmp_path("nonblock");
    cleanup(&path);

    let mut group = c.benchmark_group("append_nonblocking");
    group.throughput(Throughput::Bytes(frame_size(PAYLOAD_SIZE, 1)));

    group.bench_function("error_mode", |b| {
        let wal = Wal::open(&path)
            .sync_mode(SyncMode::Never)
            .queue_capacity(65536)
            .backpressure(Backpressure::Error)
            .build()
            .unwrap();
        let payload = make_payload(PAYLOAD_SIZE);

        b.iter(|| {
            let _ = wal.append(payload.clone());
        });

        wal.shutdown(Duration::from_secs(5)).unwrap();
        cleanup(&path);
    });
    group.finish();
}

// ── 16. ScanBatchHeader — direct comparison with Go ─────────────────────────

fn bench_scan_batch_header(c: &mut Criterion) {
    let payload = make_payload(PAYLOAD_SIZE);
    let events: Vec<Event> = (0..10)
        .map(|_| Event {
            lsn: 0,
            payload: payload.clone(),
            meta: None,
        })
        .collect();
    let mut buf = Vec::new();
    rewal::encoding::encode_batch_frame(&mut buf, &events, 1, None).unwrap();
    let frame_len = buf.len() as u64;

    let mut group = c.benchmark_group("scan_batch_header");
    group.throughput(Throughput::Bytes(frame_len));

    group.bench_function("10x128B_with_crc", |b| {
        b.iter(|| {
            std::hint::black_box(rewal::encoding::decode_batch_header(&buf, 0).unwrap());
        });
    });

    group.bench_function("10x128B_no_crc", |b| {
        b.iter(|| {
            std::hint::black_box(rewal::scan_batch_header(&buf, 0).unwrap());
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_append_async,
    bench_append_durable,
    bench_append_batch_10,
    bench_append_batch_100,
    bench_append_parallel,
    bench_flush,
    bench_flush_sync,
    bench_replay,
    bench_iterator,
    bench_encode_batch,
    bench_decode_batch,
    bench_recovery,
    bench_crc32c,
    bench_append_zero_copy,
    bench_append_nonblocking,
    bench_scan_batch_header,
);
criterion_main!(benches);
