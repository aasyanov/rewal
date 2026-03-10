# REWAL — Rust Embedded Write-Ahead Log

[![CI](https://github.com/aasyanov/rewal/actions/workflows/ci.yml/badge.svg)](https://github.com/aasyanov/rewal/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/rewal.svg)](https://crates.io/crates/rewal)
[![docs.rs](https://docs.rs/rewal/badge.svg)](https://docs.rs/rewal)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Production-grade embedded WAL for Rust. Zero external dependencies.

```toml
[dependencies]
rewal = "0.1"
```

## Overview

REWAL is a strict, minimalist, high-performance Write-Ahead Log engine for single-process Rust applications. Designed for event sourcing, state machine recovery, embedded database logs, durable queues, and high-frequency buffering.

Wire-compatible with [UEWAL](https://github.com/aasyanov/uewal) (Go implementation) — same EWAL batch frame format.

**Not** a distributed log, server, async runtime, or multi-process WAL.

## Architecture

```
append()  ──► LsnCounter (AtomicU64) ──► WriteQueue ──► writer thread ──► Storage
                                            ▲                 │
                                       FlushBarrier       group commit
                                                      encode_batch + CRC32C
                                                      compress (optional)
                                                      write_all + maybe_sync
                                                      indexer.on_append
```

- **Append-only**, single writer thread
- **Lock-free** LSN assignment via `AtomicU64`
- **Group commit**: writer drains all available batches before issuing a single write
- **Batch-framed format** with single CRC-32C per batch (true batch atomicity)
- **Zero-copy replay** via mmap with `BorrowedEvent` (no heap allocation)
- **Optional compression** via pluggable `Compressor` trait
- **Optional indexing** via pluggable `Indexer` trait
- **Event metadata** via `Event.meta` field (zero-cost when `None`)
- **0 allocations per append** (`EventVec::Inline` for single events, atomic LSN per batch)
- No external dependencies, no async runtime

## Quick Start

```rust
use rewal::{Wal, Batch, Result};
use std::time::Duration;

fn main() -> Result<()> {
    let wal = Wal::open("/tmp/my.wal").build()?;

    // Single event
    let lsn = wal.append("hello world")?;

    // Single event with metadata
    let lsn = wal.append_with_meta("user_created", "aggregate:user:123")?;

    // Batch write
    let mut batch = Batch::with_capacity(3);
    batch.add("event-1");
    batch.add_with_meta("event-2", "type:update");
    batch.add("event-3");
    let lsn = wal.append_batch(batch)?;

    // Read (zero-copy via mmap)
    wal.flush()?;
    wal.replay(0, |ev| {
        println!("LSN={} payload={:?}", ev.lsn, ev.payload);
        Ok(())
    })?;

    // Zero-copy iterator
    let mut it = wal.iterator(0)?;
    while let Some(ev) = it.next_borrowed()? {
        println!("LSN={} payload={:?}", ev.lsn, ev.payload);
    }

    wal.shutdown(Duration::from_secs(5))?;
    Ok(())
}
```

## Public API

| Method | Description |
|---|---|
| `Wal::open(path).build()` | Create or open a WAL via builder |
| `append(payload)` | Write a single event, returns assigned LSN |
| `append_with_meta(payload, meta)` | Write event with metadata |
| `append_batch(batch)` | Write a pre-built batch atomically |
| `flush()` | Wait for writer to process all queued batches |
| `sync()` | fsync to make written data durable |
| `replay(from, fn)` | Callback-based read (zero-copy via mmap) |
| `iterator(from)` | Pull-based iterator (zero-copy via mmap) |
| `last_lsn()` | Most recently persisted LSN |
| `stats()` | Lock-free runtime statistics snapshot |
| `shutdown(timeout)` | Graceful shutdown with timeout |
| `close()` | Immediate close without draining |

## Configuration

All configuration is via the builder returned by `Wal::open`:

```rust
let wal = Wal::open("/tmp/my.wal")
    .sync_mode(SyncMode::Batch)                    // fsync after every write
    .backpressure(Backpressure::Block)              // block when queue full
    .queue_capacity(4096)                           // write queue capacity
    .buffer_capacity(64 * 1024)                     // encoder buffer size
    // .compressor(my_zstd_compressor)              // optional compression
    // .indexer(my_indexer)                          // optional indexer
    // .hooks(my_hooks)                             // observability callbacks
    .build()?;
```

### Durability Modes

| Mode | Behavior | Throughput |
|---|---|---|
| `SyncMode::Never` | No fsync, OS page cache only | Highest |
| `SyncMode::Batch` | fsync after every write batch | Lowest latency risk |
| `SyncMode::Interval(dur)` | fsync at configurable interval | Balanced |

### Backpressure Modes

| Mode | Behavior |
|---|---|
| `Backpressure::Block` | Caller blocks until queue has space (default) |
| `Backpressure::Drop` | Events silently dropped, `Stats.drops` incremented |
| `Backpressure::Error` | `Error::QueueFull` returned immediately |

## Lifecycle

```
Init ──► Running ──► Draining ──► Closed
 open()    append/replay  shutdown()   done
```

- `shutdown(timeout)`: graceful — drains queue, flushes, syncs, closes storage. Respects timeout. Idempotent.
- `close()`: immediate — stops writer, closes storage, discards pending data. Idempotent.

## Batch Frame Format (v2)

```
┌──────────────────────────────────────────────────┐
│ Magic        4 bytes   "EWAL"                    │
│ Version      2 bytes   (2)                       │
│ Flags        2 bytes   (bit 0 = compressed)      │
│ RecordCount  4 bytes                             │
│ FirstLSN     8 bytes                             │
│ BatchSize    4 bytes   (total frame incl. CRC)   │
├──────────────────────────────────────────────────┤
│ Records region (possibly compressed):            │
│   Record 0:                                      │
│     PayloadLen  4 bytes                          │
│     MetaLen     2 bytes                          │
│     Meta        MetaLen bytes                    │
│     Payload     PayloadLen bytes                 │
│   Record 1: ...                                  │
├──────────────────────────────────────────────────┤
│ CRC32C       4 bytes   (covers Magic..records)   │
└──────────────────────────────────────────────────┘
```

- **Batch header**: 24 bytes. **Per-record overhead**: 6 bytes. **Batch overhead**: 28 bytes.
- CRC-32C (Castagnoli) with hardware acceleration (SSE4.2 / ARM CRC)
- Little-endian encoding, 4-byte magic "EWAL" for frame detection
- **True batch atomicity**: single CRC covers entire batch; on recovery, either all events are valid or the entire frame is discarded
- **Wire-compatible** with [UEWAL](https://github.com/aasyanov/uewal) (Go) — files are interchangeable

## Compressor Trait

```rust
pub trait Compressor: Send + Sync {
    fn compress(&self, src: &[u8], dst: &mut Vec<u8>) -> Result<()>;
    fn decompress(&self, src: &[u8], dst: &mut Vec<u8>) -> Result<()>;
}
```

The WAL never depends on any compression library — users provide their own implementation wrapping zstd, lz4, snappy, or anything else. The output parameter avoids heap allocation on the hot path.

## Indexer Trait

```rust
pub trait Indexer: Send + Sync {
    fn on_append(&self, lsn: Lsn, meta: Option<&[u8]>, offset: u64);
}
```

Called from the writer thread after each event is persisted. Panics are recovered. Useful for building external indexes, LSN-to-offset lookup tables, or routing events by metadata.

## Storage Trait

```rust
pub trait Storage: Send + Sync {
    fn write(&mut self, buf: &[u8]) -> Result<usize>;
    fn sync(&mut self) -> Result<()>;
    fn close(&mut self) -> Result<()>;
    fn size(&self) -> Result<u64>;
    fn truncate(&mut self, size: u64) -> Result<()>;
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
}
```

The default `FileStorage` uses `std::fs::File` with flock/LockFileEx to prevent concurrent access.

## Crash Recovery

On open, the WAL scans all existing batch frame headers to recover the last valid LSN. If corruption is detected (CRC mismatch or truncated frame), the file is automatically truncated to the last valid batch boundary. A corrupted batch is entirely discarded (true batch atomicity). Recovery is O(n) in file size but runs only once at startup.

## Flush vs Sync

| Operation | What it does | Durability |
|---|---|---|
| `flush()` | Waits for writer to process all pending batches and `write()` them to storage | Data is in OS page cache |
| `sync()` | Calls `fsync()` on the underlying file | Data survives power failure |

For maximum durability: `flush()` then `sync()`.

## Observability

### Hooks

```rust
pub trait Hooks: Send + Sync {
    fn on_start(&self) {}
    fn on_shutdown_start(&self) {}
    fn on_shutdown_done(&self, elapsed: Duration) {}
    fn before_append(&self, events: &[Event]) {}
    fn after_append(&self, lsn: Lsn, count: usize) {}
    fn before_write(&self, size: usize) {}
    fn after_write(&self, written: usize) {}
    fn before_sync(&self) {}
    fn after_sync(&self, elapsed: Duration) {}
    fn on_corruption(&self, offset: u64) {}
    fn on_drop(&self, count: usize) {}
}
```

All 11 hooks have default no-op implementations, are panic-safe, and never affect WAL consistency.

### Stats

```rust
let s = wal.stats();
// s.events_written, s.batches_written, s.bytes_written, s.bytes_synced,
// s.sync_count, s.compressed_bytes, s.drops, s.corruptions,
// s.queue_len, s.file_size, s.last_lsn, s.state
```

All counters are lock-free (atomic). Safe to call from any thread, in any state.

## Errors

14 error variants, all `Debug + Display + Clone + Eq`:

| Error | Returned by |
|---|---|
| `Closed` | Any operation on a closed WAL |
| `Draining` | `append` during graceful shutdown |
| `NotRunning` | Operations requiring `State::Running` |
| `Corrupted` | `WalIterator::err` on CRC mismatch |
| `QueueFull` | `append` in `Backpressure::Error` mode |
| `FileLocked` | `FileStorage::new` when file is locked |
| `InvalidLsn` | Invalid LSN argument |
| `ShortWrite` | Storage returns n=0 without error |
| `InvalidRecord` | Truncated/unsupported record header |
| `CrcMismatch` | CRC-32C validation failure |
| `InvalidState` | Illegal lifecycle transition |
| `EmptyBatch` | `append_batch` with zero events |
| `CompressorRequired` | Compressed data without `Compressor` |
| `Io(io::Error)` | OS-level I/O error |

## Platform Support

| Platform | Status |
|---|---|
| Linux x86_64 / aarch64 | Full support + HW CRC |
| macOS x86_64 / Apple Silicon | Full support + HW CRC |
| Windows x86_64 / aarch64 | Full support + HW CRC |
| FreeBSD / OpenBSD | Full support (Unix path) |
| 32-bit x86 / ARM | Full support, SW CRC fallback |

CRC-32C uses SSE4.2 (x86_64) or ARM CRC (aarch64) intrinsics with runtime detection, falling back to a slice-by-8 table-driven algorithm on all other targets.

## Benchmark Results

Measured on Intel Core i7-10510U @ 1.80GHz, Windows 10, Rust 1.70+ (stable), NVMe SSD.
Cold-boot run in a bare terminal (no IDE overhead). Criterion with 100 samples per benchmark.

### Write Path

| Benchmark | Latency | Throughput |
|---|---|---|
| AppendAsync (128B payload) | 250 ns | 618 MiB/s |
| AppendDurable (128B, SyncBatch) | 370 ns | 418 MiB/s |
| AppendBatch10 (10 x 128B) | 1.16 µs | 1.10 GiB/s |
| AppendBatch100 (100 x 128B) | 7.43 µs | 1.68 GiB/s |
| AppendParallel (4 threads) | 474 µs total | 334 KiB/s |
| AppendZeroCopy (move Vec) | 182 ns | 849 MiB/s |
| AppendNonblocking (error mode) | 119 ns | 1.27 GiB/s |

### Flush & Sync

| Benchmark | Latency |
|---|---|
| Flush (write barrier) | 12.6 µs |
| Flush + Sync (fsync) | 1.06 ms |

### Read Path (100K events, 256B payload)

| Benchmark | Time |
|---|---|
| Replay (callback, mmap) | 52 ms |
| Iterator owned (pull-based) | 56 ms |
| Iterator borrowed (zero-copy) | 36 ms |

### Encoding (hot path, 10 x 128B)

| Benchmark | Latency | Throughput |
|---|---|---|
| EncodeBatch | 288 ns | 4.42 GiB/s |
| DecodeBatch (owned) | 4.69 µs | 278 MiB/s |
| DecodeBatch (zero-copy) | 223 ns | 5.71 GiB/s |

### Recovery & Integrity

| Benchmark | Latency | Throughput |
|---|---|---|
| Recovery (100K events) | 13.9 ms | — |
| CRC-32C 1 KiB | 64.6 ns | 14.8 GiB/s |
| CRC-32C 4 KiB | 319 ns | 11.9 GiB/s |
| CRC-32C 64 KiB | 5.55 µs | 11.0 GiB/s |
| ScanHeader (with CRC) | 100 ns | 12.7 GiB/s |
| ScanHeader (no CRC) | 1.66 ns | 767 GiB/s |

### REWAL (Rust) vs UEWAL (Go) — Head-to-Head

Both implementations use the same EWAL v2 wire format. UEWAL numbers are verified medians from 5 runs (`go test . -bench . -count 5`) on the same i7-10510U, Windows machine. REWAL numbers from cold-boot Criterion run on the same hardware. Go reports decimal MB/s; Rust reports binary MiB/s — noted where relevant.

| Benchmark | UEWAL (Go) | REWAL (Rust) | Notes |
|---|---|---|---|
| AppendAsync 128B | 265 ns / 612 MB/s | 250 ns / 618 MiB/s | **Rust 1.06x** faster |
| AppendDurable 128B | 482 ns / 337 MB/s | 370 ns / 418 MiB/s | **Rust 1.30x** faster |
| AppendBatch10 | 1.13 µs / 1206 MB/s | 1.16 µs / 1.10 GiB/s | Parity |
| AppendBatch100 | 15.2 µs / 885 MB/s | 7.43 µs / 1.68 GiB/s | **Rust 2.04x** faster |
| AppendParallel | 344 ns / 471 MB/s | 474 µs / 4 threads | Different semantics (see below) |
| Flush | 5.2 µs | 12.6 µs | Go 2.4x (M:N scheduler wake) |
| Flush+Sync | 1.09 ms | 1.06 ms | Parity — fsync-bound |
| EncodeBatch 10x128B | 222 ns / 6.2 GB/s | 288 ns / 4.42 GiB/s | Go 1.3x |
| DecodeBatch (owned) | 363 ns / 3.8 GB/s | 4.69 µs / 278 MiB/s | Go (Rust allocates per event) |
| DecodeBatchInto/ZC | 172 ns / 7.9 GB/s | 223 ns / 5.71 GiB/s | Go 1.3x |
| ScanHeader (with CRC) | 86 ns / 15.9 GB/s | 100 ns / 12.7 GiB/s | Go 1.2x |
| Replay 100K | 36.3 ms | 52 ms | Go 1.4x (slice reuse vs alloc) |
| Iterator (owned) | 33.9 ms | 56 ms | Go 1.7x (same reason) |
| Iterator (borrowed) | — | 36 ms | **Rust-only** zero-copy path |
| Recovery 100K | 16.4 ms | 13.9 ms | **Rust 1.18x** faster |
| ScanHeader (no CRC) | — | 1.66 ns / 767 GiB/s | **Rust-only** fast scan |

*Go throughput in decimal units (1 GB/s = 10^9 B/s). Rust in binary (1 GiB/s = 2^30 B/s). 1 GiB/s ≈ 1.074 GB/s.*

**AppendParallel note**: Go bench uses 8 goroutines and measures per-goroutine latency (344 ns). Rust bench uses 4 OS threads and measures total wall-clock time for all threads (474 µs). These are different metrics and not directly comparable.

### Analysis

**Write path**: With verified Go numbers, Rust now wins on **single-event async append** (250 ns vs 265 ns, 1.06x). The previous uewal README claimed 225 ns which overstated Go's advantage — fresh 5-run medians show 265 ns. AppendDurable gap widens to 1.3x in Rust's favor (370 ns vs 482 ns). AppendBatch100 is the clearest win: **2.04x** faster (7.43 µs vs 15.2 µs) — Rust's batch-level `LsnCounter::next_n` assigns all LSNs in a single atomic op, and `EventVec::Heap` avoids per-event overhead. AppendNonblocking at 119 ns / 1.27 GiB/s shows the raw cost of LSN assignment + queue enqueue with immediate error return.

**Durability cost**: SyncBatch adds ~120 ns per append (370 ns vs 250 ns). Flush+Sync is ~1.06-1.09 ms — identical between languages, confirming the bottleneck is NVMe hardware. Flush without sync at 12.6 µs is higher than Go's 5.2 µs due to `thread::park` latency on Windows vs Go's goroutine scheduling. On Linux this gap shrinks significantly.

**Read path**: Owned decode and iteration are slower than Go because Rust `.to_vec()` per event is a heap allocation, while Go's `[]byte` slices can share backing arrays with the mmap region without explicit lifetime tracking. Rust's answer: `next_borrowed()` at 36 ms achieves near-parity with Go's Iterator (33.9 ms) by returning `BorrowedEvent<'_>` with payload/meta pointing directly into the mmap'd region — zero heap allocations, enforced by the borrow checker.

**Encoding**: Go leads in encoding throughput — EncodeBatch at 6.2 GB/s vs Rust 4.75 GB/s (4.42 GiB/s), and DecodeBatchInto at 7.9 GB/s vs Rust 6.13 GB/s (5.71 GiB/s). Go's `binary.LittleEndian.PutUint*` compiles to equally efficient code, and Go's escape analysis eliminates some bounds checks. ScanHeader shows a similar 1.2x gap (86 ns vs 100 ns).

**CRC-32C**: Both use SSE4.2 `_mm_crc32_u64` hardware intrinsics. Rust at 14.8 GiB/s (1 KiB) sustains high throughput across all buffer sizes. Go's CRC is not benchmarked separately but is included in ScanHeader numbers where Go holds a slight edge (15.9 GB/s vs 12.7 GiB/s = 13.6 GB/s).

**Recovery**: Rust at 13.9 ms for 100K events is 1.18x faster than Go's 16.4 ms. Both scan headers + validate CRC per frame without deserializing events. `scan_batch_header` (no CRC) at 767 GiB/s is a Rust-only fast path: a pointer comparison + 3 integer reads — useful for index building where integrity was already verified.

**Memory**: 0 heap allocations per `append()` hot path (`EventVec::Inline` avoids `Vec<Event>`, `LsnCounter::next()` is a single `AtomicU64::fetch_add`). The encoder buffer grows dynamically and is reused across all writes. `FlushBarrier` is stack-allocated (no `Arc` overhead).

## Test Suite

```
cargo test: 60 passed, 0 failed, 2 doc-tests passed
cargo clippy: 0 warnings
```

| Category | Count | Description |
|---|---|---|
| Unit tests | 60 | CRC, encoding, error, event, hooks, queue, state, stats, storage, mmap, replay, WAL lifecycle |
| Doc-tests | 2 | Quick start examples in lib.rs and wal.rs |
| Benchmarks | 18 | Write path, read path, encoding, flush, recovery, CRC, scan header |

Tests cover: round-trip encode/decode, CRC validation, corruption detection, file locking, concurrent appends (4 threads), recovery after reopen, lifecycle transitions, iterator seek, empty/edge cases.

All tests pass on cold boot in bare terminal (no IDE interference).

## Zero Dependencies

REWAL has no external dependencies. CRC-32C uses hardware intrinsics with a software fallback. Memory mapping uses platform-native syscalls. The only dev-dependencies are `criterion` (benchmarks) and `tempfile` (tests).

## File Structure

```
rewal/
├── src/
│   ├── lib.rs          # Crate root, re-exports
│   ├── wal.rs          # Wal struct, open, append, flush, shutdown
│   ├── writer.rs       # Single writer thread, group commit
│   ├── queue.rs        # Bounded write queue, FlushBarrier, EventVec
│   ├── encoding.rs     # Batch frame codec v2, Encoder, BorrowedEvent
│   ├── replay.rs       # WalIterator, callback replay, mmap-based
│   ├── event.rs        # Event, Batch, Lsn
│   ├── error.rs        # Error enum (14 variants)
│   ├── options.rs      # WalBuilder, SyncMode, Backpressure, traits
│   ├── hooks.rs        # Hooks trait, panic-safe HooksRunner
│   ├── storage.rs      # Storage trait, FileStorage
│   ├── stats.rs        # Stats struct, StatsCollector (atomic)
│   ├── state.rs        # State enum, StateMachine (atomic CAS)
│   ├── crc.rs          # CRC-32C (HW SSE4.2 + ARM CRC + SW fallback)
│   ├── mmap.rs         # MmapReader abstraction
│   ├── sys.rs          # Platform dispatch
│   └── sys/
│       ├── unix.rs     # flock, mmap, munmap
│       └── windows.rs  # LockFileEx, CreateFileMapping, MapViewOfFile
├── benches/
│   └── wal.rs          # Criterion benchmarks
├── Cargo.toml          # Zero dependencies, MIT license
├── LICENSE
└── README.md
```

## License

MIT
