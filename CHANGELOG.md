# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] — 2026-03-10

Initial public release.

### Added

- Core WAL engine with single writer thread and lock-free LSN assignment (`AtomicU64`).
- `Wal::open` builder API with `build()` for configuration and construction.
- `append`, `append_with_meta`, `append_batch`, `append_zero_copy` write methods.
- `flush`, `sync`, `shutdown`, `close` lifecycle methods.
- `replay(from_lsn, fn)` streaming replay with mmap zero-copy reads.
- `iterator(from_lsn)` pull-based read interface with `next()` (owned) and `next_borrowed()` (zero-copy).
- `BorrowedEvent<'_>` for zero-copy deserialization — payload/meta reference mmap'd memory directly.
- Batch-framed wire format (EWAL v2) with single CRC-32C per batch for true batch atomicity.
- Wire-compatible with [UEWAL](https://github.com/aasyanov/uewal) (Go) — files are interchangeable.
- `Event.meta` field for opaque per-event metadata.
- `Batch` type with `add`, `add_with_meta`, `reset` for reusable batch construction.
- Pluggable `Compressor` trait (`compress`/`decompress`) via builder.
- Pluggable `Indexer` trait (`on_append`) via builder.
- Exported `Storage` trait with `FileStorage` implementation.
- File locking via flock (Unix) / `LockFileEx` (Windows).
- Platform-specific mmap: `mmap(2)` on Unix, `CreateFileMapping`/`MapViewOfFile` on Windows.
- `compile_error!` for unsupported targets (neither unix nor windows).
- Three durability modes: `SyncNever`, `SyncBatch`, `SyncInterval`.
- Three backpressure modes: `Block`, `Drop`, `Error`.
- Group commit: writer drains all pending batches into a single `write()` syscall.
- `EventVec::Inline` optimization: single-event appends avoid `Vec<Event>` heap allocation.
- `LsnCounter::next_n` for batch LSN assignment in a single atomic operation.
- `FlushBarrier` stack-allocated (no `Arc` overhead).
- Writer thread wake via `AtomicPtr<Thread>` + `thread::park/unpark` (no `Condvar`).
- Hardware-accelerated CRC-32C: SSE4.2 (`_mm_crc32_u64`) with software fallback.
- Automatic crash recovery: truncate to last valid batch boundary on open.
- Lifecycle state machine: Init → Running → Draining → Closed with atomic CAS transitions.
- 11 observability hooks with panic recovery (`Hooks` trait via builder).
- Lock-free atomic `Stats` with 12 runtime metrics.
- 14 error variants, all `Debug + Display + Clone + Eq`.
- `scan_batch_header` for fast header-only frame scanning (no CRC, no record decode).
- Comprehensive test suite: 60 unit tests, 2 doc-tests, 21 benchmarks.
- 3 fuzz targets: `fuzz_decode_batch`, `fuzz_append_replay`, `fuzz_recovery_after_corruption`.
- All tests pass under Miri (pure-logic subset; I/O tests skipped).
- GitHub Actions CI: fmt, clippy, test (matrix: 3 OS × 2 Rust versions), miri, fuzz, benchmark.
- Coverage threshold check (80%) via cargo-tarpaulin.
- MSRV: Rust 1.81.
- Zero external dependencies (stdlib only). Dev-dependencies: `criterion`, `tempfile`.

[Unreleased]: https://github.com/aasyanov/rewal/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/aasyanov/rewal/releases/tag/v0.1.0
