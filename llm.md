# REWAL — LLM Reference

> Crate: `rewal` | Rust 1.70+ | Zero dependencies | Wire-compatible with UEWAL (Go)

## CRITICAL RULES

1. Config via **builder pattern** from `Wal::open(path)` — never construct structs directly
2. Errors are an **enum** (`Error::Closed`, etc.) implementing `Debug + Display + Clone + Eq`
3. `append` returns **assigned** LSN; `last_lsn()` returns **persisted** LSN — they differ
4. `replay`/`iterator` payloads are **owned copies**; use `next_borrowed()` for zero-copy
5. **Single writer thread** — all writes serialized through write queue
6. **Single-process** — flock/LockFileEx prevents concurrent access

---

## API

```rust
// Lifecycle
let wal = Wal::open(path).build()?;
wal.shutdown(Duration::from_secs(5))?;  // graceful: drain → flush → sync → close
wal.close()?;                            // immediate: discard pending

// Write
let lsn = wal.append("payload")?;
let lsn = wal.append_with_meta("payload", "meta")?;
let lsn = wal.append_batch(batch)?;     // Batch::with_capacity(n) + add/add_with_meta

// Durability
wal.flush()?;                            // wait for writer to write pending batches
wal.sync()?;                             // fsync — flush+sync = full durability

// Read (zero-copy via mmap)
wal.replay(from_lsn, |ev| { ... Ok(()) })?;
let mut it = wal.iterator(from_lsn)?;   // drop or it.close() to release mmap

// Observe
let lsn = wal.last_lsn();               // persisted, not assigned
let s = wal.stats();                     // lock-free atomic snapshot
```

## BUILDER OPTIONS

```rust
Wal::open(path)
    .sync_mode(SyncMode::Never | Batch | Interval(Duration))  // default: Never
    .backpressure(Backpressure::Block | Drop | Error)          // default: Block
    .queue_capacity(4096)         // default: 4096
    .buffer_capacity(65536)       // default: 64KiB
    .compressor(impl Compressor)  // compress/decompress trait
    .indexer(impl Indexer)        // on_append(lsn, meta, offset) from writer thread
    .hooks(impl Hooks)            // 11 optional panic-safe callbacks
    .build()?
```

## ERRORS

```rust
Error::Closed, Draining, NotRunning, Corrupted, QueueFull,
       FileLocked, InvalidLsn, ShortWrite, InvalidRecord,
       CrcMismatch, InvalidState, EmptyBatch, CompressorRequired,
       Io(io::Error)
```

## TYPES

```rust
type Lsn = u64;
struct Event { lsn: Lsn, payload: Vec<u8>, meta: Option<Vec<u8>> }
struct BorrowedEvent<'a> { lsn: Lsn, payload: &'a [u8], meta: Option<&'a [u8]> }
struct Batch { /* add(), add_with_meta(), len(), is_empty(), clear() */ }
enum State { Init, Running, Draining, Closed }
```

---

## INTERNALS

### Pipeline

```
append → AtomicU64 LSN → WriteQueue → writer thread → Storage
                            ▲                │
                       FlushBarrier     encode → compress → write_all → maybe_sync → indexer
```

### Writer loop

```
dequeue_all_into → encode_all (for each batch) → flush_buffer → signal_all_barriers
```

- Group commit: `dequeue_all_into` drains ALL available batches in a single mutex acquisition
- `EventVec::Inline`: single events avoid heap allocation (no `Vec<Event>`)
- `maybe_sync`: Batch=every write, Interval=on elapsed time, Never=skip
- `notify_indexer`: re-decodes via `decode_batch_frame_borrowed` with reused buffer

### Recovery (open)

1. mmap file → sequential `decode_batch_header` (header + CRC) → track last valid offset
2. On CRC error: `truncate(last_valid)` — entire corrupted batch discarded
3. Restore LSN counter from header fields (first_lsn + count - 1)
4. Idempotent: crash during recovery → next open reaches same result

### Concurrency

- Append: any thread (single `AtomicU64::fetch_add` per batch for LSN)
- Writer: single thread (no mutex in encode/write, `dequeue_all_into` single lock)
- Replay/Iterator: any thread (mmap snapshot, `decode_batch_frame_borrowed`)
- Stats: any thread (atomic loads)
- FileStorage: mutex-protected for concurrent read+write
- Queue: consumer woken via `thread::park/unpark` + `AtomicPtr<Thread>` (not Condvar)

### Batch frame format (v2)

Header 24B: Magic("EWAL") + Version(2) + Flags + RecordCount + FirstLSN + BatchSize.
Per-record: PayloadLen(4B) + MetaLen(2B) + Meta + Payload.
Trailer: CRC32C(4B) covering header+records.
Compressed flag in Flags bit 0; CRC covers compressed bytes.

---

## MISTAKES TO AVOID

1. **Holding mmap'd data** — `BorrowedEvent` is only valid until next `next_borrowed()` call
2. **Assuming append=durable** — use `flush()` + `sync()` for durability
3. **Iterator after close** — returns `Error::Closed`
4. **Blocking Indexer** — stalls entire write pipeline
5. **Not closing Iterator** — leaks mmap mapping (Drop impl handles it, but explicit is better)
6. **Multi-process access** — flock prevents it; one WAL per process
7. **Nil compressor + compressed data** — `Error::CompressorRequired`
8. **Ignoring flush() error** — surfaces writer's last error

## PERFORMANCE NOTES (v0.1.0)

- **0 allocs/op** on append hot path (`EventVec::Inline`, `AtomicU64::fetch_add`)
- **Single atomic.fetch_add** per batch for LSN assignment (not per-event)
- **`dequeue_all_into`**: combined dequeue+drain in one mutex acquisition + spin-wait
- **`scan_batch_header`**: header-only scan (no CRC), fast forward scanning
- **`decode_batch_frame_borrowed`**: zero-copy decode, 0 allocs when uncompressed
- **`next_borrowed()`**: zero-copy iterator avoiding `.to_vec()` per event
- **FlushBarrier**: stack-allocated, no Arc overhead
- **CRC-32C**: SSE4.2 (x86_64) / ARM CRC (aarch64) with SW fallback
- **Encoding**: `ptr::write_unaligned` + `copy_nonoverlapping` for single-pass writes

## NOT IN SCOPE

No replication, no queries, no compaction, no log rotation, no multi-process, no async.
