//! # REWAL — Rust Embedded Write-Ahead Log
//!
//! Production-grade, zero-dependency, embedded WAL for Rust applications.
//!
//! REWAL is a strict, minimalist, high-performance Write-Ahead Log engine
//! designed for event sourcing, state machine recovery, embedded database
//! logs, durable queues, and high-frequency buffering.
//!
//! **Not** a distributed log, server, async runtime, or multi-process WAL.
//!
//! # Architecture
//!
//! The WAL is append-only, single-process, and uses a single writer thread.
//! Events submitted via [`Wal::append`] or [`Wal::append_batch`] are assigned
//! monotonic [`Lsn`] values and enqueued into a bounded write queue. The
//! writer thread drains the queue, encodes events into batch frames with
//! CRC-32C integrity, and writes them to the underlying [`Storage`].
//!
//! Group commit is performed automatically: the writer drains all immediately
//! available batches before issuing a single write system call.
//!
//! Replay uses memory-mapped I/O ([`Wal::replay`], [`Wal::iterator`]) for
//! zero-copy sequential reads.
//!
//! # Quick Start
//!
//! ```no_run
//! use rewal::{Wal, Batch, Result};
//! use std::time::Duration;
//!
//! fn main() -> Result<()> {
//!     let wal = Wal::open("/tmp/my.wal").build()?;
//!
//!     // Single event
//!     let lsn = wal.append("hello world")?;
//!
//!     // Batch write
//!     let mut batch = Batch::with_capacity(3);
//!     batch.add("event-1");
//!     batch.add_with_meta("event-2", "type:update");
//!     batch.add("event-3");
//!     wal.append_batch(batch)?;
//!
//!     // Replay
//!     wal.flush()?;
//!     wal.replay(0, |ev| {
//!         println!("LSN={} payload={:?}", ev.lsn, ev.payload);
//!         Ok(())
//!     })?;
//!
//!     wal.shutdown(Duration::from_secs(5))?;
//!     Ok(())
//! }
//! ```
//!
//! # Lifecycle
//!
//! ```text
//! Init → Running → Draining → Closed
//! ```
//!
//! [`Wal::shutdown`] performs a graceful shutdown (drain queue, flush, sync,
//! close). [`Wal::close`] performs an immediate close without draining.
//!
//! # Durability
//!
//! Controlled via [`SyncMode`]:
//! - [`SyncMode::Never`]: no fsync, OS page cache only (maximum throughput).
//! - [`SyncMode::Batch`]: fsync after every write batch (strongest guarantee).
//! - [`SyncMode::Interval`]: fsync at a configurable interval (balanced).
//!
//! [`Wal::flush`] waits for the writer to process all pending batches.
//! [`Wal::sync`] issues an fsync. For full durability: flush then sync.
//!
//! # Zero Dependencies
//!
//! REWAL has no external dependencies. CRC-32C uses hardware intrinsics
//! (SSE4.2 / ARM CRC) with a software fallback. Memory mapping uses
//! platform-native syscalls.

pub mod crc;
pub mod encoding;
mod error;
mod event;
mod hooks;
mod mmap;
mod options;
mod queue;
mod replay;
mod state;
mod stats;
mod storage;
mod sys;
mod wal;
mod writer;

pub use encoding::{
    decode_all_batches, decode_batch_frame_borrowed, scan_batch_header, BorrowedEvent,
};
pub use error::{Error, Result};
pub use event::{Batch, Event, Lsn};
pub use hooks::Hooks;
pub use options::{Backpressure, Compressor, Indexer, SyncMode, WalBuilder};
pub use replay::WalIterator;
pub use state::State;
pub use stats::Stats;
pub use storage::{FileStorage, Storage};
pub use wal::Wal;
