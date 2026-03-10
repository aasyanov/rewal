use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::error::Result;
use crate::event::Lsn;
use crate::hooks::{Hooks, HooksRunner};

/// Determines when the writer thread calls fsync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// No explicit fsync. Maximum throughput, data in OS page cache only.
    Never,
    /// fsync after every write batch. Strongest durability.
    Batch,
    /// fsync at a fixed time interval.
    Interval(Duration),
}

/// Controls [`Wal::append`] behavior when the write queue is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backpressure {
    /// Block the caller until space is available (default).
    Block,
    /// Silently drop events; [`Stats::drops`] incremented.
    Drop,
    /// Return [`Error::QueueFull`] immediately.
    Error,
}

/// Optional compression for batch frame records.
///
/// The WAL never depends on any compression library — users provide their own
/// implementation wrapping zstd, lz4, snappy, or anything else.
pub trait Compressor: Send + Sync {
    fn compress(&self, src: &[u8], dst: &mut Vec<u8>) -> Result<()>;
    fn decompress(&self, src: &[u8], dst: &mut Vec<u8>) -> Result<()>;
}

/// Receives notifications when events are persisted to storage.
///
/// Called from the writer thread after a batch has been successfully written.
/// Panics in [`on_append`](Indexer::on_append) are recovered and silently
/// discarded.
pub trait Indexer: Send + Sync {
    fn on_append(&self, lsn: Lsn, meta: Option<&[u8]>, offset: u64);
}

// ── Internal config ─────────────────────────────────────────────────────────

pub(crate) const DEFAULT_QUEUE_CAPACITY: usize = 4096;
pub(crate) const DEFAULT_BUFFER_CAPACITY: usize = 64 * 1024;
pub(crate) struct Config {
    pub sync_mode: SyncMode,
    pub backpressure: Backpressure,
    pub queue_capacity: usize,
    pub buffer_capacity: usize,
    pub compressor: Option<Arc<dyn Compressor>>,
    pub indexer: Option<Box<dyn Indexer>>,
    pub hooks: HooksRunner,
}

impl Config {
    fn new() -> Self {
        Self {
            sync_mode: SyncMode::Never,
            backpressure: Backpressure::Block,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            buffer_capacity: DEFAULT_BUFFER_CAPACITY,
            compressor: None,
            indexer: None,
            hooks: HooksRunner::noop(),
        }
    }
}

// ── Builder ─────────────────────────────────────────────────────────────────

/// Configures and opens a [`Wal`](crate::Wal) instance.
///
/// Created by [`Wal::open`](crate::Wal::open). All parameters have sensible
/// defaults; call [`build`](WalBuilder::build) to finalize.
pub struct WalBuilder {
    pub(crate) path: PathBuf,
    pub(crate) config: Config,
}

impl WalBuilder {
    pub(crate) fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            config: Config::new(),
        }
    }

    /// Sets the durability mode.
    pub fn sync_mode(mut self, mode: SyncMode) -> Self {
        self.config.sync_mode = mode;
        self
    }

    /// Sets the backpressure strategy when the write queue is full.
    pub fn backpressure(mut self, mode: Backpressure) -> Self {
        self.config.backpressure = mode;
        self
    }

    /// Sets the write queue capacity (number of batches). Must be > 0.
    pub fn queue_capacity(mut self, n: usize) -> Self {
        if n > 0 {
            self.config.queue_capacity = n;
        }
        self
    }

    /// Sets the initial encoder buffer capacity in bytes. Must be > 0.
    pub fn buffer_capacity(mut self, n: usize) -> Self {
        if n > 0 {
            self.config.buffer_capacity = n;
        }
        self
    }

    /// Sets an optional compressor for batch frame compression.
    pub fn compressor(mut self, c: impl Compressor + 'static) -> Self {
        self.config.compressor = Some(Arc::new(c));
        self
    }

    /// Sets an optional indexer notified after each batch is persisted.
    pub fn indexer(mut self, idx: impl Indexer + 'static) -> Self {
        self.config.indexer = Some(Box::new(idx));
        self
    }

    /// Sets observability hooks.
    pub fn hooks(mut self, h: impl Hooks + 'static) -> Self {
        self.config.hooks = HooksRunner::new(Box::new(h));
        self
    }

    /// Opens or creates the WAL.
    pub fn build(self) -> Result<crate::wal::Wal> {
        crate::wal::Wal::open_with(self)
    }
}
