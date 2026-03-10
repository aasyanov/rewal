use std::sync::Arc;
use std::time::Instant;

use crate::encoding::Encoder;
use crate::error::{Error, Result};
use crate::hooks::HooksRunner;
use crate::options::{Compressor, Config, Indexer, SyncMode};
use crate::queue::{WriteBatch, WriteQueue};
use crate::stats::StatsCollector;
use crate::storage::Storage;

/// Single background thread responsible for persisting events to storage.
///
/// Implements group commit by draining all immediately available batches from
/// the queue into one write syscall. Not safe for concurrent use — only one
/// instance runs per WAL.
pub(crate) struct Writer {
    handle: Option<std::thread::JoinHandle<()>>,
}

/// Shared state accessible from both the writer thread and the WAL.
pub(crate) struct WriterShared {
    pub queue: Arc<WriteQueue>,
    pub stats: Arc<StatsCollector>,
    pub hooks: Arc<HooksRunner>,
}

struct WriterLoop {
    storage: Box<dyn Storage>,
    queue: Arc<WriteQueue>,
    enc: Encoder,
    stats: Arc<StatsCollector>,
    hooks: Arc<HooksRunner>,
    compressor: Option<Arc<dyn Compressor>>,
    indexer: Option<Box<dyn Indexer>>,
    sync_mode: SyncMode,
    sync_interval: Option<Instant>,
    batch_buf: Vec<WriteBatch>,
    decomp_buf: Vec<u8>,
    pending_sync_bytes: u64,
    write_offset: u64,
}

impl Writer {
    /// Creates and starts the writer thread.
    pub fn start(
        storage: Box<dyn Storage>,
        shared: &WriterShared,
        config: &mut Config,
        start_offset: u64,
    ) -> Self {
        let queue = shared.queue.clone();
        let stats = shared.stats.clone();
        let hooks = shared.hooks.clone();

        let compressor = config.compressor.clone();
        let indexer = config.indexer.take();
        let sync_mode = config.sync_mode;
        let buffer_capacity = config.buffer_capacity;
        let queue_capacity = config.queue_capacity;

        let handle = std::thread::Builder::new()
            .name("rewal-writer".into())
            .spawn(move || {
                let mut wl = WriterLoop {
                    storage,
                    queue,
                    enc: Encoder::new(buffer_capacity),
                    stats,
                    hooks,
                    compressor,
                    indexer,
                    sync_mode,
                    sync_interval: match sync_mode {
                        SyncMode::Interval(_) => Some(Instant::now()),
                        _ => None,
                    },
                    batch_buf: Vec::with_capacity(queue_capacity),
                    decomp_buf: Vec::new(),
                    pending_sync_bytes: 0,
                    write_offset: start_offset,
                };
                wl.run();
            })
            .expect("failed to spawn writer thread");

        Self {
            handle: Some(handle),
        }
    }

    /// Waits for the writer thread to finish. Must be called after
    /// [`WriteQueue::close`].
    pub fn join(&mut self) {
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }

    pub fn into_handle(mut self) -> std::thread::JoinHandle<()> {
        self.handle.take().expect("writer handle already consumed")
    }
}

impl WriterLoop {
    fn run(&mut self) {
        self.queue.register_consumer();
        loop {
            self.batch_buf.clear();
            if !self.queue.dequeue_all_into(&mut self.batch_buf) {
                return;
            }

            self.encode_all();
            self.flush_buffer();
            self.signal_all_barriers();
        }
    }

    fn encode_all(&mut self) {
        let comp = self.compressor.as_deref();

        for i in 0..self.batch_buf.len() {
            let batch = &self.batch_buf[i];
            let events = batch.events.as_slice();
            if events.is_empty() {
                continue;
            }

            let size_before = self.enc.as_bytes().len();
            if self
                .enc
                .encode_batch(events, batch.lsn_start, comp)
                .is_err()
            {
                return;
            }
            let encoded = self.enc.as_bytes().len() - size_before;

            self.stats.add_events(events.len() as u64);
            self.stats.add_batches(1);
            self.stats.store_lsn(batch.lsn_end);
            self.hooks.after_append(batch.lsn_end, events.len());

            if comp.is_some() {
                let uncompressed = crate::encoding::FRAME_OVERHEAD
                    + events
                        .iter()
                        .map(|e| 6 + e.payload.len() + e.meta.as_ref().map_or(0, |m| m.len()))
                        .sum::<usize>();
                if uncompressed > encoded {
                    self.stats.add_compressed((uncompressed - encoded) as u64);
                }
            }
        }
    }

    fn flush_buffer(&mut self) {
        let buf = self.enc.as_bytes();
        if buf.is_empty() {
            return;
        }

        self.hooks.before_write(buf.len());

        let write_offset = self.write_offset;
        match self.write_all() {
            Ok(n) => {
                self.hooks.after_write(n);
                self.stats.add_bytes(n as u64);
                self.write_offset += n as u64;

                if self.indexer.is_some() {
                    self.notify_indexer_impl(write_offset);
                }

                self.maybe_sync(n as u64);
            }
            Err(_) => {
                self.hooks.after_write(0);
            }
        }

        self.enc.reset();
    }

    fn write_all(&mut self) -> Result<usize> {
        let buf = self.enc.as_bytes();
        let mut total = 0;
        let mut remaining = buf;
        while !remaining.is_empty() {
            let n = self.storage.write(remaining)?;
            if n == 0 {
                return Err(Error::ShortWrite);
            }
            total += n;
            remaining = &remaining[n..];
        }
        Ok(total)
    }

    fn maybe_sync(&mut self, written: u64) {
        match self.sync_mode {
            SyncMode::Batch => self.do_sync(written),
            SyncMode::Interval(dur) => {
                self.pending_sync_bytes += written;
                let should_sync = self
                    .sync_interval
                    .as_ref()
                    .is_some_and(|last| last.elapsed() >= dur);
                if should_sync {
                    let bytes = self.pending_sync_bytes;
                    self.do_sync(bytes);
                    self.pending_sync_bytes = 0;
                    self.sync_interval = Some(Instant::now());
                }
            }
            SyncMode::Never => {}
        }
    }

    fn do_sync(&mut self, written: u64) {
        self.hooks.before_sync();
        let start = Instant::now();
        if self.storage.sync().is_ok() {
            self.stats.add_synced(written);
            self.stats.add_sync();
        }
        self.hooks.after_sync(start.elapsed());
    }

    fn signal_all_barriers(&self) {
        for batch in &self.batch_buf {
            if !batch.barrier.is_null() {
                unsafe { &*batch.barrier }.signal();
            }
        }
    }

    fn notify_indexer_impl(&mut self, base_offset: u64) {
        let buf = self.enc.as_bytes();
        let comp = self.compressor.as_deref();
        let mut off = 0;
        while off < buf.len() {
            match crate::encoding::decode_batch_frame_borrowed(buf, off, comp, &mut self.decomp_buf)
            {
                Ok((events, next)) => {
                    let frame_off = base_offset + off as u64;
                    let indexer = self.indexer.as_ref().unwrap();
                    for ev in &events {
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            indexer.on_append(ev.lsn, ev.meta, frame_off);
                        }));
                    }
                    off = next;
                }
                Err(_) => break,
            }
        }
    }
}
