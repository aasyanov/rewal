use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::encoding::decode_batch_header;
use crate::error::{Error, Result};
use crate::event::{Batch, Event, Lsn};
use crate::hooks::HooksRunner;
use crate::mmap::MmapReader;
use crate::options::{Backpressure, SyncMode, WalBuilder};
use crate::queue::{EventVec, FlushBarrier, WriteBatch, WriteQueue};
use crate::replay::{self, WalIterator};
use crate::state::{State, StateMachine};
use crate::stats::{Stats, StatsCollector};
use crate::storage::{FileStorage, Storage};
use crate::writer::{Writer, WriterShared};

/// The main write-ahead log.
///
/// Create instances exclusively via [`Wal::open`]. All methods are safe for
/// concurrent use from multiple threads.
///
/// A WAL progresses through four lifecycle states:
/// `Init → Running → Draining → Closed`.
/// Once closed, a WAL instance cannot be reopened. To continue using the
/// same file, call [`Wal::open`] again.
pub struct Wal {
    sm: StateMachine,
    lsn: LsnCounter,
    queue: Arc<WriteQueue>,
    stats: Arc<StatsCollector>,
    hooks: Arc<HooksRunner>,
    storage: FileStorage,
    writer: Mutex<Option<Writer>>,
    backpressure: Backpressure,
    sync_mode: SyncMode,
    compressor: Option<Arc<dyn crate::options::Compressor>>,
}

impl Wal {
    /// Starts building a WAL at the given path.
    ///
    /// Returns a [`WalBuilder`] to configure options before opening.
    /// Call [`build()`](WalBuilder::build) to finalize.
    ///
    /// ```no_run
    /// # use rewal::{Wal, Result};
    /// # fn main() -> Result<()> {
    /// let wal = Wal::open("/tmp/my.wal").build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open(path: impl AsRef<Path>) -> WalBuilder {
        WalBuilder::new(path)
    }

    /// Internal constructor called from [`WalBuilder::build`].
    pub(crate) fn open_with(mut builder: WalBuilder) -> Result<Self> {
        let mut storage = FileStorage::new(&builder.path)?;

        let stats = Arc::new(StatsCollector::new());
        let hooks = Arc::new(std::mem::replace(
            &mut builder.config.hooks,
            HooksRunner::noop(),
        ));

        let lsn = LsnCounter::new();

        // Recovery: scan batch frame headers only (no event deserialization).
        {
            let size = storage.size()?;
            if size > 0 {
                let reader = MmapReader::from_file_storage(&storage, size)?;
                let data = reader.as_bytes();
                let mut off = 0;
                let mut last_lsn: Lsn = 0;
                let mut last_valid = 0;
                let mut corrupted = false;

                while off < data.len() {
                    match decode_batch_header(data, off) {
                        Ok((count, first_lsn, total_size)) => {
                            let batch_last = first_lsn + count as u64 - 1;
                            if batch_last > last_lsn {
                                last_lsn = batch_last;
                            }
                            last_valid = off + total_size;
                            off = last_valid;
                        }
                        Err(_) => {
                            corrupted = true;
                            break;
                        }
                    }
                }

                drop(reader);

                if corrupted {
                    stats.add_corruption();
                    hooks.on_corruption(last_valid as u64);
                    Storage::truncate(&mut storage, last_valid as u64)?;
                }

                lsn.store(last_lsn);
                stats.store_lsn(last_lsn);
            }
        }

        let queue = Arc::new(WriteQueue::new(builder.config.queue_capacity));
        let start_offset = storage.size()?;

        let sm = StateMachine::new();

        let shared = WriterShared {
            queue: queue.clone(),
            stats: stats.clone(),
            hooks: hooks.clone(),
        };

        let writer_file = storage.try_clone_file().ok_or(Error::Closed)??;

        let writer_storage = WriteStorage::new(writer_file);

        let compressor_for_replay = builder.config.compressor.clone();

        let writer = Writer::start(
            Box::new(writer_storage),
            &shared,
            &mut builder.config,
            start_offset,
        );

        if !sm.transition(State::Init, State::Running) {
            return Err(Error::InvalidState);
        }

        hooks.on_start();

        Ok(Self {
            sm,
            lsn,
            queue,
            stats,
            hooks,
            storage,
            writer: Mutex::new(Some(writer)),
            backpressure: builder.config.backpressure,
            sync_mode: builder.config.sync_mode,
            compressor: compressor_for_replay,
        })
    }

    // ── Append ──────────────────────────────────────────────────────────────

    /// Writes a single event. Returns the assigned LSN.
    pub fn append(&self, payload: impl Into<Vec<u8>>) -> Result<Lsn> {
        self.sm.must_be_running()?;

        let lsn = self.lsn.next();
        let ev = Event {
            lsn,
            payload: payload.into(),
            meta: None,
        };

        let wb = WriteBatch {
            events: EventVec::Inline(ev),
            lsn_start: lsn,
            lsn_end: lsn,
            barrier: std::ptr::null(),
        };

        self.enqueue_batch(wb, lsn)
    }

    /// Writes a single event with metadata. Returns the assigned LSN.
    pub fn append_with_meta(
        &self,
        payload: impl Into<Vec<u8>>,
        meta: impl Into<Vec<u8>>,
    ) -> Result<Lsn> {
        self.sm.must_be_running()?;

        let lsn = self.lsn.next();
        let ev = Event {
            lsn,
            payload: payload.into(),
            meta: Some(meta.into()),
        };

        let wb = WriteBatch {
            events: EventVec::Inline(ev),
            lsn_start: lsn,
            lsn_end: lsn,
            barrier: std::ptr::null(),
        };

        self.enqueue_batch(wb, lsn)
    }

    /// Writes a batch of events atomically. Returns the last assigned LSN.
    ///
    /// Takes `Batch` by value to avoid cloning payloads (move semantics).
    /// Build a new `Batch` for each call, or `.clone()` explicitly if reuse is needed.
    pub fn append_batch(&self, batch: Batch) -> Result<Lsn> {
        if batch.is_empty() {
            return Err(Error::EmptyBatch);
        }
        self.sm.must_be_running()?;

        let count = batch.events.len();
        let first_lsn = self.lsn.next_n(count as u64);
        let last_lsn = first_lsn + count as u64 - 1;

        let mut events = batch.events;
        let mut lsn = first_lsn;
        for ev in events.iter_mut() {
            ev.lsn = lsn;
            lsn += 1;
        }

        let wb = WriteBatch {
            events: EventVec::Heap(events),
            lsn_start: first_lsn,
            lsn_end: last_lsn,
            barrier: std::ptr::null(),
        };

        self.enqueue_batch(wb, last_lsn)
    }

    #[inline]
    fn enqueue_batch(&self, wb: WriteBatch, last_lsn: Lsn) -> Result<Lsn> {
        self.hooks.before_append(wb.events.as_slice());

        match self.backpressure {
            Backpressure::Block => {
                if !self.queue.enqueue(wb) {
                    return Err(Error::Closed);
                }
            }
            Backpressure::Drop => {
                if let Err(wb) = self.queue.try_enqueue(wb) {
                    let count = wb.events.len();
                    self.stats.add_drop(count as u64);
                    self.hooks.on_drop(count);
                    return Ok(last_lsn);
                }
            }
            Backpressure::Error => {
                if self.queue.try_enqueue(wb).is_err() {
                    return Err(Error::QueueFull);
                }
            }
        }

        Ok(last_lsn)
    }

    // ── Flush & Sync ────────────────────────────────────────────────────────

    /// Blocks until the writer has processed all queued batches.
    ///
    /// Does NOT call fsync. For full durability: `flush()` then `sync()`.
    pub fn flush(&self) -> Result<()> {
        self.sm.must_be_running()?;

        let barrier = FlushBarrier::new();
        let wb = WriteBatch {
            events: EventVec::Empty,
            lsn_start: 0,
            lsn_end: 0,
            barrier: &barrier as *const FlushBarrier,
        };

        if !self.queue.enqueue(wb) {
            return Err(Error::Closed);
        }

        barrier.wait();

        Ok(())
    }

    /// Calls fsync on the underlying storage, making written data durable.
    pub fn sync(&self) -> Result<()> {
        self.sm.must_be_running()?;
        self.storage
            .with_file(|f| f.sync_all())
            .ok_or(Error::Closed)?
            .map_err(Error::Io)
    }

    // ── Read ────────────────────────────────────────────────────────────────

    /// Reads all events with LSN >= `from`, calling `f` for each.
    ///
    /// Uses mmap for zero-copy access. The [`Event`] passed to `f` has
    /// payload and meta that are owned copies; they are safe to keep.
    pub fn replay(&self, from: Lsn, f: impl FnMut(Event) -> Result<()>) -> Result<()> {
        match self.sm.load() {
            State::Init => return Err(Error::NotRunning),
            State::Closed => return Err(Error::Closed),
            _ => {}
        }

        replay::replay_callback(
            &self.storage,
            from,
            f,
            self.compressor.as_deref(),
            &self.stats,
            &self.hooks,
        )
    }

    /// Returns a pull-based iterator starting from the given LSN.
    pub fn iterator(&self, from: Lsn) -> Result<WalIterator> {
        match self.sm.load() {
            State::Init => return Err(Error::NotRunning),
            State::Closed => return Err(Error::Closed),
            _ => {}
        }

        replay::new_iterator(&self.storage, from, self.compressor.clone())
    }

    // ── Info ────────────────────────────────────────────────────────────────

    /// Most recently persisted LSN. May lag behind `append` return values.
    #[inline]
    pub fn last_lsn(&self) -> Lsn {
        self.stats.load_lsn()
    }

    /// Point-in-time snapshot of WAL statistics.
    pub fn stats(&self) -> Stats {
        let state = self.sm.load();
        let (file_size, queue_len) = if state != State::Closed {
            (self.storage.size().unwrap_or(0), self.queue.len())
        } else {
            (0, 0)
        };
        self.stats.snapshot(queue_len, file_size, state)
    }

    // ── Lifecycle ───────────────────────────────────────────────────────────

    /// Graceful shutdown: drains queue, flushes, syncs, closes storage.
    ///
    /// Transitions to Draining (new appends rejected), then to Closed.
    /// Respects the timeout — returns [`Error::Io`] on timeout.
    /// Idempotent: calling on a closed WAL returns `Ok(())`.
    pub fn shutdown(&self, timeout: Duration) -> Result<()> {
        if self.sm.load() == State::Closed {
            return Ok(());
        }

        if !self.sm.transition(State::Running, State::Draining) {
            let current = self.sm.load();
            if current == State::Closed || current == State::Draining {
                return Ok(());
            }
            return Err(Error::InvalidState);
        }

        let start = Instant::now();
        self.hooks.on_shutdown_start();

        self.queue.close();

        {
            let mut writer_guard = self.writer.lock().unwrap();
            if let Some(writer) = writer_guard.take() {
                let handle = writer.into_handle();
                if timeout.is_zero() {
                    handle.join().ok();
                } else {
                    use std::sync::atomic::{AtomicBool, Ordering as AO};
                    let remaining = timeout.saturating_sub(start.elapsed());
                    let done = Arc::new(AtomicBool::new(false));
                    let done2 = done.clone();
                    let caller = std::thread::current();
                    std::thread::spawn(move || {
                        handle.join().ok();
                        done2.store(true, AO::Release);
                        caller.unpark();
                    });
                    let deadline = Instant::now() + remaining;
                    while !done.load(AO::Acquire) {
                        let left = deadline.saturating_duration_since(Instant::now());
                        if left.is_zero() {
                            break;
                        }
                        std::thread::park_timeout(left);
                    }
                    if !done.load(AO::Acquire) {
                        self.sm.transition(State::Draining, State::Closed);
                        self.hooks.on_shutdown_done(start.elapsed());
                        return Err(Error::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "shutdown timed out waiting for writer",
                        )));
                    }
                }
            }
        }

        if self.sync_mode != SyncMode::Never {
            self.storage.with_file(|f| {
                let _ = f.sync_all();
            });
        }

        self.sm.transition(State::Draining, State::Closed);
        self.hooks.on_shutdown_done(start.elapsed());

        Ok(())
    }

    /// Immediate close without draining the write queue.
    ///
    /// Pending batches are discarded. For graceful shutdown, use
    /// [`shutdown`](Wal::shutdown).
    /// Idempotent.
    pub fn close(&self) -> Result<()> {
        if self.sm.load() == State::Closed {
            return Ok(());
        }

        let current = self.sm.load();
        if current == State::Running {
            self.sm.transition(State::Running, State::Draining);
        }

        self.queue.close();

        {
            let mut writer_guard = self.writer.lock().unwrap();
            if let Some(mut writer) = writer_guard.take() {
                writer.join();
            }
        }

        self.sm.transition(State::Draining, State::Closed);
        Ok(())
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

// ── LSN counter ─────────────────────────────────────────────────────────────

struct LsnCounter(AtomicU64);

impl LsnCounter {
    const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    #[inline]
    fn next(&self) -> Lsn {
        self.0.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Reserves `n` contiguous LSNs in a single atomic op. Returns the first.
    #[inline]
    fn next_n(&self, n: u64) -> Lsn {
        self.0.fetch_add(n, Ordering::Relaxed) + 1
    }

    fn store(&self, v: Lsn) {
        self.0.store(v, Ordering::Relaxed);
    }
}

// ── WriteStorage ────────────────────────────────────────────────────────────

/// Minimal Storage wrapper around a cloned File, used by the writer thread.
struct WriteStorage {
    file: Option<std::fs::File>,
}

impl WriteStorage {
    fn new(file: std::fs::File) -> Self {
        Self { file: Some(file) }
    }
}

impl Storage for WriteStorage {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        use std::io::Write;
        let f = self.file.as_mut().ok_or(Error::Closed)?;
        Ok(f.write(buf)?)
    }

    fn sync(&mut self) -> Result<()> {
        let f = self.file.as_ref().ok_or(Error::Closed)?;
        Ok(f.sync_all()?)
    }

    fn close(&mut self) -> Result<()> {
        self.file = None;
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        let f = self.file.as_ref().ok_or(Error::Closed)?;
        Ok(f.metadata()?.len())
    }

    fn truncate(&mut self, size: u64) -> Result<()> {
        let f = self.file.as_ref().ok_or(Error::Closed)?;
        Ok(f.set_len(size)?)
    }

    fn read_at(&self, _buf: &mut [u8], _offset: u64) -> Result<usize> {
        Err(Error::Closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "rewal_{name}_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn open_append_replay() {
        let path = temp_path("open_append_replay");
        let _ = std::fs::remove_file(&path);

        let wal = Wal::open(&path).build().unwrap();
        let lsn1 = wal.append("hello").unwrap();
        let lsn2 = wal.append_with_meta("world", "tag").unwrap();
        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);

        wal.flush().unwrap();

        let mut events = Vec::new();
        wal.replay(0, |ev| {
            events.push(ev);
            Ok(())
        })
        .unwrap();

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].payload, b"hello");
        assert_eq!(events[1].payload, b"world");
        assert_eq!(events[1].meta.as_deref(), Some(b"tag".as_slice()));

        wal.shutdown(Duration::from_secs(5)).unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn batch_append() {
        let path = temp_path("batch_append");
        let _ = std::fs::remove_file(&path);

        let wal = Wal::open(&path).build().unwrap();
        let mut batch = Batch::with_capacity(3);
        batch.add("a");
        batch.add("b");
        batch.add("c");
        let lsn = wal.append_batch(batch).unwrap();
        assert_eq!(lsn, 3);

        wal.flush().unwrap();

        let mut count = 0;
        wal.replay(0, |_| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 3);

        wal.shutdown(Duration::from_secs(5)).unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn iterator_read() {
        let path = temp_path("iterator");
        let _ = std::fs::remove_file(&path);

        let wal = Wal::open(&path).build().unwrap();
        for i in 0..10 {
            wal.append(format!("event-{i}")).unwrap();
        }
        wal.flush().unwrap();

        let mut it = wal.iterator(5).unwrap();
        let mut lsns = Vec::new();
        while let Ok(Some(ev)) = it.next() {
            lsns.push(ev.lsn);
        }
        assert_eq!(lsns, vec![5, 6, 7, 8, 9, 10]);

        wal.shutdown(Duration::from_secs(5)).unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn stats_tracking() {
        let path = temp_path("stats");
        let _ = std::fs::remove_file(&path);

        let wal = Wal::open(&path).build().unwrap();
        wal.append("x").unwrap();
        wal.flush().unwrap();

        let s = wal.stats();
        assert_eq!(s.state, State::Running);
        assert!(s.events_written >= 1);
        assert!(s.last_lsn >= 1);

        wal.shutdown(Duration::from_secs(5)).unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn shutdown_idempotent() {
        let path = temp_path("shutdown_idempotent");
        let _ = std::fs::remove_file(&path);

        let wal = Wal::open(&path).build().unwrap();
        wal.shutdown(Duration::from_secs(1)).unwrap();
        wal.shutdown(Duration::from_secs(1)).unwrap();

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn close_immediate() {
        let path = temp_path("close_immediate");
        let _ = std::fs::remove_file(&path);

        let wal = Wal::open(&path).build().unwrap();
        wal.append("data").unwrap();
        wal.close().unwrap();

        assert_eq!(wal.stats().state, State::Closed);
        assert!(wal.append("more").is_err());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn recovery_after_reopen() {
        let path = temp_path("recovery");
        let _ = std::fs::remove_file(&path);

        {
            let wal = Wal::open(&path).build().unwrap();
            wal.append("event-1").unwrap();
            wal.append("event-2").unwrap();
            wal.append("event-3").unwrap();
            wal.flush().unwrap();
            wal.shutdown(Duration::from_secs(5)).unwrap();
        }

        {
            let wal = Wal::open(&path).build().unwrap();
            assert_eq!(wal.last_lsn(), 3);

            let lsn = wal.append("event-4").unwrap();
            assert_eq!(lsn, 4);

            wal.flush().unwrap();

            let mut count = 0;
            wal.replay(0, |_| {
                count += 1;
                Ok(())
            })
            .unwrap();
            assert_eq!(count, 4);

            wal.shutdown(Duration::from_secs(5)).unwrap();
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn empty_batch_rejected() {
        let path = temp_path("empty_batch");
        let _ = std::fs::remove_file(&path);

        let wal = Wal::open(&path).build().unwrap();
        let batch = Batch::new();
        assert_eq!(wal.append_batch(batch).unwrap_err(), Error::EmptyBatch);

        wal.shutdown(Duration::from_secs(1)).unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn concurrent_appends() {
        let path = temp_path("concurrent");
        let _ = std::fs::remove_file(&path);

        let wal = Arc::new(Wal::open(&path).build().unwrap());
        let per_thread = 100;
        let threads = 4;

        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let w = wal.clone();
                std::thread::spawn(move || {
                    for i in 0..per_thread {
                        w.append(format!("t{t}-e{i}")).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        wal.flush().unwrap();

        let mut count = 0u64;
        wal.replay(0, |_| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, (threads * per_thread) as u64);

        wal.shutdown(Duration::from_secs(5)).unwrap();
        let _ = std::fs::remove_file(&path);
    }
}
