use std::sync::atomic::{AtomicU64, Ordering};

use crate::event::Lsn;
use crate::state::State;

/// Point-in-time snapshot of WAL runtime statistics.
///
/// Each field is individually consistent but the snapshot as a whole is not
/// globally atomic (no lock). Safe to call from any thread, in any state.
#[derive(Debug, Clone)]
pub struct Stats {
    pub events_written: u64,
    pub batches_written: u64,
    pub bytes_written: u64,
    pub bytes_synced: u64,
    pub sync_count: u64,
    pub compressed_bytes: u64,
    pub drops: u64,
    pub corruptions: u64,
    pub queue_len: usize,
    pub file_size: u64,
    pub last_lsn: Lsn,
    pub state: State,
}

/// Lock-free atomic counters updated by the writer thread and read by
/// [`Wal::stats`].
pub(crate) struct StatsCollector {
    events_written: AtomicU64,
    batches_written: AtomicU64,
    bytes_written: AtomicU64,
    bytes_synced: AtomicU64,
    sync_count: AtomicU64,
    compressed_bytes: AtomicU64,
    drops: AtomicU64,
    corruptions: AtomicU64,
    last_lsn: AtomicU64,
}

impl StatsCollector {
    pub const fn new() -> Self {
        Self {
            events_written: AtomicU64::new(0),
            batches_written: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            bytes_synced: AtomicU64::new(0),
            sync_count: AtomicU64::new(0),
            compressed_bytes: AtomicU64::new(0),
            drops: AtomicU64::new(0),
            corruptions: AtomicU64::new(0),
            last_lsn: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn add_events(&self, n: u64) {
        self.events_written.fetch_add(n, Ordering::Relaxed);
    }
    #[inline]
    pub fn add_batches(&self, n: u64) {
        self.batches_written.fetch_add(n, Ordering::Relaxed);
    }
    #[inline]
    pub fn add_bytes(&self, n: u64) {
        self.bytes_written.fetch_add(n, Ordering::Relaxed);
    }
    #[inline]
    pub fn add_synced(&self, n: u64) {
        self.bytes_synced.fetch_add(n, Ordering::Relaxed);
    }
    #[inline]
    pub fn add_sync(&self) {
        self.sync_count.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn add_compressed(&self, n: u64) {
        self.compressed_bytes.fetch_add(n, Ordering::Relaxed);
    }
    #[inline]
    pub fn add_drop(&self, n: u64) {
        self.drops.fetch_add(n, Ordering::Relaxed);
    }
    #[inline]
    pub fn add_corruption(&self) {
        self.corruptions.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn store_lsn(&self, lsn: Lsn) {
        self.last_lsn.store(lsn, Ordering::Relaxed);
    }
    #[inline]
    pub fn load_lsn(&self) -> Lsn {
        self.last_lsn.load(Ordering::Relaxed)
    }

    /// Captures a snapshot. `queue_len`, `file_size`, and `state` are provided
    /// by the caller because they come from separate subsystems.
    pub fn snapshot(&self, queue_len: usize, file_size: u64, state: State) -> Stats {
        Stats {
            events_written: self.events_written.load(Ordering::Relaxed),
            batches_written: self.batches_written.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            bytes_synced: self.bytes_synced.load(Ordering::Relaxed),
            sync_count: self.sync_count.load(Ordering::Relaxed),
            compressed_bytes: self.compressed_bytes.load(Ordering::Relaxed),
            drops: self.drops.load(Ordering::Relaxed),
            corruptions: self.corruptions.load(Ordering::Relaxed),
            queue_len,
            file_size,
            last_lsn: self.last_lsn.load(Ordering::Relaxed),
            state,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_values() {
        let sc = StatsCollector::new();
        let s = sc.snapshot(0, 0, State::Init);
        assert_eq!(s.events_written, 0);
        assert_eq!(s.last_lsn, 0);
        assert_eq!(s.state, State::Init);
    }

    #[test]
    fn increments() {
        let sc = StatsCollector::new();
        sc.add_events(10);
        sc.add_batches(2);
        sc.add_bytes(1024);
        sc.add_synced(512);
        sc.add_sync();
        sc.add_drop(3);
        sc.add_corruption();
        sc.store_lsn(42);

        let s = sc.snapshot(5, 2048, State::Running);
        assert_eq!(s.events_written, 10);
        assert_eq!(s.batches_written, 2);
        assert_eq!(s.bytes_written, 1024);
        assert_eq!(s.bytes_synced, 512);
        assert_eq!(s.sync_count, 1);
        assert_eq!(s.drops, 3);
        assert_eq!(s.corruptions, 1);
        assert_eq!(s.queue_len, 5);
        assert_eq!(s.file_size, 2048);
        assert_eq!(s.last_lsn, 42);
        assert_eq!(s.state, State::Running);
    }

    #[test]
    fn lsn_store_load() {
        let sc = StatsCollector::new();
        assert_eq!(sc.load_lsn(), 0);
        sc.store_lsn(999);
        assert_eq!(sc.load_lsn(), 999);
    }
}
