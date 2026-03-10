use std::time::Duration;

use crate::event::{Event, Lsn};

/// Observability callbacks for WAL lifecycle and pipeline events.
///
/// All methods have default no-op implementations. Panics within any hook
/// are recovered and silently discarded — a panicking hook never affects
/// WAL consistency or crashes the writer thread.
///
/// Hooks are invoked synchronously: pipeline hooks from the writer thread,
/// lifecycle hooks from the caller thread. Avoid blocking for extended
/// periods, as this directly impacts write throughput.
pub trait Hooks: Send + Sync {
    /// Called once after open successfully transitions to Running.
    fn on_start(&self) {}
    /// Called when shutdown begins (Running → Draining).
    fn on_shutdown_start(&self) {}
    /// Called when shutdown completes. `elapsed` measures total shutdown time.
    fn on_shutdown_done(&self, _elapsed: Duration) {}
    /// Called before a batch is enqueued to the writer.
    fn before_append(&self, _events: &[Event]) {}
    /// Called by the writer after a batch has been encoded.
    fn after_append(&self, _lsn: Lsn, _count: usize) {}
    /// Called before a write syscall. `size` is the buffer length in bytes.
    fn before_write(&self, _size: usize) {}
    /// Called after a write syscall. `written` is the number of bytes written.
    fn after_write(&self, _written: usize) {}
    /// Called before an fsync (from automatic sync logic only).
    fn before_sync(&self) {}
    /// Called after an fsync completes.
    fn after_sync(&self, _elapsed: Duration) {}
    /// Called when corruption is detected. `offset` is the first invalid byte.
    fn on_corruption(&self, _offset: u64) {}
    /// Called in Drop mode when events are discarded.
    fn on_drop(&self, _count: usize) {}
}

/// No-op hooks implementation used as the default.
pub(crate) struct NoopHooks;
impl Hooks for NoopHooks {}

/// Wraps a [`Hooks`] trait object with panic-safe invocation.
///
/// When no user hooks are registered (the default), all methods are inlined
/// to a no-op via the `is_noop` flag, avoiding `catch_unwind` overhead on
/// the hot path.
pub(crate) struct HooksRunner {
    inner: Box<dyn Hooks>,
    is_noop: bool,
}

impl HooksRunner {
    pub fn new(hooks: Box<dyn Hooks>) -> Self {
        Self {
            inner: hooks,
            is_noop: false,
        }
    }

    pub fn noop() -> Self {
        Self {
            inner: Box::new(NoopHooks),
            is_noop: true,
        }
    }

    #[inline]
    pub fn on_start(&self) {
        if self.is_noop {
            return;
        }
        safe_call(|| self.inner.on_start());
    }
    #[inline]
    pub fn on_shutdown_start(&self) {
        if self.is_noop {
            return;
        }
        safe_call(|| self.inner.on_shutdown_start());
    }
    #[inline]
    pub fn on_shutdown_done(&self, elapsed: Duration) {
        if self.is_noop {
            return;
        }
        safe_call(|| self.inner.on_shutdown_done(elapsed));
    }
    #[inline]
    pub fn before_append(&self, events: &[Event]) {
        if self.is_noop {
            return;
        }
        safe_call(|| self.inner.before_append(events));
    }
    #[inline]
    pub fn after_append(&self, lsn: Lsn, count: usize) {
        if self.is_noop {
            return;
        }
        safe_call(|| self.inner.after_append(lsn, count));
    }
    #[inline]
    pub fn before_write(&self, size: usize) {
        if self.is_noop {
            return;
        }
        safe_call(|| self.inner.before_write(size));
    }
    #[inline]
    pub fn after_write(&self, written: usize) {
        if self.is_noop {
            return;
        }
        safe_call(|| self.inner.after_write(written));
    }
    #[inline]
    pub fn before_sync(&self) {
        if self.is_noop {
            return;
        }
        safe_call(|| self.inner.before_sync());
    }
    #[inline]
    pub fn after_sync(&self, elapsed: Duration) {
        if self.is_noop {
            return;
        }
        safe_call(|| self.inner.after_sync(elapsed));
    }
    #[inline]
    pub fn on_corruption(&self, offset: u64) {
        if self.is_noop {
            return;
        }
        safe_call(|| self.inner.on_corruption(offset));
    }
    #[inline]
    pub fn on_drop(&self, count: usize) {
        if self.is_noop {
            return;
        }
        safe_call(|| self.inner.on_drop(count));
    }
}

#[inline]
fn safe_call<F: FnOnce()>(f: F) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct CountingHooks {
        starts: AtomicUsize,
        appends: AtomicUsize,
    }

    impl CountingHooks {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                starts: AtomicUsize::new(0),
                appends: AtomicUsize::new(0),
            })
        }
    }

    impl Hooks for Arc<CountingHooks> {
        fn on_start(&self) {
            self.starts.fetch_add(1, Ordering::Relaxed);
        }
        fn after_append(&self, _lsn: Lsn, _count: usize) {
            self.appends.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn hooks_fire() {
        let h = CountingHooks::new();
        let runner = HooksRunner::new(Box::new(h.clone()));
        runner.on_start();
        runner.on_start();
        runner.after_append(1, 1);
        assert_eq!(h.starts.load(Ordering::Relaxed), 2);
        assert_eq!(h.appends.load(Ordering::Relaxed), 1);
    }

    struct PanicHooks;
    impl Hooks for PanicHooks {
        fn on_start(&self) {
            panic!("boom");
        }
    }

    #[test]
    fn panic_recovered() {
        let runner = HooksRunner::new(Box::new(PanicHooks));
        runner.on_start(); // must not panic
    }

    #[test]
    fn noop_hooks() {
        let runner = HooksRunner::noop();
        runner.on_start();
        runner.before_sync();
        runner.on_drop(100);
    }
}
