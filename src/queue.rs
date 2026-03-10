use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::Mutex;

use crate::event::{Event, Lsn};

/// Lightweight flush barrier using thread park/unpark.
/// Avoids Arc overhead: the caller parks and the writer unparks via stored handle.
pub(crate) struct FlushBarrier {
    done: AtomicBool,
    thread: std::thread::Thread,
}

impl FlushBarrier {
    #[inline]
    pub fn new() -> Self {
        Self {
            done: AtomicBool::new(false),
            thread: std::thread::current(),
        }
    }

    #[inline]
    pub fn signal(&self) {
        self.done.store(true, Ordering::Release);
        self.thread.unpark();
    }

    #[inline]
    pub fn wait(&self) {
        while !self.done.load(Ordering::Acquire) {
            std::thread::park();
        }
    }
}

/// Inline-or-heap event storage. Single events (the common case) avoid
/// heap allocation entirely.
pub(crate) enum EventVec {
    Inline(Event),
    Heap(Vec<Event>),
    Empty,
}

impl EventVec {
    #[inline]
    pub fn as_slice(&self) -> &[Event] {
        match self {
            EventVec::Inline(ev) => std::slice::from_ref(ev),
            EventVec::Heap(v) => v.as_slice(),
            EventVec::Empty => &[],
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            EventVec::Inline(_) => 1,
            EventVec::Heap(v) => v.len(),
            EventVec::Empty => 0,
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        matches!(self, EventVec::Empty)
    }
}

/// A single unit of work sent from [`Wal::append`] to the writer thread.
pub(crate) struct WriteBatch {
    pub events: EventVec,
    pub lsn_start: Lsn,
    pub lsn_end: Lsn,
    /// Raw pointer to a FlushBarrier on the caller's stack. The writer
    /// signals it after this batch (and all prior) have been written.
    /// Safety: the caller (`Wal::flush`) blocks until signaled, so the
    /// barrier outlives the writer's use of this pointer.
    pub barrier: *const FlushBarrier,
}

unsafe impl Send for WriteBatch {}

struct Inner {
    items: Vec<Option<WriteBatch>>,
    head: usize,
    tail: usize,
    count: usize,
    closed: bool,
}

/// Bounded, blocking FIFO queue transferring [`WriteBatch`]es from append
/// callers to the single writer thread.
///
/// Uses a Mutex-protected circular buffer. The consumer (writer thread) is
/// woken via `thread::unpark` instead of a Condvar for minimal wake latency.
/// Producers blocked on a full queue still use a Condvar for `not_full`.
pub(crate) struct WriteQueue {
    mu: Mutex<Inner>,
    not_full: std::sync::Condvar,
    cap: usize,
    /// Lock-free count mirroring inner.count for spin-polling without locking.
    fast_count: AtomicUsize,
    /// Lock-free closed flag for spin-polling.
    fast_closed: AtomicBool,
    /// Lock-free pointer to the consumer Thread handle (heap-allocated).
    /// Null until register_consumer() is called.
    consumer: AtomicPtr<std::thread::Thread>,
}

impl WriteQueue {
    /// Creates a queue with the given capacity (number of batches).
    pub fn new(capacity: usize) -> Self {
        let mut items = Vec::with_capacity(capacity);
        items.resize_with(capacity, || None);
        Self {
            mu: Mutex::new(Inner {
                items,
                head: 0,
                tail: 0,
                count: 0,
                closed: false,
            }),
            not_full: std::sync::Condvar::new(),
            cap: capacity,
            fast_count: AtomicUsize::new(0),
            fast_closed: AtomicBool::new(false),
            consumer: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    /// Registers the consumer (writer) thread for park/unpark wakeups.
    /// Must be called from the writer thread before the first dequeue.
    pub fn register_consumer(&self) {
        let handle = Box::new(std::thread::current());
        let old = self.consumer.swap(Box::into_raw(handle), Ordering::Release);
        if !old.is_null() {
            unsafe {
                drop(Box::from_raw(old));
            }
        }
    }

    #[inline(always)]
    fn wake_consumer(&self) {
        let p = self.consumer.load(Ordering::Acquire);
        if !p.is_null() {
            unsafe {
                (*p).unpark();
            }
        }
    }

    /// Blocking enqueue. Returns `false` if the queue has been closed.
    pub fn enqueue(&self, batch: WriteBatch) -> bool {
        let mut inner = self.mu.lock().unwrap();
        while inner.count == self.cap && !inner.closed {
            inner = self.not_full.wait(inner).unwrap();
        }
        if inner.closed {
            return false;
        }
        let tail = inner.tail;
        inner.items[tail] = Some(batch);
        inner.tail = (tail + 1) % self.cap;
        inner.count += 1;
        self.fast_count.store(inner.count, Ordering::Release);
        drop(inner);
        self.wake_consumer();
        true
    }

    /// Non-blocking enqueue. Returns the batch back on failure.
    #[inline]
    pub fn try_enqueue(&self, batch: WriteBatch) -> core::result::Result<(), WriteBatch> {
        let mut inner = self.mu.lock().unwrap();
        if inner.closed || inner.count == self.cap {
            return Err(batch);
        }
        let tail = inner.tail;
        inner.items[tail] = Some(batch);
        inner.tail = (tail + 1) % self.cap;
        inner.count += 1;
        self.fast_count.store(inner.count, Ordering::Release);
        drop(inner);
        self.wake_consumer();
        Ok(())
    }

    /// Blocks until at least one item is available, then drains ALL
    /// immediately available items into `buf` in a single lock acquisition.
    /// Returns `false` only when the queue is closed AND empty.
    ///
    /// Spins briefly on an atomic counter before parking to reduce wake
    /// latency for flush-heavy workloads.
    pub fn dequeue_all_into(&self, buf: &mut Vec<WriteBatch>) -> bool {
        const SPIN_ITERS: u32 = 128;
        loop {
            for _ in 0..SPIN_ITERS {
                if self.fast_count.load(Ordering::Acquire) > 0
                    || self.fast_closed.load(Ordering::Acquire)
                {
                    break;
                }
                std::hint::spin_loop();
            }
            {
                let mut inner = self.mu.lock().unwrap();
                if inner.count > 0 {
                    let drained = inner.count;
                    while inner.count > 0 {
                        let head = inner.head;
                        let batch = inner.items[head].take().unwrap();
                        inner.head = (head + 1) % self.cap;
                        inner.count -= 1;
                        buf.push(batch);
                    }
                    self.fast_count.store(0, Ordering::Release);
                    drop(inner);
                    if drained > 0 {
                        self.not_full.notify_all();
                    }
                    return true;
                }
                if inner.closed {
                    return false;
                }
            }
            std::thread::park();
        }
    }

    /// Signals shutdown: no new items accepted, but pending items can still
    /// be drained. Wakes all blocked producers and the consumer.
    pub fn close(&self) {
        let mut inner = self.mu.lock().unwrap();
        inner.closed = true;
        self.fast_closed.store(true, Ordering::Release);
        drop(inner);
        self.not_full.notify_all();
        self.wake_consumer();
    }

    /// Current number of pending batches.
    pub fn len(&self) -> usize {
        self.mu.lock().unwrap().count
    }
}

impl Drop for WriteQueue {
    fn drop(&mut self) {
        let p = *self.consumer.get_mut();
        if !p.is_null() {
            unsafe {
                drop(Box::from_raw(p));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn make_batch(lsn: Lsn) -> WriteBatch {
        WriteBatch {
            events: EventVec::Inline(Event::new(format!("ev-{lsn}"))),
            lsn_start: lsn,
            lsn_end: lsn,
            barrier: std::ptr::null(),
        }
    }

    #[test]
    fn enqueue_dequeue() {
        let q = Arc::new(WriteQueue::new(4));
        assert!(q.enqueue(make_batch(1)));
        assert!(q.enqueue(make_batch(2)));
        assert_eq!(q.len(), 2);

        let q2 = q.clone();
        let handle = thread::spawn(move || {
            q2.register_consumer();
            let mut buf = Vec::new();
            q2.dequeue_all_into(&mut buf);
            buf
        });

        let buf = handle.join().unwrap();
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0].lsn_start, 1);
    }

    #[test]
    fn try_enqueue_full() {
        let q = WriteQueue::new(2);
        assert!(q.try_enqueue(make_batch(1)).is_ok());
        assert!(q.try_enqueue(make_batch(2)).is_ok());
        let rejected = q.try_enqueue(make_batch(3));
        assert!(rejected.is_err());
        assert_eq!(rejected.unwrap_err().lsn_start, 3);
    }

    #[test]
    fn drain_all() {
        let q = Arc::new(WriteQueue::new(8));
        for i in 1..=5 {
            q.enqueue(make_batch(i));
        }
        let q2 = q.clone();
        let handle = thread::spawn(move || {
            q2.register_consumer();
            let mut buf = Vec::new();
            q2.dequeue_all_into(&mut buf);
            buf
        });
        let buf = handle.join().unwrap();
        assert_eq!(buf.len(), 5);
        assert_eq!(q.len(), 0);
        assert_eq!(buf[0].lsn_start, 1);
        assert_eq!(buf[4].lsn_start, 5);
    }

    #[test]
    fn close_unblocks_dequeue() {
        let q = Arc::new(WriteQueue::new(4));
        let q2 = q.clone();

        let handle = thread::spawn(move || {
            q2.register_consumer();
            let mut buf = Vec::new();
            q2.dequeue_all_into(&mut buf)
        });

        thread::sleep(std::time::Duration::from_millis(50));
        q.close();

        let result = handle.join().unwrap();
        assert!(!result);
    }

    #[test]
    fn close_rejects_enqueue() {
        let q = WriteQueue::new(4);
        q.close();
        assert!(!q.enqueue(make_batch(1)));
    }

    #[test]
    fn blocking_enqueue_on_full() {
        let q = Arc::new(WriteQueue::new(2));
        q.enqueue(make_batch(1));
        q.enqueue(make_batch(2));

        let q2 = q.clone();
        let enqueue_handle = thread::spawn(move || {
            q2.enqueue(make_batch(3));
        });

        thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(q.len(), 2);

        let q3 = q.clone();
        let dequeue_handle = thread::spawn(move || {
            q3.register_consumer();
            let mut buf = Vec::new();
            q3.dequeue_all_into(&mut buf);
            buf
        });

        enqueue_handle.join().unwrap();
        q.close();
        let _ = dequeue_handle.join().unwrap();
    }
}
