/// Log Sequence Number — monotonically increasing identifier assigned to each
/// event. LSNs start at 1; zero means "no LSN assigned".
pub type Lsn = u64;

/// A single entry in the write-ahead log.
///
/// When submitting via [`Wal::append`] or [`Wal::append_batch`], leave `lsn`
/// as zero — it is assigned automatically. During replay, `payload` and `meta`
/// may reference memory-mapped storage (zero-copy); copy them if the data must
/// outlive the callback or iterator.
#[derive(Debug, Clone)]
pub struct Event {
    /// Assigned by the WAL upon write. Zero on input.
    pub lsn: Lsn,
    /// Raw event data. The WAL never interprets its contents.
    pub payload: Vec<u8>,
    /// Optional opaque metadata (event type, aggregate ID, tags, etc.).
    pub meta: Option<Vec<u8>>,
}

impl Event {
    /// Creates an event with only a payload.
    #[inline]
    pub fn new(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            lsn: 0,
            payload: payload.into(),
            meta: None,
        }
    }

    /// Creates an event with payload and metadata.
    #[inline]
    pub fn with_meta(payload: impl Into<Vec<u8>>, meta: impl Into<Vec<u8>>) -> Self {
        Self {
            lsn: 0,
            payload: payload.into(),
            meta: Some(meta.into()),
        }
    }
}

/// A group of events submitted atomically via [`Wal::append_batch`].
///
/// Events within a batch receive contiguous LSNs and are encoded into a single
/// batch frame with one CRC-32C checksum. On recovery, either all events in
/// the frame are valid or the entire frame is discarded.
#[derive(Debug, Clone)]
pub struct Batch {
    pub(crate) events: Vec<Event>,
}

impl Batch {
    /// Creates an empty batch.
    #[inline]
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    /// Creates a batch pre-allocated for `n` events.
    #[inline]
    pub fn with_capacity(n: usize) -> Self {
        Self {
            events: Vec::with_capacity(n),
        }
    }

    /// Appends an event with the given payload.
    #[inline]
    pub fn add(&mut self, payload: impl Into<Vec<u8>>) {
        self.events.push(Event::new(payload));
    }

    /// Appends an event with payload and metadata.
    #[inline]
    pub fn add_with_meta(&mut self, payload: impl Into<Vec<u8>>, meta: impl Into<Vec<u8>>) {
        self.events.push(Event::with_meta(payload, meta));
    }

    /// Returns the number of events in the batch.
    #[inline]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Returns `true` if the batch contains no events.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Removes all events, retaining allocated capacity.
    #[inline]
    pub fn clear(&mut self) {
        self.events.clear();
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_new() {
        let ev = Event::new(b"hello".to_vec());
        assert_eq!(ev.lsn, 0);
        assert_eq!(ev.payload, b"hello");
        assert!(ev.meta.is_none());
    }

    #[test]
    fn event_with_meta() {
        let ev = Event::with_meta("payload", "meta");
        assert_eq!(ev.payload, b"payload");
        assert_eq!(ev.meta.as_deref(), Some(b"meta".as_slice()));
    }

    #[test]
    fn batch_operations() {
        let mut b = Batch::with_capacity(4);
        assert!(b.is_empty());

        b.add("one");
        b.add_with_meta("two", "m");
        assert_eq!(b.len(), 2);

        b.clear();
        assert!(b.is_empty());
    }

    #[test]
    fn batch_default() {
        let b = Batch::default();
        assert!(b.is_empty());
    }
}
