use std::sync::Arc;

use crate::encoding::decode_batch_frame_borrowed;
use crate::error::{Error, Result};
use crate::event::{Event, Lsn};
use crate::hooks::HooksRunner;
use crate::mmap::MmapReader;
use crate::options::Compressor;
use crate::stats::StatsCollector;
use crate::storage::{FileStorage, Storage};

/// Pull-based sequential reader over WAL records using mmap for zero-copy
/// access.
///
/// Created by [`Wal::iterator`](crate::Wal::iterator). The caller must ensure
/// the iterator is dropped (or explicitly close it) to release mapped memory.
///
/// ```text
/// let mut it = wal.iterator(0)?;
/// while let Some(ev) = it.next()? {
///     // process ev
/// }
/// ```
pub struct WalIterator {
    reader: MmapReader,
    decomp: Option<Arc<dyn Compressor>>,
    data_ptr: *const u8,
    data_len: usize,
    offset: usize,
    batch: Vec<BorrowedEventOwned>,
    batch_pos: usize,
    decomp_buf: Vec<u8>,
    err: Option<Error>,
}

struct BorrowedEventOwned {
    lsn: Lsn,
    payload_off: usize,
    payload_len: usize,
    meta_off: usize,
    meta_len: usize,
}

// Safety: WalIterator holds a MmapReader (which is Send+Sync) and
// only accesses its data through shared references.
unsafe impl Send for WalIterator {}

impl WalIterator {
    /// Advances to the next event, returning an owned copy.
    ///
    /// For zero-copy access (no heap allocation), use [`next_borrowed`](Self::next_borrowed).
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Event>> {
        match self.next_borrowed()? {
            Some(ev) => Ok(Some(ev.to_owned())),
            None => Ok(None),
        }
    }

    /// Advances to the next event, returning a borrowed view into the
    /// memory-mapped region. No heap allocation occurs.
    ///
    /// The returned `BorrowedEvent` borrows from the iterator and is valid
    /// until the next call to any `next*` method.
    pub fn next_borrowed(&mut self) -> Result<Option<crate::encoding::BorrowedEvent<'_>>> {
        if let Some(ref e) = self.err {
            return Err(e.clone());
        }

        if self.batch_pos < self.batch.len() {
            let data = unsafe { std::slice::from_raw_parts(self.data_ptr, self.data_len) };
            let rec = &self.batch[self.batch_pos];
            self.batch_pos += 1;
            return Ok(Some(crate::encoding::BorrowedEvent {
                lsn: rec.lsn,
                payload: &data[rec.payload_off..rec.payload_off + rec.payload_len],
                meta: if rec.meta_len > 0 {
                    Some(&data[rec.meta_off..rec.meta_off + rec.meta_len])
                } else {
                    None
                },
            }));
        }

        if self.offset >= self.data_len {
            return Ok(None);
        }

        let data = unsafe { std::slice::from_raw_parts(self.data_ptr, self.data_len) };
        let decomp = self.decomp.as_deref();

        match decode_batch_frame_borrowed(data, self.offset, decomp, &mut self.decomp_buf) {
            Ok((events, next)) => {
                self.offset = next;
                self.batch.clear();

                if events.is_empty() {
                    return Ok(None);
                }

                let base_ptr = data.as_ptr() as usize;
                for ev in &events {
                    self.batch.push(BorrowedEventOwned {
                        lsn: ev.lsn,
                        payload_off: ev.payload.as_ptr() as usize - base_ptr,
                        payload_len: ev.payload.len(),
                        meta_off: ev.meta.map_or(0, |m| m.as_ptr() as usize - base_ptr),
                        meta_len: ev.meta.map_or(0, |m| m.len()),
                    });
                }

                self.batch_pos = 0;
                let rec = &self.batch[self.batch_pos];
                self.batch_pos += 1;
                let data = unsafe { std::slice::from_raw_parts(self.data_ptr, self.data_len) };
                Ok(Some(crate::encoding::BorrowedEvent {
                    lsn: rec.lsn,
                    payload: &data[rec.payload_off..rec.payload_off + rec.payload_len],
                    meta: if rec.meta_len > 0 {
                        Some(&data[rec.meta_off..rec.meta_off + rec.meta_len])
                    } else {
                        None
                    },
                }))
            }
            Err(Error::CrcMismatch) => {
                self.err = Some(Error::Corrupted);
                Err(Error::Corrupted)
            }
            Err(_) => Ok(None),
        }
    }

    /// Returns any error that stopped iteration.
    pub fn err(&self) -> Option<&Error> {
        self.err.as_ref()
    }

    /// Releases mapped memory. Safe to call multiple times.
    pub fn close(&mut self) -> Result<()> {
        self.reader.close()
    }
}

impl Drop for WalIterator {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

// ── Construction ────────────────────────────────────────────────────────────

pub(crate) fn new_iterator(
    fs: &FileStorage,
    from_lsn: Lsn,
    decomp: Option<Arc<dyn Compressor>>,
) -> Result<WalIterator> {
    let size = fs.size()?;
    if size == 0 {
        return Ok(WalIterator {
            reader: MmapReader::from_file_storage(fs, 0)?,
            decomp,
            data_ptr: std::ptr::null(),
            data_len: 0,
            offset: 0,
            batch: Vec::new(),
            batch_pos: 0,
            decomp_buf: Vec::new(),
            err: None,
        });
    }

    let reader = MmapReader::from_file_storage(fs, size)?;
    let data_ptr = reader.as_bytes().as_ptr();
    let data_len = reader.as_bytes().len();

    let mut it = WalIterator {
        reader,
        decomp,
        data_ptr,
        data_len,
        offset: 0,
        batch: Vec::new(),
        batch_pos: 0,
        decomp_buf: Vec::new(),
        err: None,
    };

    if from_lsn > 0 {
        it.seek_forward(from_lsn);
    }

    Ok(it)
}

impl WalIterator {
    fn seek_forward(&mut self, from_lsn: Lsn) {
        let data = unsafe { std::slice::from_raw_parts(self.data_ptr, self.data_len) };
        let decomp = self.decomp.as_deref();
        while self.offset < data.len() {
            match decode_batch_frame_borrowed(data, self.offset, decomp, &mut self.decomp_buf) {
                Ok((events, next)) => {
                    if !events.is_empty() && events.last().unwrap().lsn >= from_lsn {
                        let base_ptr = data.as_ptr() as usize;
                        self.batch.clear();
                        let skip = events.iter().position(|e| e.lsn >= from_lsn).unwrap_or(0);
                        for ev in &events {
                            self.batch.push(BorrowedEventOwned {
                                lsn: ev.lsn,
                                payload_off: ev.payload.as_ptr() as usize - base_ptr,
                                payload_len: ev.payload.len(),
                                meta_off: ev.meta.map_or(0, |m| m.as_ptr() as usize - base_ptr),
                                meta_len: ev.meta.map_or(0, |m| m.len()),
                            });
                        }
                        self.batch_pos = skip;
                        self.offset = next;
                        return;
                    }
                    self.offset = next;
                }
                Err(_) => return,
            }
        }
    }
}

// ── Callback-based replay ───────────────────────────────────────────────────

pub(crate) fn replay_callback(
    fs: &FileStorage,
    from_lsn: Lsn,
    mut f: impl FnMut(Event) -> Result<()>,
    decomp: Option<&dyn Compressor>,
    stats: &StatsCollector,
    hooks: &HooksRunner,
) -> Result<()> {
    let size = fs.size()?;
    if size == 0 {
        return Ok(());
    }

    let reader = MmapReader::from_file_storage(fs, size)?;
    let data = reader.as_bytes();
    let mut off = 0;
    let mut last_valid = 0;
    let mut corrupted = false;
    let mut decomp_buf = Vec::new();

    while off < data.len() {
        match decode_batch_frame_borrowed(data, off, decomp, &mut decomp_buf) {
            Ok((events, next)) => {
                last_valid = next;
                off = next;
                for ev in events {
                    if ev.lsn < from_lsn {
                        continue;
                    }
                    f(ev.to_owned())?;
                }
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
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::encode_batch_frame;
    use std::io::Write;

    fn setup_file(events_per_batch: &[usize]) -> (FileStorage, u64) {
        let path = std::env::temp_dir().join(format!(
            "rewal_replay_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&path);

        let mut buf = Vec::new();
        let mut lsn = 1u64;
        for &count in events_per_batch {
            let events: Vec<Event> = (0..count)
                .map(|i| Event {
                    lsn: 0,
                    payload: format!("ev-{}", lsn + i as u64).into_bytes(),
                    meta: None,
                })
                .collect();
            encode_batch_frame(&mut buf, &events, lsn, None).unwrap();
            lsn += count as u64;
        }

        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(&buf).unwrap();
            f.sync_all().unwrap();
        }

        let total_events = events_per_batch.iter().sum::<usize>() as u64;
        (FileStorage::new(&path).unwrap(), total_events)
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn iterator_all_events() {
        let (fs, total) = setup_file(&[3, 5, 2]);
        let mut it = new_iterator(&fs, 0, None).unwrap();
        let mut count = 0;
        while let Ok(Some(ev)) = it.next() {
            count += 1;
            assert_eq!(ev.lsn, count);
        }
        assert_eq!(count, total);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn iterator_from_lsn() {
        let (fs, _) = setup_file(&[5, 5]);
        let mut it = new_iterator(&fs, 7, None).unwrap();
        let mut lsns = Vec::new();
        while let Ok(Some(ev)) = it.next() {
            lsns.push(ev.lsn);
        }
        assert_eq!(lsns, vec![7, 8, 9, 10]);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn iterator_empty_file() {
        let path = std::env::temp_dir().join("rewal_replay_empty");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::File::create(&path).unwrap();
        let fs = FileStorage::new(&path).unwrap();
        let mut it = new_iterator(&fs, 0, None).unwrap();
        assert!(it.next().unwrap().is_none());
        drop(fs);
        let _ = std::fs::remove_file(&path);
    }
}
