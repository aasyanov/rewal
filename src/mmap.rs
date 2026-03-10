use crate::error::Result;
use crate::storage::FileStorage;
use crate::sys;

/// Provides zero-copy read access to storage via memory mapping.
///
/// Maps the file via mmap (Unix) or MapViewOfFile (Windows) through
/// [`FileStorage`].
pub(crate) struct MmapReader {
    ptr: *mut u8,
    len: usize,
}

// Safety: MmapReader holds a read-only view of a memory-mapped file region.
// The pointer is valid for the reader's lifetime and is only used for reads.
unsafe impl Send for MmapReader {}
unsafe impl Sync for MmapReader {}

impl MmapReader {
    /// Creates a reader from a known [`FileStorage`].
    pub fn from_file_storage(fs: &FileStorage, size: u64) -> Result<Self> {
        if size == 0 {
            return Ok(Self {
                ptr: std::ptr::null_mut(),
                len: 0,
            });
        }

        let ptr = fs
            .with_file(|f| sys::mmap_fd(f, size as usize))
            .ok_or(crate::error::Error::Closed)??;

        Ok(Self {
            ptr,
            len: size as usize,
        })
    }

    /// Returns the mapped data as a byte slice. Empty if size was zero or
    /// the reader has been closed.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        if self.ptr.is_null() {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// Releases the mapped region. Safe to call multiple times.
    pub fn close(&mut self) -> Result<()> {
        if self.ptr.is_null() {
            return Ok(());
        }

        sys::munmap_ptr(self.ptr, self.len)?;

        self.ptr = std::ptr::null_mut();
        self.len = 0;
        Ok(())
    }
}

impl Drop for MmapReader {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use std::io::Write;

    #[test]
    #[cfg_attr(miri, ignore)]
    fn mmap_reader_file() {
        let dir = std::env::temp_dir().join("rewal_test_mmap");
        let _ = std::fs::remove_file(&dir);

        {
            let mut f = std::fs::File::create(&dir).unwrap();
            f.write_all(b"hello mmap world").unwrap();
            f.sync_all().unwrap();
        }

        let fs = FileStorage::new(&dir).unwrap();
        let size = fs.size().unwrap();
        let mut reader = MmapReader::from_file_storage(&fs, size).unwrap();
        assert_eq!(reader.as_bytes(), b"hello mmap world");

        reader.close().unwrap();
        assert!(reader.as_bytes().is_empty());

        drop(fs);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn mmap_reader_empty() {
        let dir = std::env::temp_dir().join("rewal_test_mmap_empty");
        let _ = std::fs::remove_file(&dir);
        let _ = std::fs::File::create(&dir).unwrap();

        let fs = FileStorage::new(&dir).unwrap();
        let reader = MmapReader::from_file_storage(&fs, 0).unwrap();
        assert!(reader.as_bytes().is_empty());

        drop(fs);
        let _ = std::fs::remove_file(&dir);
    }
}
