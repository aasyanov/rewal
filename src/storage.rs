use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

use crate::error::{Error, Result};
use crate::sys::{lock_file, unlock_file, FileLock};

/// Persistence backend for the WAL.
///
/// The writer thread calls [`write`](Storage::write) sequentially; replay and
/// recovery use [`read_at`](Storage::read_at) for random reads. Implementations
/// must handle their own thread safety for concurrent read + write access.
pub trait Storage: Send + Sync {
    /// Appends data. The WAL does not seek before writing.
    fn write(&mut self, buf: &[u8]) -> Result<usize>;
    /// Ensures all written data is durable (fsync).
    fn sync(&mut self) -> Result<()>;
    /// Releases all resources. After close, all methods must return
    /// [`Error::Closed`].
    fn close(&mut self) -> Result<()>;
    /// Current storage size in bytes.
    fn size(&self) -> Result<u64>;
    /// Shrinks storage to `size` bytes. Used during recovery to remove
    /// corrupted trailing records.
    fn truncate(&mut self, size: u64) -> Result<()>;
    /// Reads `buf.len()` bytes starting at `offset`. Used as a fallback when
    /// mmap is not available.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
}

/// Default [`Storage`] backed by [`std::fs::File`] with advisory file locking.
///
/// Uses flock (Unix) or LockFileEx (Windows) to prevent concurrent access.
/// All methods are protected by a mutex for thread safety.
pub struct FileStorage {
    inner: Mutex<FileStorageInner>,
    path: String,
}

struct FileStorageInner {
    file: Option<File>,
    lock: Option<FileLock>,
}

impl FileStorage {
    /// Opens or creates a WAL file at `path`. Acquires an exclusive lock.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;

        let lock = lock_file(&file).map_err(|_| Error::FileLocked)?;

        let mut f = file;
        f.seek(SeekFrom::End(0))?;

        Ok(Self {
            inner: Mutex::new(FileStorageInner {
                file: Some(f),
                lock: Some(lock),
            }),
            path: path.to_string_lossy().into_owned(),
        })
    }

    /// Returns the filesystem path (lock-free).
    #[inline]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Executes a closure with a reference to the underlying [`File`], if open.
    /// Used by mmap and sync to obtain the file descriptor / handle.
    pub(crate) fn with_file<R>(&self, f: impl FnOnce(&std::fs::File) -> R) -> Option<R> {
        let guard = self.inner.lock().unwrap();
        guard.file.as_ref().map(f)
    }

    /// Clones the underlying file handle (via `try_clone`), if open.
    pub(crate) fn try_clone_file(&self) -> Option<std::io::Result<std::fs::File>> {
        let guard = self.inner.lock().unwrap();
        guard.file.as_ref().map(|f| f.try_clone())
    }
}

impl FileStorageInner {
    fn file_ref(&self) -> Result<&File> {
        self.file.as_ref().ok_or(Error::Closed)
    }

    fn file_mut(&mut self) -> Result<&mut File> {
        self.file.as_mut().ok_or(Error::Closed)
    }
}

impl Storage for FileStorage {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let mut inner = self.inner.lock().unwrap();
        let f = inner.file_mut()?;
        Ok(f.write(buf)?)
    }

    fn sync(&mut self) -> Result<()> {
        let inner = self.inner.lock().unwrap();
        let f = inner.file_ref()?;
        Ok(f.sync_all()?)
    }

    fn close(&mut self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(lock) = inner.lock.take() {
            unlock_file(&lock);
        }
        inner.file = None;
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        let inner = self.inner.lock().unwrap();
        let f = inner.file_ref()?;
        Ok(f.metadata()?.len())
    }

    fn truncate(&mut self, size: u64) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let f = inner.file_mut()?;
        f.set_len(size)?;
        f.seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let mut inner = self.inner.lock().unwrap();
        let f = inner.file_mut()?;
        f.seek(SeekFrom::Start(offset))?;
        Ok(f.read(buf)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_storage_lifecycle() {
        let dir = std::env::temp_dir().join("rewal_test_storage");
        let _ = std::fs::remove_file(&dir);

        let mut fs = FileStorage::new(&dir).unwrap();
        assert_eq!(fs.size().unwrap(), 0);

        let n = Storage::write(&mut fs, b"hello").unwrap();
        assert_eq!(n, 5);
        assert_eq!(fs.size().unwrap(), 5);

        fs.sync().unwrap();

        let mut buf = vec![0u8; 5];
        let n = fs.read_at(&mut buf, 0).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");

        fs.truncate(3).unwrap();
        assert_eq!(fs.size().unwrap(), 3);

        fs.close().unwrap();
        assert!(Storage::write(&mut fs, b"x").is_err());

        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn file_locking() {
        let dir = std::env::temp_dir().join("rewal_test_flock");
        let _ = std::fs::remove_file(&dir);

        let _fs1 = FileStorage::new(&dir).unwrap();
        let result = FileStorage::new(&dir);
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e, Error::FileLocked);
        }

        drop(_fs1);
        let _ = std::fs::remove_file(&dir);
    }
}
