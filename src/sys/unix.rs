use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;

// ── FFI declarations ────────────────────────────────────────────────────────

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, offset: i64) -> *mut u8;
    fn munmap(addr: *mut u8, len: usize) -> i32;
    fn flock(fd: i32, operation: i32) -> i32;
}

const PROT_READ: i32 = 1;
const MAP_SHARED: i32 = 1;
const MAP_FAILED: *mut u8 = !0 as *mut u8;

const LOCK_EX: i32 = 2;
const LOCK_NB: i32 = 4;
const LOCK_UN: i32 = 8;

// ── mmap ────────────────────────────────────────────────────────────────────

/// Maps `size` bytes of `file` into read-only shared memory.
/// Returns a raw pointer and the mapped length.
pub(crate) fn mmap_fd(file: &File, size: usize) -> io::Result<*mut u8> {
    let fd = file.as_raw_fd();
    let ptr = unsafe { mmap(std::ptr::null_mut(), size, PROT_READ, MAP_SHARED, fd, 0) };
    if ptr == MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    Ok(ptr)
}

/// Unmaps a previously mapped region.
pub(crate) fn munmap_ptr(ptr: *mut u8, size: usize) -> io::Result<()> {
    let ret = unsafe { munmap(ptr, size) };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

// ── flock ───────────────────────────────────────────────────────────────────

/// Opaque handle representing a held file lock.
pub(crate) struct FileLock {
    fd: i32,
}

/// Acquires an exclusive, non-blocking advisory lock on `file`.
pub(crate) fn lock_file(file: &File) -> io::Result<FileLock> {
    let fd = file.as_raw_fd();
    let ret = unsafe { flock(fd, LOCK_EX | LOCK_NB) };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(FileLock { fd })
}

/// Releases a previously acquired file lock.
pub(crate) fn unlock_file(lock: &FileLock) {
    unsafe {
        flock(lock.fd, LOCK_UN);
    }
}
