use std::fs::File;
use std::io;
use std::os::windows::io::AsRawHandle;

// ── FFI declarations ────────────────────────────────────────────────────────

type Handle = *mut std::ffi::c_void;

#[repr(C)]
struct Overlapped {
    internal: usize,
    internal_high: usize,
    offset: u32,
    offset_high: u32,
    event: Handle,
}

impl Overlapped {
    fn zeroed() -> Self {
        Self {
            internal: 0,
            internal_high: 0,
            offset: 0,
            offset_high: 0,
            event: std::ptr::null_mut(),
        }
    }
}

extern "system" {
    fn CreateFileMappingW(
        file: Handle,
        attrs: *const std::ffi::c_void,
        protect: u32,
        size_high: u32,
        size_low: u32,
        name: *const u16,
    ) -> Handle;

    fn MapViewOfFile(
        mapping: Handle,
        access: u32,
        offset_high: u32,
        offset_low: u32,
        bytes: usize,
    ) -> *mut u8;

    fn UnmapViewOfFile(base: *const u8) -> i32;

    fn CloseHandle(handle: Handle) -> i32;

    fn LockFileEx(
        file: Handle,
        flags: u32,
        reserved: u32,
        bytes_low: u32,
        bytes_high: u32,
        overlapped: *mut Overlapped,
    ) -> i32;

    fn UnlockFileEx(
        file: Handle,
        reserved: u32,
        bytes_low: u32,
        bytes_high: u32,
        overlapped: *mut Overlapped,
    ) -> i32;
}

const PAGE_READONLY: u32 = 0x02;
const FILE_MAP_READ: u32 = 0x04;
const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x02;
const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x01;

// ── mmap ────────────────────────────────────────────────────────────────────

/// Maps `size` bytes of `file` into read-only memory via
/// `CreateFileMappingW` + `MapViewOfFile`.
pub(crate) fn mmap_fd(file: &File, size: usize) -> io::Result<*mut u8> {
    let handle = file.as_raw_handle() as Handle;
    let size_high = ((size as u64) >> 32) as u32;
    let size_low = size as u32;

    let mapping = unsafe {
        CreateFileMappingW(
            handle,
            std::ptr::null(),
            PAGE_READONLY,
            size_high,
            size_low,
            std::ptr::null(),
        )
    };
    if mapping.is_null() {
        return Err(io::Error::last_os_error());
    }

    let ptr = unsafe { MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, size) };
    unsafe {
        CloseHandle(mapping);
    }

    if ptr.is_null() {
        return Err(io::Error::last_os_error());
    }
    Ok(ptr)
}

/// Unmaps a previously mapped region via `UnmapViewOfFile`.
pub(crate) fn munmap_ptr(ptr: *mut u8, _size: usize) -> io::Result<()> {
    let ret = unsafe { UnmapViewOfFile(ptr as *const u8) };
    if ret == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

// ── flock ───────────────────────────────────────────────────────────────────

/// Opaque handle representing a held file lock.
pub(crate) struct FileLock {
    handle: Handle,
}

// Safety: FileLock holds an OS handle that is valid across threads.
// The handle is only used for unlock, which is a thread-safe OS call.
unsafe impl Send for FileLock {}
unsafe impl Sync for FileLock {}

/// Acquires an exclusive, non-blocking lock via `LockFileEx`.
pub(crate) fn lock_file(file: &File) -> io::Result<FileLock> {
    let handle = file.as_raw_handle() as Handle;
    let flags = LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY;
    let mut ol = Overlapped::zeroed();

    let ret = unsafe { LockFileEx(handle, flags, 0, 1, 0, &mut ol) };
    if ret == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(FileLock { handle })
}

/// Releases a previously acquired file lock via `UnlockFileEx`.
pub(crate) fn unlock_file(lock: &FileLock) {
    let mut ol = Overlapped::zeroed();
    unsafe {
        UnlockFileEx(lock.handle, 0, 1, 0, &mut ol);
    }
}
