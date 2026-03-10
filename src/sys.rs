//! Platform-specific implementations for mmap and file locking.

#[cfg(unix)]
#[path = "sys/unix.rs"]
mod platform;

#[cfg(windows)]
#[path = "sys/windows.rs"]
mod platform;

#[cfg(not(any(unix, windows)))]
compile_error!("rewal requires a unix or windows target");

pub(crate) use platform::{lock_file, mmap_fd, munmap_ptr, unlock_file, FileLock};
