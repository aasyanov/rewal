use std::fmt;
use std::io;

/// All possible errors returned by REWAL operations.
///
/// Domain-specific variants cover every failure mode in the WAL lifecycle.
/// The [`Io`](Error::Io) variant wraps OS-level errors from file and syscall
/// operations.
#[derive(Debug)]
pub enum Error {
    /// Operation attempted on a closed WAL.
    Closed,
    /// [`Wal::append`] called during graceful shutdown.
    Draining,
    /// Operation requires [`State::Running`] but the WAL has not started.
    NotRunning,
    /// CRC mismatch detected during iteration.
    Corrupted,
    /// Write queue is full ([`Backpressure::Error`] mode).
    QueueFull,
    /// WAL file is locked by another process or instance.
    FileLocked,
    /// Invalid LSN argument.
    InvalidLsn,
    /// [`Storage::write`] returned `n=0` without an error.
    ShortWrite,
    /// Truncated or unsupported batch frame header.
    InvalidRecord,
    /// CRC-32C validation failure.
    CrcMismatch,
    /// Illegal lifecycle state transition.
    InvalidState,
    /// Zero events submitted to append.
    EmptyBatch,
    /// Compressed data encountered without a [`Compressor`].
    CompressorRequired,
    /// OS-level I/O error.
    Io(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("rewal: WAL is closed"),
            Self::Draining => f.write_str("rewal: WAL is draining"),
            Self::NotRunning => f.write_str("rewal: WAL is not running"),
            Self::Corrupted => f.write_str("rewal: data corruption detected"),
            Self::QueueFull => f.write_str("rewal: write queue is full"),
            Self::FileLocked => f.write_str("rewal: file is locked by another instance"),
            Self::InvalidLsn => f.write_str("rewal: invalid LSN"),
            Self::ShortWrite => f.write_str("rewal: short write"),
            Self::InvalidRecord => f.write_str("rewal: invalid record"),
            Self::CrcMismatch => f.write_str("rewal: CRC mismatch"),
            Self::InvalidState => f.write_str("rewal: invalid state transition"),
            Self::EmptyBatch => f.write_str("rewal: empty batch"),
            Self::CompressorRequired => {
                f.write_str("rewal: compressor required for compressed data")
            }
            Self::Io(e) => write!(f, "rewal: I/O error: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl Eq for Error {}

impl Clone for Error {
    fn clone(&self) -> Self {
        match self {
            Self::Closed => Self::Closed,
            Self::Draining => Self::Draining,
            Self::NotRunning => Self::NotRunning,
            Self::Corrupted => Self::Corrupted,
            Self::QueueFull => Self::QueueFull,
            Self::FileLocked => Self::FileLocked,
            Self::InvalidLsn => Self::InvalidLsn,
            Self::ShortWrite => Self::ShortWrite,
            Self::InvalidRecord => Self::InvalidRecord,
            Self::CrcMismatch => Self::CrcMismatch,
            Self::InvalidState => Self::InvalidState,
            Self::EmptyBatch => Self::EmptyBatch,
            Self::CompressorRequired => Self::CompressorRequired,
            Self::Io(e) => Self::Io(io::Error::new(e.kind(), e.to_string())),
        }
    }
}

/// Alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as StdError;

    #[test]
    fn display_messages() {
        assert_eq!(Error::Closed.to_string(), "rewal: WAL is closed");
        assert_eq!(Error::QueueFull.to_string(), "rewal: write queue is full");
    }

    #[test]
    fn io_error_conversion() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "gone");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
        assert!(err.to_string().contains("gone"));
    }

    #[test]
    fn equality_by_variant() {
        assert_eq!(Error::Closed, Error::Closed);
        assert_ne!(Error::Closed, Error::Draining);
        let a = Error::Io(io::Error::new(io::ErrorKind::Other, "a"));
        let b = Error::Io(io::Error::new(io::ErrorKind::Other, "b"));
        assert_eq!(a, b);
    }

    #[test]
    fn clone_preserves_variant() {
        let err = Error::CrcMismatch;
        assert_eq!(err.clone(), Error::CrcMismatch);
    }

    #[test]
    fn error_trait_source() {
        let io_err = Error::Io(io::Error::new(io::ErrorKind::Other, "x"));
        assert!(StdError::source(&io_err).is_some());
        assert!(StdError::source(&Error::Closed).is_none());
    }
}
