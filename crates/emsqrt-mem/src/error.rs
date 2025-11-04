use thiserror::Error;

/// Result type local to emsqrt-mem.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("memory budget exceeded for tag '{tag}': requested {requested} bytes, capacity {capacity}, used {used}")]
    BudgetExceeded {
        tag: &'static str,
        requested: usize,
        capacity: usize,
        used: usize,
    },

    #[error("allocation failed for {bytes} bytes (tag '{tag}')")]
    AllocFailed { tag: &'static str, bytes: usize },

    #[error("memory budget error: {0}")]
    Budget(String),

    #[error("spill storage error: {0}")]
    Storage(String),

    #[error("unsupported codec: {0}")]
    CodecUnsupported(&'static str),

    #[error("codec error: {0}")]
    Codec(String),

    #[error("checksum mismatch")]
    ChecksumMismatch,
}

impl Error {
    /// Add context to an error.
    pub fn with_context(self, context: impl Into<String>) -> Self {
        let ctx = context.into();
        match self {
            Error::BudgetExceeded { tag, requested, capacity, used } => {
                Error::Budget(format!("{}: budget exceeded for tag '{}': requested {} bytes, capacity {}, used {}", 
                    ctx, tag, requested, capacity, used))
            }
            Error::AllocFailed { tag, bytes } => {
                Error::Budget(format!("{}: allocation failed for {} bytes (tag '{}')", ctx, bytes, tag))
            }
            Error::Budget(msg) => Error::Budget(format!("{}: {}", ctx, msg)),
            Error::Storage(msg) => Error::Storage(format!("{}: {}", ctx, msg)),
            Error::Codec(msg) => Error::Codec(format!("{}: {}", ctx, msg)),
            other => other,
        }
    }

    /// Get suggestions for common errors.
    pub fn suggestions(&self) -> Vec<String> {
        match self {
            Error::BudgetExceeded { requested, capacity, .. } => {
                vec![
                    format!("Requested {} bytes but only {} bytes available", requested, capacity),
                    "Try increasing memory_cap_bytes in configuration".into(),
                    "Consider using external operators (external sort, hash join) for large datasets".into(),
                    "Check if data can be processed in smaller batches".into(),
                ]
            }
            Error::Storage(msg) => {
                vec![
                    "Check storage path and permissions".into(),
                    "Verify disk space is available".into(),
                    format!("Storage error details: {}", msg),
                ]
            }
            Error::CodecUnsupported(codec) => {
                vec![
                    format!("Codec '{}' is not supported", codec),
                    "Supported codecs: uncompressed, zstd (if feature enabled), lz4 (if feature enabled)".into(),
                ]
            }
            _ => vec![],
        }
    }
}
