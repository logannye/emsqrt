use thiserror::Error;

/// Canonical result for core.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid configuration: {0}")]
    Config(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Planning error: {0}")]
    Plan(String),

    #[error("Hashing error: {0}")]
    Hash(String),

    // The core crate does not do I/O, but higher layers may map their I/O
    // errors into this variant for convenience.
    #[error("I/O-like error (mapped into core): {0}")]
    IoLike(String),

    #[error("Internal invariant failed: {0}")]
    Invariant(String),

    /// Error with context chain for better debugging
    #[error("Error in {context}: {source}")]
    Context {
        context: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl Error {
    /// Add context to an error, creating an error chain.
    ///
    /// # Example
    /// ```rust,no_run
    /// use emsqrt_core::error::Error;
    /// let err = Error::Schema("unknown column".into());
    /// let err = err.with_context("while processing filter operator");
    /// ```
    pub fn with_context(self, context: impl Into<String>) -> Self {
        Error::Context {
            context: context.into(),
            source: Box::new(self) as Box<dyn std::error::Error + Send + Sync>,
        }
    }

    /// Get suggestions for common errors (e.g., column name suggestions).
    pub fn suggestions(&self) -> Vec<String> {
        match self {
            Error::Schema(msg) => {
                // Try to extract column name from schema error
                if msg.contains("unknown column") || msg.contains("column") {
                    // Could add fuzzy matching here if we had access to schema
                    vec!["Check that the column name is spelled correctly".into(),
                         "Verify the column exists in the input schema".into()]
                } else {
                    vec![]
                }
            }
            Error::Config(msg) => {
                if msg.contains("memory") || msg.contains("cap") {
                    vec!["Try increasing the memory cap".into(),
                         "Check that memory_cap_bytes is set correctly".into()]
                } else {
                    vec![]
                }
            }
            _ => vec![],
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Hash(e.to_string())
    }
}
