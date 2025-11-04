#![forbid(unsafe_code)]
//! emsqrt-io: storage adapters and streaming readers/writers.
//!
//! - `storage`: concrete impls of `emsqrt_mem::spill::Storage` (FS now; cloud placeholders).
//! - `buf`: bounded buffered readers (read-ahead within a max buffer cap).
//! - `readers`: CSV/JSONL stream readers → simple `RowBatch` (no Arrow here).
//! - `writers`: CSV/JSONL stream writers.
//!
//! Parquet modules are feature-gated and stubbed unless `--features parquet`.

pub mod buf;
pub mod readers;
pub mod storage;
pub mod writers;

pub mod error;

#[cfg(feature = "parquet")]
pub mod arrow_convert;

pub use storage::FsStorage;
