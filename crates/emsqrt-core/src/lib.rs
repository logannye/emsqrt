#![forbid(unsafe_code)]
//! emsqrt-core: shared kernel for the EM-√ engine.
//!
//! This crate contains only *pure* types, small helpers, and interfaces
//! (traits) that other crates implement. There is **no I/O**, **no async**,
//! and **no allocation policy** here, by design.
//!
//! Crates that use this:
//! - emsqrt-te: consumes DAG/Block types to plan TE block orders.
//! - emsqrt-mem: implements the MemoryBudget trait (actual guards/pools live there).
//! - emsqrt-io: implements Storage for object stores (S3/GCS/Azure/FS) – referenced via traits.
//! - emsqrt-operators: implements operators that reference IDs, Schema, and Block metadata.
//! - emsqrt-planner: produces LogicalPlan/PhysicalPlan using these types.
//! - emsqrt-exec: orchestrates everything and emits RunManifest.

pub mod block;
pub mod budget;
pub mod config;
pub mod dag;
pub mod error;
pub mod expr;
pub mod hash;
pub mod id;
pub mod manifest;
pub mod prelude;
pub mod schema;
pub mod stats;
pub mod types;

#[cfg(feature = "arrow")]
pub mod arrow;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
