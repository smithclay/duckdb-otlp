//! Thin staticlib wrapper that re-exports the C FFI surface of the upstream
//! `otlp2records` crate (consumed from crates.io) so it can be linked into the
//! DuckDB extension.

#[cfg(feature = "ffi")]
pub use otlp2records::ffi::*;
