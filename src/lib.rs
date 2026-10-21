//! A Redis-compatible in-memory key-value server.
//!
//! The binary (`src/main.rs`) is a thin wrapper around this library. Keeping
//! the logic here lets integration tests, benchmarks and fuzz targets use it
//! directly.

pub mod config;
pub mod resp;
