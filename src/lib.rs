//! # OCC Engine
//!
//! An Optimistic Concurrency Control implementation in Rust
//! based on Kung & Robinson (1981).
mod storage;

mod error;
mod transaction;

pub mod engine;

pub use engine::OccEngine;
pub use engine::parallel::ParallelEngine;
pub use engine::serial::SerialEngine;
pub use error::OccError;
pub use transaction::Transaction;
