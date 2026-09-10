//! Observe Beacon sources and import complete payloads into owned execution clients.
pub mod config;
pub mod payload;
mod service;
mod source;
mod stats;
mod target;
pub mod transport;
mod tree_hash;
pub use service::BlockRelay;
use service::Work;
pub use stats::Sample;

#[cfg(test)]
mod tests;
