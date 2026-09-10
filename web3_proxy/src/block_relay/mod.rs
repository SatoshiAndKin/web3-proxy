//! Relay execution payloads and complete signed Beacon blocks to owned clients.
pub mod config;
mod consensus;
mod journal;
pub mod payload;
mod recording;
mod service;
mod source;
mod stats;
mod status;
mod target;
pub mod transport;
mod tree_hash;
pub use service::BlockRelay;
use service::Work;
pub use stats::Sample;

#[cfg(test)]
mod tests;
