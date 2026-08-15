#![feature(lazy_cell)]
#![feature(let_chains)]
#![feature(trait_alias)]
#![feature(result_flattening)]
#![forbid(unsafe_code)]

pub mod app;
pub mod block_number;
pub mod config;
pub mod errors;
pub mod frontend;
pub mod globals;
pub mod jsonrpc;
pub mod pagerduty;
pub mod prelude;
pub mod prometheus;
pub mod rpcs;
pub mod test_utils;

#[cfg(feature = "rdkafka")]
pub mod kafka;
