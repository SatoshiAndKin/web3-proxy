use crate::app::Web3ProxyJoinHandle;
use crate::rpcs::blockchain::{
    BlockHydrationCoordinator, BlockResponseCache, BlocksByHashCache, BlocksByNumberCache,
    HeadObservationPublisher,
};
use crate::rpcs::one::Web3Rpc;
use alloy::primitives::{TxHash, U256, U64};
use deduped_broadcast::DedupedBroadcaster;
use hashbrown::HashMap;
use sentry::types::Dsn;
use serde::{de, Deserialize, Deserializer};
use serde_inline_default::serde_inline_default;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct TopConfig {
    pub app: AppConfig,
    pub balanced_rpcs: HashMap<String, Web3RpcConfig>,
    #[serde(default = "Default::default")]
    pub private_rpcs: HashMap<String, Web3RpcConfig>,
    #[serde(default = "Default::default")]
    pub bundler_4337_rpcs: HashMap<String, Web3RpcConfig>,
    /// unknown config options get put here
    #[serde(flatten, default = "HashMap::default")]
    pub extra: HashMap<String, toml::Value>,
}

impl TopConfig {
    pub fn from_toml_str(input: &str) -> anyhow::Result<Self> {
        let expanded = shellexpand::env(input)?;
        Ok(toml::from_str(&expanded)?)
    }

    /// TODO: this should probably be part of Deserialize
    pub fn clean(&mut self) {
        if !self.extra.is_empty() {
            warn!(
                extra=?self.extra.keys(),
                "unknown TopConfig fields!",
            );
        }

        self.app.clean();
    }
}

/// shared configuration between Web3Rpcs
// TODO: no String, only &str
#[serde_inline_default]
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct AppConfig {
    /// erigon defaults to pruning beyond 90,000 blocks
    #[serde_inline_default(90_000u64)]
    pub archive_depth: u64,

    /// Maximum combined size of cached block responses.
    #[serde_inline_default(268_435_456u64)]
    pub block_cache_max_bytes: u64,

    /// EVM chain id. 1 for ETH
    /// TODO: better type for chain_id? max of `u64::MAX / 2 - 36` <https://github.com/ethereum/EIPs/issues/2294>
    #[serde_inline_default(1u64)]
    pub chain_id: u64,

    /// minimum amount to increase eth_estimateGas results
    pub gas_increase_min: Option<U256>,

    /// percentage to increase eth_estimateGas results. 100 == 100%
    pub gas_increase_percent: Option<U256>,

    /// do not serve any requests if the best known block is behind the best known block by more than this many blocks.
    pub max_head_block_lag: Option<U64>,

    /// The soft limit prevents thundering herds as new blocks are seen.
    #[serde_inline_default(1u32)]
    pub min_sum_soft_limit: u32,

    /// Another knob for preventing thundering herds as new blocks are seen.
    #[serde_inline_default(1usize)]
    pub min_synced_rpcs: usize,

    /// the stats page url for an anonymous user.
    pub redirect_public_url: Option<String>,

    /// optional script to run before shutting the frontend down.
    /// this is useful for keeping load balancers happy.
    pub shutdown_script: Option<String>,

    /// optional arguments for your shutdown script.
    #[serde_inline_default(vec![])]
    pub shutdown_script_args: Vec<String>,

    /// optional script to run before shutting the frontend down.
    /// this is useful for keeping load balancers happy.
    pub start_script: Option<String>,

    /// optional arguments for your shutdown script.
    #[serde_inline_default(vec![])]
    pub start_script_args: Vec<String>,

    /// Optionally send errors to <https://sentry.io>
    pub sentry_url: Option<Dsn>,

    /// unknown config options get put here
    #[serde(flatten, default = "HashMap::default")]
    pub extra: HashMap<String, toml::Value>,
}

impl Default for AppConfig {
    fn default() -> Self {
        sonic_rs::from_str("{}").unwrap()
    }
}

impl AppConfig {
    /// TODO: this should probably be part of Deserialize
    fn clean(&mut self) {
        if !self.extra.is_empty() {
            warn!(
                extra=?self.extra.keys(),
                "unknown Web3ProxyAppConfig fields!",
            );
        }
    }
}

/// TODO: we can't query a provider because we need this to create a provider
/// TODO: cache this
pub fn average_block_interval(chain_id: u64) -> Duration {
    match chain_id {
        // ethereum
        1 => Duration::from_secs(12),
        // ethereum-goerli
        5 => Duration::from_secs(12),
        // optimism
        10 => Duration::from_secs(2),
        // binance
        56 => Duration::from_secs(3),
        // polygon
        137 => Duration::from_secs(2),
        // fantom
        250 => Duration::from_secs(1),
        // zkevm polygon
        1101 => Duration::from_secs(7),
        // base
        8453 => Duration::from_secs(2),
        // development
        31337 => Duration::from_secs(10),
        // arbitrum
        42161 => Duration::from_millis(500),
        // web3-proxy tests
        999_001_999 => Duration::from_secs(10),
        // anything else
        _ => {
            let default = 10;
            warn!(
                "unknown chain_id ({}). defaulting average_block_interval to {} seconds",
                chain_id, default
            );
            Duration::from_secs(default)
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum BlockDataLimit {
    /// archive nodes can return all data
    Archive,
    /// prune nodes don't have all the data
    /// some devs will argue about what "prune" means but we use it to mean that any of the data is gone.
    /// TODO: this is too simple. erigon can prune the different types of data differently
    Set(u64),
    /// Automatically detect the limit
    #[default]
    Unknown,
}

impl<'de> Deserialize<'de> for BlockDataLimit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BlockDataLimitVisitor;

        impl<'de> de::Visitor<'de> for BlockDataLimitVisitor {
            type Value = BlockDataLimit;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string 'archive', 'unknown' or an positive signed 64-bit integer. 0 means automatically detect")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                match value.to_ascii_lowercase().as_str() {
                    "archive" => Ok(BlockDataLimit::Archive),
                    "unknown" => Ok(BlockDataLimit::Unknown),
                    _ => Err(de::Error::custom(format!("Unexpected value {}", value))),
                }
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                if v < 0 {
                    Err(de::Error::custom("Negative values are not allowed"))
                } else {
                    Ok(BlockDataLimit::Set(v as u64))
                }
            }
        }

        deserializer.deserialize_any(BlockDataLimitVisitor)
    }
}

impl From<BlockDataLimit> for AtomicU64 {
    fn from(value: BlockDataLimit) -> Self {
        match value {
            BlockDataLimit::Archive => AtomicU64::new(u64::MAX),
            BlockDataLimit::Set(limit) => AtomicU64::new(limit),
            BlockDataLimit::Unknown => AtomicU64::new(0),
        }
    }
}

/// Configuration for a backend web3 RPC server
#[serde_inline_default]
#[derive(Clone, Deserialize, PartialEq)]
pub struct Web3RpcConfig {
    /// only use this rpc if everything else is lagging too far. this allows us to ignore fast but very low limit rpcs
    #[serde(default = "Default::default")]
    pub backup: bool,
    /// block data limit. If None, will be queried
    #[serde(default = "Default::default")]
    pub block_data_limit: BlockDataLimit,
    /// simple way to disable a connection without deleting the row
    #[serde(default = "Default::default")]
    pub disabled: bool,
    /// a name used in /status and other user facing messages
    pub display_name: Option<String>,
    /// while not absolutely required, a http:// or https:// connection will allow erigon to stream JSON
    pub http_url: Option<String>,
    /// while not absolutely required, a ipc connection should be fastest
    pub ipc_path: Option<PathBuf>,
    /// the requests per second at which the server starts slowing down
    #[serde_inline_default(1u32)]
    pub soft_limit: u32,
    /// Subscribe to the firehose of pending transactions
    /// Don't do this with free rpcs
    #[serde(default = "Default::default")]
    pub subscribe_txs: bool,
    /// while not absolutely required, a ws:// or wss:// connection will be able to subscribe to head blocks
    pub ws_url: Option<String>,
    /// unknown config options get put here
    #[serde(flatten, default = "HashMap::default")]
    pub extra: HashMap<String, toml::Value>,
}

impl Default for Web3RpcConfig {
    fn default() -> Self {
        sonic_rs::from_str("{}").unwrap()
    }
}

impl fmt::Debug for Web3RpcConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Web3RpcConfig")
            .field("backup", &self.backup)
            .field("block_data_limit", &self.block_data_limit)
            .field("disabled", &self.disabled)
            .field("display_name", &self.display_name)
            .field("http_url", &self.http_url.as_ref().map(|_| "[REDACTED]"))
            .field("ipc_path", &self.ipc_path)
            .field("soft_limit", &self.soft_limit)
            .field("subscribe_txs", &self.subscribe_txs)
            .field("ws_url", &self.ws_url.as_ref().map(|_| "[REDACTED]"))
            .field("extra", &self.extra)
            .finish()
    }
}

impl Web3RpcConfig {
    /// Create a Web3Rpc from config
    /// TODO: move this into Web3Rpc? (just need to make things pub(crate))
    #[allow(clippy::too_many_arguments)]
    pub async fn spawn(
        self,
        name: String,
        chain_id: u64,
        block_interval: Duration,
        http_client: Option<reqwest::Client>,
        blocks_by_hash_cache: BlocksByHashCache,
        blocks_by_number_cache: BlocksByNumberCache,
        block_response_cache: BlockResponseCache,
        head_observation_publisher: Option<HeadObservationPublisher>,
        block_hydration: Option<Arc<BlockHydrationCoordinator>>,
        pending_txid_firehouse: Option<Arc<DedupedBroadcaster<TxHash>>>,
        max_head_block_age: Duration,
    ) -> anyhow::Result<(Arc<Web3Rpc>, Web3ProxyJoinHandle<()>)> {
        if !self.extra.is_empty() {
            // TODO: move this to a `clean` function
            warn!(extra=?self.extra.keys(), "unknown Web3RpcConfig fields!");
        }

        Web3Rpc::spawn(
            self,
            name,
            chain_id,
            http_client,
            block_interval,
            blocks_by_hash_cache,
            blocks_by_number_cache,
            block_response_cache,
            head_observation_publisher,
            block_hydration,
            pending_txid_firehouse,
            max_head_block_age,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::{AppConfig, TopConfig, Web3RpcConfig};
    use sonic_rs::json;
    use std::env;

    #[test]
    fn expected_app_defaults() {
        // a is from serde
        let a: AppConfig = sonic_rs::from_value(&json!({
            "chain_id": 1,
        }))
        .unwrap();

        assert_eq!(a.min_synced_rpcs, 1);
        assert_eq!(a.block_cache_max_bytes, 268_435_456);

        // b is from Default
        let b = AppConfig::default();

        assert_eq!(b.min_synced_rpcs, 1);
        assert_eq!(b.block_cache_max_bytes, 268_435_456);

        assert_eq!(a, b);
    }

    #[test]
    fn expected_rpc_defaults() {
        let a: Web3RpcConfig = sonic_rs::from_str("{}").unwrap();

        assert_eq!(a.soft_limit, 1);

        let b: Web3RpcConfig = Default::default();

        assert_eq!(b.soft_limit, 1);

        assert_eq!(a, b);
    }

    #[test]
    fn rpc_debug_output_redacts_urls() {
        let config = Web3RpcConfig {
            http_url: Some("https://example.com/http-secret".to_string()),
            ws_url: Some("wss://example.com/ws-secret".to_string()),
            ..Default::default()
        };

        assert_eq!(
            format!("{config:?}"),
            "Web3RpcConfig { backup: false, block_data_limit: Unknown, disabled: false, display_name: None, http_url: Some(\"[REDACTED]\"), ipc_path: None, soft_limit: 1, subscribe_txs: false, ws_url: Some(\"[REDACTED]\"), extra: {} }"
        );
    }

    #[test]
    fn top_config_expands_environment_variables() {
        const VARIABLE: &str = "WEB3_PROXY_TEST_RPC_URL";
        let previous_value = env::var_os(VARIABLE);
        // FIXME: Audit that the environment access only happens in single-threaded code.
        unsafe { env::set_var(VARIABLE, "http://127.0.0.1:8545") };

        let config = TopConfig::from_toml_str(
            r#"
                [app]
                chain_id = 1

                [balanced_rpcs.local]
                http_url = "${WEB3_PROXY_TEST_RPC_URL}"
            "#,
        )
        .unwrap();

        match previous_value {
            // FIXME: Audit that the environment access only happens in single-threaded code.
            Some(value) => unsafe { env::set_var(VARIABLE, value) },
            // FIXME: Audit that the environment access only happens in single-threaded code.
            None => unsafe { env::remove_var(VARIABLE) },
        }

        assert_eq!(
            config.balanced_rpcs["local"].http_url.as_deref(),
            Some("http://127.0.0.1:8545")
        );
    }

    #[test]
    fn top_config_rejects_missing_environment_variables() {
        const VARIABLE: &str = "WEB3_PROXY_TEST_MISSING_RPC_URL";
        let previous_value = env::var_os(VARIABLE);
        // FIXME: Audit that the environment access only happens in single-threaded code.
        unsafe { env::remove_var(VARIABLE) };

        let error = TopConfig::from_toml_str(
            r#"
                [app]
                chain_id = 1

                [balanced_rpcs.local]
                http_url = "${WEB3_PROXY_TEST_MISSING_RPC_URL}"
            "#,
        )
        .unwrap_err();

        if let Some(value) = previous_value {
            // FIXME: Audit that the environment access only happens in single-threaded code.
            unsafe { env::set_var(VARIABLE, value) };
        }

        assert_eq!(
            error.to_string(),
            "error looking key 'WEB3_PROXY_TEST_MISSING_RPC_URL' up: environment variable not found"
        );
    }
}
