use alloy::primitives::{FixedBytes, B256};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt, path::PathBuf};
use url::Url;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Observe,
    Inject,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub mode: Mode,
    pub network: Network,
    pub sources: BTreeMap<String, Source>,
    pub execution_targets: BTreeMap<String, ExecutionTarget>,
    pub consensus_targets: BTreeMap<String, ConsensusTarget>,
    #[serde(default = "proof_workers")]
    pub proof_workers: usize,
    #[serde(default = "cache_bytes")]
    pub cache_max_bytes: u64,
}
fn proof_workers() -> usize {
    2
}
fn cache_bytes() -> u64 {
    128 * 1024 * 1024
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Network {
    pub genesis_validators_root: B256,
    pub genesis_time: u64,
    #[serde(default = "seconds_per_slot")]
    pub seconds_per_slot: u64,
    pub forks: Vec<Fork>,
}
fn seconds_per_slot() -> u64 {
    12
}
/// The relay uses the mainnet SSZ preset; slot duration and fork epochs can differ on devnets.
pub const SLOTS_PER_EPOCH: u64 = 32;
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Fork {
    pub name: String,
    pub version: FixedBytes<4>,
    pub epoch: u64,
}
impl Network {
    pub fn fork_at(&self, slot: u64) -> Option<&Fork> {
        self.forks
            .iter()
            .rev()
            .find(|f| f.epoch <= slot / SLOTS_PER_EPOCH)
    }
    pub fn timestamp(&self, slot: u64) -> Result<u64> {
        slot.checked_mul(self.seconds_per_slot)
            .and_then(|v| self.genesis_time.checked_add(v))
            .ok_or_else(|| anyhow::anyhow!("slot timestamp overflow"))
    }
    pub fn slot(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(self.genesis_time)
            / self.seconds_per_slot
    }
}

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub beacon_url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}
#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionTarget {
    pub engine_url: String,
    pub rpc_url: String,
    pub jwt_secret_path: PathBuf,
}
#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConsensusTarget {
    pub beacon_url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}
impl fmt::Debug for ConsensusTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConsensusTarget { credentials: [REDACTED] }")
    }
}
impl fmt::Debug for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Source { credentials: [REDACTED] }")
    }
}
impl fmt::Debug for ExecutionTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ExecutionTarget { credentials: [REDACTED] }")
    }
}
pub fn url(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|_| anyhow::anyhow!("invalid relay URL"))?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.fragment().is_none(),
        "relay URL must use HTTP(S), with no fragment"
    );
    Ok(url)
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.sources.is_empty()
                && !(self.execution_targets.is_empty() && self.consensus_targets.is_empty()),
            "relay requires sources and targets"
        );
        ensure!(
            self.sources.len() <= 16
                && self.execution_targets.len() + self.consensus_targets.len() <= 64,
            "relay supports at most 16 sources and 64 targets per process"
        );
        ensure!(
            (1..=16).contains(&self.proof_workers),
            "proof workers must be between 1 and 16"
        );
        ensure!(
            self.network.seconds_per_slot > 0 && self.network.seconds_per_slot <= 60,
            "invalid slot duration"
        );
        ensure!(
            self.cache_max_bytes > 0,
            "relay cache capacity must be positive"
        );
        ensure!(
            !self.network.forks.is_empty(),
            "relay requires a fork schedule"
        );
        for pair in self.network.forks.windows(2) {
            ensure!(pair[0].epoch < pair[1].epoch, "fork epochs must increase");
        }
        let mut versions = std::collections::BTreeSet::new();
        let mut names = std::collections::BTreeSet::new();
        for fork in &self.network.forks {
            ensure!(versions.insert(fork.version), "duplicate fork version");
            ensure!(names.insert(&fork.name), "duplicate fork name");
        }
        for source in self.sources.values() {
            url(&source.beacon_url)?;
        }
        let mut engines = std::collections::BTreeSet::new();
        for target in self.execution_targets.values() {
            ensure!(
                engines.insert(url(&target.engine_url)?.to_string()),
                "duplicate Engine endpoint"
            );
            url(&target.rpc_url)?;
        }
        let mut beacons = std::collections::BTreeSet::new();
        for target in self.consensus_targets.values() {
            ensure!(
                beacons.insert(url(&target.beacon_url)?.to_string()),
                "duplicate Beacon target endpoint"
            );
        }
        Ok(())
    }
}
