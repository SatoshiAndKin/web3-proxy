use super::{
    config::{Network, Source, SLOTS_PER_EPOCH},
    transport,
};
use alloy::primitives::B256;
use alloy_rpc_types_beacon::{
    config::{ForkScheduleResponse, SpecResponse},
    genesis::GenesisResponse,
    header::HeaderResponse,
};
use anyhow::{ensure, Result};
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{de::DeserializeOwned, Deserialize};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};
use url::Url;

#[derive(Clone, Debug)]
pub struct Announcement {
    pub root: B256,
    pub slot: u64,
    pub source: String,
    pub kind: &'static str,
    pub at: Instant,
}

pub struct BeaconSource {
    pub name: String,
    base: Url,
    client: reqwest::Client,
    headers: HeaderMap,
    pub verified: AtomicBool,
    pub reads: tokio::sync::Semaphore,
}
impl BeaconSource {
    pub fn new(name: String, config: &Source) -> Result<Self> {
        let mut headers = HeaderMap::new();
        for (key, value) in &config.headers {
            let key = HeaderName::from_bytes(key.as_bytes())
                .map_err(|_| anyhow::anyhow!("invalid source header name"))?;
            let mut value = HeaderValue::from_str(value)
                .map_err(|_| anyhow::anyhow!("invalid source header value"))?;
            value.set_sensitive(true);
            headers.insert(key, value);
        }
        Ok(Self {
            name,
            base: super::config::url(&config.beacon_url)?,
            client: transport::client()?,
            headers,
            verified: AtomicBool::new(false),
            reads: tokio::sync::Semaphore::new(2),
        })
    }
    fn endpoint(&self, path: &str) -> Url {
        let mut url = self.base.clone();
        url.set_path(&format!(
            "{}{}",
            self.base.path().trim_end_matches('/'),
            path
        ));
        url
    }
    pub async fn get_bytes(&self, path: &str) -> Result<bytes::Bytes> {
        let _permit = self.reads.acquire().await?;
        let response = self
            .client
            .get(self.endpoint(path))
            .headers(self.headers.clone())
            .timeout(transport::READ_TIMEOUT)
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("Beacon transport error"))?;
        transport::body(response).await
    }
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        sonic_rs::from_slice(&self.get_bytes(path).await?)
            .map_err(|_| anyhow::anyhow!("invalid Beacon response"))
    }
    pub async fn validate(&self, network: &Network) -> Result<()> {
        let (genesis, schedule, spec) = tokio::try_join!(
            self.get::<GenesisResponse>("/eth/v1/beacon/genesis"),
            self.get::<ForkScheduleResponse>("/eth/v1/config/fork_schedule"),
            self.get::<SpecResponse>("/eth/v1/config/spec"),
        )?;
        ensure!(
            genesis.data.genesis_validators_root == network.genesis_validators_root
                && genesis.data.genesis_time == network.genesis_time,
            "Beacon genesis mismatch"
        );
        ensure!(
            spec.data.get("PRESET_BASE").is_some_and(|s| s == "mainnet"),
            "unsupported Beacon preset"
        );
        ensure!(
            spec.data
                .get("SECONDS_PER_SLOT")
                .and_then(|s| s.parse::<u64>().ok())
                == Some(network.seconds_per_slot),
            "slot duration mismatch"
        );
        for fork in &network.forks {
            ensure!(
                schedule
                    .data
                    .iter()
                    .any(|f| f.current_version == fork.version && f.epoch == fork.epoch),
                "Beacon fork schedule mismatch"
            );
        }
        // Older forks may be omitted from our short-horizon schedule. New ones may not.
        let first = network.forks.first().expect("validated config").epoch;
        for fork in schedule
            .data
            .iter()
            .filter(|f| f.epoch >= first && f.epoch != u64::MAX)
        {
            ensure!(
                network
                    .forks
                    .iter()
                    .any(|f| f.version == fork.current_version && f.epoch == fork.epoch),
                "unconfigured Beacon fork; update relay schedule"
            );
        }
        Ok(())
    }
    async fn reconcile(&self, tx: &mpsc::Sender<Announcement>, network: &Network) -> Result<()> {
        // Follow roots, not slot numbers, so skipped slots and reorgs remain well-defined.
        let mut id = "head".to_string();
        for _ in 0..8 {
            let header: HeaderResponse = self.get(&format!("/eth/v1/beacon/headers/{id}")).await?;
            let h = header.data.header.message;
            if h.slot.saturating_add(2 * SLOTS_PER_EPOCH) < network.slot() {
                break;
            }
            tx.send(Announcement {
                root: header.data.root,
                slot: h.slot,
                source: self.name.clone(),
                kind: "reconcile",
                at: Instant::now(),
            })
            .await?;
            id = h.parent_root.to_string();
        }
        Ok(())
    }
    pub async fn run(
        self: Arc<Self>,
        network: Network,
        tx: mpsc::Sender<Announcement>,
        stats: super::stats::Shared,
    ) {
        loop {
            self.verified.store(false, Ordering::Release);
            match self.validate(&network).await {
                Ok(()) => {
                    self.verified.store(true, Ordering::Release);
                }
                Err(error) => {
                    stats.lock().source_error(&self.name, &error.to_string());
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            }
            if let Err(error) = self.stream(&network, &tx, &stats).await {
                stats.lock().source_error(&self.name, &error.to_string());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    async fn stream(
        &self,
        network: &Network,
        tx: &mpsc::Sender<Announcement>,
        stats: &super::stats::Shared,
    ) -> Result<()> {
        let mut url = self.endpoint("/eth/v1/events");
        url.query_pairs_mut()
            .append_pair("topics", "block_gossip,block");
        let send = |url| self.client.get(url).headers(self.headers.clone()).send();
        let mut response = tokio::time::timeout(transport::ENGINE_TIMEOUT, send(url))
            .await?
            .map_err(|_| anyhow::anyhow!("event stream connection error"))?;
        let mut topic = "block_gossip,block";
        if response.status() == reqwest::StatusCode::BAD_REQUEST {
            let mut url = self.endpoint("/eth/v1/events");
            url.query_pairs_mut().append_pair("topics", "block");
            response = tokio::time::timeout(transport::ENGINE_TIMEOUT, send(url))
                .await?
                .map_err(|_| anyhow::anyhow!("event stream connection error"))?;
            topic = "block";
        }
        ensure!(
            response.status().is_success(),
            "SSE HTTP status {}",
            response.status().as_u16()
        );
        ensure!(
            response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.starts_with("text/event-stream")),
            "expected SSE content type"
        );
        stats.lock().source_connected(&self.name, topic);
        // Bound parser memory even for a source that never terminates an SSE frame.
        // The cap applies to one connection; normal reconnect reconciles any gap.
        let stream = response
            .bytes_stream()
            .scan(0usize, |total, chunk| {
                let result = match chunk {
                    Ok(bytes) if bytes.len() <= 1024 * 1024 - *total => {
                        *total += bytes.len();
                        Ok(bytes)
                    }
                    _ => Err(std::io::Error::other(
                        "event stream byte limit or transport error",
                    )),
                };
                futures_util::future::ready(Some(result))
            })
            .eventsource();
        tokio::pin!(stream);
        let reconcile = self.reconcile(tx, network);
        tokio::pin!(reconcile);
        let mut reconciled = false;
        let revalidate = async {
            loop {
                tokio::time::sleep(Duration::from_secs(
                    network.seconds_per_slot * SLOTS_PER_EPOCH,
                ))
                .await;
                if let Err(error) = self.validate(network).await {
                    break Err(error);
                }
            }
        };
        tokio::pin!(revalidate);
        loop {
            tokio::select! {
                result = &mut reconcile, if !reconciled => {
                    reconciled = true;
                    if result.is_err() {
                        let mut s = stats.lock();
                        let source = s.sources.entry(self.name.clone()).or_default();
                        source.errors += 1;
                        source.detail = "event stream connected; reconciliation failed".into();
                    }
                }
                result = &mut revalidate => return result,
                next = tokio::time::timeout(Duration::from_secs(network.seconds_per_slot * 3), stream.next()) => {
                    let event = next?.ok_or_else(|| anyhow::anyhow!("event stream ended"))?
                        .map_err(|_| anyhow::anyhow!("invalid or interrupted SSE stream"))?;
                    let kind = match event.event.as_str() { "block_gossip" => "block_gossip", "block" => "block", _ => continue };
                    #[derive(Deserialize)]
                    struct BlockEvent { block: B256, slot: String }
                    let event: BlockEvent = sonic_rs::from_str(&event.data).map_err(|_| anyhow::anyhow!("invalid block event"))?;
                    let slot = event.slot.parse().map_err(|_| anyhow::anyhow!("invalid event slot"))?;
                    stats.lock().source_event(&self.name);
                    tx.send(Announcement { root: event.block, slot, source: self.name.clone(), kind, at: Instant::now() }).await?;
                }
            }
        }
    }
}
