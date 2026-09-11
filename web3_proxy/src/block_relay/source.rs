use super::{
    config::{Network, Source, SLOTS_PER_EPOCH},
    transport,
};
use alloy::primitives::B256;
use alloy_rpc_types_beacon::{
    config::ForkScheduleResponse, genesis::GenesisResponse, header::HeaderResponse,
};
use anyhow::{ensure, Result};
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use serde::{de::DeserializeOwned, Deserialize};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};

/// Retry failed sources without canceling or waiting for unrelated pending reads.
pub(super) async fn race_sources<T, F, Fut>(
    sources: &[Arc<BeaconSource>],
    deadline: Instant,
    notify: &tokio::sync::Notify,
    read: F,
) -> Result<(T, String)>
where
    F: Fn(Arc<BeaconSource>) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut race = futures_util::stream::FuturesUnordered::new();
    let mut in_flight = vec![false; sources.len()];
    let mut retry = true;
    let mut delay = Duration::from_millis(20);
    let mut next_retry = Instant::now() + delay;
    loop {
        if retry {
            for (index, source) in sources.iter().enumerate() {
                if in_flight[index] || !source.verified.load(Ordering::Acquire) {
                    continue;
                }
                in_flight[index] = true;
                let future = read(source.clone());
                race.push(async move { (index, future.await) });
            }
            retry = false;
        }
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => anyhow::bail!("acquisition deadline"),
            Some((index, result)) = race.next(), if !race.is_empty() => {
                in_flight[index] = false;
                if let Ok(value) = result { return Ok((value, sources[index].name.clone())); }
            }
            _ = notify.notified() => retry = true,
            _ = tokio::time::sleep_until(next_retry) => {
                retry = true;
                delay = (delay * 2).min(Duration::from_millis(200));
                next_retry = Instant::now() + delay;
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct Announcement {
    pub at_unix_us: u64,
    pub root: B256,
    pub slot: u64,
    pub source: String,
    pub kind: &'static str,
    pub at: Instant,
}

pub struct BeaconSource {
    pub name: String,
    pub(super) http: transport::BeaconHttp,
    pub verified: AtomicBool,
}
impl BeaconSource {
    pub fn new(name: String, config: &Source) -> Result<Self> {
        Ok(Self {
            name,
            http: transport::BeaconHttp::new(&config.beacon_url, &config.headers)?,
            verified: AtomicBool::new(false),
        })
    }
    pub async fn get_bytes(&self, path: &str) -> Result<bytes::Bytes> {
        self.http
            .get_bytes(path)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Beacon block not found"))
    }
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        sonic_rs::from_slice(&self.get_bytes(path).await?)
            .map_err(|_| anyhow::anyhow!("invalid Beacon response"))
    }
    pub async fn validate(&self, network: &Network) -> Result<()> {
        validate_network(&self.http, network).await
    }
}

// Decode only the required network fields. The Beacon spec also contains arrays
// such as BLOB_SCHEDULE, so its values cannot all be decoded as strings.
#[derive(Deserialize)]
struct NetworkSpecResponse {
    data: NetworkSpec,
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct NetworkSpec {
    preset_base: String,
    seconds_per_slot: String,
}

pub(super) async fn validate_network(
    http: &transport::BeaconHttp,
    network: &Network,
) -> Result<()> {
    let (genesis, schedule, spec) = tokio::try_join!(
        http.get_optional::<GenesisResponse>("/eth/v1/beacon/genesis"),
        http.get_optional::<ForkScheduleResponse>("/eth/v1/config/fork_schedule"),
        http.get_optional::<NetworkSpecResponse>("/eth/v1/config/spec"),
    )?;
    let genesis = genesis.ok_or_else(|| anyhow::anyhow!("missing Beacon genesis"))?;
    let schedule = schedule.ok_or_else(|| anyhow::anyhow!("missing Beacon fork schedule"))?;
    let spec = spec.ok_or_else(|| anyhow::anyhow!("missing Beacon spec"))?;
    ensure!(
        genesis.data.genesis_validators_root == network.genesis_validators_root
            && genesis.data.genesis_time == network.genesis_time,
        "Beacon genesis mismatch"
    );
    ensure!(
        spec.data.preset_base == "mainnet",
        "unsupported Beacon preset"
    );
    ensure!(
        spec.data.seconds_per_slot.parse::<u64>().ok() == Some(network.seconds_per_slot),
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
impl BeaconSource {
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
                at_unix_us: super::stats::unix_micros(),
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
            match self.validate(&network).await {
                Ok(()) => {
                    self.verified.store(true, Ordering::Release);
                }
                Err(error) => {
                    self.verified.store(false, Ordering::Release);
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
        let mut url = self.http.endpoint("/eth/v1/events");
        url.query_pairs_mut()
            .append_pair("topics", "block_gossip,block");
        let send = |url| {
            self.http
                .client
                .get(url)
                .headers(self.http.headers.clone())
                .send()
        };
        let mut response = tokio::time::timeout(transport::ENGINE_TIMEOUT, send(url))
            .await?
            .map_err(|_| anyhow::anyhow!("event stream connection error"))?;
        let mut topic = "block_gossip,block";
        if response.status() == reqwest::StatusCode::BAD_REQUEST {
            let mut url = self.http.endpoint("/eth/v1/events");
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
                    if network.slot().abs_diff(slot) <= 3 { stats.lock().source_event(&self.name, slot); }
                    tx.send(Announcement { root: event.block, slot, source: self.name.clone(), kind, at: Instant::now(), at_unix_us: super::stats::unix_micros() }).await?;
                }
            }
        }
    }
}
