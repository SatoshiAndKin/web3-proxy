//! Rate-limited communication with a web3 provider.
use super::blockchain::{
    ArcHeader, BlockHeader, BlockHydrationCoordinator, BlockResponseCache, BlockResponseCacheKey,
    BlocksByHashCache, BlocksByNumberCache, CachedBlockResponse, HeadObservationPublisher,
};
use super::provider::{connect_ws, AlloyWsProvider};
use super::request::{OpenRequestHandle, OpenRequestResult};
use crate::app::Web3ProxyJoinHandle;
use crate::config::Web3RpcConfig;
use crate::errors::{Web3ProxyError, Web3ProxyErrorContext, Web3ProxyResult};
use crate::globals;
use crate::jsonrpc::ValidatedRequest;
use crate::jsonrpc::{self, JsonRpcParams, JsonRpcResultData};
use crate::rpcs::request::RequestErrorHandler;
use alloy::primitives::{Address, Bytes, TxHash, B256, U256, U64};
use alloy::providers::Provider;
use alloy::rpc::types::Block;
use anyhow::{anyhow, Context};
use arc_swap::ArcSwapOption;
use deduped_broadcast::DedupedBroadcaster;
use futures::future::select_all;
use futures::StreamExt;
use latency::{EwmaLatency, PeakEwmaLatency, RollingQuantileLatency};
use nanorand::tls::TlsWyRand;
use nanorand::Rng;
use parking_lot::RwLock;
use serde::ser::{SerializeStruct, Serializer};
use serde::Serialize;
use sonic_rs::{json, OwnedLazyValue};
use std::borrow::Cow;
use std::cmp::Reverse;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{self, AtomicBool, AtomicU32, AtomicU64, AtomicUsize};
use std::{cmp::Ordering, sync::Arc};
use tokio::select;
use tokio::sync::watch;
use tokio::time::{interval, sleep, sleep_until, Duration, Instant, MissedTickBehavior};
use tracing::{debug, error, info, trace, warn, Level};
use url::Url;

/// An active connection to a Web3 RPC server like geth or erigon.
/// TODO: smarter Default derive or move the channels around so they aren't part of this at all
#[derive(Default)]
pub struct Web3Rpc {
    pub name: String,
    pub chain_id: u64,
    pub client_version: RwLock<Option<String>>,
    pub block_interval: Duration,
    pub display_name: Option<String>,

    /// Track in-flight requests
    pub(super) active_requests: AtomicUsize,
    /// mapping of block numbers and hashes
    pub(super) block_map: Option<BlocksByHashCache>,
    /// canonical mapping of block numbers to hashes
    pub(super) block_number_map: Option<BlocksByNumberCache>,
    /// raw block responses keyed by hash and transaction form
    pub(super) block_response_cache: Option<BlockResponseCache>,
    /// created_at is only inside an Option so that the "Default" derive works. it will always be set.
    pub(super) created_at: Option<Instant>,
    /// if no ipc_stream, most all requests prefer to use the http_provider
    pub(super) http_client: Option<reqwest::Client>,
    pub(super) http_url: Option<Url>,
    /// the websocket url is only used for subscriptions
    pub(super) ws_url: Option<Url>,
    /// the websocket provider is only used for subscriptions
    pub(super) ws_provider: ArcSwapOption<AlloyWsProvider>,
    /// most all requests prefer the ipc provider.
    /// TODO: ArcSwapOption?
    pub(super) ipc_path: Option<PathBuf>,
    /// keep track of hard limits
    /// hard_limit_until is only inside an Option so that the "Default" derive works. it will always be set.
    pub(super) hard_limit_until: Option<watch::Sender<Instant>>,
    /// used for ensuring enough requests are available before advancing the head block
    pub(super) soft_limit: u32,
    /// use web3 queries to find the block data limit for archive/pruned nodes
    pub(super) automatic_block_limit: bool,
    /// Use web3 queries to find the separate retained log history.
    pub(super) automatic_log_limit: bool,
    /// only use this rpc if everything else is lagging too far. this allows us to ignore fast but very low limit rpcs
    pub backup: bool,
    /// if subscribed to new heads, blocks are sent through this channel to update a parent Web3Rpcs
    pub(super) head_observation_publisher: Option<HeadObservationPublisher>,
    pub(super) block_hydration: Option<Arc<BlockHydrationCoordinator>>,
    /// TODO: have an enum for this so that "no limit" prints pretty?
    pub(super) block_data_limit: AtomicU64,
    /// Oldest log-query range relative to the current head.
    pub(super) log_data_limit: AtomicU64,
    /// head_block is only inside an Option so that the "Default" derive works. it will always be set.
    pub(super) head_block_sender: Option<watch::Sender<Option<BlockHeader>>>,
    /// Track head block latency.
    /// TODO: This is in a sync lock, but writes are infrequent and quick. Is this actually okay? Set from a spawned task and read an atomic instead?
    pub(super) head_delay: RwLock<EwmaLatency>,
    /// false if a health check has failed
    pub(super) healthy: AtomicBool,
    /// Track peak request latency
    /// peak_latency is only inside an Option so that the "Default" derive works. it will always be set.
    pub(super) peak_latency: Option<PeakEwmaLatency>,
    /// Automatically set priority based on request latency and active requests
    pub(super) tier: AtomicU32,
    /// Track total requests served.
    pub(super) total_requests: AtomicUsize,
    /// If the head block is too old, it is ignored.
    pub(super) max_head_block_age: Duration,
    /// Track request latency.
    /// request_ms_histogram is only inside an Option so that the "Default" derive works. it will always be set.
    pub(super) median_latency: Option<RollingQuantileLatency>,
    /// disconnect_watch is only inside an Option so that the "Default" derive works. it will always be set.
    /// todo!(qthis gets cloned a TON. probably too much. something seems wrong)
    pub(super) disconnect_watch: Option<watch::Sender<bool>>,
    /// if subscribed to pending transactions, transactions are sent through this channel to update a parent Web3App
    pub(super) pending_txid_firehose: Option<Arc<DedupedBroadcaster<TxHash>>>,
}

impl Web3Rpc {
    /// Connect to a web3 rpc
    // TODO: have this take a builder (which will have channels attached). or maybe just take the config and give the config public fields
    #[allow(clippy::too_many_arguments)]
    pub async fn spawn(
        config: Web3RpcConfig,
        name: String,
        chain_id: u64,
        // optional because this is only used for http providers. websocket-only providers don't use it
        http_client: Option<reqwest::Client>,
        block_interval: Duration,
        block_map: BlocksByHashCache,
        block_number_map: BlocksByNumberCache,
        block_response_cache: BlockResponseCache,
        head_observation_publisher: Option<HeadObservationPublisher>,
        block_hydration: Option<Arc<BlockHydrationCoordinator>>,
        pending_txid_firehose: Option<Arc<DedupedBroadcaster<TxHash>>>,
        max_head_block_age: Duration,
    ) -> anyhow::Result<(Arc<Web3Rpc>, Web3ProxyJoinHandle<()>)> {
        let created_at = Instant::now();

        let backup = config.backup;

        let block_data_limit: AtomicU64 = config.block_data_limit.into();
        let automatic_block_limit = (block_data_limit.load(atomic::Ordering::SeqCst) == 0)
            && head_observation_publisher.is_some();
        let log_data_limit: AtomicU64 = config.log_data_limit.into();
        let automatic_log_limit = (log_data_limit.load(atomic::Ordering::SeqCst) == 0)
            && head_observation_publisher.is_some();

        // have a sender for tracking hard limit anywhere. we use this in case we
        // and track on servers that have a configured hard limit
        let (hard_limit_until, _) = watch::channel(Instant::now());

        if config.ws_url.is_none() && config.http_url.is_none() {
            return Err(anyhow!(
                "either ws_url or http_url are required. it is best to set both. they must both point to the same server!"
            ));
        }

        let (head_block, _) = watch::channel(None);

        // Spawn the task for calculting average peak latency
        // TODO Should these defaults be in config
        let peak_latency = PeakEwmaLatency::spawn(
            // Decay over 15s
            Duration::from_secs(15),
            // Peak requests so far around 5k, we will use an order of magnitude
            // more to be safe. Should only use about 50mb RAM
            50_000,
            // Start latency at 1 second
            Duration::from_secs(1),
        );

        let median_request_latency = RollingQuantileLatency::spawn_median(1_000).await;

        let (http_url, http_client) = if let Some(http_url) = config.http_url {
            let http_url = http_url.parse::<Url>()?;
            // TODO: double-check not missing anything from connect_http()
            let http_client = http_client.unwrap_or_default();
            (Some(http_url), Some(http_client))
        } else {
            (None, None)
        };

        let ws_url = if let Some(ws_url) = config.ws_url {
            let ws_url = ws_url.parse::<Url>()?;

            Some(ws_url)
        } else {
            None
        };

        let (disconnect_watch, _) = watch::channel(false);

        // TODO: start optimistically?
        let healthy = false.into();

        let pending_txid_firehose = if config.subscribe_txs {
            // TODO: error if subscribe_txs but not pending_txid_firehose
            pending_txid_firehose
        } else {
            None
        };

        let new_rpc = Self {
            automatic_block_limit,
            automatic_log_limit,
            backup,
            block_data_limit,
            log_data_limit,
            block_interval,
            block_map: Some(block_map),
            block_number_map: Some(block_number_map),
            block_response_cache: Some(block_response_cache),
            chain_id,
            created_at: Some(created_at),
            display_name: config.display_name,
            hard_limit_until: Some(hard_limit_until),
            head_block_sender: Some(head_block),
            http_url,
            http_client,
            ipc_path: config.ipc_path,
            max_head_block_age,
            name,
            peak_latency: Some(peak_latency),
            median_latency: Some(median_request_latency),
            soft_limit: config.soft_limit,
            pending_txid_firehose,
            head_observation_publisher,
            block_hydration,
            ws_url,
            disconnect_watch: Some(disconnect_watch),
            healthy,
            ..Default::default()
        };

        let new_connection = Arc::new(new_rpc);

        // subscribe to new blocks and new transactions
        // subscribing starts the connection (with retries)
        let handle = {
            let new_connection = new_connection.clone();
            tokio::spawn(async move { new_connection.subscribe_with_reconnect().await })
        };

        Ok((new_connection, handle))
    }

    pub fn next_available(&self, now: Instant) -> Instant {
        if let Some(hard_limit_until) = self.hard_limit_until.as_ref() {
            let hard_limit_until = *hard_limit_until.borrow();

            hard_limit_until.max(now)
        } else {
            now
        }
    }

    /// sort by...
    /// - rate limit (ascending)
    /// - backups last
    /// - block number (descending)
    /// - tier (ascending)
    ///
    /// TODO: tests on this!
    /// TODO: should tier or block number take priority?
    /// TODO: should this return a struct that implements sorting traits?
    /// TODO: better return type!
    /// TODO: move this to consensus.rs?
    fn sort_on(
        &self,
        max_block: Option<U64>,
        start_instant: Instant,
    ) -> (Instant, bool, Reverse<U64>, u32) {
        let mut head_block = self
            .head_block_sender
            .as_ref()
            .and_then(|x| x.borrow().as_ref().map(|x| x.number()))
            .unwrap_or_default();

        if let Some(max_block) = max_block {
            head_block = head_block.min(max_block);
        }

        let tier = self.tier.load(atomic::Ordering::SeqCst);

        let backup = self.backup;

        let next_available = self.next_available(start_instant);

        (next_available, !backup, Reverse(head_block), tier)
    }

    /// sort with `sort_on` and then on `weighted_peak_latency`
    /// This is useful when you care about latency over spreading the load
    /// For example, use this when selecting rpcs for balanced_rpcs
    /// TODO: move this to consensus.rs?
    /// TODO: better return type!
    pub fn sort_for_load_balancing_on(
        &self,
        max_block: Option<U64>,
        start_instant: Instant,
    ) -> ((Instant, bool, Reverse<U64>, u32), Duration) {
        let sort_on = self.sort_on(max_block, start_instant);

        // // TODO: once we do power-of-2 choices, use median_latency here instead of weighted_latency. though its already part of tiers so maybe its fine
        // let median_latency = self
        //     .median_latency
        //     .as_ref()
        //     .map(|x| x.seconds())
        //     .unwrap_or_default();

        let weighted_latency = self.weighted_peak_latency();

        let x = (sort_on, weighted_latency);

        trace!("sort_for_load_balancing {}: {:?}", self, x);

        x
    }

    /// like sort_for_load_balancing, but shuffles tiers randomly instead of sorting by weighted_peak_latency
    /// This is useful when you care about spreading the load over latency.
    /// For example, use this when selecting rpcs for protected_rpcs
    /// TODO: move this to consensus.rs?
    /// TODO: better return type
    pub fn shuffle_for_load_balancing_on(
        &self,
        max_block: Option<U64>,
        rng: &mut TlsWyRand,
        start_instant: Instant,
    ) -> ((Instant, bool, Reverse<U64>, u32), u8) {
        let sort_on = self.sort_on(max_block, start_instant);

        let r = rng.generate::<u8>();

        (sort_on, r)
    }

    pub fn weighted_peak_latency(&self) -> Duration {
        let peak_latency = if let Some(peak_latency) = self.peak_latency.as_ref() {
            peak_latency.latency()
        } else {
            Duration::from_secs(1)
        };

        // TODO: what scaling?
        // TODO: figure out how many requests add what level of latency
        let request_scaling = 0.01;
        // TODO: what ordering?
        let active_requests =
            self.active_requests.load(atomic::Ordering::SeqCst) as f32 * request_scaling + 1.0;

        peak_latency.mul_f32(active_requests)
    }

    // TODO: would be great if rpcs exposed this. see https://github.com/ledgerwatch/erigon/issues/6391
    async fn check_block_data_limit(self: &Arc<Self>) -> anyhow::Result<Option<u64>> {
        if !self.automatic_block_limit {
            // TODO: is this a good thing to return?
            return Ok(None);
        }

        // TODO: check eth_syncing. if it is not false, return Ok(None)

        let mut limit = None;

        // TODO: binary search between 90k and max?
        // TODO: start at 0 or 1?
        let mut last = U256::MAX;
        // TODO: these should all be U256, not u64
        for block_data_limit in [0, 32, 64, 128, 256, 512, 1024, 90_000, u64::MAX] {
            let head_block_num = self
                .internal_request::<_, U256>(
                    "eth_blockNumber".into(),
                    &[(); 0],
                    // error here are expected, so keep the level low
                    Some(Level::DEBUG.into()),
                    Some(Duration::from_secs(5)),
                )
                .await
                .context("head_block_num error during check_block_data_limit")?;

            let maybe_archive_block =
                head_block_num.saturating_sub(U256::from_limbs([block_data_limit, 0, 0, 0]));

            if last == maybe_archive_block {
                // we already checked it. exit early
                break;
            }

            last = maybe_archive_block;

            trace!(
                "checking maybe_archive_block on {}: {}",
                self,
                maybe_archive_block
            );

            // TODO: wait for the handle BEFORE we check the current block number. it might be delayed too!
            // TODO: what should the request be?
            let archive_result: Result<Bytes, _> = self
                .internal_request(
                    "eth_getCode".into(),
                    &json!((
                        "0xdead00000000000000000000000000000000beef",
                        maybe_archive_block,
                    )),
                    // error here are expected, so keep the level low
                    Some(Level::TRACE.into()),
                    Some(Duration::from_secs(5)),
                )
                .await;

            trace!(
                "archive_result on {} for {} ({}): {:?}",
                self,
                block_data_limit,
                maybe_archive_block,
                archive_result
            );

            if archive_result.is_err() {
                break;
            }

            limit = Some(block_data_limit);
        }

        if let Some(limit) = limit {
            if limit == 0 {
                warn!("{} is unable to serve requests", self);
            }

            self.block_data_limit.store(limit, atomic::Ordering::SeqCst);
        }

        if limit == Some(u64::MAX) {
            info!("block data limit on {}: archive", self);
        } else {
            info!("block data limit on {}: {:?}", self, limit);
        }

        Ok(limit)
    }

    async fn check_log_data_limit(self: &Arc<Self>) -> anyhow::Result<Option<u64>> {
        if !self.automatic_log_limit {
            return Ok(None);
        }

        let head_block_num = self
            .internal_request::<_, U256>(
                "eth_blockNumber".into(),
                &[(); 0],
                Some(Level::DEBUG.into()),
                Some(Duration::from_secs(5)),
            )
            .await
            .context("head_block_num error during check_log_data_limit")?;
        let mut limit = None;
        let mut last = U256::MAX;

        for log_data_limit in [0, 32, 64, 128, 256, 512, 1024, 90_000, u64::MAX] {
            let maybe_historical_block =
                head_block_num.saturating_sub(U256::from_limbs([log_data_limit, 0, 0, 0]));

            if last == maybe_historical_block {
                break;
            }
            last = maybe_historical_block;

            let log_result: Result<Vec<OwnedLazyValue>, _> = self
                .internal_request(
                    "eth_getLogs".into(),
                    &json!([{
                        "fromBlock": maybe_historical_block,
                        "toBlock": maybe_historical_block,
                    }]),
                    Some(Level::TRACE.into()),
                    Some(Duration::from_secs(5)),
                )
                .await;

            trace!(
                rpc = %self,
                log_data_limit,
                block = %maybe_historical_block,
                ?log_result,
                "checked log data limit"
            );

            if log_result.is_err() {
                break;
            }

            limit = Some(log_data_limit);
        }

        if let Some(limit) = limit {
            self.log_data_limit.store(limit, atomic::Ordering::SeqCst);
        }

        if limit == Some(u64::MAX) {
            info!(rpc = %self, "log data limit is archive");
        } else {
            info!(rpc = %self, ?limit, "detected log data limit");
        }

        Ok(limit)
    }

    /// TODO: this might be too simple. different nodes can prune differently. its possible we will have a block range
    pub fn block_data_limit(&self) -> U64 {
        U64::from_limbs([self.block_data_limit.load(atomic::Ordering::SeqCst)])
    }

    pub fn log_data_limit(&self) -> U64 {
        U64::from_limbs([self.log_data_limit.load(atomic::Ordering::SeqCst)])
    }

    fn health_status(&self, provider_health_check_passed: bool) -> bool {
        provider_health_check_passed && self.block_data_limit.load(atomic::Ordering::SeqCst) > 0
    }

    /// TODO: get rid of this now that consensus rpcs does it
    pub fn has_block_data(&self, needed_block_num: U64) -> bool {
        self.has_data_with_limit(needed_block_num, self.block_data_limit())
    }

    pub fn has_log_data(&self, needed_block_num: U64) -> bool {
        self.has_data_with_limit(needed_block_num, self.log_data_limit())
    }

    pub fn has_data_for_request(&self, request: &ValidatedRequest, needed_block_num: U64) -> bool {
        if request.requires_log_history() {
            self.has_log_data(needed_block_num)
        } else {
            self.has_block_data(needed_block_num)
        }
    }

    fn has_data_with_limit(&self, needed_block_num: U64, data_limit: U64) -> bool {
        if let Some(head_block_sender) = self.head_block_sender.as_ref() {
            // TODO: this needs a max of our overall head block number
            let head_block_num = match head_block_sender.borrow().as_ref() {
                None => return false,
                Some(x) => x.number(),
            };

            // this rpc doesn't have that block yet. still syncing
            if needed_block_num > head_block_num {
                trace!(
                    "{} has head {} but needs {}",
                    self,
                    head_block_num,
                    needed_block_num,
                );
                return false;
            }

            // if this is a pruning node, we might not actually have the block
            let oldest_block_num = head_block_num.saturating_sub(data_limit);

            if needed_block_num < oldest_block_num {
                trace!(
                    "{} needs {} but the oldest available is {}",
                    self,
                    needed_block_num,
                    oldest_block_num
                );
                return false;
            }
            true
        } else {
            // do we want true or false here? false is accurate, but it stops the proxy from sending any requests so I think we want to lie
            true
        }
    }

    /// query the web3 provider to confirm it is on the expected chain with the expected data available
    /// TODO: this currently checks only the http if both http and ws are set. it should check both and make sure they match
    async fn check_provider(self: &Arc<Self>) -> Web3ProxyResult<()> {
        // TODO: different handlers for backup vs primary
        let error_handler = Some(Level::TRACE.into());

        // TODO: make this configurable. voltaire bundler uses web3_bundlerVersion
        match self
            .internal_request::<_, String>(
                "web3_clientVersion".into(),
                &(),
                error_handler,
                Some(Duration::from_secs(5)),
            )
            .await
        {
            Ok(client_version) => {
                // this is a sync lock, but we only keep it open for a short time
                // TODO: something more friendly to async that also works with serde's Serialize
                let mut lock = self.client_version.write();

                *lock = Some(client_version);
            }
            Err(err) => {
                let mut lock = self.client_version.write();

                *lock = Some(format!("error: {}", err));

                error!(?err, "failed fetching client version of {}", self);
            }
        }

        // check the server's chain_id here
        // TODO: some public rpcs (on bsc and fantom) do not return an id and so this ends up being an error
        // TODO: what should the timeout be? should there be a request timeout?
        // trace!("waiting on chain id for {}", self);
        let found_chain_id: U64 = self
            .internal_request(
                "eth_chainId".into(),
                &[(); 0],
                error_handler,
                Some(Duration::from_secs(5)),
            )
            .await?;

        trace!("found_chain_id: {:#?}", found_chain_id);

        if self.chain_id != found_chain_id.to::<u64>() {
            return Err(anyhow::anyhow!(
                "incorrect chain id! Config has {}, but RPC has {}",
                self.chain_id,
                found_chain_id
            )
            .context(format!("failed @ {}", self))
            .into());
        }

        // TODO: only do this for balanced_rpcs. this errors on 4337 rpcs
        self.check_block_data_limit()
            .await
            .context(format!("unable to check_block_data_limit of {}", self))?;
        self.check_log_data_limit()
            .await
            .context(format!("unable to check_log_data_limit of {}", self))?;

        info!("successfully connected to {}", self);

        Ok(())
    }

    pub(crate) async fn send_head_block_result(
        self: &Arc<Self>,
        new_head_block: Web3ProxyResult<Option<ArcHeader>>,
    ) -> Web3ProxyResult<()> {
        let head_block_sender = self
            .head_block_sender
            .as_ref()
            .expect("head_block_sender is always set");
        let mut observation_sent = false;

        let new_head_block = match new_head_block {
            Ok(x) => {
                let x = x.map(BlockHeader::new);

                match x {
                    None => {
                        if head_block_sender.borrow().is_none() {
                            // we previously sent a None. return early
                            return Ok(());
                        }

                        let age = self.created_at.unwrap().elapsed().as_millis();

                        trace!("clearing head block on {} ({}ms old)!", self, age);

                        // TODO: clear self.block_data_limit?

                        // send an empty block to take this server out of rotation
                        None
                    }
                    Some(new_head_block) => {
                        let new_hash = *new_head_block.hash();

                        if let Some(head_observation_publisher) = &self.head_observation_publisher {
                            head_observation_publisher
                                .publish(Some(new_head_block.clone()), self.clone())
                                .context("head observation publisher failed sending")?;
                            observation_sent = true;
                        }

                        if let Some(block_hydration) = &self.block_hydration {
                            block_hydration.announce(self.clone(), new_hash).await;
                        }

                        // if we already have this block saved, set new_head_block to that arc. otherwise store this copy
                        let new_head_block = self
                            .block_map
                            .as_ref()
                            .unwrap()
                            .get_with(new_hash, async move { new_head_block })
                            .await;

                        // we are synced! yey!

                        Some(new_head_block)
                    }
                }
            }
            Err(err) => {
                warn!(?err, "unable to get block from {}", self);

                // send an empty block to take this server out of rotation
                head_block_sender.send_replace(None);

                // TODO: clear self.block_data_limit?

                None
            }
        };

        if observation_sent {
            return Ok(());
        }

        if let Some(head_observation_publisher) = &self.head_observation_publisher {
            // tell web3rpcs about this rpc having this block
            // web3rpcs will do `self.head_block_sender.send_replace(new_head_block)`
            head_observation_publisher
                .publish(new_head_block.clone(), self.clone())
                .context("head observation publisher failed sending")?;
        } else {
            head_block_sender.send_replace(new_head_block.clone());
        }

        Ok(())
    }

    pub(super) async fn invalidate_uncle_headers_if_canonical(&self, block: &CachedBlockResponse) {
        let (Some(block_map), Some(block_number_map)) = (&self.block_map, &self.block_number_map)
        else {
            return;
        };

        if block_number_map.get(&block.block_number()).await != Some(block.block_hash()) {
            return;
        }

        for uncle_hash in block.uncle_hashes() {
            block_map.invalidate(uncle_hash).await;
        }
    }

    async fn cache_hashes_block_response(&self, result: Arc<OwnedLazyValue>) {
        let Some(cache) = &self.block_response_cache else {
            return;
        };

        match CachedBlockResponse::from_hashes(result) {
            Ok(block) => {
                cache
                    .insert(
                        BlockResponseCacheKey::new(block.block_hash(), false),
                        block.clone(),
                    )
                    .await;
                self.invalidate_uncle_headers_if_canonical(&block).await;
            }
            Err(err) => {
                debug!(?err, "not caching malformed hash-only block from {}", self);
            }
        }
    }

    pub(super) async fn fetch_full_block(
        self: &Arc<Self>,
        block_hash: B256,
    ) -> Web3ProxyResult<(CachedBlockResponse, CachedBlockResponse)> {
        let result = self
            .internal_request::<_, Option<Arc<OwnedLazyValue>>>(
                "eth_getBlockByHash".into(),
                &(block_hash, true),
                None,
                Some(Duration::from_secs(5)),
            )
            .await?
            .ok_or_else(|| Web3ProxyError::BadResponse("hydrated block result was null".into()))?;
        CachedBlockResponse::from_full(result, block_hash)
    }

    async fn latest_block_header(
        self: &Arc<Self>,
        error_handler: Option<RequestErrorHandler>,
    ) -> Web3ProxyResult<Option<ArcHeader>> {
        let result = self
            .internal_request::<_, Option<Arc<OwnedLazyValue>>>(
                "eth_getBlockByNumber".into(),
                &("latest", false),
                error_handler,
                Some(Duration::from_secs(5)),
            )
            .await?;

        let Some(result) = result else {
            return Ok(None);
        };
        let block: Block = sonic_rs::from_str(
            &sonic_rs::to_string(&result).expect("latest block result must serialize"),
        )?;
        self.cache_hashes_block_response(result).await;

        Ok(Some(Arc::new(block.header)))
    }

    #[inline(always)]
    fn should_disconnect(&self) -> bool {
        *self.disconnect_watch.as_ref().unwrap().borrow()
    }

    async fn check_health(
        self: &Arc<Self>,
        detailed_healthcheck: bool,
        error_handler: Option<RequestErrorHandler>,
    ) -> Web3ProxyResult<()> {
        let head_block = self.head_block_sender.as_ref().unwrap().borrow().clone();

        if let Some(head_block) = head_block {
            if head_block.age() > self.max_head_block_age {
                // TODO: if the server is expected to be syncing, make a way to quiet this error
                return Err(Web3ProxyError::OldHead(self.clone(), head_block));
            }

            if detailed_healthcheck {
                let block_number = head_block.number();
                let probe_address = "0xdead00000000000000000000000000000000beef"
                    .parse::<Address>()
                    .expect("fixed health-check address must be valid");

                let _code = self
                    .internal_request::<_, Option<Bytes>>(
                        "eth_getCode".into(),
                        &(probe_address, block_number),
                        error_handler,
                        Some(Duration::from_secs(5)),
                    )
                    .await?;
            }
        } else {
            // TODO: if head block is none for too long, give an error
        }

        Ok(())
    }

    /// TODO: this needs to be a subscribe_with_reconnect that does a retry with jitter and exponential backoff
    async fn subscribe_with_reconnect(self: Arc<Self>) -> Web3ProxyResult<()> {
        loop {
            match self.clone().subscribe().await {
                Err(err) => {
                    if self.should_disconnect() {
                        break;
                    }

                    warn!(?err, "subscribe err on {}", self);
                }
                _ => {
                    if self.should_disconnect() {
                        break;
                    }
                }
            }

            // TODO: exponential backoff with jitter
            if self.backup {
                debug!("reconnecting to {} in 10 seconds", self);
            } else {
                info!("reconnecting to {} in 10 seconds", self);
            }
            sleep(Duration::from_secs(10)).await;
        }

        Ok(())
    }

    /// subscribe to blocks and transactions
    /// This should only exit when the program is exiting.
    /// TODO: should more of these args be on self? chain_id for sure
    async fn subscribe(self: Arc<Self>) -> Web3ProxyResult<()> {
        let error_handler = if self.backup {
            Some(RequestErrorHandler::DebugLevel)
        } else {
            // TODO: info level?
            Some(RequestErrorHandler::InfoLevel)
        };

        if self.should_disconnect() {
            return Ok(());
        }

        if let Some(url) = self.ws_url.clone() {
            trace!("starting websocket provider on {}", self);

            let x = connect_ws(url).await?;

            let x = Arc::new(x);

            self.ws_provider.store(Some(x));
        }

        if self.should_disconnect() {
            return Ok(());
        }

        trace!("starting subscriptions on {}", self);

        if let Err(err) = self
            .check_provider()
            .await
            .web3_context("failed check_provider")
        {
            self.healthy.store(false, atomic::Ordering::SeqCst);
            return Err(err);
        }

        let mut futures = Vec::new();
        let mut abort_handles = vec![];

        // health check that runs if there haven't been any recent requests
        let health_handle = if self.head_observation_publisher.is_some() {
            // TODO: move this into a proper function
            let rpc = self.clone();

            // TODO: how often? different depending on the chain?
            // TODO: reset this timeout when a new block is seen? we need to keep median_request_latency updated though
            let health_sleep_seconds = 10;
            let block_data_limit_refresh_interval = Duration::from_secs(60);

            // health check loop
            let f = async move {
                // // TODO: benchmark this and lock contention
                // let mut old_total_requests = 0;
                // let mut new_total_requests;
                let mut block_data_limit_refresh_at =
                    Instant::now() + block_data_limit_refresh_interval;
                let mut log_data_limit_refresh_at = block_data_limit_refresh_at;

                // errors here should not cause the loop to exit! only mark unhealthy
                loop {
                    if rpc.should_disconnect() {
                        break;
                    }

                    // new_total_requests = rpc.total_requests.load(atomic::Ordering::Relaxed);

                    // let detailed_healthcheck = new_total_requests - old_total_requests < 5;

                    // TODO: i think there is an erigon bug when fetching transactions from a fresh block. disable detailed health checks for now
                    let detailed_healthcheck = false;

                    // TODO: if this fails too many times, reset the connection
                    let provider_health_check_passed =
                        match rpc.check_health(detailed_healthcheck, error_handler).await {
                            Err(err) => {
                                // TODO: different level depending on the error handler
                                // TODO: if rate limit error, set "retry_at"
                                if rpc.backup {
                                    warn!(?err, "health check on {} failed", rpc);
                                } else {
                                    error!(?err, "health check on {} failed", rpc);
                                }

                                false
                            }
                            Ok(()) => true,
                        };

                    if rpc.automatic_block_limit
                        && rpc.block_data_limit.load(atomic::Ordering::SeqCst) == 0
                        && Instant::now() >= block_data_limit_refresh_at
                    {
                        if let Err(err) = rpc.check_block_data_limit().await {
                            warn!(?err, "unable to refresh block data limit on {}", rpc);
                        }

                        block_data_limit_refresh_at =
                            Instant::now() + block_data_limit_refresh_interval;
                    }

                    if rpc.automatic_log_limit
                        && rpc.log_data_limit.load(atomic::Ordering::SeqCst) == 0
                        && Instant::now() >= log_data_limit_refresh_at
                    {
                        if let Err(err) = rpc.check_log_data_limit().await {
                            warn!(?err, "unable to refresh log data limit on {}", rpc);
                        }

                        log_data_limit_refresh_at =
                            Instant::now() + block_data_limit_refresh_interval;
                    }

                    rpc.healthy.store(
                        rpc.health_status(provider_health_check_passed),
                        atomic::Ordering::SeqCst,
                    );

                    // TODO: should we count the requests done inside this health check
                    // old_total_requests = new_total_requests;

                    sleep(Duration::from_secs(health_sleep_seconds)).await;
                }

                Ok(())
            };

            // TODO: log quick_check lik
            let initial_check = match self.check_health(false, error_handler).await {
                Err(err) => {
                    if self.backup {
                        warn!(?err, "initial health check on {} failed", self);
                    } else {
                        error!(?err, "initial health check on {} failed", self);
                    }

                    false
                }
                _ => true,
            };

            self.healthy
                .store(self.health_status(initial_check), atomic::Ordering::SeqCst);

            tokio::spawn(f)
        } else {
            let rpc = self.clone();
            let health_sleep_seconds = 60;

            let f = async move {
                // errors here should not cause the loop to exit! only mark unhealthy
                loop {
                    if rpc.should_disconnect() {
                        break;
                    }

                    // TODO: if this fails too many times, reset the connection
                    match rpc.check_provider().await {
                        Err(err) => {
                            rpc.healthy.store(false, atomic::Ordering::SeqCst);

                            // TODO: if rate limit error, set "retry_at"
                            if rpc.backup {
                                warn!(?err, "provider check on {} failed", rpc);
                            } else {
                                error!(?err, "provider check on {} failed", rpc);
                            }
                        }
                        _ => {
                            rpc.healthy.store(true, atomic::Ordering::SeqCst);
                        }
                    }

                    sleep(Duration::from_secs(health_sleep_seconds)).await;
                }

                Ok(())
            };

            tokio::spawn(f)
        };

        abort_handles.push(health_handle.abort_handle());
        futures.push(health_handle);

        // subscribe to new heads
        if self.head_observation_publisher.is_some() {
            let clone = self.clone();

            let f = async move { clone.subscribe_new_heads().await };

            let h = tokio::spawn(f);
            let a = h.abort_handle();

            futures.push(h);
            abort_handles.push(a);
        }

        // subscribe to new transactions
        if self.pending_txid_firehose.is_some() && self.ws_provider.load().is_some() {
            let clone = self.clone();

            let f = async move {
                let app = globals::APP.get().unwrap();
                let permit = app.tx_subscriptions.acquire().await?;

                let result = clone.subscribe_new_transactions().await;

                std::mem::drop(permit);

                result
            };

            // TODO: this is waking itself alot
            let h = tokio::spawn(f);
            let a = h.abort_handle();

            futures.push(h);
            abort_handles.push(a);
        }

        // exit if any of the futures exit
        let (first_exit, _, _) = select_all(futures).await;

        // mark unhealthy
        self.healthy.store(false, atomic::Ordering::SeqCst);

        debug!(?first_exit, "subscriptions on {} exited", self);

        // clear the head block
        self.send_head_block_result(Ok(None)).await?;

        // stop the other futures
        for a in abort_handles {
            a.abort();
        }

        self.ws_provider.store(None);

        Ok(())
    }

    async fn subscribe_new_transactions(self: &Arc<Self>) -> Web3ProxyResult<()> {
        trace!("subscribing to new transactions on {}", self);

        let pending_txid_firehose = self.pending_txid_firehose.as_ref().unwrap();

        if let Some(ws_provider) = self.ws_provider.load().as_ref() {
            // todo: move subscribe_blocks onto the request handle instead of having a seperate wait_for_throttle
            self.wait_for_throttle(Instant::now() + Duration::from_secs(5))
                .await?;

            // TODO: only subscribe if a user has subscribed
            let subscription = ws_provider.subscribe_pending_transactions().await?;
            let mut pending_txs_sub = subscription.into_stream();

            while let Some(x) = pending_txs_sub.next().await {
                pending_txid_firehose.send(x).await;
            }
        } else {
            // only websockets subscribe to pending transactions
            // its possible to do with http, but not recommended
            // TODO: what should we do here?
            unimplemented!()
        }

        Ok(())
    }

    /// Subscribe to new block headers.
    async fn subscribe_new_heads(self: &Arc<Self>) -> Web3ProxyResult<()> {
        info!("subscribing to new heads on {}", self);

        let error_handler = if self.backup {
            Some(Level::DEBUG.into())
        } else {
            Some(Level::ERROR.into())
        };

        if let Some(ws_provider) = self.ws_provider.load().as_ref() {
            self.wait_for_throttle(Instant::now() + Duration::from_secs(5))
                .await?;

            let subscription = ws_provider.subscribe_blocks().await?;
            let mut headers = subscription.into_stream();

            // query the block once since the subscription doesn't send the current block
            // there is a very small race condition here where the stream could send us a new block right now
            // but sending the same block twice won't break anything
            let latest_header = self.latest_block_header(error_handler).await;
            self.send_head_block_result(latest_header).await?;

            while let Some(header) = headers.next().await {
                let header = Ok(Some(Arc::new(header)));

                self.send_head_block_result(header).await?;
            }
        } else if self.http_client.is_some() {
            // there is a "watch_blocks" function, but a lot of public nodes (including ones using web3_proxy) do not support the necessary rpc endpoints
            // TODO: is 1/2 the block time okay?
            let mut i = interval(self.block_interval / 2);
            i.set_missed_tick_behavior(MissedTickBehavior::Delay);

            loop {
                let header_result = self.latest_block_header(error_handler).await;
                self.send_head_block_result(header_result).await?;

                // TODO: should this select be at the start or end of the loop?
                i.tick().await;
            }
        } else {
            return Err(anyhow!("no ws or http provider!").into());
        }

        // clear the head block. this might not be needed, but it won't hurt
        self.send_head_block_result(Ok(None)).await?;

        if self.should_disconnect() {
            trace!(%self, "new heads subscription exited");
            Ok(())
        } else {
            Err(anyhow!("new_heads subscription exited. reconnect needed").into())
        }
    }

    pub async fn wait_for_request_handle(
        self: &Arc<Self>,
        web3_request: &Arc<ValidatedRequest>,
        error_handler: Option<RequestErrorHandler>,
        allow_unhealthy: bool,
    ) -> Web3ProxyResult<OpenRequestHandle> {
        let connect_timeout_at = sleep_until(web3_request.connect_timeout_at());
        tokio::pin!(connect_timeout_at);

        loop {
            match self
                .try_request_handle(web3_request, error_handler, allow_unhealthy)
                .await
            {
                Ok(OpenRequestResult::Handle(handle)) => return Ok(handle),
                Ok(OpenRequestResult::RetryAt(retry_at)) => {
                    // TODO: emit a stat?
                    let wait = retry_at.duration_since(Instant::now());

                    trace!(
                        "waiting {} millis for request handle on {}",
                        wait.as_millis(),
                        self
                    );

                    // if things are slow this could happen in prod. but generally its a problem
                    debug_assert!(wait > Duration::from_secs(0));

                    // TODO: have connect_timeout in addition to the full ttl
                    if retry_at > web3_request.connect_timeout_at() {
                        // break now since we will wait past our maximum wait time
                        return Err(Web3ProxyError::Timeout(Some(
                            web3_request.start_instant.elapsed(),
                        )));
                    }

                    sleep_until(retry_at).await;
                }
                Ok(OpenRequestResult::Lagged(now_synced_f)) => {
                    select! {
                        _ = now_synced_f => {}
                        _ = &mut connect_timeout_at => {
                            break;
                        }
                    }
                }
                Ok(OpenRequestResult::Failed) => {
                    // TODO: when can this happen? log? emit a stat? is breaking the right thing to do?
                    trace!("{} has no handle ready", self);
                    break;
                }
                Err(err) => return Err(err),
            }
        }

        Err(Web3ProxyError::NoServersSynced)
    }

    async fn wait_for_throttle(self: &Arc<Self>, wait_until: Instant) -> Web3ProxyResult<()> {
        let now = Instant::now();
        let retry_at = self.next_available(now);
        if retry_at > wait_until {
            return Err(Web3ProxyError::Timeout(None));
        }

        if retry_at > now {
            sleep_until(retry_at).await;
        }

        Ok(())
    }

    pub async fn try_request_handle(
        self: &Arc<Self>,
        web3_request: &Arc<ValidatedRequest>,
        error_handler: Option<RequestErrorHandler>,
        allow_unhealthy: bool,
    ) -> Web3ProxyResult<OpenRequestResult> {
        // TODO: if websocket is reconnecting, return an error?

        if !allow_unhealthy {
            if !(self.healthy.load(atomic::Ordering::SeqCst)) {
                return Ok(OpenRequestResult::Failed);
            }

            if self.head_observation_publisher.is_some() {
                // make sure this rpc has the oldest block that this request needs
                if let Some(block_needed) = web3_request.min_block_needed() {
                    if !self.has_data_for_request(web3_request, block_needed) {
                        trace!(%web3_request, %block_needed, "{} cannot serve this request. Missing min block", self);
                        return Ok(OpenRequestResult::Failed);
                    }
                }

                // make sure this rpc has the newest block that this request needs
                if let Some(block_needed) = web3_request.max_block_needed() {
                    if !self.has_data_for_request(web3_request, block_needed) {
                        trace!(%web3_request, %block_needed, "{} cannot serve this request. Missing max block", self);

                        let rpc = self.clone();
                        let connect_timeout_at = web3_request.connect_timeout_at();

                        let mut head_block_receiver =
                            self.head_block_sender.as_ref().unwrap().subscribe();

                        // if head_block is far behind block_needed, return now
                        // TODO: future block limit from the config
                        if let Some(head_block) = head_block_receiver.borrow_and_update().as_ref() {
                            let head_block_number = head_block.number();

                            if head_block_number >= block_needed {
                                return Ok(OpenRequestResult::Failed);
                            }

                            if head_block_number + U64::from(5) < block_needed {
                                return Err(Web3ProxyError::FarFutureBlock {
                                    head: Some(head_block_number),
                                    requested: block_needed,
                                });
                            }
                        } else {
                            return Ok(OpenRequestResult::Failed);
                        }

                        // create a future that resolves once this rpc can serve this request
                        // TODO: i don't love this future. think about it more
                        let synced_f = async move {
                            loop {
                                select! {
                                    _ = head_block_receiver.changed() => {
                                        let head_block = head_block_receiver.borrow_and_update();

                                        if let Some(head_block_number) = head_block.as_ref().map(|x| x.number()) {
                                            if head_block_number >= block_needed {
                                                trace!("the block we needed has arrived!");
                                                return Ok(rpc);
                                            }
                                        } else {
                                            // TODO: what should we do? this server has no blocks at all. we can wait, but i think exiting now is best
                                            error!("no head block during try_request_handle on {}", rpc);
                                            break;
                                        }
                                    }
                                    _ = sleep_until(connect_timeout_at) => {
                                        error!("connection timeout on {}", rpc);
                                        break;
                                    }
                                }
                            }

                            if let Some(head_block_number) = head_block_receiver
                                .borrow_and_update()
                                .as_ref()
                                .map(|x| x.number())
                            {
                                Err(Web3ProxyError::FarFutureBlock {
                                    head: Some(head_block_number),
                                    requested: block_needed,
                                })
                            } else {
                                Err(Web3ProxyError::FarFutureBlock {
                                    head: None,
                                    requested: block_needed,
                                })
                            }
                        };

                        return Ok(OpenRequestResult::Lagged(Box::pin(synced_f)));
                    }
                }
            }
        }

        let now = Instant::now();
        let retry_at = self.next_available(now);
        if retry_at > now {
            return Ok(OpenRequestResult::RetryAt(retry_at));
        }

        let handle =
            OpenRequestHandle::new(web3_request.clone(), self.clone(), error_handler).await;

        Ok(handle.into())
    }

    pub async fn internal_request<P: JsonRpcParams, R: JsonRpcResultData>(
        self: &Arc<Self>,
        method: Cow<'static, str>,
        params: &P,
        error_handler: Option<RequestErrorHandler>,
        max_wait: Option<Duration>,
    ) -> Web3ProxyResult<R> {
        // TODO: should this be the app, or this RPC's head block? i think we want None so that "latest" gets left alone
        let head_block = None;

        let web3_request =
            ValidatedRequest::new_internal(method, params, head_block, max_wait).await?;

        // TODO: if we are inside the health checks and we aren't healthy yet. we need some sort of flag to force try_handle to not error

        let response = self
            .authorized_request(&web3_request, error_handler, true)
            .await;

        match &response {
            Ok(x) => {
                // TODO: this is not efficient :(
                let x = json!(x);
                web3_request.set_response(&x)
            }
            Err(e) => web3_request.set_error_response(e),
        }

        response
    }

    pub async fn authorized_request<R: JsonRpcResultData>(
        self: &Arc<Self>,
        web3_request: &Arc<ValidatedRequest>,
        error_handler: Option<RequestErrorHandler>,
        allow_unhealthy: bool,
    ) -> Web3ProxyResult<R> {
        let handle = self
            .wait_for_request_handle(web3_request, error_handler, allow_unhealthy)
            .await?;

        let response = handle.request().await?;
        let parsed = response.parsed().await?;
        match parsed.payload {
            jsonrpc::ResponsePayload::Success { result } => Ok(result),
            jsonrpc::ResponsePayload::Error { error } => Err(error.into()),
        }
    }
}

impl Hash for Web3Rpc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // do not include automatic block limit because it can change
        // do not include tier because it can change

        self.backup.hash(state);
        self.created_at.hash(state);
        self.display_name.hash(state);
        self.name.hash(state);

        self.http_url.hash(state);
        self.ws_url.hash(state);

        // TODO: don't include soft_limit if we change them to be dynamic
        self.soft_limit.hash(state);
    }
}

impl Eq for Web3Rpc {}

impl Ord for Web3Rpc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

impl PartialOrd for Web3Rpc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Web3Rpc {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Serialize for Web3Rpc {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Web3Rpc", 16)?;

        // the url is excluded because it likely includes private information. just show the name that we use in keys
        state.serialize_field("name", &self.name)?;
        // a longer name for display to users
        state.serialize_field("display_name", &self.display_name)?;

        state.serialize_field("backup", &self.backup)?;

        state.serialize_field("web3_clientVersion", &self.client_version.read().as_ref())?;

        match self.block_data_limit.load(atomic::Ordering::SeqCst) {
            u64::MAX => {
                state.serialize_field("block_data_limit", &None::<()>)?;
            }
            block_data_limit => {
                state.serialize_field("block_data_limit", &block_data_limit)?;
            }
        }

        match self.log_data_limit.load(atomic::Ordering::SeqCst) {
            u64::MAX => {
                state.serialize_field("log_data_limit", &None::<()>)?;
            }
            log_data_limit => {
                state.serialize_field("log_data_limit", &log_data_limit)?;
            }
        }

        state.serialize_field("tier", &self.tier)?;

        state.serialize_field("soft_limit", &self.soft_limit)?;

        // TODO: maybe this is too much data. serialize less?
        {
            let head_block = self.head_block_sender.as_ref().unwrap();
            let head_block = head_block.borrow();
            let head_block = head_block.as_ref();
            state.serialize_field("head_block", &head_block)?;
        }

        state.serialize_field(
            "total_requests",
            &self.total_requests.load(atomic::Ordering::Relaxed),
        )?;

        state.serialize_field(
            "active_requests",
            &self.active_requests.load(atomic::Ordering::SeqCst),
        )?;

        {
            let head_delay_ms = self.head_delay.read().latency().as_secs_f32() * 1000.0;
            state.serialize_field("head_delay_ms", &(head_delay_ms))?;
        }

        {
            let median_latency_ms = self
                .median_latency
                .as_ref()
                .unwrap()
                .latency()
                .as_secs_f32()
                * 1000.0;
            state.serialize_field("median_latency_ms", &(median_latency_ms))?;
        }

        {
            let peak_latency_ms =
                self.peak_latency.as_ref().unwrap().latency().as_secs_f32() * 1000.0;
            state.serialize_field("peak_latency_ms", &peak_latency_ms)?;
        }
        {
            let weighted_latency_ms = self.weighted_peak_latency().as_secs_f32() * 1000.0;
            state.serialize_field("weighted_latency_ms", &weighted_latency_ms)?;
        }
        {
            let healthy = self.healthy.load(atomic::Ordering::SeqCst);
            state.serialize_field("healthy", &healthy)?;
        }

        state.end()
    }
}

impl fmt::Debug for Web3Rpc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f.debug_struct("Web3Rpc");

        f.field("name", &self.name);

        let block_data_limit = self.block_data_limit.load(atomic::Ordering::SeqCst);
        if block_data_limit == u64::MAX {
            f.field("blocks", &"all");
        } else {
            f.field("blocks", &block_data_limit);
        }

        let log_data_limit = self.log_data_limit.load(atomic::Ordering::SeqCst);
        if log_data_limit == u64::MAX {
            f.field("logs", &"all");
        } else {
            f.field("logs", &log_data_limit);
        }

        f.field("backup", &self.backup);

        f.field("tier", &self.tier.load(atomic::Ordering::SeqCst));

        f.field("weighted_ms", &self.weighted_peak_latency().as_millis());

        if let Some(head_block_watch) = self.head_block_sender.as_ref() {
            if let Some(head_block) = head_block_watch.borrow().as_ref() {
                f.field("head_num", &head_block.number());
                f.field("head_hash", head_block.hash());
            } else {
                f.field("head_num", &None::<()>);
                f.field("head_hash", &None::<()>);
            }
        }

        f.finish_non_exhaustive()
    }
}

impl fmt::Display for Web3Rpc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    use super::*;
    use crate::rpcs::many::{Web3Rpcs, Web3RpcsSpawnConfig};
    use alloy::primitives::{B256, U256};
    use alloy::rpc::types::{Block, Header};
    use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
    use axum::extract::State;
    use axum::response::IntoResponse;
    use axum::{routing::get, Router};
    use futures::SinkExt;
    use std::sync::Mutex;
    use tokio::net::TcpListener;
    use tokio::sync::{mpsc, Notify, Semaphore};

    #[derive(Clone)]
    struct WebSocketStubState {
        full_block: Arc<Mutex<serde_json::Value>>,
        header: Header,
        hydration_release: Arc<Semaphore>,
        hydration_sent: Arc<Notify>,
        hydration_started: Arc<Notify>,
        latest_block: serde_json::Value,
        requests: Arc<Mutex<Vec<serde_json::Value>>>,
    }

    async fn websocket_stub_upgrade(
        State(state): State<WebSocketStubState>,
        upgrade: WebSocketUpgrade,
    ) -> impl IntoResponse {
        upgrade.on_upgrade(move |socket| websocket_stub(socket, state))
    }

    async fn websocket_stub(mut socket: WebSocket, state: WebSocketStubState) {
        while let Some(Ok(WsMessage::Text(request))) = socket.next().await {
            let request: serde_json::Value = serde_json::from_str(request.as_str()).unwrap();
            let method = request["method"].as_str().unwrap();

            state.requests.lock().unwrap().push(request.clone());

            let response = match method {
                "eth_subscribe" => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": "0x1",
                }),
                "eth_getBlockByNumber" => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": &state.latest_block,
                }),
                "eth_getBlockByHash" => {
                    state.hydration_started.notify_one();
                    let permit = state.hydration_release.acquire().await.unwrap();
                    permit.forget();
                    let full_block = state.full_block.lock().unwrap().clone();
                    if full_block == "rpc-error" {
                        serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": request["id"],
                            "error": {"code": -32000, "message": "hydration failed"},
                        })
                    } else {
                        serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": request["id"],
                            "result": full_block,
                        })
                    }
                }
                _ => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "error": {"code": -32601, "message": "method not found"},
                }),
            };

            socket
                .send(WsMessage::Text(response.to_string().into()))
                .await
                .unwrap();

            if method == "eth_getBlockByHash" {
                state.hydration_sent.notify_one();
            }

            if method == "eth_getBlockByNumber" {
                let notification = serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": "eth_subscription",
                    "params": {
                        "subscription": "0x1",
                        "result": &state.header,
                    },
                });
                for _ in 0..2 {
                    socket
                        .send(WsMessage::Text(notification.to_string().into()))
                        .await
                        .unwrap();
                }
            }
        }
    }

    fn header(number: u64, timestamp: u64) -> Header {
        let mut header: Header = Header {
            hash: B256::with_last_byte(number as u8),
            ..Default::default()
        };
        header.inner.number = number;
        header.inner.timestamp = timestamp;
        header
    }

    struct HydrationStub {
        rpc: Arc<Web3Rpc>,
        full_block: Arc<Mutex<serde_json::Value>>,
        hydration_release: Arc<Semaphore>,
        hydration_sent: Arc<Notify>,
        hydration_started: Arc<Notify>,
        requests: Arc<Mutex<Vec<serde_json::Value>>>,
        server: tokio::task::JoinHandle<()>,
    }

    impl HydrationStub {
        fn hydration_request_count(&self) -> usize {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .filter(|request| request["method"] == "eth_getBlockByHash")
                .count()
        }
    }

    impl Drop for HydrationStub {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    async fn hydration_stub(name: &str, full_block: serde_json::Value) -> HydrationStub {
        let full_block = Arc::new(Mutex::new(full_block));
        let hydration_release = Arc::new(Semaphore::new(0));
        let hydration_sent = Arc::new(Notify::new());
        let hydration_started = Arc::new(Notify::new());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = WebSocketStubState {
            full_block: full_block.clone(),
            header: Header::default(),
            hydration_release: hydration_release.clone(),
            hydration_sent: hydration_sent.clone(),
            hydration_started: hydration_started.clone(),
            latest_block: serde_json::Value::Null,
            requests: requests.clone(),
        };
        let router = Router::new()
            .route("/", get(websocket_stub_upgrade))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let provider = connect_ws(format!("ws://{address}").parse().unwrap())
            .await
            .unwrap();
        let (hard_limit_until, _) = watch::channel(Instant::now());
        let (head_block_sender, _) = watch::channel(None);
        let (disconnect_watch, _) = watch::channel(false);
        let rpc = Arc::new(Web3Rpc {
            name: name.into(),
            created_at: Some(Instant::now()),
            hard_limit_until: Some(hard_limit_until),
            head_block_sender: Some(head_block_sender),
            peak_latency: Some(PeakEwmaLatency::spawn(
                Duration::from_secs(1),
                4,
                Duration::from_secs(1),
            )),
            median_latency: Some(RollingQuantileLatency::spawn_median(4).await),
            disconnect_watch: Some(disconnect_watch),
            ..Default::default()
        });
        rpc.ws_provider.store(Some(Arc::new(provider)));

        HydrationStub {
            rpc,
            full_block,
            hydration_release,
            hydration_sent,
            hydration_started,
            requests,
            server,
        }
    }

    fn full_block(block_hash: B256, source: &str) -> serde_json::Value {
        serde_json::json!({
            "hash": block_hash,
            "number": "0x2a",
            "transactions": [{
                "hash": B256::with_last_byte(0x11),
                "source": source,
            }],
            "uncles": [],
            "source": source,
        })
    }

    async fn wait_for_cached_block(
        cache: &BlockResponseCache,
        block_hash: B256,
        full_transactions: bool,
    ) -> serde_json::Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(block) = cache
                    .get(&BlockResponseCacheKey::new(block_hash, full_transactions))
                    .await
                {
                    break serde_json::from_str(&sonic_rs::to_string(&block.result()).unwrap())
                        .unwrap();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the hydration race must cache a block")
    }

    #[test_log::test(tokio::test)]
    async fn block_header_hash_miss_fetches_and_populates_cache() {
        let requested_header = header(42, 1_700_000_000);
        let block_hash = requested_header.hash;
        let block: Block = Block::empty(requested_header);
        let stub = hydration_stub(
            "block-header-cache-through",
            serde_json::to_value(block).unwrap(),
        )
        .await;
        stub.rpc.healthy.store(true, atomic::Ordering::SeqCst);

        let (rpcs, _handle, _) = Web3Rpcs::spawn(
            Web3RpcsSpawnConfig::new(1, None, 0, 0, 1_000_000),
            "block-header-cache-through".into(),
            None,
            None,
        )
        .await
        .unwrap();
        rpcs.by_name
            .write()
            .insert(stub.rpc.name.clone(), stub.rpc.clone());

        assert_eq!(rpcs.blocks_by_hash.get(&block_hash).await, None);

        stub.hydration_release.add_permits(1);
        let fetched = rpcs.block_header_by_hash(block_hash).await.unwrap();

        assert_eq!(*fetched.hash(), block_hash);
        assert_eq!(fetched.number(), U64::from(42));
        let cached = rpcs.blocks_by_hash.get(&block_hash).await.unwrap();
        assert_eq!(*cached.hash(), block_hash);
        assert_eq!(cached.number(), U64::from(42));

        let fetched_again = tokio::time::timeout(
            Duration::from_secs(1),
            rpcs.block_header_by_hash(block_hash),
        )
        .await
        .expect("the cached lookup must not wait for another RPC response")
        .unwrap();
        assert_eq!(*fetched_again.hash(), block_hash);

        let requests = stub.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["method"], "eth_getBlockByHash");
        assert_eq!(
            requests[0]["params"],
            serde_json::json!([block_hash, false])
        );
    }

    #[test_log::test(tokio::test)]
    async fn faster_announcer_wins_shared_hydration_race() {
        let block_hash = B256::with_last_byte(0x42);
        let cache = moka::future::Cache::new(16);
        let coordinator = BlockHydrationCoordinator::new(cache.clone());
        let slow = hydration_stub("slow", full_block(block_hash, "slow")).await;
        let fast = hydration_stub("fast", full_block(block_hash, "fast")).await;

        coordinator.announce(slow.rpc.clone(), block_hash).await;
        slow.hydration_started.notified().await;
        coordinator.announce(slow.rpc.clone(), block_hash).await;
        tokio::task::yield_now().await;
        assert_eq!(slow.hydration_request_count(), 1);
        assert_eq!(slow.rpc.total_requests.load(atomic::Ordering::Relaxed), 1);
        assert_eq!(fast.hydration_request_count(), 0);
        assert_eq!(fast.rpc.total_requests.load(atomic::Ordering::Relaxed), 0);

        coordinator.announce(fast.rpc.clone(), block_hash).await;
        fast.hydration_started.notified().await;
        fast.hydration_release.add_permits(1);

        let cached_full = wait_for_cached_block(&cache, block_hash, true).await;
        let cached_hashes = wait_for_cached_block(&cache, block_hash, false).await;
        assert_eq!(cached_full["source"], "fast");
        assert_eq!(cached_hashes["source"], "fast");
        assert_eq!(fast.hydration_request_count(), 1);
        assert_eq!(fast.rpc.total_requests.load(atomic::Ordering::Relaxed), 1);

        slow.hydration_release.add_permits(1);
        slow.hydration_sent.notified().await;
        tokio::task::yield_now().await;
        let cached_after_loser = wait_for_cached_block(&cache, block_hash, true).await;
        assert_eq!(cached_after_loser["source"], "fast");
    }

    #[test_log::test(tokio::test)]
    async fn invalid_hydration_results_do_not_cancel_a_valid_announcer() {
        let block_hash = B256::with_last_byte(0x42);
        let cache = moka::future::Cache::new(16);
        let coordinator = BlockHydrationCoordinator::new(cache.clone());
        let valid = hydration_stub("valid", full_block(block_hash, "valid")).await;
        let invalid_results = [
            serde_json::Value::Null,
            serde_json::json!("rpc-error"),
            serde_json::json!({"hash": block_hash, "number": "0x2a"}),
            full_block(B256::with_last_byte(0x99), "wrong-hash"),
        ];
        let mut invalid = Vec::new();
        for (index, result) in invalid_results.into_iter().enumerate() {
            invalid.push(hydration_stub(&format!("invalid-{index}"), result).await);
        }

        coordinator.announce(valid.rpc.clone(), block_hash).await;
        valid.hydration_started.notified().await;
        for stub in &invalid {
            coordinator.announce(stub.rpc.clone(), block_hash).await;
            stub.hydration_started.notified().await;
            stub.hydration_release.add_permits(1);
            stub.hydration_sent.notified().await;
        }
        tokio::task::yield_now().await;
        assert!(cache
            .get(&BlockResponseCacheKey::new(block_hash, true))
            .await
            .is_none());

        valid.hydration_release.add_permits(1);
        let cached = wait_for_cached_block(&cache, block_hash, true).await;
        assert_eq!(cached["source"], "valid");
        for stub in &invalid {
            assert_eq!(stub.hydration_request_count(), 1);
            assert_eq!(stub.rpc.total_requests.load(atomic::Ordering::Relaxed), 1);
        }
    }

    #[test_log::test(tokio::test)]
    async fn failed_announcer_can_retry_after_another_announcement() {
        let block_hash = B256::with_last_byte(0x42);
        let cache = moka::future::Cache::new(16);
        let coordinator = BlockHydrationCoordinator::new(cache.clone());
        let retrying = hydration_stub("retrying", serde_json::Value::Null).await;

        coordinator.announce(retrying.rpc.clone(), block_hash).await;
        retrying.hydration_started.notified().await;
        retrying.hydration_release.add_permits(1);
        retrying.hydration_sent.notified().await;
        *retrying.full_block.lock().unwrap() = full_block(block_hash, "retry");

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                coordinator.announce(retrying.rpc.clone(), block_hash).await;
                if retrying.hydration_request_count() == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a failed announcer must be allowed to retry");
        retrying.hydration_release.add_permits(1);

        let cached = wait_for_cached_block(&cache, block_hash, true).await;
        assert_eq!(cached["source"], "retry");
        assert_eq!(retrying.hydration_request_count(), 2);
        assert_eq!(
            retrying.rpc.total_requests.load(atomic::Ordering::Relaxed),
            2
        );
    }

    #[test]
    fn block_data_limit_is_part_of_health_status() {
        let rpc = Web3Rpc {
            block_data_limit: 0.into(),
            ..Default::default()
        };

        assert!(!rpc.health_status(true));

        rpc.block_data_limit.store(32, atomic::Ordering::SeqCst);

        assert!(rpc.health_status(true));
        assert!(!rpc.health_status(false));
    }

    #[test_log::test(tokio::test)]
    async fn websocket_new_head_starts_one_background_block_hydration() {
        let expected_hash = B256::with_last_byte(0x42);
        let expected_parent_hash = B256::with_last_byte(0x41);
        let expected_number = 42;
        let expected_timestamp = 1_234;

        let mut expected_header = header(expected_number, expected_timestamp);
        expected_header.hash = expected_hash;
        expected_header.inner.parent_hash = expected_parent_hash;

        let transaction_hashes = [B256::with_last_byte(0x11), B256::with_last_byte(0x12)];
        let uncle_hash = B256::with_last_byte(0x33);
        let full_block = serde_json::json!({
            "hash": expected_hash,
            "number": "0x2a",
            "transactions": [
                {"hash": transaction_hashes[0], "providerTxField": "first"},
                {"hash": transaction_hashes[1], "providerTxField": "second"},
            ],
            "uncles": [uncle_hash],
            "withdrawals": [{"index": "0x1", "amount": "0x2"}],
            "providerBlockField": {"preserved": true},
        });

        let requests = Arc::new(Mutex::new(Vec::new()));
        let hydration_release = Arc::new(Semaphore::new(0));
        let hydration_started = Arc::new(Notify::new());
        let stub_full_block = Arc::new(Mutex::new(full_block.clone()));
        let state = WebSocketStubState {
            full_block: stub_full_block.clone(),
            header: expected_header,
            hydration_release: hydration_release.clone(),
            hydration_sent: Arc::new(Notify::new()),
            hydration_started: hydration_started.clone(),
            latest_block: serde_json::Value::Null,
            requests: requests.clone(),
        };
        let router = Router::new()
            .route("/", get(websocket_stub_upgrade))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        let provider = connect_ws(format!("ws://{address}").parse().unwrap())
            .await
            .unwrap();
        let (hard_limit_until, _) = watch::channel(Instant::now());
        let (head_block_sender, _) = watch::channel(None);
        let (disconnect_watch, _) = watch::channel(false);
        let (head_observation_sender, mut head_observation_receiver) = mpsc::unbounded_channel();
        let head_observation_publisher = HeadObservationPublisher::new(head_observation_sender);
        let block_response_cache = moka::future::Cache::new(16);
        let block_hydration = BlockHydrationCoordinator::new(block_response_cache.clone());
        let rpc = Arc::new(Web3Rpc {
            name: "websocket-stub".into(),
            block_map: Some(moka::future::Cache::new(16)),
            block_number_map: Some(moka::future::Cache::new(16)),
            block_response_cache: Some(block_response_cache.clone()),
            created_at: Some(Instant::now()),
            hard_limit_until: Some(hard_limit_until),
            head_observation_publisher: Some(head_observation_publisher),
            block_hydration: Some(block_hydration),
            head_block_sender: Some(head_block_sender),
            peak_latency: Some(PeakEwmaLatency::spawn(
                Duration::from_secs(1),
                4,
                Duration::from_secs(1),
            )),
            median_latency: Some(RollingQuantileLatency::spawn_median(4).await),
            disconnect_watch: Some(disconnect_watch),
            ..Default::default()
        });
        rpc.ws_provider.store(Some(Arc::new(provider)));

        let rpc_for_subscription = rpc.clone();
        let subscription =
            tokio::spawn(async move { rpc_for_subscription.subscribe_new_heads().await });

        let observation =
            tokio::time::timeout(Duration::from_secs(5), head_observation_receiver.recv())
                .await
                .expect("new head must arrive before the test timeout")
                .expect("new head channel must stay open");
        let new_head = observation
            .block
            .expect("the pushed head must not be empty");

        assert!(Arc::ptr_eq(&observation.rpc, &rpc));
        assert_eq!(*new_head.hash(), expected_hash);
        assert_eq!(*new_head.parent_hash(), expected_parent_hash);
        assert_eq!(new_head.number(), U64::from(expected_number));
        assert_eq!(new_head.timestamp(), expected_timestamp);

        tokio::time::timeout(Duration::from_secs(5), hydration_started.notified())
            .await
            .expect("hydration must start after the head is sent");
        assert!(block_response_cache
            .get(&BlockResponseCacheKey::new(expected_hash, true))
            .await
            .is_none());

        let duplicate_observation =
            tokio::time::timeout(Duration::from_secs(5), head_observation_receiver.recv())
                .await
                .expect("duplicate pushed head must arrive before hydration finishes")
                .expect("new head channel must stay open");
        assert_eq!(duplicate_observation.block.unwrap().hash(), &expected_hash);

        hydration_release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let has_full = block_response_cache
                    .get(&BlockResponseCacheKey::new(expected_hash, true))
                    .await
                    .is_some();
                let has_hashes = block_response_cache
                    .get(&BlockResponseCacheKey::new(expected_hash, false))
                    .await
                    .is_some();
                if has_full && has_hashes {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("deduplicated hydration must finish");

        let cached_full = block_response_cache
            .get(&BlockResponseCacheKey::new(expected_hash, true))
            .await
            .expect("full block response must be cached");
        let cached_hashes = block_response_cache
            .get(&BlockResponseCacheKey::new(expected_hash, false))
            .await
            .expect("hash-only block response must be cached");
        let cached_full: serde_json::Value =
            serde_json::from_str(&sonic_rs::to_string(&cached_full.result()).unwrap()).unwrap();
        let cached_hashes: serde_json::Value =
            serde_json::from_str(&sonic_rs::to_string(&cached_hashes.result()).unwrap()).unwrap();

        assert_eq!(cached_full, full_block);
        assert_eq!(
            cached_hashes["transactions"],
            serde_json::json!(transaction_hashes)
        );
        assert_eq!(cached_hashes["uncles"], full_block["uncles"]);
        assert_eq!(cached_hashes["withdrawals"], full_block["withdrawals"]);
        assert_eq!(
            cached_hashes["providerBlockField"],
            full_block["providerBlockField"]
        );

        let initial_methods = {
            let requests = requests.lock().unwrap();
            assert_eq!(requests[0]["params"], serde_json::json!(["newHeads"]));
            assert_eq!(requests[1]["params"], serde_json::json!(["latest", false]));
            requests
                .iter()
                .map(|request| request["method"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            initial_methods,
            [
                "eth_subscribe",
                "eth_getBlockByNumber",
                "eth_getBlockByHash"
            ]
        );

        rpc.healthy.store(true, atomic::Ordering::SeqCst);
        for bad_result in [
            serde_json::Value::Null,
            serde_json::json!({"hash": expected_hash, "number": "0x2a"}),
            serde_json::json!("rpc-error"),
        ] {
            block_response_cache
                .invalidate(&BlockResponseCacheKey::new(expected_hash, true))
                .await;
            block_response_cache
                .invalidate(&BlockResponseCacheKey::new(expected_hash, false))
                .await;
            *stub_full_block.lock().unwrap() = bad_result;
            hydration_release.add_permits(1);

            assert!(rpc.fetch_full_block(expected_hash).await.is_err());
            assert!(block_response_cache
                .get(&BlockResponseCacheKey::new(expected_hash, true))
                .await
                .is_none());
            assert!(block_response_cache
                .get(&BlockResponseCacheKey::new(expected_hash, false))
                .await
                .is_none());
            assert!(rpc.healthy.load(atomic::Ordering::SeqCst));
        }

        let hydration_requests = requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request["method"] == "eth_getBlockByHash")
            .count();
        assert_eq!(hydration_requests, 4);

        subscription.abort();
        server.abort();
        let _ = subscription.await;
        let _ = server.await;
    }

    #[test_log::test(tokio::test)]
    async fn latest_hash_only_block_seeds_the_response_cache() {
        let expected_hash = B256::with_last_byte(0x42);
        let mut expected_header = header(42, 1_234);
        expected_header.hash = expected_hash;
        expected_header.inner.parent_hash = B256::with_last_byte(0x41);
        let mut latest_block = serde_json::to_value(&expected_header).unwrap();
        let latest_block_object = latest_block.as_object_mut().unwrap();
        latest_block_object.insert("transactions".into(), serde_json::json!([]));
        latest_block_object.insert("uncles".into(), serde_json::json!([]));
        latest_block_object.insert("withdrawals".into(), serde_json::json!([]));
        latest_block_object.insert("providerBlockField".into(), serde_json::json!("kept"));

        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = WebSocketStubState {
            full_block: Arc::new(Mutex::new(serde_json::Value::Null)),
            header: expected_header,
            hydration_release: Arc::new(Semaphore::new(0)),
            hydration_sent: Arc::new(Notify::new()),
            hydration_started: Arc::new(Notify::new()),
            latest_block: latest_block.clone(),
            requests: requests.clone(),
        };
        let router = Router::new()
            .route("/", get(websocket_stub_upgrade))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        let provider = connect_ws(format!("ws://{address}").parse().unwrap())
            .await
            .unwrap();
        let (hard_limit_until, _) = watch::channel(Instant::now());
        let (disconnect_watch, _) = watch::channel(false);
        let block_response_cache = moka::future::Cache::new(16);
        let rpc = Arc::new(Web3Rpc {
            name: "latest-block-stub".into(),
            block_map: Some(moka::future::Cache::new(16)),
            block_number_map: Some(moka::future::Cache::new(16)),
            block_response_cache: Some(block_response_cache.clone()),
            created_at: Some(Instant::now()),
            hard_limit_until: Some(hard_limit_until),
            peak_latency: Some(PeakEwmaLatency::spawn(
                Duration::from_secs(1),
                4,
                Duration::from_secs(1),
            )),
            median_latency: Some(RollingQuantileLatency::spawn_median(4).await),
            disconnect_watch: Some(disconnect_watch),
            ..Default::default()
        });
        rpc.ws_provider.store(Some(Arc::new(provider)));

        let latest_header = rpc.latest_block_header(None).await.unwrap().unwrap();

        assert_eq!(latest_header.hash, expected_hash);
        let cached = block_response_cache
            .get(&BlockResponseCacheKey::new(expected_hash, false))
            .await
            .expect("latest hash-only block must be cached");
        let cached: serde_json::Value =
            serde_json::from_str(&sonic_rs::to_string(&cached.result()).unwrap()).unwrap();
        assert_eq!(cached, latest_block);
        assert!(block_response_cache
            .get(&BlockResponseCacheKey::new(expected_hash, true))
            .await
            .is_none());
        {
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0]["method"], "eth_getBlockByNumber");
            assert_eq!(requests[0]["params"], serde_json::json!(["latest", false]));
        }

        server.abort();
        let _ = server.await;
    }

    #[test]
    fn test_archive_node_has_block_data() {
        let now = u64::try_from(jiff::Timestamp::now().as_second()).unwrap();

        let random_block = header(1_000_000, now);

        let random_block = Arc::new(random_block);

        let head_block = BlockHeader::new(random_block);
        let block_data_limit = u64::MAX;

        let (tx, _) = watch::channel(Some(head_block.clone()));

        let x = Web3Rpc {
            name: "name".to_string(),
            soft_limit: 1_000,
            automatic_block_limit: false,
            backup: false,
            block_data_limit: block_data_limit.into(),
            head_block_sender: Some(tx),
            ..Default::default()
        };

        assert!(x.has_block_data(U64::ZERO));
        assert!(x.has_block_data(U64::from(1)));
        assert!(x.has_block_data(head_block.number()));
        assert!(!x.has_block_data(head_block.number() + U64::from(1)));
        assert!(!x.has_block_data(head_block.number() + U64::from(1000)));
    }

    #[test]
    fn test_pruned_node_has_block_data() {
        let now = u64::try_from(jiff::Timestamp::now().as_second()).unwrap();

        let head_block = BlockHeader::new(Arc::new(header(1_000_000, now)));

        let block_data_limit = 64;

        let (tx, _rx) = watch::channel(Some(head_block.clone()));

        let x = Web3Rpc {
            name: "name".to_string(),
            soft_limit: 1_000,
            automatic_block_limit: false,
            backup: false,
            block_data_limit: block_data_limit.into(),
            head_block_sender: Some(tx),
            ..Default::default()
        };

        assert!(!x.has_block_data(U64::ZERO));
        assert!(!x.has_block_data(U64::from(1)));
        assert!(
            !x.has_block_data(head_block.number() - U64::from(block_data_limit) - U64::from(1),)
        );
        assert!(x.has_block_data(head_block.number() - U64::from(block_data_limit),));
        assert!(x.has_block_data(head_block.number()));
        assert!(!x.has_block_data(head_block.number() + U64::from(1)));
        assert!(!x.has_block_data(head_block.number() + U64::from(1000)));
    }

    #[test_log::test(tokio::test)]
    async fn pruned_node_does_not_wait_for_an_old_max_block() {
        let now = u64::try_from(jiff::Timestamp::now().as_second()).unwrap();
        let head_block = BlockHeader::new(Arc::new(header(1_000, now)));
        let (head_block_sender, _) = watch::channel(Some(head_block.clone()));
        let (head_observation_sender, head_observation_receiver) = mpsc::unbounded_channel();
        let head_observation_publisher = HeadObservationPublisher::new(head_observation_sender);
        drop(head_observation_receiver);
        let rpc = Arc::new(Web3Rpc {
            name: "pruned".to_owned(),
            healthy: AtomicBool::new(true),
            block_data_limit: 128.into(),
            head_observation_publisher: Some(head_observation_publisher),
            head_block_sender: Some(head_block_sender),
            ..Default::default()
        });
        let request = ValidatedRequest::new_internal(
            "eth_getBlockByNumber".into(),
            &("0x320", false),
            Some(head_block),
            Some(Duration::from_secs(1)),
        )
        .await
        .unwrap();

        assert_eq!(request.max_block_needed(), Some(U64::from(800)));
        assert!(matches!(
            rpc.try_request_handle(&request, None, false).await.unwrap(),
            OpenRequestResult::Failed
        ));
    }

    #[test_log::test(tokio::test)]
    async fn temporarily_unhealthy_node_requests_a_retry() {
        let rpc = Arc::new(Web3Rpc {
            name: "temporarily-unhealthy".to_owned(),
            ..Default::default()
        });
        let request = ValidatedRequest::new_internal(
            "eth_blockNumber".into(),
            &[(); 0],
            None,
            Some(Duration::from_secs(2)),
        )
        .await
        .unwrap();

        assert!(matches!(
            rpc.try_request_handle(&request, None, false).await.unwrap(),
            OpenRequestResult::RetryAt(_)
        ));
    }

    /*
    // TODO: think about how to bring the concept of a "lagged" node back
    #[test]
    fn test_lagged_node_not_has_block_data() {
        let now = jiff::Timestamp::now().as_second().into();

        // head block is an hour old
        let head_block = Block {
            hash: Some(H256::random()),
            number: Some(1_000_000.into()),
            timestamp: now - 3600,
            ..Default::default()
        };

        let head_block = Arc::new(head_block);

        let head_block = Web3ProxyBlock::new(head_block);
        let block_data_limit = u64::MAX;

        let metrics = OpenRequestHandleMetrics::default();

        let x = Web3Rpc {
            name: "name".to_string(),
            display_name: None,
            url: "ws://example.com".to_string(),
            http_client: None,
            active_requests: 0.into(),
            frontend_requests: 0.into(),
            internal_requests: 0.into(),
            provider_state: AsyncRwLock::new(ProviderState::None),
            hard_limit: None,
            soft_limit: 1_000,
            automatic_block_limit: false,
            backup: false,
            block_data_limit: block_data_limit.into(),
            tier: 0,
            head_block: AsyncRwLock::new(Some(head_block.clone())),
        };

        assert!(!x.has_block_data(0.into()));
        assert!(!x.has_block_data(1.into()));
        assert!(!x.has_block_data(head_block.number());
        assert!(!x.has_block_data(head_block.number() + 1));
        assert!(!x.has_block_data(head_block.number() + 1000));
    }
    */
}
