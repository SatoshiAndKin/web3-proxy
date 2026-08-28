use super::blockchain::{BlockHeader, HeadObservation};
use super::many::Web3Rpcs;
use super::one::Web3Rpc;
use super::request::OpenRequestHandle;
use crate::errors::{Web3ProxyError, Web3ProxyErrorContext, Web3ProxyResult};
use crate::jsonrpc::ValidatedRequest;
use crate::rpcs::request::OpenRequestResult;
use alloy::primitives::{B256, U64};
use async_stream::stream;
use base64::engine::general_purpose;
use futures::future::select_all;
use futures::Stream;
use hashbrown::{HashMap, HashSet};
use hdrhistogram::serialization::{Serializer, V2DeflateSerializer};
use hdrhistogram::Histogram;
use itertools::{Itertools, MinMaxResult};
use moka::future::Cache;
use serde::Serialize;
use std::cmp::{Ordering, Reverse};
use std::sync::{atomic, Arc};
use std::time::Duration;
use tokio::select;
use tokio::time::{sleep_until, Instant};
use tracing::{debug, enabled, error, info, trace, warn, Level};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct RpcRanking {
    backup: bool,
    /// note: the servers in this tier might have blocks higher than this
    consensus_head_num: Option<U64>,
    tier: u32,
}

impl RpcRanking {
    pub fn default_with_backup(backup: bool) -> Self {
        Self {
            backup,
            ..Default::default()
        }
    }

    fn sort_key(&self) -> (bool, Reverse<Option<U64>>, u32) {
        // TODO: add sum_soft_limit here? add peak_ewma here?
        // TODO: should backup or tier be checked first? now that tiers are automated, backups should be more reliable, but still leave them last
        // while tier might give us better latency, giving requests to a server that is behind by a block will get in the way of it syncing. better to only query synced servers
        // TODO: should we include a random number in here?
        (!self.backup, Reverse(self.consensus_head_num), self.tier)
    }
}

impl Ord for RpcRanking {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sort_key().cmp(&other.sort_key())
    }
}

impl PartialOrd for RpcRanking {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// TODO: i think we can get rid of this in favor of
pub enum ShouldWaitForBlock {
    Ready,
    // BackupReady,
    /// how many blocks you will have to wait
    Wait(U64),
    // WaitForBackup { current: Option<U64> },
    NeverReady,
}

#[derive(Clone, Debug, Serialize)]
enum SortMethod {
    /// shuffle the servers randomly instead of by latency
    Shuffle,
    /// sort the servers by latency (among other things)
    Sort,
}

/// A collection of Web3Rpcs that are on the same block.
/// Serialize is so we can print it on our /status endpoint
/// TODO: remove head_block/head_rpcs/tier and replace with one RankedRpcMap
/// TODO: add `best_rpc(method_data_kind, min_block_needed, max_block_needed, include_backups)`
/// TODO: make serializing work. the key needs to be a string. I think we need `serialize_with`
#[derive(Clone, Debug, Serialize)]
pub struct RankedRpcs {
    pub head_block: Option<BlockHeader>,
    pub num_synced: usize,
    pub backups_needed: bool,
    pub check_block_data: bool,

    pub(crate) inner: HashSet<Arc<Web3Rpc>>,

    sort_mode: SortMethod,
}

// TODO: could these be refs? The owning RankedRpcs lifetime might work. `stream!` might make it complicated
#[derive(Debug)]
pub struct RpcsForRequest {
    inner: Vec<Arc<Web3Rpc>>,
    outer: Vec<Arc<Web3Rpc>>,
    request: Arc<ValidatedRequest>,
}

impl RankedRpcs {
    pub fn from_rpcs(
        rpcs: Vec<Arc<Web3Rpc>>,
        head_block: Option<BlockHeader>,
        check_block_data: bool,
    ) -> Self {
        // we don't need to sort the rpcs now. we will sort them when a request neds them
        // TODO: the shame about this is that we lose just being able to compare 2 random servers

        let rpcs: HashSet<_> = rpcs.into_iter().collect();

        let backups_needed = rpcs.iter().any(|x| x.backup);

        let num_synced = rpcs.len();

        let sort_mode = SortMethod::Shuffle;

        Self {
            backups_needed,
            check_block_data,
            head_block,
            inner: rpcs,
            num_synced,
            sort_mode,
        }
    }

    pub fn from_votes(
        min_synced_rpcs: usize,
        min_sum_soft_limit: u32,
        max_lag_block: U64,
        votes: HashMap<BlockHeader, (HashSet<&Arc<Web3Rpc>>, u32)>,
        heads: HashMap<Arc<Web3Rpc>, BlockHeader>,
    ) -> Option<Self> {
        // find the blocks that meets our min_sum_soft_limit and min_synced_rpcs
        let mut votes: Vec<_> = votes
            .into_iter()
            .filter_map(|(block, (rpcs, sum_soft_limit))| {
                if block.number() < max_lag_block
                    || sum_soft_limit < min_sum_soft_limit
                    || rpcs.len() < min_synced_rpcs
                {
                    None
                } else {
                    Some((block, sum_soft_limit, rpcs))
                }
            })
            .collect();

        // sort the votes
        votes.sort_by_key(|(block, sum_soft_limit, _)| {
            (
                Reverse(block.number()),
                // TODO: block total difficulty (if we have it)
                Reverse(*sum_soft_limit),
                // TODO: median/peak latency here?
            )
        });

        // return the first result that exceededs confgured minimums (if any)
        if let Some((best_block, _, best_rpcs)) = votes.into_iter().next() {
            let mut best_rpcs: HashSet<_> = best_rpcs.into_iter().map(Arc::clone).collect();

            let backups_needed = best_rpcs.iter().any(|x| x.backup);
            let num_synced = best_rpcs.len();

            // add all the rpcs that are behind the ranked rpcs. these might be needed for serving archive requests
            for (x, x_head) in heads.iter() {
                // TODO: do we care about this "contains" when a set won't add more than once anyways?
                if best_rpcs.contains(x) {
                    continue;
                }

                // we only want to include backups if they voted for the head block
                // TODO: think more about this. maybe have a config option about how easily to use backup rpcs
                if x.backup && !backups_needed {
                    continue;
                }

                if x_head.number() < max_lag_block {
                    // server is too far behind
                    continue;
                }

                // TODO: max age here too?

                best_rpcs.insert(x.clone());
            }

            // consensus found!
            trace!(?best_rpcs);

            let sort_mode = SortMethod::Sort;

            let consensus = RankedRpcs {
                backups_needed,
                check_block_data: true,
                head_block: Some(best_block),
                sort_mode,
                inner: best_rpcs,
                num_synced,
            };

            return Some(consensus);
        }

        None
    }

    pub fn for_request(&self, web3_request: &Arc<ValidatedRequest>) -> Option<RpcsForRequest> {
        if self.num_active_rpcs() == 0 {
            return None;
        }

        let head_block_num = self.head_block.as_ref().map(|x| x.number());

        let num_active = self.num_active_rpcs();

        // these are bigger than we need, but how much does that matter?
        let mut inner_for_request = Vec::with_capacity(num_active);
        let mut outer_for_request = Vec::with_capacity(num_active);

        // TODO: what if min is set to some future block?
        // TODO: what if max is set to some future block?
        let min_block_needed = web3_request.min_block_needed();
        let max_block_needed = web3_request.max_block_needed();

        // max lag was already handled
        for rpc in self.inner.iter().cloned() {
            if rpc.backup && !self.backups_needed {
                // this backup check was already done, but
                // TODO: push these into `backup_for_request` Vec?
                continue;
            }

            if self.check_block_data {
                if let Some(block_needed) = min_block_needed {
                    if !rpc.has_data_for_request(web3_request, block_needed) {
                        outer_for_request.push(rpc);
                        continue;
                    }
                }
                if let Some(block_needed) = max_block_needed {
                    if !rpc.has_data_for_request(web3_request, block_needed) {
                        outer_for_request.push(rpc);
                        continue;
                    }
                }
            }

            inner_for_request.push(rpc);
        }

        // TODO: use web3_request.start_instant? I think we want it to be as recent as possible
        let now = Instant::now();

        match self.sort_mode {
            SortMethod::Shuffle => {
                // if we are shuffling, it is because we don't watch the head_blocks of the rpcs
                // clone all of the rpcs
                let mut rng = nanorand::tls_rng();

                // we use shuffle instead of sort si that the load gets spread around more
                // we will still compare weights during `RpcsForRequest::to_stream`

                inner_for_request.sort_by_cached_key(|x| {
                    x.shuffle_for_load_balancing_on(max_block_needed, &mut rng, now)
                });
                outer_for_request.sort_by_cached_key(|x| {
                    x.shuffle_for_load_balancing_on(max_block_needed, &mut rng, now)
                });
            }
            SortMethod::Sort => {
                // we sort so that the best nodes are always preferred. we will compare weights during `RpcsForRequest::to_stream`
                inner_for_request
                    .sort_by_cached_key(|x| x.sort_for_load_balancing_on(max_block_needed, now));
                outer_for_request
                    .sort_by_cached_key(|x| x.sort_for_load_balancing_on(max_block_needed, now));
            }
        }

        if inner_for_request.is_empty() {
            warn!(?inner_for_request, ?outer_for_request, %web3_request, head_block=%MaybeBlockNum(&head_block_num), "no rpcs for request");
            None
        } else {
            trace!(?inner_for_request, ?outer_for_request, %web3_request, "for_request");
            Some(RpcsForRequest {
                inner: inner_for_request,
                outer: outer_for_request,
                request: web3_request.clone(),
            })
        }
    }

    pub fn all(&self) -> hashbrown::hash_set::Iter<'_, Arc<Web3Rpc>> {
        self.inner.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// TODO! we should also keep the number on the head block saved
    #[inline]
    pub fn num_active_rpcs(&self) -> usize {
        self.inner.len()
    }

    // TODO: sum_hard_limit?
}

// TODO: refs for all of these. borrow on a Sender is cheap enough
// TODO: move this to many.rs
impl Web3Rpcs {
    #[inline]
    pub fn head_block(&self) -> Option<BlockHeader> {
        self.watch_head_block
            .as_ref()
            .and_then(|x| x.borrow().clone())
    }

    #[inline]
    pub fn head_block_num(&self) -> Option<U64> {
        self.watch_head_block
            .as_ref()
            .and_then(|x| x.borrow().as_ref().map(|x| x.number()))
    }

    pub fn synced(&self) -> bool {
        let consensus = self.watch_ranked_rpcs.borrow();

        if let Some(consensus) = consensus.as_ref() {
            !consensus.is_empty()
        } else {
            false
        }
    }

    pub fn num_synced_rpcs(&self) -> usize {
        let consensus = self.watch_ranked_rpcs.borrow();

        if let Some(consensus) = consensus.as_ref() {
            consensus.num_synced
        } else {
            0
        }
    }
}

type FirstSeenCache = Cache<B256, Instant>;
type SampledHeadsCache = Cache<(B256, String), ()>;

/// A ConsensusConnections builder that tracks all connection heads across multiple groups of servers
pub struct ConsensusFinder {
    rpc_heads: HashMap<Arc<Web3Rpc>, BlockHeader>,
    /// no consensus if the best known block is too old
    max_head_block_age: Option<Duration>,
    /// no consensus if the best consensus block is too far behind the best known
    max_head_block_lag: U64,
    /// Block Hash -> First Seen Instant. used to track rpc.head_delay. The same cache should be shared between all ConnectionsGroups
    first_seen: FirstSeenCache,
    /// Block hash and RPC pairs that already contributed a head-delay sample.
    sampled_heads: SampledHeadsCache,
}

impl ConsensusFinder {
    pub fn new(max_head_block_age: Option<Duration>, max_head_block_lag: U64) -> Self {
        // TODO: what's a good capacity for this? it shouldn't need to be very large
        let first_seen = Cache::new(16);
        let sampled_heads = Cache::new(256);

        let rpc_heads = HashMap::new();

        Self {
            rpc_heads,
            max_head_block_age,
            max_head_block_lag,
            first_seen,
            sampled_heads,
        }
    }

    pub fn len(&self) -> usize {
        self.rpc_heads.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rpc_heads.is_empty()
    }

    /// `connection_heads` is a mapping of rpc_names to head block hashes.
    /// self.blockchain_map is a mapping of hashes to block headers.
    /// TODO: return something?
    /// TODO: move this onto ConsensusFinder
    pub(super) async fn refresh(
        &mut self,
        web3_rpcs: &Web3Rpcs,
        rpc: Option<&Arc<Web3Rpc>>,
        new_block: Option<BlockHeader>,
    ) -> Web3ProxyResult<bool> {
        let rpc_block_sender = rpc.and_then(|x| x.head_block_sender.as_ref());

        let new_ranked_rpcs = match self
            .rank_rpcs(web3_rpcs)
            .await
            .web3_context("error while finding consensus head block!")?
        {
            None => {
                warn!(?rpc, ?new_block, "no ranked rpcs found!");

                if let Some(rpc_block_sender) = rpc_block_sender {
                    rpc_block_sender.send_replace(new_block);
                }

                return Ok(false);
            }
            Some(x) => x,
        };

        trace!(?new_ranked_rpcs);

        let watch_consensus_head_sender = web3_rpcs.watch_head_block.as_ref().unwrap();
        // TODO: think more about the default for tiers
        let best_tier = self.best_tier().unwrap_or_default();
        let worst_tier = self.worst_tier().unwrap_or_default();
        let backups_needed = new_ranked_rpcs.backups_needed;
        let consensus_head_block = new_ranked_rpcs.head_block.clone();
        let num_consensus_rpcs = new_ranked_rpcs.num_synced;
        let num_active_rpcs = self.len();
        let total_rpcs = web3_rpcs.len();

        let new_ranked_rpcs = Arc::new(new_ranked_rpcs);

        if let Some(rpc_block_sender) = rpc_block_sender {
            rpc_block_sender.send_replace(new_block.clone());
        }

        let old_ranked_rpcs = web3_rpcs
            .watch_ranked_rpcs
            .send_replace(Some(new_ranked_rpcs.clone()));

        let backups_voted_str = if backups_needed { "B " } else { "" };

        let update_source = ConsensusUpdateSource {
            consensus_block: &consensus_head_block,
            rpc: rpc.map(Arc::as_ref),
            rpc_block: &new_block,
        };

        match old_ranked_rpcs.as_ref() {
            None => {
                info!(
                    "first {}/{} {}{}/{}/{} {}",
                    best_tier,
                    worst_tier,
                    backups_voted_str,
                    num_consensus_rpcs,
                    num_active_rpcs,
                    total_rpcs,
                    update_source,
                );

                if backups_needed {
                    // TODO: what else should be in this error?
                    warn!("Backup RPCs are in use!");
                }

                // this should already be cached, but now we set to consensus_head
                let consensus_head_block = if let Some(consensus_head_block) = consensus_head_block
                {
                    let consensus_head_block = web3_rpcs
                        .try_cache_block_header(consensus_head_block, true)
                        .await?;

                    Some(consensus_head_block)
                } else {
                    None
                };

                watch_consensus_head_sender
                    .send(consensus_head_block)
                    .or(Err(Web3ProxyError::WatchSendError))
                    .web3_context(
                        "watch_consensus_head_sender failed sending first consensus_head_block",
                    )?;
            }
            Some(old_consensus_connections) => {
                let old_head_block = &old_consensus_connections.head_block;

                let consensus_num = consensus_head_block.as_ref().map(|x| x.number());
                let old_head_num = old_head_block.as_ref().map(|x| x.number());

                let consensus_hash = consensus_head_block.as_ref().map(|x| x.hash());
                let old_head_hash = old_head_block.as_ref().map(|x| x.hash());

                match consensus_num.cmp(&old_head_num) {
                    Ordering::Equal => {
                        // multiple blocks with the same number! fork detected!
                        if consensus_hash == old_head_hash {
                            // no change in hash. no need to use watch_consensus_head_sender
                            // TODO: trace level if rpc is backup
                            debug!(
                                "con {}/{} {}{}/{}/{} {}",
                                best_tier,
                                worst_tier,
                                backups_voted_str,
                                num_consensus_rpcs,
                                num_active_rpcs,
                                total_rpcs,
                                update_source,
                            )
                        } else {
                            // hash changed

                            debug!(
                                "unc {}/{} {}{}/{}/{} old={} {}",
                                best_tier,
                                worst_tier,
                                backups_voted_str,
                                num_consensus_rpcs,
                                num_active_rpcs,
                                total_rpcs,
                                MaybeBlock(old_head_block),
                                update_source,
                            );

                            let consensus_head_block = if let Some(consensus_head_block) =
                                consensus_head_block
                            {
                                let consensus_head_block = web3_rpcs
                                    .try_cache_block_header(consensus_head_block, true)
                                    .await
                                    .web3_context("save consensus_head_block as heaviest chain")?;

                                Some(consensus_head_block)
                            } else {
                                None
                            };

                            watch_consensus_head_sender
                                .send(consensus_head_block)
                                .or(Err(Web3ProxyError::WatchSendError))
                                .web3_context("watch_consensus_head_sender failed sending uncled consensus_head_block")?;
                        }
                    }
                    Ordering::Less => {
                        // this is unlikely but possible
                        // TODO: better log that includes all the votes
                        warn!(
                            "chain rolled back {}/{} {}{}/{}/{} old={} {}",
                            best_tier,
                            worst_tier,
                            backups_voted_str,
                            num_consensus_rpcs,
                            num_active_rpcs,
                            total_rpcs,
                            MaybeBlock(old_head_block),
                            update_source,
                        );

                        if backups_needed {
                            // TODO: what else should be in this error?
                            warn!("Backup RPCs are in use!");
                        }

                        // TODO: tell save_block to remove any higher block numbers from the cache. not needed because we have other checks on requested blocks being > head, but still seems like a good idea
                        let consensus_head_block =
                            if let Some(consensus_head_block) = consensus_head_block {
                                let consensus_head_block = web3_rpcs
                                    .try_cache_block_header(consensus_head_block, true)
                                    .await
                                    .web3_context(
                                        "save_block sending consensus_head_block as heaviest chain",
                                    )?;

                                Some(consensus_head_block)
                            } else {
                                None
                            };

                        watch_consensus_head_sender
                            .send(consensus_head_block)
                            .or(Err(Web3ProxyError::WatchSendError))
                            .web3_context("watch_consensus_head_sender failed sending rollback consensus_head_block")?;
                    }
                    Ordering::Greater => {
                        info!(
                            "new {}/{} {}{}/{}/{} {}",
                            best_tier,
                            worst_tier,
                            backups_voted_str,
                            num_consensus_rpcs,
                            num_active_rpcs,
                            total_rpcs,
                            update_source,
                        );

                        if backups_needed {
                            // TODO: what else should be in this error?
                            warn!("Backup RPCs are in use!");
                        }

                        // this should already be cached, but now we set to consensus_head
                        let consensus_head_block =
                            if let Some(consensus_head_block) = consensus_head_block {
                                Some(
                                    web3_rpcs
                                        .try_cache_block_header(consensus_head_block, true)
                                        .await?,
                                )
                            } else {
                                None
                            };

                        watch_consensus_head_sender.send(consensus_head_block)
                            .or(Err(Web3ProxyError::WatchSendError))
                            .web3_context("watch_consensus_head_sender failed sending new consensus_head_block")?;
                    }
                }
            }
        }

        Ok(true)
    }

    pub(super) async fn process_block_from_rpc(
        &mut self,
        web3_rpcs: &Web3Rpcs,
        observation: HeadObservation,
    ) -> Web3ProxyResult<bool> {
        let HeadObservation {
            block: new_block,
            rpc,
            observed_at,
        } = observation;
        // TODO: how should we handle an error here?
        if !self
            .update_rpc(new_block.clone(), rpc.clone(), observed_at, web3_rpcs)
            .await
            .web3_context("failed to update rpc")?
        {
            // nothing changed. no need to scan for a new consensus head
            // TODO: this should this be true if there is an existing consensus?
            return Ok(false);
        }

        self.refresh(web3_rpcs, Some(&rpc), new_block).await
    }

    fn remove(&mut self, rpc: &Arc<Web3Rpc>) -> Option<BlockHeader> {
        self.rpc_heads.remove(rpc)
    }

    async fn insert(
        &mut self,
        rpc: Arc<Web3Rpc>,
        block: BlockHeader,
        observed_at: Instant,
    ) -> Option<BlockHeader> {
        let block_hash = *block.hash();
        let first_seen = self
            .first_seen
            .get_with(block_hash, async { observed_at })
            .await;

        let sample_key = (block_hash, rpc.name.clone());
        if self.sampled_heads.get(&sample_key).await.is_none() {
            self.sampled_heads.insert(sample_key, ()).await;
            let latency = observed_at
                .checked_duration_since(first_seen)
                .unwrap_or_default();
            rpc.head_delay.write().record_secs(latency.as_secs_f32());
        }

        // update the local mapping of rpc -> block
        self.rpc_heads.insert(rpc, block)
    }

    /// Update our tracking of the rpc and return true if something changed
    pub(crate) async fn update_rpc(
        &mut self,
        rpc_head_block: Option<BlockHeader>,
        rpc: Arc<Web3Rpc>,
        observed_at: Instant,
        // we need this so we can save the block to caches. i don't like it though. maybe we should use a lazy_static Cache wrapper that has a "save_block" method?. i generally dislike globals but i also dislike all the types having to pass eachother around
        web3_connections: &Web3Rpcs,
    ) -> Web3ProxyResult<bool> {
        // add the rpc's block to connection_heads, or remove the rpc from connection_heads
        let changed = match rpc_head_block {
            Some(mut rpc_head_block) => {
                // we don't know if its on the heaviest chain yet
                rpc_head_block = web3_connections
                    .try_cache_block_header(rpc_head_block, false)
                    .await
                    .web3_context("failed caching block")?;

                match self.insert(rpc, rpc_head_block.clone(), observed_at).await {
                    Some(prev_block) => {
                        // false if this block was already sent by this rpc
                        // true if new block for this rpc
                        prev_block.hash() != rpc_head_block.hash()
                    }
                    _ => {
                        // first block for this rpc
                        true
                    }
                }
            }
            None => {
                // false if this rpc was already removed
                // true if rpc head changed from being synced to not
                self.remove(&rpc).is_some()
            }
        };

        Ok(changed)
    }

    pub async fn update_tiers(&mut self) -> Web3ProxyResult<()> {
        match self.rpc_heads.len() {
            0 => {}
            1 => {
                for rpc in self.rpc_heads.keys() {
                    rpc.tier.store(1, atomic::Ordering::SeqCst)
                }
            }
            _ => {
                // iterate first to find bounds
                // min_latency_sec is actual min_median_latency_sec
                let mut min_median_latency_sec = f32::MAX;
                let mut max_median_latency_sec = f32::MIN;
                let mut median_latencies_sec = HashMap::new();
                for rpc in self.rpc_heads.keys() {
                    let median_latency_sec = rpc
                        .median_latency
                        .as_ref()
                        .map(|x| x.seconds())
                        .unwrap_or_default();

                    min_median_latency_sec = min_median_latency_sec.min(median_latency_sec);
                    max_median_latency_sec = min_median_latency_sec.max(median_latency_sec);

                    median_latencies_sec.insert(rpc, median_latency_sec);
                }

                // dev logging of a histogram
                if enabled!(Level::TRACE) {
                    // convert to ms because the histogram needs ints
                    let max_median_latency_ms = (max_median_latency_sec * 1000.0).ceil() as u64;

                    // create the histogram
                    // histogram requires high to be at least 2 x low
                    // using min_latency for low does not work how we want it though
                    // so just set the default range = 1ms..1s
                    let hist_low = 1;
                    let hist_high = max_median_latency_ms.max(1_000);
                    let mut hist_ms =
                        Histogram::<u32>::new_with_bounds(hist_low, hist_high, 3).unwrap();

                    // TODO: resize shouldn't be necessary, but i've seen it error
                    hist_ms.auto(true);

                    for median_sec in median_latencies_sec.values() {
                        let median_ms = (median_sec * 1000.0).round() as u64;

                        hist_ms.record(median_ms)?;
                    }

                    // print the histogram. see docs/histograms.txt for more info
                    let mut encoder =
                        base64::write::EncoderWriter::new(Vec::new(), &general_purpose::STANDARD);

                    V2DeflateSerializer::new()
                        .serialize(&hist_ms, &mut encoder)
                        .unwrap();

                    let encoded = encoder.finish().unwrap();

                    let encoded = String::from_utf8(encoded).unwrap();

                    trace!("weighted_latencies: {}", encoded);
                }

                trace!("median_latencies_sec: {:#?}", median_latencies_sec);

                trace!("min_median_latency_sec: {}", min_median_latency_sec);

                // TODO: get someone who is better at math to do something smarter. maybe involving stddev? maybe involving cutting the histogram at the troughs?
                // bucket sizes of the larger of 20ms or 1/2 the lowest latency
                // TODO: is 20ms an okay default? make it configurable?
                // TODO: does keeping the buckets the same size make sense?
                let tier_sec_size = 0.020f32.max(min_median_latency_sec / 2.0);

                trace!("tier_sec_size: {}", tier_sec_size);

                for (rpc, median_latency_sec) in median_latencies_sec.into_iter() {
                    let tier = (median_latency_sec - min_median_latency_sec) / tier_sec_size;

                    // start tiers at 1
                    let tier = (tier.floor() as u32).saturating_add(1);

                    trace!("{} - p50_sec: {}, tier {}", rpc, median_latency_sec, tier);

                    rpc.tier.store(tier, atomic::Ordering::SeqCst);
                }
            }
        }

        Ok(())
    }

    /// TODO: this is probably way too slow and buggy
    pub async fn rank_rpcs(&mut self, web3_rpcs: &Web3Rpcs) -> Web3ProxyResult<Option<RankedRpcs>> {
        self.update_tiers().await?;

        let minmax_block = self
            .rpc_heads
            .values()
            .filter(|x| {
                if let Some(max_block_age) = self.max_head_block_age {
                    if x.age() > max_block_age {
                        return false;
                    }
                }

                true
            })
            .minmax_by_key(|x| x.number());

        let (lowest_block, highest_block) = match minmax_block {
            MinMaxResult::NoElements => return Ok(None),
            MinMaxResult::OneElement(x) => (x, x),
            MinMaxResult::MinMax(min, max) => (min, max),
        };

        let highest_block_number = highest_block.number();

        trace!("highest_block_number: {}", highest_block_number);

        trace!("lowest_block_number: {}", lowest_block.number());

        // TODO: move this default. should be in config, not here
        // TODO: arbitrum needs more slack
        let max_lag_block_number = highest_block_number.saturating_sub(self.max_head_block_lag);

        trace!("max_lag_block_number: {}", max_lag_block_number);

        let num_known = self.rpc_heads.len();

        // No ancestor can produce a higher consensus block when the highest
        // primary head already has enough direct votes.
        let mut highest_primary_votes: HashMap<BlockHeader, (HashSet<&Arc<Web3Rpc>>, u32)> =
            HashMap::with_capacity(num_known);

        for (rpc, rpc_head) in self.rpc_heads.iter() {
            if !rpc.healthy.load(atomic::Ordering::SeqCst)
                || rpc.backup
                || rpc_head.number() != highest_block_number
            {
                continue;
            }

            if let Some(max_age) = self.max_head_block_age {
                if rpc_head.age() > max_age {
                    continue;
                }
            }

            let entry = highest_primary_votes.entry(rpc_head.clone()).or_default();
            entry.0.insert(rpc);
            entry.1 += rpc.soft_limit;
        }

        if highest_primary_votes
            .values()
            .any(|(rpcs, sum_soft_limit)| {
                rpcs.len() >= web3_rpcs.min_synced_rpcs
                    && *sum_soft_limit >= web3_rpcs.min_sum_soft_limit
            })
        {
            return Ok(RankedRpcs::from_votes(
                web3_rpcs.min_synced_rpcs,
                web3_rpcs.min_sum_soft_limit,
                max_lag_block_number,
                highest_primary_votes,
                self.rpc_heads.clone(),
            ));
        }

        // TODO: also track the sum of *available* hard_limits? if any servers have no hard limits, use their soft limit or no limit?
        // TODO: struct for the value of the votes hashmap?
        let mut primary_votes: HashMap<BlockHeader, (HashSet<&Arc<Web3Rpc>>, u32)> =
            HashMap::with_capacity(num_known);
        let mut backup_votes: HashMap<BlockHeader, (HashSet<&Arc<Web3Rpc>>, u32)> =
            HashMap::with_capacity(num_known);

        for (rpc, rpc_head) in self.rpc_heads.iter() {
            if !rpc.healthy.load(atomic::Ordering::SeqCst) {
                // TODO: should unhealthy servers get a vote? they were included in minmax_block. i think that is enough
                continue;
            }

            let mut block_to_check = rpc_head.clone();

            while block_to_check.number() >= max_lag_block_number {
                if let Some(max_age) = self.max_head_block_age {
                    if block_to_check.age() > max_age {
                        break;
                    }
                }

                if !rpc.backup {
                    // backup nodes are excluded from the primary voting
                    let entry = primary_votes.entry(block_to_check.clone()).or_default();

                    entry.0.insert(rpc);
                    entry.1 += rpc.soft_limit;
                }

                // both primary and backup rpcs get included in the backup voting
                let backup_entry = backup_votes.entry(block_to_check.clone()).or_default();

                backup_entry.0.insert(rpc);
                backup_entry.1 += rpc.soft_limit;

                if block_to_check.number() == max_lag_block_number {
                    break;
                }

                let parent_hash = block_to_check.parent_hash();

                match web3_rpcs.blocks_by_hash.get(parent_hash).await {
                    Some(parent_block) => block_to_check = parent_block,
                    None => {
                        debug!(
                            "Unknown hash {:?} (parent of {:?}) during consensus finding",
                            parent_hash,
                            block_to_check.hash(),
                        );
                        break;
                    }
                }
            }
        }

        // we finished processing all tiers. check for primary results (if anything but the last tier found consensus, we already returned above)
        if let Some(consensus) = RankedRpcs::from_votes(
            web3_rpcs.min_synced_rpcs,
            web3_rpcs.min_sum_soft_limit,
            max_lag_block_number,
            primary_votes,
            self.rpc_heads.clone(),
        ) {
            return Ok(Some(consensus));
        }

        // primary votes didn't work. hopefully backup tiers are synced
        Ok(RankedRpcs::from_votes(
            web3_rpcs.min_synced_rpcs,
            web3_rpcs.min_sum_soft_limit,
            max_lag_block_number,
            backup_votes,
            self.rpc_heads.clone(),
        ))
    }

    pub fn best_tier(&self) -> Option<u32> {
        self.rpc_heads
            .iter()
            .map(|(x, _)| x.tier.load(atomic::Ordering::SeqCst))
            .min()
    }

    pub fn worst_tier(&self) -> Option<u32> {
        self.rpc_heads
            .iter()
            .map(|(x, _)| x.tier.load(atomic::Ordering::SeqCst))
            .max()
    }
}

/*
fn best_rpc<'a>(rpc_a: &'a Arc<Web3Rpc>, rpc_b: &'a Arc<Web3Rpc>) -> &'a Arc<Web3Rpc> {
    let now = Instant::now();

    let faster = min_by_key(rpc_a, rpc_b, |x| {
        (x.next_available(now), x.backup, x.weighted_peak_latency())
    });

    trace!("{} vs {} = {}", rpc_a, rpc_b, faster);

    faster
}
*/

impl RpcsForRequest {
    pub async fn open_batch_handles(&self) -> Vec<OpenRequestHandle> {
        let mut handles = Vec::with_capacity(self.inner.len());
        for rpc in &self.inner {
            match rpc.try_request_handle(&self.request, None, false).await {
                Ok(OpenRequestResult::Handle(handle)) => handles.push(handle),
                Ok(
                    OpenRequestResult::RetryAt(_)
                    | OpenRequestResult::Lagged(_)
                    | OpenRequestResult::Failed,
                )
                | Err(_) => {}
            }
        }
        handles
    }

    pub fn to_stream(self) -> impl Stream<Item = OpenRequestHandle> {
        stream! {
            trace!("entered stream");
            let error_handler = None;
            let mut opened_any = false;

            // todo!("be sure to set server_error if we exit without any rpcs!");
            while if opened_any {
                !self.request.expired()
            } else {
                !self.request.connect_timeout()
            } {
                let mut earliest_retry_at: Option<Instant> = None;
                let mut opened = 0;
                let mut tried = 0;
                let mut wait_for_sync = Vec::new();

                // TODO: we used to do a neat power of 2 random choices here, but it had bugs. bring that back
                for rpcs in [self.inner.iter(), self.outer.iter()] {
                    for best_rpc in rpcs {
                        tried += 1;

                        match best_rpc
                            .try_request_handle(&self.request, error_handler, false)
                            .await
                        {
                            Ok(OpenRequestResult::Handle(handle)) => {
                                trace!("opened handle: {}", best_rpc);
                                opened += 1;
                                opened_any = true;
                                yield handle;
                            }
                            Ok(OpenRequestResult::RetryAt(retry_at)) => {
                                trace!(
                                    "retry on {} @ {}",
                                    best_rpc,
                                    retry_at.duration_since(Instant::now()).as_secs_f32()
                                );
                                earliest_retry_at = Some(
                                    earliest_retry_at
                                        .map_or(retry_at, |earliest| earliest.min(retry_at)),
                                );
                            }
                            Ok(OpenRequestResult::Lagged(x)) => {
                                // this will probably always be the same block, right?
                                trace!("{} is lagged. will not work now", best_rpc);
                                wait_for_sync.push(x);
                            }
                            Ok(OpenRequestResult::Failed) => {
                                // TODO: log a warning? emit a stat?
                                trace!("best_rpc not ready: {}", best_rpc);
                            }
                            Err(err) => {
                                trace!("No request handle for {}. err={:?}", best_rpc, err);
                            }
                        }
                    }
                }

                // if we got this far, no inner or outer rpcs are ready. thats suprising since an inner should have been ready. maybe it got rate limited
                // TODO: log block needed and such
                warn!(?earliest_retry_at, num_waits=%wait_for_sync.len(), %tried, %opened, "no rpcs ready");

                let min_wait_until = Instant::now() + Duration::from_millis(10);
                let retry_deadline = if opened_any {
                    self.request.expire_at()
                } else {
                    self.request.connect_timeout_at()
                };

                // clear earliest_retry_at if it is too far in the future to help us
                if let Some(retry_at) = earliest_retry_at {
                    let corrected = retry_at.max(min_wait_until).min(retry_deadline);

                    // set a minimum of 100ms. this is probably actually a bug we should figure out.
                    earliest_retry_at = Some(corrected);
                } else if wait_for_sync.is_empty() {
                    break;
                } else {
                    earliest_retry_at = Some(retry_deadline);
                }

                let retry_until = sleep_until(earliest_retry_at.expect("retry_at should always be set by now"));

                if wait_for_sync.is_empty() {
                    retry_until.await;
                } else {
                    select!{
                        (x, _, _) = select_all(wait_for_sync) => {
                            match x {
                                Ok(rpc) => {
                                    trace!(%rpc, "rpc ready. it might be used on the next loop");

                                    // TODO: i don't think this sleep should be necessary. but i just want the cpus to cool down
                                    sleep_until(min_wait_until).await;
                                },
                                Err(err) => {
                                    error!(?err, "problem while waiting for an rpc for a request");

                                    // TODO: break or continue?
                                    // TODO: i don't think this sleep should be necessary. but i just want the cpus to cool down
                                    sleep_until(min_wait_until).await;
                                },
                            }
                        },
                        _ = retry_until => {
                            // we've waited long enough that trying again might work
                        },
                    }
                }
            }
        }

        // TODO: log that no servers were available. this might not be a server error. the user might have requested something in the far future (common when people mix up chains)
    }
}

struct MaybeBlock<'a>(pub &'a Option<BlockHeader>);

impl std::fmt::Display for MaybeBlock<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(x) => write!(f, "{}", x),
            None => write!(f, "None"),
        }
    }
}

struct ConsensusUpdateSource<'a> {
    consensus_block: &'a Option<BlockHeader>,
    rpc: Option<&'a Web3Rpc>,
    rpc_block: &'a Option<BlockHeader>,
}

impl std::fmt::Display for ConsensusUpdateSource<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.rpc {
            Some(rpc) if self.consensus_block == self.rpc_block => {
                write!(f, "rpc={}@{}", rpc, MaybeBlock(self.rpc_block))
            }
            Some(rpc) => write!(
                f,
                "heads_differ con={} rpc={}@{}",
                MaybeBlock(self.consensus_block),
                rpc,
                MaybeBlock(self.rpc_block),
            ),
            None => write!(f, "con={} rpc=None", MaybeBlock(self.consensus_block),),
        }
    }
}

struct MaybeBlockNum<'a>(pub &'a Option<U64>);

impl std::fmt::Display for MaybeBlockNum<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(x) => write!(f, "{}", x),
            None => write!(f, "None"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ConsensusFinder, ConsensusUpdateSource, RankedRpcs, RpcsForRequest};
    use crate::block_number::{BlockNumOrHash, RequestBlocks};
    use crate::jsonrpc::{RequestOrMethod, ValidatedRequest};
    use crate::rpcs::blockchain::{
        BlockHeader, BlockHydrationCoordinator, BlocksByHashCache, HeadObservationPublisher,
    };
    use crate::rpcs::many::Web3Rpcs;
    use crate::rpcs::one::Web3Rpc;
    use alloy::primitives::{B256, U64};
    use alloy::rpc::types::Header;
    use futures::StreamExt;
    use hashbrown::HashMap;
    use latency::EwmaLatency;
    use moka::future::Cache;
    use parking_lot::RwLock;
    use std::io::{self, Write};
    use std::sync::{atomic::AtomicBool, Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::{mpsc, watch};
    use tokio::time::{timeout, Instant};
    use tracing::Level;

    #[derive(Clone)]
    struct LogWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for LogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .expect("log output lock should be valid")
                .write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.0
                .lock()
                .expect("log output lock should be valid")
                .flush()
        }
    }

    fn block(number: u64, hash: B256, parent_hash: B256) -> BlockHeader {
        let mut header: Header = Header {
            hash,
            ..Default::default()
        };
        header.inner.number = number;
        header.inner.parent_hash = parent_hash;
        BlockHeader::new(Arc::new(header))
    }

    fn rpc_with_history_limits(
        name: &str,
        head: &BlockHeader,
        block_data_limit: u64,
        log_data_limit: u64,
    ) -> Arc<Web3Rpc> {
        let (head_block_sender, _) = watch::channel(Some(head.clone()));

        Arc::new(Web3Rpc {
            name: name.into(),
            block_data_limit: block_data_limit.into(),
            log_data_limit: log_data_limit.into(),
            head_block_sender: Some(head_block_sender),
            healthy: AtomicBool::new(true),
            ..Default::default()
        })
    }

    fn request(method: &'static str, request_blocks: RequestBlocks) -> Arc<ValidatedRequest> {
        Arc::new(ValidatedRequest {
            inner: RequestOrMethod::Method(method.into(), 0),
            request_blocks,
            ..Default::default()
        })
    }

    #[test]
    fn historical_logs_use_log_history_without_changing_archive_state_routing() {
        let head = block(1_000, B256::repeat_byte(0x11), B256::ZERO);
        let state_archive = rpc_with_history_limits("state-archive", &head, u64::MAX, 128);
        let log_archive = rpc_with_history_limits("log-archive", &head, 128, u64::MAX);
        let ranked = RankedRpcs::from_rpcs(
            vec![state_archive.clone(), log_archive.clone()],
            Some(head),
            true,
        );
        let log_request = request(
            "eth_getLogs",
            RequestBlocks::Range {
                from_block: BlockNumOrHash::Num(U64::from(100)),
                to_block: BlockNumOrHash::Num(U64::from(100)),
            },
        );
        let log_rpcs = ranked
            .for_request(&log_request)
            .expect("a log-history backend should serve historical logs");
        assert_eq!(log_rpcs.inner.len(), 1);
        assert_eq!(log_rpcs.inner[0].name, "log-archive");

        let recent_log_request = request(
            "eth_getLogs",
            RequestBlocks::Range {
                from_block: BlockNumOrHash::Num(U64::from(900)),
                to_block: BlockNumOrHash::Num(U64::from(900)),
            },
        );
        let recent_log_rpcs = ranked
            .for_request(&recent_log_request)
            .expect("recent logs should use all backends that retain them");
        let mut recent_log_rpc_names = recent_log_rpcs
            .inner
            .iter()
            .map(|rpc| rpc.name.as_str())
            .collect::<Vec<_>>();
        recent_log_rpc_names.sort_unstable();
        assert_eq!(recent_log_rpc_names, ["log-archive", "state-archive"]);

        let state_request = request(
            "eth_getCode",
            RequestBlocks::Point {
                block_needed: BlockNumOrHash::Num(U64::from(100)),
            },
        );
        let state_rpcs = ranked
            .for_request(&state_request)
            .expect("a state-history backend should serve historical state");
        assert_eq!(state_rpcs.inner.len(), 1);
        assert_eq!(state_rpcs.inner[0].name, "state-archive");
    }

    #[test_log::test(tokio::test)]
    async fn request_stream_waits_for_the_earliest_backend_retry() {
        let retry_at = Instant::now() + Duration::from_millis(20);
        let (hard_limit_until, _) = watch::channel(retry_at);
        let rpc = Arc::new(Web3Rpc {
            name: "rate-limited".to_owned(),
            healthy: AtomicBool::new(true),
            hard_limit_until: Some(hard_limit_until),
            ..Default::default()
        });
        let request = ValidatedRequest::new_internal(
            "eth_blockNumber".into(),
            &[(); 0],
            None,
            Some(Duration::from_secs(1)),
        )
        .await
        .unwrap();
        let rpcs = RpcsForRequest {
            inner: vec![rpc],
            outer: Vec::new(),
            request,
        };
        let mut stream = Box::pin(rpcs.to_stream());

        assert!(timeout(Duration::from_millis(100), stream.next())
            .await
            .expect("request stream should honor the backend retry time")
            .is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn request_stream_retries_after_a_backend_outlives_the_connect_deadline() {
        let (first_limit, _) = watch::channel(Instant::now());
        let first_limit_control = first_limit.clone();
        let first = Arc::new(Web3Rpc {
            name: "slow-failure".to_owned(),
            healthy: AtomicBool::new(true),
            hard_limit_until: Some(first_limit),
            ..Default::default()
        });
        let (retry_limit, _) = watch::channel(Instant::now() + Duration::from_millis(30));
        let retry = Arc::new(Web3Rpc {
            name: "recovering".to_owned(),
            healthy: AtomicBool::new(true),
            hard_limit_until: Some(retry_limit),
            ..Default::default()
        });
        let request = Arc::new(ValidatedRequest {
            inner: RequestOrMethod::Method("eth_call".into(), 0),
            connect_timeout: Duration::from_millis(10),
            expire_timeout: Duration::from_millis(100),
            ..Default::default()
        });
        let rpcs = RpcsForRequest {
            inner: vec![first, retry],
            outer: Vec::new(),
            request,
        };
        let mut stream = Box::pin(rpcs.to_stream());

        let first_handle = stream
            .next()
            .await
            .expect("the first backend should open immediately");
        assert_eq!(first_handle.connection_name(), "slow-failure");

        tokio::time::advance(Duration::from_millis(20)).await;
        first_limit_control.send_replace(Instant::now() + Duration::from_millis(40));
        drop(first_handle);

        let retry_handle = timeout(Duration::from_millis(50), stream.next())
            .await
            .expect("the stream should wait within the request expiry")
            .expect("the stream should retry after the connect deadline");
        assert_eq!(retry_handle.connection_name(), "recovering");
    }

    #[test]
    fn consensus_update_source_prints_matching_block_once_with_rpc() {
        let hash = B256::repeat_byte(0x11);
        let consensus_block = Some(block(10, hash, B256::ZERO));
        let rpc_block = consensus_block.clone();
        let rpc = Web3Rpc {
            name: "test-rpc".into(),
            ..Default::default()
        };

        let output = ConsensusUpdateSource {
            consensus_block: &consensus_block,
            rpc: Some(&rpc),
            rpc_block: &rpc_block,
        }
        .to_string();

        assert!(output.starts_with(&format!("rpc=test-rpc@10 ({hash}, ")));
        assert!(!output.contains("con="));
        assert_eq!(output.matches(&hash.to_string()).count(), 1);
    }

    #[test]
    fn consensus_update_source_marks_different_blocks_and_prints_both_hashes() {
        let consensus_hash = B256::repeat_byte(0x11);
        let rpc_hash = B256::repeat_byte(0x22);
        let consensus_block = Some(block(10, consensus_hash, B256::ZERO));
        let rpc_block = Some(block(11, rpc_hash, consensus_hash));
        let rpc = Web3Rpc {
            name: "test-rpc".into(),
            ..Default::default()
        };

        let output = ConsensusUpdateSource {
            consensus_block: &consensus_block,
            rpc: Some(&rpc),
            rpc_block: &rpc_block,
        }
        .to_string();

        assert!(output.starts_with(&format!("heads_differ con=10 ({consensus_hash}, ")));
        assert!(output.contains(&format!("rpc=test-rpc@11 ({rpc_hash}, ")));
    }

    fn web3_rpcs(blocks_by_hash: BlocksByHashCache, min_synced_rpcs: usize) -> Web3Rpcs {
        let (head_observation_sender, _) = mpsc::unbounded_channel();
        let head_observation_publisher = HeadObservationPublisher::new(head_observation_sender);
        let (watch_ranked_rpcs, _) = watch::channel(None);
        let block_responses = Cache::new(16);
        let block_hydration = BlockHydrationCoordinator::new(block_responses.clone());

        Web3Rpcs {
            name: "test".into(),
            chain_id: 1,
            head_observation_publisher,
            by_name: RwLock::new(HashMap::new()),
            watch_ranked_rpcs,
            watch_head_block: None,
            blocks_by_hash,
            blocks_by_number: Cache::new(16),
            block_responses,
            block_hydration,
            min_synced_rpcs,
            min_sum_soft_limit: u32::try_from(min_synced_rpcs)
                .expect("test RPC count should fit in a u32"),
            max_head_block_lag: U64::from(1u64),
            max_head_block_age: Duration::from_secs(60),
            pending_txid_firehose: None,
        }
    }

    async fn rank_with_debug_logs(
        heads: Vec<BlockHeader>,
        blocks_by_hash: BlocksByHashCache,
        min_synced_rpcs: usize,
    ) -> (Option<RankedRpcs>, String) {
        let web3_rpcs = web3_rpcs(blocks_by_hash, min_synced_rpcs);
        let mut consensus_finder = ConsensusFinder::new(None, U64::from(1u64));

        for (index, head) in heads.into_iter().enumerate() {
            let rpc = Arc::new(Web3Rpc {
                name: format!("test-rpc-{index}"),
                healthy: AtomicBool::new(true),
                soft_limit: 1,
                ..Default::default()
            });
            consensus_finder.rpc_heads.insert(rpc, head);
        }

        let output = Arc::new(Mutex::new(Vec::new()));
        let writer = LogWriter(output.clone());
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .without_time()
            .with_target(false)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let subscriber_guard = tracing::subscriber::set_default(subscriber);

        let ranked_rpcs = consensus_finder
            .rank_rpcs(&web3_rpcs)
            .await
            .expect("consensus ranking should succeed");

        drop(subscriber_guard);
        let output = output
            .lock()
            .expect("log output lock should be valid")
            .clone();
        let logs = String::from_utf8(output).expect("debug logs should be valid UTF-8");

        (ranked_rpcs, logs)
    }

    #[tokio::test(start_paused = true)]
    async fn head_delay_uses_observation_time_and_ignores_duplicate_heads() {
        let web3_rpcs = web3_rpcs(Cache::new(16), 1);
        let mut consensus_finder = ConsensusFinder::new(None, U64::from(1));
        let block_hash = B256::with_last_byte(0x42);
        let head = block(42, block_hash, B256::with_last_byte(0x41));
        let first_rpc = Arc::new(Web3Rpc {
            name: "first".into(),
            head_delay: RwLock::new(EwmaLatency::new(1.0, 0.0)),
            ..Default::default()
        });
        let second_rpc = Arc::new(Web3Rpc {
            name: "second".into(),
            head_delay: RwLock::new(EwmaLatency::new(1.0, 0.0)),
            ..Default::default()
        });
        let first_observed_at = Instant::now();

        consensus_finder
            .update_rpc(Some(head.clone()), first_rpc, first_observed_at, &web3_rpcs)
            .await
            .unwrap();

        let second_observed_at = first_observed_at + Duration::from_millis(25);
        tokio::time::sleep(Duration::from_secs(2)).await;
        consensus_finder
            .update_rpc(
                Some(head.clone()),
                second_rpc.clone(),
                second_observed_at,
                &web3_rpcs,
            )
            .await
            .unwrap();

        assert_eq!(
            second_rpc.head_delay.read().latency(),
            Duration::from_millis(25)
        );

        consensus_finder
            .update_rpc(
                Some(head),
                second_rpc.clone(),
                first_observed_at + Duration::from_secs(1),
                &web3_rpcs,
            )
            .await
            .unwrap();

        assert_eq!(
            second_rpc.head_delay.read().latency(),
            Duration::from_millis(25)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rank_rpcs_does_not_request_parent_before_lag_boundary() {
        let missing_parent_hash = B256::with_last_byte(9);
        let boundary = block(9, B256::with_last_byte(10), missing_parent_hash);
        let head = block(10, B256::with_last_byte(11), *boundary.hash());
        let blocks_by_hash = Cache::new(16);
        blocks_by_hash
            .insert(*boundary.hash(), boundary.clone())
            .await;

        let (ranked_rpcs, logs) =
            rank_with_debug_logs(vec![head, boundary.clone()], blocks_by_hash, 2).await;

        assert_eq!(
            ranked_rpcs
                .and_then(|ranked| ranked.head_block)
                .map(|block| (block.number(), *block.hash())),
            Some((boundary.number(), *boundary.hash()))
        );
        assert_eq!(
            logs.matches("Unknown hash").count(),
            0,
            "the scan requested the out-of-window parent {missing_parent_hash:?}: {logs}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rank_rpcs_does_not_scan_parent_when_highest_head_has_consensus() {
        let missing_parent_hash = B256::with_last_byte(10);
        let head = block(10, B256::with_last_byte(11), missing_parent_hash);

        let (ranked_rpcs, logs) =
            rank_with_debug_logs(vec![head.clone(), head.clone()], Cache::new(16), 2).await;

        assert_eq!(
            ranked_rpcs
                .as_ref()
                .and_then(|ranked| ranked.head_block.as_ref())
                .map(|block| (block.number(), *block.hash())),
            Some((head.number(), *head.hash()))
        );
        assert_eq!(
            ranked_rpcs.as_ref().map(|ranked| ranked.num_synced),
            Some(2)
        );
        assert_eq!(
            logs.matches("Unknown hash").count(),
            0,
            "the scan requested unnecessary parent {missing_parent_hash:?}: {logs}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rank_rpcs_logs_missing_parent_needed_for_consensus() {
        let first_missing_parent = B256::with_last_byte(8);
        let second_missing_parent = B256::with_last_byte(9);
        let first_head = block(10, B256::with_last_byte(10), first_missing_parent);
        let second_head = block(10, B256::with_last_byte(11), second_missing_parent);

        let (ranked_rpcs, logs) =
            rank_with_debug_logs(vec![first_head, second_head], Cache::new(16), 2).await;

        assert_eq!(ranked_rpcs.and_then(|ranked| ranked.head_block), None);
        assert_eq!(logs.matches("Unknown hash").count(), 2, "{logs}");
        assert_eq!(
            logs.matches(&format!("{first_missing_parent:?}")).count(),
            1,
            "{logs}"
        );
        assert_eq!(
            logs.matches(&format!("{second_missing_parent:?}")).count(),
            1,
            "{logs}"
        );
    }
}
