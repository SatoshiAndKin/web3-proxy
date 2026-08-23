//! Keep track of the blockchain as seen by a Web3Rpcs.
use super::consensus::ConsensusFinder;
use super::many::Web3Rpcs;
use crate::config::{average_block_interval, BlockAndRpc};
use crate::errors::{Web3ProxyError, Web3ProxyResult};
use crate::jsonrpc::{ParsedResponse, SingleRequest, SingleResponse};
use alloy::primitives::{B256, U64};
use alloy::rpc::types::{Block, Header};
use moka::future::{Cache, CacheBuilder};
use serde::ser::SerializeStruct;
use serde::Serialize;
use sonic_rs::{json, JsonContainerTrait, JsonValueTrait, OwnedLazyValue};
use std::fmt::Debug;
use std::hash::Hash;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fmt::Display, sync::Arc};
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{debug, error, warn};

// TODO: type for Hydrated Blocks with their full transactions?
pub type ArcBlock = Arc<Block>;
pub type ArcHeader = Arc<Header>;

pub type BlocksByHashCache = Cache<B256, BlockHeader>;
pub type BlocksByNumberCache = Cache<U64, B256>;
pub type BlockResponseCache = Cache<BlockResponseCacheKey, CachedBlockResponse>;

pub fn new_block_response_cache(max_bytes: u64) -> BlockResponseCache {
    CacheBuilder::new(max_bytes)
        .name("block_responses")
        .weigher(|_, block: &CachedBlockResponse| block.num_bytes())
        .time_to_idle(Duration::from_secs(30 * 60))
        .build()
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BlockResponseCacheKey {
    pub block_hash: B256,
    pub full_transactions: bool,
}

impl BlockResponseCacheKey {
    pub fn new(block_hash: B256, full_transactions: bool) -> Self {
        Self {
            block_hash,
            full_transactions,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CachedBlockResponse {
    result: Arc<OwnedLazyValue>,
    block_hash: B256,
    block_number: U64,
    uncle_hashes: Arc<[B256]>,
}

impl CachedBlockResponse {
    fn metadata(value: &serde_json::Value) -> Web3ProxyResult<(B256, U64, Arc<[B256]>)> {
        let object = value
            .as_object()
            .ok_or_else(|| Web3ProxyError::BadResponse("block result must be an object".into()))?;
        let block_hash = object
            .get("hash")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| Web3ProxyError::BadResponse("block result has no valid hash".into()))?;
        let block_number = object
            .get("number")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| {
                Web3ProxyError::BadResponse("block result has no valid number".into())
            })?;
        let uncle_hashes = object
            .get("uncles")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| Web3ProxyError::BadResponse("block result has no uncle array".into()))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .and_then(|value| value.parse().ok())
                    .ok_or_else(|| {
                        Web3ProxyError::BadResponse(
                            "block result contains an invalid uncle hash".into(),
                        )
                    })
            })
            .collect::<Web3ProxyResult<Vec<_>>>()?
            .into();

        Ok((block_hash, block_number, uncle_hashes))
    }

    fn parse_json(result: &OwnedLazyValue) -> Web3ProxyResult<serde_json::Value> {
        serde_json::from_str(
            &sonic_rs::to_string(result).expect("lazy JSON block results must serialize"),
        )
        .map_err(|_| Web3ProxyError::BadResponse("block result contains invalid JSON".into()))
    }

    pub fn from_hashes(result: Arc<OwnedLazyValue>) -> Web3ProxyResult<Self> {
        let value = Self::parse_json(&result)?;
        let (block_hash, block_number, uncle_hashes) = Self::metadata(&value)?;
        let transactions = value
            .get("transactions")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                Web3ProxyError::BadResponse("block result has no transaction array".into())
            })?;

        if transactions.iter().any(|transaction| {
            transaction
                .as_str()
                .and_then(|value| value.parse::<B256>().ok())
                .is_none()
        }) {
            return Err(Web3ProxyError::BadResponse(
                "hash-only block result contains an invalid transaction hash".into(),
            ));
        }

        Ok(Self {
            result,
            block_hash,
            block_number,
            uncle_hashes,
        })
    }

    pub fn from_full(
        result: Arc<OwnedLazyValue>,
        expected_hash: B256,
    ) -> Web3ProxyResult<(Self, Self)> {
        let mut hashes_value = Self::parse_json(&result)?;
        let (block_hash, block_number, uncle_hashes) = Self::metadata(&hashes_value)?;

        if block_hash != expected_hash {
            return Err(Web3ProxyError::BadResponse(
                "hydrated block hash does not match the requested hash".into(),
            ));
        }

        let transactions = hashes_value
            .get_mut("transactions")
            .and_then(serde_json::Value::as_array_mut)
            .ok_or_else(|| {
                Web3ProxyError::BadResponse("hydrated block has no transaction array".into())
            })?;
        let transaction_hashes = transactions
            .iter()
            .map(|transaction| {
                let hash = transaction
                    .as_object()
                    .and_then(|transaction| transaction.get("hash"))
                    .cloned()
                    .ok_or_else(|| {
                        Web3ProxyError::BadResponse("hydrated transaction has no hash field".into())
                    })?;
                if hash
                    .as_str()
                    .and_then(|value| value.parse::<B256>().ok())
                    .is_none()
                {
                    return Err(Web3ProxyError::BadResponse(
                        "hydrated transaction has an invalid hash field".into(),
                    ));
                }
                Ok(hash)
            })
            .collect::<Web3ProxyResult<Vec<_>>>()?;
        *transactions = transaction_hashes;

        let hashes_result = Arc::new(
            sonic_rs::from_str::<OwnedLazyValue>(
                &serde_json::to_string(&hashes_value).expect("JSON block values must serialize"),
            )
            .expect("serialized JSON block values must parse"),
        );

        let full = Self {
            result,
            block_hash,
            block_number,
            uncle_hashes: uncle_hashes.clone(),
        };
        let hashes = Self {
            result: hashes_result,
            block_hash,
            block_number,
            uncle_hashes,
        };

        Ok((full, hashes))
    }

    pub fn result(&self) -> Arc<OwnedLazyValue> {
        self.result.clone()
    }

    pub fn block_hash(&self) -> B256 {
        self.block_hash
    }

    pub fn block_number(&self) -> U64 {
        self.block_number
    }

    pub fn uncle_hashes(&self) -> &[B256] {
        &self.uncle_hashes
    }

    pub fn num_bytes(&self) -> u32 {
        u32::try_from(
            sonic_rs::to_string(&self.result)
                .expect("cached JSON block results must serialize")
                .len(),
        )
        .unwrap_or(u32::MAX)
    }
}

/// A block header and its age with a less verbose serialized format.
/// This does **not** implement Default. We rarely want a header with number 0 and hash 0.
#[derive(Clone)]
pub struct BlockHeader(pub ArcHeader);

impl Debug for BlockHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Web3ProxyBlock")
            .field("number", &self.number())
            .field("hash", &self.hash())
            .finish()
    }
}

impl Serialize for BlockHeader {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // TODO: i'm not sure about this name
        let mut state = serializer.serialize_struct("saved_block", 2)?;

        state.serialize_field("age", &self.age().as_secs_f32())?;

        let block = json!({
            "hash": self.hash(),
            "parent_hash": self.parent_hash(),
            "number": self.number().to::<u64>(),
            "timestamp": self.timestamp(),
        });

        state.serialize_field("block", &block)?;

        state.end()
    }
}

impl PartialEq for BlockHeader {
    fn eq(&self, other: &Self) -> bool {
        self.0.hash == other.0.hash
    }
}

impl Eq for BlockHeader {}

impl Hash for BlockHeader {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash.hash(state);
    }
}

impl BlockHeader {
    pub fn new(header: ArcHeader) -> Self {
        Self(header)
    }

    pub fn age(&self) -> Duration {
        self.age_at(SystemTime::now())
    }

    fn age_at(&self, now: SystemTime) -> Duration {
        let Some(block_timestamp) = UNIX_EPOCH.checked_add(Duration::from_secs(self.timestamp()))
        else {
            return Duration::ZERO;
        };

        now.duration_since(block_timestamp).unwrap_or_default()
    }

    #[inline(always)]
    pub fn parent_hash(&self) -> &B256 {
        &self.0.parent_hash
    }

    #[inline(always)]
    pub fn hash(&self) -> &B256 {
        &self.0.hash
    }

    #[inline(always)]
    pub fn number(&self) -> U64 {
        U64::from(self.0.number)
    }

    #[inline(always)]
    pub fn timestamp(&self) -> u64 {
        self.0.timestamp
    }
}

impl Display for BlockHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({}, {}ms old)",
            self.number(),
            self.hash(),
            self.age().as_millis()
        )
    }
}

impl From<ArcHeader> for BlockHeader {
    fn from(header: ArcHeader) -> Self {
        Self::new(header)
    }
}

impl Web3Rpcs {
    async fn invalidate_uncle_headers_if_canonical(&self, block: &CachedBlockResponse) {
        if self.blocks_by_number.get(&block.block_number).await != Some(block.block_hash) {
            return;
        }

        for uncle_hash in block.uncle_hashes.iter() {
            self.blocks_by_hash.invalidate(uncle_hash).await;
        }
    }

    async fn reconcile_cached_uncles(&self, block_hash: B256) {
        let hashes_key = BlockResponseCacheKey::new(block_hash, false);
        let block = if let Some(block) = self.block_responses.get(&hashes_key).await {
            Some(block)
        } else {
            self.block_responses
                .get(&BlockResponseCacheKey::new(block_hash, true))
                .await
        };

        if let Some(block) = block {
            self.invalidate_uncle_headers_if_canonical(&block).await;
        }
    }

    pub(crate) async fn cached_block_response(
        &self,
        request: &SingleRequest,
    ) -> Option<SingleResponse> {
        let params = request.params.as_array()?;
        if params.len() != 2 {
            return None;
        }
        let full_transactions = params[1].as_bool()?;
        let block_hash = match request.method.as_ref() {
            "eth_getBlockByHash" => params[0].as_str()?.parse().ok()?,
            "eth_getBlockByNumber" => {
                let block_number = if let Some(number) = params[0].as_u64() {
                    U64::from(number)
                } else {
                    sonic_rs::from_value::<U64>(&params[0]).ok()?
                };
                self.blocks_by_number.get(&block_number).await?
            }
            _ => return None,
        };
        let cached = self
            .block_responses
            .get(&BlockResponseCacheKey::new(block_hash, full_transactions))
            .await?;

        Some(ParsedResponse::from_result(cached.result(), request.id.clone()).into())
    }

    /// add a block to our mappings and track the heaviest chain
    pub async fn try_cache_block_header(
        &self,
        block: BlockHeader,
        consensus_head: bool,
    ) -> Web3ProxyResult<BlockHeader> {
        let block_hash = *block.hash();

        // TODO: i think we can rearrange this function to make it faster on the hot path
        if block_hash.is_zero() {
            debug!("Skipping block without hash!");
            return Ok(block);
        }

        // this block is very likely already in block_hashes

        if consensus_head {
            let block_num = block.number();

            // TODO: if there is an existing entry with a different block_hash,
            // TODO: use entry api to handle changing existing entries
            self.blocks_by_number.insert(block_num, block_hash).await;
            self.reconcile_cached_uncles(block_hash).await;

            // loop to make sure parent hashes match our caches
            // set the first ancestor to the blocks' parent hash. but keep going up the chain
            if let Some(parent_num) = block.number().checked_sub(U64::from(1)) {
                self.blocks_by_number
                    .insert(parent_num, *block.parent_hash())
                    .await;
                self.reconcile_cached_uncles(*block.parent_hash()).await;
            }
        }

        let block = self
            .blocks_by_hash
            .get_with_by_ref(&block_hash, async move { block })
            .await;

        Ok(block)
    }

    pub(super) async fn process_incoming_blocks(
        &self,
        mut block_and_rpc_receiver: mpsc::UnboundedReceiver<BlockAndRpc>,
    ) -> Web3ProxyResult<()> {
        if self.watch_head_block.is_none() {
            return Ok(());
        }

        // TODO: should this be spawned and then we just hold onto the handle here?
        let mut consensus_finder =
            ConsensusFinder::new(Some(self.max_head_block_age), self.max_head_block_lag);

        // TODO: what timeout on block receiver? we want to keep consensus_finder fresh so that server tiers are correct
        let triple_block_time = average_block_interval(self.chain_id).mul_f32(3.0);

        loop {
            select! {
                x = block_and_rpc_receiver.recv() => {
                    match x {
                        Some((new_block, rpc)) => {
                            let rpc_name = rpc.name.clone();

                            // TODO: we used to have a timeout on this, but i think it was obscuring a bug
                            match consensus_finder
                                .process_block_from_rpc(self, new_block, rpc)
                                .await
                            {
                                Ok(_) => {},
                                Err(err) => {
                                    error!(
                                        "error while processing block from rpc {}: {:#?}",
                                        rpc_name, err
                                    );
                                }
                            }
                        }
                        None => {
                            // TODO: panic is probably too much, but getting here is definitely not good
                            return Err(anyhow::anyhow!("block_receiver on {} exited", self).into());
                        }
                    }
                }
                _ = sleep(triple_block_time) => {
                    // TODO: what timeout on this?
                    match consensus_finder.refresh(self, None, None).await {
                        Ok(_) => {
                            warn!("had to refresh consensus finder. is the network going slow?");
                        }
                        Err(err) => {
                            error!("error while refreshing consensus finder: {:#?}", err);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_number::RequestBlocks;
    use crate::rpcs::many::Web3Rpcs;
    use hashbrown::HashMap;
    use parking_lot::RwLock;
    use sonic_rs::JsonValueTrait;
    use tokio::sync::{mpsc, watch};
    use tokio::time::Duration;

    fn block_header(timestamp: u64) -> BlockHeader {
        let mut header: Header = Header::default();
        header.inner.timestamp = timestamp;
        BlockHeader::new(Arc::new(header))
    }

    fn observed_header(number: u64, hash: B256, parent_hash: B256) -> BlockHeader {
        let mut header: Header = Header {
            hash,
            ..Default::default()
        };
        header.inner.number = number;
        header.inner.parent_hash = parent_hash;
        BlockHeader::new(Arc::new(header))
    }

    fn web3_rpcs(block_cache_max_bytes: u64) -> Web3Rpcs {
        let (block_and_rpc_sender, _) = mpsc::unbounded_channel();
        let (watch_ranked_rpcs, _) = watch::channel(None);

        Web3Rpcs {
            name: "block-cache-test".into(),
            chain_id: 1,
            block_and_rpc_sender,
            by_name: RwLock::new(HashMap::new()),
            watch_ranked_rpcs,
            watch_head_block: None,
            blocks_by_hash: Cache::new(16),
            blocks_by_number: Cache::new(16),
            block_responses: new_block_response_cache(block_cache_max_bytes),
            min_synced_rpcs: 1,
            min_sum_soft_limit: 1,
            max_head_block_lag: U64::from(1),
            max_head_block_age: Duration::from_secs(60),
            pending_txid_firehose: None,
        }
    }

    fn raw_block(value: serde_json::Value) -> Arc<OwnedLazyValue> {
        Arc::new(sonic_rs::from_str(&value.to_string()).unwrap())
    }

    async fn insert_block_forms(
        rpcs: &Web3Rpcs,
        value: serde_json::Value,
        hash: B256,
    ) -> (CachedBlockResponse, CachedBlockResponse) {
        let (full, hashes) = CachedBlockResponse::from_full(raw_block(value), hash).unwrap();
        rpcs.block_responses
            .insert(BlockResponseCacheKey::new(hash, true), full.clone())
            .await;
        rpcs.block_responses
            .insert(BlockResponseCacheKey::new(hash, false), hashes.clone())
            .await;
        (full, hashes)
    }

    async fn cached_response_json(
        rpcs: &Web3Rpcs,
        request: &SingleRequest,
    ) -> Option<serde_json::Value> {
        let response = rpcs.cached_block_response(request).await?;
        let response = response.parsed().await.unwrap();
        Some(serde_json::from_str(&sonic_rs::to_string(&response).unwrap()).unwrap())
    }

    #[test]
    fn block_age_preserves_subsecond_precision() {
        let block = block_header(1_000);
        let now = UNIX_EPOCH + Duration::from_millis(1_003_592);

        assert_eq!(block.age_at(now), Duration::from_millis(3_592));
    }

    #[test]
    fn block_age_is_exact_at_second_boundaries() {
        let block = block_header(1_000);
        let now = UNIX_EPOCH + Duration::from_secs(1_003);

        assert_eq!(block.age_at(now), Duration::from_secs(3));
    }

    #[test]
    fn block_age_clamps_non_past_timestamps_to_zero() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000);

        assert_eq!(block_header(1_000).age_at(now), Duration::ZERO);
        assert_eq!(block_header(1_001).age_at(now), Duration::ZERO);
        assert_eq!(block_header(u64::MAX).age_at(now), Duration::ZERO);
    }

    #[test]
    fn new_heads_payload_is_a_header_without_block_body_fields() {
        let mut header: Header = Header {
            hash: B256::with_last_byte(0x42),
            ..Default::default()
        };
        header.inner.parent_hash = B256::with_last_byte(0x41);
        header.inner.number = 42;
        header.inner.timestamp = 1_234;
        let head = BlockHeader::new(Arc::new(header));

        let payload = sonic_rs::to_value(&head.0).unwrap();

        assert_eq!(
            payload.get("hash").and_then(|value| value.as_str()),
            Some("0x0000000000000000000000000000000000000000000000000000000000000042")
        );
        assert_eq!(
            payload.get("parentHash").and_then(|value| value.as_str()),
            Some("0x0000000000000000000000000000000000000000000000000000000000000041")
        );
        assert_eq!(
            payload.get("number").and_then(|value| value.as_str()),
            Some("0x2a")
        );
        assert_eq!(
            payload.get("timestamp").and_then(|value| value.as_str()),
            Some("0x4d2")
        );
        assert_eq!(payload.get("transactions"), None);
        assert_eq!(payload.get("uncles"), None);
        assert_eq!(payload.get("withdrawals"), None);
    }

    #[tokio::test]
    async fn cache_serves_exact_hash_and_canonical_number_forms_with_caller_ids() {
        let rpcs = web3_rpcs(1_000_000);
        let canonical_hash = B256::with_last_byte(0x42);
        let competing_hash = B256::with_last_byte(0x43);
        let transaction_hash = B256::with_last_byte(0x11);
        let canonical = serde_json::json!({
            "hash": canonical_hash,
            "number": "0x2a",
            "transactions": [{"hash": transaction_hash, "providerTx": 1}],
            "uncles": [],
            "withdrawals": [{"index": "0x1"}],
            "providerBlock": "canonical",
        });
        let competing = serde_json::json!({
            "hash": competing_hash,
            "number": "0x2a",
            "transactions": [{"hash": transaction_hash, "providerTx": 2}],
            "uncles": [],
            "withdrawals": [],
            "providerBlock": "competing",
        });
        insert_block_forms(&rpcs, canonical.clone(), canonical_hash).await;
        insert_block_forms(&rpcs, competing.clone(), competing_hash).await;
        rpcs.blocks_by_number
            .insert(U64::from(42), canonical_hash)
            .await;

        let hash_full: SingleRequest = sonic_rs::from_str(&format!(
            r#"{{"jsonrpc":"2.0","id":"hash-full","method":"eth_getBlockByHash","params":["{canonical_hash}",true]}}"#
        ))
        .unwrap();
        let hash_hashes: SingleRequest = sonic_rs::from_str(&format!(
            r#"{{"jsonrpc":"2.0","id":71,"method":"eth_getBlockByHash","params":["{canonical_hash}",false]}}"#
        ))
        .unwrap();
        let number_full: SingleRequest = sonic_rs::from_str(
            r#"{"jsonrpc":"2.0","id":"number-full","method":"eth_getBlockByNumber","params":["0x2a",true]}"#,
        )
        .unwrap();
        let competing_by_hash: SingleRequest = sonic_rs::from_str(&format!(
            r#"{{"jsonrpc":"2.0","id":"competing","method":"eth_getBlockByHash","params":["{competing_hash}",true]}}"#
        ))
        .unwrap();

        let hash_full_response = cached_response_json(&rpcs, &hash_full).await.unwrap();
        assert_eq!(hash_full_response["id"], "hash-full");
        assert_eq!(hash_full_response["result"], canonical);

        let hash_hashes_response = cached_response_json(&rpcs, &hash_hashes).await.unwrap();
        assert_eq!(hash_hashes_response["id"], 71);
        assert_eq!(
            hash_hashes_response["result"]["transactions"],
            serde_json::json!([transaction_hash])
        );
        assert_eq!(hash_hashes_response["result"]["providerBlock"], "canonical");

        let number_full_response = cached_response_json(&rpcs, &number_full).await.unwrap();
        assert_eq!(number_full_response["id"], "number-full");
        assert_eq!(number_full_response["result"], canonical);

        let competing_response = cached_response_json(&rpcs, &competing_by_hash)
            .await
            .unwrap();
        assert_eq!(competing_response["result"], competing);

        let mut normalized_latest: SingleRequest = sonic_rs::from_str(
            r#"{"jsonrpc":"2.0","id":"latest","method":"eth_getBlockByNumber","params":["latest",false]}"#,
        )
        .unwrap();
        let canonical_head = observed_header(42, canonical_hash, B256::with_last_byte(0x41));
        RequestBlocks::try_new(&mut normalized_latest, Some(&canonical_head), None)
            .await
            .unwrap();
        let latest_response = cached_response_json(&rpcs, &normalized_latest)
            .await
            .unwrap();
        assert_eq!(latest_response["id"], "latest");
        assert_eq!(latest_response["result"]["hash"], canonical["hash"]);

        rpcs.blocks_by_number
            .insert(U64::from(42), competing_hash)
            .await;
        let reorged_response = cached_response_json(&rpcs, &number_full).await.unwrap();
        assert_eq!(reorged_response["result"], competing);

        for request in [
            r#"{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["pending",true]}"#,
            r#"{"jsonrpc":"2.0","id":2,"method":"eth_getBlockByNumber","params":["latest",true]}"#,
            r#"{"jsonrpc":"2.0","id":3,"method":"eth_getBlockByHash","params":["bad",true]}"#,
            r#"{"jsonrpc":"2.0","id":4,"method":"eth_getBlockByHash","params":["0x00"]}"#,
        ] {
            let request = sonic_rs::from_str(request).unwrap();
            assert!(rpcs.cached_block_response(&request).await.is_none());
        }
    }

    #[tokio::test]
    async fn block_response_cache_counts_both_forms_by_raw_json_bytes() {
        let rpcs = web3_rpcs(1_000_000);
        let block_hash = B256::with_last_byte(0x42);
        let (full, hashes) = insert_block_forms(
            &rpcs,
            serde_json::json!({
                "hash": block_hash,
                "number": "0x2a",
                "transactions": [{"hash": B256::with_last_byte(0x11), "data": "0x1234"}],
                "uncles": [],
            }),
            block_hash,
        )
        .await;
        rpcs.block_responses.run_pending_tasks().await;

        assert_eq!(
            rpcs.block_responses.weighted_size(),
            u64::from(full.num_bytes()) + u64::from(hashes.num_bytes())
        );
    }

    #[tokio::test]
    async fn uncle_headers_wait_for_consensus_when_hydration_finishes_first() {
        let rpcs = web3_rpcs(1_000_000);
        let block_hash = B256::with_last_byte(0x42);
        let parent_hash = B256::with_last_byte(0x41);
        let uncle_hash = B256::with_last_byte(0x33);
        let uncle_header = observed_header(41, uncle_hash, B256::with_last_byte(0x40));
        rpcs.blocks_by_hash.insert(uncle_hash, uncle_header).await;
        let (full, _) = insert_block_forms(
            &rpcs,
            serde_json::json!({
                "hash": block_hash,
                "number": "0x2a",
                "transactions": [],
                "uncles": [uncle_hash],
            }),
            block_hash,
        )
        .await;

        rpcs.invalidate_uncle_headers_if_canonical(&full).await;
        assert!(rpcs.blocks_by_hash.get(&uncle_hash).await.is_some());

        let block = observed_header(42, block_hash, parent_hash);
        rpcs.try_cache_block_header(block, true).await.unwrap();

        assert!(rpcs.blocks_by_hash.get(&uncle_hash).await.is_none());
        assert!(rpcs
            .block_responses
            .get(&BlockResponseCacheKey::new(block_hash, true))
            .await
            .is_some());
    }

    #[tokio::test]
    async fn uncle_headers_are_invalidated_when_hydration_finishes_after_consensus() {
        let rpcs = web3_rpcs(1_000_000);
        let block_hash = B256::with_last_byte(0x42);
        let uncle_hash = B256::with_last_byte(0x33);
        let uncle_header = observed_header(41, uncle_hash, B256::with_last_byte(0x40));
        rpcs.blocks_by_hash.insert(uncle_hash, uncle_header).await;
        rpcs.try_cache_block_header(
            observed_header(42, block_hash, B256::with_last_byte(0x41)),
            true,
        )
        .await
        .unwrap();
        assert!(rpcs.blocks_by_hash.get(&uncle_hash).await.is_some());

        let (full, _) = insert_block_forms(
            &rpcs,
            serde_json::json!({
                "hash": block_hash,
                "number": "0x2a",
                "transactions": [],
                "uncles": [uncle_hash],
            }),
            block_hash,
        )
        .await;
        rpcs.invalidate_uncle_headers_if_canonical(&full).await;

        assert!(rpcs.blocks_by_hash.get(&uncle_hash).await.is_none());
        assert!(rpcs
            .block_responses
            .get(&BlockResponseCacheKey::new(block_hash, false))
            .await
            .is_some());
    }
}
