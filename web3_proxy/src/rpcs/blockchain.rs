//! Keep track of the blockchain as seen by a Web3Rpcs.
use super::consensus::ConsensusFinder;
use super::many::Web3Rpcs;
use crate::config::{average_block_interval, BlockAndRpc};
use crate::errors::Web3ProxyResult;
use alloy::primitives::{B256, U64};
use alloy::rpc::types::{Block, Header};
use moka::future::Cache;
use serde::ser::SerializeStruct;
use serde::Serialize;
use sonic_rs::json;
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

            // loop to make sure parent hashes match our caches
            // set the first ancestor to the blocks' parent hash. but keep going up the chain
            if let Some(parent_num) = block.number().checked_sub(U64::from(1)) {
                self.blocks_by_number
                    .insert(parent_num, *block.parent_hash())
                    .await;
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
    use sonic_rs::JsonValueTrait;

    fn block_header(timestamp: u64) -> BlockHeader {
        let mut header: Header = Header::default();
        header.inner.timestamp = timestamp;
        BlockHeader::new(Arc::new(header))
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
}
