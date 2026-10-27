//! Convert a complete Beacon block to a standard Engine request.
use alloy::eips::eip4844::{kzg_to_versioned_hash, Bytes48};
use alloy::primitives::{Bytes, B256};
use alloy::rpc::types::beacon::block::{BeaconBlockBodyElectra, BlockResponse};
use alloy::rpc::types::beacon::{block::SignedBeaconBlock, requests::ExecutionRequestsV4};
use alloy::rpc::types::engine::ExecutionPayload;
use alloy::rpc::types::engine::ExecutionPayloadV3;
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BeaconPayload(
    #[serde(with = "alloy::rpc::types::beacon::payload::beacon_payload")] pub ExecutionPayload,
);

pub type BeaconResponse = BlockResponse<BeaconBlockBodyElectra<BeaconPayload>>;

/// Keep consensus data opaque and borrow its exact bytes for publication.
#[derive(Deserialize)]
struct TransportResponse<'a> {
    version: String,
    #[serde(borrow)]
    data: sonic_rs::LazyValue<'a>,
}

#[derive(Deserialize)]
struct TransportBody {
    #[serde(with = "alloy::rpc::types::beacon::payload::beacon_payload_v3")]
    execution_payload: ExecutionPayloadV3,
    #[serde(with = "RequiredRequests")]
    execution_requests: ExecutionRequestsV4,
    blob_kzg_commitments: Vec<Bytes48>,
}

// Alloy accepts absent request lists as empty. The relay needs all three lists
// to format a complete Engine request, so require them during the same decode.
#[derive(Deserialize)]
#[serde(remote = "ExecutionRequestsV4")]
struct RequiredRequests {
    deposits: Vec<alloy::eips::eip6110::DepositRequest>,
    withdrawals: Vec<alloy::eips::eip7002::WithdrawalRequest>,
    consolidations: Vec<alloy::eips::eip7251::ConsolidationRequest>,
}

/// The body is encoded once and shared between target requests. It never includes JWT data.
#[derive(Debug)]
pub struct RelayPayload {
    pub beacon_root: B256,
    pub parent_beacon_root: B256,
    pub hash: B256,
    pub parent_hash: B256,
    pub slot: u64,
    pub number: u64,
    pub body: bytes::Bytes,
    pub fork: String,
    pub signed_block: bytes::Bytes,
    pub blob_commitments: Vec<Bytes48>,
}

impl RelayPayload {
    pub fn versioned_hashes(&self) -> Vec<B256> {
        self.blob_commitments
            .iter()
            .map(|commitment| kzg_to_versioned_hash(commitment.as_slice()))
            .collect()
    }
    pub fn decode(bytes: &[u8], root: B256) -> Result<Self> {
        let response: TransportResponse<'_> = sonic_rs::from_slice(bytes)?;
        ensure!(
            matches!(response.version.as_str(), "electra" | "fulu"),
            "unsupported Beacon fork"
        );
        let signed_block = bytes::Bytes::copy_from_slice(response.data.as_raw_str().as_bytes());
        let block: SignedBeaconBlock<TransportBody> =
            sonic_rs::from_str(response.data.as_raw_str())?;
        Self::from_beacon(block, response.version, root, signed_block)
    }

    fn from_beacon(
        block: SignedBeaconBlock<TransportBody>,
        fork: String,
        root: B256,
        signed_block: bytes::Bytes,
    ) -> Result<Self> {
        let message = block.message;
        let payload = message.body.execution_payload;
        let requests = message.body.execution_requests.to_requests();
        let hashes: Vec<B256> = message
            .body
            .blob_kzg_commitments
            .iter()
            .map(|c| kzg_to_versioned_hash(c.as_slice()))
            .collect();
        let inner = &payload.payload_inner.payload_inner;
        let hash = inner.block_hash;
        let parent_hash = inner.parent_hash;
        let number = inner.block_number;
        // The same ID is safe: each target uses a separate, non-batched HTTP request.
        #[derive(Serialize)]
        struct Request<'a> {
            jsonrpc: &'static str,
            id: u64,
            method: &'static str,
            params: (&'a ExecutionPayloadV3, &'a [B256], B256, &'a [Bytes]),
        }
        let body = sonic_rs::to_vec(&Request {
            jsonrpc: "2.0",
            id: 1,
            method: "engine_newPayloadV4",
            params: (&payload, &hashes, message.parent_root, requests.as_ref()),
        })?
        .into();
        Ok(Self {
            beacon_root: root,
            parent_beacon_root: message.parent_root,
            hash,
            parent_hash,
            slot: message.slot,
            number,
            body,
            fork,
            signed_block,
            blob_commitments: message.body.blob_kzg_commitments,
        })
    }

    pub fn cache_bytes(&self) -> u32 {
        (self.body.len() + self.signed_block.len() + self.blob_commitments.len() * 48)
            .min(u32::MAX as usize) as u32
    }
}

/// Publication contents assembled from source data without Ethereum validation.
#[derive(Debug)]
pub struct ConsensusPayload {
    pub block: std::sync::Arc<RelayPayload>,
    pub body: bytes::Bytes,
}

impl ConsensusPayload {
    pub fn from_blobs(
        block: std::sync::Arc<RelayPayload>,
        blobs: Vec<Bytes>,
        proofs: Vec<Bytes48>,
    ) -> Result<Self> {
        #[derive(Serialize)]
        struct Contents<'a> {
            signed_block: sonic_rs::LazyValue<'a>,
            kzg_proofs: &'a [Bytes48],
            blobs: &'a [Bytes],
        }
        let contents = Contents {
            signed_block: sonic_rs::from_slice(&block.signed_block)?,
            kzg_proofs: &proofs,
            blobs: &blobs,
        };
        let body = sonic_rs::to_vec(&contents)?.into();
        Ok(Self { block, body })
    }
}
