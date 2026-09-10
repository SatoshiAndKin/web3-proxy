//! Convert a complete Beacon block to a standard Engine request.
use alloy::consensus::{Transaction, TxEnvelope};
use alloy::eips::eip4844::kzg_to_versioned_hash;
use alloy::primitives::{Bytes, B256};
use alloy_rpc_types_beacon::block::{BeaconBlockBodyElectra, BlockResponse};
use alloy_rpc_types_engine::{ExecutionPayload, ExecutionPayloadV3};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

use super::config::Network;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BeaconPayload(
    #[serde(with = "alloy_rpc_types_beacon::payload::beacon_payload")] pub ExecutionPayload,
);

pub type BeaconResponse = BlockResponse<BeaconBlockBodyElectra<BeaconPayload>>;

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
}

impl RelayPayload {
    pub fn decode(bytes: &[u8], expected_root: B256, network: &Network) -> Result<Self> {
        // Check the version before decoding a body whose format a future fork can change.
        #[derive(Deserialize)]
        struct Version {
            version: String,
        }
        let version: Version = sonic_rs::from_slice(bytes)?;
        ensure!(
            matches!(version.version.as_str(), "electra" | "fulu"),
            "unsupported Beacon fork"
        );
        // Alloy defaults missing execution requests to empty. At this trust
        // boundary require the complete fork schema, including all three lists.
        use sonic_rs::JsonValueTrait;
        let requests = sonic_rs::get(bytes, ["data", "message", "body", "execution_requests"])
            .map_err(|_| anyhow::anyhow!("missing execution requests"))?;
        for name in ["deposits", "withdrawals", "consolidations"] {
            ensure!(
                requests.get(name).is_some_and(|v| v.is_array()),
                "missing execution request list"
            );
        }
        let response: BeaconResponse = sonic_rs::from_slice(bytes)?;
        let message = &response.data.message;
        ensure!(
            network
                .fork_at(message.slot)
                .is_some_and(|f| f.name == version.version),
            "fork schedule mismatch"
        );
        let ExecutionPayload::V3(payload) = &message.body.execution_payload.0 else {
            anyhow::bail!("expected ExecutionPayloadV3");
        };
        ensure!(
            payload.payload_inner.payload_inner.timestamp == network.timestamp(message.slot)?,
            "payload timestamp mismatch"
        );
        let root = super::tree_hash::block_root(message)?;
        ensure!(root == expected_root, "Beacon block root mismatch");
        Self::from_beacon(response, root)
    }

    fn from_beacon(response: BeaconResponse, root: B256) -> Result<Self> {
        let message = response.data.message;
        let ExecutionPayload::V3(payload) = message.body.execution_payload.0 else {
            anyhow::bail!("expected ExecutionPayloadV3");
        };
        let requests = message.body.execution_requests.to_requests();
        let hashes: Vec<B256> = message
            .body
            .blob_kzg_commitments
            .iter()
            .map(|c| kzg_to_versioned_hash(c.as_slice()))
            .collect();
        validate_execution(&payload, message.parent_root, &requests, &hashes)?;
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
        })
    }
}

fn validate_execution(
    payload: &ExecutionPayloadV3,
    parent_root: B256,
    requests: &alloy::eips::eip7685::Requests,
    expected_hashes: &[B256],
) -> Result<()> {
    let mut block = payload.clone().try_into_block::<TxEnvelope>()?;
    block.header.parent_beacon_block_root = Some(parent_root);
    block.header.requests_hash = Some(requests.requests_hash());
    ensure!(
        block.header.hash_slow() == payload.payload_inner.payload_inner.block_hash,
        "execution block hash mismatch"
    );
    let hashes: Vec<_> = block
        .body
        .transactions
        .iter()
        .flat_map(|tx| tx.blob_versioned_hashes().unwrap_or_default())
        .copied()
        .collect();
    ensure!(
        hashes == expected_hashes,
        "blob commitments do not match transactions"
    );
    Ok(())
}
