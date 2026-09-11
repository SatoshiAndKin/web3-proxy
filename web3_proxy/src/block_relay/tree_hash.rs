//! Electra/Fulu mainnet-preset SSZ schema over Alloy's existing types.
//!
//! Alloy 2.4.1 provides these types and SSZ encoding, but no complete Beacon-block
//! TreeHash implementation. Use tree_hash for Merkleization, with consensus-spec
//! list limits (not the current list length). This is not BLS/consensus validation.
use super::payload::BeaconPayload;
use alloy::eips::{
    eip6110::MAX_DEPOSIT_RECEIPTS_PER_PAYLOAD, eip7002::MAX_WITHDRAWAL_REQUESTS_PER_BLOCK,
    eip7251::MAX_CONSOLIDATION_REQUESTS_PER_BLOCK,
};
use alloy::primitives::B256;
use alloy_rpc_types_beacon::{block::*, header::BeaconBlockHeader};
use alloy_rpc_types_engine::{ExecutionPayload, ExecutionPayloadV3};
use anyhow::{ensure, Result};
use tree_hash::{merkle_root, mix_in_length};

fn uint(n: u64) -> B256 {
    merkle_root(&n.to_le_bytes(), 1)
}
fn vector(bytes: &[u8]) -> B256 {
    merkle_root(bytes, bytes.len().div_ceil(32))
}
fn root_bytes(roots: &[B256]) -> Vec<u8> {
    roots.iter().flat_map(|r| r.0).collect()
}
fn container(fields: &[B256]) -> B256 {
    merkle_root(&root_bytes(fields), fields.len())
}
fn list(roots: &[B256], limit: usize) -> Result<B256> {
    ensure!(roots.len() <= limit, "SSZ list exceeds preset limit");
    Ok(mix_in_length(
        &merkle_root(&root_bytes(roots), limit),
        roots.len(),
    ))
}
fn byte_list(bytes: &[u8], limit: usize) -> Result<B256> {
    ensure!(bytes.len() <= limit, "SSZ byte list exceeds preset limit");
    Ok(mix_in_length(
        &merkle_root(bytes, limit.div_ceil(32)),
        bytes.len(),
    ))
}
fn bitlist(bytes: &[u8], limit: usize) -> Result<B256> {
    let last = *bytes
        .last()
        .ok_or_else(|| anyhow::anyhow!("empty bitlist encoding"))?;
    ensure!(last != 0, "missing bitlist delimiter");
    let bit = 7 - last.leading_zeros() as usize;
    let len = (bytes.len() - 1) * 8 + bit;
    ensure!(len <= limit, "SSZ bitlist exceeds preset limit");
    let mut packed = bytes.to_vec();
    *packed.last_mut().expect("nonempty") &= !(1 << bit);
    if bit == 0 {
        packed.pop();
    }
    Ok(mix_in_length(
        &merkle_root(&packed, limit.div_ceil(256)),
        len,
    ))
}
fn checkpoint(c: &Checkpoint) -> B256 {
    container(&[uint(c.epoch), c.root])
}
fn attestation_data(a: &AttestationData) -> B256 {
    container(&[
        uint(a.slot),
        uint(a.index),
        a.beacon_block_root,
        checkpoint(&a.source),
        checkpoint(&a.target),
    ])
}
fn indexed(a: &IndexedAttestation) -> Result<B256> {
    const LIMIT: usize = 2048 * 64;
    ensure!(
        a.attesting_indices.len() <= LIMIT,
        "too many attesting indices"
    );
    let bytes: Vec<_> = a
        .attesting_indices
        .iter()
        .flat_map(|x| x.to_le_bytes())
        .collect();
    let indices = mix_in_length(&merkle_root(&bytes, LIMIT / 4), a.attesting_indices.len());
    Ok(container(&[
        indices,
        attestation_data(&a.data),
        vector(a.signature.as_slice()),
    ]))
}
pub(super) fn header(h: &BeaconBlockHeader) -> B256 {
    container(&[
        uint(h.slot),
        uint(h.proposer_index),
        h.parent_root,
        h.state_root,
        h.body_root,
    ])
}
fn signed_header(h: &SignedBeaconBlockHeader) -> B256 {
    container(&[header(&h.message), vector(h.signature.as_slice())])
}

fn execution(p: &ExecutionPayloadV3) -> Result<B256> {
    let p2 = &p.payload_inner;
    let p1 = &p2.payload_inner;
    let txs: Vec<_> = p1
        .transactions
        .iter()
        .map(|t| byte_list(t, 1 << 30))
        .collect::<Result<_>>()?;
    let withdrawals: Vec<_> = p2
        .withdrawals
        .iter()
        .map(|w| {
            container(&[
                uint(w.index),
                uint(w.validator_index),
                vector(w.address.as_slice()),
                uint(w.amount),
            ])
        })
        .collect();
    Ok(container(&[
        p1.parent_hash,
        vector(p1.fee_recipient.as_slice()),
        p1.state_root,
        p1.receipts_root,
        vector(p1.logs_bloom.as_slice()),
        p1.prev_randao,
        uint(p1.block_number),
        uint(p1.gas_limit),
        uint(p1.gas_used),
        uint(p1.timestamp),
        byte_list(&p1.extra_data, 32)?,
        B256::from(p1.base_fee_per_gas.to_le_bytes::<32>()),
        p1.block_hash,
        list(&txs, 1 << 20)?,
        list(&withdrawals, 16)?,
        uint(p.blob_gas_used),
        uint(p.excess_blob_gas),
    ]))
}

pub fn block_root(block: &BeaconBlock<BeaconBlockBodyElectra<BeaconPayload>>) -> Result<B256> {
    Ok(header(&block_header(block)?))
}

pub(super) fn block_header(
    block: &BeaconBlock<BeaconBlockBodyElectra<BeaconPayload>>,
) -> Result<BeaconBlockHeader> {
    let b = &block.body;
    let ExecutionPayload::V3(payload) = &b.execution_payload.0 else {
        anyhow::bail!("unsupported execution payload");
    };
    let proposer_slashings: Vec<_> = b
        .proposer_slashings
        .iter()
        .map(|p| {
            container(&[
                signed_header(&p.signed_header_1),
                signed_header(&p.signed_header_2),
            ])
        })
        .collect();
    let attester_slashings: Vec<_> = b
        .attester_slashings
        .iter()
        .map(|a| {
            Ok(container(&[
                indexed(&a.attestation_1)?,
                indexed(&a.attestation_2)?,
            ]))
        })
        .collect::<Result<_>>()?;
    let attestations: Vec<_> = b
        .attestations
        .iter()
        .map(|a| {
            Ok(container(&[
                bitlist(&a.aggregation_bits, 2048 * 64)?,
                attestation_data(&a.data),
                vector(a.signature.as_slice()),
                vector(a.committee_bits.as_slice()),
            ]))
        })
        .collect::<Result<_>>()?;
    let deposits: Vec<_> = b
        .deposits
        .iter()
        .map(|d| {
            ensure!(d.proof.len() == 33, "deposit proof must have 33 roots");
            let data = container(&[
                vector(d.data.pubkey.as_slice()),
                d.data.withdrawal_credentials,
                uint(d.data.amount),
                vector(d.data.signature.as_slice()),
            ]);
            Ok(container(&[merkle_root(&root_bytes(&d.proof), 33), data]))
        })
        .collect::<Result<_>>()?;
    let exits: Vec<_> = b
        .voluntary_exits
        .iter()
        .map(|e| {
            container(&[
                container(&[uint(e.message.epoch), uint(e.message.validator_index)]),
                vector(e.signature.as_slice()),
            ])
        })
        .collect();
    let changes: Vec<_> = b
        .bls_to_execution_changes
        .iter()
        .map(|c| {
            container(&[
                container(&[
                    uint(c.message.validator_index),
                    vector(c.message.from_bls_pubkey.as_slice()),
                    vector(c.message.to_execution_address.as_slice()),
                ]),
                vector(c.signature.as_slice()),
            ])
        })
        .collect();
    let commitments: Vec<_> = b
        .blob_kzg_commitments
        .iter()
        .map(|c| vector(c.as_slice()))
        .collect();
    let requests = &b.execution_requests;
    let deposit_requests: Vec<_> = requests
        .deposits
        .iter()
        .map(|d| {
            container(&[
                vector(d.pubkey.as_slice()),
                d.withdrawal_credentials,
                uint(d.amount),
                vector(d.signature.as_slice()),
                uint(d.index),
            ])
        })
        .collect();
    let withdrawals: Vec<_> = requests
        .withdrawals
        .iter()
        .map(|w| {
            container(&[
                vector(w.source_address.as_slice()),
                vector(w.validator_pubkey.as_slice()),
                uint(w.amount),
            ])
        })
        .collect();
    let consolidations: Vec<_> = requests
        .consolidations
        .iter()
        .map(|c| {
            container(&[
                vector(c.source_address.as_slice()),
                vector(c.source_pubkey.as_slice()),
                vector(c.target_pubkey.as_slice()),
            ])
        })
        .collect();
    ensure!(
        b.sync_aggregate.sync_committee_bits.len() == 64,
        "wrong sync committee bitvector length"
    );
    let body = container(&[
        vector(b.randao_reveal.as_slice()),
        container(&[
            b.eth1_data.deposit_root,
            uint(b.eth1_data.deposit_count),
            b.eth1_data.block_hash,
        ]),
        b.graffiti,
        list(&proposer_slashings, 16)?,
        list(&attester_slashings, 1)?,
        list(&attestations, 8)?,
        list(&deposits, 16)?,
        list(&exits, 16)?,
        container(&[
            vector(b.sync_aggregate.sync_committee_bits.as_slice()),
            vector(b.sync_aggregate.sync_committee_signature.as_slice()),
        ]),
        execution(payload)?,
        list(&changes, 16)?,
        list(&commitments, 4096)?,
        container(&[
            list(&deposit_requests, MAX_DEPOSIT_RECEIPTS_PER_PAYLOAD)?,
            list(&withdrawals, MAX_WITHDRAWAL_REQUESTS_PER_BLOCK)?,
            list(&consolidations, MAX_CONSOLIDATION_REQUESTS_PER_BLOCK)?,
        ]),
    ]);
    Ok(BeaconBlockHeader {
        slot: block.slot,
        proposer_index: block.proposer_index,
        parent_root: block.parent_root,
        state_root: block.state_root,
        body_root: body,
    })
}
