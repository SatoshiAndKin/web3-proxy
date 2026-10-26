//! Read-only compatibility check against existing nodes. Starts no clients.
use alloy::primitives::{B256, U64};
use alloy_rpc_types_beacon::header::HeaderResponse;
use anyhow::{ensure, Context, Result};
use serde::de::DeserializeOwned;
use web3_proxy::block_relay::{
    payload::RelayPayload,
    transport::{self, Rpc},
};

async fn get<T: DeserializeOwned>(client: &reqwest::Client, base: &str, path: &str) -> Result<T> {
    let response = client
        .get(format!("{}{path}", base.trim_end_matches('/')))
        .timeout(transport::READ_TIMEOUT)
        .send()
        .await?;
    Ok(sonic_rs::from_slice(&transport::body(response).await?)?)
}
#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    ensure!(
        args.len() >= 2,
        "usage: block_relay_check BEACON_URL RPC_URL [RPC_URL ...]"
    );
    let client = transport::client()?;
    let header: HeaderResponse = get(&client, &args[0], "/eth/v1/beacon/headers/head").await?;
    let response = client
        .get(format!(
            "{}/eth/v2/beacon/blocks/{}",
            args[0].trim_end_matches('/'),
            header.data.root
        ))
        .timeout(transport::READ_TIMEOUT)
        .send()
        .await?;
    let bytes = transport::body(response).await?;
    let payload = RelayPayload::decode(&bytes, header.data.root)?;
    println!(
        "Read Beacon block {}, execution hash {}, slot {}, Engine body {} bytes",
        payload.beacon_root,
        payload.hash,
        payload.slot,
        payload.body.len()
    );
    #[derive(serde::Deserialize)]
    struct Block {
        hash: B256,
        number: U64,
    }
    for (index, url) in args[1..].iter().enumerate() {
        let rpc = Rpc::new(url, None)?;
        let version: String = rpc.call("web3_clientVersion", ()).await?;
        let block: Option<Block> = rpc
            .call("eth_getBlockByHash", (payload.hash, false))
            .await?;
        let block = block.context("execution node does not return the block")?;
        ensure!(
            block.hash == payload.hash && block.number.to::<u64>() == payload.number,
            "execution response mismatch"
        );
        println!(
            "Target {}: {} returns the matching block",
            index + 1,
            version
        );
    }
    Ok(())
}
