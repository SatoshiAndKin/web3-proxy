//! Subscribe to blocks from a WebSocket RPC endpoint.

use web3_proxy::prelude::alloy::providers::{Provider, ProviderBuilder, RootProvider, WsConnect};
use web3_proxy::prelude::anyhow;
use web3_proxy::prelude::fdlimit;
use web3_proxy::prelude::futures::StreamExt;
use web3_proxy::prelude::tokio;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // install global collector configured based on RUST_LOG env var.
    fdlimit::raise_fd_limit()?;

    // erigon
    let url = "ws://10.11.12.16:8548";
    // geth
    // let url = "ws://10.11.12.16:8546";

    println!("Subscribing to blocks from {}", url);

    let provider: RootProvider = ProviderBuilder::default()
        .connect_ws(WsConnect::new(url))
        .await?;

    let subscription = provider.subscribe_blocks().await?;
    let mut stream = subscription.into_stream();
    while let Some(header) = stream.next().await {
        println!(
            "{:?} = Ts: {:?}, block number: {}",
            header.hash, header.timestamp, header.number,
        );
    }

    Ok(())
}
