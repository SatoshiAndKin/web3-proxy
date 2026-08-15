//! Poll blocks from an HTTP RPC endpoint.

use web3_proxy::prelude::alloy_provider::{Provider, ProviderBuilder, RootProvider};
use web3_proxy::prelude::anyhow;
use web3_proxy::prelude::fdlimit;
use web3_proxy::prelude::futures::{self, StreamExt};
use web3_proxy::prelude::tokio;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fdlimit::raise_fd_limit()?;

    // erigon does not support most filters
    // let url = "http://10.11.12.16:8545";
    // geth
    let url = "http://10.11.12.16:8545";

    println!("Watching blocks from {:?}", url);

    let provider: RootProvider = ProviderBuilder::default().connect_http(url.parse()?);

    let poller = provider.watch_blocks().await?;
    let mut stream = poller.into_stream().flat_map(futures::stream::iter);
    while let Some(block_hash) = stream.next().await {
        let block = provider.get_block_by_hash(block_hash).await?.unwrap();
        println!(
            "{:?} = Ts: {:?}, block number: {}",
            block.header.hash, block.header.timestamp, block.header.number,
        );
    }

    Ok(())
}
