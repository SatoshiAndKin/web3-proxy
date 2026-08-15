use alloy::providers::{ProviderBuilder, RootProvider, WsConnect};
use url::Url;

use crate::errors::Web3ProxyResult;

pub type AlloyWsProvider = RootProvider;
pub type AlloyHttpProvider = RootProvider;

pub fn connect_http(url: Url) -> Web3ProxyResult<AlloyHttpProvider> {
    if !url.scheme().starts_with("http") {
        return Err(anyhow::anyhow!("only HTTP servers are supported: {url}").into());
    }

    Ok(ProviderBuilder::default().connect_http(url))
}

pub async fn connect_ws(url: Url) -> Web3ProxyResult<AlloyWsProvider> {
    if !url.scheme().starts_with("ws") {
        return Err(anyhow::anyhow!("only WebSocket servers are supported: {url}").into());
    }

    let provider = ProviderBuilder::default()
        .connect_ws(WsConnect::new(url.as_str()))
        .await?;

    Ok(provider)
}
