use alloy::providers::{IpcConnect, Provider, ProviderBuilder, RootProvider, WsConnect};
use alloy::transports::ws::WebSocketConfig;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use url::Url;

use crate::errors::Web3ProxyResult;

pub type AlloyWsProvider = RootProvider;
pub type AlloyHttpProvider = RootProvider;
pub type AlloyIpcProvider = RootProvider;

/// Two physical streams share one logical backend and its admission budget.
/// Alloy owns framing, request IDs, subscriptions, and reconnects on each stream.
pub(super) struct IpcProviderPool {
    providers: [Arc<AlloyIpcProvider>; 2],
    next: AtomicUsize,
}

impl IpcProviderPool {
    pub(super) fn query(&self) -> &AlloyIpcProvider {
        // This counter only selects an immutable, already-published provider.
        // ArcSwap publishes the pool; the selector establishes no happens-before
        // relationship. Wrapping is harmless because the pool has two entries.
        let index = self.next.fetch_add(1, Ordering::Relaxed) & 1;
        &self.providers[index]
    }

    pub(super) fn subscription(&self) -> Arc<AlloyIpcProvider> {
        self.providers[0].clone()
    }

    /// A failed query-only service must retire this generation too. A temporary
    /// reconnect keeps the frontend ready; Alloy retains its own retry policy.
    pub(super) async fn wait_closed(&self) -> Web3ProxyResult<()> {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            for provider in &self.providers {
                let mut transport = provider.client().transport().clone();
                std::future::poll_fn(|cx| tower_service::Service::poll_ready(&mut transport, cx))
                    .await?;
            }
        }
    }

    pub(super) fn size(&self) -> usize {
        self.providers.len()
    }
}

pub(super) async fn connect_ipc_pool(path: PathBuf) -> Web3ProxyResult<IpcProviderPool> {
    let (first, second) = tokio::try_join!(connect_ipc(path.clone()), connect_ipc(path))?;
    Ok(IpcProviderPool {
        providers: [Arc::new(first), Arc::new(second)],
        next: AtomicUsize::new(0),
    })
}

// Match the owned nodes' 1024-MiB RPC response allowance. Query responses can
// exceed the default 16-MiB frame size; head-only consumers retain their defaults.
const QUERY_WS_RESPONSE_BYTES: usize = 1024 * 1024 * 1024;

fn query_ws_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(QUERY_WS_RESPONSE_BYTES))
        .max_frame_size(Some(QUERY_WS_RESPONSE_BYTES))
}

pub fn connect_http(url: Url) -> Web3ProxyResult<AlloyHttpProvider> {
    if !url.scheme().starts_with("http") {
        return Err(anyhow::anyhow!("only HTTP servers are supported: {url}").into());
    }

    Ok(ProviderBuilder::default().connect_http(url))
}

pub(super) async fn connect_ipc(path: PathBuf) -> Web3ProxyResult<AlloyIpcProvider> {
    Ok(ProviderBuilder::default()
        .connect_ipc(IpcConnect::new(path))
        .await?)
}

pub async fn connect_ws(url: Url) -> Web3ProxyResult<AlloyWsProvider> {
    connect_ws_configured(url, WebSocketConfig::default()).await
}

pub(super) async fn connect_query_ws(url: Url) -> Web3ProxyResult<AlloyWsProvider> {
    connect_ws_configured(url, query_ws_config()).await
}

async fn connect_ws_configured(
    url: Url,
    config: WebSocketConfig,
) -> Web3ProxyResult<AlloyWsProvider> {
    if !url.scheme().starts_with("ws") {
        return Err(anyhow::anyhow!("only WebSocket servers are supported: {url}").into());
    }

    let provider = ProviderBuilder::default()
        .connect_ws(WsConnect::new(url.as_str()).with_config(config))
        .await?;

    Ok(provider)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::providers::Provider;
    use axum::{
        extract::{ws::Message, State, WebSocketUpgrade},
        routing::get,
        Router,
    };
    use std::{sync::Arc, time::Duration};
    use tokio::net::TcpListener;
    use tokio_util::{sync::CancellationToken, task::TaskTracker};

    #[tokio::test]
    async fn a_closed_query_service_retires_the_pool_with_a_live_primary() {
        use alloy::{pubsub::PubSubFrontend, rpc::client::RpcClient};
        let (first_tx, first_rx) = tokio::sync::mpsc::unbounded_channel();
        let (second_tx, second_rx) = tokio::sync::mpsc::unbounded_channel();
        let pool = Arc::new(IpcProviderPool {
            providers: [
                Arc::new(RootProvider::new(RpcClient::new(
                    PubSubFrontend::new(first_tx),
                    true,
                ))),
                Arc::new(RootProvider::new(RpcClient::new(
                    PubSubFrontend::new(second_tx),
                    true,
                ))),
            ],
            next: AtomicUsize::new(0),
        });
        let watched = pool.clone();
        let monitor = tokio::spawn(async move { watched.wait_closed().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !monitor.is_finished(),
            "healthy services must keep the generation alive"
        );
        drop(second_rx);
        assert!(tokio::time::timeout(Duration::from_secs(2), monitor)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        assert!(
            !first_rx.is_closed(),
            "the primary stayed alive throughout the failure"
        );
    }

    #[test]
    fn query_websocket_retains_a_finite_response_budget() {
        let config = query_ws_config();
        assert_eq!(config.max_message_size, Some(1024 * 1024 * 1024));
        assert_eq!(config.max_frame_size, config.max_message_size);
    }

    #[tokio::test]
    async fn query_websocket_accepts_a_response_larger_than_the_default_frame() {
        let stop = CancellationToken::new();
        let tasks = TaskTracker::new();
        let payload: Arc<str> = format!("0x{}", "a".repeat(17 * 1024 * 1024)).into();
        let payload_len = payload.len();
        let router = Router::new()
            .route(
                "/",
                get(
                    |upgrade: WebSocketUpgrade,
                     State((payload, stop, tasks)): State<(
                        Arc<str>,
                        CancellationToken,
                        TaskTracker,
                    )>| async move {
                        upgrade.on_upgrade(move |mut socket| async move {
                            tasks.spawn(async move {
                                tokio::select! {
                                    _ = stop.cancelled() => {}
                                    _ = async {
                                        let request = socket.recv().await.unwrap().unwrap();
                                        let request: serde_json::Value =
                                            serde_json::from_str(&request.into_text().unwrap()).unwrap();
                                        let response = serde_json::json!({
                                            "jsonrpc": "2.0", "id": request["id"],
                                            "result": payload.as_ref()
                                        });
                                        socket.send(Message::Text(response.to_string().into())).await.unwrap();
                                        stop.cancelled().await;
                                    } => {}
                                }
                            });
                        })
                    },
                ),
            )
            .with_state((payload, stop.clone(), tasks.clone()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = stop.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        let provider = connect_query_ws(format!("ws://{address}").parse().unwrap())
            .await
            .unwrap();
        let params = serde_json::value::RawValue::from_string("[]".into()).unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            provider.raw_request_dyn("eth_getLogs".into(), &params),
        )
        .await;
        drop(provider);
        stop.cancel();
        server.await.unwrap();
        tasks.close();
        tasks.wait().await;
        let result = result.unwrap().unwrap();
        let bytes = result.get().as_bytes();
        assert_eq!(bytes.len(), payload_len + 2);
        assert!(bytes.starts_with(b"\"0x"));
        assert!(bytes.ends_with(b"\""));
        assert!(bytes[3..bytes.len() - 1].iter().all(|byte| *byte == b'a'));
    }
}
