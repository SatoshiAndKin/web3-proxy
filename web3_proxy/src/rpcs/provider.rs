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

/// A bounded set of query streams and a separate subscription stream share one backend.
/// Alloy owns framing, request IDs, subscriptions, and reconnects on each stream.
pub(super) struct PubSubProviderPool {
    providers: Box<[Arc<RootProvider>]>,
    subscriptions: Arc<RootProvider>,
    active: Box<[AtomicUsize]>,
    next: AtomicUsize,
}

/// Keep the selected stream occupied until the response is consumed or cancelled.
pub(super) struct PubSubQuery<'a> {
    pool: &'a PubSubProviderPool,
    index: usize,
    weight: usize,
}

impl std::ops::Deref for PubSubQuery<'_> {
    type Target = RootProvider;

    fn deref(&self) -> &Self::Target {
        &self.pool.providers[self.index]
    }
}

impl Drop for PubSubQuery<'_> {
    fn drop(&mut self) {
        self.pool.active[self.index].fetch_sub(self.weight, Ordering::Relaxed);
    }
}

impl PubSubProviderPool {
    pub(super) fn new(
        providers: impl IntoIterator<Item = Arc<RootProvider>>,
        subscriptions: Arc<RootProvider>,
    ) -> Self {
        let providers: Box<[_]> = providers.into_iter().collect();
        assert!(!providers.is_empty());
        let active = (0..providers.len()).map(|_| AtomicUsize::new(0)).collect();
        Self {
            providers,
            subscriptions,
            active,
            next: AtomicUsize::new(0),
        }
    }

    pub(super) fn query(&self) -> PubSubQuery<'_> {
        self.query_weighted(1)
    }

    pub(super) fn query_weighted(&self, weight: usize) -> PubSubQuery<'_> {
        // These counters guide routing; they do not publish data or enforce the
        // logical backend's admission budget. ArcSwap publishes the immutable
        // pool. Rotate the starting point so equal-load streams share requests.
        let start = self.next.fetch_add(1, Ordering::Relaxed) % self.active.len();
        let mut index = start;
        let mut least = self.active[index].load(Ordering::Relaxed);
        for offset in 1..self.active.len() {
            let candidate = (start + offset) % self.active.len();
            let load = self.active[candidate].load(Ordering::Relaxed);
            if load < least {
                index = candidate;
                least = load;
            }
        }
        self.active[index].fetch_add(weight, Ordering::Relaxed);
        PubSubQuery {
            pool: self,
            index,
            weight,
        }
    }

    pub(super) fn subscription(&self) -> Arc<RootProvider> {
        self.subscriptions.clone()
    }

    /// A terminal service failure retires the entire published generation.
    pub(super) async fn wait_closed(&self) -> Web3ProxyResult<()> {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            for provider in self
                .providers
                .iter()
                .chain(std::iter::once(&self.subscriptions))
            {
                let mut transport = provider.client().transport().clone();
                std::future::poll_fn(|cx| tower_service::Service::poll_ready(&mut transport, cx))
                    .await?;
            }
        }
    }

    pub(super) fn in_flight(&self) -> Vec<usize> {
        self.active
            .iter()
            .map(|active| active.load(Ordering::Relaxed))
            .collect()
    }

    /// Number of query streams; the subscription stream is separate.
    pub(super) fn size(&self) -> usize {
        self.providers.len()
    }
}

pub(super) async fn connect_ipc_pool(
    path: PathBuf,
    connections: usize,
    native_batches: bool,
) -> Web3ProxyResult<PubSubProviderPool> {
    if !(1..=crate::config::MAX_IPC_QUERY_CONNECTIONS).contains(&connections) {
        return Err(anyhow::anyhow!("IPC query pool size is outside its configured bounds").into());
    }
    let paths = vec![path.clone(); connections];
    let queries = futures::future::try_join_all(
        paths
            .into_iter()
            .map(|path| connect_ipc_with_batches(path, native_batches)),
    );
    let (providers, subscriptions) = tokio::try_join!(queries, connect_ipc(path))?;
    Ok(PubSubProviderPool::new(
        providers.into_iter().map(Arc::new),
        Arc::new(subscriptions),
    ))
}

pub(super) async fn connect_query_ws_pool(
    url: Url,
    native_batches: bool,
) -> Web3ProxyResult<PubSubProviderPool> {
    let (first, second, subscriptions) = tokio::try_join!(
        connect_query_ws(url.clone(), native_batches),
        connect_query_ws(url.clone(), native_batches),
        connect_ws(url)
    )?;
    Ok(PubSubProviderPool::new(
        [Arc::new(first), Arc::new(second)],
        Arc::new(subscriptions),
    ))
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
    connect_ipc_with_batches(path, false).await
}

async fn connect_ipc_with_batches(
    path: PathBuf,
    native_batches: bool,
) -> Web3ProxyResult<AlloyIpcProvider> {
    Ok(ProviderBuilder::default()
        .connect_ipc(IpcConnect::new(path).with_wire_batches(native_batches))
        .await?)
}

pub async fn connect_ws(url: Url) -> Web3ProxyResult<AlloyWsProvider> {
    connect_ws_configured(url, WebSocketConfig::default(), false).await
}

async fn connect_query_ws(url: Url, native_batches: bool) -> Web3ProxyResult<AlloyWsProvider> {
    connect_ws_configured(url, query_ws_config(), native_batches).await
}

async fn connect_ws_configured(
    url: Url,
    config: WebSocketConfig,
    native_batches: bool,
) -> Web3ProxyResult<AlloyWsProvider> {
    if !url.scheme().starts_with("ws") {
        return Err(anyhow::anyhow!("only WebSocket servers are supported: {url}").into());
    }

    let provider = ProviderBuilder::default()
        .connect_ws(
            WsConnect::new(url.as_str())
                .with_config(config)
                .with_wire_batches(native_batches),
        )
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
    async fn terminal_failure_of_any_query_or_subscription_retires_pool() {
        use alloy::{pubsub::PubSubFrontend, rpc::client::RpcClient};
        for connections in [1, 2, 4, 8] {
            for failed in 0..=connections {
                let mut receivers = Vec::new();
                let mut provider = || {
                    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                    receivers.push(rx);
                    Arc::new(RootProvider::new(RpcClient::new(
                        PubSubFrontend::new(tx),
                        true,
                    )))
                };
                let providers = (0..connections).map(|_| provider()).collect::<Vec<_>>();
                let pool = Arc::new(PubSubProviderPool::new(providers, provider()));
                let watched = pool.clone();
                let monitor = tokio::spawn(async move { watched.wait_closed().await });
                tokio::time::sleep(Duration::from_millis(20)).await;
                assert!(!monitor.is_finished());
                drop(receivers.remove(failed));
                assert!(tokio::time::timeout(Duration::from_secs(2), monitor)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_err());
                assert!(
                    receivers.iter().all(|receiver| !receiver.is_closed()),
                    "unrelated streams stayed live"
                );
            }
        }
    }

    #[tokio::test]
    async fn query_load_returns_to_zero_on_completion_and_cancellation() {
        use alloy::{pubsub::PubSubFrontend, rpc::client::RpcClient};
        let (tx, _receiver) = tokio::sync::mpsc::unbounded_channel();
        let provider = Arc::new(RootProvider::new(RpcClient::new(
            PubSubFrontend::new(tx),
            true,
        )));
        let pool = Arc::new(PubSubProviderPool::new(
            [provider.clone(), provider.clone()],
            provider,
        ));
        let held = pool.query();
        for _ in 0..8 {
            let fast = pool.query();
            assert_ne!(
                fast.index, held.index,
                "an idle stream must win over an occupied stream"
            );
        }
        assert_eq!(pool.in_flight().iter().sum::<usize>(), 1);
        drop(held);
        assert_eq!(pool.in_flight(), [0, 0]);
        let (started, waiting) = tokio::sync::oneshot::channel();
        let busy_pool = pool.clone();
        let task = tokio::spawn(async move {
            let _query = busy_pool.query();
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        waiting.await.unwrap();
        assert_eq!(pool.in_flight().iter().sum::<usize>(), 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(pool.in_flight(), [0, 0]);
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
        let provider = connect_query_ws(format!("ws://{address}").parse().unwrap(), false)
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

#[cfg(test)]
mod hol_tests {
    use super::*;
    use crate::rpcs::test_support;
    use alloy::primitives::B256;
    use futures::{SinkExt, StreamExt};
    use serde_json::json;
    use tokio::{
        net::TcpListener,
        sync::{mpsc, watch, Mutex, Semaphore},
        time::{timeout, Duration},
    };
    use tokio_tungstenite::tungstenite::{
        protocol::frame::{
            coding::{Data, OpCode},
            Frame,
        },
        Message,
    };

    #[tokio::test]
    async fn websocket_pool_keeps_fast_calls_and_subscriptions_off_a_partial_message() {
        test_support::with_cleanup(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let release = Arc::new(Semaphore::new(0));
            let server_release = release.clone();
            let (started, mut partial) = mpsc::unbounded_channel();
            let (pulse, updates) = watch::channel(false);
            let server = test_support::spawn(async move {
                let mut handlers = tokio::task::JoinSet::new();
                for _ in 0..3 {
                    let (socket, _) = listener.accept().await.unwrap();
                    let socket = tokio_tungstenite::accept_async(socket).await.unwrap();
                    let release = server_release.clone();
                    let started = started.clone();
                    let mut updates = updates.clone();
                    handlers.spawn(async move {
                        let (writer, mut reader) = socket.split();
                        let writer = Arc::new(Mutex::new(writer));
                        let mut subscribed = false;
                        let mut responses = futures::stream::FuturesUnordered::new();
                        loop {
                            tokio::select! {
                                message = reader.next() => {
                                    let Some(Ok(Message::Text(text))) = message else { break; };
                                    let request: serde_json::Value = serde_json::from_str(&text).unwrap();
                                    if request["method"] == "eth_subscribe" {
                                        assert!(!subscribed);
                                        subscribed = true;
                                        writer.lock().await.send(Message::Text(json!({"jsonrpc":"2.0","id":request["id"],"result":"0x1"}).to_string().into())).await.unwrap();
                                    } else {
                                        let writer = writer.clone();
                                        let release = release.clone();
                                        let started = started.clone();
                                        responses.push(async move {
                                            let value = request["params"][0].as_u64().unwrap();
                                            let mut writer = writer.lock().await;
                                            if value == 0 {
                                                let prefix = format!("{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":", request["id"]);
                                                writer.send(Message::Frame(Frame::message(prefix.into_bytes(), OpCode::Data(Data::Text), false))).await.unwrap();
                                                started.send(()).unwrap();
                                                release.acquire().await.unwrap().forget();
                                                writer.send(Message::Frame(Frame::message(b"0}".to_vec(), OpCode::Data(Data::Continue), true))).await.unwrap();
                                            } else {
                                                writer.send(Message::Text(json!({"jsonrpc":"2.0","id":request["id"],"result":value}).to_string().into())).await.unwrap();
                                            }
                                        });
                                    }
                                }
                                _ = updates.changed(), if subscribed => {
                                    writer.lock().await.send(Message::Text(json!({"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0x1","result":B256::repeat_byte(0x77)}}).to_string().into())).await.unwrap();
                                }
                                Some(()) = responses.next(), if !responses.is_empty() => {}
                            }
                        }
                    });
                }
                while let Some(result) = handlers.join_next().await { result.unwrap(); }
            });
            let pool = Arc::new(connect_query_ws_pool(format!("ws://{address}").parse().unwrap(), false).await.unwrap());
            let provider = pool.subscription();
            let subscription = provider.subscribe_pending_transactions().await.unwrap();
            let mut events = subscription.into_stream();
            let slow_pool = pool.clone();
            let slow = test_support::spawn(async move {
                let query = slow_pool.query();
                let params = serde_json::value::RawValue::from_string("[0]".into()).unwrap();
                query.raw_request_dyn("eth_call".into(), &params).await.unwrap()
            });
            timeout(Duration::from_secs(2), partial.recv()).await.unwrap().unwrap();
            pulse.send_replace(true);
            assert_eq!(timeout(Duration::from_secs(2), events.next()).await.expect("partial query frames must not block subscription events"), Some(B256::repeat_byte(0x77)));
            timeout(Duration::from_secs(2), async {
                for value in 1..=8 {
                    let query = pool.query();
                    let params = serde_json::value::RawValue::from_string(format!("[{value}]")).unwrap();
                    let response = query.raw_request_dyn("eth_call".into(), &params).await.unwrap();
                    assert_eq!(response.get(), value.to_string());
                }
            }).await.expect("fast calls must avoid a stream with a partial WebSocket message");
            assert!(!slow.is_finished());
            release.add_permits(1);
            assert_eq!(timeout(Duration::from_secs(2), slow).await.unwrap().unwrap().get(), "0");
            server.abort();
            assert!(server.await.unwrap_err().is_cancelled());
            drop(events);
        }).await;
    }
}
