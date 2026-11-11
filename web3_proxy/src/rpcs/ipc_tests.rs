//! Exercise IPC framing, multiplexing and recovery through real Unix sockets.
use super::{
    one::Web3Rpc,
    provider::{connect_ipc, connect_ipc_pool},
    test_support,
};
use crate::{errors::Web3ProxyError, jsonrpc::JsonRpcResultData};
use alloy::{primitives::B256, providers::Provider};
use futures::StreamExt;
use latency::{PeakEwmaLatency, RollingQuantileLatency};
use serde_json::{json, Value};
use sonic_rs::JsonValueTrait;
use std::{future::Future, path::Path, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::watch,
    time::{timeout, Duration, Instant},
};

async fn read_request<S: tokio::io::AsyncRead + Unpin>(
    socket: &mut S,
    buffer: &mut Vec<u8>,
) -> Value {
    loop {
        let mut values = serde_json::Deserializer::from_slice(buffer).into_iter::<Value>();
        if let Some(Ok(value)) = values.next() {
            let end = values.byte_offset();
            buffer.drain(..end);
            return value;
        }
        assert!(
            buffer.len() < 64 * 1024,
            "fixture request exceeded its limit"
        );
        assert_ne!(socket.read_buf(buffer).await.unwrap(), 0);
    }
}

async fn rpc(path: &Path) -> Arc<Web3Rpc> {
    rpc_with_connections(path, 2, false).await
}

async fn rpc_with_connections(
    path: &Path,
    connections: usize,
    ipc_native_batches: bool,
) -> Arc<Web3Rpc> {
    let (hard_limit_until, _) = watch::channel(Instant::now());
    let rpc = Web3Rpc {
        name: "local-ipc".into(),
        ipc_path: Some(path.into()),
        ipc_native_batches,
        // A configured HTTP alternative must not override IPC.
        http_url: Some("http://127.0.0.1:1".parse().unwrap()),
        http_client: Some(reqwest::Client::new()),
        hard_limit_until: Some(hard_limit_until),
        peak_latency: Some(PeakEwmaLatency::spawn(
            Duration::from_secs(15),
            100,
            Duration::from_secs(1),
        )),
        median_latency: Some(RollingQuantileLatency::spawn_median(100).await),
        ..Default::default()
    };
    rpc.ipc_provider.store(Some(Arc::new(
        connect_ipc_pool(path.into(), connections, ipc_native_batches)
            .await
            .unwrap(),
    )));
    Arc::new(rpc)
}

async fn serve_pool<F, Fut>(listener: UnixListener, handler: F)
where
    F: Fn(UnixStream) -> Fut + Send,
    Fut: Future<Output = ()> + Send,
{
    serve_connections(listener, 2, handler).await;
}

async fn serve_connections<F, Fut>(listener: UnixListener, connections: usize, handler: F)
where
    F: Fn(UnixStream) -> Fut + Send,
    Fut: Future<Output = ()> + Send,
{
    let mut sockets = Vec::new();
    for _ in 0..=connections {
        sockets.push(listener.accept().await.unwrap().0);
    }
    futures::future::join_all(sockets.into_iter().map(handler)).await;
}

async fn call<R: JsonRpcResultData>(rpc: &Arc<Web3Rpc>, value: u64) -> R {
    rpc.internal_request(
        "eth_call".into(),
        &json!([value]),
        None,
        Some(Duration::from_secs(10)),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn ipc_pool_preserves_fragmented_large_and_out_of_order_replies() {
    test_support::with_cleanup(async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("node.ipc");
        let listener = UnixListener::bind(&path).unwrap();
        let payload: Arc<str> = format!("0x{}", "a".repeat(17 * 1024 * 1024)).into();
        let expected = payload.clone();
        let server = test_support::spawn(serve_pool(listener, move |mut socket| {
            let payload = payload.clone();
            async move {
                let mut buffer = Vec::new();
                let mut first = read_request(&mut socket, &mut buffer).await;
                if first["params"][0] == 0 {
                    let response =
                        json!({"jsonrpc":"2.0", "id":first["id"], "result":payload.as_ref()});
                    for chunk in response.to_string().as_bytes().chunks(8192) {
                        socket.write_all(chunk).await.unwrap();
                        tokio::task::yield_now().await;
                    }
                    first = read_request(&mut socket, &mut buffer).await;
                }
                let second = read_request(&mut socket, &mut buffer).await;
                assert_ne!(first["id"], second["id"]);
                // Each stream returns two concatenated replies in reverse order.
                // IDs may overlap across streams; their values must stay isolated.
                let responses = [second, first].map(|request| {
                    json!({"jsonrpc":"2.0", "id":request["id"], "result":request["params"][0]})
                        .to_string()
                });
                socket
                    .write_all(responses.concat().as_bytes())
                    .await
                    .unwrap();
                let mut byte = [0];
                let _ = socket.read(&mut byte).await;
            }
        }));
        let rpc = rpc(&path).await;
        assert!(
            !rpc.supports_batch(),
            "IPC must take precedence for client batches"
        );
        let result: String = call(&rpc, 0).await;
        assert_eq!(result, expected.as_ref());
        let values = tokio::join!(
            call::<u64>(&rpc, 11),
            call::<u64>(&rpc, 22),
            call::<u64>(&rpc, 33),
            call::<u64>(&rpc, 44)
        );
        assert_eq!(values, (11, 22, 33, 44));
        assert_eq!(
            rpc.active_requests
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        drop(rpc);
    })
    .await;
}

#[tokio::test]
async fn ipc_preserves_reverts_and_does_not_mix_a_late_reply_with_the_next_call() {
    test_support::with_cleanup(async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("node.ipc");
        let listener = UnixListener::bind(&path).unwrap();
        let server = test_support::spawn(serve_pool(listener, |mut socket| async move {
            let mut buffer = Vec::new();
            loop {
                let request = read_request(&mut socket, &mut buffer).await;
                let value = request["params"][0].as_u64().unwrap();
                if value == 1 {
                    // This request times out. Return the next call on the same
                    // stream first, then the cancelled call's late response.
                    let next = read_request(&mut socket, &mut buffer).await;
                    let replies = [
                        json!({"jsonrpc":"2.0", "id":next["id"], "result":40 + next["params"][0].as_u64().unwrap()}).to_string(),
                        json!({"jsonrpc":"2.0", "id":request["id"], "result":99}).to_string(),
                    ];
                    socket.write_all(replies.concat().as_bytes()).await.unwrap();
                } else {
                    let response = if value == 0 {
                        json!({"jsonrpc":"2.0", "id":request["id"], "error":{"code":3,"message":"execution reverted","data":"0x1234"}})
                    } else {
                        json!({"jsonrpc":"2.0", "id":request["id"], "result":40 + value})
                    };
                    socket.write_all(response.to_string().as_bytes()).await.unwrap();
                }
            }
        }));
        let rpc = rpc(&path).await;
        let revert = rpc.internal_request::<_, u64>("eth_call".into(), &json!([0]), None, Some(Duration::from_secs(2))).await.unwrap_err();
        assert!(matches!(revert, Web3ProxyError::JsonRpcErrorData(error)
            if error.code == 3 && error.message == "execution reverted"
                && error.data.as_ref().is_some_and(|data| data.as_str() == Some("0x1234"))));
        let late = rpc.internal_request::<_, u64>("eth_call".into(), &json!([1]), None, Some(Duration::from_millis(50))).await;
        assert!(matches!(late, Err(Web3ProxyError::Timeout(_))));
        assert_eq!(rpc.active_requests.load(std::sync::atomic::Ordering::SeqCst), 0);
        for value in 2..=5 {
            assert_eq!(call::<u64>(&rpc, value).await, 40 + value);
        }
        assert_eq!(rpc.active_requests.load(std::sync::atomic::Ordering::SeqCst), 0);
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        drop(rpc);
    }).await;
}

#[tokio::test]
async fn ipc_subscribes_and_reconnects_after_the_socket_is_replaced() {
    test_support::with_cleanup(async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("node.ipc");
        let listener = UnixListener::bind(&path).unwrap();
        let next_path = path.clone();
        let server = test_support::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = Vec::new();
            let subscription = read_request(&mut socket, &mut buffer).await;
            assert_eq!(subscription["method"], "eth_subscribe");
            socket
                .write_all(
                    json!({"jsonrpc":"2.0", "id":subscription["id"], "result":"0x1"})
                        .to_string()
                        .as_bytes(),
                )
                .await
                .unwrap();
            socket
                .write_all(
                    json!({"jsonrpc":"2.0", "method":"eth_subscription", "params":{
                "subscription":"0x1", "result":B256::repeat_byte(0x44)}})
                    .to_string()
                    .as_bytes(),
                )
                .await
                .unwrap();
            // Replace the inode before closing the first stream. The provider
            // must resolve the pathname again, as it will after a node restart.
            std::fs::remove_file(&next_path).unwrap();
            drop(listener);
            let listener = UnixListener::bind(next_path).unwrap();
            drop(socket);
            let (mut socket, _) = listener.accept().await.unwrap();
            loop {
                let request = read_request(&mut socket, &mut buffer).await;
                let response = if request["method"] == "eth_subscribe" {
                    json!({"jsonrpc":"2.0", "id":request["id"], "result":"0x2"})
                } else {
                    json!({"jsonrpc":"2.0", "id":request["id"], "result":"0x1"})
                };
                socket
                    .write_all(response.to_string().as_bytes())
                    .await
                    .unwrap();
            }
        });
        let provider = connect_ipc(path).await.unwrap();
        let subscription = provider.subscribe_pending_transactions().await.unwrap();
        let mut stream = subscription.into_stream();
        assert_eq!(
            timeout(Duration::from_secs(3), stream.next())
                .await
                .unwrap(),
            Some(B256::repeat_byte(0x44))
        );
        let params = serde_json::value::RawValue::from_string("[]".into()).unwrap();
        let reply = timeout(
            Duration::from_secs(5),
            provider.raw_request_dyn("eth_chainId".into(), &params),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(reply.get(), "\"0x1\"");
        drop(stream);
        drop(provider);
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    })
    .await;
}

#[tokio::test]
async fn ipc_retains_the_configured_public_module_surface() {
    test_support::with_cleanup(async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("node.ipc");
        let listener = UnixListener::bind(&path).unwrap();
        let server = test_support::spawn(serve_pool(listener, |mut socket| async move {
            let mut buffer = Vec::new();
            loop {
                let request = read_request(&mut socket, &mut buffer).await;
                let result = match request["method"].as_str().unwrap() {
                    "rpc_modules" => json!({"eth":"1.0", "rpc":"1.0", "engine":"1.0", "testing":"1.0", "miner":"1.0"}),
                    "eth_call" => json!(7),
                    method => panic!("a private method reached IPC: {method}"),
                };
                socket.write_all(json!({"jsonrpc":"2.0", "id":request["id"], "result":result}).to_string().as_bytes()).await.unwrap();
            }
        }));
        let mut rpc = rpc(&path).await;
        Arc::get_mut(&mut rpc).unwrap().ipc_allowed_modules = Some(vec!["eth".into(), "rpc".into()]);
        for method in ["engine_getClientVersionV1", "testing_buildBlockV1", "miner_setGasPrice"] {
            let result = rpc.internal_request::<_, Value>(method.into(), &json!([]), None, Some(Duration::from_secs(2))).await;
            assert!(matches!(result, Err(Web3ProxyError::MethodNotFound(_))));
        }
        assert_eq!(rpc.total_requests.load(std::sync::atomic::Ordering::Relaxed), 0);
        // Exercise the module filter and ordinary RPC on both physical streams.
        for _ in 0..2 {
            let modules: std::collections::BTreeMap<String, String> = rpc.internal_request("rpc_modules".into(), &json!([]), None, Some(Duration::from_secs(2))).await.unwrap();
            assert_eq!(modules.keys().map(String::as_str).collect::<Vec<_>>(), ["eth", "rpc"]);
        }
        assert_eq!(tokio::join!(call::<u64>(&rpc, 0), call::<u64>(&rpc, 1)), (7, 7));
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        drop(rpc);
    }).await;
}

#[tokio::test]
async fn ipc_only_configuration_starts_healthy_and_publishes_heads() {
    test_support::with_cleanup(async {
        use super::blockchain::{new_block_response_cache, HeadObservationPublisher};
        use crate::config::Web3RpcConfig;
        use alloy::rpc::types::Header;
        use tokio::sync::mpsc;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("node.ipc");
        let listener = UnixListener::bind(&path).unwrap();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        let block = |number, hash, parent_hash| {
            let header = Header {
                hash,
                inner: alloy::consensus::Header { number, parent_hash, timestamp, ..Default::default() },
                ..Default::default()
            };
            let mut value = serde_json::to_value(header).unwrap();
            value["transactions"] = json!([]);
            value["uncles"] = json!([]);
            value
        };
        let initial = block(42, B256::repeat_byte(0x42), B256::repeat_byte(0x41));
        let pushed = block(43, B256::repeat_byte(0x43), B256::repeat_byte(0x42));
        let subscriptions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server_subscriptions = subscriptions.clone();
        let initial = Arc::new(initial);
        let pushed = Arc::new(pushed);
        let server = test_support::spawn(serve_pool(listener, move |mut socket| {
            let initial = initial.clone();
            let pushed = pushed.clone();
            let subscriptions = server_subscriptions.clone();
            async move {
            let mut buffer = Vec::new();
            loop {
                let request = read_request(&mut socket, &mut buffer).await;
                let response = match request["method"].as_str().unwrap() {
                    "web3_clientVersion" => json!({"result":"ipc-startup-fixture"}),
                    "eth_chainId" => json!({"result":"0x1"}),
                    "eth_blockNumber" => json!({"result":"0x2a"}),
                    "eth_getBlockByNumber" => json!({"result":initial.as_ref()}),
                    "eth_getBlockByHash" => json!({"result":if request["params"][0] == pushed["hash"] { pushed.as_ref() } else { initial.as_ref() }}),
                    "eth_call" => json!({"result":request["params"][0]}),
                    "eth_subscribe" => {
                        assert_eq!(request["params"], json!(["newHeads"]));
                        assert_eq!(subscriptions.fetch_add(1, std::sync::atomic::Ordering::SeqCst), 0);
                        json!({"result":"0x1"})
                    }
                    _ => json!({"error":{"code":-32601,"message":"method not found"}}),
                };
                let mut response = response;
                response["jsonrpc"] = json!("2.0");
                response["id"] = request["id"].clone();
                socket.write_all(response.to_string().as_bytes()).await.unwrap();
                if request["method"] == "eth_subscribe" {
                    socket.write_all(json!({"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0x1","result":pushed.as_ref()}}).to_string().as_bytes()).await.unwrap();
                }
            }
            }
        }));
        let config: Web3RpcConfig = sonic_rs::from_str(&json!({
            "ipc_path":path,
            "ipc_allowed_modules":["debug","eth","net","rpc","txpool","web3"],
            "block_data_limit":"archive", "log_data_limit":"archive"
        }).to_string()).unwrap();
        assert!(config.http_url.is_none() && config.ws_url.is_none());
        let (sender, heads) = mpsc::unbounded_channel();
        let heads = Arc::new(tokio::sync::Mutex::new(heads));
        let (rpc, task) = Web3Rpc::spawn(
            config, "ipc-only-startup".into(), 1, None, Duration::from_secs(12),
            moka::future::Cache::new(16), moka::future::Cache::new(16),
            new_block_response_cache(1_000_000), Some(HeadObservationPublisher::new(sender)),
            None, None, Duration::from_secs(60),
        ).await.expect("an IPC-only backend must enter the real startup path");
        let cleanup_rpc = rpc.clone();
        let cleanup_heads = heads.clone();
        test_support::on_cleanup(async move {
            cleanup_rpc.disconnect_watch.as_ref().unwrap().send_replace(true);
            server.abort();
            let _ = server.await;
            timeout(Duration::from_secs(12), task).await.expect("startup tasks must stop").unwrap().unwrap();
            drop(cleanup_heads);
        });
        let observation = timeout(Duration::from_secs(5), async {
            loop {
                let head = heads.lock().await.recv().await.expect("head publisher stays open");
                if head.block.as_ref().is_some_and(|block| block.number().to::<u64>() == 43) {
                    break head;
                }
            }
        }).await.expect("the IPC subscription must publish the pushed head");
        assert!(Arc::ptr_eq(&observation.rpc, &rpc));
        let status = serde_json::to_value(rpc.as_ref()).unwrap();
        assert_eq!(status["healthy"], true);
        assert_eq!(status["web3_clientVersion"], "ipc-startup-fixture");
        assert_eq!(call::<u64>(&rpc, 7).await, 7);
        assert!(rpc.ipc_provider.load().is_some());
        assert_eq!(status["ipc_pool_size"], 2);
        assert_eq!(subscriptions.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(rpc.http_client.is_none() && rpc.ws_provider.load().is_none());
    }).await;
}

#[tokio::test]
async fn ipc_pool_shares_one_admission_budget_across_all_configured_streams() {
    test_support::with_cleanup(async {
        use super::one::RequestPermits;
        use tokio::sync::{mpsc, Semaphore};
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("node.ipc");
        let listener = UnixListener::bind(&path).unwrap();
        let gate = Arc::new(Semaphore::new(0));
        let server_gate = gate.clone();
        let (seen, mut received) = mpsc::unbounded_channel();
        let server = test_support::spawn(serve_connections(listener, 4, move |socket| {
            let gate = server_gate.clone();
            let seen = seen.clone();
            async move {
                let (mut reader, writer) = socket.into_split();
                let writer = Arc::new(tokio::sync::Mutex::new(writer));
                let mut buffer = Vec::new();
                let mut responses = futures::stream::FuturesUnordered::new();
                loop {
                    tokio::select! {
                        request = read_request(&mut reader, &mut buffer) => {
                            let value = request["params"][0].as_u64().unwrap();
                            seen.send(value).unwrap();
                            let gate = gate.clone();
                            let writer = writer.clone();
                            responses.push(async move {
                                gate.acquire().await.unwrap().forget();
                                writer.lock().await.write_all(json!({"jsonrpc":"2.0", "id":request["id"], "result":value}).to_string().as_bytes()).await.unwrap();
                            });
                        }
                        Some(()) = responses.next(), if !responses.is_empty() => {}
                    }
                }
            }
        }));
        let mut rpc = rpc_with_connections(&path, 4, false).await;
        Arc::get_mut(&mut rpc).unwrap().request_permits = RequestPermits::new(2, 64);
        let a = rpc.clone();
        let a = test_support::spawn(async move { call::<u64>(&a, 10).await });
        let b = rpc.clone();
        let b = test_support::spawn(async move { call::<u64>(&b, 20).await });
        let mut first = vec![timeout(Duration::from_secs(2), received.recv()).await.unwrap().unwrap(), timeout(Duration::from_secs(2), received.recv()).await.unwrap().unwrap()];
        first.sort_unstable();
        assert_eq!(first, [10, 20]);
        let c = rpc.clone();
        let c = test_support::spawn(async move { call::<u64>(&c, 30).await });
        assert!(timeout(Duration::from_millis(50), received.recv()).await.is_err(), "a third request bypassed the logical backend's budget");
        assert_eq!(rpc.active_requests.load(std::sync::atomic::Ordering::SeqCst), 2);
        gate.add_permits(1);
        assert_eq!(timeout(Duration::from_secs(2), received.recv()).await.unwrap(), Some(30));
        gate.add_permits(2);
        assert_eq!((a.await.unwrap(), b.await.unwrap(), c.await.unwrap()), (10, 20, 30));
        assert_eq!(rpc.active_requests.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(rpc.request_permits.has_capacity());
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        drop(rpc);
    }).await;
}

#[tokio::test]
async fn ipc_pool_recovers_one_query_stream_and_a_replaced_socket() {
    test_support::with_cleanup(async {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use tokio::sync::{mpsc, oneshot};
        use tokio::task::JoinSet;
        use tokio_util::sync::CancellationToken;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("node.ipc");
        let mut listener = UnixListener::bind(&path).unwrap();
        let server_path = path.clone();
        let connections = Arc::new(AtomicUsize::new(0));
        let counted = connections.clone();
        let closed_once = Arc::new(AtomicBool::new(false));
        let (replace, mut replacements) = mpsc::channel::<oneshot::Sender<()>>(1);
        let stop = CancellationToken::new();
        let server_stop = stop.clone();
        let server = test_support::spawn(async move {
            let mut clients = JoinSet::new();
            let mut generation = CancellationToken::new();
            loop {
                tokio::select! {
                    _ = server_stop.cancelled() => break,
                    Some(ack) = replacements.recv() => {
                        std::fs::remove_file(&server_path).unwrap();
                        listener = UnixListener::bind(&server_path).unwrap();
                        generation.cancel();
                        while let Some(result) = clients.join_next().await { result.unwrap(); }
                        generation = CancellationToken::new();
                        ack.send(()).unwrap();
                    }
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap();
                        counted.fetch_add(1, Ordering::SeqCst);
                        let stop = generation.clone();
                        let closed_once = closed_once.clone();
                        clients.spawn(async move {
                            tokio::select! {
                                _ = stop.cancelled() => {},
                                _ = async {
                                    let mut buffer = Vec::new();
                                    loop {
                                        let request = read_request(&mut socket, &mut buffer).await;
                                        let value = request["params"][0].as_u64().unwrap();
                                        if value == 1 && !closed_once.swap(true, Ordering::SeqCst) { break; }
                                        socket.write_all(json!({"jsonrpc":"2.0", "id":request["id"], "result":value}).to_string().as_bytes()).await.unwrap();
                                    }
                                } => {}
                            }
                        });
                    }
                    Some(result) = clients.join_next(), if !clients.is_empty() => { result.unwrap(); }
                }
            }
            generation.cancel();
            while let Some(result) = clients.join_next().await { result.unwrap(); }
        });
        let rpc = rpc(&path).await;
        assert_eq!(call::<u64>(&rpc, 0).await, 0);
        assert_eq!(connections.load(Ordering::SeqCst), 3);
        let values = timeout(Duration::from_secs(5), async { tokio::join!(call::<u64>(&rpc, 1), call::<u64>(&rpc, 2)) }).await.unwrap();
        assert_eq!(values, (1, 2));
        assert_eq!(connections.load(Ordering::SeqCst), 4, "only the failed stream should reconnect");
        for value in 3..7 { assert_eq!(call::<u64>(&rpc, value).await, value); }
        assert_eq!(connections.load(Ordering::SeqCst), 4, "ordinary calls must reuse the pool");
        let (ack, replaced) = oneshot::channel();
        replace.send(ack).await.unwrap();
        replaced.await.unwrap();
        let values = timeout(Duration::from_secs(5), async { tokio::join!(call::<u64>(&rpc, 100), call::<u64>(&rpc, 101)) }).await.unwrap();
        assert_eq!(values, (100, 101));
        assert_eq!(connections.load(Ordering::SeqCst), 7, "all streams should reconnect to the new inode");
        assert_eq!(rpc.active_requests.load(Ordering::SeqCst), 0);
        stop.cancel();
        server.await.unwrap();
        drop(rpc);
    }).await;
}

#[tokio::test]
async fn startup_rejects_a_backend_without_any_transport() {
    let config = sonic_rs::from_str("{}").unwrap();
    let result = Web3Rpc::spawn(
        config,
        "missing-transport".into(),
        1,
        None,
        Duration::from_secs(12),
        moka::future::Cache::new(16),
        moka::future::Cache::new(16),
        super::blockchain::new_block_response_cache(1_000_000),
        None,
        None,
        None,
        Duration::from_secs(60),
    )
    .await;
    assert_eq!(
        result
            .expect_err("empty transport must remain invalid")
            .to_string(),
        "one of ipc_path, ws_url, or http_url is required"
    );
}

#[tokio::test]
async fn ipc_pool_does_not_queue_fast_calls_behind_two_partial_responses() {
    test_support::with_cleanup(async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("node.ipc");
        let listener = UnixListener::bind(&path).unwrap();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let server_release = release.clone();
        let (started, mut partial) = tokio::sync::mpsc::unbounded_channel();
        let server = test_support::spawn(serve_connections(listener, 4, move |socket| {
            let release = server_release.clone();
            let started = started.clone();
            async move {
                let (mut reader, writer) = socket.into_split();
                let writer = Arc::new(tokio::sync::Mutex::new(writer));
                let mut buffer = Vec::new();
                let mut responses = futures::stream::FuturesUnordered::new();
                loop {
                    tokio::select! {
                        request = read_request(&mut reader, &mut buffer) => {
                            let writer = writer.clone();
                            let release = release.clone();
                            let started = started.clone();
                            responses.push(async move {
                                let value = request["params"][0].as_u64().unwrap();
                                let mut writer = writer.lock().await;
                                if value < 2 {
                                    // A partially transferred JSON value owns this byte stream.
                                    // Keep reading requests, but never interleave response bytes.
                                    let prefix = format!("{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":", request["id"]);
                                    writer.write_all(prefix.as_bytes()).await.unwrap();
                                    started.send(()).unwrap();
                                    release.acquire().await.unwrap().forget();
                                    writer.write_all(format!("{value}}}").as_bytes()).await.unwrap();
                                } else {
                                    let response = json!({"jsonrpc":"2.0","id":request["id"],"result":value});
                                    writer.write_all(response.to_string().as_bytes()).await.unwrap();
                                }
                            });
                        }
                        Some(()) = responses.next(), if !responses.is_empty() => {}
                    }
                }
            }
        }));
        let rpc = rpc_with_connections(&path, 4, false).await;
        let slow_rpc = rpc.clone();
        let slow = test_support::spawn(async move { call::<u64>(&slow_rpc, 0).await });
        timeout(Duration::from_secs(2), partial.recv()).await.unwrap().unwrap();
        let other_rpc = rpc.clone();
        let other = test_support::spawn(async move { call::<u64>(&other_rpc, 1).await });
        timeout(Duration::from_secs(2), partial.recv()).await.unwrap().unwrap();
        timeout(Duration::from_secs(2), async {
            for value in 2..=9 {
                assert_eq!(call::<u64>(&rpc, value).await, value);
            }
        }).await.expect("fast calls must use an idle IPC stream while a response is incomplete");
        assert!(!slow.is_finished() && !other.is_finished());
        assert_eq!(rpc.active_requests.load(std::sync::atomic::Ordering::SeqCst), 2);
        release.add_permits(2);
        assert_eq!(timeout(Duration::from_secs(2), slow).await.unwrap().unwrap(), 0);
        assert_eq!(timeout(Duration::from_secs(2), other).await.unwrap().unwrap(), 1);
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        drop(rpc);
    }).await;
}

#[tokio::test]
async fn ipc_subscription_progresses_while_a_query_frame_is_incomplete() {
    test_support::with_cleanup(async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("node.ipc");
        let listener = UnixListener::bind(&path).unwrap();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let server_release = release.clone();
        let (started, mut partial) = tokio::sync::mpsc::unbounded_channel();
        let (pulse, head_update) = watch::channel(false);
        let server = test_support::spawn(serve_pool(listener, move |socket| {
            let release = server_release.clone();
            let started = started.clone();
            let mut head_update = head_update.clone();
            async move {
                let (mut reader, writer) = socket.into_split();
                let writer = Arc::new(tokio::sync::Mutex::new(writer));
                let mut buffer = Vec::new();
                let mut subscribed = false;
                let mut responses = futures::stream::FuturesUnordered::new();
                loop {
                    tokio::select! {
                        request = read_request(&mut reader, &mut buffer) => {
                            if request["method"] == "eth_subscribe" {
                                assert!(!subscribed);
                                subscribed = true;
                                let reply = json!({"jsonrpc":"2.0","id":request["id"],"result":"0x1"});
                                writer.lock().await.write_all(reply.to_string().as_bytes()).await.unwrap();
                            } else {
                                let writer = writer.clone();
                                let release = release.clone();
                                let started = started.clone();
                                responses.push(async move {
                                    let value = request["params"][0].as_u64().unwrap();
                                    let mut writer = writer.lock().await;
                                    if value == 0 {
                                        let prefix = format!("{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":", request["id"]);
                                        writer.write_all(prefix.as_bytes()).await.unwrap();
                                        started.send(()).unwrap();
                                        release.acquire().await.unwrap().forget();
                                        writer.write_all(b"0}").await.unwrap();
                                    } else {
                                        let reply = json!({"jsonrpc":"2.0","id":request["id"],"result":value});
                                        writer.write_all(reply.to_string().as_bytes()).await.unwrap();
                                    }
                                });
                            }
                        }
                        _ = head_update.changed(), if subscribed => {
                            let notification = json!({"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0x1","result":B256::repeat_byte(0x77)}});
                            writer.lock().await.write_all(notification.to_string().as_bytes()).await.unwrap();
                        }
                        Some(()) = responses.next(), if !responses.is_empty() => {}
                    }
                }
            }
        }));
        let rpc = rpc(&path).await;
        let provider = rpc.ipc_provider.load_full().unwrap().subscription();
        let subscription = provider.subscribe_pending_transactions().await.unwrap();
        let mut heads = subscription.into_stream();
        let slow_rpc = rpc.clone();
        let slow = test_support::spawn(async move { call::<u64>(&slow_rpc, 0).await });
        timeout(Duration::from_secs(2), partial.recv()).await.unwrap().unwrap();
        pulse.send_replace(true);
        assert_eq!(timeout(Duration::from_secs(2), heads.next()).await.expect("query framing must not block subscriptions"), Some(B256::repeat_byte(0x77)));
        assert_eq!(timeout(Duration::from_secs(2), call::<u64>(&rpc, 7)).await.unwrap(), 7);
        assert!(!slow.is_finished());
        release.add_permits(1);
        assert_eq!(timeout(Duration::from_secs(2), slow).await.unwrap().unwrap(), 0);
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        drop(heads);
        drop(rpc);
    }).await;
}

#[tokio::test]
async fn configured_ipc_pool_uses_each_query_stream_and_a_separate_subscription_stream() {
    for connections in [4, 8] {
        test_support::with_cleanup(async move {
            use std::sync::atomic::{AtomicUsize, Ordering};
            use tokio::sync::{mpsc, Semaphore};
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("pool.ipc");
            let listener = UnixListener::bind(&path).unwrap();
            let gate = Arc::new(Semaphore::new(0));
            let server_gate = gate.clone();
            let accepted = Arc::new(AtomicUsize::new(0));
            let server_accepted = accepted.clone();
            let (seen, mut received) = mpsc::unbounded_channel();
            let server = test_support::spawn(serve_connections(listener, connections, move |mut socket| {
                let index = server_accepted.fetch_add(1, Ordering::Relaxed);
                let gate = server_gate.clone();
                let seen = seen.clone();
                async move {
                    let mut buffer = Vec::new();
                    loop {
                        let request = read_request(&mut socket, &mut buffer).await;
                        seen.send((index, request["params"][0].as_u64().unwrap())).unwrap();
                        gate.acquire().await.unwrap().forget();
                        socket.write_all(json!({"jsonrpc":"2.0", "id":request["id"], "result":request["params"][0]}).to_string().as_bytes()).await.unwrap();
                    }
                }
            }));
            let pool = Arc::new(connect_ipc_pool(path, connections, false).await.unwrap());
            assert_eq!(pool.size(), connections);
            let calls = (0..connections).map(|value| {
                let pool = pool.clone();
                test_support::spawn(async move {
                    let provider = pool.query();
                    provider.raw_request::<_, u64>("eth_call".into(), [value]).await.unwrap()
                })
            }).collect::<Vec<_>>();
            let mut streams = std::collections::HashSet::new();
            let mut values = std::collections::HashSet::new();
            for _ in 0..connections {
                let (stream, value) = timeout(Duration::from_secs(2), received.recv()).await.unwrap().unwrap();
                assert!(streams.insert(stream), "queries must use distinct idle streams");
                assert!(values.insert(value));
            }
            assert_eq!(accepted.load(Ordering::Relaxed), connections + 1);
            assert_eq!(pool.in_flight(), vec![1; connections]);
            gate.add_permits(connections + 1);
            for (value, call) in calls.into_iter().enumerate() {
                assert_eq!(call.await.unwrap(), value as u64);
            }
            assert_eq!(pool.in_flight(), vec![0; connections]);
            assert_eq!(pool.subscription().raw_request::<_, u64>("eth_call".into(), [999]).await.unwrap(), 999);
            let (subscription_stream, value) = received.recv().await.unwrap();
            assert_eq!(value, 999);
            assert!(!streams.contains(&subscription_stream));
            server.abort();
            assert!(server.await.unwrap_err().is_cancelled());
        }).await;
    }
}

#[tokio::test]
async fn ipc_pool_rejects_zero_and_excessive_connections_before_connecting() {
    for connections in [0, crate::config::MAX_IPC_QUERY_CONNECTIONS + 1] {
        let result = connect_ipc_pool("/not-an-ipc-socket".into(), connections, false).await;
        assert!(result.is_err_and(|error| error.to_string().contains("pool size")));
    }
}

#[tokio::test]
async fn startup_rejects_invalid_ipc_pool_sizes() {
    for connections in [0, crate::config::MAX_IPC_QUERY_CONNECTIONS + 1] {
        let config = crate::config::Web3RpcConfig {
            ipc_path: Some("/not-an-ipc-socket".into()),
            ipc_query_connections: connections,
            ..Default::default()
        };
        let result = Web3Rpc::spawn(
            config,
            "invalid-ipc-pool".into(),
            1,
            None,
            Duration::from_secs(12),
            moka::future::Cache::new(16),
            moka::future::Cache::new(16),
            super::blockchain::new_block_response_cache(1_000_000),
            None,
            None,
            None,
            Duration::from_secs(60),
        )
        .await;
        assert!(result
            .expect_err("invalid sizes must fail before connecting")
            .to_string()
            .contains("ipc_query_connections must be between"));
    }
}

#[tokio::test]
async fn native_ipc_batch_counts_each_call_against_the_existing_pool_budget() {
    use super::{one::RequestPermits, request::OpenRequestResult};
    use crate::jsonrpc::ValidatedRequest;
    use std::sync::atomic::Ordering;
    use tokio::sync::{mpsc, Semaphore};

    test_support::with_cleanup(async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("batch.ipc");
        let listener = UnixListener::bind(&path).unwrap();
        let gate = Arc::new(Semaphore::new(0));
        let server_gate = gate.clone();
        let (seen, mut received) = mpsc::unbounded_channel();
        let server = test_support::spawn(serve_pool(listener, move |mut socket| {
            let gate = server_gate.clone();
            let seen = seen.clone();
            async move {
                let mut buffer = Vec::new();
                loop {
                    let request = read_request(&mut socket, &mut buffer).await;
                    let reply = if let Some(batch) = request.as_array() {
                        assert_eq!(batch.len(), 2);
                        assert_ne!(batch[0]["id"], batch[1]["id"]);
                        seen.send(batch.len()).unwrap();
                        gate.acquire().await.unwrap().forget();
                        Value::Array(batch.iter().rev().map(|request| {
                            json!({"jsonrpc":"2.0","id":request["id"],"result":request["params"][0]})
                        }).collect())
                    } else {
                        seen.send(1).unwrap();
                        json!({"jsonrpc":"2.0","id":request["id"],"result":request["params"][0]})
                    };
                    socket.write_all(reply.to_string().as_bytes()).await.unwrap();
                }
            }
        }));
        let mut rpc = rpc_with_connections(&path, 2, true).await;
        Arc::get_mut(&mut rpc).unwrap().request_permits = RequestPermits::new(2, 8);
        let one = ValidatedRequest::new_internal("eth_call".into(), &json!([10]), None, None).await.unwrap();
        let two = ValidatedRequest::new_internal("eth_call".into(), &json!([20]), None, None).await.unwrap();
        let OpenRequestResult::Handle(mut handle) = rpc.try_request_handle(&one, None, true).await.unwrap() else {
            panic!("batch admission failed");
        };
        assert_eq!(handle.reserve_batch_size(3), 2);
        let batch = test_support::spawn(async move { handle.request_batch(&[one, two]).await.unwrap() });
        assert_eq!(timeout(Duration::from_secs(2), received.recv()).await.unwrap(), Some(2));
        assert_eq!(rpc.active_requests.load(Ordering::SeqCst), 2);
        let single_rpc = rpc.clone();
        let single = test_support::spawn(async move { call::<u64>(&single_rpc, 30).await });
        assert!(timeout(Duration::from_millis(50), received.recv()).await.is_err());
        gate.add_permits(1);
        let replies = batch.await.unwrap();
        let values: Vec<_> = replies.into_iter().map(|reply| {
            let reply = reply.unwrap();
            assert_eq!(reply.id.as_u64(), Some(1));
            reply.result().unwrap().as_u64().unwrap()
        }).collect();
        assert_eq!(values, [10, 20]);
        assert_eq!(single.await.unwrap(), 30);
        assert_eq!(rpc.backend_batch_requests.load(Ordering::Relaxed), 1);
        assert_eq!(rpc.active_requests.load(Ordering::SeqCst), 0);
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }).await;
}
