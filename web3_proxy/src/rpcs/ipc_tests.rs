//! Exercise IPC framing, multiplexing and recovery through real Unix sockets.
use super::{one::Web3Rpc, provider::connect_ipc, test_support};
use crate::{errors::Web3ProxyError, jsonrpc::JsonRpcResultData};
use alloy::{primitives::B256, providers::Provider};
use futures::StreamExt;
use latency::{PeakEwmaLatency, RollingQuantileLatency};
use serde_json::{json, Value};
use sonic_rs::JsonValueTrait;
use std::{path::Path, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::watch,
    time::{timeout, Duration, Instant},
};

async fn read_request(socket: &mut UnixStream, buffer: &mut Vec<u8>) -> Value {
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
    let (hard_limit_until, _) = watch::channel(Instant::now());
    let rpc = Web3Rpc {
        name: "local-ipc".into(),
        ipc_path: Some(path.into()),
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
    rpc.ipc_provider
        .store(Some(Arc::new(connect_ipc(path.into()).await.unwrap())));
    Arc::new(rpc)
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
async fn ipc_reuses_one_connection_for_fragmented_large_and_out_of_order_replies() {
    test_support::with_cleanup(async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("node.ipc");
        let listener = UnixListener::bind(&path).unwrap();
        let payload = format!("0x{}", "a".repeat(17 * 1024 * 1024));
        let expected = payload.clone();
        let server = test_support::spawn(async move {
            // The whole sequence must use this one accepted connection.
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = Vec::new();
            let request = read_request(&mut socket, &mut buffer).await;
            let response = json!({"jsonrpc":"2.0", "id":request["id"], "result":payload});
            for chunk in response.to_string().as_bytes().chunks(8192) {
                socket.write_all(chunk).await.unwrap();
                tokio::task::yield_now().await;
            }
            let first = read_request(&mut socket, &mut buffer).await;
            let second = read_request(&mut socket, &mut buffer).await;
            assert_ne!(first["id"], second["id"]);
            let responses = [second, first].map(|request| {
                json!({"jsonrpc":"2.0", "id":request["id"], "result":request["params"][0]})
                    .to_string()
            });
            // Two concatenated responses, in reverse order, without newlines.
            socket
                .write_all(responses.concat().as_bytes())
                .await
                .unwrap();
            // Keep the stream open until the client releases its provider.
            let mut byte = [0];
            let _ = socket.read(&mut byte).await;
        });
        let rpc = rpc(&path).await;
        assert!(
            !rpc.supports_batch(),
            "IPC must also take precedence for client batches"
        );
        let result: String = call(&rpc, 0).await;
        assert_eq!(result, expected);
        let (first, second) = tokio::join!(call::<u64>(&rpc, 11), call::<u64>(&rpc, 22));
        assert_eq!((first, second), (11, 22));
        assert_eq!(
            rpc.active_requests
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        drop(rpc);
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    })
    .await;
}

#[tokio::test]
async fn ipc_preserves_reverts_and_does_not_mix_a_late_reply_with_the_next_call() {
    test_support::with_cleanup(async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("node.ipc");
        let listener = UnixListener::bind(&path).unwrap();
        let server = test_support::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = Vec::new();
            let revert = read_request(&mut socket, &mut buffer).await;
            socket
                .write_all(
                    json!({"jsonrpc":"2.0", "id":revert["id"],
                "error":{"code":3,"message":"execution reverted","data":"0x1234"}})
                    .to_string()
                    .as_bytes(),
                )
                .await
                .unwrap();
            let late = read_request(&mut socket, &mut buffer).await;
            let next = read_request(&mut socket, &mut buffer).await;
            let replies = [
                json!({"jsonrpc":"2.0", "id":next["id"], "result":42}).to_string(),
                json!({"jsonrpc":"2.0", "id":late["id"], "result":99}).to_string(),
            ];
            socket.write_all(replies.concat().as_bytes()).await.unwrap();
            let mut byte = [0];
            let _ = socket.read(&mut byte).await;
        });
        let rpc = rpc(&path).await;
        let revert = rpc
            .internal_request::<_, u64>(
                "eth_call".into(),
                &json!([0]),
                None,
                Some(Duration::from_secs(2)),
            )
            .await
            .unwrap_err();
        assert!(matches!(revert, Web3ProxyError::JsonRpcErrorData(error)
            if error.code == 3 && error.message == "execution reverted"
                && error.data.as_ref().is_some_and(|data| data.as_str() == Some("0x1234"))));
        let late = rpc
            .internal_request::<_, u64>(
                "eth_call".into(),
                &json!([1]),
                None,
                Some(Duration::from_millis(50)),
            )
            .await;
        assert!(matches!(late, Err(Web3ProxyError::Timeout(_))));
        assert_eq!(
            rpc.active_requests
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        let result: u64 = call(&rpc, 2).await;
        assert_eq!(result, 42);
        assert_eq!(
            rpc.active_requests
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        drop(rpc);
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    })
    .await;
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
        let server = test_support::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = Vec::new();
            let request = read_request(&mut socket, &mut buffer).await;
            assert_eq!(
                request["method"], "rpc_modules",
                "a private method reached IPC"
            );
            socket
                .write_all(
                    json!({"jsonrpc":"2.0", "id":request["id"], "result":{
                "eth":"1.0", "rpc":"1.0", "engine":"1.0", "testing":"1.0", "miner":"1.0"}})
                    .to_string()
                    .as_bytes(),
                )
                .await
                .unwrap();
            let request = read_request(&mut socket, &mut buffer).await;
            assert_eq!(request["method"], "eth_call");
            socket
                .write_all(
                    json!({"jsonrpc":"2.0", "id":request["id"], "result":7})
                        .to_string()
                        .as_bytes(),
                )
                .await
                .unwrap();
            let mut byte = [0];
            let _ = socket.read(&mut byte).await;
        });
        let mut rpc = rpc(&path).await;
        Arc::get_mut(&mut rpc).unwrap().ipc_allowed_modules =
            Some(vec!["eth".into(), "rpc".into()]);
        for method in [
            "engine_getClientVersionV1",
            "testing_buildBlockV1",
            "miner_setGasPrice",
        ] {
            let result = rpc
                .internal_request::<_, Value>(
                    method.into(),
                    &json!([]),
                    None,
                    Some(Duration::from_secs(2)),
                )
                .await;
            assert!(matches!(result, Err(Web3ProxyError::MethodNotFound(_))));
        }
        assert_eq!(
            rpc.total_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        let modules: std::collections::BTreeMap<String, String> = rpc
            .internal_request(
                "rpc_modules".into(),
                &json!([]),
                None,
                Some(Duration::from_secs(2)),
            )
            .await
            .unwrap();
        assert_eq!(
            modules.keys().map(String::as_str).collect::<Vec<_>>(),
            ["eth", "rpc"]
        );
        assert_eq!(call::<u64>(&rpc, 0).await, 7);
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        drop(rpc);
    })
    .await;
}
