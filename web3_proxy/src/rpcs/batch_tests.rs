use super::consensus::RankedRpcs;
use super::many::{Web3Rpcs, Web3RpcsSpawnConfig};
use super::one::{RequestPermits, Web3Rpc};
use crate::app::App;
use crate::config::AppConfig;
use crate::frontend::rpc_proxy_ws::ProxyMode;
use crate::jsonrpc::{JsonRpcRequestEnum, SingleRequest};
use crate::rpcs::blockchain::BlockHeader;
use alloy::rpc::types::Header;
use arc_swap::ArcSwapOption;
use axum::body::{Body, Bytes};
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::extract::State;
use axum::http::Response;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{routing::post, Router};
use deduped_broadcast::DedupedBroadcaster;
use latency::{PeakEwmaLatency, RollingQuantileLatency};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration, Instant};

struct Incoming {
    body: Value,
    reply: oneshot::Sender<Response<Body>>,
}

impl Incoming {
    fn respond(self, body: Value) {
        self.reply
            .send(
                Response::builder()
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .unwrap();
    }

    fn succeed(self) {
        let response = if let Some(calls) = self.body.as_array() {
            Value::Array(calls.iter().rev().map(answer).collect())
        } else {
            answer(&self.body)
        };
        self.respond(response);
    }

    fn reject(self) {
        self.respond(
            json!({"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"batch rejected"}}),
        );
    }
}

fn answer(call: &Value) -> Value {
    json!({"jsonrpc":"2.0","id":call["id"],"result":call["params"][0]["data"]})
}

async fn receive(
    State(sender): State<mpsc::UnboundedSender<Incoming>>,
    body: Bytes,
) -> Response<Body> {
    let (reply, response) = oneshot::channel();
    sender
        .send(Incoming {
            body: serde_json::from_slice(&body).unwrap(),
            reply,
        })
        .unwrap();
    response
        .await
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

struct Harness {
    app: Arc<App>,
    rpc: Arc<Web3Rpc>,
    incoming: mpsc::UnboundedReceiver<Incoming>,
    server: JoinHandle<()>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Harness {
    async fn new(concurrency: usize, packet_size: usize) -> Self {
        let (sender, incoming) = mpsc::unbounded_channel();
        let router = Router::new().route("/", post(receive)).with_state(sender);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (hard_limit_until, _) = watch::channel(Instant::now());
        let rpc = Arc::new(Web3Rpc {
            name: "controlled-backend".into(),
            healthy: AtomicBool::new(true),
            http_client: Some(reqwest::Client::new()),
            http_url: Some(format!("http://{address}").parse().unwrap()),
            hard_limit_until: Some(hard_limit_until),
            request_permits: RequestPermits::new(concurrency, packet_size),
            peak_latency: Some(PeakEwmaLatency::spawn(
                Duration::from_secs(15),
                100,
                Duration::from_secs(1),
            )),
            median_latency: Some(RollingQuantileLatency::spawn_median(100).await),
            ..Default::default()
        });
        let (head_sender, watch_consensus_head_receiver) = watch::channel(None);
        let (balanced_rpcs, background, _) = Web3Rpcs::spawn(
            Web3RpcsSpawnConfig::new(1, None, 0, 0, 1_000_000),
            "batch-test".into(),
            Some(head_sender.clone()),
            None,
        )
        .await
        .unwrap();
        let mut header: Header = Header::default();
        header.inner.number = 42;
        let head = BlockHeader::new(Arc::new(header));
        balanced_rpcs
            .by_name
            .write()
            .insert(rpc.name.clone(), rpc.clone());
        balanced_rpcs
            .watch_ranked_rpcs
            .send_replace(Some(Arc::new(RankedRpcs::from_rpcs(
                vec![rpc.clone()],
                Some(head.clone()),
                false,
            ))));
        background.abort();
        head_sender.send_replace(Some(head));
        let app = Arc::new(App {
            balanced_rpcs: balanced_rpcs.clone(),
            bundler_4337_rpcs: balanced_rpcs.clone(),
            config: AppConfig::default(),
            http_client: None,
            watch_consensus_head_receiver,
            pending_txid_firehose: DedupedBroadcaster::new(4, 16),
            hostname: None,
            frontend_port: Arc::new(AtomicU16::new(0)),
            protected_rpcs: balanced_rpcs,
            start: Instant::now(),
            tx_subscriptions: Semaphore::new(1),
        });
        Self {
            app,
            rpc,
            incoming,
            server,
        }
    }

    fn start(&self, count: usize) -> JoinHandle<Value> {
        let app = self.app.clone();
        tokio::spawn(async move {
            let requests = (0..count).map(|i| SingleRequest::new(
                ((i % 2) as u64).into(), "eth_call".into(),
                sonic_rs::from_str(&json!([{"to":"0x0000000000000000000000000000000000000000", "data":format!("0x{i:04x}")}, "latest"]).to_string()).unwrap(),
            ).unwrap()).collect();
            let (_, response, _) = app
                .proxy_web3_rpc(ProxyMode::Best, JsonRpcRequestEnum::Batch(requests), None)
                .await
                .unwrap();
            serde_json::from_str(&response.to_json_string().await.unwrap()).unwrap()
        })
    }

    async fn next(&mut self) -> Incoming {
        timeout(Duration::from_secs(2), self.incoming.recv())
            .await
            .expect("backend traffic must start")
            .unwrap()
    }

    async fn quiet(&mut self) {
        assert!(
            timeout(Duration::from_millis(50), self.incoming.recv())
                .await
                .is_err(),
            "unexpected backend request"
        );
    }

    fn idle(&self) {
        assert_eq!(self.rpc.active_requests.load(Ordering::SeqCst), 0);
    }
}

fn assert_answers(response: Value, count: usize) {
    assert_eq!(
        response,
        Value::Array(
            (0..count)
                .map(|i| json!({"jsonrpc":"2.0","id":i % 2,"result":format!("0x{i:04x}")}))
                .collect()
        )
    );
}

#[tokio::test]
async fn batch_one_slot_sends_three_sequential_packets() {
    let mut h = Harness::new(1, 64).await;
    let task = h.start(129);
    for size in [64, 64, 1] {
        let packet = h.next().await;
        assert_eq!(packet.body.as_array().unwrap().len(), size);
        assert_eq!(h.rpc.active_requests.load(Ordering::SeqCst), 1);
        h.quiet().await;
        packet.succeed();
    }
    assert_answers(task.await.unwrap(), 129);
    assert_eq!(h.rpc.total_requests.load(Ordering::Relaxed), 129);
    assert_eq!(h.rpc.backend_batch_requests.load(Ordering::Relaxed), 3);
    h.idle();
}

#[tokio::test]
async fn batch_two_slots_allow_exactly_two_packets() {
    let mut h = Harness::new(2, 64).await;
    let task = h.start(129);
    let first = h.next().await;
    let second = h.next().await;
    assert_eq!(h.rpc.active_requests.load(Ordering::SeqCst), 2);
    h.quiet().await;
    first.succeed();
    let third = h.next().await;
    h.quiet().await;
    second.succeed();
    third.succeed();
    assert_answers(task.await.unwrap(), 129);
    h.idle();
}

#[tokio::test]
async fn batch_failure_recovers_while_other_packet_is_blocked() {
    let mut h = Harness::new(4, 2).await;
    let task = h.start(4);
    let first = h.next().await;
    let second = h.next().await;
    let failed = first
        .body
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["params"].clone())
        .collect::<Vec<_>>();
    first.reject();
    let mut retried = Vec::new();
    for _ in 0..2 {
        let call = h.next().await;
        assert!(call.body.is_object());
        retried.push(call.body["params"].clone());
        call.succeed();
    }
    retried.sort_by_key(Value::to_string);
    assert_eq!(retried, failed);
    second.succeed();
    assert_answers(task.await.unwrap(), 4);
    h.quiet().await;
    h.idle();
}

#[tokio::test]
async fn batch_mixed_errors_retry_only_rate_limit() {
    let mut h = Harness::new(4, 64).await;
    let task = h.start(3);
    let packet = h.next().await;
    let calls = packet.body.as_array().unwrap();
    let expected_retry = calls[2]["params"].clone();
    let response = json!([
        answer(&calls[0]),
        {"jsonrpc":"2.0","id":calls[1]["id"],"error":{"code":3,"message":"execution reverted"}},
        {"jsonrpc":"2.0","id":calls[2]["id"],"error":{"code":-32005,"message":"rate limit exceeded"}}
    ]);
    packet.respond(response);
    let retry = h.next().await;
    assert_eq!(retry.body["params"], expected_retry);
    assert_eq!(retry.body["id"], 0);
    retry.succeed();
    assert_eq!(
        task.await.unwrap(),
        json!([
            {"jsonrpc":"2.0","id":0,"result":"0x0000"},
            {"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}},
            {"jsonrpc":"2.0","id":0,"result":"0x0002"}
        ])
    );
    h.quiet().await;
    h.idle();
}

#[tokio::test]
async fn batch_reversed_responses_restore_duplicate_client_ids() {
    let mut h = Harness::new(8, 64).await;
    let task = h.start(4);
    let packet = h.next().await;
    let mut ids = packet
        .body
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["id"].to_string())
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 4, "backend IDs must be unique");
    packet.succeed();
    assert_answers(task.await.unwrap(), 4);
}

#[tokio::test]
async fn batch_invalid_response_ids_fall_back_individually() {
    for invalid in ["missing", "duplicate", "unknown"] {
        let mut h = Harness::new(4, 64).await;
        let task = h.start(2);
        let packet = h.next().await;
        let calls = packet.body.as_array().unwrap();
        let mut responses = vec![answer(&calls[0]), answer(&calls[1])];
        match invalid {
            "missing" => {
                responses[1].as_object_mut().unwrap().remove("id");
            }
            "duplicate" => responses[1]["id"] = responses[0]["id"].clone(),
            "unknown" => responses[1]["id"] = json!("unknown"),
            _ => unreachable!(),
        }
        packet.respond(Value::Array(responses));
        for _ in 0..2 {
            let call = h.next().await;
            assert!(call.body.is_object());
            call.succeed();
        }
        assert_answers(task.await.unwrap(), 2);
    }
}

#[tokio::test]
async fn batch_cancellation_releases_slots() {
    let mut h = Harness::new(2, 64).await;
    let task = h.start(129);
    let first = h.next().await;
    let second = h.next().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop((first, second));
    h.idle();
    let task = h.start(2);
    h.next().await.succeed();
    assert_answers(task.await.unwrap(), 2);
}

#[tokio::test]
async fn batch_recovery_after_connection_window_keeps_request_data() {
    let mut h = Harness::new(4, 64).await;
    let task = h.start(2);
    let packet = h.next().await;
    let expected = packet
        .body
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["params"].clone())
        .collect::<Vec<_>>();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(11)).await;
    tokio::time::resume();
    packet.reject();
    let mut retried = Vec::new();
    for _ in 0..2 {
        let call = h.next().await;
        retried.push(call.body["params"].clone());
        call.succeed();
    }
    retried.sort_by_key(Value::to_string);
    assert_eq!(retried, expected);
    assert_answers(task.await.unwrap(), 2);
}

#[tokio::test]
async fn batch_stalled_packet_and_queue_keep_original_deadline() {
    let mut h = Harness::new(1, 64).await;
    let task = h.start(129);
    let packet = h.next().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::time::resume();
    let response = timeout(Duration::from_millis(500), task)
        .await
        .expect("original deadline must end stalled and queued packets")
        .unwrap();
    assert_eq!(response.as_array().unwrap().len(), 129);
    for (i, call) in response.as_array().unwrap().iter().enumerate() {
        assert_eq!(call["id"], i % 2);
        assert!(call["error"].is_object());
    }
    drop(packet);
    h.quiet().await;
    h.idle();
    let task = h.start(2);
    h.next().await.succeed();
    assert_answers(task.await.unwrap(), 2);
}

#[tokio::test]
async fn batch_final_failure_preserves_other_packet_answers() {
    let mut h = Harness::new(4, 2).await;
    let task = h.start(4);
    let first = h.next().await;
    let second = h.next().await;
    let completed = first
        .body
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["params"][0]["data"].clone())
        .collect::<Vec<_>>();
    first.succeed();
    second.reject();
    let retry_a = h.next().await;
    let retry_b = h.next().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::time::resume();
    let response = timeout(Duration::from_millis(500), task)
        .await
        .expect("fallback must use the original deadline")
        .unwrap();
    for (i, call) in response.as_array().unwrap().iter().enumerate() {
        assert_eq!(call["id"], i % 2);
        let value = json!(format!("0x{i:04x}"));
        if completed.contains(&value) {
            assert_eq!(call["result"], value);
        } else {
            assert!(call["error"].is_object());
        }
    }
    drop((retry_a, retry_b));
    h.idle();
}

#[tokio::test]
async fn batch_large_fallback_error_is_checked_before_success() {
    let mut h = Harness::new(4, 64).await;
    let task = h.start(2);
    h.next().await.reject();
    let first = h.next().await;
    let retry_params = first.body["params"].clone();
    let error = json!({"jsonrpc":"2.0","id":first.body["id"],"error":{"code":-32005,"message":"rate limit exceeded","data":"x".repeat(140_000)}});
    first.respond(error);
    h.next().await.succeed();
    let retry = h.next().await;
    assert_eq!(retry.body["params"], retry_params);
    retry.succeed();
    assert_answers(task.await.unwrap(), 2);
    h.idle();
}

#[tokio::test]
async fn batch_large_fallback_body_obeys_original_deadline() {
    let mut h = Harness::new(4, 64).await;
    let task = h.start(2);
    h.next().await.reject();
    let first = h.next().await;
    let first_id = first.body["id"].clone();
    let prefix = format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":{first_id},\"result\":\"{}",
        "x".repeat(140_000)
    );
    let (sender, receiver) = mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    sender.send(Ok(Bytes::from(prefix))).await.unwrap();
    first
        .reply
        .send(
            Response::builder()
                .header("content-type", "application/json")
                .body(Body::from_stream(
                    tokio_stream::wrappers::ReceiverStream::new(receiver),
                ))
                .unwrap(),
        )
        .unwrap();
    h.next().await.succeed();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::time::resume();
    let response = timeout(Duration::from_millis(500), task)
        .await
        .expect("reading a large fallback must end at the original deadline")
        .unwrap();
    assert!(response
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["id"] == first_id && r["error"].is_object()));
    drop(sender);
    h.idle();
}

#[tokio::test]
async fn batch_configuration_accepts_independent_positive_limits() {
    let h = Harness::new(1, 64).await;
    for (concurrency, batch_size, expected_error) in [
        (1, 64, None),
        (
            0,
            64,
            Some("max_concurrent_requests must be greater than zero"),
        ),
        (
            1,
            0,
            Some("max_backend_batch_items must be greater than zero"),
        ),
    ] {
        let config = sonic_rs::from_str(&json!({"http_url":h.rpc.http_url.as_ref().unwrap().as_str(),"max_concurrent_requests":concurrency,"max_backend_batch_items":batch_size}).to_string()).unwrap();
        let result = Web3Rpc::spawn(
            config,
            "config-test".into(),
            1,
            None,
            Duration::from_secs(12),
            h.app.balanced_rpcs.blocks_by_hash.clone(),
            h.app.balanced_rpcs.blocks_by_number.clone(),
            h.app.balanced_rpcs.block_responses.clone(),
            None,
            None,
            None,
            Duration::from_secs(60),
        )
        .await;
        match (result, expected_error) {
            (Ok((_, task)), None) => task.abort(),
            (Err(error), Some(expected)) => assert_eq!(error.to_string(), expected),
            (Err(error), None) => panic!("positive independent limits must start: {error}"),
            (Ok((_, task)), Some(expected)) => {
                task.abort();
                panic!("expected {expected}");
            }
        }
    }
}

async fn websocket_backend(
    upgrade: WebSocketUpgrade,
    State(sender): State<mpsc::UnboundedSender<Value>>,
) -> impl IntoResponse {
    upgrade.on_upgrade(async move |mut socket| {
        while let Some(Ok(message)) = socket.recv().await {
            let Ok(text) = message.to_text() else {
                continue;
            };
            let call: Value = serde_json::from_str(text).unwrap();
            sender.send(call.clone()).unwrap();
            socket
                .send(Message::Text(answer(&call).to_string().into()))
                .await
                .unwrap();
        }
    })
}

#[tokio::test]
async fn batch_ws_only_backend_uses_individual_forwarding() {
    let mut h = Harness::new(4, 64).await;
    let (sender, mut incoming) = mpsc::unbounded_channel();
    let router = Router::new()
        .route("/", get(websocket_backend))
        .with_state(sender);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let provider = super::provider::connect_ws(format!("ws://{address}").parse().unwrap())
        .await
        .unwrap();
    let (hard_limit_until, _) = watch::channel(Instant::now());
    let rpc = Arc::new(Web3Rpc {
        name: "ws-only".into(),
        healthy: AtomicBool::new(true),
        ws_provider: ArcSwapOption::from(Some(Arc::new(provider))),
        hard_limit_until: Some(hard_limit_until),
        request_permits: RequestPermits::new(2, 64),
        peak_latency: Some(PeakEwmaLatency::spawn(
            Duration::from_secs(15),
            100,
            Duration::from_secs(1),
        )),
        median_latency: Some(RollingQuantileLatency::spawn_median(100).await),
        ..Default::default()
    });
    let head = h.app.balanced_rpcs.head_block();
    h.app.balanced_rpcs.by_name.write().clear();
    h.app
        .balanced_rpcs
        .by_name
        .write()
        .insert(rpc.name.clone(), rpc.clone());
    h.app
        .balanced_rpcs
        .watch_ranked_rpcs
        .send_replace(Some(Arc::new(RankedRpcs::from_rpcs(
            vec![rpc.clone()],
            head,
            false,
        ))));
    h.rpc = rpc;
    assert_answers(
        timeout(Duration::from_secs(2), h.start(4))
            .await
            .unwrap()
            .unwrap(),
        4,
    );
    for _ in 0..4 {
        let call = incoming.try_recv().unwrap();
        assert!(call.is_object());
        assert_eq!(call["method"], "eth_call");
    }
    assert!(incoming.try_recv().is_err());
    assert_eq!(h.rpc.backend_batch_requests.load(Ordering::Relaxed), 0);
    assert_eq!(h.rpc.total_requests.load(Ordering::Relaxed), 4);
    h.quiet().await;
    h.idle();
    server.abort();
}

#[tokio::test]
async fn individual_stream_shares_batch_slots_until_body_ends_or_is_dropped() {
    for finish_body in [true, false] {
        let mut h = Harness::new(1, 64).await;
        let request = crate::jsonrpc::ValidatedRequest::new_internal(
            "eth_call".into(),
            &sonic_rs::json!([]),
            None,
            None,
        )
        .await
        .unwrap();
        let handle = super::request::OpenRequestHandle::new(request, h.rpc.clone(), None).await;
        let task = tokio::spawn(handle.request::<Arc<sonic_rs::OwnedLazyValue>>());
        let incoming = h.next().await;
        let prefix = format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"{}",
            "x".repeat(140_000)
        );
        let expected = format!("{prefix}\"}}");
        let (sender, receiver) = mpsc::channel::<Result<Bytes, std::io::Error>>(2);
        sender.send(Ok(Bytes::from(prefix))).await.unwrap();
        incoming
            .reply
            .send(
                Response::builder()
                    .header("content-length", expected.len())
                    .body(Body::from_stream(
                        tokio_stream::wrappers::ReceiverStream::new(receiver),
                    ))
                    .unwrap(),
            )
            .unwrap();
        let response = task.await.unwrap().unwrap();
        assert!(matches!(
            response,
            crate::jsonrpc::SingleResponse::Stream(_)
        ));
        assert_eq!(h.rpc.active_requests.load(Ordering::SeqCst), 1);
        let batch = h.start(2);
        h.quiet().await;
        let mut body = response.into_response().into_body().into_data_stream();
        if finish_body {
            sender.send(Ok(Bytes::from_static(b"\"}"))).await.unwrap();
            drop(sender);
            let mut received = Vec::new();
            while let Some(chunk) = futures::StreamExt::next(&mut body).await {
                received.extend(chunk.unwrap());
            }
            assert_eq!(received, expected.as_bytes());
            // Keep the completed body alive: EOF itself must release the slot.
            h.next().await.succeed();
            assert_answers(batch.await.unwrap(), 2);
            h.idle();
        } else {
            drop(body);
            h.next().await.succeed();
            assert_answers(batch.await.unwrap(), 2);
            h.idle();
        }
    }
}

#[tokio::test]
async fn batch_disconnect_after_connection_window_keeps_request_data() {
    let mut h = Harness::new(4, 64).await;
    let task = h.start(2);
    let packet = h.next().await;
    let expected = packet
        .body
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["params"].clone())
        .collect::<Vec<_>>();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(11)).await;
    tokio::time::resume();
    packet
        .reply
        .send(Response::new(Body::from_stream(futures::stream::iter([
            Err::<Bytes, _>(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "backend disconnected",
            )),
        ]))))
        .unwrap();
    let mut retried = Vec::new();
    for _ in 0..2 {
        let call = h.next().await;
        retried.push(call.body["params"].clone());
        call.succeed();
    }
    retried.sort_by_key(Value::to_string);
    assert_eq!(retried, expected);
    assert_answers(task.await.unwrap(), 2);
}
