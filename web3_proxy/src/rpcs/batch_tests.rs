use super::consensus::RankedRpcs;
use super::many::{Web3Rpcs, Web3RpcsSpawnConfig};
use super::one::{RequestPermits, Web3Rpc};
use crate::app::App;
use crate::config::AppConfig;
use crate::errors::Web3ProxyError;
use crate::frontend::rpc_proxy_ws::ProxyMode;
use crate::jsonrpc::{JsonRpcRequestEnum, SingleRequest, ValidatedRequest};
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

pub(super) struct Incoming {
    pub(super) body: Value,
    pub(super) reply: oneshot::Sender<Response<Body>>,
}

impl Incoming {
    pub(super) fn respond(self, body: Value) {
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

pub(super) struct Harness {
    pub(super) app: Arc<App>,
    pub(super) rpc: Arc<Web3Rpc>,
    incoming: mpsc::UnboundedReceiver<Incoming>,
    server: JoinHandle<()>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Harness {
    pub(super) async fn new(concurrency: usize, packet_size: usize) -> Self {
        Self::named("controlled-backend", concurrency, packet_size).await
    }

    pub(super) async fn named(name: &str, concurrency: usize, packet_size: usize) -> Self {
        Self::configured(name, concurrency, packet_size, |_| {}).await
    }

    pub(super) async fn configured(
        name: &str,
        concurrency: usize,
        packet_size: usize,
        configure: impl FnOnce(&mut Web3Rpc),
    ) -> Self {
        let (sender, incoming) = mpsc::unbounded_channel();
        let router = Router::new().route("/", post(receive)).with_state(sender);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (hard_limit_until, _) = watch::channel(Instant::now());
        let mut rpc = Web3Rpc {
            name: name.into(),
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
        };
        configure(&mut rpc);
        let rpc = Arc::new(rpc);
        let (head_sender, watch_consensus_head_receiver) = watch::channel(None);
        let frontend_tasks = tokio_util::task::TaskTracker::new();
        let (frontend_shutdown, _) = watch::channel(false);
        let (balanced_rpcs, background, _) = Web3Rpcs::spawn(
            Web3RpcsSpawnConfig::new(1, None, 0, 0, 1_000_000),
            "batch-test".into(),
            frontend_tasks.clone(),
            frontend_shutdown.subscribe(),
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
            frontend_tasks,
            frontend_shutdown,
            block_relay: crate::block_relay::BlockRelay::new(),
            balanced_rpcs: balanced_rpcs.clone(),
            bundler_4337_rpcs: balanced_rpcs.clone(),
            config: AppConfig::default(),
            fastest_rpcs: watch::channel(crate::config::DEFAULT_FASTEST_RPCS).0,
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
        self.start_requests((0..count).map(client_call).collect())
    }

    fn start_requests(&self, requests: Vec<SingleRequest>) -> JoinHandle<Value> {
        let app = self.app.clone();
        tokio::spawn(async move {
            let (_, response, _) = app
                .proxy_web3_rpc(ProxyMode::Best, JsonRpcRequestEnum::Batch(requests), None)
                .await
                .unwrap();
            serde_json::from_str(&response.to_json_string().await.unwrap()).unwrap()
        })
    }

    pub(super) async fn next(&mut self) -> Incoming {
        timeout(Duration::from_secs(2), self.incoming.recv())
            .await
            .expect("backend traffic must start")
            .unwrap()
    }

    pub(super) async fn quiet(&mut self) {
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

fn client_call(index: usize) -> SingleRequest {
    SingleRequest::new(
        ((index % 2) as u64).into(),
        "eth_call".into(),
        sonic_rs::json!([{
            "to": "0x0000000000000000000000000000000000000000",
            "data": format!("0x{index:04x}"),
        }, "latest"]),
    )
    .unwrap()
}

fn timeout_answer(id: Value) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "error":{"code":408, "message":"request timed out"}})
}

/// Timeout diagnostics include elapsed time and backend metadata. Compare the
/// complete stable response contract while leaving those diagnostics intact.
fn assert_timeout_responses(mut actual: Value, expected: Value) {
    let actual = actual.as_array_mut().unwrap();
    let expected = expected.as_array().unwrap();
    assert_eq!(actual.len(), expected.len());
    for (index, (response, expected)) in actual.iter_mut().zip(expected).enumerate() {
        if response["error"]["code"] == 408 {
            let error = response["error"].as_object_mut().unwrap();
            let diagnostics = error.remove("data").expect("timeouts include diagnostics");
            assert_eq!(diagnostics["duration"], Value::Null);
            assert!(diagnostics["request"].is_object());
        }
        assert_eq!(response, expected, "response at index {index}");
    }
}

pub(super) async fn advance_to(target: Instant) {
    tokio::time::pause();
    tokio::time::advance(target.saturating_duration_since(Instant::now())).await;
    tokio::time::resume();
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
async fn frontend_shutdown_rejects_readiness_and_drains_an_active_http_request() {
    let mut h = Harness::new(1, 64).await;
    let health = crate::frontend::status::health(State(h.app.clone()))
        .await
        .unwrap()
        .into_response();
    assert_eq!(health.status(), 200);
    let mut server = tokio::spawn(crate::frontend::serve(h.app.clone()));
    let port = timeout(Duration::from_secs(2), async {
        loop {
            let port = h.app.frontend_port.load(Ordering::Relaxed);
            if port != 0 {
                break port;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let request = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("http://127.0.0.1:{port}/"))
            .header("x-forwarded-for", "127.0.0.1")
            .header("content-type", "application/json")
            .body(sonic_rs::to_vec(&client_call(0)).unwrap())
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()
    });
    let incoming = h.next().await;
    h.app.frontend_shutdown.send_replace(true);
    let health = crate::frontend::status::health(State(h.app.clone()))
        .await
        .unwrap()
        .into_response();
    assert_eq!(health.status(), 503);
    assert!(
        timeout(Duration::from_millis(30), &mut server)
            .await
            .is_err(),
        "shutdown must wait for the active HTTP request"
    );
    incoming.succeed();
    assert_eq!(
        request.await.unwrap(),
        json!({"jsonrpc":"2.0","id":0,"result":"0x0000"})
    );
    timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
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
    let packet = h.next().await;
    let mut retried = packet_params(&packet);
    packet.succeed();
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
    let task = h.start(4);
    let packet = h.next().await;
    let calls = packet.body.as_array().unwrap();
    let expected_retry = calls[2]["params"].clone();
    let response = json!([
        answer(&calls[0]),
        {"jsonrpc":"2.0","id":calls[1]["id"],"error":{"code":3,"message":"execution reverted"}},
        {"jsonrpc":"2.0","id":calls[2]["id"],"error":{"code":-32005,"message":"rate limit exceeded"}},
        {"jsonrpc":"2.0","id":calls[3]["id"],"error":{"code":-32602,"message":"invalid argument","data":{"field":"to"}}}
    ]);
    let rejected_at = Instant::now();
    packet.respond(response);
    assert!(
        timeout(Duration::from_millis(900), h.incoming.recv())
            .await
            .is_err(),
        "the backend must receive no traffic during cooldown"
    );
    let retry = h.next().await;
    assert!(
        Instant::now() >= rejected_at + Duration::from_secs(1),
        "retry must wait for the full backend cooldown"
    );
    assert_eq!(packet_params(&retry), vec![expected_retry]);
    retry.succeed();
    assert_eq!(
        task.await.unwrap(),
        json!([
            {"jsonrpc":"2.0","id":0,"result":"0x0000"},
            {"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}},
            {"jsonrpc":"2.0","id":0,"result":"0x0002"},
            {"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid argument","data":{"field":"to"}}}
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
async fn batch_invalid_response_ids_retry_as_a_packet() {
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
        let retry = h.next().await;
        assert_eq!(retry.body.as_array().expect("retry stays batched").len(), 2);
        retry.succeed();
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
    let packet = h.next().await;
    let mut retried = packet_params(&packet);
    packet.succeed();
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
    assert_timeout_responses(
        response,
        Value::Array((0..129).map(|i| timeout_answer(json!(i % 2))).collect()),
    );
    assert_eq!(h.rpc.total_requests.load(Ordering::Relaxed), 64);
    assert_eq!(h.rpc.backend_batch_requests.load(Ordering::Relaxed), 1);
    drop(packet);
    h.quiet().await;
    h.idle();
    let task = h.start(2);
    h.next().await.succeed();
    assert_answers(task.await.unwrap(), 2);
}

#[tokio::test]
async fn batch_final_failure_preserves_other_packet_answers() {
    let mut h = Harness::new(1, 2).await;
    let task = h.start(4);
    let first = h.next().await;
    let completed = first
        .body
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["params"][0]["data"].clone())
        .collect::<Vec<_>>();
    first.succeed();
    h.next().await.reject();
    let retry_a = h.next().await;
    h.quiet().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::time::resume();
    let response = timeout(Duration::from_millis(500), task)
        .await
        .expect("fallback must use the original deadline")
        .unwrap();
    let expected = (0..4)
        .map(|i| {
            let value = json!(format!("0x{i:04x}"));
            if completed.contains(&value) {
                json!({"jsonrpc":"2.0", "id":i % 2, "result":value})
            } else {
                timeout_answer(json!(i % 2))
            }
        })
        .collect();
    assert_timeout_responses(response, Value::Array(expected));
    drop(retry_a);
    h.quiet().await;
    h.idle();
}

#[tokio::test]
async fn batch_large_retry_error_is_checked_before_success() {
    let mut h = Harness::new(4, 64).await;
    let task = h.start(2);
    h.next().await.reject();
    let first = h.next().await;
    let calls = first.body.as_array().expect("retry stays batched");
    let retry_params = calls[0]["params"].clone();
    let response = json!([
        {"jsonrpc":"2.0","id":calls[0]["id"],"error":{"code":-32005,"message":"rate limit exceeded","data":"x".repeat(140_000)}},
        answer(&calls[1])
    ]);
    first.respond(response);
    let retry = h.next().await;
    assert_eq!(packet_params(&retry), vec![retry_params]);
    retry.succeed();
    assert_answers(task.await.unwrap(), 2);
    h.idle();
}

#[tokio::test]
async fn batch_large_retry_body_obeys_original_deadline() {
    let mut h = Harness::new(1, 2).await;
    let task = h.start(4);
    h.next().await.succeed();
    h.next().await.reject();
    let retry = h.next().await;
    let calls = retry.body.as_array().expect("retry stays batched");
    let prefix = format!(
        "[{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":\"{}",
        calls[0]["id"],
        "x".repeat(140_000)
    );
    let sender = send_stalled_body(retry, prefix).await;
    advance_to(Instant::now() + Duration::from_secs(60)).await;
    let response = timeout(Duration::from_millis(500), task)
        .await
        .expect("large retry body must end at the original deadline")
        .unwrap();
    assert_timeout_responses(
        response,
        json!([
            {"jsonrpc":"2.0","id":0,"result":"0x0000"},
            {"jsonrpc":"2.0","id":1,"result":"0x0001"},
            timeout_answer(json!(0)), timeout_answer(json!(1))
        ]),
    );
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
    State(sender): State<mpsc::UnboundedSender<Incoming>>,
) -> impl IntoResponse {
    use futures::{stream::FuturesUnordered, StreamExt};

    upgrade.on_upgrade(async move |mut socket| {
        let mut replies = FuturesUnordered::new();
        loop {
            tokio::select! {
                message = socket.recv() => {
                    let text = match message {
                        Some(Ok(Message::Text(text))) => text,
                        Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                        _ => break,
                    };
                    let (reply, response) = oneshot::channel();
                    if sender.send(Incoming {
                        body: serde_json::from_str(&text).unwrap(),
                        reply,
                    }).is_err() { break; }
                    replies.push(async move {
                        let response = response.await.ok()?;
                        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.ok()?;
                        String::from_utf8(bytes.to_vec()).ok()
                    });
                }
                response = replies.next(), if !replies.is_empty() => {
                    if let Some(Some(text)) = response {
                        if socket.send(Message::Text(text.into())).await.is_err() { break; }
                    }
                }
            }
        }
    })
}

pub(super) struct WebSocketHarness {
    pub(super) rpc: Arc<Web3Rpc>,
    incoming: mpsc::UnboundedReceiver<Incoming>,
    server: JoinHandle<()>,
}

impl Drop for WebSocketHarness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl WebSocketHarness {
    pub(super) async fn new(concurrency: usize) -> Self {
        let (sender, incoming) = mpsc::unbounded_channel();
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
            request_permits: RequestPermits::new(concurrency, 64),
            peak_latency: Some(PeakEwmaLatency::spawn(
                Duration::from_secs(15),
                100,
                Duration::from_secs(1),
            )),
            median_latency: Some(RollingQuantileLatency::spawn_median(100).await),
            ..Default::default()
        });
        Self {
            rpc,
            incoming,
            server,
        }
    }

    pub(super) async fn next(&mut self) -> Incoming {
        let call = timeout(Duration::from_secs(2), self.incoming.recv())
            .await
            .expect("WebSocket fallback must start")
            .unwrap();
        assert!(
            call.body.is_object(),
            "WebSocket forwarding must be individual"
        );
        assert_eq!(call.body["method"], "eth_call");
        assert_eq!(call.body["params"][1], "0x2a");
        call
    }

    pub(super) async fn quiet(&mut self) {
        match timeout(Duration::from_millis(50), self.incoming.recv()).await {
            Err(_) => {}
            Ok(Some(call)) => panic!("unexpected WebSocket request: {}", call.body),
            Ok(None) => panic!("WebSocket test connection closed"),
        }
    }

    fn completed(&self, calls: usize) {
        assert_eq!(self.rpc.total_requests.load(Ordering::Relaxed), calls);
        assert_eq!(self.rpc.backend_batch_requests.load(Ordering::Relaxed), 0);
        assert_eq!(self.rpc.active_requests.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn batch_ws_only_backend_uses_individual_forwarding() {
    let mut h = Harness::new(4, 64).await;
    let mut ws = WebSocketHarness::new(2).await;
    h.app.balanced_rpcs.by_name.write().clear();
    add_backend(&h.app, vec![ws.rpc.clone()]);
    let task = h.start(4);
    for _ in 0..4 {
        ws.next().await.succeed();
    }
    assert_answers(task.await.unwrap(), 4);
    ws.quiet().await;
    ws.completed(4);
    h.quiet().await;
    h.idle();
}

#[tokio::test]
async fn batch_mixed_pool_uses_websocket_when_http_is_unavailable() {
    for unavailable in ["unhealthy", "cooldown", "full"] {
        let mut h = Harness::new(1, 64).await;
        let mut ws = WebSocketHarness::new(2).await;
        let held = match unavailable {
            "unhealthy" => {
                h.rpc.healthy.store(false, Ordering::SeqCst);
                None
            }
            "cooldown" => {
                h.rpc
                    .hard_limit_until
                    .as_ref()
                    .unwrap()
                    .send_replace(Instant::now() + Duration::from_secs(60));
                None
            }
            "full" => {
                let task = h.start(2);
                Some((task, h.next().await))
            }
            _ => unreachable!(),
        };
        add_backend(&h.app, vec![h.rpc.clone(), ws.rpc.clone()]);
        let task = h.start(4);
        let first = ws.next().await;
        let second = ws.next().await;
        assert_eq!(ws.rpc.active_requests.load(Ordering::SeqCst), 2);
        ws.quiet().await;
        second.succeed();
        first.succeed();
        ws.next().await.succeed();
        ws.next().await.succeed();
        assert_answers(
            timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap(),
            4,
        );
        ws.completed(4);
        ws.quiet().await;
        h.quiet().await;
        if let Some((task, packet)) = held {
            packet.succeed();
            assert_answers(task.await.unwrap(), 2);
            assert_eq!(h.rpc.total_requests.load(Ordering::Relaxed), 2);
            assert_eq!(h.rpc.backend_batch_requests.load(Ordering::Relaxed), 1);
        } else {
            assert_eq!(h.rpc.total_requests.load(Ordering::Relaxed), 0);
            assert_eq!(h.rpc.backend_batch_requests.load(Ordering::Relaxed), 0);
        }
        h.idle();
    }
}

#[tokio::test]
async fn batch_mixed_pool_retries_failed_http_on_websocket_without_repeating_successes() {
    let mut h = Harness::new(3, 2).await;
    let mut ws = WebSocketHarness::new(2).await;
    add_backend(&h.app, vec![h.rpc.clone(), ws.rpc.clone()]);
    let task = h.start(6);
    let success = h.next().await;
    let blocked = h.next().await;
    let failed = h.next().await;
    let expected = packet_params(&failed);
    ws.quiet().await;
    success.succeed();
    advance_to(Instant::now() + Duration::from_secs(11)).await;
    failed.reject();
    let first = ws.next().await;
    let second = ws.next().await;
    let actual = vec![first.body["params"].clone(), second.body["params"].clone()];
    assert_eq!(actual, expected);
    second.succeed();
    first.succeed();
    ws.quiet().await;
    assert!(
        !task.is_finished(),
        "the unrelated HTTP packet is still blocked"
    );
    blocked.succeed();
    assert_answers(task.await.unwrap(), 6);
    h.quiet().await;
    ws.completed(2);
    assert_eq!(h.rpc.total_requests.load(Ordering::Relaxed), 6);
    assert_eq!(h.rpc.backend_batch_requests.load(Ordering::Relaxed), 3);
    h.idle();
}

#[tokio::test]
async fn batch_mixed_pool_requeues_websocket_errors_as_http_packets() {
    let mut h = Harness::new(1, 64).await;
    let mut ws = WebSocketHarness::new(2).await;
    h.rpc
        .hard_limit_until
        .as_ref()
        .unwrap()
        .send_replace(Instant::now() + Duration::from_secs(60));
    add_backend(&h.app, vec![h.rpc.clone(), ws.rpc.clone()]);
    let task = h.start(2);
    let success = ws.next().await;
    let limited = ws.next().await;
    let expected = limited.body["params"].clone();
    let id = limited.body["id"].clone();
    h.rpc
        .hard_limit_until
        .as_ref()
        .unwrap()
        .send_replace(Instant::now());
    success.succeed();
    limited.respond(json!({"jsonrpc":"2.0", "id":id,
        "error":{"code":-32005, "message":"rate limit exceeded"}}));
    let retry = h.next().await;
    assert_eq!(packet_params(&retry), vec![expected]);
    retry.succeed();
    assert_answers(task.await.unwrap(), 2);
    assert!(ws.rpc.next_available(Instant::now()) > Instant::now());
    ws.quiet().await;
    ws.completed(2);
    assert_eq!(h.rpc.total_requests.load(Ordering::Relaxed), 1);
    assert_eq!(h.rpc.backend_batch_requests.load(Ordering::Relaxed), 1);
    h.idle();
}

#[tokio::test]
async fn batch_mixed_pool_websocket_timeout_and_cancellation_release_slots() {
    for cancel in [false, true] {
        let mut h = Harness::new(1, 64).await;
        let mut ws = WebSocketHarness::new(1).await;
        add_backend(&h.app, vec![h.rpc.clone(), ws.rpc.clone()]);
        let task = h.start(3);
        let failed = h.next().await;
        assert_eq!(packet_params(&failed).len(), 3);
        if !cancel {
            advance_to(Instant::now() + Duration::from_secs(50)).await;
        }
        h.rpc
            .hard_limit_until
            .as_ref()
            .unwrap()
            .send_replace(Instant::now() + Duration::from_secs(120));
        failed.reject();
        let stalled = ws.next().await;
        ws.quiet().await;
        if cancel {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            advance_to(Instant::now() + Duration::from_secs(10)).await;
            let response = timeout(Duration::from_millis(500), task)
                .await
                .unwrap()
                .unwrap();
            assert_timeout_responses(
                response,
                json!([
                    timeout_answer(json!(0)),
                    timeout_answer(json!(1)),
                    timeout_answer(json!(0))
                ]),
            );
        }
        ws.completed(1);
        ws.quiet().await;
        let next = h.start(2);
        ws.next().await.succeed();
        ws.next().await.succeed();
        assert_answers(next.await.unwrap(), 2);
        ws.completed(3);
        h.quiet().await;
        assert_eq!(h.rpc.total_requests.load(Ordering::Relaxed), 3);
        assert_eq!(h.rpc.backend_batch_requests.load(Ordering::Relaxed), 1);
        h.idle();
        drop(stalled);
    }
}

#[tokio::test]
async fn individual_response_holds_batch_slot_until_validated_or_cancelled() {
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
        let handle = h
            .rpc
            .wait_for_request_handle(&request, None, true)
            .await
            .unwrap();
        let task = tokio::spawn(handle.request::<Arc<sonic_rs::OwnedLazyValue>>());
        let incoming = h.next().await;
        let prefix = format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":\"{}",
            incoming.body["id"],
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
        assert!(!task.is_finished());
        assert_eq!(h.rpc.active_requests.load(Ordering::SeqCst), 1);
        let batch = h.start(2);
        h.quiet().await;
        if finish_body {
            sender.send(Ok(Bytes::from_static(b"\"}"))).await.unwrap();
            drop(sender);
            let response = task.await.unwrap().unwrap();
            let body = axum::body::to_bytes(response.into_response().into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(body, expected.as_bytes());
        } else {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        }
        h.next().await.succeed();
        assert_answers(batch.await.unwrap(), 2);
        h.idle();
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
    let packet = h.next().await;
    let mut retried = packet_params(&packet);
    packet.succeed();
    retried.sort_by_key(Value::to_string);
    assert_eq!(retried, expected);
    assert_answers(task.await.unwrap(), 2);
}

#[tokio::test]
async fn batch_failure_at_fifty_seconds_leaves_ten_seconds_for_active_and_queued_packets() {
    let mut h = Harness::new(1, 2).await;
    let started = Instant::now();
    let mut task = h.start(4);
    let packet = h.next().await;
    // Validation starts between task creation and the first backend packet.
    let deadline_upper_bound = Instant::now() + Duration::from_secs(60);
    advance_to(started + Duration::from_secs(50)).await;
    packet.reject();
    let active = h.next().await;
    assert_eq!(
        active.body.as_array().expect("retry stays batched").len(),
        2
    );
    h.quiet().await;
    advance_to(started + Duration::from_secs(59)).await;
    assert!(
        timeout(Duration::from_millis(50), &mut task).await.is_err(),
        "fallback must remain active while the original request has time left"
    );
    advance_to(deadline_upper_bound).await;
    let response = timeout(Duration::from_millis(500), task)
        .await
        .expect("fallback must end at the original deadline, not sixty seconds after the retry")
        .unwrap();
    assert_timeout_responses(
        response,
        json!([
            timeout_answer(json!(0)),
            timeout_answer(json!(1)),
            timeout_answer(json!(0)),
            timeout_answer(json!(1))
        ]),
    );
    drop(active);
    h.quiet().await;
    assert_eq!(
        h.rpc.total_requests.load(Ordering::Relaxed),
        4,
        "the queued fallback must not reach the backend after expiry"
    );
    h.idle();
}

#[tokio::test]
async fn batch_failure_uses_another_backend_without_changing_the_selected_block() {
    let mut first = Harness::named("backend-a", 1, 2).await;
    let mut second = Harness::named("backend-b", 1, 2).await;
    let task = first.start(4);
    let completed = first.next().await;
    completed.succeed();
    let failed = first.next().await;
    let expected_calls = packet_params(&failed);
    assert!(expected_calls.iter().all(|params| params[1] == "0x2a"));
    // Keep A present but unavailable. B becomes ready at a newer head.
    first.rpc.healthy.store(false, Ordering::SeqCst);
    let mut header: Header = Header::default();
    header.inner.number = 43;
    let head = BlockHeader::new(Arc::new(header));
    first
        .app
        .balanced_rpcs
        .watch_head_block
        .as_ref()
        .unwrap()
        .send_replace(Some(head.clone()));
    first
        .app
        .balanced_rpcs
        .by_name
        .write()
        .insert(second.rpc.name.clone(), second.rpc.clone());
    first
        .app
        .balanced_rpcs
        .watch_ranked_rpcs
        .send_replace(Some(Arc::new(RankedRpcs::from_rpcs(
            vec![first.rpc.clone(), second.rpc.clone()],
            Some(head),
            false,
        ))));
    failed.reject();
    let retry = second.next().await;
    assert_eq!(packet_params(&retry), expected_calls);
    retry.succeed();
    assert_answers(task.await.unwrap(), 4);
    first.quiet().await;
    second.quiet().await;
    assert_eq!(first.rpc.total_requests.load(Ordering::Relaxed), 4);
    assert_eq!(first.rpc.backend_batch_requests.load(Ordering::Relaxed), 2);
    assert_eq!(second.rpc.total_requests.load(Ordering::Relaxed), 2);
    assert_eq!(second.rpc.backend_batch_requests.load(Ordering::Relaxed), 1);
    first.idle();
    second.idle();
}

#[tokio::test]
async fn batch_earliest_deadline_expires_only_the_call_without_remaining_time() {
    let mut h = Harness::new(1, 64).await;
    let mut requests = Vec::new();
    for (index, lifetime) in [10, 60].into_iter().enumerate() {
        requests.push(
            ValidatedRequest::new_with_app(
                &h.app,
                ProxyMode::Best,
                Some(Duration::from_secs(lifetime)),
                client_call(index).into(),
                h.app.balanced_rpcs.head_block(),
                None,
            )
            .await
            .unwrap(),
        );
    }
    let earliest = requests[0].expire_at();
    let later = requests[1].expire_at();
    let handle = h
        .rpc
        .wait_for_request_handle(&requests[0], None, true)
        .await
        .unwrap();
    let packet_requests = requests.clone();
    let packet_task = tokio::spawn(async move { handle.request_batch(&packet_requests).await });
    let packet = h.next().await;
    advance_to(earliest).await;
    let result = timeout(Duration::from_millis(500), packet_task)
        .await
        .expect("the earliest call deadline must stop the packet")
        .unwrap();
    assert!(
        matches!(result, Err(Web3ProxyError::Timeout(None))),
        "{result:?}"
    );
    assert!(Instant::now() < later);
    h.idle();
    let expired = h
        .app
        .balanced_rpcs
        .continue_request::<Arc<sonic_rs::OwnedLazyValue>>(&requests[0])
        .await;
    assert!(
        matches!(expired, Err(Web3ProxyError::Timeout(None))),
        "{expired:?}"
    );
    let balanced = h.app.balanced_rpcs.clone();
    let remaining = requests[1].clone();
    let retry_task = tokio::spawn(async move {
        balanced
            .continue_request::<Arc<sonic_rs::OwnedLazyValue>>(&remaining)
            .await
    });
    let retry = h.next().await;
    assert_eq!(
        retry.body,
        json!({"jsonrpc":"2.0", "id":1, "method":"eth_call",
        "params":[{"to":"0x0000000000000000000000000000000000000000","data":"0x0001"}, "0x2a"]})
    );
    retry.succeed();
    let response = retry_task.await.unwrap().unwrap().parsed().await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&sonic_rs::to_string(&response).unwrap()).unwrap(),
        json!({"jsonrpc":"2.0","id":1,"result":"0x0001"})
    );
    assert_eq!(requests[0].expire_at(), earliest);
    assert_eq!(requests[1].expire_at(), later);
    assert_eq!(h.rpc.total_requests.load(Ordering::Relaxed), 3);
    assert_eq!(h.rpc.backend_batch_requests.load(Ordering::Relaxed), 1);
    drop(packet);
    h.quiet().await;
    h.idle();
}

fn packet_params(packet: &Incoming) -> Vec<Value> {
    packet
        .body
        .as_array()
        .expect("HTTP retries must stay batched")
        .iter()
        .map(|call| call["params"].clone())
        .collect()
}

pub(super) async fn send_stalled_body(
    incoming: Incoming,
    prefix: String,
) -> mpsc::Sender<Result<Bytes, std::io::Error>> {
    let (sender, receiver) = mpsc::channel(2);
    sender.send(Ok(Bytes::from(prefix))).await.unwrap();
    incoming
        .reply
        .send(
            Response::builder()
                .body(Body::from_stream(
                    tokio_stream::wrappers::ReceiverStream::new(receiver),
                ))
                .unwrap(),
        )
        .unwrap();
    sender
}

fn code_call(index: usize) -> SingleRequest {
    SingleRequest::new(
        (index as u64).into(),
        "eth_getCode".into(),
        sonic_rs::json!(["0x0000000000000000000000000000000000000000", "latest"]),
    )
    .unwrap()
}

fn add_backend(app: &Arc<App>, rpcs: Vec<Arc<Web3Rpc>>) {
    let head = app.balanced_rpcs.head_block().unwrap();
    for rpc in &rpcs {
        app.balanced_rpcs
            .by_name
            .write()
            .insert(rpc.name.clone(), rpc.clone());
    }
    let votes = hashbrown::HashMap::from([(head.clone(), (rpcs.iter().collect(), 2))]);
    let heads = rpcs.iter().map(|rpc| (rpc.clone(), head.clone())).collect();
    let ranked = RankedRpcs::from_votes(1, 1, alloy::primitives::U64::ZERO, votes, heads).unwrap();
    app.balanced_rpcs
        .watch_ranked_rpcs
        .send_replace(Some(Arc::new(ranked)));
}

#[tokio::test]
async fn ordinary_batch_drains_each_large_body_before_waiting_for_queued_calls() {
    let mut h = Harness::new(1, 64).await;
    let task = h.start_requests(vec![code_call(0), code_call(1)]);
    let data = format!("0x{}", "ab".repeat(70_000));
    for id in 0..2 {
        let call = h.next().await;
        assert_eq!(call.body["id"], id);
        call.respond(json!({"jsonrpc":"2.0","id":id,"result":data}));
    }
    assert_eq!(
        task.await.unwrap(),
        json!([
            {"jsonrpc":"2.0","id":0,"result":data},
            {"jsonrpc":"2.0","id":1,"result":data}
        ])
    );
    assert_eq!(h.rpc.total_requests.load(Ordering::Relaxed), 2);
    h.idle();
}

#[tokio::test]
async fn ordinary_batch_stalled_body_keeps_its_original_deadline_and_other_answers() {
    let mut h = Harness::new(1, 64).await;
    let task = h.start_requests(vec![code_call(0), code_call(1)]);
    let first = h.next().await;
    first.respond(json!({"jsonrpc":"2.0","id":0,"result":"0xbeef"}));
    let second = h.next().await;
    let sender = send_stalled_body(
        second,
        format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"{}",
            "x".repeat(140_000)
        ),
    )
    .await;
    advance_to(Instant::now() + Duration::from_secs(60)).await;
    let response = timeout(Duration::from_millis(500), task)
        .await
        .expect("the body must obey the call deadline")
        .unwrap();
    assert_timeout_responses(
        response,
        json!([
            {"jsonrpc":"2.0","id":0,"result":"0xbeef"}, timeout_answer(json!(1))
        ]),
    );
    drop(sender);
    h.idle();
}

#[tokio::test]
async fn routing_skips_a_full_preferred_backend_for_individual_calls_and_packets() {
    for batched in [false, true] {
        let mut first = Harness::named("preferred", 1, 64).await;
        let mut second = Harness::named("available", 1, 64).await;
        second.rpc.tier.store(10, Ordering::SeqCst);
        let held = first.start(2);
        let occupied = first.next().await;
        add_backend(&first.app, vec![first.rpc.clone(), second.rpc.clone()]);
        let task = if batched {
            first.start(64)
        } else {
            first.start_requests(vec![code_call(0), code_call(1)])
        };
        if batched {
            let packet = second.next().await;
            assert_eq!(packet.body.as_array().unwrap().len(), 64);
            packet.succeed();
            assert_answers(
                timeout(Duration::from_millis(500), task)
                    .await
                    .unwrap()
                    .unwrap(),
                64,
            );
        } else {
            for id in 0..2 {
                let call = second.next().await;
                assert_eq!(call.body["method"], "eth_getCode");
                call.respond(json!({"jsonrpc":"2.0","id":id,"result":"0xbeef"}));
            }
            assert_eq!(
                timeout(Duration::from_millis(500), task)
                    .await
                    .unwrap()
                    .unwrap(),
                json!([
                    {"jsonrpc":"2.0","id":0,"result":"0xbeef"},
                    {"jsonrpc":"2.0","id":1,"result":"0xbeef"}
                ])
            );
        }
        first.quiet().await;
        occupied.succeed();
        assert_answers(held.await.unwrap(), 2);
        first.idle();
        second.idle();
    }
}

#[tokio::test]
async fn queued_calls_and_packets_recheck_cooldown_before_submission() {
    for batched in [false, true] {
        for http_status in [false, true] {
            let mut h = Harness::new(1, 2).await;
            let task = if batched {
                h.start(4)
            } else {
                h.start_requests(vec![code_call(0), code_call(1)])
            };
            let first = h.next().await;
            h.quiet().await;
            let error = |id: &Value| json!({"jsonrpc":"2.0","id":id,"error":{"code":-32005,"message":"rate limit exceeded"}});
            if http_status {
                first
                    .reply
                    .send(Response::builder().status(429).body(Body::empty()).unwrap())
                    .unwrap();
            } else {
                let response = if batched {
                    Value::Array(
                        first
                            .body
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|c| error(&c["id"]))
                            .collect(),
                    )
                } else {
                    error(&first.body["id"])
                };
                first.respond(response);
            }
            assert!(
                timeout(Duration::from_millis(850), h.incoming.recv())
                    .await
                    .is_err(),
                "queued work bypassed a newly applied cooldown"
            );
            for _ in 0..2 {
                let incoming = h.next().await;
                if batched {
                    assert_eq!(packet_params(&incoming).len(), 2);
                    incoming.succeed();
                } else {
                    let id = incoming.body["id"].clone();
                    incoming.respond(json!({"jsonrpc":"2.0","id":id,"result":"0xbeef"}));
                }
            }
            let response = task.await.unwrap();
            if batched {
                assert_answers(response, 4);
            } else {
                assert_eq!(
                    response,
                    json!([
                        {"jsonrpc":"2.0","id":0,"result":"0xbeef"}, {"jsonrpc":"2.0","id":1,"result":"0xbeef"}
                    ])
                );
            }
            h.idle();
        }
    }
}

#[tokio::test]
async fn batch_retry_moves_to_ready_backend_and_uses_its_packet_limit() {
    for packet_size in [64, 16] {
        let mut first = Harness::named("failed-node", 2, 64).await;
        let mut second = Harness::named("ready-node", 2, packet_size).await;
        let task = first.start(128);
        let failed = first.next().await;
        let blocked = first.next().await;
        let mut expected = packet_params(&failed);
        add_backend(&first.app, vec![first.rpc.clone(), second.rpc.clone()]);
        first
            .rpc
            .hard_limit_until
            .as_ref()
            .unwrap()
            .send_replace(Instant::now() + Duration::from_secs(30));
        failed.reject();
        let mut actual = Vec::new();
        for _ in 0..64 / packet_size {
            let packet = second.next().await;
            assert_eq!(packet_params(&packet).len(), packet_size);
            actual.extend(packet_params(&packet));
            packet.succeed();
        }
        actual.sort_by_key(Value::to_string);
        expected.sort_by_key(Value::to_string);
        assert_eq!(actual, expected);
        first.quiet().await;
        blocked.succeed();
        assert_answers(task.await.unwrap(), 128);
        assert_eq!(first.rpc.total_requests.load(Ordering::Relaxed), 128);
        assert_eq!(second.rpc.total_requests.load(Ordering::Relaxed), 64);
        assert_eq!(
            second.rpc.backend_batch_requests.load(Ordering::Relaxed),
            64 / packet_size
        );
        first.idle();
        second.idle();
    }
}

#[tokio::test]
async fn batch_thousands_of_calls_keep_packet_throughput_after_failure() {
    let mut first = Harness::named("failed-node", 2, 64).await;
    let mut second = Harness::named("ready-node", 2, 64).await;
    let task = first.start(4096);
    let failed = first.next().await;
    let blocked = first.next().await;
    add_backend(&first.app, vec![first.rpc.clone(), second.rpc.clone()]);
    first
        .rpc
        .hard_limit_until
        .as_ref()
        .unwrap()
        .send_replace(Instant::now() + Duration::from_secs(30));
    failed.reject();
    for pair in 0..32 {
        let a = second.next().await;
        assert_eq!(packet_params(&a).len(), 64);
        if pair < 31 {
            let b = second.next().await;
            assert_eq!(packet_params(&b).len(), 64);
            assert_eq!(second.rpc.active_requests.load(Ordering::SeqCst), 2);
            second.quiet().await;
            a.succeed();
            b.succeed();
        } else {
            a.succeed();
        }
    }
    first.quiet().await;
    blocked.succeed();
    assert_answers(task.await.unwrap(), 4096);
    assert_eq!(first.rpc.backend_batch_requests.load(Ordering::Relaxed), 2);
    assert_eq!(
        second.rpc.backend_batch_requests.load(Ordering::Relaxed),
        63
    );
    assert_eq!(second.rpc.total_requests.load(Ordering::Relaxed), 4032);
    first.idle();
    second.idle();
}

#[tokio::test]
async fn batch_block_hash_validation_is_bounded_concurrent_and_reused() {
    let mut h = Harness::new(128, 64).await;
    let requests = (0..65)
        .map(|index| {
            let mut request = client_call(index);
            request.params[1] = sonic_rs::json!({"blockHash": format!("0x{:064x}", index + 1)});
            request
        })
        .collect();
    let task = h.start_requests(requests);
    let mut lookups = Vec::new();
    for _ in 0..64 {
        let lookup = h.next().await;
        assert_eq!(lookup.body["method"], "eth_getBlockByHash");
        lookups.push(lookup);
    }
    h.quiet().await;
    let respond_header = |lookup: Incoming| {
        let mut block: alloy::rpc::types::Block = alloy::rpc::types::Block::default();
        let hash = lookup.body["params"][0].as_str().unwrap();
        block.header.hash = hash.parse().unwrap();
        block.header.inner.number = (u64::from_str_radix(&hash[2..], 16).unwrap() % 30) + 1;
        let id = lookup.body["id"].clone();
        lookup.respond(json!({"jsonrpc":"2.0","id":id,"result":block}));
    };
    for lookup in lookups {
        respond_header(lookup);
    }
    let last = h.next().await;
    assert_eq!(last.body["method"], "eth_getBlockByHash");
    // Removing cached headers makes a second validation pass observable on the wire.
    h.app.balanced_rpcs.blocks_by_hash.invalidate_all();
    respond_header(last);
    for _ in 0..65 {
        let call = h.next().await;
        assert!(
            call.body.is_object(),
            "differing blocks retain individual forwarding"
        );
        assert_eq!(
            call.body["method"], "eth_call",
            "validated calls must not repeat block lookups"
        );
        call.succeed();
    }
    assert_answers(task.await.unwrap(), 65);
    assert_eq!(h.rpc.total_requests.load(Ordering::Relaxed), 130);
    h.idle();
}

#[tokio::test]
async fn queued_direct_call_keeps_response_deadline_after_connection_window_and_cooldown() {
    let mut h = Harness::new(1, 64).await;
    let held = h.start(2);
    let occupied = h.next().await;
    let request = ValidatedRequest::new_internal(
        "eth_getCode".into(),
        &sonic_rs::json!(["0x0000000000000000000000000000000000000000", "latest"]),
        None,
        None,
    )
    .await
    .unwrap();
    let deadline = request.expire_at();
    let rpc = h.rpc.clone();
    let original = request.clone();
    let queued = tokio::spawn(async move {
        rpc.authorized_request::<Arc<sonic_rs::OwnedLazyValue>>(&original, None, false)
            .await
    });
    h.quiet().await;
    advance_to(Instant::now() + Duration::from_secs(11)).await;
    h.rpc
        .hard_limit_until
        .as_ref()
        .unwrap()
        .send_replace(Instant::now() + Duration::from_secs(1));
    occupied.succeed();
    assert_answers(held.await.unwrap(), 2);
    h.quiet().await;
    advance_to(Instant::now() + Duration::from_secs(1)).await;
    let call = h.next().await;
    let id = call.body["id"].clone();
    call.respond(json!({"jsonrpc":"2.0","id":id,"result":"0xbeef"}));
    let response = queued.await.unwrap().unwrap();
    assert_eq!(sonic_rs::to_string(&response).unwrap(), "\"0xbeef\"");
    assert_eq!(request.expire_at(), deadline);
    h.idle();
}

#[tokio::test]
async fn queued_individual_retry_exhausts_a_method_failure_before_trying_another_node() {
    let mut first = Harness::named("preferred", 1, 64).await;
    let mut second = Harness::named("second", 1, 64).await;
    second.rpc.tier.store(10, Ordering::SeqCst);
    let held_a = first.start(2);
    let occupied_a = first.next().await;
    let held_b = second.start(2);
    let occupied_b = second.next().await;
    add_backend(&first.app, vec![first.rpc.clone(), second.rpc.clone()]);
    let task = first.start_requests(vec![code_call(0)]);
    first.quiet().await;
    second.quiet().await;
    occupied_a.succeed();
    assert_answers(held_a.await.unwrap(), 2);
    let failed = first.next().await;
    let id = failed.body["id"].clone();
    failed.respond(
        json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"Method not found"}}),
    );
    first.quiet().await;
    occupied_b.succeed();
    assert_answers(held_b.await.unwrap(), 2);
    let retry = second.next().await;
    retry.respond(json!({"jsonrpc":"2.0","id":0,"result":"0xbeef"}));
    assert_eq!(
        task.await.unwrap(),
        json!([{"jsonrpc":"2.0","id":0,"result":"0xbeef"}])
    );
    assert_eq!(first.rpc.total_requests.load(Ordering::Relaxed), 3);
    assert_eq!(second.rpc.total_requests.load(Ordering::Relaxed), 3);
    first.idle();
    second.idle();
}

#[tokio::test]
async fn ordinary_batch_starts_ready_calls_while_other_validation_is_blocked() {
    let mut h = Harness::new(2, 64).await;
    let mut historical = client_call(1);
    historical.params[1] = sonic_rs::json!({"blockHash": format!("0x{:064x}", 1)});
    let task = h.start_requests(vec![code_call(0), historical]);
    let first = h.next().await;
    let second = h.next().await;
    let (lookup, code) = if first.body["method"] == "eth_getBlockByHash" {
        (first, second)
    } else {
        (second, first)
    };
    assert_eq!(code.body["method"], "eth_getCode");
    assert_eq!(lookup.body["method"], "eth_getBlockByHash");
    code.respond(json!({"jsonrpc":"2.0","id":0,"result":"0xbeef"}));
    h.quiet().await;
    let mut block: alloy::rpc::types::Block = alloy::rpc::types::Block::default();
    block.header.hash = lookup.body["params"][0].as_str().unwrap().parse().unwrap();
    block.header.inner.number = 1;
    let id = lookup.body["id"].clone();
    lookup.respond(json!({"jsonrpc":"2.0","id":id,"result":block}));
    let call = h.next().await;
    assert_eq!(call.body["method"], "eth_call");
    call.succeed();
    assert_eq!(
        task.await.unwrap(),
        json!([
            {"jsonrpc":"2.0","id":0,"result":"0xbeef"},
            {"jsonrpc":"2.0","id":1,"result":"0x0001"}
        ])
    );
    h.idle();
}
