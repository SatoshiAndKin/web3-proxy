//! Controlled HTTP servers only. No Ethereum clients or external endpoints.
use super::{
    batch_tests::{advance_to, send_stalled_body, Harness, Incoming},
    blockchain::{BlockHeader, HeadObservationPublisher},
    consensus::RankedRpcs,
};
use crate::{
    app::App,
    config::{AppConfig, TopConfig},
    errors::{Web3ProxyError, Web3ProxyResult},
    frontend::rpc_proxy_ws::ProxyMode,
    jsonrpc::{JsonRpcRequestEnum, SingleRequest, SingleResponse, ValidatedRequest},
};
use alloy::primitives::{U256, U64};
use alloy::providers::Provider;
use alloy::rpc::types::Header;
use hashbrown::HashMap;
use serde_json::{json, Value};
use std::sync::{atomic::Ordering, Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{timeout, Duration, Instant},
};

struct Fleet {
    nodes: Vec<Harness>,
    app: Arc<App>,
}
impl Fleet {
    async fn new(synced: usize, total: usize) -> Self {
        Self::configured(synced, total, AppConfig::default()).await
    }

    async fn configured(synced: usize, total: usize, config: AppConfig) -> Self {
        let mut nodes = Vec::new();
        for i in 0..total {
            let node = Harness::configured(&format!("node-{i}"), 4, 64, |rpc| {
                let mut header: Header = Header::default();
                header.inner.number = 42;
                rpc.head_block_sender =
                    Some(watch::channel(Some(BlockHeader::new(Arc::new(header)))).0);
                rpc.head_observation_publisher =
                    Some(HeadObservationPublisher::new(mpsc::unbounded_channel().0));
                rpc.block_data_limit.store(u64::MAX, Ordering::SeqCst);
                rpc.log_data_limit.store(u64::MAX, Ordering::SeqCst);
            })
            .await;
            node.rpc.tier.store(i as u32 + 1, Ordering::SeqCst);
            nodes.push(node);
        }
        let app = Arc::get_mut(&mut nodes[0].app).unwrap();
        app.fastest_rpcs.send_replace(config.fastest_rpcs);
        app.config = config;
        let app = nodes[0].app.clone();
        let fleet = Self { nodes, app };
        fleet.sync(synced);
        fleet
    }
    fn sync(&self, synced: usize) {
        let head = self.app.balanced_rpcs.head_block().unwrap();
        let rpcs: Vec<_> = self.nodes.iter().map(|n| n.rpc.clone()).collect();
        let ranked = RankedRpcs::from_votes(
            1,
            1,
            U64::ZERO,
            HashMap::from([(
                head.clone(),
                (rpcs.iter().take(synced).collect(), synced as u32),
            )]),
            rpcs.iter().map(|rpc| (rpc.clone(), head.clone())).collect(),
        );
        self.app
            .balanced_rpcs
            .watch_ranked_rpcs
            .send_replace(ranked.map(Arc::new));
        *self.app.balanced_rpcs.by_name.write() =
            rpcs.into_iter().map(|r| (r.name.clone(), r)).collect();
    }
    async fn request(&self, count: usize) -> Arc<ValidatedRequest> {
        ValidatedRequest::new_with_app(
            &self.app,
            ProxyMode::Fastest(count),
            None,
            SingleRequest::new(7u64.into(), "eth_gasPrice".into(), sonic_rs::json!([]))
                .unwrap()
                .into(),
            self.app.balanced_rpcs.head_block(),
            None,
        )
        .await
        .unwrap()
    }
    fn start(&self, request: Arc<ValidatedRequest>) -> JoinHandle<Web3ProxyResult<SingleResponse>> {
        let pool = self.app.balanced_rpcs.clone();
        tokio::spawn(async move { pool.try_proxy_connection(&request).await })
    }
    fn counts(&self) -> Vec<usize> {
        self.nodes
            .iter()
            .map(|n| n.rpc.total_requests.load(Ordering::Relaxed))
            .collect()
    }
    fn idle(&self) {
        for node in &self.nodes {
            assert_eq!(node.rpc.active_requests.load(Ordering::SeqCst), 0);
            let permits: Vec<_> = (0..4)
                .map(|_| node.rpc.request_permits.try_acquire().unwrap())
                .collect();
            assert!(node.rpc.request_permits.try_acquire().is_err());
            drop(permits);
        }
    }

    async fn wait_for_active(&self, expected: &[usize]) {
        while self
            .nodes
            .iter()
            .map(|node| node.rpc.active_requests.load(Ordering::SeqCst))
            .collect::<Vec<_>>()
            != expected
        {
            tokio::task::yield_now().await;
        }
    }

    async fn reload(&self, count: usize) {
        let config =
            TopConfig::from_toml_str(&format!("[app]\nfastest_rpcs = {count}\n[balanced_rpcs]\n"))
                .unwrap();
        // Empty backend groups leave the controlled pool in place. This still
        // exercises the same App reload entry point used by the config watcher.
        self.app.apply_top_config(&config).await.unwrap();
    }

    async fn frontend(&self) -> (u16, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let router = crate::frontend::make_router(self.app.clone());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        (port, server)
    }
}
fn succeed(call: Incoming, result: Value) {
    let id = call.body["id"].clone();
    call.respond(json!({"jsonrpc":"2.0", "id":id, "result":result}));
}
fn fail(call: Incoming, code: i64, message: &str) {
    let id = call.body["id"].clone();
    call.respond(json!({"jsonrpc":"2.0", "id":id, "error":{"code":code,"message":message}}));
}
async fn result(task: JoinHandle<Web3ProxyResult<SingleResponse>>) -> Value {
    let response = timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .parsed()
        .await
        .unwrap();
    serde_json::from_str(&sonic_rs::to_string(&response).unwrap()).unwrap()
}

#[tokio::test]
async fn fastest_counts_only_synced_nodes_and_releases_losers() {
    for (synced, count, dispatched) in [(1, 2, 1), (3, 1, 1), (3, 2, 2), (3, 10, 3), (3, 0, 3)] {
        let mut fleet = Fleet::new(synced, 3).await;
        let request = fleet.request(count).await;
        let task = fleet.start(request.clone());
        let mut calls = Vec::new();
        for node in fleet.nodes.iter_mut().take(dispatched) {
            calls.push(node.next().await);
        }
        // All selected requests have entered transport before any response is released.
        assert_eq!(
            fleet.counts(),
            (0..3)
                .map(|i| usize::from(i < dispatched))
                .collect::<Vec<_>>()
        );
        succeed(calls.pop().unwrap(), json!("0x22"));
        assert_eq!(
            result(task).await,
            json!({"jsonrpc":"2.0","id":7,"result":"0x22"})
        );
        assert_eq!(request.backend_rpcs_used().len(), dispatched);
        fleet.idle();
    }
}

#[tokio::test]
async fn fastest_uses_tiers_then_existing_weighted_latency() {
    let mut fleet = Fleet::new(3, 3).await;
    for (node, seconds) in fleet.nodes.iter().zip([30, 20, 10]) {
        node.rpc
            .peak_latency
            .as_ref()
            .unwrap()
            .report(Duration::from_secs(seconds));
        timeout(Duration::from_secs(2), async {
            while node.rpc.peak_latency.as_ref().unwrap().latency()
                < Duration::from_secs(seconds / 2)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    fleet.nodes[1].rpc.tier.store(1, Ordering::SeqCst);
    fleet.nodes[2].rpc.tier.store(2, Ordering::SeqCst);
    let task = fleet.start(fleet.request(1).await);
    succeed(fleet.nodes[1].next().await, json!("0x11"));
    assert_eq!(result(task).await["result"], json!("0x11"));
    assert_eq!(fleet.counts(), [0, 1, 0]);
    let task = fleet.start(fleet.request(2).await);
    let slower = fleet.nodes[0].next().await;
    succeed(fleet.nodes[1].next().await, json!("0x22"));
    assert_eq!(result(task).await["result"], json!("0x22"));
    assert_eq!(fleet.counts(), [1, 2, 0]);
    drop(slower);
    fleet.idle();
}

#[tokio::test]
async fn fastest_skips_unavailable_nodes_without_delaying_others() {
    for unavailable in ["busy", "unhealthy", "cooldown"] {
        let mut fleet = Fleet::new(3, 3).await;
        let held = if unavailable == "busy" {
            (0..4)
                .map(|_| fleet.nodes[0].rpc.request_permits.try_acquire().unwrap())
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        if unavailable == "unhealthy" {
            fleet.nodes[0].rpc.healthy.store(false, Ordering::SeqCst);
        }
        if unavailable == "cooldown" {
            fleet.nodes[0]
                .rpc
                .hard_limit_until
                .as_ref()
                .unwrap()
                .send_replace(Instant::now() + Duration::from_secs(10));
        }
        let task = fleet.start(fleet.request(2).await);
        let pending = fleet.nodes[1].next().await;
        succeed(fleet.nodes[2].next().await, json!("0x33"));
        assert_eq!(result(task).await["result"], json!("0x33"));
        assert_eq!(fleet.counts(), [0, 1, 1]);
        drop((pending, held));
        fleet.idle();
    }
}

#[tokio::test]
async fn fastest_errors_refill_the_race_without_repeating_an_active_node() {
    for error in ["rpc", "rate", "malformed", "wrong_id"] {
        let mut fleet = Fleet::new(3, 3).await;
        let task = fleet.start(fleet.request(2).await);
        let first = fleet.nodes[0].next().await;
        let pending = fleet.nodes[1].next().await;
        match error {
            "rate" => fail(first, 429, "rate limited"),
            "rpc" => fail(first, -32602, "invalid params"),
            "wrong_id" => first.respond(json!({"jsonrpc":"2.0","id":8,"result":"wrong"})),
            _ => first.respond(json!({"unexpected":"envelope"})),
        }
        succeed(fleet.nodes[2].next().await, json!("0x44"));
        assert_eq!(result(task).await["result"], json!("0x44"));
        assert_eq!(fleet.counts(), [1, 1, 1]);
        drop(pending);
        fleet.idle();
    }
}

#[tokio::test]
async fn fastest_accepts_null_and_reverts_and_returns_first_exhausted_error() {
    for response in [
        json!({"jsonrpc":"2.0","id":7,"result":null}),
        json!({"jsonrpc":"2.0","id":7,"error":{"code":3,"message":"execution reverted","data":"0xab"}}),
    ] {
        let mut fleet = Fleet::new(2, 2).await;
        let task = fleet.start(fleet.request(2).await);
        let pending = fleet.nodes[0].next().await;
        fleet.nodes[1].next().await.respond(response.clone());
        assert_eq!(result(task).await, response);
        drop(pending);
        fleet.idle();
    }
    let mut fleet = Fleet::new(2, 2).await;
    let task = fleet.start(fleet.request(1).await);
    fail(fleet.nodes[0].next().await, -32602, "first failure");
    fail(fleet.nodes[1].next().await, -32602, "second failure");
    let error = task.await.unwrap().unwrap_err();
    let Web3ProxyError::ExhaustedBackends(error) = error else {
        panic!("expected exhausted backends, got {error:?}");
    };
    assert!(
        matches!(*error, Web3ProxyError::JsonRpcErrorData(ref e) if e.code == -32602 && e.message == "first failure")
    );
    assert_eq!(fleet.counts(), [1, 1]);
    fleet.idle();
}

#[tokio::test]
async fn fastest_waits_for_complete_bodies_and_keeps_original_deadline() {
    let mut fleet = Fleet::new(2, 2).await;
    let task = fleet.start(fleet.request(2).await);
    let first = fleet.nodes[0].next().await;
    let second = fleet.nodes[1].next().await;
    let sender = send_stalled_body(
        first,
        format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":\"{}",
            "x".repeat(140_000)
        ),
    )
    .await;
    succeed(second, json!("complete"));
    assert_eq!(result(task).await["result"], json!("complete"));
    drop(sender);
    fleet.idle();

    let mut request = fleet.request(1).await;
    Arc::get_mut(&mut request).unwrap().expire_timeout = Duration::from_secs(2);
    let deadline = request.expire_at();
    let task = fleet.start(request.clone());
    let pending = fleet.nodes[0].next().await;
    let sender = send_stalled_body(
        pending,
        "{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":\"".into(),
    )
    .await;
    advance_to(deadline).await;
    assert!(matches!(
        task.await.unwrap(),
        Err(Web3ProxyError::Timeout(_))
    ));
    assert_eq!(request.expire_at(), deadline);
    drop(sender);
    fleet.idle();
}

#[test]
fn fastest_config_defaults_and_boundaries() {
    assert_eq!(AppConfig::default().fastest_rpcs, 2);
    for count in [0, 1, 2, 100] {
        let config: AppConfig = toml::from_str(&format!("fastest_rpcs = {count}")).unwrap();
        assert_eq!(config.fastest_rpcs, count);
        assert!(config.extra.is_empty());
    }
    for invalid in ["-1", "1.5", "\"2\""] {
        assert!(toml::from_str::<AppConfig>(&format!("fastest_rpcs = {invalid}")).is_err());
    }
}

fn http_request(port: u16, path: &str, body: Value) -> JoinHandle<Value> {
    let url = format!("http://127.0.0.1:{port}{path}");
    tokio::spawn(async move {
        let response = reqwest::Client::new()
            .post(url)
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        response.json().await.unwrap()
    })
}

async fn websocket_client(port: u16) -> TcpStream {
    let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    client.write_all(b"GET /fastest HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
    let mut headers = Vec::new();
    while !headers.ends_with(b"\r\n\r\n") {
        headers.push(client.read_u8().await.unwrap());
    }
    assert!(headers.starts_with(b"HTTP/1.1 101"));
    client
}

async fn websocket_send(client: &mut TcpStream, body: &[u8]) {
    assert!(body.len() < 126);
    // Masked client text frame, with a deterministic all-zero mask.
    client
        .write_all(&[0x81, 0x80 | body.len() as u8, 0, 0, 0, 0])
        .await
        .unwrap();
    client.write_all(body).await.unwrap();
}

async fn websocket_response(client: &mut TcpStream) -> Value {
    assert_eq!(client.read_u8().await.unwrap(), 0x81);
    let len = client.read_u8().await.unwrap();
    let len = match len {
        0..=125 => u64::from(len),
        126 => u64::from(client.read_u16().await.unwrap()),
        127 => client.read_u64().await.unwrap(),
        _ => panic!("server frame must not be masked"),
    };
    assert!(len < 8192);
    let mut body = vec![0; len as usize];
    client.read_exact(&mut body).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

/// Keep Tokio time fixed while allowing socket I/O and ready tasks to run.
/// The wall-clock bound only detects a hung test; it cannot fire the App retry timer.
async fn without_advancing_time<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::pause();
    let frozen = Instant::now();
    let wall_start = std::time::Instant::now();
    tokio::pin!(future);
    let result = loop {
        tokio::select! {
            biased;
            value = &mut future => break value,
            _ = tokio::task::yield_now() => {
                assert!(wall_start.elapsed() < Duration::from_secs(2), "response waited for an application retry");
            }
        }
    };
    assert_eq!(Instant::now(), frozen);
    tokio::time::resume();
    result
}

fn terminal_error(label: &str) -> Value {
    json!({"code":-32602,"message":format!("invalid {label}"),"data":{"field":label,"details":[1,null,"exact"]}})
}

fn fail_with(call: Incoming, error: Value) {
    let id = call.body["id"].clone();
    call.respond(json!({"jsonrpc":"2.0","id":id,"error":error}));
}

fn gas_estimate_request(id: Value, data: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"eth_estimateGas","params":[{"to":"0x0000000000000000000000000000000000000000","data":data}]})
}

#[tokio::test]
async fn accepted_fastest_gas_estimate_revert_returns_without_app_retry() {
    let mut fleet = Fleet::new(3, 3).await;
    let (port, server) = fleet.frontend().await;
    let task = http_request(
        port,
        "/fastest",
        gas_estimate_request(json!("client-id"), "0x"),
    );
    let pending = fleet.nodes[0].next().await;
    let winner = fleet.nodes[1].next().await;
    let error = json!({"code":3,"message":"execution reverted: estimate failed","data":{"reason":"exact \"data\"","return":"0xdeadbeef","details":[1,null]}});
    assert_eq!(fleet.counts(), [1, 1, 0]);
    let response = without_advancing_time(async {
        fail_with(winner, error.clone());
        task.await.unwrap()
    })
    .await;
    assert_eq!(
        response,
        json!({"jsonrpc":"2.0","id":"client-id","error":error})
    );
    assert_eq!(fleet.counts(), [1, 1, 0]);
    fleet.idle();
    drop(pending);
    server.abort();
}

#[tokio::test]
async fn accepted_fastest_gas_estimate_revert_does_not_restart_on_rankings() {
    let mut fleet = Fleet::new(3, 3).await;
    let (port, server) = fleet.frontend().await;
    let mut task = http_request(port, "/fastest", gas_estimate_request(json!(42), "0x"));
    let pending = fleet.nodes[0].next().await;
    let winner = fleet.nodes[1].next().await;
    let error = json!({"code":-32000,"message":"execution reverted"});
    assert_eq!(fleet.counts(), [1, 1, 0]);
    let response = without_advancing_time(async {
        fail_with(winner, error.clone());
        // The held loser can release its permit only when the race cancels it.
        fleet.wait_for_active(&[0, 0, 0]).await;
        fleet.idle();
        fleet.sync(3);
        loop {
            assert_eq!(
                fleet.counts(),
                [1, 1, 0],
                "ranking update restarted an accepted race"
            );
            tokio::select! {
                response = &mut task => break response.unwrap(),
                _ = tokio::task::yield_now() => {}
            }
        }
    })
    .await;
    assert_eq!(response, json!({"jsonrpc":"2.0","id":42,"error":error}));
    fleet.sync(3);
    for node in &mut fleet.nodes {
        node.quiet().await;
    }
    assert_eq!(fleet.counts(), [1, 1, 0]);
    fleet.idle();
    drop(pending);
    server.abort();
}

#[tokio::test]
async fn accepted_fastest_gas_estimate_websocket_preserves_revert_and_escaped_id() {
    let mut fleet = Fleet::new(3, 3).await;
    let (port, server) = fleet.frontend().await;
    let mut client = websocket_client(port).await;
    websocket_send(
        &mut client,
        br#"{"jsonrpc":"2.0","id":"ws\u002d\"id\\","method":"eth_estimateGas","params":[{}]}"#,
    )
    .await;
    let pending = fleet.nodes[0].next().await;
    let winner = fleet.nodes[1].next().await;
    let error =
        json!({"code":3,"message":"execution reverted: websocket estimate","data":"0xdeadbeef"});
    let response = without_advancing_time(async {
        fail_with(winner, error.clone());
        websocket_response(&mut client).await
    })
    .await;
    assert_eq!(
        response,
        json!({"jsonrpc":"2.0","id":"ws-\"id\\","error":error})
    );
    fleet.sync(3);
    for node in &mut fleet.nodes {
        node.quiet().await;
    }
    assert_eq!(fleet.counts(), [1, 1, 0]);
    fleet.idle();
    drop((pending, client));
    server.abort();
}

#[tokio::test]
async fn accepted_fastest_gas_estimate_http_array_preserves_order_and_independent_results() {
    let mut fleet = Fleet::new(3, 3).await;
    let (port, server) = fleet.frontend().await;
    let task = http_request(
        port,
        "/fastest",
        json!([
            gas_estimate_request(json!("duplicate"), "0x00"),
            gas_estimate_request(json!("duplicate"), "0x01")
        ]),
    );
    let mut pending = Vec::new();
    let mut winners = Vec::new();
    for (index, node) in fleet.nodes.iter_mut().take(2).enumerate() {
        for _ in 0..2 {
            let call = node.next().await;
            assert!(
                call.body.is_object(),
                "gas estimates must use individual backend requests"
            );
            assert_eq!(call.body["method"], "eth_estimateGas");
            assert_eq!(call.body["id"], "duplicate");
            if index == 0 {
                pending.push(call);
            } else {
                winners.push(call);
            }
        }
    }
    winners.sort_by_key(|call| call.body["params"][0]["data"].as_str().unwrap().to_owned());
    assert_eq!(fleet.counts(), [2, 2, 0]);
    let error = json!({"code":3,"message":"execution reverted: first item","data":"0xcafe"});
    let response = without_advancing_time(async {
        // Complete the second item, including its cancellation, before the first.
        let success = winners.pop().unwrap();
        assert_eq!(success.body["params"][0]["data"], "0x01");
        succeed(success, json!("0x5208"));
        fleet.wait_for_active(&[1, 1, 0]).await;
        assert!(!task.is_finished());
        let revert = winners.pop().unwrap();
        assert_eq!(revert.body["params"][0]["data"], "0x00");
        fail_with(revert, error.clone());
        task.await.unwrap()
    })
    .await;
    assert_eq!(
        response,
        json!([
            {"jsonrpc":"2.0","id":"duplicate","error":error},
            {"jsonrpc":"2.0","id":"duplicate","result":"0x5208"}
        ])
    );
    fleet.sync(3);
    for node in &mut fleet.nodes {
        node.quiet().await;
    }
    assert_eq!(fleet.counts(), [2, 2, 0]);
    fleet.idle();
    drop(pending);
    server.abort();
}

#[tokio::test]
async fn fastest_gas_estimate_keeps_configured_increases() {
    for (percent, minimum, expected) in [
        (None, None, 21_001u64),
        (Some(10), None, 23_101),
        (Some(10), Some(3_000), 24_001),
        (None, Some(3_000), 24_001),
    ] {
        for path in ["/fastest", "/"] {
            let config = AppConfig {
                gas_increase_percent: percent.map(U256::from),
                gas_increase_min: minimum.map(U256::from),
                ..AppConfig::default()
            };
            let mut fleet = Fleet::configured(3, 3, config).await;
            let (port, server) = fleet.frontend().await;
            let task = http_request(port, path, gas_estimate_request(json!(7), "0x"));
            let first = fleet.nodes[0].next().await;
            let pending = if path == "/fastest" {
                Some(fleet.nodes[1].next().await)
            } else {
                None
            };
            let response = without_advancing_time(async {
                succeed(first, json!("0x5209"));
                task.await.unwrap()
            })
            .await;
            assert_eq!(
                response,
                json!({"jsonrpc":"2.0","id":7,"result":format!("0x{expected:x}")})
            );
            assert_eq!(fleet.counts(), [1, usize::from(pending.is_some()), 0]);
            fleet.idle();
            drop(pending);
            server.abort();
        }
    }
}

#[tokio::test]
async fn best_gas_estimate_revert_keeps_app_error_handling_and_single_node_selection() {
    let mut fleet = Fleet::new(3, 3).await;
    let (port, server) = fleet.frontend().await;
    let mut task = http_request(port, "/", gas_estimate_request(json!("best-id"), "0x"));
    let call = fleet.nodes[0].next().await;
    let error = json!({"code":3,"message":"execution reverted: best estimate","data":"0xabcd"});
    without_advancing_time(async {
        fail_with(call, error.clone());
        fleet.wait_for_active(&[0, 0, 0]).await;
    })
    .await;
    fleet.idle();
    assert!(
        timeout(Duration::from_millis(50), &mut task).await.is_err(),
        "Best must retain the application retry wait"
    );
    assert_eq!(fleet.counts(), [1, 0, 0]);
    advance_to(Instant::now() + Duration::from_secs(3)).await;
    assert_eq!(
        without_advancing_time(task).await.unwrap(),
        json!({"jsonrpc":"2.0","id":"best-id","error":error})
    );
    assert_eq!(fleet.counts(), [1, 0, 0]);
    fleet.idle();
    server.abort();
}

async fn exhausted_http_method(method: &str, params: Value) {
    for rankings in [false, true] {
        let mut fleet = Fleet::new(2, 2).await;
        fleet.reload(1).await;
        let (port, server) = fleet.frontend().await;
        let task = http_request(
            port,
            "/fastest",
            json!({"jsonrpc":"2.0","id":"client-id","method":method,"params":params}),
        );
        fail_with(fleet.nodes[0].next().await, terminal_error("first"));
        let last = fleet.nodes[1].next().await;
        if rankings {
            fleet.sync(2);
        }
        fail_with(last, terminal_error("last"));
        assert_eq!(
            without_advancing_time(task).await.unwrap(),
            json!({"jsonrpc":"2.0","id":"client-id","error":terminal_error("first")})
        );
        fleet.sync(2);
        for node in &mut fleet.nodes {
            node.quiet().await;
        }
        assert_eq!(fleet.counts(), [1, 1]);
        fleet.idle();
        server.abort();
    }
}

#[tokio::test]
async fn exhausted_fastest_http_returns_without_app_retry() {
    exhausted_http_method("eth_gasPrice", json!([])).await;
}

#[tokio::test]
async fn exhausted_fastest_gas_estimate_preserves_error() {
    exhausted_http_method(
        "eth_estimateGas",
        json!([{"to":"0x0000000000000000000000000000000000000000"}]),
    )
    .await;
}

#[tokio::test]
async fn exhausted_fastest_transaction_does_not_retry_archive() {
    exhausted_http_method(
        "eth_getTransactionByHash",
        json!([format!("0x{}", "11".repeat(32))]),
    )
    .await;
}

#[tokio::test]
async fn exhausted_fastest_receipt_does_not_retry_archive() {
    exhausted_http_method(
        "eth_getTransactionReceipt",
        json!([format!("0x{}", "11".repeat(32))]),
    )
    .await;
}

#[tokio::test]
async fn exhausted_fastest_http_batch_preserves_errors_ids_and_order() {
    let mut fleet = Fleet::new(2, 2).await;
    fleet.reload(1).await;
    let (port, server) = fleet.frontend().await;
    let ids = [json!("duplicate"), json!("duplicate"), Value::Null];
    let requests: Vec<_> = ids.iter().enumerate().map(|(i, id)| json!({"jsonrpc":"2.0","id":id,"method":"eth_call","params":[{"to":"0x0000000000000000000000000000000000000000","data":format!("0x{i:02x}")},"latest"]})).collect();
    let task = http_request(port, "/fastest", json!(requests));
    for index in 0..2 {
        let mut calls = Vec::new();
        for _ in &ids {
            calls.push(fleet.nodes[index].next().await);
        }
        fleet.sync(2);
        calls.sort_by_key(|call| call.body["params"][0]["data"].as_str().unwrap().to_owned());
        for call in calls.into_iter().rev() {
            assert_eq!(call.body["params"][1], "0x2a");
            let label = format!(
                "{index}-{}",
                call.body["params"][0]["data"].as_str().unwrap()
            );
            fail_with(call, terminal_error(&label));
        }
    }
    let expected: Vec<_> = ids.iter().enumerate().map(|(i, id)| json!({"jsonrpc":"2.0","id":id,"error":terminal_error(&format!("0-0x{i:02x}"))})).collect();
    assert_eq!(without_advancing_time(task).await.unwrap(), json!(expected));
    fleet.sync(2);
    for node in &mut fleet.nodes {
        node.quiet().await;
    }
    assert_eq!(fleet.counts(), [3, 3]);
    fleet.idle();
    server.abort();
}

#[tokio::test]
async fn exhausted_fastest_websocket_preserves_wire_error_and_id() {
    let mut fleet = Fleet::new(2, 2).await;
    fleet.reload(1).await;
    let (port, server) = fleet.frontend().await;
    let mut client = websocket_client(port).await;
    let body = br#"{"jsonrpc":"2.0","id":"ws\u002did","method":"eth_gasPrice","params":[]}"#;
    websocket_send(&mut client, body).await;
    fail_with(fleet.nodes[0].next().await, terminal_error("first"));
    let last = fleet.nodes[1].next().await;
    fleet.sync(2);
    fail_with(last, terminal_error("last"));
    let response = without_advancing_time(websocket_response(&mut client)).await;
    assert_eq!(
        response,
        json!({"jsonrpc":"2.0","id":"ws-id","error":terminal_error("first")})
    );
    fleet.sync(2);
    for node in &mut fleet.nodes {
        node.quiet().await;
    }
    assert_eq!(fleet.counts(), [1, 1]);
    fleet.idle();
    drop(client);
    server.abort();
}

#[tokio::test]
async fn fastest_null_transaction_still_uses_archive_fallback() {
    for method in ["eth_getTransactionByHash", "eth_getTransactionReceipt"] {
        let mut fleet = Fleet::new(1, 1).await;
        let (port, server) = fleet.frontend().await;
        let task = http_request(
            port,
            "/fastest",
            json!({"jsonrpc":"2.0","id":42,"method":method,"params":[format!("0x{}", "11".repeat(32))]}),
        );
        succeed(fleet.nodes[0].next().await, Value::Null);
        succeed(fleet.nodes[0].next().await, json!({"found":"archive"}));
        assert_eq!(
            task.await.unwrap(),
            json!({"jsonrpc":"2.0","id":42,"result":{"found":"archive"}})
        );
        assert_eq!(fleet.counts(), [2]);
        fleet.idle();
        server.abort();
    }
}

#[tokio::test]
async fn fastest_app_waits_for_initial_sync() {
    let mut fleet = Fleet::new(0, 1).await;
    let (port, server) = fleet.frontend().await;
    let task = http_request(
        port,
        "/fastest",
        json!({"jsonrpc":"2.0","id":42,"method":"eth_gasPrice","params":[]}),
    );
    timeout(Duration::from_secs(1), async {
        while fleet.app.balanced_rpcs.watch_ranked_rpcs.receiver_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!task.is_finished());
    assert_eq!(fleet.counts(), [0]);
    fleet.sync(1);
    succeed(fleet.nodes[0].next().await, json!("0x1"));
    assert_eq!(
        task.await.unwrap(),
        json!({"jsonrpc":"2.0","id":42,"result":"0x1"})
    );
    assert_eq!(fleet.counts(), [1]);
    fleet.idle();
    server.abort();
}

#[tokio::test]
async fn fastest_http_reload_changes_new_requests_not_an_active_race_or_best() {
    let mut fleet = Fleet::new(3, 3).await;
    let (port, server) = fleet.frontend().await;
    let call = json!({"jsonrpc":"2.0","id":"client-id","method":"eth_gasPrice","params":[]});
    let task = http_request(port, "/fastest", call.clone());
    let first = fleet.nodes[0].next().await;
    let second = fleet.nodes[1].next().await;
    fleet.reload(0).await;
    // Reload cannot widen the already active two-node race.
    fleet.nodes[2].quiet().await;
    succeed(second, json!("0x1"));
    assert_eq!(
        task.await.unwrap(),
        json!({"jsonrpc":"2.0","id":"client-id","result":"0x1"})
    );
    drop(first);

    for (path, configured, dispatched) in [("/fastest/", 0, 3), ("/fastest", 1, 1), ("/", 0, 1)] {
        fleet.reload(configured).await;
        let before = fleet.counts();
        let task = http_request(port, path, call.clone());
        let mut calls = Vec::new();
        for node in fleet.nodes.iter_mut().take(dispatched) {
            calls.push(node.next().await);
        }
        succeed(calls.pop().unwrap(), json!("0x2"));
        assert_eq!(
            task.await.unwrap(),
            json!({"jsonrpc":"2.0","id":"client-id","result":"0x2"})
        );
        assert_eq!(
            fleet.counts(),
            before
                .iter()
                .enumerate()
                .map(|(i, count)| count + usize::from(i < dispatched))
                .collect::<Vec<_>>()
        );
        drop(calls);
        fleet.idle();
    }
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn fastest_existing_websocket_uses_reloaded_count() {
    let mut fleet = Fleet::new(3, 3).await;
    let (port, server) = fleet.frontend().await;
    let provider =
        super::provider::connect_ws(format!("ws://127.0.0.1:{port}/fastest").parse().unwrap())
            .await
            .unwrap();
    for (configured, dispatched) in [(2, 2), (1, 1), (0, 3)] {
        fleet.reload(configured).await;
        let before = fleet.counts();
        let provider = provider.clone();
        let task = tokio::spawn(async move {
            let params = serde_json::value::RawValue::from_string("[]".into()).unwrap();
            provider
                .raw_request_dyn("eth_gasPrice".into(), &params)
                .await
                .unwrap()
        });
        let mut calls = Vec::new();
        for node in fleet.nodes.iter_mut().take(dispatched) {
            calls.push(node.next().await);
        }
        succeed(calls.pop().unwrap(), json!("0x3"));
        assert_eq!(task.await.unwrap().get(), "\"0x3\"");
        assert_eq!(
            fleet.counts(),
            before
                .iter()
                .enumerate()
                .map(|(i, count)| count + usize::from(i < dispatched))
                .collect::<Vec<_>>()
        );
        drop(calls);
        fleet.idle();
    }
    fleet.app.frontend_shutdown.send_replace(true);
    fleet.app.frontend_tasks.close();
    timeout(Duration::from_secs(2), fleet.app.frontend_tasks.wait())
        .await
        .unwrap();
    drop(provider);
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn fastest_batch_preserves_duplicate_ids_order_and_pinned_block() {
    let mut fleet = Fleet::new(3, 3).await;
    let requests = (0..3).map(|index| {
        SingleRequest::new(
            sonic_rs::from_str::<sonic_rs::OwnedLazyValue>("\"same\\u002did\"").unwrap().into(),
            "eth_call".into(),
            sonic_rs::json!([{"to":"0x0000000000000000000000000000000000000000","data":format!("0x{index:02x}")},"latest"]),
        ).unwrap()
    }).collect();
    let app = fleet.app.clone();
    let task = tokio::spawn(async move {
        let (_, response, _) = app
            .proxy_web3_rpc(
                ProxyMode::Fastest(2),
                JsonRpcRequestEnum::Batch(requests),
                None,
            )
            .await
            .unwrap();
        serde_json::from_str::<Value>(&response.to_json_string().await.unwrap()).unwrap()
    });
    let mut calls = Vec::new();
    for node in fleet.nodes.iter_mut().take(2) {
        for _ in 0..3 {
            let call = node.next().await;
            assert_eq!(call.body["params"][1], "0x2a");
            assert_eq!(call.body["id"], "same-id");
            calls.push(call);
        }
    }
    calls.sort_by_key(|call| call.body["params"][0]["data"].as_str().unwrap().to_owned());
    // Return one result per logical item in reverse order; all copies use
    // the same client ID and must still correlate to the right batch item.
    let mut losers = Vec::new();
    while let Some(call) = calls.pop() {
        let data = call.body["params"][0]["data"].clone();
        succeed(call, data);
        losers.push(calls.pop().unwrap());
    }
    assert_eq!(
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap(),
        json!([
            {"jsonrpc":"2.0","id":"same-id","result":"0x00"},
            {"jsonrpc":"2.0","id":"same-id","result":"0x01"},
            {"jsonrpc":"2.0","id":"same-id","result":"0x02"}
        ])
    );
    assert_eq!(fleet.counts(), [3, 3, 0]);
    drop(losers);
    fleet.idle();
}

#[tokio::test]
async fn fastest_wakes_for_new_synced_nodes_and_cancels_on_consumer_drop() {
    let mut fleet = Fleet::new(1, 3).await;
    let request = fleet.request(2).await;
    let task = fleet.start(request.clone());
    let first = fleet.nodes[0].next().await;
    fleet.sync(3);
    let second = fleet.nodes[1].next().await;
    assert_eq!(fleet.counts(), [1, 1, 0]);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(request.backend_rpcs_used().len(), 2);
    drop((first, second));
    fleet.idle();

    fleet.sync(0);
    let task = fleet.start(fleet.request(2).await);
    assert!(matches!(
        task.await.unwrap(),
        Err(Web3ProxyError::NoServersSynced)
    ));
    assert_eq!(fleet.counts(), [1, 1, 0]);
}

#[tokio::test]
async fn fastest_respects_history_limits_and_rechecks_after_waiting_for_a_slot() {
    for method in ["eth_getBalance", "eth_getLogs"] {
        let mut fleet = Fleet::new(3, 3).await;
        let params = if method == "eth_getLogs" {
            sonic_rs::json!([{"fromBlock":"0x0", "toBlock":"0x2a"}])
        } else {
            sonic_rs::json!(["0x0000000000000000000000000000000000000000", "0x0"])
        };
        let request = ValidatedRequest::new_with_app(
            &fleet.app,
            ProxyMode::Fastest(2),
            None,
            SingleRequest::new(7u64.into(), method.into(), params)
                .unwrap()
                .into(),
            fleet.app.balanced_rpcs.head_block(),
            None,
        )
        .await
        .unwrap();
        let history = if method == "eth_getLogs" {
            &fleet.nodes[0].rpc.log_data_limit
        } else {
            &fleet.nodes[0].rpc.block_data_limit
        };
        history.store(1, Ordering::SeqCst);
        let task = fleet.start(request);
        let first = fleet.nodes[1].next().await;
        succeed(fleet.nodes[2].next().await, json!(null));
        assert_eq!(result(task).await["result"], Value::Null);
        assert_eq!(fleet.counts(), [0, 1, 1]);
        drop(first);
        fleet.idle();
    }

    let mut fleet = Fleet::new(2, 3).await;
    let held: Vec<_> = (0..4)
        .map(|_| fleet.nodes[1].rpc.request_permits.try_acquire().unwrap())
        .collect();
    let task = fleet.start(fleet.request(2).await);
    let first = fleet.nodes[0].next().await;
    // Lose consensus eligibility while queued. Releasing a slot must not
    // dispatch the stale candidate from the old ranking.
    fleet.sync(1);
    drop(held);
    fleet.nodes[1].quiet().await;
    succeed(first, json!("only-synced"));
    assert_eq!(result(task).await["result"], "only-synced");
    assert_eq!(fleet.counts(), [1, 0, 0]);
    fleet.idle();
}

#[tokio::test]
async fn fastest_queued_cancellation_and_timeout_do_not_count_as_dispatch() {
    for cancel in [true, false] {
        let fleet = Fleet::new(1, 1).await;
        let held: Vec<_> = (0..4)
            .map(|_| fleet.nodes[0].rpc.request_permits.try_acquire().unwrap())
            .collect();
        let mut request = fleet.request(2).await;
        Arc::get_mut(&mut request).unwrap().expire_timeout = Duration::from_secs(2);
        let task = fleet.start(request.clone());
        // Poll the scheduler once while no permit is available.
        tokio::task::yield_now().await;
        if cancel {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            advance_to(request.expire_at()).await;
            assert!(matches!(
                task.await.unwrap(),
                Err(Web3ProxyError::Timeout(_))
            ));
        }
        assert_eq!(fleet.counts(), [0]);
        assert!(request.backend_rpcs_used().is_empty());
        drop(held);
        fleet.idle();
    }
}

#[tokio::test]
async fn fastest_rate_limit_retry_keeps_original_deadline() {
    let mut fleet = Fleet::new(1, 1).await;
    let mut request = fleet.request(2).await;
    Arc::get_mut(&mut request).unwrap().expire_timeout = Duration::from_secs(2);
    let deadline = request.expire_at();
    let task = fleet.start(request.clone());
    fail(fleet.nodes[0].next().await, -32005, "rate limit exceeded");
    timeout(Duration::from_secs(2), async {
        while fleet.nodes[0].rpc.next_available(Instant::now()) <= Instant::now() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let retry_at = *fleet.nodes[0]
        .rpc
        .hard_limit_until
        .as_ref()
        .unwrap()
        .borrow();
    advance_to(retry_at).await;
    let retry = fleet.nodes[0].next().await;
    assert_eq!(fleet.counts(), [2]);
    advance_to(deadline).await;
    assert!(matches!(
        task.await.unwrap(),
        Err(Web3ProxyError::Timeout(_))
    ));
    assert_eq!(request.expire_at(), deadline);
    assert_eq!(request.backend_rpcs_used().len(), 2);
    drop(retry);
    fleet.idle();
}

#[tokio::test]
async fn fastest_admits_a_queued_node_when_its_slot_opens() {
    let mut fleet = Fleet::new(2, 2).await;
    let held: Vec<_> = (0..4)
        .map(|_| fleet.nodes[1].rpc.request_permits.try_acquire().unwrap())
        .collect();
    let task = fleet.start(fleet.request(2).await);
    let pending = fleet.nodes[0].next().await;
    assert_eq!(fleet.counts(), [1, 0]);
    drop(held);
    succeed(fleet.nodes[1].next().await, json!("queued-winner"));
    assert_eq!(result(task).await["result"], "queued-winner");
    assert_eq!(fleet.counts(), [1, 1]);
    drop(pending);
    fleet.idle();
}

#[tokio::test]
async fn fastest_local_response_needs_no_backend_request() {
    let fleet = Fleet::new(3, 3).await;
    let (_, response, backends) = fleet
        .app
        .proxy_web3_rpc(
            ProxyMode::Fastest(0),
            SingleRequest::new(7u64.into(), "eth_blockNumber".into(), sonic_rs::json!([]))
                .unwrap()
                .into(),
            None,
        )
        .await
        .unwrap();
    let response: Value = serde_json::from_str(&response.to_json_string().await.unwrap()).unwrap();
    assert_eq!(response, json!({"jsonrpc":"2.0","id":7,"result":"0x2a"}));
    assert!(backends.is_empty());
    assert_eq!(fleet.counts(), [0, 0, 0]);
    fleet.idle();
}
