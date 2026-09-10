use super::*;
use crate::rpcs::batch_tests::{Harness, Incoming};
use alloy::rpc::types::Header;
use axum::body::Body;
use axum::http::Response;
use serde_json::{json, Value};

async fn probe_rpc(h: &Harness, automatic: bool, limit: u64) -> Arc<Web3Rpc> {
    let mut header: Header = Header::default();
    header.inner.number = 100_000;
    let (head_block_sender, _) = watch::channel(Some(BlockHeader::new(Arc::new(header))));
    let (hard_limit_until, _) = watch::channel(Instant::now());
    Arc::new(Web3Rpc {
        name: "log-probe".into(),
        automatic_log_limit: automatic,
        log_data_limit: limit.into(),
        head_block_sender: Some(head_block_sender),
        hard_limit_until: Some(hard_limit_until),
        http_url: h.rpc.http_url.clone(),
        http_client: Some(
            reqwest::Client::builder()
                .timeout(Duration::from_millis(300))
                .build()
                .unwrap(),
        ),
        peak_latency: Some(latency::PeakEwmaLatency::spawn(
            Duration::from_secs(15),
            100,
            Duration::from_secs(1),
        )),
        median_latency: Some(latency::RollingQuantileLatency::spawn_median(100).await),
        ..Default::default()
    })
}

fn success(incoming: Incoming, result: Value) {
    let id = incoming.body["id"].clone();
    incoming.respond(json!({"jsonrpc":"2.0","id":id,"result":result}));
}

async fn start_probe(
    rpc: &Arc<Web3Rpc>,
    h: &mut Harness,
) -> tokio::task::JoinHandle<anyhow::Result<Option<u64>>> {
    let rpc = rpc.clone();
    let task = tokio::spawn(async move { rpc.check_log_data_limit().await });
    let head = h.next().await;
    assert_eq!(head.body["method"], "eth_blockNumber");
    success(head, json!("0x186a0"));
    task
}

async fn successful_depth(h: &mut Harness, depth: u64) {
    let incoming = h.next().await;
    assert_eq!(incoming.body["method"], "eth_getLogs");
    let number = format!("0x{:x}", 100_000u64.saturating_sub(depth));
    assert_eq!(
        incoming.body["params"],
        json!([{"fromBlock":number,"toBlock":number}])
    );
    success(incoming, json!([]));
}

#[tokio::test]
async fn log_probe_transient_errors_do_not_publish_pruning_and_recovery_retries() {
    for failure in ["timeout", "http429", "rpc429", "disconnect", "unexpected"] {
        for previous_limit in [0, 128] {
            let mut h = Harness::new(1, 64).await;
            let rpc = probe_rpc(&h, true, previous_limit).await;
            let task = start_probe(&rpc, &mut h).await;
            successful_depth(&mut h, 0).await;
            successful_depth(&mut h, 32).await;
            let failed = h.next().await;
            assert_eq!(failed.body["params"][0]["fromBlock"], "0x18660");
            match failure {
                "timeout" => {
                    tokio::time::sleep(Duration::from_millis(400)).await;
                    drop(failed);
                }
                "http429" => {
                    failed
                        .reply
                        .send(Response::builder().status(429).body(Body::empty()).unwrap())
                        .unwrap();
                }
                "disconnect" => {
                    drop(failed);
                }
                _ => {
                    let id = failed.body["id"].clone();
                    let code = if failure == "rpc429" { -32005 } else { -32603 };
                    let message = if failure == "rpc429" {
                        "rate limit exceeded"
                    } else {
                        "probe unavailable"
                    };
                    failed.respond(
                        json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}}),
                    );
                }
            }
            assert!(
                task.await.unwrap().is_err(),
                "{failure} must leave detection incomplete"
            );
            assert_eq!(
                rpc.log_data_limit.load(atomic::Ordering::SeqCst),
                previous_limit,
                "{failure} must not publish the partial depth 32"
            );
            let recovered = start_probe(&rpc, &mut h).await;
            for depth in [0, 32, 64, 128, 256, 512, 1024, 90_000, u64::MAX] {
                successful_depth(&mut h, depth).await;
            }
            assert_eq!(recovered.await.unwrap().unwrap(), Some(u64::MAX));
            assert_eq!(rpc.log_data_limit.load(atomic::Ordering::SeqCst), u64::MAX);
            assert!(rpc.has_log_data(U64::ZERO));
        }
    }
}

#[tokio::test]
async fn log_probe_explicit_pruning_publishes_the_last_confirmed_depth() {
    let mut h = Harness::new(1, 64).await;
    let rpc = probe_rpc(&h, true, 0).await;
    let task = start_probe(&rpc, &mut h).await;
    successful_depth(&mut h, 0).await;
    successful_depth(&mut h, 32).await;
    let failed = h.next().await;
    let id = failed.body["id"].clone();
    failed.respond(json!({"jsonrpc":"2.0","id":id,"error":{"code":4444,"message":"pruned history unavailable"}}));
    assert_eq!(task.await.unwrap().unwrap(), Some(32));
    assert_eq!(rpc.log_data_limit.load(atomic::Ordering::SeqCst), 32);
    assert!(rpc.has_log_data(U64::from(99_968)));
    assert!(!rpc.has_log_data(U64::from(99_967)));
}

#[tokio::test]
async fn log_probe_keeps_manual_limits_without_backend_traffic() {
    let mut h = Harness::new(1, 64).await;
    let rpc = probe_rpc(&h, false, 128).await;
    assert_eq!(rpc.check_log_data_limit().await.unwrap(), None);
    assert_eq!(rpc.log_data_limit.load(atomic::Ordering::SeqCst), 128);
    h.quiet().await;
}
