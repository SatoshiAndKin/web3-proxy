//! Manual release benchmark using the production HTTP router and controlled backends.
//! Run the ignored `serve` test and use the `rpc_latency_load` example in another process.
use super::{Harness, Incoming};
use crate::rpcs::{consensus::RankedRpcs, one::RequestPermits};
use axum::{extract::State, routing::post, Router};
use latency::{PeakEwmaLatency, RollingQuantileLatency};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::oneshot};

fn reply(call: &Value, answer: &Value) -> Value {
    let result = if call["method"] == "eth_getBlockByHash" {
        let mut block: alloy::rpc::types::Block = alloy::rpc::types::Block::default();
        block.header.hash = call["params"][0].as_str().unwrap().parse().unwrap();
        block.header.inner.number = 41;
        serde_json::to_value(block).unwrap()
    } else {
        answer.clone()
    };
    json!({"jsonrpc":"2.0", "id":call["id"], "result":result})
}

#[test]
#[ignore = "manual release performance benchmark; starts loopback listeners"]
fn serve() {
    tracing_subscriber::fmt()
        .with_env_filter("info,alloy_transport=error")
        .with_writer(std::io::sink)
        .try_init()
        .unwrap();
    let workers = std::env::var("W3P_BENCH_WORKERS")
        .unwrap_or("4".into())
        .parse()
        .unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(workers)
        .build()
        .unwrap()
        .block_on(super::super::test_support::with_cleanup(async {
            let mut nodes = Vec::new();
            for (name, capacity) in [("lambo-reth", 128), ("lambo-geth", 64), ("nuc-reth", 32)] {
                let median = RollingQuantileLatency::spawn_median(1_000).await;
                nodes.push(
                    Harness::configured(name, capacity, 64, move |rpc| {
                        rpc.request_permits = RequestPermits::new(capacity, 64);
                        rpc.block_data_limit
                            .store(u64::MAX, std::sync::atomic::Ordering::SeqCst);
                        rpc.log_data_limit
                            .store(u64::MAX, std::sync::atomic::Ordering::SeqCst);
                        let mut header: alloy::rpc::types::Header = Default::default();
                        header.inner.number = 42;
                        rpc.head_block_sender = Some(
                            tokio::sync::watch::channel(Some(
                                crate::rpcs::blockchain::BlockHeader::new(Arc::new(header)),
                            ))
                            .0,
                        );
                        rpc.peak_latency = Some(PeakEwmaLatency::spawn(
                            Duration::from_secs(15),
                            50_000,
                            Duration::from_secs(1),
                        ));
                        rpc.median_latency = Some(median);
                    })
                    .await,
                );
            }
            let app = nodes[0].app.clone();
            let rpcs: Vec<_> = nodes.iter().map(|node| node.rpc.clone()).collect();
            *app.balanced_rpcs.by_name.write() = rpcs
                .iter()
                .map(|rpc| (rpc.name.clone(), rpc.clone()))
                .collect();
            let head = app.balanced_rpcs.head_block().unwrap();
            let ranked = RankedRpcs::from_votes(
                1,
                1,
                alloy::primitives::U64::ZERO,
                hashbrown::HashMap::from([(head.clone(), (rpcs.iter().collect(), 3))]),
                rpcs.iter().map(|rpc| (rpc.clone(), head.clone())).collect(),
            )
            .unwrap();
            app.balanced_rpcs
                .watch_ranked_rpcs
                .send_replace(Some(Arc::new(ranked)));
            let bytes: usize = std::env::var("W3P_BENCH_BYTES")
                .unwrap_or("66".into())
                .parse()
                .unwrap();
            let answer = Arc::new(json!(format!("0x{}", "a".repeat(bytes - 2))));
            for mut node in nodes {
                let answer = answer.clone();
                super::super::test_support::spawn(async move {
                    while let Some(Incoming {
                        body,
                        reply: sender,
                    }) = node.incoming.recv().await
                    {
                        // Readiness is independent from response delays, as with a real server.
                        let answer = answer.clone();
                        tokio::spawn(async move {
                            if body["method"] == "eth_getBlockByHash" {
                                let delay = std::env::var("W3P_BENCH_HEADER_DELAY_MS")
                                    .unwrap_or("0".into())
                                    .parse()
                                    .unwrap();
                                tokio::time::sleep(Duration::from_millis(delay)).await;
                            }
                            let body = if let Some(calls) = body.as_array() {
                                Value::Array(
                                    calls
                                        .iter()
                                        .rev()
                                        .map(|call| reply(call, &answer))
                                        .collect(),
                                )
                            } else {
                                reply(&body, &answer)
                            };
                            let _ = sender.send(
                                axum::http::Response::builder()
                                    .header("content-type", "application/json")
                                    .body(axum::body::Body::from(body.to_string()))
                                    .unwrap(),
                            );
                        });
                    }
                });
            }
            async fn reset(State(app): State<Arc<crate::app::App>>) {
                app.balanced_rpcs.blocks_by_hash.invalidate_all();
                app.balanced_rpcs.blocks_by_number.invalidate_all();
                app.balanced_rpcs.block_responses.invalidate_all();
                app.balanced_rpcs.blocks_by_hash.run_pending_tasks().await;
            }
            let controls = Router::new()
                .route("/bench/reset", post(reset))
                .with_state(app.clone());
            let router = crate::frontend::make_router(app).merge(controls);
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let ready = std::env::var("W3P_BENCH_READY")
                .expect("set W3P_BENCH_READY to an external task record path");
            std::fs::write(
                ready,
                json!({"port":port,"workers":workers,"pid":std::process::id()}).to_string(),
            )
            .unwrap();
            let (done, wait) = oneshot::channel();
            let signal = tokio::spawn(async move {
                tokio::signal::ctrl_c().await.unwrap();
                let _ = done.send(());
            });
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = wait.await;
                })
                .await
                .unwrap();
            signal.abort();
        }));
}
