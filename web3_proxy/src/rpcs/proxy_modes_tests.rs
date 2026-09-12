//! Mode contracts exercised through controlled transports and the application routes.
use super::{
    batch_tests::send_stalled_body,
    fastest_tests::{
        fail, http_request, result, succeed, websocket_client_at, websocket_response,
        websocket_send, without_advancing_time, Fleet,
    },
};
use crate::{
    config::AppConfig,
    errors::Web3ProxyError,
    frontend::rpc_proxy_ws::ProxyMode,
    jsonrpc::{SingleRequest, ValidatedRequest},
};
use serde_json::{json, Value};
use std::sync::{atomic::Ordering, Arc};
use tokio::time::{timeout, Duration, Instant};

async fn request(fleet: &Fleet, mode: ProxyMode) -> Arc<ValidatedRequest> {
    let mut request = fleet.request(1).await;
    Arc::get_mut(&mut request).unwrap().proxy_mode = mode;
    request
}

async fn drained(fleet: &Fleet) {
    fleet.app.frontend_tasks.close();
    timeout(Duration::from_secs(2), fleet.app.frontend_tasks.wait())
        .await
        .expect("comparison must finish");
    fleet.idle();
}

async fn measured(fleet: &Fleet, indices: &[usize]) {
    timeout(Duration::from_secs(2), async {
        for &index in indices {
            while fleet.nodes[index]
                .rpc
                .median_latency
                .as_ref()
                .unwrap()
                .seconds()
                == 0.0
            {
                tokio::task::yield_now().await;
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn best_and_fastest_prefer_eligible_primary_over_backup() {
    for mode in [ProxyMode::Best, ProxyMode::Fastest(1)] {
        let mut fleet = Fleet::with_backups(2, 2, AppConfig::default(), &[0]).await;
        fleet.nodes[1].rpc.tier.store(1, Ordering::SeqCst);
        let task = fleet.start(request(&fleet, mode).await);
        succeed(fleet.nodes[1].next().await, json!("primary"));
        assert_eq!(result(task).await["result"], "primary");
        assert_eq!(fleet.counts(), [0, 1]);
        fleet.nodes[1].rpc.healthy.store(false, Ordering::SeqCst);
        let task = fleet.start(request(&fleet, mode).await);
        succeed(fleet.nodes[0].next().await, json!("backup"));
        assert_eq!(result(task).await["result"], "backup");
        assert_eq!(fleet.counts(), [1, 1]);
        fleet.idle();
    }
}

#[tokio::test]
async fn all_modes_compare_decoded_ids_and_reject_invalid_envelopes_without_samples() {
    for mode in [ProxyMode::Best, ProxyMode::Fastest(1), ProxyMode::Versus] {
        for bad in [
            r#"{"jsonrpc":"2.0","id":"other","result":"wrong"}"#,
            r#"{"jsonrpc":"2.0","result":"missing ID"}"#,
            r#"{"jsonrpc":"1.0","id":"client-id","result":"old version"}"#,
            r#"{"jsonrpc":"2.0","id":"client-id","result":null,"error":{"code":3,"message":"execution reverted"}}"#,
            r#"{"jsonrpc":"2.0","id":"client-id","result":null,"result":1}"#,
        ] {
            let mut fleet = Fleet::new(2, 2).await;
            let call: SingleRequest = sonic_rs::from_str(
                r#"{"jsonrpc":"2.0","id":"client\u002did","method":"eth_gasPrice","params":[]}"#,
            )
            .unwrap();
            let request = ValidatedRequest::new_with_app(
                &fleet.app,
                mode,
                None,
                call.into(),
                fleet.app.balanced_rpcs.head_block(),
                None,
            )
            .await
            .unwrap();
            let task = fleet.start(request);
            let first = fleet.nodes[0].next().await;
            first
                .reply
                .send(axum::http::Response::new(axum::body::Body::from(bad)))
                .unwrap();
            // The backend uses an unescaped representation of the same client ID.
            fleet.nodes[1]
                .next()
                .await
                .respond(json!({"jsonrpc":"2.0","id":"client-id","result":null}));
            assert_eq!(
                result(task).await,
                json!({"jsonrpc":"2.0","id":"client-id","result":null})
            );
            drained(&fleet).await;
            measured(&fleet, &[1]).await;
            assert_eq!(
                fleet.nodes[0]
                    .rpc
                    .median_latency
                    .as_ref()
                    .unwrap()
                    .seconds(),
                0.0
            );
            assert_eq!(fleet.counts(), [1, 1]);
        }
    }
}

#[tokio::test]
async fn best_large_late_errors_and_wrong_ids_fail_over_after_complete_read() {
    for late in ["internal", "id", "malformed"] {
        let mut fleet = Fleet::new(2, 2).await;
        let task = fleet.start(request(&fleet, ProxyMode::Best).await);
        let first = fleet.nodes[0].next().await;
        let (prefix, suffix) = match late {
            "internal" => (
                format!(
                    r#"{{"jsonrpc":"2.0","id":7,"error":{{"message":"{}""#,
                    "x".repeat(140_000)
                ),
                r#", "code":-32603}}"#,
            ),
            "id" => (
                format!(r#"{{"jsonrpc":"2.0","result":"{}""#, "x".repeat(140_000)),
                r#", "id":8}"#,
            ),
            _ => (
                format!(
                    r#"{{"jsonrpc":"2.0","id":7,"result":"{}""#,
                    "x".repeat(140_000)
                ),
                "broken}",
            ),
        };
        let sender = send_stalled_body(first, prefix).await;
        fleet.nodes[1].quiet().await;
        assert!(!task.is_finished());
        assert_eq!(fleet.nodes[0].rpc.active_requests.load(Ordering::SeqCst), 1);
        assert_eq!(
            fleet.nodes[0]
                .rpc
                .median_latency
                .as_ref()
                .unwrap()
                .seconds(),
            0.0
        );
        sender
            .send(Ok(axum::body::Bytes::from(suffix)))
            .await
            .unwrap();
        drop(sender);
        succeed(fleet.nodes[1].next().await, json!("valid"));
        assert_eq!(result(task).await["result"], "valid");
        assert_eq!(
            fleet.nodes[0]
                .rpc
                .median_latency
                .as_ref()
                .unwrap()
                .seconds(),
            0.0
        );
        fleet.idle();
    }
}

#[tokio::test]
async fn best_large_stalled_body_uses_original_deadline_and_releases_permit() {
    let mut fleet = Fleet::new(1, 1).await;
    let mut request = request(&fleet, ProxyMode::Best).await;
    Arc::get_mut(&mut request).unwrap().expire_timeout = Duration::from_secs(1);
    let deadline = request.expire_at();
    let task = fleet.start(request);
    let sender = send_stalled_body(
        fleet.nodes[0].next().await,
        format!(
            r#"{{"jsonrpc":"2.0","id":7,"result":"{}"#,
            "x".repeat(140_000)
        ),
    )
    .await;
    super::batch_tests::advance_to(deadline).await;
    assert!(matches!(
        task.await.unwrap(),
        Err(Web3ProxyError::Timeout(_))
    ));
    drop(sender);
    fleet.idle();
    assert_eq!(
        fleet.nodes[0]
            .rpc
            .median_latency
            .as_ref()
            .unwrap()
            .seconds(),
        0.0
    );
}

#[tokio::test]
async fn versus_returns_winner_then_finishes_selected_nodes_and_records_successes() {
    let mut fleet = Fleet::new(3, 4).await;
    let request = request(&fleet, ProxyMode::Versus).await;
    let task = fleet.start(request.clone());
    let fast = fleet.nodes[0].next().await;
    let slow = fleet.nodes[1].next().await;
    let revert = fleet.nodes[2].next().await;
    succeed(fast, json!("winner"));
    let answer = result(task).await;
    assert_eq!(answer, json!({"jsonrpc":"2.0","id":7,"result":"winner"}));
    request.set_response(answer.to_string().len() as u64);
    let response_millis = request.response.lock().response_millis;
    assert_eq!(fleet.app.frontend_tasks.len(), 1);
    assert_eq!(fleet.counts(), [1, 1, 1, 0]);
    fleet.wait_for_active(&[0, 1, 1, 0]).await;
    assert_eq!(
        fleet.nodes[1]
            .rpc
            .median_latency
            .as_ref()
            .unwrap()
            .seconds(),
        0.0
    );
    tokio::time::sleep(Duration::from_millis(25)).await;
    succeed(slow, json!("later"));
    fail(revert, 3, "execution reverted");
    drained(&fleet).await;
    measured(&fleet, &[0, 1]).await;
    assert_eq!(
        fleet.nodes[2]
            .rpc
            .median_latency
            .as_ref()
            .unwrap()
            .seconds(),
        0.0
    );
    assert_eq!(request.response.lock().response_millis, response_millis);
    assert_eq!(
        request.response.lock().response_bytes,
        answer.to_string().len() as u64
    );
    assert_eq!(fleet.counts(), [1, 1, 1, 0]);
}

#[tokio::test]
async fn versus_retains_queued_work_after_winner_and_client_drop() {
    for disconnect in [false, true] {
        let mut fleet = Fleet::new(2, 2).await;
        let slots: Vec<_> = (0..4)
            .map(|_| fleet.nodes[1].rpc.request_permits.try_acquire().unwrap())
            .collect();
        let task = fleet.start(request(&fleet, ProxyMode::Versus).await);
        let fast = fleet.nodes[0].next().await;
        if disconnect {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            succeed(fast, json!("disconnected"));
        } else {
            succeed(fast, Value::Null);
            assert_eq!(result(task).await["result"], Value::Null);
        }
        assert_eq!(fleet.app.frontend_tasks.len(), 1);
        assert_eq!(fleet.counts(), [1, 0]);
        drop(slots);
        let queued = fleet.nodes[1].next().await;
        assert_eq!(fleet.nodes[1].rpc.active_requests.load(Ordering::SeqCst), 1);
        succeed(queued, json!("queued"));
        drained(&fleet).await;
        measured(&fleet, &[0, 1]).await;
        assert_eq!(fleet.counts(), [1, 1]);
    }
}

#[tokio::test]
async fn versus_rechecks_queued_membership_without_adding_new_nodes() {
    let mut fleet = Fleet::new(2, 3).await;
    let slots: Vec<_> = (0..4)
        .map(|_| fleet.nodes[1].rpc.request_permits.try_acquire().unwrap())
        .collect();
    let task = fleet.start(request(&fleet, ProxyMode::Versus).await);
    succeed(fleet.nodes[0].next().await, json!("winner"));
    assert_eq!(result(task).await["result"], "winner");
    fleet.sync(1);
    fleet.app.frontend_tasks.close();
    timeout(Duration::from_secs(2), fleet.app.frontend_tasks.wait())
        .await
        .unwrap();
    fleet.sync(3);
    drop(slots);
    for node in &mut fleet.nodes {
        node.quiet().await;
    }
    assert_eq!(fleet.counts(), [1, 0, 0]);
}

#[tokio::test]
async fn versus_waits_for_cooldown_before_submission_but_never_retries_an_attempt() {
    let mut fleet = Fleet::new(2, 2).await;
    let cooldown = Instant::now() + Duration::from_secs(12);
    fleet.nodes[0]
        .rpc
        .hard_limit_until
        .as_ref()
        .unwrap()
        .send_replace(cooldown);
    let task = fleet.start(request(&fleet, ProxyMode::Versus).await);
    let slower = fleet.nodes[1].next().await;
    super::batch_tests::advance_to(cooldown).await;
    fail(fleet.nodes[0].next().await, -32005, "rate limit exceeded");
    fleet.wait_for_active(&[0, 1]).await;
    super::batch_tests::advance_to(cooldown + Duration::from_secs(2)).await;
    fleet.nodes[0].quiet().await;
    succeed(slower, json!("only success"));
    assert_eq!(result(task).await["result"], "only success");
    drained(&fleet).await;
    assert_eq!(fleet.counts(), [1, 1]);
}

#[tokio::test]
async fn versus_deadline_and_shutdown_end_running_and_queued_work() {
    for shutdown in [false, true] {
        let mut fleet = Fleet::new(3, 3).await;
        let slots: Vec<_> = (0..4)
            .map(|_| fleet.nodes[2].rpc.request_permits.try_acquire().unwrap())
            .collect();
        let request = request(&fleet, ProxyMode::Versus).await;
        let deadline = request.expire_at();
        let task = fleet.start(request);
        succeed(fleet.nodes[0].next().await, json!("winner"));
        let stalled = fleet.nodes[1].next().await;
        assert_eq!(result(task).await["result"], "winner");
        let cutoff = if shutdown {
            fleet.app.frontend_shutdown.send_replace(true);
            fleet.app.frontend_tasks.close();
            // Give the tracked task a chance to observe shutdown before advancing time.
            without_advancing_time(async {
                for _ in 0..10 {
                    tokio::task::yield_now().await;
                }
            })
            .await;
            Instant::now() + Duration::from_secs(20)
        } else {
            deadline
        };
        super::batch_tests::advance_to(cutoff - Duration::from_secs(1)).await;
        assert_eq!(fleet.app.frontend_tasks.len(), 1);
        assert_eq!(fleet.nodes[1].rpc.active_requests.load(Ordering::SeqCst), 1);
        super::batch_tests::advance_to(cutoff + Duration::from_millis(1)).await;
        fleet.app.frontend_tasks.close();
        timeout(Duration::from_secs(2), fleet.app.frontend_tasks.wait())
            .await
            .unwrap();
        drop(slots);
        drop(stalled);
        fleet.idle();
        assert_eq!(fleet.counts(), [1, 1, 0]);
        fleet.nodes[2].quiet().await;
    }
}

#[tokio::test]
async fn versus_all_failed_preserves_first_completion_and_timeout_overrides_failures() {
    for times_out in [false, true] {
        let mut fleet = Fleet::new(2, 2).await;
        let request = request(&fleet, ProxyMode::Versus).await;
        let deadline = request.expire_at();
        let task = fleet.start(request);
        let last = fleet.nodes[0].next().await;
        fail(
            fleet.nodes[1].next().await,
            -32603,
            "first completed failure",
        );
        fleet.wait_for_active(&[1, 0]).await;
        if times_out {
            super::batch_tests::advance_to(deadline).await;
        } else {
            fail(last, -32602, "last completed failure");
        }
        let error = task.await.unwrap().unwrap_err();
        let Web3ProxyError::ExhaustedBackends(error) = error else {
            panic!("{error:?}");
        };
        if times_out {
            assert!(matches!(*error, Web3ProxyError::Timeout(_)));
        } else {
            assert!(
                matches!(*error, Web3ProxyError::JsonRpcErrorData(ref e) if e.code == -32603 && e.message == "first completed failure")
            );
        }
        drained(&fleet).await;
        assert_eq!(fleet.counts(), [1, 1]);
    }
}

#[tokio::test]
async fn versus_app_keeps_null_transactions_and_reverts_without_restarting_comparison() {
    for (method, params, reply) in [
        (
            "eth_getTransactionByHash",
            json!([format!("0x{}", "11".repeat(32))]),
            json!({"result":null}),
        ),
        (
            "eth_getTransactionReceipt",
            json!([format!("0x{}", "11".repeat(32))]),
            json!({"result":null}),
        ),
        (
            "eth_estimateGas",
            json!([{"to":"0x0000000000000000000000000000000000000000"}]),
            json!({"error":{"code":3,"message":"execution reverted","data":"0xab"}}),
        ),
    ] {
        let mut fleet = Fleet::new(2, 2).await;
        let (port, server) = fleet.frontend().await;
        let task = http_request(
            port,
            "/versus",
            json!({"jsonrpc":"2.0","id":"client","method":method,"params":params}),
        );
        let first = fleet.nodes[0].next().await;
        let slow = fleet.nodes[1].next().await;
        let mut expected = reply.clone();
        expected["jsonrpc"] = json!("2.0");
        expected["id"] = json!("client");
        first.respond(expected.clone());
        assert_eq!(without_advancing_time(task).await.unwrap(), expected);
        fleet.sync(2);
        succeed(slow, json!("0x5208"));
        drained(&fleet).await;
        assert_eq!(fleet.counts(), [1, 1]);
        server.abort();
    }
}

async fn exhausted_versus_app_method(method: &str, params: Value) {
    for rankings in [false, true] {
        let mut fleet = Fleet::new(2, 3).await;
        let (port, server) = fleet.frontend().await;
        let slots: Vec<_> = (0..4)
            .map(|_| fleet.nodes[1].rpc.request_permits.try_acquire().unwrap())
            .collect();
        let task = http_request(
            port,
            "/versus",
            json!({"jsonrpc":"2.0","id":"client-id","method":method,"params":params}),
        );
        let first_error = json!({"code":-32602,"message":"first completed failure","data":{"field":method,"details":[1,null,"exact"]}});
        let expected = json!({"jsonrpc":"2.0","id":"client-id","error":first_error});
        fleet.nodes[0].next().await.respond(expected.clone());
        fleet.wait_for_active(&[0, 0, 0]).await;
        assert!(!task.is_finished(), "selected queued work must continue");
        assert_eq!(fleet.app.frontend_tasks.len(), 1);
        assert_eq!(fleet.counts(), [1, 0, 0]);
        if rankings {
            fleet.sync(3);
        }
        drop(slots);
        let last = fleet.nodes[1].next().await;
        let response = without_advancing_time(async {
            if rankings {
                fleet.sync(3);
            }
            fail(last, -32603, "last completed failure");
            task.await.unwrap()
        })
        .await;
        assert_eq!(response, expected);
        drained(&fleet).await;
        fleet.sync(3);
        for node in &mut fleet.nodes {
            node.quiet().await;
        }
        assert_eq!(fleet.counts(), [1, 1, 0]);
        assert_eq!(fleet.app.frontend_tasks.len(), 0);
        fleet.idle();
        server.abort();
    }
}

#[tokio::test]
async fn exhausted_versus_app_returns_without_retry_on_rankings() {
    exhausted_versus_app_method("eth_gasPrice", json!([])).await;
}

#[tokio::test]
async fn exhausted_versus_app_gas_estimate_preserves_error_without_retry() {
    exhausted_versus_app_method(
        "eth_estimateGas",
        json!([{"to":"0x0000000000000000000000000000000000000000"}]),
    )
    .await;
}

#[tokio::test]
async fn exhausted_versus_app_transaction_does_not_retry_archive() {
    exhausted_versus_app_method(
        "eth_getTransactionByHash",
        json!([format!("0x{}", "11".repeat(32))]),
    )
    .await;
}

#[tokio::test]
async fn exhausted_versus_app_receipt_does_not_retry_archive() {
    exhausted_versus_app_method(
        "eth_getTransactionReceipt",
        json!([format!("0x{}", "11".repeat(32))]),
    )
    .await;
}

#[tokio::test]
async fn best_and_versus_batches_keep_duplicate_ids_order_and_pinned_blocks() {
    for (mode, path) in [(ProxyMode::Best, "/"), (ProxyMode::Versus, "/versus")] {
        let mut fleet = Fleet::new(2, 2).await;
        let (port, server) = fleet.frontend().await;
        // Estimate calls use individual requests in Best as well as Versus.
        let ids = [json!(7), json!(7), json!("escaped-id")];
        let batch: Vec<_> = ids.iter().enumerate().map(|(index, id)| json!({"jsonrpc":"2.0","id":id,"method":"eth_estimateGas","params":[{"to":"0x0000000000000000000000000000000000000000","data":format!("0x{index:02x}")},"latest"]})).collect();
        let task = http_request(port, path, json!(batch));
        let mut slow = Vec::new();
        for _ in &ids {
            slow.push(fleet.nodes[0].next().await);
        }
        if matches!(mode, ProxyMode::Best) {
            for call in slow.drain(..) {
                fail(call, -32603, "Internal error");
            }
        }
        let mut answers = Vec::new();
        for _ in &ids {
            answers.push(fleet.nodes[1].next().await);
        }
        for call in answers.into_iter().rev() {
            assert_eq!(call.body["params"][1], "0x2a");
            let index = usize::from_str_radix(
                call.body["params"][0]["data"]
                    .as_str()
                    .unwrap()
                    .trim_start_matches("0x"),
                16,
            )
            .unwrap();
            succeed(call, json!(format!("0x{:x}", 21000 + index)));
        }
        let expected: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| json!({"jsonrpc":"2.0","id":id,"result":format!("0x{:x}", 21000+i)}))
            .collect();
        assert_eq!(without_advancing_time(task).await.unwrap(), json!(expected));
        for call in slow {
            succeed(call, json!("0xffff"));
        }
        drained(&fleet).await;
        assert_eq!(fleet.counts(), [3, 3]);
        server.abort();
    }
}

#[tokio::test]
async fn versus_websocket_disconnect_keeps_comparison_and_local_responses_use_no_backend() {
    let mut fleet = Fleet::new(2, 2).await;
    let (port, server) = fleet.frontend().await;
    let mut client = websocket_client_at(port, "/versus").await;
    websocket_send(
        &mut client,
        br#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}"#,
    )
    .await;
    assert_eq!(
        websocket_response(&mut client).await,
        json!({"jsonrpc":"2.0","id":1,"result":"0x1"})
    );
    assert_eq!(fleet.counts(), [0, 0]);
    websocket_send(
        &mut client,
        br#"{"jsonrpc":"2.0","id":"ws\u002did","method":"eth_gasPrice","params":[]}"#,
    )
    .await;
    let fast = fleet.nodes[0].next().await;
    let slow = fleet.nodes[1].next().await;
    fail(fast, 3, "execution reverted");
    assert_eq!(
        websocket_response(&mut client).await,
        json!({"jsonrpc":"2.0","id":"ws-id","error":{"code":3,"message":"execution reverted"}})
    );
    drop(client);
    succeed(slow, json!("0x12"));
    drained(&fleet).await;
    measured(&fleet, &[1]).await;
    assert_eq!(fleet.counts(), [1, 1]);
    server.abort();
}

#[tokio::test]
async fn versus_websocket_backend_accepts_jsonrpc_2_response_and_preserves_client_id() {
    let fleet = Fleet::new(1, 1).await;
    let mut ws = super::batch_tests::WebSocketHarness::new(1).await;
    fleet
        .app
        .balanced_rpcs
        .watch_ranked_rpcs
        .send_replace(Some(Arc::new(super::consensus::RankedRpcs::from_rpcs(
            vec![ws.rpc.clone()],
            fleet.app.balanced_rpcs.head_block(),
            false,
        ))));
    let call: SingleRequest = sonic_rs::from_str(
        r#"{"jsonrpc":"2.0","id":"client\u002did","method":"eth_call","params":[{},"latest"]}"#,
    )
    .unwrap();
    let request = ValidatedRequest::new_with_app(
        &fleet.app,
        ProxyMode::Versus,
        None,
        call.into(),
        fleet.app.balanced_rpcs.head_block(),
        None,
    )
    .await
    .unwrap();
    let task = fleet.start(request);
    let incoming = ws.next().await;
    let id = incoming.body["id"].clone();
    incoming.respond(json!({"jsonrpc":"2.0", "id":id, "result":"0x42"}));
    assert_eq!(
        result(task).await,
        json!({"jsonrpc":"2.0","id":"client-id","result":"0x42"})
    );
    drained(&fleet).await;
    ws.quiet().await;
    assert_eq!(ws.rpc.total_requests.load(Ordering::Relaxed), 1);
    assert_eq!(ws.rpc.active_requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn versus_waits_past_connection_window_for_first_cooled_node() {
    let mut fleet = Fleet::new(1, 1).await;
    let request = request(&fleet, ProxyMode::Versus).await;
    let cooldown = request.connect_timeout_at() + Duration::from_secs(1);
    fleet.nodes[0]
        .rpc
        .hard_limit_until
        .as_ref()
        .unwrap()
        .send_replace(cooldown);
    let task = fleet.start(request);
    // Start the comparison while every selected node is cooling down.
    without_advancing_time(async {
        while fleet.app.frontend_tasks.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    super::batch_tests::advance_to(cooldown).await;
    succeed(fleet.nodes[0].next().await, json!("cooled"));
    assert_eq!(result(task).await["result"], "cooled");
    drained(&fleet).await;
    assert_eq!(fleet.counts(), [1]);
}

#[tokio::test]
async fn versus_rechecks_queued_health_and_history_before_submission() {
    for change in ["health", "history"] {
        let mut fleet = Fleet::new(2, 2).await;
        let slots: Vec<_> = (0..4)
            .map(|_| fleet.nodes[1].rpc.request_permits.try_acquire().unwrap())
            .collect();
        let call: SingleRequest = sonic_rs::from_str(
            r#"{"jsonrpc":"2.0","id":7,"method":"eth_call","params":[{},"0x1"]}"#,
        )
        .unwrap();
        let request = ValidatedRequest::new_with_app(
            &fleet.app,
            ProxyMode::Versus,
            None,
            call.into(),
            fleet.app.balanced_rpcs.head_block(),
            None,
        )
        .await
        .unwrap();
        let task = fleet.start(request);
        succeed(fleet.nodes[0].next().await, json!("winner"));
        assert_eq!(result(task).await["result"], "winner");
        if change == "health" {
            fleet.nodes[1].rpc.healthy.store(false, Ordering::SeqCst);
        } else {
            fleet.nodes[1]
                .rpc
                .block_data_limit
                .store(1, Ordering::SeqCst);
        }
        drop(slots);
        drained(&fleet).await;
        assert_eq!(fleet.counts(), [1, 0]);
        fleet.nodes[1].quiet().await;
    }
}

#[tokio::test]
async fn versus_cache_hit_adds_no_backend_samples_or_comparison_tasks() {
    use super::blockchain::{BlockResponseCacheKey, CachedBlockResponse};
    use alloy::primitives::B256;
    let mut fleet = Fleet::new(2, 2).await;
    let hash = B256::with_last_byte(42);
    let block = json!({"hash":hash, "number":"0x2a", "transactions":[], "uncles":[]});
    let raw = Arc::new(sonic_rs::from_str::<sonic_rs::OwnedLazyValue>(&block.to_string()).unwrap());
    let (full, _) = CachedBlockResponse::from_full(raw, hash).unwrap();
    fleet
        .app
        .balanced_rpcs
        .block_responses
        .insert(BlockResponseCacheKey::new(hash, true), full)
        .await;
    let (port, server) = fleet.frontend().await;
    let reply = http_request(
        port,
        "/versus",
        json!({"jsonrpc":"2.0","id":"cached","method":"eth_getBlockByHash","params":[hash,true]}),
    )
    .await
    .unwrap();
    assert_eq!(reply, json!({"jsonrpc":"2.0","id":"cached","result":block}));
    assert_eq!(fleet.counts(), [0, 0]);
    assert!(fleet.app.frontend_tasks.is_empty());
    for node in &mut fleet.nodes {
        node.quiet().await;
        assert_eq!(node.rpc.median_latency.as_ref().unwrap().seconds(), 0.0);
    }
    server.abort();
}

#[tokio::test]
async fn versus_websocket_subscription_stays_local_and_preserves_acknowledgement_order() {
    let mut fleet = Fleet::new(2, 2).await;
    let (port, server) = fleet.frontend().await;
    let mut client = websocket_client_at(port, "/versus").await;
    websocket_send(
        &mut client,
        br#"{"jsonrpc":"2.0","id":"sub","method":"eth_subscribe","params":["newHeads"]}"#,
    )
    .await;
    let ack = websocket_response(&mut client).await;
    assert_eq!(ack, json!({"jsonrpc":"2.0","id":"sub","result":"0x1"}));
    let notification = websocket_response(&mut client).await;
    assert_eq!(notification["method"], "eth_subscription");
    assert_eq!(notification["params"]["subscription"], "0x1");
    assert_eq!(notification["params"]["result"]["number"], "0x2a");
    websocket_send(
        &mut client,
        br#"{"jsonrpc":"2.0","id":"unsub","method":"eth_unsubscribe","params":["0x1"]}"#,
    )
    .await;
    assert_eq!(
        websocket_response(&mut client).await,
        json!({"jsonrpc":"2.0","id":"unsub","result":true})
    );
    assert_eq!(fleet.counts(), [0, 0]);
    for node in &mut fleet.nodes {
        node.quiet().await;
    }
    drop(client);
    fleet.app.frontend_shutdown.send_replace(true);
    drained(&fleet).await;
    server.abort();
}

#[tokio::test]
async fn versus_transaction_submission_keeps_single_private_backend_selection() {
    use alloy::network::TxSignerSync;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::{
        consensus::{SignableTransaction, TxEip1559},
        eips::eip2718::Encodable2718,
        primitives::{Bytes, TxKind},
    };
    let signer = PrivateKeySigner::random();
    let mut transaction = TxEip1559 {
        chain_id: 1,
        gas_limit: 21_000,
        to: TxKind::Call(signer.address()),
        ..Default::default()
    };
    let signature = signer.sign_transaction_sync(&mut transaction).unwrap();
    let signed = transaction.into_signed(signature);
    let expected_hash = *signed.hash();
    let encoded = Bytes::from(signed.encoded_2718());
    let mut fleet = Fleet::new(2, 2).await;
    let (port, server) = fleet.frontend().await;
    let task = http_request(
        port,
        "/versus",
        json!({"jsonrpc":"2.0","id":7,"method":"eth_sendRawTransaction","params":[encoded]}),
    );
    let call = fleet.nodes[0].next().await;
    assert_eq!(call.body["method"], "eth_sendRawTransaction");
    assert_eq!(call.body["params"], json!([encoded]));
    succeed(call, json!(expected_hash));
    assert_eq!(
        task.await.unwrap(),
        json!({"jsonrpc":"2.0","id":7,"result":expected_hash})
    );
    assert_eq!(fleet.counts(), [1, 0]);
    assert!(fleet.app.frontend_tasks.is_empty());
    fleet.nodes[1].quiet().await;
    fleet.idle();
    server.abort();
}

#[tokio::test]
async fn versus_websocket_reconnect_resends_within_one_pipeline_invocation() {
    let mut fleet = Fleet::new(1, 2).await;
    let mut ws = super::batch_tests::WebSocketHarness::new(1).await;
    let publish = |nodes| {
        fleet
            .app
            .balanced_rpcs
            .watch_ranked_rpcs
            .send_replace(Some(Arc::new(super::consensus::RankedRpcs::from_rpcs(
                nodes,
                fleet.app.balanced_rpcs.head_block(),
                false,
            ))));
    };
    publish(vec![ws.rpc.clone(), fleet.nodes[0].rpc.clone()]);
    let slots: Vec<_> = (0..4)
        .map(|_| fleet.nodes[0].rpc.request_permits.try_acquire().unwrap())
        .collect();
    let call: SingleRequest = sonic_rs::from_str(
        r#"{"jsonrpc":"2.0","id":"client\u002did","method":"eth_call","params":[{"to":"0x0000000000000000000000000000000000000001","data":"0x1234"},"latest"]}"#,
    )
    .unwrap();
    let request = ValidatedRequest::new_with_app(
        &fleet.app,
        ProxyMode::Versus,
        None,
        call.into(),
        fleet.app.balanced_rpcs.head_block(),
        None,
    )
    .await
    .unwrap();
    let task = fleet.start(request.clone());
    let incoming = ws.next().await;
    let original = incoming.body.clone();
    assert_eq!(
        original["params"],
        json!([{"to":"0x0000000000000000000000000000000000000001","data":"0x1234"},"0x2a"])
    );
    publish(vec![
        ws.rpc.clone(),
        fleet.nodes[0].rpc.clone(),
        fleet.nodes[1].rpc.clone(),
    ]);
    // A malformed frame makes Alloy reconnect and resend the pending call.
    incoming
        .reply
        .send(axum::http::Response::new(axum::body::Body::from(
            "malformed JSON",
        )))
        .unwrap();
    let replay = ws.next().await;
    assert_eq!(replay.body, original);
    assert_eq!(ws.rpc.total_requests.load(Ordering::Relaxed), 1);
    assert_eq!(ws.rpc.active_requests.load(Ordering::SeqCst), 1);
    succeed(replay, json!("0x42"));
    assert_eq!(
        result(task).await,
        json!({"jsonrpc":"2.0","id":"client-id","result":"0x42"})
    );
    // The original queued node still runs after the client receives its answer.
    assert_eq!(fleet.counts(), [0, 0]);
    assert_eq!(fleet.app.frontend_tasks.len(), 1);
    drop(slots);
    let queued = fleet.nodes[0].next().await;
    assert_eq!(queued.body["params"], original["params"]);
    assert_eq!(queued.body["id"], "client-id");
    succeed(queued, json!("later"));
    drained(&fleet).await;
    ws.quiet().await;
    for node in &mut fleet.nodes {
        node.quiet().await;
    }
    assert_eq!(fleet.counts(), [1, 0]);
    assert_eq!(
        request
            .backend_rpcs_used()
            .iter()
            .map(|rpc| rpc.name.as_str())
            .collect::<Vec<_>>(),
        ["ws-only", "node-0"]
    );
    assert_eq!(ws.rpc.total_requests.load(Ordering::Relaxed), 1);
    assert_eq!(ws.rpc.active_requests.load(Ordering::SeqCst), 0);
    let permit = ws.rpc.request_permits.try_acquire().unwrap();
    assert!(ws.rpc.request_permits.try_acquire().is_err());
    drop(permit);
}

#[tokio::test]
async fn versus_null_gas_estimate_is_an_accepted_answer() {
    let mut fleet = Fleet::new(2, 2).await;
    let (port, server) = fleet.frontend().await;
    let task = http_request(
        port,
        "/versus",
        json!({"jsonrpc":"2.0","id":7,"method":"eth_estimateGas","params":[{}]}),
    );
    let slow = fleet.nodes[0].next().await;
    succeed(fleet.nodes[1].next().await, Value::Null);
    assert_eq!(
        without_advancing_time(task).await.unwrap(),
        json!({"jsonrpc":"2.0","id":7,"result":null})
    );
    succeed(slow, json!("0x5208"));
    drained(&fleet).await;
    assert_eq!(fleet.counts(), [1, 1]);
    server.abort();
}
