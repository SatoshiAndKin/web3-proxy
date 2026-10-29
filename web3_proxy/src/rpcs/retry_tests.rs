//! Backend restarts through the real HTTP frontend and controlled backend sockets.
use super::{
    batch_tests::{advance_to, send_stalled_body, Incoming},
    fastest_tests::{fail, succeed, Fleet},
};
use serde_json::{json, Value};
use std::io::{Error, ErrorKind};
use tokio::time::{timeout, Duration, Instant};

fn http_request(port: u16, path: &str, body: Value) -> tokio::task::JoinHandle<Value> {
    let url = format!("http://127.0.0.1:{port}{path}");
    super::test_support::spawn(async move {
        reqwest::Client::new()
            .post(url)
            .json(&body)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    })
}

fn log_call() -> Value {
    json!({"jsonrpc":"2.0","id":"logs-during-upgrade","method":"eth_getLogs",
        "params":[{"fromBlock":"0x1","toBlock":"0x2a",
            "address":"0x0000000000000000000000000000000000000001"}]})
}

async fn reset(call: Incoming) {
    let sender = send_stalled_body(call, "{\"jsonrpc\":\"2.0\",".into()).await;
    sender
        .send(Err(Error::new(
            ErrorKind::ConnectionReset,
            "backend restarted",
        )))
        .await
        .unwrap();
}

async fn recovers(path: &str, rankings_lost: bool) {
    let mut fleet = Fleet::new(1, 1).await;
    let (port, server) = fleet.frontend().await;
    let mut task = http_request(port, path, log_call());
    let call = fleet.nodes[0].next().await;
    let original = call.body.clone();
    // The restart happens after both App's three-second window and the
    // initial backend connection window. The response still has time left.
    advance_to(Instant::now() + Duration::from_secs(11)).await;
    if rankings_lost {
        fleet.app.balanced_rpcs.watch_ranked_rpcs.send_replace(None);
    }
    reset(call).await;
    fleet.wait_for_active(&[0]).await;
    assert!(
        timeout(Duration::from_millis(50), &mut task).await.is_err(),
        "a restart must not complete the client request with an error"
    );
    if rankings_lost {
        advance_to(Instant::now() + Duration::from_secs(4)).await;
        fleet.sync(1);
    }
    let retry = fleet.nodes[0].next().await;
    assert_eq!(
        retry.body, original,
        "retry must preserve the complete call"
    );
    succeed(retry, json!([]));
    let response = timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        response,
        json!({"jsonrpc":"2.0","id":"logs-during-upgrade","result":[]})
    );
    fleet.idle();
    drop(server);
}

#[tokio::test]
async fn best_log_request_survives_reset_and_lost_rankings() {
    super::test_support::with_cleanup(recovers("/", true)).await;
}

#[tokio::test]
async fn fastest_log_request_survives_reset_and_lost_rankings() {
    super::test_support::with_cleanup(recovers("/fastest", true)).await;
}

#[tokio::test]
async fn versus_log_request_retries_connection_reset() {
    super::test_support::with_cleanup(recovers("/versus", false)).await;
}

#[tokio::test]
async fn versus_log_request_survives_reset_and_lost_rankings() {
    super::test_support::with_cleanup(recovers("/versus", true)).await;
}

#[tokio::test]
async fn versus_reset_does_not_repeat_another_nodes_terminal_error() {
    super::test_support::with_cleanup(async {
        let mut fleet = Fleet::new(2, 2).await;
        let (port, server) = fleet.frontend().await;
        let task = http_request(port, "/versus", log_call());
        let call = fleet.nodes[0].next().await;
        fail(fleet.nodes[1].next().await, -32602, "invalid params");
        fleet.wait_for_active(&[1, 0]).await;
        reset(call).await;
        succeed(fleet.nodes[0].next().await, json!([]));
        assert_eq!(task.await.unwrap()["result"], json!([]));
        assert_eq!(fleet.counts(), [2, 1]);
        fleet.nodes[1].quiet().await;
        fleet.idle();
        drop(server);
    })
    .await;
}

#[tokio::test]
async fn versus_ranking_recovery_preserves_completed_attempts_and_original_membership() {
    super::test_support::with_cleanup(async {
        let mut fleet = Fleet::new(2, 3).await;
        let (port, server) = fleet.frontend().await;
        let task = http_request(port, "/versus", log_call());
        let call = fleet.nodes[0].next().await;
        fail(fleet.nodes[1].next().await, -32602, "invalid params");
        fleet.wait_for_active(&[1, 0, 0]).await;
        fleet.app.balanced_rpcs.watch_ranked_rpcs.send_replace(None);
        reset(call).await;
        fleet.wait_for_active(&[0, 0, 0]).await;
        fleet.sync(3);
        succeed(fleet.nodes[0].next().await, json!([]));
        assert_eq!(task.await.unwrap()["result"], json!([]));
        for node in &mut fleet.nodes {
            node.quiet().await;
        }
        assert_eq!(fleet.counts(), [2, 1, 0]);
        fleet.idle();
        drop(server);
    })
    .await;
}

#[tokio::test]
async fn versus_withdrawn_node_does_not_restart_a_completed_comparison() {
    super::test_support::with_cleanup(async {
        let mut fleet = Fleet::new(2, 3).await;
        let slots: Vec<_> = (0..4)
            .map(|_| fleet.nodes[1].rpc.request_permits.try_acquire().unwrap())
            .collect();
        let (port, server) = fleet.frontend().await;
        let task = http_request(port, "/versus", log_call());
        let call = fleet.nodes[0].next().await;
        let rankings = &fleet.app.balanced_rpcs.watch_ranked_rpcs;
        let receivers = rankings.receiver_count();
        fleet.sync(1);
        timeout(Duration::from_secs(2), async {
            // The queued node drops its ranking receiver when its attempt ends.
            while rankings.receiver_count() >= receivers {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the withdrawn node must finish its attempt");
        // A new comparison would now replay node 0 and add node 2.
        fleet.sync(3);
        drop(slots);
        fail(call, -32602, "invalid params");
        let response = timeout(Duration::from_millis(500), task)
            .await
            .expect("completed comparisons must return without restarting")
            .unwrap();
        assert_eq!(response["error"]["code"], 502);
        assert_eq!(response["id"], "logs-during-upgrade");
        for node in &mut fleet.nodes {
            node.quiet().await;
        }
        assert_eq!(fleet.counts(), [1, 0, 0]);
        fleet.idle();
        drop(server);
    })
    .await;
}

#[tokio::test]
async fn log_reset_uses_another_ready_backend_without_waiting_for_recovery() {
    super::test_support::with_cleanup(async {
        for path in ["/", "/fastest"] {
            let mut fleet = Fleet::configured(
                2,
                2,
                crate::config::AppConfig {
                    fastest_rpcs: 1,
                    ..Default::default()
                },
            )
            .await;
            let (port, server) = fleet.frontend().await;
            let task = http_request(port, path, log_call());
            let call = fleet.nodes[0].next().await;
            let original = call.body.clone();
            reset(call).await;
            let retry = fleet.nodes[1].next().await;
            assert_eq!(retry.body, original);
            succeed(retry, json!([]));
            assert_eq!(task.await.unwrap()["result"], json!([]));
            assert_eq!(fleet.counts(), [1, 1]);
            fleet.idle();
            drop(server);
        }
    })
    .await;
}

#[tokio::test]
async fn log_restart_recovery_keeps_original_deadline() {
    super::test_support::with_cleanup(async {
        for path in ["/", "/fastest", "/versus"] {
            let mut fleet = Fleet::new(1, 1).await;
            let (port, server) = fleet.frontend().await;
            let started = Instant::now();
            let task = http_request(port, path, log_call());
            let call = fleet.nodes[0].next().await;
            let deadline_upper_bound = Instant::now() + Duration::from_secs(60);
            advance_to(started + Duration::from_secs(50)).await;
            reset(call).await;
            let retry = fleet.nodes[0].next().await;
            advance_to(deadline_upper_bound).await;
            let response = timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                response["error"]["code"], 408,
                "the original deadline must end recovery with a timeout"
            );
            assert_eq!(response["id"], "logs-during-upgrade");
            drop(retry);
            fleet.idle();
            drop(server);
        }
    })
    .await;
}

#[tokio::test]
async fn log_reset_without_rankings_recovery_times_out_at_original_deadline() {
    super::test_support::with_cleanup(async {
        for path in ["/", "/fastest", "/versus"] {
            let mut fleet = Fleet::new(1, 1).await;
            let (port, server) = fleet.frontend().await;
            let mut task = http_request(port, path, log_call());
            let call = fleet.nodes[0].next().await;
            let deadline_upper_bound = Instant::now() + Duration::from_secs(60);
            advance_to(Instant::now() + Duration::from_secs(11)).await;
            fleet.app.balanced_rpcs.watch_ranked_rpcs.send_replace(None);
            reset(call).await;
            fleet.wait_for_active(&[0]).await;
            assert!(timeout(Duration::from_millis(50), &mut task).await.is_err());
            advance_to(deadline_upper_bound).await;
            let response = timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(response["error"]["code"], 408);
            assert_eq!(response["id"], "logs-during-upgrade");
            fleet.idle();
            drop(server);
        }
    })
    .await;
}
