//! Primary-first behavior through controlled backend transports.
use super::{
    blockchain::BlockHeader,
    consensus::ConsensusFinder,
    fastest_tests::{fail, http_request, result, succeed, Fleet},
};
use crate::{
    config::AppConfig,
    errors::Web3ProxyError,
    frontend::rpc_proxy_ws::ProxyMode,
    jsonrpc::{JsonRpcRequestEnum, SingleRequest, ValidatedRequest},
};
use alloy::{
    primitives::{B256, U64},
    rpc::types::Header,
};
use serde_json::{json, Value};
use std::sync::{atomic::Ordering, Arc};
use tokio::time::{timeout, Duration, Instant};

fn modes() -> [ProxyMode; 5] {
    [
        ProxyMode::Best,
        ProxyMode::Fastest(1),
        ProxyMode::Fastest(2),
        ProxyMode::Fastest(0),
        ProxyMode::Versus,
    ]
}

async fn request(fleet: &Fleet, mode: ProxyMode) -> Arc<ValidatedRequest> {
    let mut request = fleet.request(1).await;
    Arc::get_mut(&mut request).unwrap().proxy_mode = mode;
    request
}

async fn drained(fleet: &Fleet) {
    fleet.app.frontend_tasks.close();
    timeout(Duration::from_secs(2), fleet.app.frontend_tasks.wait())
        .await
        .unwrap();
    fleet.idle();
}

#[tokio::test]
async fn healthy_primaries_keep_backups_out_of_every_mode() {
    super::test_support::with_cleanup(async {
        for mode in modes() {
            let mut fleet = Fleet::with_backups(2, 2, AppConfig::default(), &[1]).await;
            let task = fleet.start(request(&fleet, mode).await);
            let primary = fleet.nodes[0].next().await;
            fleet.nodes[1].quiet().await;
            assert!(!task.is_finished());
            succeed(primary, json!("primary"));
            assert_eq!(result(task).await["result"], "primary");
            drained(&fleet).await;
            assert_eq!(fleet.counts(), [1, 0]);
        }
    })
    .await;
}

#[tokio::test]
async fn backup_capacity_handles_unavailable_primaries_and_recovery() {
    super::test_support::with_cleanup(async {
        for mode in modes() {
            for cause in ["busy", "cooldown", "unhealthy"] {
                let mut fleet = Fleet::with_backups(2, 2, AppConfig::default(), &[1]).await;
                let held = if cause == "busy" {
                    (0..4)
                        .map(|_| fleet.nodes[0].rpc.request_permits.try_acquire().unwrap())
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                if cause == "cooldown" {
                    fleet.nodes[0]
                        .rpc
                        .hard_limit_until
                        .as_ref()
                        .unwrap()
                        .send_replace(Instant::now() + Duration::from_secs(30));
                }
                if cause == "unhealthy" {
                    fleet.nodes[0].rpc.healthy.store(false, Ordering::SeqCst);
                }
                let task = fleet.start(request(&fleet, mode).await);
                succeed(fleet.nodes[1].next().await, json!("backup"));
                assert_eq!(result(task).await["result"], "backup");
                assert_eq!(fleet.counts(), [0, 1]);
                drop(held);
                fleet.nodes[0].rpc.healthy.store(true, Ordering::SeqCst);
                fleet.nodes[0]
                    .rpc
                    .hard_limit_until
                    .as_ref()
                    .unwrap()
                    .send_replace(Instant::now());
                let task = fleet.start(request(&fleet, mode).await);
                succeed(fleet.nodes[0].next().await, json!("recovered"));
                assert_eq!(result(task).await["result"], "recovered");
                drained(&fleet).await;
                assert_eq!(fleet.counts(), [1, 1]);
            }
        }
    })
    .await;
}

#[tokio::test]
async fn primary_only_consensus_keeps_a_backup_available_for_failover() {
    super::test_support::with_cleanup(async {
        for mode in [
            ProxyMode::Best,
            ProxyMode::Fastest(2),
            ProxyMode::Fastest(0),
        ] {
            let mut fleet = Fleet::with_backups(1, 2, AppConfig::default(), &[1]).await;
            let ranking = fleet
                .app
                .balanced_rpcs
                .watch_ranked_rpcs
                .borrow()
                .clone()
                .unwrap();
            assert!(!ranking.backups_needed);
            assert_eq!(ranking.num_synced, 1);
            let task = fleet.start(request(&fleet, mode).await);
            fail(fleet.nodes[0].next().await, -32603, "Internal error");
            succeed(fleet.nodes[1].next().await, json!("fallback"));
            assert_eq!(result(task).await["result"], "fallback");
            drained(&fleet).await;
            assert_eq!(fleet.counts(), [1, 1]);
        }
    })
    .await;
}

#[tokio::test]
async fn fastest_waits_for_all_active_primaries_before_backup_failover() {
    super::test_support::with_cleanup(async {
        for count in [2, 0] {
            let mut fleet = Fleet::with_backups(3, 3, AppConfig::default(), &[2]).await;
            let task = fleet.start(request(&fleet, ProxyMode::Fastest(count)).await);
            let first = fleet.nodes[0].next().await;
            let second = fleet.nodes[1].next().await;
            fail(first, -32603, "Internal error");
            fleet.nodes[2].quiet().await;
            assert!(!task.is_finished());
            fail(second, -32603, "Internal error");
            succeed(fleet.nodes[2].next().await, json!("fallback"));
            assert_eq!(result(task).await["result"], "fallback");
            fleet.idle();
            assert_eq!(fleet.counts(), [1, 1, 1]);
        }
    })
    .await;
}

#[tokio::test]
async fn versus_does_not_replace_failed_primaries_with_backups() {
    super::test_support::with_cleanup(async {
        let mut fleet = Fleet::with_backups(3, 3, AppConfig::default(), &[2]).await;
        let task = fleet.start(request(&fleet, ProxyMode::Versus).await);
        fail(fleet.nodes[0].next().await, -32603, "Internal error");
        fail(fleet.nodes[1].next().await, -32603, "Internal error");
        assert!(matches!(
            timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap(),
            Err(Web3ProxyError::ExhaustedBackends(_))
        ));
        drained(&fleet).await;
        assert_eq!(fleet.counts(), [1, 1, 0]);
    })
    .await;
}

fn head(number: u64) -> BlockHeader {
    let mut header: Header = Header {
        hash: B256::with_last_byte(number as u8),
        ..Default::default()
    };
    header.inner.number = number;
    header.inner.parent_hash = B256::with_last_byte(number.saturating_sub(1) as u8);
    header.inner.timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    BlockHeader::new(Arc::new(header))
}

async fn rank(fleet: &Fleet, primary: u64, backup: u64, lag: u64) {
    let pool = &fleet.app.balanced_rpcs;
    for number in primary.saturating_sub(lag)..=backup {
        let head = head(number);
        pool.blocks_by_hash.insert(*head.hash(), head).await;
    }
    rank_heads(fleet, [head(primary), head(backup)], lag).await;
}

async fn rank_heads(fleet: &Fleet, heads: [BlockHeader; 2], lag: u64) {
    let pool = &fleet.app.balanced_rpcs;
    let mut finder = ConsensusFinder::new(None, U64::from(lag));
    for (index, head) in heads.into_iter().enumerate() {
        fleet.nodes[index]
            .rpc
            .head_block_sender
            .as_ref()
            .unwrap()
            .send_replace(Some(head.clone()));
        finder
            .update_rpc(
                Some(head),
                fleet.nodes[index].rpc.clone(),
                Instant::now(),
                pool,
            )
            .await
            .unwrap();
    }
    let ranking = finder.rank_rpcs(pool).await.unwrap().unwrap();
    pool.watch_head_block
        .as_ref()
        .unwrap()
        .send_replace(ranking.head_block.clone());
    pool.watch_ranked_rpcs.send_replace(Some(Arc::new(ranking)));
}

fn fork_head(number: u64) -> BlockHeader {
    let mut header = head(number).0.as_ref().clone();
    header.hash = B256::repeat_byte(0xff);
    BlockHeader::new(Arc::new(header))
}

async fn latest_code_request(fleet: &Fleet, mode: ProxyMode) -> Arc<ValidatedRequest> {
    let call = SingleRequest::new(
        9u64.into(),
        "eth_getCode".into(),
        sonic_rs::json!(["0x0000000000000000000000000000000000000000", "latest"]),
    )
    .unwrap();
    ValidatedRequest::new_with_app(
        &fleet.app,
        mode,
        None,
        call.into(),
        fleet.app.balanced_rpcs.head_block(),
        None,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn allowed_lag_keeps_local_heads_and_lagged_archive_capacity() {
    super::test_support::with_cleanup(async {
        for (lag, ahead) in [(5, 1), (5, 5), (5, 6), (2, 2), (2, 3)] {
            for mode in modes() {
                let mut fleet = Fleet::with_backups(2, 2, AppConfig::default(), &[1]).await;
                rank(&fleet, 100, 100 + ahead, lag).await;
                let expected = if ahead > lag { 100 + ahead } else { 100 };
                assert_eq!(
                    fleet.app.balanced_rpcs.head_block_num(),
                    Some(U64::from(expected))
                );
                let task = fleet.start(request(&fleet, mode).await);
                let index = usize::from(ahead > lag);
                succeed(fleet.nodes[index].next().await, json!(expected));
                assert_eq!(result(task).await["result"], expected);
                assert_eq!(fleet.counts(), if ahead > lag { [0, 1] } else { [1, 0] });

                let call = SingleRequest::new(
                    9u64.into(),
                    "eth_getCode".into(),
                    sonic_rs::json!(["0x0000000000000000000000000000000000000000", "0x5a"]),
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
                succeed(fleet.nodes[0].next().await, json!("0xbeef"));
                let response = result(task).await;
                assert_eq!(response["id"], 9);
                assert_eq!(response["result"], "0xbeef");
                drained(&fleet).await;
                assert_eq!(fleet.counts(), if ahead > lag { [1, 1] } else { [2, 0] });
            }
        }
    })
    .await;
}

#[tokio::test]
async fn missing_primary_history_uses_backup_without_relaxing_data_limits() {
    super::test_support::with_cleanup(async {
        for mode in modes() {
            let mut fleet = Fleet::with_backups(1, 2, AppConfig::default(), &[1]).await;
            rank(&fleet, 100, 100, 5).await;
            fleet.nodes[0]
                .rpc
                .block_data_limit
                .store(5, Ordering::SeqCst);
            let call = SingleRequest::new(
                9u64.into(),
                "eth_getCode".into(),
                sonic_rs::json!(["0x0000000000000000000000000000000000000000", "0x5a"]),
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
            succeed(fleet.nodes[1].next().await, json!("0xbeef"));
            assert_eq!(result(task).await["result"], "0xbeef");
            drained(&fleet).await;
            assert_eq!(fleet.counts(), [0, 1]);
        }
    })
    .await;
}

async fn cached(fleet: &Fleet, number: u64) -> Value {
    let cache = &fleet.app.balanced_rpcs.block_responses;
    timeout(Duration::from_secs(2), async {
        loop {
            if let Some(block) = cache
                .get(&super::blockchain::BlockResponseCacheKey::new(
                    *head(number).hash(),
                    true,
                ))
                .await
            {
                return serde_json::from_str(&sonic_rs::to_string(&block.result()).unwrap())
                    .unwrap();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

fn full_block(number: u64) -> Value {
    json!({"hash":head(number).hash(), "number":format!("0x{number:x}"), "transactions":[], "uncles":[]})
}

#[tokio::test]
async fn backup_headers_do_not_prefetch_ahead_of_local_consensus() {
    super::test_support::with_cleanup(async {
        for ahead in [1, 5, 6] {
            let mut fleet = Fleet::with_backups(2, 2, AppConfig::default(), &[1]).await;
            rank(&fleet, 100, 100 + ahead, 5).await;
            let coordinator = fleet.app.balanced_rpcs.block_hydration.clone();
            coordinator
                .announce(fleet.nodes[1].rpc.clone(), *head(100 + ahead).hash())
                .await;
            coordinator.announce_backups().await;
            if ahead <= 5 {
                fleet.nodes[1].quiet().await;
                coordinator
                    .announce(fleet.nodes[0].rpc.clone(), *head(100).hash())
                    .await;
                succeed(fleet.nodes[0].next().await, full_block(100));
                assert_eq!(cached(&fleet, 100).await["number"], "0x64");
                assert_eq!(fleet.counts(), [1, 0]);
            } else {
                succeed(fleet.nodes[1].next().await, full_block(106));
                assert_eq!(cached(&fleet, 106).await["number"], "0x6a");
                assert_eq!(fleet.counts(), [0, 1]);
            }
            fleet.idle();
        }
    })
    .await;
}

#[tokio::test]
async fn cache_fetch_uses_backup_only_after_primary_failure() {
    super::test_support::with_cleanup(async {
        let mut fleet = Fleet::with_backups(1, 2, AppConfig::default(), &[1]).await;
        rank(&fleet, 100, 100, 5).await;
        let coordinator = fleet.app.balanced_rpcs.block_hydration.clone();
        coordinator
            .announce(fleet.nodes[1].rpc.clone(), *head(100).hash())
            .await;
        fleet.nodes[1].quiet().await;
        coordinator
            .announce(fleet.nodes[0].rpc.clone(), *head(100).hash())
            .await;
        let primary = fleet.nodes[0].next().await;
        coordinator.announce_backups().await;
        fleet.nodes[1].quiet().await;
        // A valid RPC envelope with the wrong block hash must not fill the cache.
        succeed(primary, full_block(99));
        succeed(fleet.nodes[1].next().await, full_block(100));
        assert_eq!(cached(&fleet, 100).await["number"], "0x64");
        fleet.idle();
        assert_eq!(fleet.counts(), [1, 1]);
    })
    .await;
}

fn batch_task(fleet: &Fleet, count: usize) -> tokio::task::JoinHandle<Value> {
    let app = fleet.app.clone();
    super::test_support::spawn(async move {
        let requests = (0..count).map(|id| SingleRequest::new((id as u64).into(), "eth_call".into(), sonic_rs::json!([{"to":"0x0000000000000000000000000000000000000000", "data":"0x00"}, "latest"])).unwrap()).collect();
        let (_, response, _) = app
            .proxy_web3_rpc(ProxyMode::Best, JsonRpcRequestEnum::Batch(requests), None)
            .await
            .unwrap();
        serde_json::from_str(&response.to_json_string().await.unwrap()).unwrap()
    })
}

fn answer_packet(packet: super::batch_tests::Incoming) {
    let calls = packet.body.as_array().unwrap();
    let response = Value::Array(
        calls
            .iter()
            .map(|call| json!({"jsonrpc":"2.0", "id":call["id"], "result":"0x00"}))
            .collect(),
    );
    packet.respond(response);
}

#[tokio::test]
async fn batches_use_primary_capacity_before_backups() {
    super::test_support::with_cleanup(async {
        let mut fleet = Fleet::with_backups(2, 2, AppConfig::default(), &[1]).await;
        let task = batch_task(&fleet, 2);
        answer_packet(fleet.nodes[0].next().await);
        let response = timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.as_array().unwrap().len(), 2);
        // Backend accounting counts logical calls, not HTTP packets.
        assert_eq!(fleet.counts(), [2, 0]);

        // Four primary slots, each with a 64-item packet, leave two calls for
        // a backup. No running primary call is duplicated on that backup.
        let task = batch_task(&fleet, 258);
        let mut primary = Vec::new();
        for _ in 0..4 {
            let packet = fleet.nodes[0].next().await;
            assert_eq!(packet.body.as_array().unwrap().len(), 64);
            primary.push(packet);
        }
        let backup = fleet.nodes[1].next().await;
        assert_eq!(backup.body.as_array().unwrap().len(), 2);
        assert_eq!(fleet.counts(), [258, 2]);
        for packet in primary {
            answer_packet(packet);
        }
        answer_packet(backup);
        let response = timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        for (id, response) in response.as_array().unwrap().iter().enumerate() {
            assert_eq!(
                response,
                &json!({"jsonrpc":"2.0", "id":id, "result":"0x00"})
            );
        }
        fleet.idle();
    })
    .await;
}

#[tokio::test]
async fn unavailable_backups_preserve_primary_capacity_waits() {
    super::test_support::with_cleanup(async {
        for mode in modes() {
            let mut fleet = Fleet::with_backups(2, 2, AppConfig::default(), &[1]).await;
            let held: Vec<_> = (0..4)
                .map(|_| fleet.nodes[0].rpc.request_permits.try_acquire().unwrap())
                .collect();
            fleet.nodes[1].rpc.healthy.store(false, Ordering::SeqCst);
            let task = fleet.start(request(&fleet, mode).await);
            fleet.nodes[1].quiet().await;
            assert!(!task.is_finished());
            drop(held);
            succeed(fleet.nodes[0].next().await, json!("primary"));
            assert_eq!(result(task).await["result"], "primary");
            drained(&fleet).await;
            assert_eq!(fleet.counts(), [1, 0]);
        }
    })
    .await;
}

#[tokio::test]
async fn backup_failover_keeps_the_original_request_deadline() {
    super::test_support::with_cleanup(async {
        for mode in [ProxyMode::Best, ProxyMode::Fastest(2)] {
            let mut fleet = Fleet::with_backups(1, 2, AppConfig::default(), &[1]).await;
            let mut request = request(&fleet, mode).await;
            Arc::get_mut(&mut request).unwrap().expire_timeout = Duration::from_millis(150);
            let task = fleet.start(request);
            fail(fleet.nodes[0].next().await, -32603, "Internal error");
            let pending_backup = fleet.nodes[1].next().await;
            assert!(matches!(
                timeout(Duration::from_secs(1), task)
                    .await
                    .unwrap()
                    .unwrap(),
                Err(Web3ProxyError::Timeout(_))
            ));
            drop(pending_backup);
            fleet.idle();
            assert_eq!(fleet.counts(), [1, 1]);
        }
    })
    .await;
}

#[tokio::test]
async fn consensus_refresh_starts_deferred_backup_cache_fetch_on_outage() {
    super::test_support::with_cleanup(async {
        let mut fleet = Fleet::with_backups(2, 2, AppConfig::default(), &[1]).await;
        let pool = fleet.app.balanced_rpcs.clone();
        let mut finder = ConsensusFinder::new(None, U64::from(5));
        finder
            .process_block_from_rpc(
                &pool,
                super::blockchain::HeadObservation {
                    block: Some(head(100)),
                    rpc: fleet.nodes[0].rpc.clone(),
                    observed_at: Instant::now(),
                },
            )
            .await
            .unwrap();
        assert_eq!(pool.head_block_num(), Some(U64::from(100)));
        // The subscription reports its header before consensus processes it.
        pool.block_hydration
            .announce(fleet.nodes[1].rpc.clone(), *head(106).hash())
            .await;
        fleet.nodes[1].quiet().await;
        finder
            .process_block_from_rpc(
                &pool,
                super::blockchain::HeadObservation {
                    block: Some(head(106)),
                    rpc: fleet.nodes[1].rpc.clone(),
                    observed_at: Instant::now(),
                },
            )
            .await
            .unwrap();
        assert_eq!(pool.head_block_num(), Some(U64::from(106)));
        succeed(fleet.nodes[1].next().await, full_block(106));
        assert_eq!(cached(&fleet, 106).await["number"], "0x6a");
        fleet.idle();
        assert_eq!(fleet.counts(), [0, 1]);
    })
    .await;
}

#[tokio::test]
async fn review_divergent_backup_receives_no_head_bound_best_or_batch_calls() {
    super::test_support::with_cleanup(async {
        for batched in [false, true] {
            let mut fleet = Fleet::with_backups(2, 2, AppConfig::default(), &[1]).await;
            rank_heads(&fleet, [head(100), fork_head(100)], 5).await;
            let primary_hash = *head(100).hash();
            assert_eq!(fleet.app.balanced_rpcs.head_block().unwrap().hash(), &primary_hash);
            let held: Vec<_> = (0..4).map(|_| fleet.nodes[0].rpc.request_permits.try_acquire().unwrap()).collect();
            let (port, server) = fleet.frontend().await;
            let body = if batched {
                json!([
                    {"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":"0x0000000000000000000000000000000000000000","data":"0x00"},"latest"]},
                    {"jsonrpc":"2.0","id":2,"method":"eth_call","params":[{"to":"0x0000000000000000000000000000000000000000","data":"0x00"},"latest"]}
                ])
            } else {
                json!({"jsonrpc":"2.0","id":9,"method":"eth_getCode","params":["0x0000000000000000000000000000000000000000","latest"]})
            };
            let task = http_request(port, "/", body);
            fleet.nodes[1].quiet().await;
            assert!(!task.is_finished());
            drop(held);
            let primary = fleet.nodes[0].next().await;
            if batched {
                answer_packet(primary);
            } else {
                assert_eq!(primary.body["params"][1], "0x64");
                succeed(primary, json!("0xbeef"));
            }
            let response = timeout(Duration::from_secs(2), task).await.unwrap().unwrap();
            if batched {
                assert_eq!(response, json!([
                    {"jsonrpc":"2.0","id":1,"result":"0x00"},
                    {"jsonrpc":"2.0","id":2,"result":"0x00"}
                ]));
            } else {
                assert_eq!(response, json!({"jsonrpc":"2.0","id":9,"result":"0xbeef"}));
            }
            assert_eq!(fleet.counts(), if batched { [2, 0] } else { [1, 0] });
            drained(&fleet).await;
            drop(server);
        }
    }).await;
}

#[tokio::test]
async fn review_divergent_backup_does_not_replace_a_failed_primary() {
    super::test_support::with_cleanup(async {
        let mut fleet = Fleet::with_backups(2, 2, AppConfig::default(), &[1]).await;
        rank_heads(&fleet, [head(100), fork_head(100)], 5).await;
        let task = fleet.start(latest_code_request(&fleet, ProxyMode::Best).await);
        fail(fleet.nodes[0].next().await, -32603, "primary failed");
        fleet.nodes[1].quiet().await;
        assert!(
            matches!(timeout(Duration::from_secs(2), task).await.unwrap().unwrap(),
            Err(Web3ProxyError::JsonRpcErrorData(error)) if error.code == -32603)
        );
        assert_eq!(fleet.counts(), [1, 0]);
        fleet.idle();
    })
    .await;
}

#[tokio::test]
async fn review_pinned_latest_keeps_head_agreement_after_rankings_advance() {
    super::test_support::with_cleanup(async {
        let mut fleet = Fleet::with_backups(2, 2, AppConfig::default(), &[1]).await;
        rank_heads(&fleet, [head(100), fork_head(100)], 5).await;
        let request = latest_code_request(&fleet, ProxyMode::Best).await;
        rank_heads(&fleet, [head(101), fork_head(100)], 5).await;
        let held: Vec<_> = (0..4)
            .map(|_| fleet.nodes[0].rpc.request_permits.try_acquire().unwrap())
            .collect();
        let task = fleet.start(request);
        fleet.nodes[1].quiet().await;
        assert!(!task.is_finished());
        drop(held);
        let primary = fleet.nodes[0].next().await;
        assert_eq!(primary.body["params"][1], "0x64");
        succeed(primary, json!("0xbeef"));
        assert_eq!(
            result(task).await,
            json!({"jsonrpc":"2.0","id":9,"result":"0xbeef"})
        );
        assert_eq!(fleet.counts(), [1, 0]);
        fleet.idle();
    })
    .await;
}

#[tokio::test]
async fn review_descendant_backup_remains_eligible_for_head_bound_fallback() {
    super::test_support::with_cleanup(async {
        for mode in modes() {
            let mut fleet = Fleet::with_backups(2, 2, AppConfig::default(), &[1]).await;
            rank(&fleet, 100, 101, 5).await;
            let held: Vec<_> = (0..4)
                .map(|_| fleet.nodes[0].rpc.request_permits.try_acquire().unwrap())
                .collect();
            let task = fleet.start(latest_code_request(&fleet, mode).await);
            let backup = fleet.nodes[1].next().await;
            assert_eq!(backup.body["params"][1], "0x64");
            succeed(backup, json!("0xbeef"));
            assert_eq!(
                result(task).await,
                json!({"jsonrpc":"2.0","id":9,"result":"0xbeef"})
            );
            assert_eq!(fleet.counts(), [0, 1]);
            drop(held);
            drained(&fleet).await;
        }
    })
    .await;
}
