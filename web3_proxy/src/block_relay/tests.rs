use super::*;
use alloy::primitives::B256;
use alloy_rpc_types_beacon::block::{BeaconBlock, BeaconBlockBodyElectra};
use alloy_rpc_types_engine::ExecutionPayload;
use sonic_rs::{json, JsonValueTrait};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{
    sync::{broadcast, watch},
    time::{timeout, Instant},
};

mod consensus;
mod isolation;
mod mocks;
mod network;
mod review;
mod telemetry;
use mocks::{MockBeacon, MockRpc, Server};

fn network() -> config::Network {
    config::Network {
        genesis_validators_root: B256::with_last_byte(1),
        genesis_time: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 768,
        seconds_per_slot: 12,
        forks: vec![config::Fork {
            name: "electra".into(),
            version: "0x05000000".parse().unwrap(),
            epoch: 0,
        }],
    }
}

fn beacon(network: &config::Network) -> payload::BeaconResponse {
    let mut block: BeaconBlock<BeaconBlockBodyElectra<payload::BeaconPayload>> =
        sonic_rs::from_str(include_str!("fixtures/electra.json")).unwrap();
    block.slot = 64;
    block.body.blob_kzg_commitments.clear();
    block.body.execution_requests = Default::default();
    let ExecutionPayload::V3(execution) = &mut block.body.execution_payload.0 else {
        unreachable!()
    };
    execution.payload_inner.payload_inner.transactions.clear();
    execution.payload_inner.withdrawals.clear();
    execution.blob_gas_used = 0;
    execution.excess_blob_gas = 0;
    execution.payload_inner.payload_inner.timestamp = network.timestamp(block.slot).unwrap();
    execution.payload_inner.payload_inner.block_number = 100;
    execution.payload_inner.payload_inner.gas_limit = 30_000_000;
    execution.payload_inner.payload_inner.gas_used = 0;
    execution.payload_inner.payload_inner.base_fee_per_gas = alloy::primitives::U256::from(1);
    let mut response = payload::BeaconResponse {
        version: "electra".into(),
        execution_optimistic: false,
        finalized: false,
        data: alloy_rpc_types_beacon::block::SignedBeaconBlock {
            message: block,
            signature: Default::default(),
        },
    };
    rehash_execution(&mut response);
    response
}

fn rehash_execution(response: &mut payload::BeaconResponse) {
    let b = &mut response.data.message;
    let ExecutionPayload::V3(p) = &mut b.body.execution_payload.0 else {
        unreachable!()
    };
    let mut execution = p
        .clone()
        .try_into_block::<alloy::consensus::TxEnvelope>()
        .unwrap();
    execution.header.parent_beacon_block_root = Some(b.parent_root);
    execution.header.requests_hash = Some(b.body.execution_requests.to_requests().requests_hash());
    p.payload_inner.payload_inner.block_hash = execution.header.hash_slow();
}

fn decode(response: &payload::BeaconResponse, network: &config::Network) -> payload::RelayPayload {
    payload::RelayPayload::decode(
        &sonic_rs::to_vec(response).unwrap(),
        tree_hash::block_root(&response.data.message).unwrap(),
        network,
    )
    .unwrap()
}

async fn until(mut condition: impl FnMut() -> bool) {
    timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("condition was not reached");
}

#[test]
fn electra_root_matches_consensus_spec_vector() {
    let block: BeaconBlock<BeaconBlockBodyElectra<payload::BeaconPayload>> =
        sonic_rs::from_str(include_str!("fixtures/electra.json")).unwrap();
    let expected: B256 = "0x853653c6733c3275753665c3e1269f9f1fed274a67809dd9dec8245097f1aa7b"
        .parse()
        .unwrap();
    assert_eq!(tree_hash::block_root(&block).unwrap(), expected);
    let mut altered = block;
    altered.body.graffiti = B256::ZERO;
    assert_ne!(tree_hash::block_root(&altered).unwrap(), expected);
}

#[test]
fn encodes_all_three_nonempty_request_types_and_parent_root_exactly() {
    let network = network();
    let mut response = beacon(&network);
    response.data.message.body.execution_requests = sonic_rs::from_value(&json!({
        "deposits": [{"pubkey": format!("0x{}", "11".repeat(48)), "withdrawal_credentials": format!("0x{}", "22".repeat(32)),
            "amount": "1", "signature": format!("0x{}", "33".repeat(96)), "index": "2"}],
        "withdrawals": [{"source_address": format!("0x{}", "44".repeat(20)), "validator_pubkey": format!("0x{}", "55".repeat(48)), "amount": "3"}],
        "consolidations": [{"source_address": format!("0x{}", "66".repeat(20)), "source_pubkey": format!("0x{}", "77".repeat(48)), "target_pubkey": format!("0x{}", "88".repeat(48))}]
    })).unwrap();
    rehash_execution(&mut response);
    let encoded = decode(&response, &network);
    let body: sonic_rs::Value = sonic_rs::from_slice(&encoded.body).unwrap();
    assert_eq!(body["method"].as_str(), Some("engine_newPayloadV4"));
    assert_eq!(
        body["params"][2].as_str(),
        Some(response.data.message.parent_root.to_string().as_str())
    );
    assert_eq!(
        body["params"][3],
        json!([
            format!(
                "0x00{}{}0100000000000000{}0200000000000000",
                "11".repeat(48),
                "22".repeat(32),
                "33".repeat(96)
            ),
            format!("0x01{}{}0300000000000000", "44".repeat(20), "55".repeat(48)),
            format!(
                "0x02{}{}{}",
                "66".repeat(20),
                "77".repeat(48),
                "88".repeat(48)
            ),
        ])
    );
    assert_eq!(body["params"][0]["blockHash"], json!(encoded.hash));
    assert_eq!(body["params"][1], json!([]));
}

#[test]
fn empty_requests_are_omitted_and_fulu_uses_v4() {
    let mut network = network();
    network.forks[0].name = "fulu".into();
    let mut response = beacon(&network);
    response.version = "fulu".into();
    let encoded = decode(&response, &network);
    let body: sonic_rs::Value = sonic_rs::from_slice(&encoded.body).unwrap();
    assert_eq!(body["method"].as_str(), Some("engine_newPayloadV4"));
    assert_eq!(body["params"][3], json!([]));
}

#[test]
fn rejects_wrong_roots_execution_hashes_blobs_and_forks() {
    let network = network();
    let response = beacon(&network);
    let bytes = sonic_rs::to_vec(&response).unwrap();
    let root = tree_hash::block_root(&response.data.message).unwrap();
    assert_eq!(
        payload::RelayPayload::decode(&bytes, B256::ZERO, &network)
            .unwrap_err()
            .to_string(),
        "Beacon block root mismatch"
    );
    let mut bad = response.clone();
    let ExecutionPayload::V3(p) = &mut bad.data.message.body.execution_payload.0 else {
        unreachable!()
    };
    p.payload_inner.payload_inner.block_hash = B256::ZERO;
    assert_eq!(
        payload::RelayPayload::decode(
            &sonic_rs::to_vec(&bad).unwrap(),
            tree_hash::block_root(&bad.data.message).unwrap(),
            &network
        )
        .unwrap_err()
        .to_string(),
        "execution block hash mismatch"
    );
    let mut bad = response.clone();
    bad.data
        .message
        .body
        .blob_kzg_commitments
        .push(Default::default());
    assert_eq!(
        payload::RelayPayload::decode(
            &sonic_rs::to_vec(&bad).unwrap(),
            tree_hash::block_root(&bad.data.message).unwrap(),
            &network
        )
        .unwrap_err()
        .to_string(),
        "blob commitments do not match transactions"
    );
    let mut bad = response.clone();
    bad.version = "amsterdam".into();
    assert_eq!(
        payload::RelayPayload::decode(&sonic_rs::to_vec(&bad).unwrap(), root, &network)
            .unwrap_err()
            .to_string(),
        "unsupported Beacon fork"
    );
    let mut wrong_schedule = network.clone();
    wrong_schedule.forks[0].epoch = 3;
    assert_eq!(
        payload::RelayPayload::decode(&bytes, root, &wrong_schedule)
            .unwrap_err()
            .to_string(),
        "fork schedule mismatch"
    );
    let mut wrong_time = network;
    wrong_time.genesis_time += 1;
    assert_eq!(
        payload::RelayPayload::decode(&bytes, root, &wrong_time)
            .unwrap_err()
            .to_string(),
        "payload timestamp mismatch"
    );
}

#[test]
fn requires_complete_execution_requests_even_when_empty() {
    let network = network();
    let response = beacon(&network);
    let root = tree_hash::block_root(&response.data.message).unwrap();
    let mut raw = sonic_rs::to_value(&response).unwrap();
    raw["data"]["message"]["body"]["execution_requests"] =
        json!({"deposits": [], "withdrawals": []});
    assert_eq!(
        payload::RelayPayload::decode(&sonic_rs::to_vec(&raw).unwrap(), root, &network)
            .unwrap_err()
            .to_string(),
        "missing execution request list"
    );
}

fn work(hash: u8, parent: u8, number: u64, mode: config::Mode) -> Arc<Work> {
    let hash = B256::with_last_byte(hash);
    let parent_hash = B256::with_last_byte(parent);
    let body = sonic_rs::to_vec(&json!({"jsonrpc": "2.0", "id": 1, "method": "engine_newPayloadV4", "params": [
        {"blockHash": hash, "parentHash": parent_hash, "blockNumber": format!("0x{number:x}")}, [], B256::ZERO, []
    ]})).unwrap().into();
    let now = Instant::now();
    Arc::new(Work {
        first_seen_unix_us: stats::unix_micros(),
        first_seen_us: 0,
        mode_epoch: 0,
        payload: Arc::new(payload::RelayPayload {
            hash,
            parent_hash,
            number,
            slot: 64,
            beacon_root: hash,
            parent_beacon_root: B256::ZERO,
            body,
            fork: "electra".into(),
            signed_block: bytes::Bytes::from_static(b"{}"),
            blob_commitments: Vec::new(),
        }),
        first_seen: now,
        acquired: now,
        deadline: now + Duration::from_secs(12),
        source: "local".into(),
        announcement_source: "local".into(),
        event: "block_gossip",
        mode,
    })
}

struct Worker {
    tx: broadcast::Sender<Arc<Work>>,
    stop: watch::Sender<bool>,
    mode: watch::Sender<config::Mode>,
    stats: stats::Shared,
    cache: moka::future::Cache<B256, Arc<payload::RelayPayload>>,
    task: tokio::task::JoinHandle<()>,
}
impl Worker {
    async fn start(target: Arc<target::Target>, mode: config::Mode) -> Self {
        let name = target.name.clone();
        let worker = Self::spawn(target, mode);
        until(|| {
            worker
                .stats
                .lock()
                .execution_targets
                .get(&name)
                .is_some_and(|t| t.health.connected)
        })
        .await;
        worker
    }
    fn spawn(target: Arc<target::Target>, mode: config::Mode) -> Self {
        let (tx, rx) = broadcast::channel(128);
        let (stop, stop_rx) = watch::channel(false);
        let (mode, mode_rx) = watch::channel(mode);
        let stats = Arc::new(parking_lot::Mutex::new(stats::Stats {
            mode: *mode.borrow(),
            ..Default::default()
        }));
        let cache = moka::future::Cache::new(128);
        let task = tokio::spawn(target.clone().run(
            rx,
            target::WorkerContext {
                chain_id: 1,
                stop: stop_rx,
                mode: mode_rx,
                cache: cache.clone(),
                stats: stats.clone(),
            },
        ));
        Self {
            tx,
            stop,
            mode,
            stats,
            cache,
            task,
        }
    }
    async fn finish(self) {
        self.stop.send_replace(true);
        self.task.await.unwrap();
    }
}

#[tokio::test]
async fn observe_never_posts_and_rpc_errors_are_not_missing_blocks() {
    let rpc = MockRpc::new();
    rpc.state.lock().read_error = true;
    let server = Server::rpc(rpc.clone()).await;
    let target = rpc.target(&server.url, "a");
    let worker = Worker::start(target.clone(), config::Mode::Observe).await;
    let mut block = work(1, 0, 1, config::Mode::Observe);
    Arc::get_mut(&mut block).unwrap().deadline = Instant::now() + Duration::from_millis(30);
    worker.tx.send(block.clone()).unwrap();
    assert_eq!(
        target.observe(block, worker.stats.clone()).await,
        stats::Observation::default()
    );
    let sample = worker.stats.lock().samples.back().unwrap().clone();
    assert_eq!(sample.last_missing_us, None);
    assert_eq!(sample.first_ready_us, None);
    assert_eq!(worker.stats.lock().execution_targets["a"].incomplete, 1);
    worker.finish().await;
    assert_eq!(rpc.payload_hashes(), Vec::<B256>::new());
    assert_eq!(rpc.state.lock().bad_jwt, 0);
}

#[tokio::test]
async fn deduplicates_by_hash_and_keeps_competing_blocks_at_the_same_slot() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    let worker = Worker::start(rpc.target(&server.url, "a"), config::Mode::Inject).await;
    let a = work(1, 0, 1, config::Mode::Inject);
    worker.tx.send(a.clone()).unwrap();
    worker.tx.send(a).unwrap();
    worker.tx.send(work(2, 0, 1, config::Mode::Inject)).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].valid == 2).await;
    worker.finish().await;
    let mut hashes = rpc.payload_hashes();
    hashes.sort();
    assert_eq!(
        hashes,
        vec![B256::with_last_byte(1), B256::with_last_byte(2)]
    );
    assert_eq!(rpc.state.lock().max_inflight, 1);
    assert_eq!(rpc.state.lock().bad_jwt, 0);
    assert!(!rpc
        .state
        .lock()
        .methods
        .iter()
        .any(|m| m.starts_with("engine_forkchoice")));
}

#[tokio::test]
async fn repairs_cached_ancestors_oldest_first_then_retries_syncing_child_once() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    rpc.state.lock().known.insert(B256::with_last_byte(1), 1);
    rpc.state
        .lock()
        .replies
        .insert(B256::with_last_byte(3), ["SYNCING", "VALID"].into());
    let worker = Worker::start(rpc.target(&server.url, "a"), config::Mode::Inject).await;
    let parent = work(2, 1, 2, config::Mode::Inject);
    worker
        .cache
        .insert(parent.payload.hash, parent.payload.clone())
        .await;
    worker.tx.send(work(3, 2, 3, config::Mode::Inject)).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].valid == 2).await;
    assert_eq!(worker.stats.lock().execution_targets["a"].repairs, 1);
    worker.finish().await;
    assert_eq!(
        rpc.payload_hashes(),
        vec![
            B256::with_last_byte(3),
            B256::with_last_byte(2),
            B256::with_last_byte(3)
        ]
    );
}

#[tokio::test]
async fn invalid_ancestor_blocks_descendants_without_retry() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    rpc.state
        .lock()
        .replies
        .insert(B256::with_last_byte(1), ["INVALID"].into());
    let worker = Worker::start(rpc.target(&server.url, "a"), config::Mode::Inject).await;
    worker.tx.send(work(1, 0, 1, config::Mode::Inject)).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].invalid == 1).await;
    worker.tx.send(work(2, 1, 2, config::Mode::Inject)).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].skipped_invalid_ancestor == 1).await;
    worker.finish().await;
    assert_eq!(rpc.payload_hashes(), vec![B256::with_last_byte(1)]);
}

#[tokio::test]
async fn slow_target_does_not_delay_others_and_shutdown_drains_current_import() {
    let slow = MockRpc::new();
    let fast = MockRpc::new();
    let slow_server = Server::rpc(slow.clone()).await;
    let fast_server = Server::rpc(fast.clone()).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    slow.state
        .lock()
        .gates
        .insert(B256::with_last_byte(1), gate.clone());
    let mut a = Worker::start(slow.target(&slow_server.url, "a"), config::Mode::Inject).await;
    let b = Worker::start(fast.target(&fast_server.url, "b"), config::Mode::Inject).await;
    let block = work(1, 0, 1, config::Mode::Inject);
    a.tx.send(block.clone()).unwrap();
    b.tx.send(block).unwrap();
    until(|| b.stats.lock().execution_targets["b"].valid == 1 && slow.payload_hashes().len() == 1)
        .await;
    a.tx.send(work(2, 1, 2, config::Mode::Inject)).unwrap();
    a.stop.send_replace(true);
    assert!(
        timeout(Duration::from_millis(20), &mut a.task)
            .await
            .is_err(),
        "shutdown canceled an Engine import"
    );
    gate.notify_one();
    a.task.await.unwrap();
    b.finish().await;
    assert_eq!(slow.payload_hashes(), vec![B256::with_last_byte(1)]);
    assert_eq!(fast.payload_hashes(), vec![B256::with_last_byte(1)]);
}

#[tokio::test]
async fn switching_to_observe_stops_queued_injections() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    rpc.state
        .lock()
        .gates
        .insert(B256::with_last_byte(1), gate.clone());
    let worker = Worker::start(rpc.target(&server.url, "a"), config::Mode::Inject).await;
    worker.tx.send(work(1, 0, 1, config::Mode::Inject)).unwrap();
    until(|| rpc.payload_hashes().len() == 1).await;
    worker.tx.send(work(2, 1, 2, config::Mode::Inject)).unwrap();
    worker.mode.send_replace(config::Mode::Observe);
    gate.notify_one();
    until(|| worker.stats.lock().execution_targets["a"].valid == 1).await;
    // FIFO marker: the worker must consume the queued item without posting it.
    until(|| worker.tx.is_empty()).await;
    worker.finish().await;
    assert_eq!(rpc.payload_hashes(), vec![B256::with_last_byte(1)]);
}

#[derive(Clone, Debug)]
struct ConfigFixture {
    config: config::Config,
    _directory: Arc<tempfile::TempDir>,
}
impl std::ops::Deref for ConfigFixture {
    type Target = config::Config;
    fn deref(&self) -> &Self::Target {
        &self.config
    }
}
impl std::ops::DerefMut for ConfigFixture {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.config
    }
}
fn relay_config(
    network: config::Network,
    sources: &[(&str, &str)],
    execution_targets: &[(&str, &str)],
) -> ConfigFixture {
    let directory = Arc::new(tempfile::tempdir().unwrap());
    let config = config::Config {
        mode: config::Mode::Observe,
        state_dir: directory.path().to_owned(),
        network,
        cache_max_bytes: 1024 * 1024,
        consensus_targets: BTreeMap::new(),
        proof_workers: 2,
        sources: sources
            .iter()
            .map(|(name, url)| {
                (
                    name.to_string(),
                    config::Source {
                        beacon_url: url.to_string(),
                        headers: BTreeMap::new(),
                    },
                )
            })
            .collect(),
        execution_targets: execution_targets
            .iter()
            .map(|(name, url)| {
                (
                    name.to_string(),
                    config::ExecutionTarget {
                        ws_url: "ws://127.0.0.1:1".into(),
                        engine_url: url.to_string(),
                        rpc_url: url.to_string(),
                        jwt_secret_path: std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                            .join("src/block_relay/fixtures/test-jwt.hex"),
                    },
                )
            })
            .collect(),
    };
    ConfigFixture {
        config,
        _directory: directory,
    }
}

#[tokio::test]
async fn complete_pipeline_races_valid_sources_deduplicates_and_reloads_mode_without_reconnect() {
    let network = network();
    let local = MockBeacon::new(network.clone());
    let external = MockBeacon::new(network.clone());
    let local_server = Server::beacon(local.clone()).await;
    let external_server = Server::beacon(external.clone()).await;
    let a = MockRpc::new();
    let b = MockRpc::new();
    let a_server = Server::rpc(a.clone()).await;
    let b_server = Server::rpc(b.clone()).await;
    let mut config = relay_config(
        network.clone(),
        &[
            ("local", &local_server.url),
            ("external", &external_server.url),
        ],
        &[("a", &a_server.url), ("b", &b_server.url)],
    );
    let relay = BlockRelay::new();
    relay.apply(Some(&config)).await.unwrap();
    let (shutdown, _) = broadcast::channel(1);
    let run = tokio::spawn(relay.clone().run(1, shutdown.subscribe()));
    until(|| {
        local.events.receiver_count() == 1
            && external.events.receiver_count() == 1
            && relay.snapshot()["execution_targets"]["a"]["health"]["connected"].as_bool()
                == Some(true)
            && relay.snapshot()["execution_targets"]["b"]["health"]["connected"].as_bool()
                == Some(true)
    })
    .await;
    let block = beacon(&network);
    let payload = decode(&block, &network);
    let root = local.add(&block);
    external.add(&block);
    a.state.lock().known.insert(payload.hash, payload.number);
    b.state.lock().known.insert(payload.hash, payload.number);
    local.announce("block", root, block.data.message.slot);
    external.announce("block", root, block.data.message.slot);
    until(|| relay.samples().len() == 2).await;
    assert_eq!(a.payload_hashes(), Vec::<B256>::new());
    assert_eq!(b.payload_hashes(), Vec::<B256>::new());
    assert_eq!(relay.snapshot()["acquired"].as_u64(), Some(1));
    assert!(relay
        .samples()
        .iter()
        .all(|s| s.mode == config::Mode::Observe
            && s.first_ready_us.is_some()
            && s.last_missing_us.is_none()));

    config.mode = config::Mode::Inject;
    relay.apply(Some(&config)).await.unwrap();
    let mut competitor = block.clone();
    competitor.data.message.parent_root = B256::with_last_byte(42);
    rehash_execution(&mut competitor);
    let expected = decode(&competitor, &network);
    let root = local.add(&competitor);
    // The external endpoint returns an earlier block under the requested root. It must not win.
    external
        .state
        .lock()
        .blocks
        .insert(root, sonic_rs::to_vec(&block).unwrap());
    local.announce("block_gossip", root, competitor.data.message.slot);
    external.announce("block", root, competitor.data.message.slot);
    until(|| {
        relay.snapshot()["execution_targets"]["a"]["valid"].as_u64() == Some(1)
            && relay.snapshot()["execution_targets"]["b"]["valid"].as_u64() == Some(1)
    })
    .await;
    assert_eq!(a.payload_hashes(), vec![expected.hash]);
    assert_eq!(b.payload_hashes(), vec![expected.hash]);
    assert_eq!(relay.snapshot()["acquired"].as_u64(), Some(2));
    assert_eq!(local.state.lock().queries.len(), 1);
    assert_eq!(external.state.lock().queries.len(), 1);
    let status = relay.snapshot().to_string();
    assert!(!status.contains(&local_server.url));
    assert!(!status.contains("test-jwt"));
    assert!(relay.snapshot().get("samples").is_none());
    assert!(!status.contains(&root.to_string()));
    shutdown.send(()).unwrap();
    run.await.unwrap();
}

#[tokio::test]
async fn imported_event_recovers_gossip_404_and_unsupported_gossip_falls_back() {
    let network = network();
    let beacon_source = MockBeacon::new(network.clone());
    let beacon_server = Server::beacon(beacon_source.clone()).await;
    let rpc = MockRpc::new();
    let rpc_server = Server::rpc(rpc.clone()).await;
    let mut config = relay_config(
        network.clone(),
        &[("local", &beacon_server.url)],
        &[("a", &rpc_server.url)],
    );
    config.mode = config::Mode::Inject;
    let relay = BlockRelay::new();
    relay.apply(Some(&config)).await.unwrap();
    let (shutdown, _) = broadcast::channel(1);
    let run = tokio::spawn(relay.clone().run(1, shutdown.subscribe()));
    until(|| beacon_source.events.receiver_count() == 1).await;
    let block = beacon(&network);
    let root = beacon_source.add(&block);
    beacon_source.state.lock().hidden.insert(root);
    beacon_source.announce("block_gossip", root, block.data.message.slot);
    until(|| !beacon_source.state.lock().block_reads.is_empty()).await;
    assert_eq!(rpc.payload_hashes(), Vec::<B256>::new());
    beacon_source.state.lock().hidden.remove(&root);
    beacon_source.announce("block", root, block.data.message.slot);
    until(|| rpc.payload_hashes().len() == 1).await;
    shutdown.send(()).unwrap();
    run.await.unwrap();
    assert_eq!(rpc.payload_hashes(), vec![decode(&block, &network).hash]);

    beacon_source.state.lock().gossip_supported = false;
    let source =
        Arc::new(source::BeaconSource::new("fallback".into(), &config.sources["local"]).unwrap());
    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    let task = tokio::spawn(source.run(
        network,
        tx,
        Arc::new(parking_lot::Mutex::new(stats::Stats::default())),
    ));
    until(|| {
        beacon_source
            .state
            .lock()
            .queries
            .last()
            .is_some_and(|q| q == "topics=block")
    })
    .await;
    beacon_source.announce("block", root, block.data.message.slot);
    let event = timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((event.root, event.kind), (root, "block"));
    assert_eq!(
        beacon_source.state.lock().queries.last().unwrap(),
        "topics=block"
    );
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn rejects_wrong_network_capability_and_bad_config_without_losing_current_config() {
    let network = network();
    let source = MockBeacon::new(network.clone());
    let server = Server::beacon(source.clone()).await;
    let beacon_source = source::BeaconSource::new(
        "source".into(),
        &config::Source {
            beacon_url: server.url.clone(),
            headers: BTreeMap::new(),
        },
    )
    .unwrap();
    let mut wrong = network.clone();
    wrong.genesis_validators_root = B256::ZERO;
    assert_eq!(
        beacon_source
            .validate(&wrong)
            .await
            .unwrap_err()
            .to_string(),
        "Beacon genesis mismatch"
    );
    source.state.lock().network.forks.push(config::Fork {
        name: "future".into(),
        version: "0x07000000".parse().unwrap(),
        epoch: 3,
    });
    assert_eq!(
        beacon_source
            .validate(&network)
            .await
            .unwrap_err()
            .to_string(),
        "unconfigured Beacon fork; update relay schedule"
    );

    let rpc = MockRpc::new();
    let rpc_server = Server::rpc(rpc.clone()).await;
    let target = rpc.target(&rpc_server.url, "a");
    rpc.state.lock().chain_id = 2;
    assert_eq!(
        target.validate(1).await.unwrap_err().to_string(),
        "execution chain ID mismatch"
    );
    rpc.state.lock().chain_id = 1;
    rpc.state.lock().supports_v4 = false;
    assert_eq!(
        target.validate(1).await.unwrap_err().to_string(),
        "target lacks engine_newPayloadV4"
    );
    let config = relay_config(
        network,
        &[("source", &server.url)],
        &[("a", &rpc_server.url)],
    );
    let relay = BlockRelay::new();
    relay.apply(Some(&config)).await.unwrap();
    let mut bad = config.clone();
    bad.proof_workers = 0;
    assert_eq!(
        relay.apply(Some(&bad)).await.unwrap_err().to_string(),
        "proof workers must be between 1 and 16"
    );
    assert_eq!(
        relay.snapshot()["config_error"].as_str(),
        Some("proof workers must be between 1 and 16")
    );
    assert!(!format!("{bad:?}").contains("credential-must-not-leak"));
    relay.apply(Some(&config)).await.unwrap();
    assert!(relay.snapshot()["config_error"].is_null());
    assert_eq!(
        relay.apply(Some(&bad)).await.unwrap_err().to_string(),
        "proof workers must be between 1 and 16"
    );
    let mut new_mode = config;
    new_mode.mode = config::Mode::Inject;
    relay.apply(Some(&new_mode)).await.unwrap();
    assert!(relay.snapshot()["config_error"].is_null());
    assert_eq!(relay.snapshot()["mode"].as_str(), Some("inject"));
    assert_eq!(rpc.payload_hashes(), Vec::<B256>::new());
}

#[tokio::test]
async fn unknown_timeout_allows_new_work_without_claiming_the_lost_import() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    rpc.state
        .lock()
        .gates
        .insert(B256::with_last_byte(1), gate.clone());
    let worker = Worker::start(rpc.target(&server.url, "a"), config::Mode::Inject).await;
    let block = work(1, 0, 1, config::Mode::Inject);
    worker.tx.send(block.clone()).unwrap();
    worker.tx.send(block).unwrap();
    until(|| rpc.payload_hashes().len() == 1).await;
    timeout(Duration::from_secs(10), async {
        while worker.stats.lock().execution_targets["a"].unknown != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    worker.tx.send(work(2, 1, 2, config::Mode::Inject)).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].valid == 1).await;
    assert_eq!(worker.stats.lock().execution_targets["a"].unknown, 1);
    {
        let s = worker.stats.lock();
        let measured = &s.telemetry.modes[&config::Mode::Inject].targets["execution:a"];
        assert_eq!(measured.calls, 2);
        assert_eq!(measured.outcomes.get("unknown"), Some(&1));
        assert_eq!(measured.outcomes.get("valid"), Some(&1));
    }
    assert!(!rpc
        .state
        .lock()
        .known
        .contains_key(&B256::with_last_byte(1)));
    gate.notify_one();
    worker.finish().await;
    assert_eq!(
        rpc.payload_hashes(),
        vec![B256::with_last_byte(1), B256::with_last_byte(2)]
    );
    assert_eq!(rpc.state.lock().max_inflight, 2);
}

#[tokio::test]
async fn retries_syncing_child_when_missing_parent_arrives_later() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    rpc.state
        .lock()
        .replies
        .insert(B256::with_last_byte(2), ["SYNCING", "VALID"].into());
    let worker = Worker::start(rpc.target(&server.url, "a"), config::Mode::Inject).await;
    worker.tx.send(work(2, 1, 2, config::Mode::Inject)).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].repair_gaps == 1).await;
    worker.tx.send(work(1, 0, 1, config::Mode::Inject)).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].valid == 2).await;
    worker.finish().await;
    assert_eq!(
        rpc.payload_hashes(),
        vec![
            B256::with_last_byte(2),
            B256::with_last_byte(1),
            B256::with_last_byte(2)
        ]
    );
}

#[tokio::test]
async fn validates_response_hash_and_does_not_count_engine_valid_as_rpc_readiness() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    rpc.state.lock().wrong_valid_hash = true;
    let target = rpc.target(&server.url, "a");
    let worker = Worker::start(target, config::Mode::Inject).await;
    worker.tx.send(work(1, 0, 1, config::Mode::Inject)).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].unknown == 1).await;
    assert_eq!(worker.stats.lock().execution_targets["a"].valid, 0);
    assert_eq!(worker.stats.lock().execution_targets["a"].ready, 0);
    worker.finish().await;
    assert_eq!(rpc.payload_hashes(), vec![B256::with_last_byte(1)]);
}

#[tokio::test]
async fn wrong_rpc_block_identity_is_an_error_not_a_missing_block() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    rpc.state.lock().known.insert(B256::with_last_byte(1), 123);
    let target = rpc.target(&server.url, "a");
    assert_eq!(
        target
            .has_block(B256::with_last_byte(1), 124)
            .await
            .unwrap_err()
            .to_string(),
        "RPC block identity mismatch"
    );
    assert!(!target
        .has_block(B256::with_last_byte(2), 124)
        .await
        .unwrap());
    assert!(target
        .has_block(B256::with_last_byte(1), 123)
        .await
        .unwrap());
}

#[test]
fn sample_config_is_separate_from_routing_and_defaults_are_safe() {
    let input = include_str!("../../../docs/block-relay.example.toml")
        .replace("${GETH_JWT_PATH}", "/test/geth.jwt")
        .replace("${RETH_JWT_PATH}", "/test/reth.jwt");
    let top = crate::config::TopConfig::from_toml_str(&input).unwrap();
    assert!(top.balanced_rpcs.is_empty());
    assert!(top.extra.is_empty());
    let mut relay = top.block_relay.unwrap();
    assert_eq!(relay.mode, config::Mode::Observe);
    assert_eq!(relay.network.fork_at(411392 * 32).unwrap().name, "fulu");
    assert_eq!(
        relay.network.fork_at(411392 * 32 - 1).unwrap().name,
        "electra"
    );
    assert_eq!(relay.sources.len(), 2);
    assert_eq!(relay.execution_targets.len(), 2);
    relay.network.seconds_per_slot = 0;
    assert_eq!(
        relay.validate().unwrap_err().to_string(),
        "invalid slot duration"
    );
    relay.network.seconds_per_slot = 12;
    let geth = relay.execution_targets["geth"].clone();
    relay.execution_targets.insert("duplicate".into(), geth);
    assert_eq!(
        relay.validate().unwrap_err().to_string(),
        "duplicate Engine endpoint"
    );
}
