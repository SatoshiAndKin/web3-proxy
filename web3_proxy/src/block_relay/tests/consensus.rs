use super::*;
use crate::block_relay::{
    consensus::{ConsensusTarget, ConsensusWork},
    payload::ConsensusPayload,
};
use alloy::eips::eip4844::{self, Bytes48};

fn blob_block(
    network: &config::Network,
    fork: &str,
) -> (payload::BeaconResponse, Vec<alloy::primitives::Bytes>) {
    let mut block = beacon(network);
    block.version = fork.into();
    block.data.signature = alloy::primitives::FixedBytes::repeat_byte(7);
    block.data.message.body.blob_kzg_commitments = vec![Bytes48::repeat_byte(3)];
    rehash_execution(&mut block);
    (block, vec![vec![255; eip4844::BYTES_PER_BLOB].into()])
}

#[test]
fn consensus_contents_preserve_source_blocks_blobs_and_proofs_without_validation() {
    for fork in ["electra", "fulu"] {
        let network = network();
        let (response, blobs) = blob_block(&network, fork);
        let block = Arc::new(decode(&response, &network));
        // Deliberately invalid proof and blob bytes must reach the destination unchanged.
        let count = if fork == "fulu" {
            alloy::eips::eip7594::CELLS_PER_EXT_BLOB
        } else {
            1
        };
        let proofs = (0..count)
            .map(|index| Bytes48::repeat_byte(index as u8))
            .collect::<Vec<_>>();
        let packet = ConsensusPayload::from_blobs(block, blobs.clone(), proofs.clone()).unwrap();
        let contents: sonic_rs::Value = sonic_rs::from_slice(&packet.body).unwrap();
        assert_eq!(
            contents["signed_block"],
            sonic_rs::to_value(&response.data).unwrap()
        );
        assert_eq!(contents["blobs"], sonic_rs::to_value(&blobs).unwrap());
        assert_eq!(contents["kzg_proofs"], sonic_rs::to_value(&proofs).unwrap());
    }
}

#[test]
fn consensus_publication_preserves_raw_signed_block_json() {
    let network = network();
    let response = beacon(&network);
    let signed = format!(
        r#"{{ "message": {}, "signature": {}, "source_extension": {{ "keep": true }} }}"#,
        sonic_rs::to_string(&response.data.message).unwrap(),
        sonic_rs::to_string(&response.data.signature).unwrap(),
    );
    let source = format!(r#"{{"version":"electra","data":{signed}}}"#);
    let block = Arc::new(payload::RelayPayload::decode(source.as_bytes(), B256::ZERO).unwrap());
    let packet = ConsensusPayload::from_blobs(block, Vec::new(), Vec::new()).unwrap();
    let forwarded = sonic_rs::get(packet.body.as_ref(), ["signed_block"]).unwrap();
    assert_eq!(forwarded.as_raw_str(), signed);
}

#[test]
fn empty_blob_blocks_publish_empty_proof_and_blob_arrays() {
    let network = network();
    let response = beacon(&network);
    let packet = ConsensusPayload::from_blobs(
        Arc::new(decode(&response, &network)),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let contents: sonic_rs::Value = sonic_rs::from_slice(&packet.body).unwrap();
    assert_eq!(
        contents,
        json!({"signed_block": response.data, "kzg_proofs": [], "blobs": []})
    );
}

fn target_config(url: &str) -> config::ConsensusTarget {
    config::ConsensusTarget {
        beacon_url: url.into(),
        headers: BTreeMap::new(),
    }
}

async fn start_relay(
    config: &config::Config,
) -> (
    Arc<BlockRelay>,
    broadcast::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let relay = BlockRelay::new();
    relay.apply(Some(config)).await.unwrap();
    let (stop, _) = broadcast::channel(1);
    let run = tokio::spawn(relay.clone().run(1, stop.subscribe()));
    (relay, stop, run)
}

#[tokio::test]
async fn relays_engine_blob_and_proof_bytes_and_leaves_rejection_to_consensus_nodes() {
    for fork in ["electra", "fulu"] {
        let mut network = network();
        network.forks[0].name = fork.into();
        let (block, blobs) = blob_block(&network, fork);
        let hashes = decode(&block, &network).versioned_hashes();
        let source = MockBeacon::new(network.clone());
        let source_server = Server::beacon(source.clone()).await;
        let root = source.add(&block);
        let engine = MockRpc::new();
        let engine_server = Server::rpc(engine.clone()).await;
        let count = if fork == "fulu" {
            alloy::eips::eip7594::CELLS_PER_EXT_BLOB
        } else {
            1
        };
        let proofs = (0..count)
            .map(|i| Bytes48::repeat_byte(i as u8))
            .collect::<Vec<_>>();
        let data = if fork == "fulu" {
            json!({"blob": blobs[0], "proofs": proofs})
        } else {
            json!({"blob": blobs[0], "proof": proofs[0]})
        };
        engine.state.lock().blobs.insert(hashes[0], data);
        let target = MockBeacon::new(network.clone());
        target.state.lock().publication_status = 400;
        let target_server = Server::beacon(target.clone()).await;
        let mut config = relay_config(
            network,
            &[("source", &source_server.url)],
            &[("el", &engine_server.url)],
        );
        config
            .consensus_targets
            .insert("cl".into(), target_config(&target_server.url));
        config.mode = config::Mode::Inject;
        let (relay, stop, run) = start_relay(&config).await;
        until(|| source.events.receiver_count() == 1).await;
        source.announce("block_gossip", root, block.data.message.slot);
        until(|| relay.snapshot()["consensus_targets"]["cl"]["rejected"].as_u64() == Some(1)).await;
        stop.send(()).unwrap();
        run.await.unwrap();
        assert_eq!(engine.state.lock().blob_requests, vec![hashes]);
        assert_eq!(engine.state.lock().bad_jwt, 0);
        let method = if fork == "fulu" {
            "engine_getBlobsV2"
        } else {
            "engine_getBlobsV1"
        };
        assert!(engine.state.lock().methods.iter().any(|m| m == method));
        assert!(source.state.lock().blob_reads.is_empty());
        let state = target.state.lock();
        assert_eq!(state.publications[0].0, root);
        assert_eq!(
            state.publications[0].2["signed_block"],
            sonic_rs::to_value(&block.data).unwrap()
        );
        assert_eq!(
            state.publications[0].2["blobs"],
            sonic_rs::to_value(&blobs).unwrap()
        );
        assert_eq!(
            state.publications[0].2["kzg_proofs"],
            sonic_rs::to_value(&proofs).unwrap()
        );
    }
}

#[tokio::test]
async fn late_engine_blobs_retry_without_delaying_execution() {
    for fork in ["electra", "fulu"] {
        let network = network();
        let (block, blobs) = blob_block(&network, fork);
        let payload = decode(&block, &network);
        let hashes = payload.versioned_hashes();
        let source = MockBeacon::new(network.clone());
        let source_server = Server::beacon(source.clone()).await;
        let root = source.add(&block);
        let engine = MockRpc::new();
        let engine_server = Server::rpc(engine.clone()).await;
        let target = MockBeacon::new(network.clone());
        let target_server = Server::beacon(target.clone()).await;
        let slow = Server::start(axum::Router::new().fallback(|| async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        }))
        .await;
        let mut config = relay_config(
            network,
            &[("source", &source_server.url)],
            &[("el", &engine_server.url), ("slow", &slow.url)],
        );
        config
            .consensus_targets
            .insert("cl".into(), target_config(&target_server.url));
        config.mode = config::Mode::Inject;
        let (relay, stop, run) = start_relay(&config).await;
        until(|| source.events.receiver_count() == 1).await;
        source.announce("block_gossip", root, block.data.message.slot);
        until(|| engine.state.lock().blob_requests.len() >= 2).await;
        assert_eq!(engine.payload_hashes(), vec![payload.hash]);
        assert!(target.state.lock().publications.is_empty());
        let data = if fork == "fulu" {
            json!({"blob": blobs[0], "proofs": [Bytes48::repeat_byte(7)]})
        } else {
            json!({"blob": blobs[0], "proof": Bytes48::repeat_byte(7)})
        };
        engine.state.lock().blobs.insert(hashes[0], data);
        timeout(
            Duration::from_millis(250),
            until(|| target.state.lock().publications.len() == 1),
        )
        .await
        .unwrap();
        stop.send(()).unwrap();
        run.await.unwrap();
        assert_eq!(
            relay.snapshot()["consensus_acquisition_failed"].as_u64(),
            Some(0)
        );
        assert_eq!(engine.payload_hashes(), vec![payload.hash]);
        assert!(source.state.lock().blob_reads.is_empty());
    }
}

#[tokio::test]
async fn missing_engine_blobs_do_not_fetch_proofless_beacon_blobs_or_block_execution_delivery() {
    let mut network = network();
    network.seconds_per_slot = 1;
    network.genesis_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 64;
    network.forks[0].name = "fulu".into();
    let (block, _) = blob_block(&network, "fulu");
    let hash = decode(&block, &network).hash;
    let source = MockBeacon::new(network.clone());
    let source_server = Server::beacon(source.clone()).await;
    let root = source.add(&block);
    let engine = MockRpc::new();
    let engine_server = Server::rpc(engine.clone()).await;
    let target = MockBeacon::new(network.clone());
    let target_server = Server::beacon(target.clone()).await;
    let mut config = relay_config(
        network,
        &[("source", &source_server.url)],
        &[("el", &engine_server.url)],
    );
    config
        .consensus_targets
        .insert("cl".into(), target_config(&target_server.url));
    config.mode = config::Mode::Inject;
    let (relay, stop, run) = start_relay(&config).await;
    until(|| source.events.receiver_count() == 1).await;
    source.announce("block_gossip", root, block.data.message.slot);
    until(|| {
        relay.snapshot()["consensus_acquisition_failed"].as_u64() == Some(1)
            && engine.payload_hashes().contains(&hash)
    })
    .await;
    stop.send(()).unwrap();
    run.await.unwrap();
    assert!(source.state.lock().blob_reads.is_empty());
    assert!(target.state.lock().publications.is_empty());
}

#[tokio::test]
async fn two_forwarders_send_to_all_three_execution_and_consensus_targets() {
    let network = network();
    let source = MockBeacon::new(network.clone());
    let source_server = Server::beacon(source.clone()).await;
    let block = beacon(&network);
    let root = source.add(&block);
    let hash = decode(&block, &network).hash;
    let mut config = relay_config(network.clone(), &[("source", &source_server.url)], &[]);
    config.mode = config::Mode::Inject;
    let mut servers = Vec::new();
    let mut engines = Vec::new();
    let mut beacons = Vec::new();
    let mut gates = Vec::new();
    for index in 0..3 {
        let engine = MockRpc::new();
        let server = Server::rpc(engine.clone()).await;
        let gate = Arc::new(tokio::sync::Notify::new());
        engine.state.lock().gates.insert(hash, gate.clone());
        gates.push(gate);
        config.execution_targets.insert(
            format!("el-{index}"),
            relay_config(network.clone(), &[], &[("x", &server.url)])
                .execution_targets
                .remove("x")
                .unwrap(),
        );
        servers.push(server);
        engines.push(engine);
        let beacon = MockBeacon::new(network.clone());
        let server = Server::beacon(beacon.clone()).await;
        let gate = Arc::new(tokio::sync::Notify::new());
        beacon
            .state
            .lock()
            .publication_gates
            .insert(root, gate.clone());
        gates.push(gate);
        config
            .consensus_targets
            .insert(format!("cl-{index}"), target_config(&server.url));
        servers.push(server);
        beacons.push(beacon);
    }
    let (a, stop_a, run_a) = start_relay(&config).await;
    let second_directory = tempfile::tempdir().unwrap();
    config.state_dir = second_directory.path().to_owned();
    let (b, stop_b, run_b) = start_relay(&config).await;
    until(|| source.events.receiver_count() == 2).await;
    source.announce("block", root, block.data.message.slot);
    until(|| {
        engines.iter().all(|e| e.payload_hashes().len() == 2)
            && beacons
                .iter()
                .all(|b| b.state.lock().publications.len() == 2)
    })
    .await;
    for gate in gates {
        gate.notify_waiters();
    }
    until(|| {
        a.snapshot()["fleet_ready_latency"]["count"].as_u64() == Some(1)
            && b.snapshot()["fleet_ready_latency"]["count"].as_u64() == Some(1)
    })
    .await;
    stop_a.send(()).unwrap();
    stop_b.send(()).unwrap();
    run_a.await.unwrap();
    run_b.await.unwrap();
    for engine in engines {
        assert_eq!(engine.payload_hashes(), vec![hash, hash]);
        assert_eq!(engine.state.lock().max_inflight, 2);
        assert!(!engine
            .state
            .lock()
            .methods
            .iter()
            .any(|m| m.starts_with("engine_forkchoice")));
    }
    for beacon in beacons {
        let s = beacon.state.lock();
        assert_eq!(s.publications.len(), 2);
        for (published_root, fork, contents) in &s.publications {
            assert_eq!(*published_root, root);
            assert_eq!(fork, "electra");
            assert_eq!(
                contents["signed_block"],
                sonic_rs::to_value(&block.data).unwrap()
            );
        }
    }
    assert_eq!(a.samples().len(), 6);
    assert_eq!(b.samples().len(), 6);
    assert_eq!(
        a.snapshot()["fleet_canonical_latency"]["count"].as_u64(),
        Some(1)
    );
}

#[tokio::test]
async fn unavailable_blobs_do_not_delay_execution_and_observe_mode_never_publishes() {
    let mut network = network();
    network.seconds_per_slot = 1;
    network.genesis_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 64;
    let (block, blobs) = blob_block(&network, "electra");
    let source = MockBeacon::new(network.clone());
    let source_server = Server::beacon(source.clone()).await;
    let root = source.add(&block);
    let hash = decode(&block, &network).hash;
    let engine = MockRpc::new();
    let engine_server = Server::rpc(engine.clone()).await;
    let target = MockBeacon::new(network.clone());
    let target_server = Server::beacon(target.clone()).await;
    let mut config = relay_config(
        network,
        &[("local", &source_server.url)],
        &[("el", &engine_server.url)],
    );
    config
        .consensus_targets
        .insert("cl".into(), target_config(&target_server.url));
    config.mode = config::Mode::Inject;
    let (relay, stop, run) = start_relay(&config).await;
    until(|| source.events.receiver_count() == 1).await;
    source.announce("block_gossip", root, block.data.message.slot);
    until(|| {
        engine.payload_hashes() == [hash]
            && relay.snapshot()["consensus_acquisition_failed"].as_u64() == Some(1)
    })
    .await;
    assert!(source.state.lock().blob_reads.is_empty());
    assert_eq!(target.state.lock().publications.len(), 0);
    config.mode = config::Mode::Observe;
    relay.apply(Some(&config)).await.unwrap();
    let mut next = block.clone();
    next.data.message.slot += 1;
    rehash_execution(&mut next);
    let next_root = source.add(&next);
    let hashes = decode(&next, &config.network).versioned_hashes();
    engine.state.lock().blobs.insert(
        hashes[0],
        json!({"blob": blobs[0], "proof": Bytes48::repeat_byte(7)}),
    );
    source.announce("block", next_root, next.data.message.slot);
    until(|| relay.snapshot()["consensus_acquired"].as_u64() == Some(1)).await;
    stop.send(()).unwrap();
    run.await.unwrap();
    assert_eq!(engine.payload_hashes(), vec![hash]);
    assert_eq!(target.state.lock().publications.len(), 0);
}

#[tokio::test]
async fn optimistic_headers_are_not_completed_imports_or_missing_blocks() {
    let network = network();
    let response = beacon(&network);
    let beacon = MockBeacon::new(network.clone());
    let server = Server::beacon(beacon.clone()).await;
    beacon.import(&response, true, true);
    let target = Arc::new(
        ConsensusTarget::new(
            "cl".into(),
            &target_config(&server.url),
            Duration::from_secs(768),
        )
        .unwrap(),
    );
    let mut w = work(1, 0, 1, config::Mode::Observe);
    let w_mut = Arc::get_mut(&mut w).unwrap();
    w_mut.payload = Arc::new(decode(&response, &network));
    w_mut.deadline = Instant::now() + Duration::from_millis(60);
    let stats = Arc::new(parking_lot::Mutex::new(stats::Stats::default()));
    let observed = target.observe(w, stats.clone()).await;
    assert_eq!(observed, stats::Observation::default());
    assert_eq!(stats.lock().consensus_targets["cl"].optimistic, 1);
    assert_eq!(stats.lock().samples.back().unwrap().last_missing_us, None);
    assert_eq!(beacon.state.lock().publications.len(), 0);
}

#[tokio::test]
async fn publication_202_without_import_does_not_count_as_ready() {
    let network = network();
    let response = beacon(&network);
    let beacon = MockBeacon::new(network.clone());
    beacon.state.lock().publication_status = 202;
    beacon.state.lock().import_publications = false;
    let server = Server::beacon(beacon.clone()).await;
    let target = Arc::new(
        ConsensusTarget::new(
            "cl".into(),
            &target_config(&server.url),
            Duration::from_secs(768),
        )
        .unwrap(),
    );
    let mut w = work(1, 0, 1, config::Mode::Inject);
    let w_mut = Arc::get_mut(&mut w).unwrap();
    w_mut.payload = Arc::new(decode(&response, &network));
    w_mut.deadline = Instant::now() + Duration::from_millis(200);
    let payload =
        Arc::new(ConsensusPayload::from_blobs(w.payload.clone(), Vec::new(), Vec::new()).unwrap());
    let stats = Arc::new(parking_lot::Mutex::new(stats::Stats::default()));
    let (tx, rx) = broadcast::channel(8);
    let (stop, stop_rx) = watch::channel(false);
    let (_, mode) = watch::channel(config::Mode::Inject);
    let run = tokio::spawn(target.clone().run(
        rx,
        crate::block_relay::consensus::Context {
            network,
            stop: stop_rx,
            mode,
            cache: moka::future::Cache::new(16),
            stats: stats.clone(),
        },
    ));
    tx.send(Arc::new(ConsensusWork {
        work: w.clone(),
        payload,
    }))
    .unwrap();
    let observed = target.observe(w, stats.clone()).await;
    stop.send_replace(true);
    run.await.unwrap();
    assert_eq!(observed, stats::Observation::default());
    assert_eq!(stats.lock().consensus_targets["cl"].accepted, 1);
    assert_eq!(stats.lock().consensus_targets["cl"].ready, 0);
    assert_eq!(stats.lock().consensus_targets["cl"].published, 0);
}

fn child_of(
    parent: &payload::BeaconResponse,
    network: &config::Network,
) -> payload::BeaconResponse {
    let mut child = parent.clone();
    child.data.message.slot += 1;
    child.data.message.parent_root = fixture_root(&parent.data.message);
    let ExecutionPayload::V3(p) = &mut child.data.message.body.execution_payload.0 else {
        unreachable!()
    };
    p.payload_inner.payload_inner.parent_hash = p.payload_inner.payload_inner.block_hash;
    p.payload_inner.payload_inner.block_number += 1;
    p.payload_inner.payload_inner.timestamp = network.timestamp(child.data.message.slot).unwrap();
    rehash_execution(&mut child);
    child
}

fn consensus_work(
    response: &payload::BeaconResponse,
    network: &config::Network,
) -> Arc<ConsensusWork> {
    let block = Arc::new(decode(response, network));
    let now = Instant::now();
    let work = Arc::new(Work {
        first_seen_unix_us: stats::unix_micros(),
        first_seen_us: 0,
        mode_epoch: 0,
        payload: block.clone(),
        first_seen: now,
        acquired: now,
        deadline: now + Duration::from_secs(12),
        source: "test".into(),
        announcement_source: "test".into(),
        event: "block",
        mode: config::Mode::Inject,
    });
    Arc::new(ConsensusWork {
        work,
        payload: Arc::new(ConsensusPayload::from_blobs(block, Vec::new(), Vec::new()).unwrap()),
    })
}

#[tokio::test]
async fn consensus_repairs_cached_ancestors_oldest_first_and_retries_child() {
    let network = network();
    let anchor = beacon(&network);
    let parent = child_of(&anchor, &network);
    let child = child_of(&parent, &network);
    let beacon = MockBeacon::new(network.clone());
    beacon.state.lock().require_parent = true;
    beacon.import(&anchor, false, true);
    let server = Server::beacon(beacon.clone()).await;
    let target = Arc::new(
        ConsensusTarget::new(
            "cl".into(),
            &target_config(&server.url),
            Duration::from_secs(768),
        )
        .unwrap(),
    );
    let parent = consensus_work(&parent, &network);
    let child = consensus_work(&child, &network);
    let cache = moka::future::Cache::new(16);
    cache
        .insert(parent.payload.block.beacon_root, parent.payload.clone())
        .await;
    let stats = Arc::new(parking_lot::Mutex::new(stats::Stats::default()));
    let (tx, rx) = broadcast::channel(8);
    let (stop, stop_rx) = watch::channel(false);
    let (_, mode) = watch::channel(config::Mode::Inject);
    let run = tokio::spawn(target.run(
        rx,
        crate::block_relay::consensus::Context {
            network,
            stop: stop_rx,
            mode,
            cache,
            stats: stats.clone(),
        },
    ));
    tx.send(child.clone()).unwrap();
    until(|| {
        stats
            .lock()
            .consensus_targets
            .get("cl")
            .is_some_and(|t| t.published == 2)
    })
    .await;
    stop.send_replace(true);
    run.await.unwrap();
    assert_eq!(
        beacon
            .state
            .lock()
            .publications
            .iter()
            .map(|(root, _, _)| *root)
            .collect::<Vec<_>>(),
        vec![
            child.payload.block.beacon_root,
            parent.payload.block.beacon_root,
            child.payload.block.beacon_root
        ]
    );
    assert_eq!(stats.lock().consensus_targets["cl"].accepted, 1);
    assert_eq!(stats.lock().consensus_targets["cl"].repairs, 1);
}

#[tokio::test]
async fn consensus_parent_readiness_wakes_waiting_child_without_republishing_parent() {
    let network = network();
    let parent = beacon(&network);
    let child = child_of(&parent, &network);
    let beacon = MockBeacon::new(network.clone());
    beacon.state.lock().require_parent = true;
    let server = Server::beacon(beacon.clone()).await;
    let target = Arc::new(
        ConsensusTarget::new(
            "cl".into(),
            &target_config(&server.url),
            Duration::from_secs(768),
        )
        .unwrap(),
    );
    let parent_work = consensus_work(&parent, &network);
    let child_work = consensus_work(&child, &network);
    let stats = Arc::new(parking_lot::Mutex::new(stats::Stats::default()));
    let (tx, rx) = broadcast::channel(8);
    let (stop, stop_rx) = watch::channel(false);
    let (_, mode) = watch::channel(config::Mode::Inject);
    let run = tokio::spawn(target.clone().run(
        rx,
        crate::block_relay::consensus::Context {
            network,
            stop: stop_rx,
            mode,
            cache: moka::future::Cache::new(16),
            stats: stats.clone(),
        },
    ));
    tx.send(child_work.clone()).unwrap();
    until(|| {
        stats
            .lock()
            .consensus_targets
            .get("cl")
            .is_some_and(|t| t.repair_gaps == 1)
    })
    .await;
    beacon.import(&parent, false, true);
    assert!(target
        .observe(parent_work.work.clone(), stats.clone())
        .await
        .ready
        .is_some());
    until(|| stats.lock().consensus_targets["cl"].published == 1).await;
    stop.send_replace(true);
    run.await.unwrap();
    assert_eq!(
        beacon
            .state
            .lock()
            .publications
            .iter()
            .map(|(root, _, _)| *root)
            .collect::<Vec<_>>(),
        vec![child_work.payload.block.beacon_root; 2]
    );
}

#[tokio::test]
async fn fresh_observe_mode_fetches_both_layers_without_any_submission() {
    let network = network();
    let block = beacon(&network);
    let source = MockBeacon::new(network.clone());
    let source_server = Server::beacon(source.clone()).await;
    let root = source.add(&block);
    let engine = MockRpc::new();
    let engine_server = Server::rpc(engine.clone()).await;
    let beacon = MockBeacon::new(network.clone());
    let beacon_server = Server::beacon(beacon.clone()).await;
    let mut config = relay_config(
        network,
        &[("source", &source_server.url)],
        &[("el", &engine_server.url)],
    );
    config
        .consensus_targets
        .insert("cl".into(), target_config(&beacon_server.url));
    let (relay, stop, run) = start_relay(&config).await;
    until(|| source.events.receiver_count() == 1).await;
    source.announce("block", root, block.data.message.slot);
    until(|| relay.snapshot()["consensus_acquired"].as_u64() == Some(1)).await;
    stop.send(()).unwrap();
    run.await.unwrap();
    assert_eq!(engine.payload_hashes(), Vec::<B256>::new());
    assert_eq!(beacon.state.lock().publications.len(), 0);
    assert_eq!(
        relay.snapshot()["execution_targets"]["el"]["sent"].as_u64(),
        Some(0)
    );
    assert_eq!(
        relay.snapshot()["consensus_targets"]["cl"]["sent"].as_u64(),
        Some(0)
    );
}

#[tokio::test]
async fn stalled_consensus_import_observations_do_not_delay_new_publications() {
    let network = network();
    let first = beacon(&network);
    let next = child_of(&first, &network);
    let beacon = MockBeacon::new(network.clone());
    beacon.state.lock().header_gate = Some(Arc::new(tokio::sync::Notify::new()));
    let server = Server::beacon(beacon.clone()).await;
    let target = Arc::new(
        ConsensusTarget::new(
            "cl".into(),
            &target_config(&server.url),
            Duration::from_secs(768),
        )
        .unwrap(),
    );
    let stats = Arc::new(parking_lot::Mutex::new(stats::Stats::default()));
    let (tx, rx) = broadcast::channel(8);
    let (stop, stop_rx) = watch::channel(false);
    let (_, mode) = watch::channel(config::Mode::Inject);
    let run = tokio::spawn(target.run(
        rx,
        crate::block_relay::consensus::Context {
            network: network.clone(),
            stop: stop_rx,
            mode,
            cache: moka::future::Cache::new(16),
            stats: stats.clone(),
        },
    ));
    tx.send(consensus_work(&first, &network)).unwrap();
    until(|| {
        stats
            .lock()
            .consensus_targets
            .get("cl")
            .is_some_and(|t| t.published == 1)
    })
    .await;
    tx.send(consensus_work(&next, &network)).unwrap();
    let delivered = timeout(
        Duration::from_millis(500),
        until(|| stats.lock().consensus_targets["cl"].published == 2),
    )
    .await;
    stop.send_replace(true);
    run.await.unwrap();
    assert!(
        delivered.is_ok(),
        "header reads delayed the publication worker"
    );
    assert_eq!(
        beacon
            .state
            .lock()
            .publications
            .iter()
            .map(|p| p.0)
            .collect::<Vec<_>>(),
        [
            fixture_root(&first.data.message),
            fixture_root(&next.data.message)
        ]
    );
    assert_eq!(stats.lock().consensus_targets["cl"].ready, 0);
}
