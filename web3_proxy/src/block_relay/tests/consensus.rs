use super::*;
use crate::block_relay::{
    consensus::{ConsensusTarget, ConsensusWork},
    payload::ConsensusPayload,
};
use alloy::{
    consensus::{SignableTransaction, TxEip4844, TxEnvelope},
    eips::{
        eip2718::Encodable2718,
        eip4844::{self, env_settings::EnvKzgSettings, AsCkzg, Blob, Bytes48},
    },
    primitives::{Signature, U256},
};

fn blob_block(
    network: &config::Network,
    fork: &str,
) -> (payload::BeaconResponse, Vec<alloy::primitives::Bytes>) {
    let bytes: alloy::primitives::Bytes = vec![0; eip4844::BYTES_PER_BLOB].into();
    let blob: &Blob = bytes.as_ref().try_into().unwrap();
    let commitment = EnvKzgSettings::Default
        .get()
        .blob_to_kzg_commitment(blob.as_ckzg())
        .unwrap()
        .to_bytes();
    let commitment = Bytes48::from_ckzg(commitment);
    let mut block = beacon(network);
    block.version = fork.into();
    block.data.signature = alloy::primitives::FixedBytes::repeat_byte(7);
    block.data.message.body.blob_kzg_commitments = vec![commitment];
    let tx = TxEip4844 {
        chain_id: 1,
        gas_limit: 21_000,
        max_fee_per_gas: 2,
        max_fee_per_blob_gas: 1,
        blob_versioned_hashes: vec![eip4844::kzg_to_versioned_hash(commitment.as_slice())],
        ..Default::default()
    };
    let tx: TxEnvelope = tx
        .into_signed(Signature::new(U256::from(1), U256::from(2), false))
        .into();
    let ExecutionPayload::V3(p) = &mut block.data.message.body.execution_payload.0 else {
        unreachable!()
    };
    p.payload_inner.payload_inner.transactions = vec![tx.encoded_2718().into()];
    p.blob_gas_used = eip4844::DATA_GAS_PER_BLOB;
    rehash_execution(&mut block);
    (block, vec![bytes])
}

#[test]
fn consensus_contents_keep_the_exact_signature_and_validate_electra_and_fulu_proofs() {
    for fork in ["electra", "fulu"] {
        let mut network = network();
        network.forks[0].name = fork.into();
        let (response, blobs) = blob_block(&network, fork);
        let block = Arc::new(decode(&response, &network));
        let packet = ConsensusPayload::from_blobs(block.clone(), blobs.clone()).unwrap();
        let contents: sonic_rs::Value = sonic_rs::from_slice(&packet.body).unwrap();
        assert_eq!(
            contents["signed_block"],
            sonic_rs::to_value(&response.data).unwrap()
        );
        assert_eq!(contents["blobs"], sonic_rs::to_value(&blobs).unwrap());
        let proofs: Vec<Bytes48> = sonic_rs::from_value(&contents["kzg_proofs"]).unwrap();
        let blob: &Blob = blobs[0].as_ref().try_into().unwrap();
        if fork == "electra" {
            assert_eq!(proofs.len(), 1);
            assert!(EnvKzgSettings::Default
                .get()
                .verify_blob_kzg_proof(
                    blob.as_ckzg(),
                    block.blob_commitments[0].as_ckzg(),
                    proofs[0].as_ckzg()
                )
                .unwrap());
        } else {
            assert_eq!(proofs.len(), alloy::eips::eip7594::CELLS_PER_EXT_BLOB);
            let settings = EnvKzgSettings::Default.get();
            let cells = settings.compute_cells(blob.as_ckzg()).unwrap();
            let commitments = vec![block.blob_commitments[0]; proofs.len()];
            let indices = (0..proofs.len() as u64).collect::<Vec<_>>();
            assert!(settings
                .verify_cell_kzg_proof_batch(
                    Bytes48::slice_as_ckzg(&commitments),
                    &indices,
                    cells.as_ref(),
                    Bytes48::slice_as_ckzg(&proofs)
                )
                .unwrap());
        }
        assert_eq!(
            packet.block.beacon_root,
            tree_hash::block_root(&response.data.message).unwrap()
        );
    }
}

#[test]
fn consensus_rejects_missing_wrong_and_invalid_field_element_blobs() {
    let network = network();
    let (response, mut blobs) = blob_block(&network, "electra");
    let block = Arc::new(decode(&response, &network));
    assert_eq!(
        ConsensusPayload::from_blobs(block.clone(), Vec::new())
            .unwrap_err()
            .to_string(),
        "blob count mismatch"
    );
    let mut wrong = blobs[0].to_vec();
    wrong[31] = 1;
    blobs[0] = wrong.clone().into();
    assert_eq!(
        ConsensusPayload::from_blobs(block.clone(), blobs.clone())
            .unwrap_err()
            .to_string(),
        "blob commitment mismatch"
    );
    wrong[0] = 255;
    blobs[0] = wrong.into();
    assert!(
        ConsensusPayload::from_blobs(block, blobs).is_err(),
        "out-of-field blob must fail KZG validation"
    );
}

#[test]
fn empty_blob_blocks_publish_empty_proof_and_blob_arrays() {
    let network = network();
    let response = beacon(&network);
    let packet =
        ConsensusPayload::from_blobs(Arc::new(decode(&response, &network)), Vec::new()).unwrap();
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
    let network = network();
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
    until(|| engine.payload_hashes() == [hash] && !source.state.lock().blob_reads.is_empty()).await;
    assert_eq!(target.state.lock().publications.len(), 0);
    config.mode = config::Mode::Observe;
    relay.apply(Some(&config)).await.unwrap();
    source
        .state
        .lock()
        .blobs
        .insert(root, sonic_rs::to_vec(&json!({"data": blobs})).unwrap());
    source.announce("block", root, block.data.message.slot);
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
    let mut w = work(1, 0, 1, config::Mode::Observe, &[]);
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
    let mut w = work(1, 0, 1, config::Mode::Inject, &[]);
    let w_mut = Arc::get_mut(&mut w).unwrap();
    w_mut.payload = Arc::new(decode(&response, &network));
    w_mut.deadline = Instant::now() + Duration::from_millis(200);
    let payload = Arc::new(ConsensusPayload::from_blobs(w.payload.clone(), Vec::new()).unwrap());
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
            ttl: Duration::from_secs(768),
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
    child.data.message.parent_root = tree_hash::block_root(&parent.data.message).unwrap();
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
        payload: block.clone(),
        first_seen: now,
        acquired: now,
        deadline: now + Duration::from_secs(12),
        source: "test".into(),
        announcement_source: "test".into(),
        event: "block",
        mode: config::Mode::Inject,
        known: BTreeMap::new(),
    });
    Arc::new(ConsensusWork {
        work,
        payload: Arc::new(ConsensusPayload::from_blobs(block, Vec::new()).unwrap()),
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
            ttl: Duration::from_secs(768),
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
            ttl: Duration::from_secs(768),
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
