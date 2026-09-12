use super::*;

#[tokio::test]
async fn invalid_ancestry_and_unknown_results_survive_worker_restart() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    let target = rpc.target(&server.url, "a");
    rpc.state
        .lock()
        .replies
        .insert(B256::with_last_byte(1), ["INVALID"].into());
    let first = Worker::start(target.clone(), config::Mode::Inject).await;
    first.tx.send(work(1, 0, 1, config::Mode::Inject)).unwrap();
    until(|| first.stats.lock().execution_targets["a"].invalid == 1).await;
    rpc.state.lock().wrong_valid_hash = true;
    first.tx.send(work(3, 0, 3, config::Mode::Inject)).unwrap();
    until(|| first.stats.lock().execution_targets["a"].unknown == 1).await;
    first.finish().await;
    rpc.state.lock().wrong_valid_hash = false;
    let second = Worker::start(target, config::Mode::Inject).await;
    second.tx.send(work(2, 1, 2, config::Mode::Inject)).unwrap();
    until(|| second.stats.lock().execution_targets["a"].skipped_invalid_ancestor == 1).await;
    second.tx.send(work(4, 3, 4, config::Mode::Inject)).unwrap();
    until(|| second.stats.lock().execution_targets["a"].valid == 1).await;
    second.tx.send(work(3, 0, 3, config::Mode::Inject)).unwrap();
    until(|| second.stats.lock().execution_targets["a"].suppressed_duplicate == 1).await;
    second.finish().await;
    assert_eq!(rpc.payload_hashes(), [1, 3, 4].map(B256::with_last_byte));
}

#[tokio::test]
async fn broken_storage_silent_source_bad_jwt_and_invalid_endpoints_leave_healthy_delivery_active()
{
    let network = network();
    let local = MockBeacon::new(network.clone());
    let silent = MockBeacon::new(network.clone());
    let local_server = Server::beacon(local.clone()).await;
    let silent_server = Server::beacon(silent.clone()).await;
    let rpc = MockRpc::new();
    let rpc_server = Server::rpc(rpc.clone()).await;
    let bad_rpc = MockRpc::new();
    let bad_server = Server::rpc(bad_rpc.clone()).await;
    let cl = MockBeacon::new(network.clone());
    let cl_server = Server::beacon(cl.clone()).await;
    let mut config = relay_config(
        network.clone(),
        &[("local", &local_server.url), ("silent", &silent_server.url)],
        &[("good", &rpc_server.url), ("bad-jwt", &bad_server.url)],
    );
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("state");
    std::fs::write(&state, "preserved historical data").unwrap();
    config.state_dir = state.clone();
    config.mode = config::Mode::Inject;
    config
        .execution_targets
        .get_mut("bad-jwt")
        .unwrap()
        .jwt_secret_path = directory.path().join("missing-jwt");
    let mut invalid = config.execution_targets["good"].clone();
    invalid.engine_url = "bad://credential-must-not-leak".into();
    config
        .execution_targets
        .insert("invalid-url".into(), invalid);
    config.consensus_targets.insert(
        "good-cl".into(),
        config::ConsensusTarget {
            beacon_url: cl_server.url.clone(),
            headers: Default::default(),
        },
    );
    config.consensus_targets.insert(
        "invalid-cl".into(),
        config::ConsensusTarget {
            beacon_url: "bad://credential-must-not-leak".into(),
            headers: Default::default(),
        },
    );
    let relay = BlockRelay::new();
    relay.apply(Some(&config)).await.unwrap();
    let (stop, _) = broadcast::channel(1);
    let run = tokio::spawn(relay.clone().run(1, stop.subscribe()));
    until(|| local.events.receiver_count() == 1 && silent.events.receiver_count() == 1).await;
    let block = beacon(&network);
    let root = local.add(&block);
    local.announce("block", root, block.data.message.slot);
    until(|| {
        relay.snapshot()["execution_targets"]["good"]["valid"].as_u64() == Some(1)
            && cl.state.lock().publications.len() == 1
    })
    .await;
    until(|| relay.snapshot()["recording_dropped"].as_u64().unwrap() > 0).await;
    assert!(relay.live());
    assert!(relay.ready());
    let status = relay.snapshot();
    assert_eq!(status["operation"].as_str(), Some("degraded"));
    assert_eq!(status["sources"]["silent"]["events"].as_u64(), Some(0));
    assert_eq!(
        status["execution_targets"]["invalid-url"]["health"]["connected"].as_bool(),
        Some(false)
    );
    assert_eq!(
        status["execution_targets"]["bad-jwt"]["health"]["connected"].as_bool(),
        Some(false)
    );
    assert!(!status.to_string().contains("credential-must-not-leak"));
    stop.send(()).unwrap();
    run.await.unwrap();
    assert_eq!(
        std::fs::read_to_string(state).unwrap(),
        "preserved historical data"
    );
    assert_eq!(rpc.payload_hashes(), [decode(&block, &network).hash]);
    assert!(bad_rpc.payload_hashes().is_empty());
    assert!(!relay.live());
}

#[tokio::test]
async fn failed_rpc_probe_does_not_block_engine_delivery() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    let mut target = rpc.target(&server.url, "a");
    let unavailable = Server::start(axum::Router::new()).await;
    Arc::get_mut(&mut target).unwrap().rpc = transport::Rpc::new(&unavailable.url, None).unwrap();
    let worker = Worker::start(target.clone(), config::Mode::Inject).await;
    let block = work(1, 0, 1, config::Mode::Inject);
    worker.tx.send(block.clone()).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].valid == 1).await;
    let mut probe_work = work(2, 1, 2, config::Mode::Inject);
    Arc::get_mut(&mut probe_work).unwrap().deadline = Instant::now() + Duration::from_millis(30);
    assert_eq!(
        target.observe(probe_work, worker.stats.clone()).await.ready,
        None
    );
    assert!(
        worker.stats.lock().execution_targets["a"]
            .observation
            .errors
            > 0
    );
    assert!(worker.stats.lock().execution_targets["a"].health.connected);
    worker.finish().await;
    assert_eq!(rpc.payload_hashes(), [block.payload.hash]);
}

#[tokio::test(start_paused = true)]
async fn supervisor_restarts_only_the_failed_worker_after_exit_or_panic() {
    for panic in [false, true] {
        let stats = Arc::new(parking_lot::Mutex::new(stats::Stats::default()));
        let (stop, stop_rx) = watch::channel(false);
        let (calls, mut receive) = tokio::sync::mpsc::unbounded_channel();
        let mut attempt = 0;
        let task = tokio::spawn(service::supervise(
            stats::WorkerLayer::Execution,
            "failed".into(),
            stats.clone(),
            stop_rx.clone(),
            move || {
                attempt += 1;
                let attempt = attempt;
                let calls = calls.clone();
                let mut stop = stop_rx.clone();
                async move {
                    calls.send(attempt).unwrap();
                    if attempt == 1 {
                        assert!(!panic, "test worker panic");
                        return;
                    }
                    let _ = stop.changed().await;
                }
            },
        ));
        assert_eq!(receive.recv().await, Some(1));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(receive.recv().await, Some(2));
        assert_eq!(stats.lock().execution_targets["failed"].health.restarts, 1);
        stop.send_replace(true);
        task.await.unwrap();
    }
}

#[tokio::test]
async fn historical_journal_files_do_not_gate_startup_or_change_on_delivery() {
    let network = network();
    let source = MockBeacon::new(network.clone());
    let source_server = Server::beacon(source.clone()).await;
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    let mut config = relay_config(
        network.clone(),
        &[("local", &source_server.url)],
        &[("a", &server.url)],
    );
    let directory = tempfile::tempdir().unwrap();
    config.state_dir = directory.path().into();
    config.mode = config::Mode::Inject;
    let journal = directory.path().join("old-engine.json");
    std::fs::write(&journal, "{ corrupt historical journal").unwrap();
    let relay = BlockRelay::new();
    relay.apply(Some(&config)).await.unwrap();
    let (stop, _) = broadcast::channel(1);
    let run = tokio::spawn(relay.clone().run(1, stop.subscribe()));
    until(|| source.events.receiver_count() == 1).await;
    let block = beacon(&network);
    let root = source.add(&block);
    source.announce("block", root, block.data.message.slot);
    until(|| relay.snapshot()["execution_targets"]["a"]["valid"].as_u64() == Some(1)).await;
    stop.send(()).unwrap();
    run.await.unwrap();
    assert_eq!(
        std::fs::read_to_string(journal).unwrap(),
        "{ corrupt historical journal"
    );
    assert_eq!(rpc.payload_hashes(), [decode(&block, &network).hash]);
}

#[tokio::test]
async fn bounded_delivery_queue_drops_old_work_and_sends_the_retained_blocks() {
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
    for n in 2..=201u8 {
        worker
            .tx
            .send(work(n, 0, u64::from(n), config::Mode::Inject))
            .unwrap();
    }
    gate.notify_one();
    until(|| worker.stats.lock().execution_targets["a"].valid == 129).await;
    assert_eq!(worker.stats.lock().execution_targets["a"].queue_dropped, 72);
    worker.finish().await;
    let hashes = rpc.payload_hashes();
    assert_eq!(hashes[0], B256::with_last_byte(1));
    assert_eq!(
        hashes[1..],
        (74..=201u8)
            .rev()
            .map(B256::with_last_byte)
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn recording_recovers_after_storage_repair_without_clearing_error_history() {
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("state");
    std::fs::write(&state, "preserve").unwrap();
    let stats = Arc::new(parking_lot::Mutex::new(stats::Stats::default()));
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    stats.lock().recorder = Some(sender);
    let run = tokio::spawn(recording::Recording::supervise(
        state.clone(),
        receiver,
        stats.clone(),
    ));
    until(|| stats.lock().recording.errors == 1).await;
    stats.lock().record(recording::Record::AcquisitionFailed {
        started_mode_epoch: 0,
        root: B256::ZERO,
        slot: 1,
        consensus: false,
    });
    until(|| stats.lock().recording_dropped == 1).await;
    std::fs::rename(&state, directory.path().join("history")).unwrap();
    timeout(Duration::from_secs(8), async {
        while !stats.lock().recording.connected {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let expected = recording::Record::AcquisitionFailed {
        started_mode_epoch: 0,
        root: B256::with_last_byte(2),
        slot: 2,
        consensus: false,
    };
    let expected = sonic_rs::to_value(&expected).unwrap();
    stats.lock().record(recording::Record::AcquisitionFailed {
        started_mode_epoch: 0,
        root: B256::with_last_byte(2),
        slot: 2,
        consensus: false,
    });
    stats.lock().recorder.take();
    run.await.unwrap();
    assert_eq!(stats.lock().recording.errors, 1);
    assert_eq!(stats.lock().recording_dropped, 1);
    let files: Vec<_> = std::fs::read_dir(state.join("observations"))
        .unwrap()
        .collect();
    assert_eq!(files.len(), 1);
    let line = std::fs::read_to_string(files[0].as_ref().unwrap().path()).unwrap();
    assert_eq!(line.lines().count(), 1);
    let mut recorded: sonic_rs::Value = sonic_rs::from_str(line.trim()).unwrap();
    assert_eq!(recorded["context"]["schema"], json!(2));
    assert_eq!(
        recorded["context"]["sequence"],
        json!(2),
        "lost first record stays visible"
    );
    use sonic_rs::JsonValueMutTrait;
    recorded.as_object_mut().unwrap().remove(&"context");
    assert_eq!(recorded, expected);
    assert_eq!(
        std::fs::read_to_string(directory.path().join("history")).unwrap(),
        "preserve"
    );
}
