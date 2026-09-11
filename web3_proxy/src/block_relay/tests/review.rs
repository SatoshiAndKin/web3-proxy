use super::*;

#[tokio::test]
async fn repeated_parent_evidence_and_redelivery_do_not_retry_syncing_forever() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    let target = rpc.target(&server.url, "a");
    rpc.state
        .lock()
        .replies
        .insert(B256::with_last_byte(2), ["SYNCING", "SYNCING"].into());
    let worker = Worker::start(target.clone(), config::Mode::Inject).await;
    let child = work(2, 1, 2, config::Mode::Inject);
    worker.tx.send(child.clone()).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].repair_gaps == 1).await;
    let parent = work(1, 0, 1, config::Mode::Inject);
    rpc.state.lock().known.insert(parent.payload.hash, 1);
    target.observe(parent.clone(), worker.stats.clone()).await;
    until(|| worker.stats.lock().execution_targets["a"].syncing == 2).await;
    for _ in 0..3 {
        target.observe(parent.clone(), worker.stats.clone()).await;
        worker.tx.send(child.clone()).unwrap();
    }
    until(|| worker.stats.lock().execution_targets["a"].suppressed_duplicate == 3).await;
    worker.finish().await;
    assert_eq!(rpc.payload_hashes(), vec![B256::with_last_byte(2); 2]);
    assert_eq!(rpc.state.lock().max_inflight, 1);
}

#[tokio::test]
async fn rpc_confirmation_does_not_revive_rejected_ancestry() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    let target = rpc.target(&server.url, "a");
    rpc.state
        .lock()
        .replies
        .insert(B256::with_last_byte(1), ["INVALID"].into());
    let worker = Worker::start(target.clone(), config::Mode::Inject).await;
    let parent = work(1, 0, 1, config::Mode::Inject);
    worker.tx.send(parent.clone()).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].invalid == 1).await;
    rpc.state.lock().known.insert(parent.payload.hash, 1);
    target.observe(parent.clone(), worker.stats.clone()).await;
    worker.tx.send(parent).unwrap();
    worker.tx.send(work(2, 1, 2, config::Mode::Inject)).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].skipped_invalid_ancestor == 1).await;
    worker.tx.send(work(3, 2, 3, config::Mode::Inject)).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].skipped_invalid_ancestor == 2).await;
    worker.finish().await;
    assert_eq!(rpc.payload_hashes(), vec![B256::with_last_byte(1)]);
}

#[tokio::test]
async fn late_rpc_parent_observation_wakes_child_without_redelivery() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    let target = rpc.target(&server.url, "a");
    {
        let mut state = rpc.state.lock();
        state
            .replies
            .insert(B256::with_last_byte(1), ["SYNCING"].into());
        state
            .replies
            .insert(B256::with_last_byte(2), ["SYNCING", "VALID"].into());
    }
    let worker = Worker::start(target.clone(), config::Mode::Inject).await;
    let parent = work(1, 0, 1, config::Mode::Inject);
    worker.tx.send(parent.clone()).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].repair_gaps == 1).await;
    worker.tx.send(work(2, 1, 2, config::Mode::Inject)).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].repair_gaps == 2).await;

    rpc.state.lock().known.insert(parent.payload.hash, 1);
    let observation = target.observe(parent, worker.stats.clone()).await;
    assert!(observation.ready.is_some());
    assert!(observation.canonical.is_some());
    let result = timeout(
        Duration::from_millis(500),
        until(|| worker.stats.lock().execution_targets["a"].valid == 1),
    )
    .await;
    worker.finish().await;
    assert!(
        result.is_ok(),
        "RPC observation did not wake the idle execution worker"
    );
    assert_eq!(
        rpc.payload_hashes(),
        vec![
            B256::with_last_byte(1),
            B256::with_last_byte(2),
            B256::with_last_byte(2)
        ]
    );
}

#[tokio::test]
async fn parent_confirmed_during_repair_retries_child_without_redelivery() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    let target = rpc.target(&server.url, "a");
    let gate = Arc::new(tokio::sync::Notify::new());
    {
        let mut state = rpc.state.lock();
        state
            .replies
            .insert(B256::with_last_byte(2), ["SYNCING", "VALID"].into());
        state.gates.insert(B256::with_last_byte(2), gate.clone());
    }
    let worker = Worker::start(target, config::Mode::Inject).await;
    worker.tx.send(work(2, 1, 2, config::Mode::Inject)).unwrap();
    until(|| rpc.payload_hashes() == vec![B256::with_last_byte(2)]).await;
    {
        let mut state = rpc.state.lock();
        state.known.insert(B256::with_last_byte(1), 1);
        state.gates.remove(&B256::with_last_byte(2));
    }
    gate.notify_one();
    let result = timeout(
        Duration::from_millis(500),
        until(|| worker.stats.lock().execution_targets["a"].valid == 1),
    )
    .await;
    worker.finish().await;
    assert!(
        result.is_ok(),
        "repair discarded new immediate-parent evidence"
    );
    assert_eq!(rpc.payload_hashes(), vec![B256::with_last_byte(2); 2]);
}

#[tokio::test]
async fn ready_source_retries_before_an_unrelated_read_finishes() {
    let network = network();
    let fast = MockBeacon::new(network.clone());
    let slow = MockBeacon::new(network.clone());
    let fast_server = Server::beacon(fast.clone()).await;
    let slow_server = Server::beacon(slow.clone()).await;
    let rpc = MockRpc::new();
    let rpc_server = Server::rpc(rpc.clone()).await;
    let mut config = relay_config(
        network.clone(),
        &[("fast", &fast_server.url), ("slow", &slow_server.url)],
        &[("a", &rpc_server.url)],
    );
    config.mode = config::Mode::Inject;
    let relay = BlockRelay::new();
    relay.apply(Some(&config)).await.unwrap();
    let (stop, _) = broadcast::channel(1);
    let run = tokio::spawn(relay.run(1, stop.subscribe()));
    until(|| fast.events.receiver_count() == 1 && slow.events.receiver_count() == 1).await;
    let block = beacon(&network);
    let root = fast.add(&block);
    fast.state.lock().hidden.insert(root);
    let gate = Arc::new(tokio::sync::Notify::new());
    slow.state.lock().block_gates.insert(root, gate.clone());
    fast.announce("block_gossip", root, block.data.message.slot);
    until(|| {
        !fast.state.lock().block_reads.is_empty() && !slow.state.lock().block_reads.is_empty()
    })
    .await;
    fast.state.lock().hidden.remove(&root);
    fast.announce("block", root, block.data.message.slot);
    let result = timeout(
        Duration::from_millis(500),
        until(|| rpc.payload_hashes().len() == 1),
    )
    .await;
    gate.notify_one();
    stop.send(()).unwrap();
    run.await.unwrap();
    assert!(
        result.is_ok(),
        "a pending slow read delayed the imported local block"
    );
    assert_eq!(rpc.payload_hashes(), vec![decode(&block, &network).hash]);
}

#[tokio::test]
async fn rpc_confirmed_parent_wakes_its_waiting_child() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    rpc.state
        .lock()
        .replies
        .insert(B256::with_last_byte(2), ["SYNCING", "VALID"].into());
    let target = rpc.target(&server.url, "a");
    let worker = Worker::start(target.clone(), config::Mode::Inject).await;
    worker.tx.send(work(2, 1, 2, config::Mode::Inject)).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].repair_gaps == 1).await;
    let parent = work(1, 0, 1, config::Mode::Inject);
    rpc.state.lock().known.insert(parent.payload.hash, 1);
    target.observe(parent.clone(), worker.stats.clone()).await;
    worker.tx.send(parent).unwrap();
    let result = timeout(
        Duration::from_millis(500),
        until(|| worker.stats.lock().execution_targets["a"].valid == 1),
    )
    .await;
    worker.finish().await;
    assert!(result.is_ok(), "RPC parent evidence did not wake its child");
    assert_eq!(rpc.payload_hashes(), vec![B256::with_last_byte(2); 2]);
}

#[tokio::test]
async fn rejected_repair_ancestor_blocks_the_entire_remaining_chain() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    {
        let mut state = rpc.state.lock();
        state.known.insert(B256::with_last_byte(1), 1);
        state
            .replies
            .insert(B256::with_last_byte(2), ["INVALID"].into());
        state
            .replies
            .insert(B256::with_last_byte(3), ["SYNCING"].into());
    }
    let worker = Worker::start(rpc.target(&server.url, "a"), config::Mode::Inject).await;
    let parent = work(2, 1, 2, config::Mode::Inject);
    worker
        .cache
        .insert(parent.payload.hash, parent.payload.clone())
        .await;
    worker.tx.send(work(3, 2, 3, config::Mode::Inject)).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].invalid == 1).await;
    worker.tx.send(work(4, 3, 4, config::Mode::Inject)).unwrap();
    until(|| worker.tx.is_empty()).await;
    let stats = worker.stats.clone();
    worker.finish().await;
    assert_eq!(
        rpc.payload_hashes(),
        vec![B256::with_last_byte(3), B256::with_last_byte(2)]
    );
    assert_eq!(
        stats.lock().execution_targets["a"].skipped_invalid_ancestor,
        1
    );
}

#[tokio::test]
async fn equivalent_engine_url_reload_retains_unknown_history_and_delivers_new_work() {
    let network = network();
    let beacon_source = MockBeacon::new(network.clone());
    let beacon_server = Server::beacon(beacon_source.clone()).await;
    let rpc = MockRpc::new();
    rpc.state.lock().wrong_valid_hash = true;
    rpc.state.lock().read_error = true;
    let rpc_server = Server::rpc(rpc.clone()).await;
    let mut config = relay_config(
        network.clone(),
        &[("local", &beacon_server.url)],
        &[("a", &rpc_server.url)],
    );
    config.mode = config::Mode::Inject;
    let relay = BlockRelay::new();
    relay.apply(Some(&config)).await.unwrap();
    let (stop, _) = broadcast::channel(1);
    let run = tokio::spawn(relay.clone().run(1, stop.subscribe()));
    until(|| beacon_source.events.receiver_count() == 1).await;
    let mut block = beacon(&network);
    let root = beacon_source.add(&block);
    beacon_source.announce("block", root, block.data.message.slot);
    until(|| relay.snapshot()["execution_targets"]["a"]["unknown"].as_u64() == Some(1)).await;
    config
        .execution_targets
        .get_mut("a")
        .unwrap()
        .engine_url
        .push('/');
    relay.apply(Some(&config)).await.unwrap();
    until(|| {
        beacon_source.state.lock().queries.len() == 2 && beacon_source.events.receiver_count() == 1
    })
    .await;
    rpc.state.lock().wrong_valid_hash = false;
    block.data.message.body.graffiti = B256::with_last_byte(55);
    let ExecutionPayload::V3(p) = &mut block.data.message.body.execution_payload.0 else {
        unreachable!()
    };
    p.payload_inner.payload_inner.extra_data = vec![1].into();
    rehash_execution(&mut block);
    let next_root = beacon_source.add(&block);
    beacon_source.announce("block", next_root, block.data.message.slot);
    until(|| relay.snapshot()["execution_targets"]["a"]["valid"].as_u64() == Some(1)).await;
    assert_eq!(
        relay.snapshot()["execution_targets"]["a"]["unknown"].as_u64(),
        Some(1)
    );
    stop.send(()).unwrap();
    run.await.unwrap();
    assert_eq!(
        rpc.payload_hashes().len(),
        2,
        "an unknown import must not block newer work after reload"
    );
}
