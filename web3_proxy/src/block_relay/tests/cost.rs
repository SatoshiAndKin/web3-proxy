use super::*;

#[tokio::test]
async fn local_block_before_fallback_delay_avoids_paid_http_request() {
    let network = network();
    let block = beacon(&network);
    let local = MockBeacon::new(network.clone());
    let paid = MockBeacon::new(network.clone());
    let local_server = Server::beacon(local.clone()).await;
    let paid_server = Server::beacon(paid.clone()).await;
    let engine_server = Server::rpc(MockRpc::new()).await;
    let root = local.add(&block);
    paid.add(&block);
    let local_gate = Arc::new(tokio::sync::Notify::new());
    local
        .state
        .lock()
        .block_gates
        .insert(root, local_gate.clone());
    // Hold the paid response so an immediate paid dispatch cannot win the race.
    paid.state
        .lock()
        .block_gates
        .insert(root, Arc::new(tokio::sync::Notify::new()));
    let mut config = relay_config(
        network,
        &[("local", &local_server.url), ("alchemy", &paid_server.url)],
        &[("el", &engine_server.url)],
    );
    config.sources.get_mut("alchemy").unwrap().cost_class = config::CostClass::Metered;
    config.rpc.metered_fallback_delay_ms = 500;
    let relay = BlockRelay::new();
    relay.apply(Some(&config)).await.unwrap();
    let (stop, _) = broadcast::channel(1);
    let run = tokio::spawn(relay.clone().run(1, stop.subscribe()));
    until(|| local.events.receiver_count() == 1 && paid.events.receiver_count() == 1).await;
    local.announce("block_gossip", root, block.data.message.slot);
    until(|| local.state.lock().block_reads == [root]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    local_gate.notify_one();
    until(|| relay.snapshot()["acquired"].as_u64() == Some(1)).await;
    stop.send(()).unwrap();
    run.await.unwrap();
    assert_eq!(local.state.lock().block_reads, vec![root]);
    assert_eq!(paid.state.lock().block_reads, Vec::<B256>::new());
}

#[tokio::test]
async fn unavailable_local_block_uses_paid_fallback_after_configured_delay() {
    let network = network();
    let block = beacon(&network);
    let local = MockBeacon::new(network.clone());
    let paid = MockBeacon::new(network.clone());
    let local_server = Server::beacon(local.clone()).await;
    let paid_server = Server::beacon(paid.clone()).await;
    let engine_server = Server::rpc(MockRpc::new()).await;
    let root = paid.add(&block);
    let mut config = relay_config(
        network,
        &[("local", &local_server.url), ("alchemy", &paid_server.url)],
        &[("el", &engine_server.url)],
    );
    config.sources.get_mut("alchemy").unwrap().cost_class = config::CostClass::Metered;
    config.rpc.metered_fallback_delay_ms = 500;
    let relay = BlockRelay::new();
    relay.apply(Some(&config)).await.unwrap();
    let (stop, _) = broadcast::channel(1);
    let run = tokio::spawn(relay.clone().run(1, stop.subscribe()));
    until(|| local.events.receiver_count() == 1 && paid.events.receiver_count() == 1).await;
    let started = Instant::now();
    local.announce("block_gossip", root, block.data.message.slot);
    until(|| !local.state.lock().block_reads.is_empty()).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let early_reads = paid.state.lock().block_reads.clone();
    until(|| relay.snapshot()["acquired"].as_u64() == Some(1)).await;
    let elapsed = started.elapsed();
    stop.send(()).unwrap();
    run.await.unwrap();
    assert_eq!(early_reads, Vec::<B256>::new());
    assert!(
        elapsed >= Duration::from_millis(500),
        "fallback ran early: {elapsed:?}"
    );
    assert_eq!(paid.state.lock().block_reads, vec![root]);
}
