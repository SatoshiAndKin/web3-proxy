//! Ready-block acquisition benchmark using captured Beacon JSON and loopback HTTP.
//! Run the ignored test with WEB3_RELAY_BENCH_FIXTURES, WEB3_RELAY_BENCH_BLOCKS,
//! WEB3_RELAY_BENCH_CONCURRENCY, and WEB3_RELAY_BENCH_WORKERS set explicitly.
use super::*;

#[test]
#[ignore = "release benchmark; requires captured Beacon JSON fixtures"]
fn ready_block_acquisition() {
    let workers: usize = std::env::var("WEB3_RELAY_BENCH_WORKERS")
        .unwrap()
        .parse()
        .unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}

async fn run() {
    let directory = std::path::PathBuf::from(std::env::var("WEB3_RELAY_BENCH_FIXTURES").unwrap());
    let blocks: usize = std::env::var("WEB3_RELAY_BENCH_BLOCKS")
        .unwrap()
        .parse()
        .unwrap();
    let concurrency: usize = std::env::var("WEB3_RELAY_BENCH_CONCURRENCY")
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=8).contains(&concurrency) && blocks >= 100);
    let network = network();
    let mut paths: Vec<_> = std::fs::read_dir(directory)
        .unwrap()
        .map(|p| p.unwrap().path())
        .filter(|p| p.extension().is_some_and(|v| v == "json"))
        .collect();
    paths.sort();
    assert!(!paths.is_empty());
    let rpc = MockRpc::new();
    let mut fixtures = Vec::new();
    for path in paths {
        let mut bytes = std::fs::read(&path).unwrap();
        // Preserve captured JSON byte order. Mutating a Sonic object materializes
        // a randomized map, so reserializing it would change signed-block bytes.
        let (offset, length) = {
            let slot = sonic_rs::get(bytes.as_slice(), ["data", "message", "slot"]).unwrap();
            let raw = slot.as_raw_str();
            (raw.as_ptr() as usize - bytes.as_ptr() as usize, raw.len())
        };
        bytes.splice(offset..offset + length, b"\"64\"".iter().copied());
        let payload = payload::RelayPayload::decode(&bytes, B256::ZERO).unwrap();
        assert_eq!(
            payload.signed_block.as_ref(),
            sonic_rs::get(bytes.as_slice(), ["data"])
                .unwrap()
                .as_raw_str()
                .as_bytes()
        );
        rpc.state.lock().known.insert(payload.hash, payload.number);
        println!(
            "fixture {} {} {} {}",
            path.file_name().unwrap().to_string_lossy(),
            bytes.len(),
            alloy::primitives::keccak256(&payload.body),
            alloy::primitives::keccak256(&payload.signed_block)
        );
        fixtures.push(bytes);
    }
    let sources: Vec<_> = (0..3).map(|_| MockBeacon::new(network.clone())).collect();
    let mut servers = Vec::new();
    for source in &sources {
        servers.push(Server::beacon(source.clone()).await);
    }
    let rpc_server = Server::rpc(rpc.clone()).await;
    let config = relay_config(
        network,
        &[
            ("a", &servers[0].url),
            ("b", &servers[1].url),
            ("c", &servers[2].url),
        ],
        &[("target", &rpc_server.url)],
    );
    let relay = BlockRelay::new();
    relay.apply(Some(&config)).await.unwrap();
    let (stop, _) = broadcast::channel(1);
    let run = tokio::spawn(relay.clone().run(1, stop.subscribe()));
    until(|| sources.iter().all(|s| s.events.receiver_count() == 1)).await;
    let warmup = 16;
    let total = warmup + blocks;
    for start in (0..total).step_by(concurrency) {
        let end = (start + concurrency).min(total);
        for index in start..end {
            let root = B256::from(alloy::primitives::U256::from(index + 1));
            for source in &sources {
                source
                    .state
                    .lock()
                    .blocks
                    .insert(root, fixtures[index % fixtures.len()].clone());
            }
            sources[0].announce("block_gossip", root, 64);
        }
        timeout(Duration::from_secs(12), async {
            while rpc
                .state
                .lock()
                .methods
                .iter()
                .filter(|method| method.as_str() == "eth_getBlockByHash")
                .count()
                < end
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        for index in start..end {
            let root = B256::from(alloy::primitives::U256::from(index + 1));
            for source in &sources {
                source.state.lock().blocks.remove(&root);
            }
        }
    }
    stop.send(()).unwrap();
    run.await.unwrap();
    let status = relay.snapshot();
    assert_eq!(status["acquired"], json!(total));
    for key in [
        "acquisition_failed",
        "acquisition_dropped",
        "recording_dropped",
        "observation_dropped",
    ] {
        assert_eq!(status[key], json!(0), "{key}");
    }
    assert!(
        rpc.payload_hashes().is_empty(),
        "observe benchmark must not submit payloads"
    );
    let mut latencies = Vec::new();
    for file in std::fs::read_dir(config.state_dir.join("observations")).unwrap() {
        for line in std::fs::read_to_string(file.unwrap().path())
            .unwrap()
            .lines()
        {
            let row: sonic_rs::Value = sonic_rs::from_str(line).unwrap();
            if row["record"] == json!("acquired")
                && row["root"].as_str().unwrap().parse::<B256>().unwrap()
                    > B256::from(alloy::primitives::U256::from(warmup))
            {
                latencies.push(row["acquired_us"].as_u64().unwrap());
            }
        }
    }
    assert_eq!(latencies.len(), blocks);
    latencies.sort_unstable();
    println!(
        "acquisition_benchmark {}",
        json!({"blocks": blocks, "concurrency": concurrency, "p50_us": latencies[blocks.div_ceil(2)-1], "p95_us": latencies[(blocks*95).div_ceil(100)-1], "p99_us": latencies[(blocks*99).div_ceil(100)-1], "samples_us": latencies, "status": status})
    );
}
