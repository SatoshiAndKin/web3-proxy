//! Release benchmark of the proxy's request decoder and block-cache JSON path.
//! Example: cargo run --release -p web3_proxy --example json_codec_bench --
//! --mode full --items 512 --data-bytes 256 --threads 8 --seconds 2
use alloy::primitives::B256;
use argh::FromArgs;
use sonic_rs::{json, JsonContainerTrait, JsonValueTrait, OwnedLazyValue};
use std::{
    hint::black_box,
    sync::{Arc, Barrier, OnceLock},
    time::{Duration, Instant},
};
use web3_proxy::{jsonrpc::JsonRpcRequestEnum, rpcs::blockchain::CachedBlockResponse};

#[derive(FromArgs)]
/// Measure production JSON codecs with fresh parsed input and checked output.
struct Args {
    /// operation: full, hashes or decode
    #[argh(option)]
    mode: String,
    /// transactions per block or calls per request batch
    #[argh(option, default = "512")]
    items: usize,
    /// transaction data or call data bytes
    #[argh(option, default = "256")]
    data_bytes: usize,
    /// concurrent native threads
    #[argh(option, default = "1")]
    threads: usize,
    /// measured seconds, after startup and correctness checks
    #[argh(option, default = "2")]
    seconds: u64,
}

fn operation(mode: &str, bytes: &[u8], hash: B256) -> usize {
    match mode {
        "decode" => {
            let request: JsonRpcRequestEnum = sonic_rs::from_slice(bytes).unwrap();
            black_box(request);
            bytes.len()
        }
        "full" => {
            let raw: OwnedLazyValue = sonic_rs::from_slice(bytes).unwrap();
            let (full, hashes) = CachedBlockResponse::from_full(Arc::new(raw), hash).unwrap();
            black_box((full.num_bytes(), hashes.num_bytes()));
            let wire = sonic_rs::to_vec(&*hashes.result()).unwrap();
            black_box(&wire);
            wire.len()
        }
        "hashes" => {
            let raw: OwnedLazyValue = sonic_rs::from_slice(bytes).unwrap();
            let block = CachedBlockResponse::from_hashes(Arc::new(raw)).unwrap();
            black_box(block.num_bytes());
            let wire = sonic_rs::to_vec(&*block.result()).unwrap();
            black_box(&wire);
            wire.len()
        }
        _ => panic!("mode must be full, hashes or decode"),
    }
}

fn main() {
    let args: Args = argh::from_env();
    assert!(args.threads > 0 && args.seconds > 0);
    let hash = B256::with_last_byte(0x42);
    let data = format!("0x{}", "ab".repeat(args.data_bytes));
    let hashes: Vec<_> = (0..args.items)
        .map(|index| B256::from(alloy::primitives::U256::from(index + 1)))
        .collect();
    let value =
        if args.mode == "decode" {
            assert!(args.items > 0);
            json!((0..args.items).map(|index| json!({
            "jsonrpc":"2.0", "id":index, "method":"eth_call",
            "params":[{"to":"0x0000000000000000000000000000000000000000", "data":data},"latest"]
        })).collect::<Vec<_>>())
        } else {
            let transactions =
                if args.mode == "full" {
                    json!(hashes.iter().map(|hash| json!({
                "hash":hash,"input":data,"nonce":"0x1","providerTx":{"nested":[null,true,7]}
            })).collect::<Vec<_>>())
                } else {
                    json!(hashes)
                };
            json!({"hash":hash,"number":"0x2a","uncles":[],"transactions":transactions,
            "withdrawals":[],"providerBlock":{"label":"unknown fields stay present","data":data}})
        };
    let bytes = Arc::new(sonic_rs::to_vec(&value).unwrap());
    // Validate all output before timing. Include weighing and hash-form wire
    // serialization during timing so lazy serialization cannot hide its cost.
    if args.mode == "decode" {
        let JsonRpcRequestEnum::Batch(requests) = sonic_rs::from_slice(&bytes).unwrap() else {
            panic!("expected a request batch");
        };
        assert_eq!(requests.len(), args.items);
    } else {
        let raw = Arc::new(sonic_rs::from_slice(&bytes).unwrap());
        let block = if args.mode == "full" {
            let (full, hashes) = CachedBlockResponse::from_full(raw, hash).unwrap();
            let full_wire: sonic_rs::Value =
                sonic_rs::from_slice(&sonic_rs::to_vec(&*full.result()).unwrap()).unwrap();
            assert_eq!(
                sonic_rs::to_string(&full_wire).unwrap(),
                sonic_rs::to_string(&value).unwrap()
            );
            hashes
        } else {
            CachedBlockResponse::from_hashes(raw).unwrap()
        };
        let parsed: sonic_rs::Value =
            sonic_rs::from_slice(&sonic_rs::to_vec(&*block.result()).unwrap()).unwrap();
        assert_eq!(parsed["transactions"].as_array().unwrap().len(), args.items);
        for (actual, expected) in parsed["transactions"]
            .as_array()
            .unwrap()
            .iter()
            .zip(hashes.iter())
        {
            assert_eq!(actual.as_str().unwrap(), expected.to_string());
        }
        for field in ["label", "data"] {
            assert_eq!(
                parsed["providerBlock"][field].as_str(),
                value["providerBlock"][field].as_str()
            );
        }
    }
    for _ in 0..16 {
        black_box(operation(&args.mode, &bytes, hash));
    }
    let start = Arc::new(OnceLock::<Instant>::new());
    let barrier = Arc::new(Barrier::new(args.threads + 1));
    let mut workers = Vec::new();
    for _ in 0..args.threads {
        let start = start.clone();
        let barrier = barrier.clone();
        let bytes = bytes.clone();
        let mode = args.mode.clone();
        let duration = Duration::from_secs(args.seconds);
        workers.push(std::thread::spawn(move || {
            let mut samples = Vec::new();
            barrier.wait();
            let began = *start.wait();
            while began.elapsed() < duration {
                let measured = Instant::now();
                black_box(operation(&mode, &bytes, hash));
                samples.push(measured.elapsed().as_nanos() as u64);
            }
            (samples, Instant::now())
        }));
    }
    barrier.wait();
    let began = Instant::now();
    start.set(began).unwrap();
    let mut ended = began;
    let mut samples = Vec::new();
    for worker in workers {
        let (times, finished) = worker.join().unwrap();
        samples.extend(times);
        ended = ended.max(finished);
    }
    let seconds = ended.duration_since(began).as_secs_f64();
    samples.sort_unstable();
    let quantile = |q: f64| samples[((samples.len() as f64 * q).ceil() as usize).saturating_sub(1)];
    println!(
        "{}",
        json!({"mode":args.mode,"items":args.items,"data_bytes":args.data_bytes,
        "threads":args.threads,"input_bytes":bytes.len(),"operations":samples.len(),"seconds":seconds,
        "ops_per_second":samples.len() as f64/seconds,"p50_ns":quantile(0.5),
        "p95_ns":quantile(0.95),"p99_ns":quantile(0.99),"errors":0})
    );
}
