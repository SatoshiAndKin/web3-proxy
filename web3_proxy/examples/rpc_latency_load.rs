//! Fixed-arrival-rate HTTP RPC load generator. Reports scheduling delay as part of latency.
//! Use with the ignored `rpcs::batch_tests::latency_bench::serve` test in release mode.
use argh::FromArgs;
use futures::{stream, StreamExt};
use hdrhistogram::Histogram;
use sonic_rs::{json, JsonValueTrait};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

#[derive(FromArgs)]
/// Measure complete, validated RPC responses over reused HTTP connections.
struct Args {
    /// loopback benchmark URL
    #[argh(option)]
    url: String,
    /// offered requests per second; zero measures saturation
    #[argh(option, default = "10000")]
    rate: u64,
    /// total requests including warmup when run separately
    #[argh(option, default = "100000")]
    requests: u64,
    /// maximum client requests in flight
    #[argh(option, default = "256")]
    concurrency: usize,
    /// length of the returned hex string, including its prefix
    #[argh(option, default = "66")]
    bytes: usize,
    /// optional historical block hash
    #[argh(option)]
    block_hash: Option<String>,
    /// requests sharing each newly generated historical hash
    #[argh(option)]
    hash_group_size: Option<std::num::NonZeroU64>,
}

fn main() {
    let args: Args = argh::from_env();
    assert!(args.concurrency > 0 && args.bytes >= 2 && args.requests > 0);
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap()
        .block_on(run(args));
}

async fn run(args: Args) {
    let client = reqwest::Client::builder()
        .tcp_nodelay(true)
        .timeout(Duration::from_secs(15))
        .pool_max_idle_per_host(args.concurrency)
        .build()
        .unwrap();
    let expected = Arc::new(format!("0x{}", "a".repeat(args.bytes - 2)));
    let block = args
        .block_hash
        .map(|hash| json!({"blockHash":hash}))
        .unwrap_or(json!("latest"));
    let body = bytes::Bytes::from(sonic_rs::to_vec(&json!({
        "jsonrpc":"2.0", "id":1, "method":"eth_call", "params":[{"to":"0x0000000000000000000000000000000000000001", "data":"0x12345678"},block]
    })).unwrap());
    let url = args.url;
    let rate = args.rate;
    let hash_group_size = args.hash_group_size;
    let started = Instant::now() + Duration::from_secs(1);
    let mut replies = stream::iter(0..args.requests)
        .map(|index| {
            let client = client.clone();
            let body = if let Some(group_size) = hash_group_size {
                bytes::Bytes::from(sonic_rs::to_vec(&json!({
                    "jsonrpc":"2.0", "id":1, "method":"eth_call",
                    "params":[{"to":"0x0000000000000000000000000000000000000001", "data":"0x12345678"},
                        {"blockHash":format!("0x{:064x}", index / group_size + 1)}]
                })).unwrap())
            } else {
                body.clone()
            };
            let expected = expected.clone();
            let url = &url;
            async move {
                let scheduled = if rate == 0 {
                    Instant::now()
                } else {
                    started + Duration::from_secs_f64(index as f64 / rate as f64)
                };
                if rate > 0 {
                    tokio::time::sleep_until(scheduled).await;
                }
                let sent = Instant::now();
                let result = async {
                    let response = client
                        .post(url)
                        .header("content-type", "application/json")
                        .body(body)
                        .send()
                        .await?;
                    let status = response.status();
                    let bytes = response.bytes().await?;
                    let complete = Instant::now();
                    let valid = status.is_success()
                        && sonic_rs::get(&bytes, ["id"]).is_ok_and(|id| id.as_u64() == Some(1))
                        && sonic_rs::get(&bytes, ["result"])
                            .is_ok_and(|value| value.as_str() == Some(expected.as_str()));
                    Ok::<_, reqwest::Error>((complete, valid))
                }
                .await;
                match result {
                    Ok((complete, valid)) => (
                        complete.duration_since(scheduled),
                        complete.duration_since(sent),
                        valid,
                    ),
                    Err(_) => (scheduled.elapsed(), sent.elapsed(), false),
                }
            }
        })
        .buffer_unordered(args.concurrency);
    let mut end_to_end = Histogram::<u64>::new(3).unwrap();
    let mut service = Histogram::<u64>::new(3).unwrap();
    let mut errors = 0u64;
    let begin = Instant::now();
    while let Some((elapsed, active, valid)) = replies.next().await {
        end_to_end.record(elapsed.as_nanos().max(1) as u64).unwrap();
        service.record(active.as_nanos().max(1) as u64).unwrap();
        errors += u64::from(!valid);
    }
    let elapsed = if rate == 0 {
        begin.elapsed()
    } else {
        started.elapsed()
    };
    println!(
        "{}",
        json!({
            "requests":args.requests,"rate":rate,"concurrency":args.concurrency,"result_bytes":args.bytes,"errors":errors,
            "seconds":elapsed.as_secs_f64(),"successful_rps":(args.requests-errors) as f64/elapsed.as_secs_f64(),
            "mean_us":end_to_end.mean()/1000.0,"p50_us":end_to_end.value_at_quantile(0.5) as f64/1000.0,
            "p95_us":end_to_end.value_at_quantile(0.95) as f64/1000.0,"p99_us":end_to_end.value_at_quantile(0.99) as f64/1000.0,
            "service_mean_us":service.mean()/1000.0,"service_p99_us":service.value_at_quantile(0.99) as f64/1000.0
        })
    );
    assert_eq!(
        errors, 0,
        "all replies must be complete and match the expected ID and result"
    );
}
