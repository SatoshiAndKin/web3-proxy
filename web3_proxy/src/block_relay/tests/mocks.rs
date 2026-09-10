//! HTTP/SSE fixtures only. These tests never start an Ethereum client.
use super::*;
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use parking_lot::Mutex;
use std::collections::VecDeque;

pub struct Server {
    pub url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    pub async fn start(router: Router) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self { url, task }
    }
    pub async fn rpc(rpc: MockRpc) -> Self {
        Self::start(Router::new().route("/", post(handle_rpc)).with_state(rpc)).await
    }
    pub async fn beacon(beacon: MockBeacon) -> Self {
        Self::start(Router::new().fallback(handle_beacon).with_state(beacon)).await
    }
}

pub struct BeaconState {
    pub network: config::Network,
    pub blocks: BTreeMap<B256, Vec<u8>>,
    pub hidden: std::collections::BTreeSet<B256>,
    pub block_reads: Vec<B256>,
    pub block_gates: BTreeMap<B256, Arc<tokio::sync::Notify>>,
    pub gossip_supported: bool,
    pub queries: Vec<String>,
}
#[derive(Clone)]
pub struct MockBeacon {
    pub state: Arc<Mutex<BeaconState>>,
    pub events: broadcast::Sender<(&'static str, String)>,
}
impl MockBeacon {
    pub fn new(network: config::Network) -> Self {
        Self {
            state: Arc::new(Mutex::new(BeaconState {
                network,
                blocks: BTreeMap::new(),
                hidden: Default::default(),
                block_reads: Vec::new(),
                block_gates: Default::default(),
                gossip_supported: true,
                queries: Vec::new(),
            })),
            events: broadcast::channel(128).0,
        }
    }
    pub fn add(&self, block: &payload::BeaconResponse) -> B256 {
        let root = tree_hash::block_root(&block.data.message).unwrap();
        self.state
            .lock()
            .blocks
            .insert(root, sonic_rs::to_vec(block).unwrap());
        root
    }
    pub fn announce(&self, kind: &'static str, root: B256, slot: u64) {
        self.events
            .send((
                kind,
                json!({"block": root, "slot": slot.to_string(), "execution_optimistic": false})
                    .to_string(),
            ))
            .unwrap();
    }
}
async fn handle_beacon(State(beacon): State<MockBeacon>, uri: axum::http::Uri) -> Response {
    use axum::response::sse::{Event, Sse};
    let gate = {
        let mut state = beacon.state.lock();
        if uri.path().starts_with("/eth/v2/beacon/blocks/") {
            let root = uri.path().rsplit('/').next().unwrap().parse().unwrap();
            state.block_reads.push(root);
            state.block_gates.get(&root).cloned()
        } else {
            None
        }
    };
    if let Some(gate) = gate {
        gate.notified().await;
    }
    let mut state = beacon.state.lock();
    let network = &state.network;
    match uri.path() {
        "/eth/v1/beacon/genesis" => response(
            json!({"data": {"genesis_time": network.genesis_time.to_string(),
            "genesis_validators_root": network.genesis_validators_root, "genesis_fork_version": "0x00000000"}}),
        ),
        "/eth/v1/config/fork_schedule" => {
            response(json!({"data": network.forks.iter().map(|f| json!({
            "current_version": f.version, "previous_version": "0x00000000", "epoch": f.epoch.to_string()
        })).collect::<Vec<_>>()}))
        }
        "/eth/v1/config/spec" => response(
            json!({"data": {"PRESET_BASE": "mainnet", "SECONDS_PER_SLOT": network.seconds_per_slot.to_string()}}),
        ),
        "/eth/v1/events" => {
            let query = uri.query().unwrap_or_default();
            state.queries.push(query.to_string());
            if !state.gossip_supported && query.contains("block_gossip") {
                return StatusCode::BAD_REQUEST.into_response();
            }
            let mut rx = beacon.events.subscribe();
            Sse::new(async_stream::stream! {
                while let Ok((kind, data)) = rx.recv().await {
                    yield Ok::<_, std::convert::Infallible>(Event::default().event(kind).data(data));
                }
            }).into_response()
        }
        path if path.starts_with("/eth/v2/beacon/blocks/") => {
            let root: B256 = path.rsplit('/').next().unwrap().parse().unwrap();
            if state.hidden.contains(&root) {
                return StatusCode::NOT_FOUND.into_response();
            }
            match state.blocks.get(&root) {
                Some(bytes) => bytes.clone().into_response(),
                None => StatusCode::NOT_FOUND.into_response(),
            }
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Default)]
pub struct RpcState {
    pub known: BTreeMap<B256, u64>,
    pub methods: Vec<String>,
    pub payloads: Vec<B256>,
    pub replies: BTreeMap<B256, VecDeque<&'static str>>,
    pub gates: BTreeMap<B256, Arc<tokio::sync::Notify>>,
    pub read_error: bool,
    pub bad_jwt: u64,
    pub inflight: usize,
    pub max_inflight: usize,
    pub wrong_valid_hash: bool,
    pub chain_id: u64,
    pub supports_v4: bool,
}
#[derive(Clone)]
pub struct MockRpc {
    pub state: Arc<Mutex<RpcState>>,
}
impl MockRpc {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(RpcState {
                chain_id: 1,
                supports_v4: true,
                ..Default::default()
            })),
        }
    }
    pub fn payload_hashes(&self) -> Vec<B256> {
        self.state.lock().payloads.clone()
    }
    pub fn target(&self, url: &str, name: &str) -> Arc<target::Target> {
        Arc::new(target::Target {
            name: name.to_string(),
            engine: transport::Rpc::new(url, Some(secret())).unwrap(),
            rpc: transport::Rpc::new(url, None).unwrap(),
            uncertain: Default::default(),
            probes: tokio::sync::Semaphore::new(4),
        })
    }
}
fn secret() -> alloy_rpc_types_engine::JwtSecret {
    alloy_rpc_types_engine::JwtSecret::from_hex(include_str!("../fixtures/test-jwt.hex").trim())
        .unwrap()
}
pub fn response(value: sonic_rs::Value) -> Response {
    ([("content-type", "application/json")], value.to_string()).into_response()
}

async fn handle_rpc(State(rpc): State<MockRpc>, headers: HeaderMap, bytes: Bytes) -> Response {
    let request: sonic_rs::Value = sonic_rs::from_slice(&bytes).unwrap();
    let method = request["method"].as_str().unwrap();
    rpc.state.lock().methods.push(method.to_string());
    if method.starts_with("engine_") {
        let valid = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .is_some_and(|token| secret().validate(token).is_ok());
        if !valid {
            rpc.state.lock().bad_jwt += 1;
            return StatusCode::UNAUTHORIZED.into_response();
        }
    }
    let result = match method {
        "eth_chainId" => json!(format!("0x{:x}", rpc.state.lock().chain_id)),
        "engine_exchangeCapabilities" => {
            if rpc.state.lock().supports_v4 {
                json!(["engine_newPayloadV4"])
            } else {
                json!([])
            }
        }
        "eth_getBlockByHash" | "eth_getBlockByNumber" => {
            let s = rpc.state.lock();
            if s.read_error {
                return response(
                    json!({"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "credential-must-not-leak"}}),
                );
            }
            let block = if method == "eth_getBlockByHash" {
                let hash: B256 = request["params"][0].as_str().unwrap().parse().unwrap();
                s.known.get(&hash).map(|n| (hash, *n))
            } else {
                let number = u64::from_str_radix(
                    request["params"][0]
                        .as_str()
                        .unwrap()
                        .trim_start_matches("0x"),
                    16,
                )
                .unwrap();
                s.known
                    .iter()
                    .find(|(_, n)| **n == number)
                    .map(|(h, n)| (*h, *n))
            };
            match block {
                Some((hash, number)) => json!({"hash": hash, "number": format!("0x{number:x}")}),
                None => json!(null),
            }
        }
        "engine_newPayloadV4" => {
            let hash: B256 = request["params"][0]["blockHash"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            let number = u64::from_str_radix(
                request["params"][0]["blockNumber"]
                    .as_str()
                    .unwrap()
                    .trim_start_matches("0x"),
                16,
            )
            .unwrap();
            let (gate, status, wrong_hash) = {
                let mut s = rpc.state.lock();
                s.payloads.push(hash);
                s.inflight += 1;
                s.max_inflight = s.max_inflight.max(s.inflight);
                (
                    s.gates.get(&hash).cloned(),
                    s.replies
                        .get_mut(&hash)
                        .and_then(|v| v.pop_front())
                        .unwrap_or("VALID"),
                    s.wrong_valid_hash,
                )
            };
            // Model execution that continues after an HTTP client disconnects.
            tokio::spawn(async move {
                if let Some(gate) = gate { gate.notified().await; }
                let mut s = rpc.state.lock(); s.inflight -= 1;
                if status == "VALID" { s.known.insert(hash, number); }
                json!({"status": status, "latestValidHash": if status == "VALID" { Some(if wrong_hash { B256::ZERO } else { hash }) } else { None },
                    "validationError": if status == "INVALID" { Some("invalid test payload") } else { None }})
            }).await.unwrap()
        }
        _ => panic!("unexpected relay method: {method}"),
    };
    response(json!({"jsonrpc": "2.0", "id": 1, "result": result}))
}
