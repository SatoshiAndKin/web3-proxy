//! Private, bounded HTTP transport. Never put URLs or remote response text in errors.
use alloy::primitives::{Bytes, B256};
use alloy_rpc_types_engine::{Claims, JwtSecret, PayloadStatus};
use anyhow::{ensure, Result};
use futures_util::StreamExt;
use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    hash::{Hash, Hasher},
    sync::Arc,
    time::Duration,
};
use url::Url;

/// Identity of a Beacon endpoint. Credentials are represented by a stable digest,
/// never by their value, so transports cannot share reads across auth boundaries.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct EndpointIdentity {
    pub url: String,
    pub credential_identity: u64,
    pub network_identity: String,
}

impl EndpointIdentity {
    pub fn new(base: &str, headers: &BTreeMap<String, String>, network: &str) -> Result<Self> {
        let mut normalized = super::config::url(base)?;
        let path = normalized.path().trim_end_matches('/').to_owned();
        normalized.set_path(if path.is_empty() { "/" } else { &path });
        if (normalized.port() == Some(80) && normalized.scheme() == "http")
            || (normalized.port() == Some(443) && normalized.scheme() == "https")
        {
            let _ = normalized.set_port(None);
        }
        let url = normalized.to_string();
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        let normalized_headers: BTreeMap<_, _> = headers
            .iter()
            .map(|(key, value)| (key.to_ascii_lowercase(), value))
            .collect();
        for (key, value) in normalized_headers {
            key.hash(&mut hasher);
            value.hash(&mut hasher);
        }
        Ok(Self {
            url,
            credential_identity: hasher.finish(),
            network_identity: network.to_owned(),
        })
    }
}

/// Process-local registry for endpoint-scoped Beacon read coordinators.
#[derive(Default)]
pub struct BeaconReadCoordinators {
    entries: HashMap<EndpointIdentity, BeaconHttp>,
}

impl BeaconReadCoordinators {
    pub(super) fn get_or_insert(
        &mut self,
        base: &str,
        headers: &BTreeMap<String, String>,
        network: &str,
    ) -> Result<BeaconHttp> {
        let identity = EndpointIdentity::new(base, headers, network)?;
        if let Some(http) = self.entries.get(&identity) {
            return Ok(http.clone());
        }
        let http = BeaconHttp::new(base, headers)?;
        self.entries.insert(identity, http.clone());
        Ok(http)
    }
}

struct SharedRead {
    result: tokio::sync::OnceCell<std::result::Result<Option<bytes::Bytes>, String>>,
}

struct RegisteredRead {
    shared: Arc<SharedRead>,
    waiters: usize,
}

/// One consumer of a registered read. Dropping its future also drops this guard.
struct SharedReadGuard<'a> {
    registry: &'a Mutex<HashMap<String, RegisteredRead>>,
    key: String,
    shared: Arc<SharedRead>,
}

impl Drop for SharedReadGuard<'_> {
    fn drop(&mut self) {
        let mut reads = self.registry.lock();
        if let Some(current) = reads.get_mut(&self.key) {
            // A late guard must not decrement or remove a replacement generation.
            if Arc::ptr_eq(&current.shared, &self.shared) {
                current.waiters -= 1;
                if current.waiters == 0 {
                    reads.remove(&self.key);
                }
            }
        }
    }
}

/// The same private Beacon transport serves source reads and consensus targets.
#[derive(Clone)]
pub(super) struct BeaconHttp {
    inner: Arc<BeaconHttpInner>,
}
struct BeaconHttpInner {
    pub client: reqwest::Client,
    pub headers: reqwest::header::HeaderMap,
    base: Url,
    reads: tokio::sync::Semaphore,
    blob_reads: tokio::sync::Semaphore,
    shared_reads: Mutex<HashMap<String, RegisteredRead>>,
}
impl BeaconHttp {
    pub fn new(base: &str, values: &std::collections::BTreeMap<String, String>) -> Result<Self> {
        use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
        let mut headers = HeaderMap::new();
        for (key, value) in values {
            let key = HeaderName::from_bytes(key.as_bytes())
                .map_err(|_| anyhow::anyhow!("invalid Beacon header name"))?;
            let mut value = HeaderValue::from_str(value)
                .map_err(|_| anyhow::anyhow!("invalid Beacon header value"))?;
            value.set_sensitive(true);
            headers.insert(key, value);
        }
        Ok(Self {
            inner: Arc::new(BeaconHttpInner {
                client: client()?,
                headers,
                base: super::config::url(base)?,
                reads: tokio::sync::Semaphore::new(2),
                blob_reads: tokio::sync::Semaphore::new(2),
                shared_reads: Mutex::new(HashMap::new()),
            }),
        })
    }
    pub fn endpoint(&self, path: &str) -> Url {
        let mut url = self.inner.base.clone();
        url.set_path(&format!(
            "{}{}",
            self.inner.base.path().trim_end_matches('/'),
            path
        ));
        url
    }
    pub fn client(&self) -> &reqwest::Client {
        &self.inner.client
    }
    pub fn headers(&self) -> &reqwest::header::HeaderMap {
        &self.inner.headers
    }
    fn shared_read(&self, path: &str) -> SharedReadGuard<'_> {
        let key = path.to_owned();
        let mut reads = self.inner.shared_reads.lock();
        let registered = reads.entry(key.clone()).or_insert_with(|| RegisteredRead {
            shared: Arc::new(SharedRead {
                result: tokio::sync::OnceCell::new(),
            }),
            waiters: 0,
        });
        registered.waiters += 1;
        SharedReadGuard {
            registry: &self.inner.shared_reads,
            key,
            shared: registered.shared.clone(),
        }
    }
    pub async fn get_bytes(&self, path: &str) -> Result<Option<bytes::Bytes>> {
        // Register the consumer and its cleanup before the first suspension point.
        let read = self.shared_read(path);
        let result = read
            .shared
            .result
            .get_or_init(|| async { self.fetch_bytes(path).await.map_err(|e| e.to_string()) })
            .await
            .clone();
        result.map_err(|e| anyhow::anyhow!(e))
    }
    async fn fetch_bytes(&self, path: &str) -> Result<Option<bytes::Bytes>> {
        let pool = if path.starts_with("/eth/v1/beacon/blobs/") {
            &self.inner.blob_reads
        } else {
            &self.inner.reads
        };
        let operation = async {
            let _permit = pool.acquire().await?;
            let response = self
                .inner
                .client
                .get(self.endpoint(path))
                .headers(self.inner.headers.clone())
                .send()
                .await
                .map_err(|_| anyhow::anyhow!("Beacon transport error"))?;
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            Ok(Some(body(response).await?))
        };
        tokio::time::timeout(READ_TIMEOUT, operation)
            .await
            .map_err(|_| anyhow::anyhow!("Beacon read timeout"))?
    }
    pub async fn get_optional<T: DeserializeOwned>(&self, path: &str) -> Result<Option<T>> {
        self.get_bytes(path)
            .await?
            .map(|bytes| {
                sonic_rs::from_slice(&bytes).map_err(|_| anyhow::anyhow!("invalid Beacon response"))
            })
            .transpose()
    }
    pub async fn publish(&self, payload: &super::payload::ConsensusPayload) -> Result<u16> {
        let operation = async {
            let mut url = self.endpoint("/eth/v2/beacon/blocks");
            url.query_pairs_mut()
                .append_pair("broadcast_validation", "gossip");
            let response = self
                .inner
                .client
                .post(url)
                .headers(self.inner.headers.clone())
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .header("Eth-Consensus-Version", &payload.block.fork)
                .body(payload.body.clone())
                .send()
                .await
                .map_err(|_| {
                    anyhow::anyhow!("Beacon publication transport error; result unknown")
                })?;
            // Never expose response text: providers can echo URLs, credentials, or payloads.
            let status = response.status().as_u16();
            if response.status().is_success() {
                let _ = body(response).await?;
            }
            Ok(status)
        };
        tokio::time::timeout(ENGINE_TIMEOUT, operation)
            .await
            .map_err(|_| anyhow::anyhow!("Beacon publication timeout; result unknown"))?
    }
}

pub const ENGINE_TIMEOUT: Duration = Duration::from_secs(8);
pub const READ_TIMEOUT: Duration = Duration::from_secs(2);
pub const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

pub fn client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(1))
        .tcp_nodelay(true)
        .pool_idle_timeout(Duration::from_secs(90))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()?)
}

pub async fn body(response: reqwest::Response) -> Result<bytes::Bytes> {
    ensure!(
        response.status().is_success(),
        "HTTP status {}",
        response.status().as_u16()
    );
    ensure!(
        response.content_length().unwrap_or_default() <= MAX_RESPONSE_BYTES as u64,
        "response exceeds size limit"
    );
    let mut stream = response.bytes_stream();
    let mut bytes = bytes::BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| anyhow::anyhow!("response transport error"))?;
        ensure!(
            chunk.len() <= MAX_RESPONSE_BYTES - bytes.len(),
            "response exceeds size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes.freeze())
}

#[derive(Clone)]
enum Credential {
    Secret(JwtSecret),
    File(std::path::PathBuf),
}

#[derive(Clone)]
pub struct Rpc {
    client: reqwest::Client,
    url: Url,
    credential: Option<Credential>,
}
impl Rpc {
    pub fn new(url: &str, jwt: Option<JwtSecret>) -> Result<Self> {
        Ok(Self {
            client: client()?,
            url: super::config::url(url)?,
            credential: jwt.map(Credential::Secret),
        })
    }
    pub fn with_jwt_file(url: &str, path: &std::path::Path) -> Result<Self> {
        let mut rpc = Self::new(url, None)?;
        rpc.credential = Some(Credential::File(path.to_owned()));
        Ok(rpc)
    }
    pub async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: impl Serialize,
    ) -> Result<T> {
        #[derive(Serialize)]
        struct Request<'a, P> {
            jsonrpc: &'static str,
            id: u64,
            method: &'a str,
            params: P,
        }
        let request = Request {
            jsonrpc: "2.0",
            id: 1,
            method,
            params,
        };
        self.send(sonic_rs::to_vec(&request)?.into(), READ_TIMEOUT)
            .await
    }
    pub async fn new_payload(
        &self,
        payload: &super::payload::RelayPayload,
    ) -> Result<PayloadStatus> {
        self.send(payload.body.clone(), ENGINE_TIMEOUT).await
    }
    pub async fn get_blobs_v2(&self, hashes: &[B256]) -> Result<Option<Vec<Bytes>>> {
        #[derive(Deserialize)]
        struct Blob {
            blob: Bytes,
            #[serde(rename = "versionedHash")]
            versioned_hash: B256,
        }
        let result: Option<Vec<Blob>> = self.call("engine_getBlobsV2", (hashes,)).await?;
        let Some(result) = result else {
            return Ok(None);
        };
        ensure!(
            result.len() == hashes.len(),
            "incomplete engine blob response"
        );
        for (item, expected) in result.iter().zip(hashes) {
            ensure!(
                item.versioned_hash == *expected,
                "engine blob hash mismatch"
            );
        }
        Ok(Some(result.into_iter().map(|item| item.blob).collect()))
    }
    pub async fn send<T: DeserializeOwned>(
        &self,
        bytes: bytes::Bytes,
        duration: Duration,
    ) -> Result<T> {
        let operation = async {
            let mut request = self
                .client
                .post(self.url.clone())
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(bytes);
            let jwt = match &self.credential {
                Some(Credential::File(path)) => {
                    let text = tokio::fs::read_to_string(path)
                        .await
                        .map_err(|_| anyhow::anyhow!("cannot read target JWT secret"))?;
                    Some(
                        JwtSecret::from_hex(text.trim())
                            .map_err(|_| anyhow::anyhow!("invalid target JWT secret"))?,
                    )
                }
                Some(Credential::Secret(secret)) => Some(*secret),
                None => None,
            };
            if let Some(jwt) = jwt {
                let token = jwt
                    .encode(&Claims::default())
                    .map_err(|_| anyhow::anyhow!("JWT creation failed"))?;
                request = request.bearer_auth(token);
            }
            let response = request
                .send()
                .await
                .map_err(|_| anyhow::anyhow!("RPC transport error; result unknown"))?;
            #[derive(Deserialize)]
            struct RemoteError {
                code: i64,
            }
            #[derive(Deserialize)]
            struct RawEnvelope {
                jsonrpc: String,
                id: u64,
                error: Option<RemoteError>,
            }
            let bytes = body(response).await?;
            let envelope: RawEnvelope = sonic_rs::from_slice(&bytes)
                .map_err(|_| anyhow::anyhow!("invalid RPC envelope"))?;
            ensure!(
                envelope.jsonrpc == "2.0" && envelope.id == 1,
                "RPC response ID/version mismatch"
            );
            if let Some(error) = envelope.error {
                anyhow::bail!("RPC error {}", error.code);
            }
            // Missing and null are not equivalent. Inspect the envelope before decoding T.
            use sonic_rs::JsonValueTrait;
            let raw: sonic_rs::Value = sonic_rs::from_slice(&bytes)
                .map_err(|_| anyhow::anyhow!("invalid RPC envelope"))?;
            let value = raw
                .get("result")
                .ok_or_else(|| anyhow::anyhow!("missing RPC result"))?;
            sonic_rs::from_value(value).map_err(|_| anyhow::anyhow!("invalid RPC result"))
        };
        tokio::time::timeout(duration, operation)
            .await
            .map_err(|_| anyhow::anyhow!("RPC timeout; result unknown"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, response::Response, routing::get, Router};
    use futures_util::poll;
    use tokio::sync::{mpsc, oneshot};

    struct ReadServer {
        http: BeaconHttp,
        requests: mpsc::UnboundedReceiver<(String, oneshot::Sender<Response>)>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for ReadServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl ReadServer {
        async fn new() -> Self {
            let (tx, requests) = mpsc::unbounded_channel();
            let router = Router::new().fallback(get(move |uri: axum::http::Uri| {
                let tx = tx.clone();
                async move {
                    let (reply, response) = oneshot::channel();
                    tx.send((uri.path().to_owned(), reply)).unwrap();
                    response.await.unwrap_or_default()
                }
            }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let http = BeaconHttp::new(
                &format!("http://{}", listener.local_addr().unwrap()),
                &BTreeMap::new(),
            )
            .unwrap();
            let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
            Self {
                http,
                requests,
                task,
            }
        }

        async fn next(&mut self) -> (String, oneshot::Sender<Response>) {
            tokio::time::timeout(Duration::from_secs(1), self.requests.recv())
                .await
                .unwrap()
                .unwrap()
        }
    }

    fn assert_permits(http: &BeaconHttp) {
        assert_eq!(http.inner.reads.available_permits(), 2);
        assert_eq!(http.inner.blob_reads.available_permits(), 2);
    }

    #[tokio::test]
    async fn cancelled_queued_shared_reads_remove_unique_roots() {
        let http = BeaconHttp::new("http://localhost", &BTreeMap::new()).unwrap();
        for prefix in ["/eth/v2/beacon/blocks/", "/eth/v1/beacon/blobs/"] {
            let pool = if prefix.contains("blobs") {
                &http.inner.blob_reads
            } else {
                &http.inner.reads
            };
            let held = pool.acquire_many(2).await.unwrap();
            for root in 0..16 {
                let path = format!("{prefix}{:x}", B256::with_last_byte(root));
                let mut read = Box::pin(http.get_bytes(&path));
                assert!(poll!(&mut read).is_pending());
                assert_eq!(http.inner.shared_reads.lock().len(), 1);
                drop(read);
                assert!(
                    http.inner.shared_reads.lock().is_empty(),
                    "abandoned {path}"
                );
            }
            drop(held);
            assert_permits(&http);
        }
    }

    #[tokio::test]
    async fn cancelled_http_shared_reads_remove_unique_roots() {
        let mut server = ReadServer::new().await;
        for prefix in ["/eth/v2/beacon/blocks/", "/eth/v1/beacon/blobs/"] {
            for root in 0..8 {
                let path = format!("{prefix}{:x}", B256::with_last_byte(root));
                let http = server.http.clone();
                let requested = path.clone();
                let read = tokio::spawn(async move { http.get_bytes(&requested).await });
                let (actual, reply) = server.next().await;
                assert_eq!(actual, path);
                // Alternate cancellation before headers and during a streamed body.
                let mut body_sender = None;
                if root % 2 == 0 {
                    let (tx, rx) =
                        mpsc::channel::<std::result::Result<bytes::Bytes, std::io::Error>>(1);
                    tx.send(Ok(bytes::Bytes::from_static(b"partial")))
                        .await
                        .unwrap();
                    reply
                        .send(Response::new(Body::from_stream(
                            tokio_stream::wrappers::ReceiverStream::new(rx),
                        )))
                        .unwrap();
                    // A free channel slot proves that the server has started the body.
                    let permit = tx.reserve().await.unwrap();
                    drop(permit);
                    body_sender = Some(tx);
                } else {
                    drop(reply);
                }
                read.abort();
                assert!(read.await.unwrap_err().is_cancelled());
                assert!(
                    server.http.inner.shared_reads.lock().is_empty(),
                    "abandoned {path}"
                );
                assert_permits(&server.http);
                drop(body_sender);
            }
        }
    }

    #[tokio::test]
    async fn cancelling_one_shared_consumer_preserves_the_survivor() {
        for cancel_initializer in [false, true] {
            let mut server = ReadServer::new().await;
            let http = server.http.clone();
            let path = "/eth/v2/beacon/blocks/shared";
            let held = http.inner.reads.acquire_many(2).await.unwrap();
            let mut initializer = Box::pin(http.get_bytes(path));
            assert!(poll!(&mut initializer).is_pending());
            let mut follower = Box::pin(http.get_bytes(path));
            assert!(poll!(&mut follower).is_pending());
            let original = http
                .inner
                .shared_reads
                .lock()
                .get(path)
                .unwrap()
                .shared
                .clone();
            let survivor = if cancel_initializer {
                drop(initializer);
                follower
            } else {
                drop(follower);
                initializer
            };
            assert!(http
                .inner
                .shared_reads
                .lock()
                .get(path)
                .is_some_and(|current| Arc::ptr_eq(&current.shared, &original)));
            drop(held);
            let control = async {
                let (_, reply) = server.next().await;
                reply.send(Response::new(Body::from("survived"))).unwrap();
            };
            let (result, ()) = tokio::join!(survivor, control);
            assert_eq!(
                result.unwrap(),
                Some(bytes::Bytes::from_static(b"survived"))
            );
            assert!(http.inner.shared_reads.lock().is_empty());
            assert_permits(&http);
        }
    }

    #[tokio::test]
    async fn cancelling_an_http_consumer_preserves_shared_initialization() {
        for cancel_initializer in [false, true] {
            let mut server = ReadServer::new().await;
            let http = server.http.clone();
            let mut initializer = Box::pin(http.get_bytes("/read"));
            let (_, reply) = tokio::select! {
                biased;
                _ = &mut initializer => panic!("read completed before response"),
                request = server.next() => request,
            };
            let mut follower = Box::pin(http.get_bytes("/read"));
            assert!(poll!(&mut follower).is_pending());
            let survivor = if cancel_initializer {
                drop(initializer);
                follower
            } else {
                drop(follower);
                initializer
            };
            assert_eq!(
                http.inner.shared_reads.lock().get("/read").unwrap().waiters,
                1
            );
            let control = async {
                if cancel_initializer {
                    // OnceCell allows the remaining consumer to restart this read.
                    let (_, restarted) = server.next().await;
                    restarted
                        .send(Response::new(Body::from("survived")))
                        .unwrap();
                    drop(reply);
                } else {
                    reply.send(Response::new(Body::from("survived"))).unwrap();
                }
            };
            let (result, ()) = tokio::join!(survivor, control);
            assert_eq!(
                result.unwrap(),
                Some(bytes::Bytes::from_static(b"survived"))
            );
            assert!(server.requests.try_recv().is_err());
            assert!(http.inner.shared_reads.lock().is_empty());
            assert_permits(&http);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shared_read_timeout_releases_registry_after_last_consumer() {
        let http = BeaconHttp::new("http://localhost", &BTreeMap::new()).unwrap();
        let held = http.inner.blob_reads.acquire_many(2).await.unwrap();
        let mut first = Box::pin(http.get_bytes("/eth/v1/beacon/blobs/timed-out"));
        let mut last = Box::pin(http.get_bytes("/eth/v1/beacon/blobs/timed-out"));
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut last).is_pending());
        tokio::time::advance(READ_TIMEOUT).await;
        assert_eq!(first.await.unwrap_err().to_string(), "Beacon read timeout");
        assert_eq!(http.inner.shared_reads.lock().len(), 1);
        assert_eq!(last.await.unwrap_err().to_string(), "Beacon read timeout");
        assert!(http.inner.shared_reads.lock().is_empty());
        drop(held);
        assert_permits(&http);
    }

    #[tokio::test]
    async fn completed_shared_consumer_keeps_entry_until_last_waiter_leaves() {
        let mut server = ReadServer::new().await;
        let http = server.http.clone();
        let mut first = Box::pin(http.get_bytes("/read"));
        let mut last = Box::pin(http.get_bytes("/read"));
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut last).is_pending());
        let control = async {
            let (_, reply) = server.next().await;
            reply.send(Response::new(Body::from("complete"))).unwrap();
        };
        let (result, ()) = tokio::join!(first, control);
        assert_eq!(
            result.unwrap(),
            Some(bytes::Bytes::from_static(b"complete"))
        );
        assert_eq!(http.inner.shared_reads.lock().len(), 1);
        assert_eq!(
            last.await.unwrap(),
            Some(bytes::Bytes::from_static(b"complete"))
        );
        assert!(http.inner.shared_reads.lock().is_empty());
        assert_permits(&http);
    }

    #[tokio::test]
    async fn source_race_winner_and_deadline_clean_up_shared_reads() {
        use super::super::{
            config::{CostClass, Resource},
            source::{race_sources, BeaconSource},
        };
        use std::sync::atomic::AtomicBool;
        for winner in [true, false] {
            let mut server = ReadServer::new().await;
            let sources: Vec<_> = (0..2)
                .map(|i| {
                    Arc::new(BeaconSource {
                        name: format!("source-{i}"),
                        http: server.http.clone(),
                        cost_class: CostClass::Free,
                        resources: vec![Resource::Block],
                        verified: AtomicBool::new(true),
                    })
                })
                .collect();
            let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
            let race = tokio::spawn(async move {
                race_sources(
                    &sources,
                    deadline,
                    &tokio::sync::Notify::new(),
                    Resource::Block,
                    Duration::ZERO,
                    |source| async move {
                        source
                            .get_bytes(&format!("/eth/v2/beacon/blocks/{}", source.name))
                            .await
                    },
                )
                .await
            });
            let first = server.next().await;
            let second = server.next().await;
            if winner {
                first.1.send(Response::new(Body::from("winner"))).unwrap();
                assert_eq!(
                    race.await.unwrap().unwrap(),
                    (
                        bytes::Bytes::from_static(b"winner"),
                        first.0.rsplit('/').next().unwrap().to_owned()
                    )
                );
            } else {
                tokio::time::pause();
                tokio::time::advance(
                    deadline.saturating_duration_since(tokio::time::Instant::now()),
                )
                .await;
                assert_eq!(
                    race.await.unwrap().unwrap_err().to_string(),
                    "acquisition deadline"
                );
                tokio::time::resume();
            }
            drop(second);
            assert!(server.http.inner.shared_reads.lock().is_empty());
            assert_permits(&server.http);
        }
    }

    #[test]
    fn old_waiter_cannot_change_replacement_read() {
        let http = BeaconHttp::new("http://localhost", &BTreeMap::new()).unwrap();
        let key = "/eth/v1/beacon/headers/head";
        let old = http.shared_read(key);
        let late_waiter = http.shared_read(key);
        old.shared.result.set(Ok(None)).unwrap();
        // Force a replacement while old guards exist, then drop both generations.
        http.inner.shared_reads.lock().remove(key);
        let replacement = http.shared_read(key);
        let replacement_follower = http.shared_read(key);
        drop(old);
        drop(late_waiter);
        {
            let reads = http.inner.shared_reads.lock();
            let current = reads.get(key).unwrap();
            assert!(Arc::ptr_eq(&current.shared, &replacement.shared));
            assert_eq!(current.waiters, 2);
        }
        drop(replacement);
        assert_eq!(http.inner.shared_reads.lock().get(key).unwrap().waiters, 1);
        drop(replacement_follower);
        assert!(http.inner.shared_reads.lock().is_empty());
    }

    #[tokio::test]
    async fn concurrent_beacon_reads_share_responses_but_not_credentials() {
        use axum::{
            body::Body,
            http::{HeaderMap, StatusCode},
            routing::get,
            Router,
        };
        use tokio::sync::{mpsc, oneshot};
        let (tx, mut requests) = mpsc::unbounded_channel();
        let server = Router::new().route(
            "/read",
            get(move |headers: HeaderMap| {
                let tx = tx.clone();
                async move {
                    let (reply, rx) = oneshot::channel::<(StatusCode, Body)>();
                    tx.send((headers["x-token"].to_str().unwrap().to_owned(), reply))
                        .unwrap();
                    rx.await.unwrap()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, server).await.unwrap() });
        let mut registry = BeaconReadCoordinators::default();
        let headers = BTreeMap::from([("x-token".into(), "first".into())]);
        let first = registry.get_or_insert(&url, &headers, "mainnet").unwrap();
        let same = registry.get_or_insert(&url, &headers, "mainnet").unwrap();
        let other = registry
            .get_or_insert(
                &url,
                &BTreeMap::from([("x-token".into(), "second".into())]),
                "mainnet",
            )
            .unwrap();
        for status in [
            StatusCode::OK,
            StatusCode::NOT_FOUND,
            StatusCode::TOO_MANY_REQUESTS,
        ] {
            let a = first.get_bytes("/read");
            let b = same.get_bytes("/read");
            let c = other.get_bytes("/read");
            let control = async {
                let mut identities = Vec::new();
                for _ in 0..2 {
                    let (identity, reply) = requests.recv().await.unwrap();
                    identities.push(identity);
                    reply.send((status, Body::from("shared body"))).unwrap();
                }
                identities.sort();
                assert_eq!(identities, ["first", "second"]);
            };
            let (a, b, c, ()) = tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(a, b, c, control)
            })
            .await
            .unwrap();
            let expected = match status {
                StatusCode::OK => Ok(Some(bytes::Bytes::from_static(b"shared body"))),
                StatusCode::NOT_FOUND => Ok(None),
                _ => Err("HTTP status 429".to_owned()),
            };
            for result in [a, b, c] {
                assert_eq!(result.map_err(|e| e.to_string()), expected);
            }
            assert!(requests.try_recv().is_err());
            for http in [&first, &same, &other] {
                assert!(http.inner.shared_reads.lock().is_empty());
                assert_permits(http);
            }
        }
        server.abort();
    }

    #[test]
    fn endpoint_identity_normalizes_url_but_keeps_credentials_and_networks_separate() {
        let mut first = BTreeMap::new();
        first.insert("X-Token".into(), "one".into());
        let mut second = BTreeMap::new();
        second.insert("x-token".into(), "two".into());
        let a = EndpointIdentity::new("https://node.example:443/api/", &first, "mainnet").unwrap();
        let b = EndpointIdentity::new("https://node.example/api", &first, "mainnet").unwrap();
        assert_eq!(a, b);
        assert_ne!(
            a,
            EndpointIdentity::new("https://node.example/api", &second, "mainnet").unwrap()
        );
        assert_ne!(
            a,
            EndpointIdentity::new("https://node.example/api", &first, "devnet").unwrap()
        );
    }
}
