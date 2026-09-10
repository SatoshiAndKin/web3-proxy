//! Private, bounded HTTP transport. Never put URLs or remote response text in errors.
use alloy_rpc_types_engine::{Claims, JwtSecret, PayloadStatus};
use anyhow::{ensure, Result};
use futures_util::StreamExt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::time::Duration;
use url::Url;

/// The same private Beacon transport serves source reads and consensus targets.
pub(super) struct BeaconHttp {
    pub client: reqwest::Client,
    pub headers: reqwest::header::HeaderMap,
    base: Url,
    reads: tokio::sync::Semaphore,
    blob_reads: tokio::sync::Semaphore,
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
            client: client()?,
            headers,
            base: super::config::url(base)?,
            reads: tokio::sync::Semaphore::new(2),
            blob_reads: tokio::sync::Semaphore::new(2),
        })
    }
    pub fn endpoint(&self, path: &str) -> Url {
        let mut url = self.base.clone();
        url.set_path(&format!(
            "{}{}",
            self.base.path().trim_end_matches('/'),
            path
        ));
        url
    }
    pub async fn get_bytes(&self, path: &str) -> Result<Option<bytes::Bytes>> {
        let pool = if path.starts_with("/eth/v1/beacon/blobs/") {
            &self.blob_reads
        } else {
            &self.reads
        };
        let operation = async {
            let _permit = pool.acquire().await?;
            let response = self
                .client
                .get(self.endpoint(path))
                .headers(self.headers.clone())
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
                .client
                .post(url)
                .headers(self.headers.clone())
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
pub struct Rpc {
    client: reqwest::Client,
    url: Url,
    jwt: Option<JwtSecret>,
}
impl Rpc {
    pub fn new(url: &str, jwt: Option<JwtSecret>) -> Result<Self> {
        Ok(Self {
            client: client()?,
            url: super::config::url(url)?,
            jwt,
        })
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
            if let Some(jwt) = &self.jwt {
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
