use super::one::Web3Rpc;
use crate::errors::{Web3ProxyError, Web3ProxyResult};
use crate::jsonrpc::{
    self, JsonRpcErrorData, JsonRpcResultData, ParsedResponse, ResponsePayload, SingleRequest,
    ValidatedRequest,
};
use alloy::providers::Provider;
use anyhow::Context;
use derive_more::From;
use futures::future::join_all;
use futures::Future;
use reqwest::StatusCode;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info, trace, warn, Level};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BackendTransportFailure {
    HttpTimeout,
    HttpConnect,
    HttpStatus(StatusCode),
    HttpBody,
    HttpDecode,
    HttpRequest,
    HttpOther,
    Io {
        kind: std::io::ErrorKind,
        os_code: Option<i32>,
    },
    Alloy,
}

impl fmt::Display for BackendTransportFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HttpTimeout => formatter.write_str("http_timeout"),
            Self::HttpConnect => formatter.write_str("http_connect"),
            Self::HttpStatus(status) => write!(formatter, "http_status_{}", status.as_u16()),
            Self::HttpBody => formatter.write_str("http_body"),
            Self::HttpDecode => formatter.write_str("http_decode"),
            Self::HttpRequest => formatter.write_str("http_request"),
            Self::HttpOther => formatter.write_str("http_other"),
            Self::Io { kind, os_code } => {
                write!(formatter, "io_{kind:?}")?;
                if let Some(os_code) = os_code {
                    write!(formatter, "_{os_code}")?;
                }
                Ok(())
            }
            Self::Alloy => formatter.write_str("alloy_transport"),
        }
    }
}

fn backend_transport_failure(error: &Web3ProxyError) -> Option<BackendTransportFailure> {
    match error {
        Web3ProxyError::Reqwest(error) if error.is_timeout() => {
            Some(BackendTransportFailure::HttpTimeout)
        }
        Web3ProxyError::Reqwest(error) if error.is_connect() => {
            Some(BackendTransportFailure::HttpConnect)
        }
        Web3ProxyError::Reqwest(error) if error.is_status() => Some(
            BackendTransportFailure::HttpStatus(error.status().expect("status error has a status")),
        ),
        Web3ProxyError::Reqwest(error) if error.is_body() => {
            Some(BackendTransportFailure::HttpBody)
        }
        Web3ProxyError::Reqwest(error) if error.is_decode() => {
            Some(BackendTransportFailure::HttpDecode)
        }
        Web3ProxyError::Reqwest(error) if error.is_request() => {
            Some(BackendTransportFailure::HttpRequest)
        }
        Web3ProxyError::Reqwest(_) => Some(BackendTransportFailure::HttpOther),
        Web3ProxyError::Io(error) => Some(BackendTransportFailure::Io {
            kind: error.kind(),
            os_code: error.raw_os_error(),
        }),
        Web3ProxyError::AlloyTransport(_) => Some(BackendTransportFailure::Alloy),
        _ => None,
    }
}

fn history_error_for_request(
    request: &ValidatedRequest,
    error: &JsonRpcErrorData,
) -> Option<Web3ProxyError> {
    if request.requires_log_history()
        && error.code == 4444
        && error.message == "pruned history unavailable"
    {
        Some(Web3ProxyError::LogHistoryRequired {
            min: request.min_block_needed(),
            max: request.max_block_needed(),
        })
    } else {
        None
    }
}

#[derive(From)]
pub enum OpenRequestResult {
    Handle(OpenRequestHandle),
    /// Unable to start a request. Retry at the given time.
    RetryAt(Instant),
    /// The rpc are not synced, but they should be soon.
    /// You should wait for the given block number.
    /// TODO: should this return an OpenRequestHandle? that might recurse
    Lagged(Pin<Box<dyn Future<Output = Web3ProxyResult<Arc<Web3Rpc>>> + Send>>),
    /// Unable to start a request because no servers are synced or the necessary data has been pruned
    Failed,
}

/// Make RPC requests through this handle and drop it when you are done.
/// Opening this handle checks rate limits. Developers, try to keep opening a handle and using it as close together as possible
pub struct OpenRequestHandle {
    web3_request: Arc<ValidatedRequest>,
    error_handler: RequestErrorHandler,
    rpc: Arc<Web3Rpc>,
}

struct BatchActiveRequestGuard {
    rpc: Arc<Web3Rpc>,
    extra_requests: usize,
}

impl BatchActiveRequestGuard {
    fn new(rpc: Arc<Web3Rpc>, request_count: usize) -> Self {
        let extra_requests = request_count.saturating_sub(1);
        rpc.active_requests
            .fetch_add(extra_requests, atomic::Ordering::SeqCst);
        Self {
            rpc,
            extra_requests,
        }
    }
}

impl Drop for BatchActiveRequestGuard {
    fn drop(&mut self) {
        self.rpc
            .active_requests
            .fetch_sub(self.extra_requests, atomic::Ordering::SeqCst);
    }
}

/// Depending on the context, RPC errors require different handling.
#[derive(Copy, Clone, Debug, Default)]
pub enum RequestErrorHandler {
    /// Log at the trace level. Use when errors are expected.
    #[default]
    TraceLevel,
    /// Log at the debug level. Use when errors are expected.
    DebugLevel,
    /// Log at the info level. Use when errors are expected.
    InfoLevel,
    /// Log at the error level. Use when errors are bad.
    ErrorLevel,
    /// Log at the warn level. Use when errors do not cause problems.
    WarnLevel,
}

impl std::fmt::Debug for OpenRequestHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenRequestHandle")
            .field("method", &self.web3_request.inner.method())
            .field("rpc", &self.rpc.name)
            .finish_non_exhaustive()
    }
}

impl From<Level> for RequestErrorHandler {
    fn from(level: Level) -> Self {
        match level {
            Level::DEBUG => RequestErrorHandler::DebugLevel,
            Level::ERROR => RequestErrorHandler::ErrorLevel,
            Level::INFO => RequestErrorHandler::InfoLevel,
            Level::TRACE => RequestErrorHandler::TraceLevel,
            Level::WARN => RequestErrorHandler::WarnLevel,
        }
    }
}

impl Drop for OpenRequestHandle {
    fn drop(&mut self) {
        self.rpc
            .active_requests
            .fetch_sub(1, atomic::Ordering::SeqCst);
    }
}

impl OpenRequestHandle {
    fn delay_reuse_for(&self, duration: Duration) {
        let retry_at = Instant::now() + duration;
        self.rpc
            .hard_limit_until
            .as_ref()
            .unwrap()
            .send_if_modified(|current| {
                if *current >= retry_at {
                    false
                } else {
                    *current = retry_at;
                    true
                }
            });
    }

    pub async fn new(
        web3_request: Arc<ValidatedRequest>,
        rpc: Arc<Web3Rpc>,
        error_handler: Option<RequestErrorHandler>,
    ) -> Self {
        // TODO: take request_id as an argument?
        // TODO: attach a unique id to this? customer requests have one, but not internal queries
        // TODO: what ordering?!
        rpc.active_requests
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let error_handler = error_handler.unwrap_or_default();

        Self {
            web3_request,
            error_handler,
            rpc,
        }
    }

    pub fn connection_name(&self) -> String {
        self.rpc.name.clone()
    }

    #[inline]
    pub fn clone_connection(&self) -> Arc<Web3Rpc> {
        self.rpc.clone()
    }

    pub fn batch_capacity(&self) -> usize {
        self.rpc.request_permits.max_concurrent_requests()
    }

    pub fn rate_limit_for(&self, duration: Duration) {
        if self.rpc.backup {
            debug!(?duration, "rate limited on {}!", self.rpc);
        } else {
            warn!(?duration, "rate limited on {}!", self.rpc);
        }

        self.delay_reuse_for(duration);
    }

    /// Forward read-only JSON-RPC calls as bounded backend batch packets.
    pub async fn request_batch(
        self,
        requests: &[SingleRequest],
    ) -> Web3ProxyResult<Vec<ParsedResponse>> {
        let client = self
            .rpc
            .http_client
            .as_ref()
            .context("backend batch forwarding requires an HTTP client")?;
        let url = self
            .rpc
            .http_url
            .clone()
            .context("backend batch forwarding requires an HTTP URL")?;
        let chunk_size = self.rpc.request_permits.max_backend_batch_items();
        let mut responses = Vec::with_capacity(requests.len());
        let _active_request_guard = BatchActiveRequestGuard::new(self.rpc.clone(), requests.len());

        let chunks = requests.chunks(chunk_size).map(|chunk| {
            let rpc = self.rpc.clone();
            let client = client.clone();
            let url = url.clone();
            let handle = &self;
            async move {
                let started_at = Instant::now();
                let result = async {
                    let _request_permits = rpc.request_permits.acquire_many(chunk.len()).await?;
                    rpc.total_requests
                        .fetch_add(chunk.len(), atomic::Ordering::Relaxed);
                    rpc.backend_batch_requests
                        .fetch_add(1, atomic::Ordering::Relaxed);
                    let body = sonic_rs::to_vec(chunk)?;
                    let response = client
                        .post(url)
                        .header(reqwest::header::CONTENT_TYPE, "application/json")
                        .body(body)
                        .send()
                        .await?;

                    if response.status() == StatusCode::TOO_MANY_REQUESTS {
                        handle.rate_limit_for(Duration::from_secs(1));
                    }

                    let response = response.error_for_status()?;
                    let bytes = response.bytes().await?;
                    let chunk_responses: Vec<ParsedResponse> = sonic_rs::from_slice(&bytes)?;
                    if chunk_responses.len() != chunk.len() {
                        return Err(anyhow::anyhow!(
                            "backend batch returned {} responses for {} requests",
                            chunk_responses.len(),
                            chunk.len()
                        )
                        .into());
                    }
                    Ok(chunk_responses)
                }
                .await;
                (result, started_at.elapsed())
            }
        });

        for (result, latency) in join_all(chunks).await {
            match result {
                Ok(chunk_responses) => {
                    let rpc = self.rpc.clone();
                    tokio::spawn(async move {
                        rpc.peak_latency.as_ref().unwrap().report(latency);
                        rpc.median_latency.as_ref().unwrap().record(latency);
                    });
                    responses.extend(chunk_responses);
                }
                Err(error) => {
                    if let Some(transport_failure) = backend_transport_failure(&error) {
                        warn!(
                            rpc = %self.rpc,
                            method = "eth_call_batch",
                            transport_failure = %transport_failure,
                            elapsed_ms = latency.as_millis(),
                            "backend transport failed; delaying reuse"
                        );
                        self.delay_reuse_for(Duration::from_secs(1));
                    }
                    return Err(error);
                }
            }
        }

        Ok(responses)
    }

    /// Just get the response from the provider without any extra handling.
    /// This lets us use the try operator which makes it much easier to read
    async fn _request<R: JsonRpcResultData + serde::Serialize>(
        &self,
    ) -> Web3ProxyResult<jsonrpc::SingleResponse<R>> {
        let request_permit = self.rpc.request_permits.acquire().await?;
        let response = if let Some(ipc_path) = self.rpc.ipc_path.as_ref() {
            // first, prefer the unix stream
            let request = self
                .web3_request
                .inner
                .jsonrpc_request()
                .context("there should always be a request here")?;

            // TODO: instead of connecting every time, use a connection pool
            let mut ipc_stream = UnixStream::connect(ipc_path).await?;

            ipc_stream.writable().await?;

            let x = sonic_rs::to_vec(request)?;

            let _ = ipc_stream.write(&x).await?;

            ipc_stream.readable().await?;

            let mut buf = Vec::new();

            let n = ipc_stream.try_read(&mut buf)?;

            let x: ParsedResponse<R> = sonic_rs::from_slice(&buf[..n])?;

            Ok(x.into())
        } else if let (Some(url), Some(client)) = (self.rpc.http_url.clone(), &self.rpc.http_client)
        {
            // second, prefer the http provider
            let request = self
                .web3_request
                .inner
                .jsonrpc_request()
                .context("there should always be a request here")?;

            let body = sonic_rs::to_vec(request)?;
            let mut request_builder = client
                .post(url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body);
            if request.method == "eth_sendRawTransaction" {
                if let Some(ref request_id) = self.web3_request.request_id {
                    let mut headers = reqwest::header::HeaderMap::with_capacity(1);
                    let request_id = reqwest::header::HeaderValue::from_str(request_id)
                        .expect("request id should be a valid header");
                    headers.insert("x-amzn-trace-id", request_id);

                    // TODO: more headers for the various rpc protection modes

                    request_builder = request_builder.headers(headers);
                }
            }
            let response = request_builder.send().await?;

            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                // TODO: how much should we actually rate limit?
                self.rate_limit_for(Duration::from_secs(1));
            }

            let response = response.error_for_status()?;

            // Buffer responses up to 128 KiB. Stream larger responses.
            jsonrpc::SingleResponse::read_if_short(response, 131_072, &self.web3_request).await
        } else if let Some(p) = self.rpc.ws_provider.load().as_ref() {
            // use the websocket provider if no other provider is available
            let method = self.web3_request.inner.method();
            let params = self.web3_request.inner.params();
            let params = sonic_rs::to_string(params)?;
            let params =
                serde_json::value::RawValue::from_string(params).map_err(anyhow::Error::from)?;

            let response = match p.raw_request_dyn(method.to_string().into(), &params).await {
                Ok(value) => {
                    let value = sonic_rs::from_str::<R>(value.get())?;
                    jsonrpc::ParsedResponse::from_result(value, self.web3_request.id())
                }
                Err(transport_error) => match JsonRpcErrorData::try_from(&transport_error) {
                    Ok(x) => jsonrpc::ParsedResponse::from_error(x, self.web3_request.id()),
                    Err(err) => {
                        warn!(?err, "error from {}", self.rpc);

                        return Err(transport_error.into());
                    }
                },
            };

            Ok(response.into())
        } else {
            // this must be a test
            Err(anyhow::anyhow!("no provider configured!").into())
        };
        drop(request_permit);
        response
    }

    pub fn error_handler(&self) -> RequestErrorHandler {
        self.error_handler
    }

    /// Send a web3 request
    /// By having the request method here, we ensure that the rate limiter was called and connection counts were properly incremented
    /// depending on how things are locked, you might need to pass the provider in
    /// we take self to ensure this function only runs once
    /// This does some inspection of the response to check for non-standard errors and rate limiting to try to give a Web3ProxyError instead of an Ok
    pub async fn request<R: JsonRpcResultData + serde::Serialize>(
        self,
    ) -> Web3ProxyResult<jsonrpc::SingleResponse<R>> {
        // TODO: use tracing spans
        // TODO: including params in this log is way too verbose
        // trace!(rpc=%self.rpc, %method, "request");
        trace!("requesting from {}", self.rpc);

        self.rpc
            .total_requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // we used to fetch_add the active_request count here, but sometimes a request is made without going through this function (like with subscriptions)

        // we generally don't want to use the try operator. we might need to log errors
        let start = Instant::now();

        let mut response = self._request().await;

        if let Err(error) = &response {
            if let Some(transport_failure) = backend_transport_failure(error) {
                warn!(
                    rpc = %self.rpc,
                    method = %self.web3_request.inner.method(),
                    transport_failure = %transport_failure,
                    elapsed_ms = start.elapsed().as_millis(),
                    "backend transport failed; delaying reuse"
                );
                self.delay_reuse_for(Duration::from_secs(1));
            }
        }

        // measure successes and errors
        // originally i thought we wouldn't want errors, but I think it's a more accurate number including all requests
        let latency = start.elapsed();

        // we used to fetch_sub the active_request count here, but sometimes the handle is dropped without request being called!

        trace!(
            "response from {} for {}: {:?}",
            self.rpc,
            self.web3_request,
            response,
        );

        // TODO: move this to a helper function?
        // true if we got a jsonrpc result. a jsonrpc error or other error is false.
        // TODO: counters for errors vs jsonrpc vs success?
        let response_is_success = match &response {
            Ok(jsonrpc::SingleResponse::Parsed(x, ..)) => {
                matches!(&x.payload, ResponsePayload::Success { .. })
            }
            Ok(jsonrpc::SingleResponse::Stream(..)) => true,
            Err(_) => false,
        };

        if response_is_success {
            // only track latency for successful requests
            tokio::spawn(async move {
                self.rpc.peak_latency.as_ref().unwrap().report(latency);
                self.rpc.median_latency.as_ref().unwrap().record(latency);

                // TODO: app-wide median and peak latency?
            });
        } else {
            // only save reverts for some types of calls
            // TODO: do something special for eth_sendRawTransaction too
            // we do **NOT** use self.error_handler here because it might have been modified
            let error_handler = self.error_handler();

            enum ResponseType {
                Error,
                Revert,
                RateLimited,
            }

            let response_type: ResponseType = match &response {
                Ok(jsonrpc::SingleResponse::Parsed(x, ..)) => match &x.payload {
                    ResponsePayload::Success { .. } => unreachable!(),
                    ResponsePayload::Error { error } => {
                        trace!(?error, "jsonrpc error data");

                        if let Some(history_error) =
                            history_error_for_request(&self.web3_request, error)
                        {
                            response = Err(history_error);
                            ResponseType::Error
                        } else if error.message.starts_with("execution reverted") {
                            ResponseType::Revert
                        } else if error.code == StatusCode::TOO_MANY_REQUESTS.as_u16() as i64 {
                            response = Err(Web3ProxyError::JsonRpcErrorData(error.clone()));
                            ResponseType::RateLimited
                        } else {
                            // TODO! THIS HAS TOO MANY FALSE POSITIVES! Theres another spot in the code that checks for things.
                            // if error.message.contains("limit") || error.message.contains("request") {
                            //     self.rate_limit_for(Duration::from_secs(1));
                            // }

                            match error.code {
                                -32000 => {
                                    if error.message.contains("MDBX_PANIC:") {
                                        response = Err(Web3ProxyError::MdbxPanic(
                                            self.connection_name(),
                                            error.message.clone(),
                                        ));
                                    } else {
                                        // TODO: regex?
                                        let archive_prefixes = [
                                            "header not found",
                                            "header for hash not found",
                                            "missing trie node",
                                        ];
                                        for prefix in archive_prefixes {
                                            if error.message.starts_with(prefix) {
                                                // TODO: what error?
                                                response = Err(Web3ProxyError::ArchiveRequired {
                                                    min: self.web3_request.min_block_needed(),
                                                    max: self.web3_request.max_block_needed(),
                                                });
                                                break;
                                            }
                                        }
                                    }

                                    ResponseType::Error
                                }
                                -32001 => {
                                    if error.message == "Exceeded the quota usage" {
                                        response =
                                            Err(Web3ProxyError::JsonRpcErrorData(error.clone()));
                                        ResponseType::RateLimited
                                    } else {
                                        ResponseType::Error
                                    }
                                }
                                -32005 => {
                                    if error.message == "rate limit exceeded" {
                                        response =
                                            Err(Web3ProxyError::JsonRpcErrorData(error.clone()));
                                        ResponseType::RateLimited
                                    } else {
                                        ResponseType::Error
                                    }
                                }
                                -32601 => {
                                    let error_msg = error.message.as_ref();

                                    // sometimes a provider does not support all rpc methods
                                    // we check other connections rather than returning the error
                                    // but sometimes the method is something that is actually unsupported,
                                    // so we save the response here to return it later

                                    // some providers look like this
                                    if (error_msg.starts_with("the method")
                                        && error_msg.ends_with("is not available"))
                                        || error_msg == "Method not found"
                                    {
                                        let method = self.web3_request.inner.method().to_string();

                                        response =
                                            Err(Web3ProxyError::MethodNotFound(method.into()))
                                    }

                                    ResponseType::Error
                                }
                                _ => ResponseType::Error,
                            }
                        }
                    }
                },
                Ok(jsonrpc::SingleResponse::Stream(..)) => unreachable!(),
                Err(_) => ResponseType::Error,
            };

            if matches!(response_type, ResponseType::RateLimited) {
                // TODO: how long?
                self.rate_limit_for(Duration::from_secs(1));
            }

            match error_handler {
                RequestErrorHandler::DebugLevel => {
                    // TODO: think about this revert check more. sometimes we might want reverts logged so this needs a flag
                    if matches!(response_type, ResponseType::Revert) {
                        trace!(
                            rpc=%self.rpc,
                            %self.web3_request,
                            ?response,
                            "revert",
                        );
                    } else {
                        debug!(
                            rpc=%self.rpc,
                            %self.web3_request,
                            ?response,
                            "bad response",
                        );
                    }
                }
                RequestErrorHandler::InfoLevel => {
                    info!(
                        rpc=%self.rpc,
                        %self.web3_request,
                        ?response,
                        "bad response",
                    );
                }
                RequestErrorHandler::TraceLevel => {
                    trace!(
                        rpc=%self.rpc,
                        %self.web3_request,
                        ?response,
                        "bad response",
                    );
                }
                RequestErrorHandler::ErrorLevel => {
                    // TODO: only include params if not running in release mode
                    error!(
                        rpc=%self.rpc,
                        %self.web3_request,
                        ?response,
                        "bad response",
                    );
                }
                RequestErrorHandler::WarnLevel => {
                    // TODO: only include params if not running in release mode
                    warn!(
                        rpc=%self.rpc,
                        %self.web3_request,
                        ?response,
                        "bad response",
                    );
                }
            }
        }

        response
    }
}

#[cfg(test)]
mod tests {
    use super::{backend_transport_failure, history_error_for_request, OpenRequestHandle};
    use crate::errors::Web3ProxyError;
    use crate::jsonrpc::{JsonRpcErrorData, RequestOrMethod, SingleRequest, ValidatedRequest};
    use crate::rpcs::one::{RequestPermits, Web3Rpc};
    use axum::extract::State;
    use axum::http::header::CONTENT_TYPE;
    use axum::{routing::post, Router};
    use sonic_rs::{json, OwnedLazyValue};
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::sync::{mpsc, watch, Semaphore};
    use tokio::time::{timeout, Duration, Instant};

    #[derive(Clone)]
    struct HeldRequestState {
        started: mpsc::UnboundedSender<()>,
        release: Arc<Semaphore>,
    }

    async fn held_json_rpc_request(
        State(state): State<HeldRequestState>,
    ) -> ([(axum::http::HeaderName, &'static str); 1], &'static str) {
        state.started.send(()).unwrap();
        state.release.acquire().await.unwrap().forget();

        (
            [(CONTENT_TYPE, "application/json")],
            r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#,
        )
    }

    async fn held_json_rpc_batch(
        State(state): State<HeldRequestState>,
    ) -> ([(axum::http::HeaderName, &'static str); 1], &'static str) {
        state.started.send(()).unwrap();
        state.release.acquire().await.unwrap().forget();

        (
            [(CONTENT_TYPE, "application/json")],
            r#"[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"result":"0x1"}]"#,
        )
    }

    fn request(method: &'static str) -> Arc<ValidatedRequest> {
        Arc::new(ValidatedRequest {
            inner: RequestOrMethod::Method(method.into(), 0),
            ..Default::default()
        })
    }

    #[test]
    fn geth_pruned_log_history_error_retries_only_log_requests() {
        let error = JsonRpcErrorData {
            code: 4444,
            message: "pruned history unavailable".into(),
            data: None,
        };

        assert!(matches!(
            history_error_for_request(&request("eth_getLogs"), &error),
            Some(Web3ProxyError::LogHistoryRequired { .. })
        ));
        assert!(history_error_for_request(&request("eth_getCode"), &error).is_none());

        let different_error = JsonRpcErrorData {
            code: 4444,
            message: "different backend error".into(),
            data: None,
        };
        assert!(history_error_for_request(&request("eth_getLogs"), &different_error).is_none());
    }

    #[tokio::test]
    async fn backend_rate_limit_is_returned_as_a_retryable_request_error() {
        let router = Router::new().route(
            "/",
            post(|| async {
                (
                    [(CONTENT_TYPE, "application/json")],
                    r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"rate limit exceeded"}}"#,
                )
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (hard_limit_until, _) = watch::channel(Instant::now());
        let rpc = Arc::new(Web3Rpc {
            name: "rate-limited".into(),
            http_client: Some(reqwest::Client::new()),
            http_url: Some(format!("http://{address}").parse().unwrap()),
            hard_limit_until: Some(hard_limit_until),
            ..Default::default()
        });
        let request = Arc::new(ValidatedRequest {
            inner: RequestOrMethod::Request(
                SingleRequest::new(1.into(), "eth_getLogs".into(), json!([])).unwrap(),
            ),
            ..Default::default()
        });

        let response = OpenRequestHandle::new(request, rpc, None)
            .await
            .request::<Arc<OwnedLazyValue>>()
            .await;

        assert!(matches!(
            response,
            Err(Web3ProxyError::JsonRpcErrorData(error))
                if error.code == -32005 && error.message == "rate limit exceeded"
        ));
        server.abort();
    }

    #[tokio::test]
    async fn backend_transport_failure_temporarily_limits_the_backend() {
        let (hard_limit_until, hard_limit_receiver) = watch::channel(Instant::now());
        let rpc = Arc::new(Web3Rpc {
            name: "unreachable".into(),
            http_client: Some(reqwest::Client::new()),
            http_url: Some("http://127.0.0.1:1".parse().unwrap()),
            hard_limit_until: Some(hard_limit_until),
            ..Default::default()
        });
        let request = Arc::new(ValidatedRequest {
            inner: RequestOrMethod::Request(
                SingleRequest::new(1.into(), "eth_call".into(), json!([])).unwrap(),
            ),
            ..Default::default()
        });

        let response = OpenRequestHandle::new(request, rpc, None)
            .await
            .request::<Arc<OwnedLazyValue>>()
            .await;

        assert!(response.is_err());
        assert!(
            *hard_limit_receiver.borrow() > Instant::now(),
            "a transport failure should delay reuse of the failing backend"
        );
    }

    #[tokio::test]
    async fn backend_transport_diagnostic_classifies_connect_without_exposing_the_url() {
        let secret_url = "http://127.0.0.1:1/private-backend-token";
        let error = reqwest::Client::new()
            .get(secret_url)
            .send()
            .await
            .expect_err("the closed local port must refuse the connection");

        let diagnostic = backend_transport_failure(&Web3ProxyError::Reqwest(error))
            .expect("reqwest failures must have transport diagnostics")
            .to_string();

        assert_eq!(diagnostic, "http_connect");
        assert!(!diagnostic.contains("private-backend-token"));
    }

    #[tokio::test]
    async fn backend_request_concurrency_never_exceeds_its_permit_limit() {
        let (started_sender, mut started_receiver) = mpsc::unbounded_channel();
        let release = Arc::new(Semaphore::new(0));
        let router = Router::new()
            .route("/", post(held_json_rpc_request))
            .with_state(HeldRequestState {
                started: started_sender,
                release: release.clone(),
            });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (hard_limit_until, _) = watch::channel(Instant::now());
        let rpc = Arc::new(Web3Rpc {
            name: "concurrency-limited".into(),
            http_client: Some(reqwest::Client::new()),
            http_url: Some(format!("http://{address}").parse().unwrap()),
            hard_limit_until: Some(hard_limit_until),
            request_permits: RequestPermits::new(2, 2),
            ..Default::default()
        });

        let mut requests = Vec::new();
        for _ in 0..3 {
            let rpc = rpc.clone();
            let request = Arc::new(ValidatedRequest {
                inner: RequestOrMethod::Request(
                    SingleRequest::new(1.into(), "eth_call".into(), json!([])).unwrap(),
                ),
                ..Default::default()
            });
            requests.push(tokio::spawn(async move {
                OpenRequestHandle::new(request, rpc, None)
                    .await
                    .request::<Arc<OwnedLazyValue>>()
                    .await
            }));
        }

        timeout(Duration::from_secs(1), started_receiver.recv())
            .await
            .expect("first backend request should start");
        timeout(Duration::from_secs(1), started_receiver.recv())
            .await
            .expect("second backend request should start");
        assert!(
            timeout(Duration::from_millis(100), started_receiver.recv())
                .await
                .is_err(),
            "a third backend request started without a permit"
        );

        release.add_permits(2);
        timeout(Duration::from_secs(1), started_receiver.recv())
            .await
            .expect("third backend request should start after a permit is released");
        release.add_permits(1);

        for request in requests {
            request.await.unwrap().unwrap();
        }
        server.abort();
    }

    #[tokio::test]
    async fn backend_batch_packets_fill_available_concurrency() {
        let (started_sender, mut started_receiver) = mpsc::unbounded_channel();
        let release = Arc::new(Semaphore::new(0));
        let router = Router::new()
            .route("/", post(held_json_rpc_batch))
            .with_state(HeldRequestState {
                started: started_sender,
                release: release.clone(),
            });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (hard_limit_until, _) = watch::channel(Instant::now());
        let rpc = Arc::new(Web3Rpc {
            name: "batch-concurrency-limited".into(),
            http_client: Some(reqwest::Client::new()),
            http_url: Some(format!("http://{address}").parse().unwrap()),
            hard_limit_until: Some(hard_limit_until),
            request_permits: RequestPermits::new(4, 2),
            ..Default::default()
        });
        let request = Arc::new(ValidatedRequest::default());
        let requests = (1..=4)
            .map(|id| SingleRequest::new(id.into(), "eth_call".into(), json!([])).unwrap())
            .collect::<Vec<_>>();

        let batch = tokio::spawn(async move {
            OpenRequestHandle::new(request, rpc, None)
                .await
                .request_batch(&requests)
                .await
        });

        timeout(Duration::from_secs(1), started_receiver.recv())
            .await
            .expect("first backend batch packet should start");
        timeout(Duration::from_secs(1), started_receiver.recv())
            .await
            .expect("second backend batch packet should use the remaining permits");
        release.add_permits(2);

        let responses = batch.await.unwrap().unwrap();
        assert_eq!(responses.len(), 4);
        server.abort();
    }
}
