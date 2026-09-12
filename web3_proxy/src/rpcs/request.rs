use super::one::Web3Rpc;
use crate::errors::{Web3ProxyError, Web3ProxyResult};
use crate::jsonrpc::{
    self, JsonRpcErrorData, JsonRpcResultData, ParsedResponse, ResponsePayload, ValidatedRequest,
};
use alloy::providers::Provider;
use anyhow::Context;
use derive_more::From;
use reqwest::StatusCode;
use sonic_rs::JsonValueTrait;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::{timeout_at, Duration, Instant};
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
    /// An eligible backend has no free slot. Poll only after checking other backends.
    Busy(Pin<Box<dyn Future<Output = Web3ProxyResult<OpenRequestHandle>> + Send>>),
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
    request: BackendRequest,
    permit: OwnedSemaphorePermit,
}

/// Request metadata and shared response checks, independent of slot ownership.
struct BackendRequest {
    web3_request: Arc<ValidatedRequest>,
    error_handler: RequestErrorHandler,
    rpc: Arc<Web3Rpc>,
    allow_unhealthy: bool,
}

/// Holds one physical backend request slot through the complete response body.
#[derive(Debug)]
pub(crate) struct ActiveRequestGuard {
    rpc: Arc<Web3Rpc>,
    _permit: OwnedSemaphorePermit,
}

impl ActiveRequestGuard {
    fn new(rpc: &Arc<Web3Rpc>, permit: OwnedSemaphorePermit) -> Self {
        rpc.active_requests.fetch_add(1, atomic::Ordering::SeqCst);
        Self {
            rpc: rpc.clone(),
            _permit: permit,
        }
    }
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        self.rpc
            .active_requests
            .fetch_sub(1, atomic::Ordering::SeqCst);
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

impl std::fmt::Debug for BackendRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendRequest")
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

impl BackendRequest {
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

    fn check_submission(&self) -> Web3ProxyResult<()> {
        if self.web3_request.expired() {
            return Err(Web3ProxyError::Timeout(None));
        }
        if !self
            .rpc
            .can_submit(&self.web3_request, self.allow_unhealthy)
        {
            return Err(Web3ProxyError::NoHandleReady);
        }
        Ok(())
    }

    pub fn rate_limit_for(&self, duration: Duration) {
        if self.rpc.backup {
            debug!(?duration, "rate limited on {}!", self.rpc);
        } else {
            warn!(?duration, "rate limited on {}!", self.rpc);
        }
        self.delay_reuse_for(duration);
    }

    /// Just get the response from the provider without any extra handling.
    /// This lets us use the try operator which makes it much easier to read
    async fn _request<R: JsonRpcResultData + serde::Serialize>(
        &self,
    ) -> Web3ProxyResult<jsonrpc::SingleResponse<R>> {
        self.web3_request
            .response
            .lock()
            .backend_rpcs
            .push(self.rpc.clone());
        self.rpc
            .total_requests
            .fetch_add(1, atomic::Ordering::Relaxed);
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

            let bytes = response.bytes().await?;
            let response: ParsedResponse<R> = sonic_rs::from_slice(&bytes)?;
            Ok(response.into())
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
        response
    }

    fn check_transport_error(&self, error: &Web3ProxyError, start: Instant) {
        if let Some(transport_failure) = backend_transport_failure(error) {
            warn!(rpc = %self.rpc, method = %self.web3_request.inner.method(),
                transport_failure = %transport_failure, elapsed_ms = start.elapsed().as_millis(),
                "backend transport failed; delaying reuse");
            self.delay_reuse_for(Duration::from_secs(1));
        }
    }

    fn check_response<R: JsonRpcResultData>(
        &self,
        mut response: Web3ProxyResult<jsonrpc::SingleResponse<R>>,
        start: Instant,
    ) -> Web3ProxyResult<jsonrpc::SingleResponse<R>> {
        if let Err(error) = &response {
            self.check_transport_error(error, start);
        }
        // Validate identity before recording a successful backend timing.
        response = response.and_then(|response| {
            let jsonrpc::SingleResponse::Parsed(parsed) = &response;
            let actual: sonic_rs::Value = sonic_rs::from_str(&sonic_rs::to_string(&parsed.id)?)?;
            let expected: sonic_rs::Value =
                sonic_rs::from_str(&sonic_rs::to_string(&self.web3_request.id())?)?;
            if actual != expected {
                return Err(anyhow::anyhow!("backend response ID mismatch").into());
            }
            Ok(response)
        });
        if self.web3_request.expired() {
            return Err(Web3ProxyError::Timeout(None));
        }
        let latency = start.elapsed();

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
            Err(_) => false,
        };

        if response_is_success {
            // only track latency for successful requests
            let rpc = self.rpc.clone();
            tokio::spawn(async move {
                rpc.peak_latency.as_ref().unwrap().report(latency);
                rpc.median_latency.as_ref().unwrap().record(latency);

                // TODO: app-wide median and peak latency?
            });
        } else {
            // only save reverts for some types of calls
            // we do **NOT** use self.error_handler here because it might have been modified
            let error_handler = self.error_handler;

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

                        if self.web3_request.inner.method() == "eth_sendRawTransaction"
                            && error.is_known_transaction()
                        {
                            // Preserve the reply for App's decoded transaction hash
                            // and pending notification, without a success sample.
                            ResponseType::Error
                        } else if let Some(history_error) =
                            history_error_for_request(&self.web3_request, error)
                        {
                            response = Err(history_error);
                            ResponseType::Error
                        } else if error.is_execution_revert() {
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
                                -32603 => {
                                    response = Err(Web3ProxyError::JsonRpcErrorData(error.clone()));
                                    ResponseType::Error
                                }
                                -32000 => {
                                    if error.message.contains("MDBX_PANIC:") {
                                        response = Err(Web3ProxyError::MdbxPanic(
                                            self.rpc.name.clone(),
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

impl OpenRequestHandle {
    pub(super) fn new(
        web3_request: Arc<ValidatedRequest>,
        rpc: Arc<Web3Rpc>,
        error_handler: Option<RequestErrorHandler>,
        allow_unhealthy: bool,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            request: BackendRequest {
                web3_request,
                rpc,
                error_handler: error_handler.unwrap_or_default(),
                allow_unhealthy,
            },
            permit,
        }
    }

    pub fn connection_name(&self) -> String {
        self.request.rpc.name.clone()
    }
    pub fn clone_connection(&self) -> Arc<Web3Rpc> {
        self.request.rpc.clone()
    }
    pub fn batch_capacity(&self) -> usize {
        self.request.rpc.request_permits.max_concurrent_requests()
    }
    pub fn batch_size(&self) -> usize {
        if self.supports_batch() {
            self.request.rpc.request_permits.max_backend_batch_items()
        } else {
            1
        }
    }
    pub fn supports_batch(&self) -> bool {
        self.request.rpc.supports_batch()
    }

    /// Hold the backend permit through the complete body read and validation.
    pub async fn request<R: JsonRpcResultData>(
        self,
    ) -> Web3ProxyResult<jsonrpc::SingleResponse<R>> {
        let Self { request, permit } = self;
        request.check_submission()?;
        let _active = ActiveRequestGuard::new(&request.rpc, permit);
        let start = Instant::now();
        let response = timeout_at(request.web3_request.expire_at(), request._request())
            .await
            .unwrap_or_else(|error| Err(error.into()));
        request.check_response(response, start)
    }

    /// Send one physical request, batching calls when the transport supports it.
    /// Failures leave unfinished calls in the scheduler's queue for recovery.
    pub async fn request_batch(
        self,
        requests: &[Arc<ValidatedRequest>],
    ) -> Web3ProxyResult<Vec<Web3ProxyResult<ParsedResponse>>> {
        let deadline = requests
            .iter()
            .map(|request| request.expire_at())
            .min()
            .context("backend packet must contain requests")?;
        let Self {
            mut request,
            permit,
        } = self;
        request.web3_request = requests[0].clone();
        if !request.rpc.supports_batch() {
            if requests.len() != 1 {
                return Err(
                    anyhow::anyhow!("backend transport requires one call per request").into(),
                );
            }
            return match (Self { request, permit }).request().await? {
                jsonrpc::SingleResponse::Parsed(response) => Ok(vec![Ok(response)]),
            };
        }
        request.check_submission()?;
        if Instant::now() >= deadline {
            return Err(Web3ProxyError::Timeout(None));
        }
        let _active = ActiveRequestGuard::new(&request.rpc, permit);
        let started_at = Instant::now();
        let result = timeout_at(deadline, async {
            let client = request
                .rpc
                .http_client
                .as_ref()
                .context("backend batch requires HTTP")?;
            let url = request
                .rpc
                .http_url
                .clone()
                .context("backend batch requires HTTP URL")?;
            // A packet can wait behind another request until its deadline.
            if Instant::now() >= deadline {
                return Err(Web3ProxyError::Timeout(None));
            }
            let packet = requests
                .iter()
                .enumerate()
                .map(|(index, request)| {
                    let mut call = request
                        .inner
                        .jsonrpc_request()
                        .expect("batch request contains JSON-RPC data")
                        .clone();
                    call.id =
                        sonic_rs::to_lazyvalue(&(index as u64 + 1)).expect("numeric IDs serialize");
                    call
                })
                .collect::<Vec<_>>();
            let body = sonic_rs::to_vec(&packet)?;
            for item in requests {
                item.response.lock().backend_rpcs.push(request.rpc.clone());
            }
            request
                .rpc
                .total_requests
                .fetch_add(requests.len(), atomic::Ordering::Relaxed);
            request
                .rpc
                .backend_batch_requests
                .fetch_add(1, atomic::Ordering::Relaxed);
            let response = client
                .post(url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body)
                .send()
                .await?;
            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                request.rate_limit_for(Duration::from_secs(1));
            }
            let bytes = response.error_for_status()?.bytes().await?;
            let responses: Vec<ParsedResponse> = sonic_rs::from_slice(&bytes)?;
            let mut matched: Vec<Option<ParsedResponse>> =
                (0..requests.len()).map(|_| None).collect();
            for response in responses {
                let id = response
                    .id
                    .as_u64()
                    .context("invalid backend batch response ID")?;
                let slot = id
                    .checked_sub(1)
                    .and_then(|id| usize::try_from(id).ok())
                    .and_then(|index| matched.get_mut(index))
                    .context("unknown backend batch response ID")?;
                if slot.replace(response).is_some() {
                    return Err(anyhow::anyhow!("duplicate backend batch response ID").into());
                }
            }
            if matched.iter().any(Option::is_none) {
                return Err(anyhow::anyhow!("missing backend batch response ID").into());
            }
            Ok(matched
                .into_iter()
                .zip(requests)
                .map(|(response, item)| {
                    let mut response = response.expect("all IDs were matched");
                    response.id = item.id();
                    let handle = BackendRequest {
                        web3_request: item.clone(),
                        error_handler: request.error_handler,
                        rpc: request.rpc.clone(),
                        allow_unhealthy: request.allow_unhealthy,
                    };
                    match handle.check_response(Ok(response.into()), started_at) {
                        Ok(jsonrpc::SingleResponse::Parsed(response)) => Ok(response),
                        Err(error) => Err(error),
                    }
                })
                .collect())
        })
        .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => Err(error.into()),
        };
        if Instant::now() >= deadline {
            return Err(Web3ProxyError::Timeout(None));
        }
        if let Err(error) = &result {
            request.check_transport_error(error, started_at);
            if !matches!(error, Web3ProxyError::Timeout(_)) {
                // Invalid IDs and rejected batch bodies are backend protocol failures too.
                request.delay_reuse_for(Duration::from_secs(1));
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::{backend_transport_failure, history_error_for_request};
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
        let request = ValidatedRequest::new_internal("eth_getLogs".into(), &json!([]), None, None)
            .await
            .unwrap();

        let response = rpc
            .wait_for_request_handle(&request, None, true)
            .await
            .unwrap()
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
        let request = ValidatedRequest::new_internal("eth_call".into(), &json!([]), None, None)
            .await
            .unwrap();

        let response = rpc
            .wait_for_request_handle(&request, None, true)
            .await
            .unwrap()
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
            let request = ValidatedRequest::new_internal("eth_call".into(), &json!([]), None, None)
                .await
                .unwrap();
            requests.push(tokio::spawn(async move {
                rpc.wait_for_request_handle(&request, None, true)
                    .await
                    .unwrap()
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
            request_permits: RequestPermits::new(2, 2),
            peak_latency: Some(latency::PeakEwmaLatency::spawn(
                Duration::from_secs(15),
                100,
                Duration::from_secs(1),
            )),
            median_latency: Some(latency::RollingQuantileLatency::spawn_median(100).await),
            ..Default::default()
        });
        let requests = (1..=4)
            .map(|id| {
                Arc::new(ValidatedRequest {
                    inner: RequestOrMethod::Request(
                        SingleRequest::new(id.into(), "eth_call".into(), json!([])).unwrap(),
                    ),
                    expire_timeout: Duration::from_secs(5),
                    ..Default::default()
                })
            })
            .collect::<Vec<_>>();

        let batch = tokio::spawn(async move {
            futures::future::join_all(requests.chunks(2).map(|chunk| {
                let rpc = rpc.clone();
                async move {
                    rpc.wait_for_request_handle(&chunk[0], None, true)
                        .await
                        .unwrap()
                        .request_batch(chunk)
                        .await
                }
            }))
            .await
        });

        timeout(Duration::from_secs(1), started_receiver.recv())
            .await
            .expect("first backend batch packet should start");
        timeout(Duration::from_secs(1), started_receiver.recv())
            .await
            .expect("second backend batch packet should use the remaining permits");
        release.add_permits(2);

        let responses = batch
            .await
            .unwrap()
            .into_iter()
            .flat_map(Result::unwrap)
            .collect::<Vec<_>>();
        assert_eq!(responses.len(), 4);
        for response in responses {
            let response = response.unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&sonic_rs::to_string(&response).unwrap())
                    .unwrap()["result"]
                    .as_str(),
                Some("0x1")
            );
        }
        server.abort();
    }
}
