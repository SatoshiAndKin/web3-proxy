use super::one::Web3Rpc;
use crate::errors::{Web3ProxyError, Web3ProxyResult};
use crate::jsonrpc::{
    self, JsonRpcErrorData, JsonRpcResultData, ParsedResponse, ResponsePayload, ValidatedRequest,
};
use alloy::providers::Provider;
use anyhow::Context;
use derive_more::From;
use futures::Future;
use reqwest::StatusCode;
use std::pin::Pin;
use std::sync::atomic;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info, trace, warn, Level};

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

    pub fn rate_limit_for(&self, duration: Duration) {
        if self.rpc.backup {
            debug!(?duration, "rate limited on {}!", self.rpc);
        } else {
            warn!(?duration, "rate limited on {}!", self.rpc);
        }

        // TODO: use send_if_modified to be sure we only send if our value is greater
        self.rpc
            .hard_limit_until
            .as_ref()
            .unwrap()
            .send_replace(Instant::now() + duration);
    }

    /// Just get the response from the provider without any extra handling.
    /// This lets us use the try operator which makes it much easier to read
    async fn _request<R: JsonRpcResultData + serde::Serialize>(
        &self,
    ) -> Web3ProxyResult<jsonrpc::SingleResponse<R>> {
        if let Some(ipc_path) = self.rpc.ipc_path.as_ref() {
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
        }
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
                                        ResponseType::RateLimited
                                    } else {
                                        ResponseType::Error
                                    }
                                }
                                -32005 => {
                                    if error.message == "rate limit exceeded" {
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
    use super::history_error_for_request;
    use crate::errors::Web3ProxyError;
    use crate::jsonrpc::{JsonRpcErrorData, RequestOrMethod, ValidatedRequest};
    use std::sync::Arc;

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
}
