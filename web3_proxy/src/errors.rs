//! Utlities for logging errors for admins and displaying errors to users.

use crate::block_number::BlockNumOrHash;
use crate::jsonrpc::ResponseData;
use crate::jsonrpc::{self, JsonRpcErrorData, ParsedResponse, SingleRequest, ValidatedRequest};
use crate::rpcs::blockchain::BlockHeader;
use crate::rpcs::one::Web3Rpc;
use alloy::primitives::{B256, U64};
use axum::extract::ws::Message;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use derive_more::{Display, Error, From};
use http::header::InvalidHeaderValue;
use http::uri::InvalidUri;
use reqwest::header::ToStrError;
use serde::Serialize;
use sonic_rs::{json, OwnedLazyValue, Value};
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;
use tokio::{sync::AcquireError, task::JoinError};
use tracing::{debug, error, trace, warn};

pub type Web3ProxyResult<T> = Result<T, Web3ProxyError>;
// TODO: take "IntoResponse" instead of Response?
pub type Web3ProxyResponse = Web3ProxyResult<Response>;

impl From<Web3ProxyError> for Web3ProxyResult<()> {
    fn from(value: Web3ProxyError) -> Self {
        Err(value)
    }
}

#[derive(Debug, Display)]
#[display("{:?} > {:?}", from, to)]
pub struct RangeTooLargeError {
    pub from: BlockNumOrHash,
    pub to: BlockNumOrHash,
    pub requested: U64,
    pub allowed: U64,
}

#[derive(Debug, Display, Error, From)]
pub enum Web3ProxyError {
    #[error(ignore)]
    #[from(ignore)]
    AccessDenied(Cow<'static, str>),
    #[error(ignore)]
    Anyhow(anyhow::Error),
    Arc(Arc<Self>),
    #[from(ignore)]
    #[display("{:?} to {:?}", min, max)]
    ArchiveRequired {
        min: Option<U64>,
        max: Option<U64>,
    },
    #[error(ignore)]
    #[from(ignore)]
    BadRequest(Cow<'static, str>),
    #[error(ignore)]
    #[from(ignore)]
    BadResponse(Cow<'static, str>),
    BadRouting,
    AlloyTransport(alloy::transports::TransportError),
    #[display("{:?} < {}", head, requested)]
    #[from(ignore)]
    FarFutureBlock {
        head: Option<U64>,
        requested: U64,
    },
    GasEstimateNotU256,
    HdrRecord(hdrhistogram::errors::RecordError),
    HeaderToString(ToStrError),
    HttpUri(InvalidUri),
    #[display("{} > {}", min, max)]
    #[from(ignore)]
    InvalidBlockBounds {
        min: u64,
        max: u64,
    },
    InvalidHeaderValue(InvalidHeaderValue),
    Io(std::io::Error),
    JoinError(JoinError),
    #[from(ignore)]
    JsonRequest(sonic_rs::Error),
    #[error(ignore)]
    #[from(ignore)]
    JsonRequestBody(Cow<'static, str>),
    #[display("{:?}", _0)]
    #[error(ignore)]
    JsonRpcErrorData(JsonRpcErrorData),
    #[from(ignore)]
    #[display("{}", _0)]
    MdbxPanic(String, Cow<'static, str>),
    NoBlockNumberOrHash,
    NoBlocksKnown,
    NoConsensusHeadBlock,
    NoHandleReady,
    NoServersSynced,
    #[display("{}/{}", num_known, min_head_rpcs)]
    #[from(ignore)]
    NotEnoughRpcs {
        num_known: usize,
        min_head_rpcs: usize,
    },
    #[display("{}/{}", available, needed)]
    #[from(ignore)]
    NotEnoughSoftLimit {
        available: u32,
        needed: u32,
    },
    NotFound,
    #[error(ignore)]
    #[from(ignore)]
    MethodNotFound(Cow<'static, str>),
    #[error(ignore)]
    #[from(ignore)]
    #[display("{} @ {}", _0, _1)]
    OldHead(Arc<Web3Rpc>, BlockHeader),
    #[display("{:?} > {:?}", from, to)]
    RangeInvalid {
        from: BlockNumOrHash,
        to: BlockNumOrHash,
    },
    #[error(ignore)]
    #[from(ignore)]
    RangeTooLarge(Box<RangeTooLargeError>),
    Reqwest(reqwest::Error),
    SemaphoreAcquireError(AcquireError),
    Sonic(sonic_rs::Error),
    /// simple way to return an error message to the user and an anyhow to our logs
    #[display("{}, {}, {:?}", _0, _1, _2)]
    StatusCode(StatusCode, Cow<'static, str>, Option<Value>),
    /// TODO: what should be attached to the timout?
    #[display("{:?}", _0)]
    #[error(ignore)]
    Timeout(Option<Duration>),
    #[error(ignore)]
    UnknownBlockHash(B256),
    #[display("known: {known}, unknown: {unknown}")]
    #[error(ignore)]
    UnknownBlockNumber {
        known: U64,
        unknown: U64,
    },
    #[error(ignore)]
    UnhandledMethod(Cow<'static, str>),
    WatchRecvError(tokio::sync::watch::error::RecvError),
    WatchSendError,
    WebsocketOnly,
    #[display("{:?}, {}", _0, _1)]
    #[error(ignore)]
    WithContext(Option<Box<Web3ProxyError>>, Cow<'static, str>),
}

#[derive(Default, From, Serialize)]
pub enum RequestForError<'a> {
    /// sometimes we don't have a request object at all
    #[default]
    None,
    /// sometimes parsing the request fails. Give them the original string
    Unparsed(&'a str),
    /// sometimes we have json
    SingleRequest(&'a SingleRequest),
    // sometimes we have json for a batch of requests
    // Batch(&'a BatchRequest),
    /// assuming things went well, we have a validated request
    Validated(&'a ValidatedRequest),
}

impl Web3ProxyError {
    pub fn range_too_large(
        from: BlockNumOrHash,
        to: BlockNumOrHash,
        requested: U64,
        allowed: U64,
    ) -> Self {
        Self::RangeTooLarge(Box::new(RangeTooLargeError {
            from,
            to,
            requested,
            allowed,
        }))
    }

    pub fn as_json_response_parts<'a, R>(
        &self,
        id: OwnedLazyValue,
        request_for_error: Option<R>,
    ) -> (StatusCode, jsonrpc::SingleResponse)
    where
        R: Into<RequestForError<'a>>,
    {
        let (code, response_data) = self.as_response_parts(request_for_error);
        let response = jsonrpc::ParsedResponse::from_response_data(response_data, id);
        (code, response.into())
    }

    /// turn the error into an axum response.
    /// <https://www.jsonrpc.org/specification#error_object>
    /// TODO? change to `to_response_parts(self)`
    pub fn as_response_parts<'a, R>(
        &self,
        request_for_error: Option<R>,
    ) -> (StatusCode, ResponseData<Arc<OwnedLazyValue>>)
    where
        R: Into<RequestForError<'a>>,
    {
        let request_for_error: RequestForError<'_> =
            request_for_error.map(Into::into).unwrap_or_default();

        // TODO: include a unique request id in the data
        let (code, err): (StatusCode, JsonRpcErrorData) = match self {
            Self::AccessDenied(msg) => {
                // TODO: attach something to this trace. probably don't include much in the message though. don't want to leak creds by accident
                trace!(%msg, "access denied");
                (
                    StatusCode::FORBIDDEN,
                    JsonRpcErrorData {
                        message: format!("FORBIDDEN: {}", msg).into(),
                        code: StatusCode::FORBIDDEN.as_u16().into(),
                        data: Some(json!({
                            "request": request_for_error,
                        })),
                    },
                )
            }
            Self::ArchiveRequired { min, max } => {
                // TODO: attach something to this trace. probably don't include much in the message though. don't want to leak creds by accident
                trace!(?min, ?max, "archive node required");
                (
                    StatusCode::OK,
                    JsonRpcErrorData {
                        message: "Archive data required".into(),
                        code: StatusCode::OK.as_u16().into(),
                        data: Some(json!({
                            "request": request_for_error,
                            "min": min,
                            "max": max,
                        })),
                    },
                )
            }
            Self::Anyhow(err) => {
                error!(?err, "anyhow: {}", err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    JsonRpcErrorData {
                        // TODO: is it safe to expose all of our anyhow strings?
                        message: "INTERNAL SERVER ERROR".into(),
                        code: StatusCode::INTERNAL_SERVER_ERROR.as_u16().into(),
                        data: Some(json!({
                            "request": request_for_error,
                            "err": err.to_string(),
                        })),
                    },
                )
            }
            Self::Arc(err) => {
                return err.as_response_parts(Some(request_for_error));
            }
            Self::BadRequest(err) => {
                trace!(?err, "BAD_REQUEST");
                (
                    StatusCode::BAD_REQUEST,
                    JsonRpcErrorData {
                        message: "bad request".into(),
                        code: StatusCode::BAD_REQUEST.as_u16().into(),
                        data: Some(json!({
                            "request": request_for_error,
                            "err": err.to_string(),
                        })),
                    },
                )
            }
            Self::BadResponse(err) => {
                // TODO: think about this one more. Some upstreams send responses without an id.
                debug!(?err, "BAD_RESPONSE: {}", err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    JsonRpcErrorData {
                        message: "bad response".into(),
                        code: StatusCode::INTERNAL_SERVER_ERROR.as_u16().into(),
                        data: Some(json!({
                            "request": request_for_error,
                            "err": err.to_string(),
                        })),
                    },
                )
            }
            Self::BadRouting => {
                error!("BadRouting");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    JsonRpcErrorData {
                        message: "bad routing".into(),
                        code: StatusCode::INTERNAL_SERVER_ERROR.as_u16().into(),
                        data: Some(json!({
                            "request": request_for_error,
                        })),
                    },
                )
            }
            Self::AlloyTransport(err) => match JsonRpcErrorData::try_from(err) {
                Ok(err) => {
                    trace!(?err, "Alloy JSON-RPC error");
                    (StatusCode::OK, err)
                }
                Err(err) => {
                    warn!(?err, "Alloy transport error");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        JsonRpcErrorData {
                            message: "Alloy transport error".into(),
                            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16().into(),
                            data: Some(json!({
                                "request": request_for_error,
                                "err": err.to_string(),
                            })),
                        },
                    )
                }
            },
            Self::FarFutureBlock { head, requested } => {
                trace!(?head, ?requested, "FarFutureBlock");
                (
                    StatusCode::OK,
                    JsonRpcErrorData {
                        message: "requested block is too far in the future".into(),
                        code: (-32002).into(),
                        data: Some(json!({
                            "head": head,
                            "requested": requested,
                            "request": request_for_error,
                        })),
                    },
                )
            }
            // Self::JsonRpcForwardedError(x) => (StatusCode::OK, x),
            Self::GasEstimateNotU256 => {
                trace!("GasEstimateNotU256");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    JsonRpcErrorData {
                        message: "gas estimate result is not an U256".into(),
                        code: StatusCode::INTERNAL_SERVER_ERROR.as_u16().into(),
                        data: Some(json!({
                            "request": request_for_error,
                        })),
                    },
                )
            }
            Self::HdrRecord(err) => {
                warn!(?err, "HdrRecord");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    JsonRpcErrorData {
                        message: "hdr record error".into(),
                        code: StatusCode::INTERNAL_SERVER_ERROR.as_u16().into(),
                        data: Some(json!({
                            "request": request_for_error,
                            "err": err.to_string(),
                        })),
                    },
                )
            }
            Self::HeaderToString(err) => {
                trace!(?err, "HeaderToString");
                (
                    StatusCode::BAD_REQUEST,
                    JsonRpcErrorData {
                        message: "header to string error".into(),
                        code: StatusCode::BAD_REQUEST.as_u16().into(),
                        data: Some(json!({
                            "request": request_for_error,
                            "err": err.to_string(),
                        })),
                    },
                )
            }
            Self::HttpUri(err) => {
                trace!(?err, "HttpUri");
                (
                    StatusCode::BAD_REQUEST,
                    JsonRpcErrorData {
                        message: err.to_string().into(),
                        code: StatusCode::BAD_REQUEST.as_u16().into(),
                        data: Some(json!({
                            "request": request_for_error,
                            "err": err.to_string(),
                        })),
                    },
                )
            }
            Self::InvalidBlockBounds { min, max } => {
                trace!(%min, %max, "InvalidBlockBounds");
                (
                    StatusCode::OK,
                    JsonRpcErrorData {
                        message: "Invalid blocks bounds requested".into(),
                        code: StatusCode::BAD_REQUEST.as_u16().into(),
                        data: Some(json!({
                            "min": min,
                            "max": max,
                            "request": request_for_error,
                        })),
                    },
                )
            }
            Self::InvalidHeaderValue(err) => {
                trace!(?err, "InvalidHeaderValue");
                (
                    StatusCode::BAD_REQUEST,
                    JsonRpcErrorData {
                        message: "invalid header value".into(),
                        code: StatusCode::BAD_REQUEST.as_u16().into(),
                        data: Some(json!({
                            "request": request_for_error,
                            "err": err.to_string(),
                        })),
                    },
                )
            }
            Self::Io(err) => {
                warn!(?err, "std io");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    JsonRpcErrorData {
                        message: "std io".into(),
                        code: StatusCode::INTERNAL_SERVER_ERROR.as_u16().into(),
                        // TODO: is it safe to expose our io error strings?
                        data: Some(json!({
                            "request": request_for_error,
                            "err": err.to_string(),
                        })),
                    },
                )
            }
            Self::JoinError(err) => {
                let code = if err.is_cancelled() {
                    trace!(?err, "JoinError. likely shutting down");
                    StatusCode::BAD_GATEWAY
                } else {
                    warn!(?err, "JoinError");
                    StatusCode::INTERNAL_SERVER_ERROR
                };

                (
                    code,
                    JsonRpcErrorData {
                        // TODO: different messages of cancelled or not?
                        message: "Unable to complete request".into(),
                        code: code.as_u16().into(),
                        data: Some(json!({
                            "request": request_for_error,
                            "err": err.to_string(),
                        })),
                    },
                )
            }
            Self::JsonRequest(err) => {
                trace!(?err, "invalid JSON request");

                let (message, code) = if err.is_syntax() || err.is_eof() {
                    ("Parse error", -32700)
                } else {
                    ("Invalid Request", -32600)
                };

                // TODO: i feel like this should be a 401, but the spec seems to say its a 200
                (
                    StatusCode::OK,
                    JsonRpcErrorData {
                        message: message.into(),
                        code: code.into(),
                        data: Some(json!({
                            "request": request_for_error,
                            "err": err.to_string(),
                        })),
                    },
                )
            }
            Self::JsonRequestBody(err) => {
                trace!(%err, "invalid JSON request body");
                (
                    StatusCode::OK,
                    JsonRpcErrorData {
                        message: "Invalid Request".into(),
                        code: -32600,
                        data: Some(json!({
                            "request": request_for_error,
                            "err": err,
                        })),
                    },
                )
            }
            Self::JsonRpcErrorData(jsonrpc_error_data) => {
                // TODO: do this without clone? the Arc needed it though
                (StatusCode::OK, jsonrpc_error_data.clone())
            }
            Self::MdbxPanic(rpc_name, msg) => {
                error!(%msg, "mdbx panic");

                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    JsonRpcErrorData {
                        message: "mdbx panic".into(),
                        code: StatusCode::INTERNAL_SERVER_ERROR.as_u16().into(),
                        data: Some(json!({
                            "err": msg,
                            "request": request_for_error,
                            "rpc": rpc_name,
                        })),
                    },
                )
            }
            Self::MethodNotFound(method) => {
                warn!("MethodNotFound: {}", method);
                (
                    StatusCode::OK,
                    JsonRpcErrorData {
                        message: "Method not found".into(),
                        code: -32601,
                        data: Some(json!({
                            "method": method,
                            "extra": "this method is not currently supported.",
                        })),
                    },
                )
            }
            Self::NoBlockNumberOrHash => {
                warn!("NoBlockNumberOrHash");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    JsonRpcErrorData {
                        message: "Internal server error".into(),
                        code: StatusCode::INTERNAL_SERVER_ERROR.as_u16().into(),
                        data: Some(json!({
                            "err": "Blocks here must have a number or hash",
                            "extra": "you found a bug. please contact us if you see this and we can help figure out what happened. https://farcaster.xyz/flashprofits.eth",
                            "request": request_for_error,
                        })),
                    },
                )
            }
            Self::NoBlocksKnown => {
                error!("NoBlocksKnown");
                (
                    StatusCode::BAD_GATEWAY,
                    JsonRpcErrorData {
                        message: "no blocks known".into(),
                        code: StatusCode::BAD_GATEWAY.as_u16().into(),
                        data: Some(json!({
                            "request": request_for_error,
                        })),
                    },
                )
            }
            Self::NoConsensusHeadBlock => {
                error!("NoConsensusHeadBlock");
                (
                    StatusCode::BAD_GATEWAY,
                    JsonRpcErrorData {
                        message: "no consensus head block".into(),
                        code: StatusCode::BAD_GATEWAY.as_u16().into(),
                        data: Some(json!({
                            "request": request_for_error,
                        })),
                    },
                )
            }
            Self::NoHandleReady => {
                error!("NoHandleReady");
                (
                    StatusCode::BAD_GATEWAY,
                    JsonRpcErrorData {
                        message: "unable to retry for request handle".into(),
                        code: StatusCode::BAD_GATEWAY.as_u16().into(),
                        data: Some(json!({
                            "request": request_for_error,
                        })),
                    },
                )
            }
            Self::NoServersSynced => {
                warn!("NoServersSynced");
                (
                    StatusCode::BAD_GATEWAY,
                    JsonRpcErrorData {
                        message: "no servers synced".into(),
                        code: StatusCode::BAD_GATEWAY.as_u16().into(),
                        data: Some(json!({
                            "request": request_for_error,
                        })),
                    },
                )
            }
            Self::NotEnoughRpcs {
                num_known,
                min_head_rpcs,
            } => {
                error!(%num_known, %min_head_rpcs, "NotEnoughRpcs");
                (
                    StatusCode::BAD_GATEWAY,
                    JsonRpcErrorData {
                        message: "not enough rpcs connected".into(),
                        code: StatusCode::BAD_GATEWAY.as_u16().into(),
                        data: Some(json!({
                            "known": num_known,
                            "needed": min_head_rpcs,
                            "request": request_for_error,
                        })),
                    },
                )
            }
            Self::NotEnoughSoftLimit { available, needed } => {
                error!(available, needed, "NotEnoughSoftLimit");
                (
                    StatusCode::BAD_GATEWAY,
                    JsonRpcErrorData {
                        message: "not enough soft limit available".into(),
                        code: StatusCode::BAD_GATEWAY.as_u16().into(),
                        data: Some(json!({
                            "available": available,
                            "needed": needed,
                            "request": request_for_error,
                        })),
                    },
                )
            }
            Self::NotFound => {
                // TODO: emit a stat?
                // TODO: instead of an error, show a normal html page for 404?
                (
                    StatusCode::NOT_FOUND,
                    JsonRpcErrorData {
                        message: "not found!".into(),
                        code: StatusCode::NOT_FOUND.as_u16().into(),
                        data: None,
                    },
                )
            }
            Self::OldHead(rpc, old_head) => {
                warn!(?old_head, "{} is lagged", rpc);
                (
                    StatusCode::BAD_GATEWAY,
                    JsonRpcErrorData {
                        message: "RPC is lagged".into(),
                        code: StatusCode::BAD_REQUEST.as_u16().into(),
                        data: Some(json!({
                            "head": old_head,
                            "request": request_for_error,
                            "rpc": rpc.name,
                        })),
                    },
                )
            }
            Self::RangeInvalid { from, to } => {
                trace!(?from, ?to, "RangeInvalid");
                (
                    StatusCode::BAD_REQUEST,
                    JsonRpcErrorData {
                        message: "invalid block range given".into(),
                        code: StatusCode::BAD_REQUEST.as_u16().into(),
                        data: Some(json!({
                            "from": from,
                            "to": to,
                            "request": request_for_error,
                        })),
                    },
                )
            }
            Self::RangeTooLarge(range) => {
                let RangeTooLargeError {
                    from,
                    to,
                    requested,
                    allowed,
                } = range.as_ref();
                trace!(?from, ?to, %requested, %allowed, "RangeTooLarge");
                (
                    StatusCode::BAD_REQUEST,
                    JsonRpcErrorData {
                        message: "invalid block range given".into(),
                        code: StatusCode::BAD_REQUEST.as_u16().into(),
                        data: Some(json!({
                            "from": from,
                            "to": to,
                            "requested": requested,
                            "allowed": allowed,
                            "request": request_for_error,
                        })),
                    },
                )
            }
            Self::Reqwest(err) => {
                warn!(?err, "reqwest");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    JsonRpcErrorData {
                        message: "reqwest error!".into(),
                        code: StatusCode::INTERNAL_SERVER_ERROR.as_u16().into(),
                        data: Some(json!({
                            "err": err.to_string(),
                        })),
                    },
                )
            }
            Self::SemaphoreAcquireError(err) => {
                error!(?err, "semaphore acquire");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    JsonRpcErrorData {
                        // TODO: is it safe to expose all of our anyhow strings?
                        message: "semaphore acquire error".into(),
                        code: StatusCode::INTERNAL_SERVER_ERROR.as_u16().into(),
                        data: Some(json!({
                            "err": err.to_string(),
                        })),
                    },
                )
            }
            Self::Sonic(err) => {
                trace!(?err, "sonic json");
                (
                    StatusCode::BAD_REQUEST,
                    JsonRpcErrorData {
                        message: "de/serialization error!".into(),
                        code: StatusCode::BAD_REQUEST.as_u16().into(),
                        data: Some(json!({
                            "err": err.to_string(),
                        })),
                    },
                )
            }
            Self::StatusCode(status_code, err_msg, data) => {
                // different status codes should get different error levels. 500s should warn. 400s should stat
                let code = status_code.as_u16();
                if (500..600).contains(&code) {
                    warn!(?data, "server error {}: {}", code, err_msg);
                } else {
                    trace!(?data, "user error {}: {}", code, err_msg);
                }

                // TODO: would be great to do this without the cloning! Something blocked that and I didn't write a comment about it though
                (
                    *status_code,
                    JsonRpcErrorData {
                        message: err_msg.clone(),
                        code: code.into(),
                        data: data.clone(),
                    },
                )
            }
            Self::Timeout(x) => {
                let data = json!({
                    "duration": x.as_ref().map(|x| x.as_secs_f32()),
                    "request": request_for_error,
                });

                (
                    StatusCode::REQUEST_TIMEOUT,
                    JsonRpcErrorData {
                        message: "request timed out".into(),
                        code: StatusCode::REQUEST_TIMEOUT.as_u16().into(),
                        data: Some(data),
                    },
                )
            }
            Self::UnhandledMethod(method) => {
                unimplemented!(
                    "unhandled method ({}) should never be shown to a user",
                    method
                );
            }
            Self::UnknownBlockHash(hash) => {
                debug!(%hash, "UnknownBlockHash");
                (
                    StatusCode::OK,
                    JsonRpcErrorData {
                        message: "block hash not found".into(),
                        code: -32000,
                        data: Some(json!({
                            "hash": hash,
                            "request": request_for_error,
                        })),
                    },
                )
            }
            Self::UnknownBlockNumber { known, unknown } => {
                debug!(%known, %unknown, "UnknownBlockNumber");
                (
                    StatusCode::OK,
                    JsonRpcErrorData {
                        message: "block number not found".into(),
                        code: -32000,
                        data: Some(json!({
                            "unknown": unknown,
                            "known": known,
                            "request": request_for_error,
                        })),
                    },
                )
            }
            Self::WatchRecvError(err) => {
                error!(?err, "WatchRecvError");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    JsonRpcErrorData {
                        message: "watch recv error!".into(),
                        code: StatusCode::INTERNAL_SERVER_ERROR.as_u16().into(),
                        data: Some(json!({
                            "err": err.to_string(),
                        })),
                    },
                )
            }
            Self::WatchSendError => {
                error!("WatchSendError");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    JsonRpcErrorData {
                        message: "watch send error!".into(),
                        code: StatusCode::INTERNAL_SERVER_ERROR.as_u16().into(),
                        data: None,
                    },
                )
            }
            Self::WebsocketOnly => {
                trace!("WebsocketOnly. redirect_public_url not set");
                (
                    StatusCode::BAD_REQUEST,
                    JsonRpcErrorData {
                        message: "only websockets work here".into(),
                        code: StatusCode::BAD_REQUEST.as_u16().into(),
                        data: None,
                    },
                )
            }
            Self::WithContext(err, msg) => match err {
                Some(err) => {
                    warn!(?err, %msg, "error w/ context");
                    return err.as_response_parts(Some(request_for_error));
                }
                None => {
                    warn!(%msg, "error w/ context");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        JsonRpcErrorData {
                            message: msg.clone(),
                            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16().into(),
                            data: Some(json!({
                                "request": request_for_error,
                            })),
                        },
                    )
                }
            },
        };

        (code, ResponseData::from(err))
    }

    pub fn into_response_with_id<'a, R>(
        self,
        id: Option<OwnedLazyValue>,
        request_for_error: Option<R>,
    ) -> Response
    where
        R: Into<RequestForError<'a>>,
    {
        let (status_code, response_data) = self.as_response_parts(request_for_error);

        let id = id.unwrap_or_default();

        let response = ParsedResponse::from_response_data(response_data, id);

        jsonrpc::response::json_response(status_code, &response)
    }
}

impl From<tokio::time::error::Elapsed> for Web3ProxyError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout(None)
    }
}

impl IntoResponse for Web3ProxyError {
    #[inline]
    /// TODO: maybe we don't want this anymore. maybe we want to require a web3_request?
    fn into_response(self) -> Response {
        self.into_response_with_id(Default::default(), None::<RequestForError>)
    }
}

pub trait Web3ProxyErrorContext<T> {
    fn web3_context<S: Into<Cow<'static, str>>>(self, msg: S) -> Result<T, Web3ProxyError>;
}

impl<T> Web3ProxyErrorContext<T> for Option<T> {
    fn web3_context<S: Into<Cow<'static, str>>>(self, msg: S) -> Result<T, Web3ProxyError> {
        self.ok_or(Web3ProxyError::WithContext(None, msg.into()))
    }
}

impl<T, E> Web3ProxyErrorContext<T> for Result<T, E>
where
    E: Into<Web3ProxyError>,
{
    fn web3_context<S: Into<Cow<'static, str>>>(self, msg: S) -> Result<T, Web3ProxyError> {
        self.map_err(|err| Web3ProxyError::WithContext(Some(Box::new(err.into())), msg.into()))
    }
}

impl Web3ProxyError {
    pub fn into_message<'a, R>(
        self,
        id: Option<OwnedLazyValue>,
        request_for_error: Option<R>,
    ) -> Message
    where
        R: Into<RequestForError<'a>>,
    {
        let (_, err) = self.as_response_parts(request_for_error);

        let id = id.unwrap_or_default();

        let err = ParsedResponse::from_response_data(err, id);

        let msg = sonic_rs::to_string(&err).expect("errors should always serialize to json");

        // TODO: what about a binary message?
        Message::Text(msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::Web3ProxyError;
    use std::mem::size_of;

    #[test]
    fn web3_proxy_error_fits_result_error_budget() {
        const CLIPPY_ERROR_SIZE_THRESHOLD: usize = 128;
        let actual_size = size_of::<Web3ProxyError>();

        assert!(
            actual_size < CLIPPY_ERROR_SIZE_THRESHOLD,
            "Web3ProxyError must be smaller than {CLIPPY_ERROR_SIZE_THRESHOLD} bytes; actual size is {actual_size} bytes"
        );
    }
}
