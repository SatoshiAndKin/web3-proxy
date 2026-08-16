//! Public HTTP JSON-RPC entrypoints.

use super::request_id::RequestId;
use super::rpc_proxy_ws::ProxyMode;
use crate::errors::{RequestForError, Web3ProxyError};
use crate::{app::App, jsonrpc::JsonRpcRequestEnum};
use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::State;
use axum::http::{header, HeaderMap};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use axum_macros::debug_handler;
use itertools::Itertools;
use std::sync::Arc;

#[debug_handler]
pub async fn proxy_web3_rpc(
    State(app): State<Arc<App>>,
    Extension(RequestId(request_id)): Extension<RequestId>,
    headers: HeaderMap,
    payload: Result<Bytes, BytesRejection>,
) -> Response {
    let payload = parse_payload(&headers, payload);
    proxy(app, payload, ProxyMode::Best, request_id)
        .await
        .unwrap_or_else(|response| *response)
}

#[debug_handler]
pub async fn fastest_proxy_web3_rpc(
    State(app): State<Arc<App>>,
    Extension(RequestId(request_id)): Extension<RequestId>,
    headers: HeaderMap,
    payload: Result<Bytes, BytesRejection>,
) -> Response {
    let payload = parse_payload(&headers, payload);
    proxy(app, payload, ProxyMode::Fastest(0), request_id)
        .await
        .unwrap_or_else(|response| *response)
}

#[debug_handler]
pub async fn versus_proxy_web3_rpc(
    State(app): State<Arc<App>>,
    Extension(RequestId(request_id)): Extension<RequestId>,
    headers: HeaderMap,
    payload: Result<Bytes, BytesRejection>,
) -> Response {
    let payload = parse_payload(&headers, payload);
    proxy(app, payload, ProxyMode::Versus, request_id)
        .await
        .unwrap_or_else(|response| *response)
}

fn parse_payload(
    headers: &HeaderMap,
    payload: Result<Bytes, BytesRejection>,
) -> Result<JsonRpcRequestEnum, Web3ProxyError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    let valid_content_type = content_type.is_some_and(|value| {
        value.split_once('/').is_some_and(|(kind, subtype)| {
            kind.eq_ignore_ascii_case("application")
                && (subtype.eq_ignore_ascii_case("json")
                    || subtype
                        .get(subtype.len().saturating_sub(5)..)
                        .is_some_and(|suffix| suffix.eq_ignore_ascii_case("+json")))
        })
    });

    if !valid_content_type {
        return Err(Web3ProxyError::JsonRequestBody(
            "expected an application/json content type".into(),
        ));
    }

    let payload = payload.map_err(|error| {
        Web3ProxyError::JsonRequestBody(format!("unable to read request body: {error}").into())
    })?;

    sonic_rs::from_slice(&payload).map_err(Web3ProxyError::JsonRequest)
}

async fn proxy(
    app: Arc<App>,
    payload: Result<JsonRpcRequestEnum, Web3ProxyError>,
    proxy_mode: ProxyMode,
    request_id: String,
) -> Result<Response, Box<Response>> {
    let payload = payload
        .map_err(|error| Box::new(error.into_response_with_id(None, None::<RequestForError>)))?;

    let first_id = payload.first_id();
    if let Some(error_id) = payload.validate() {
        let error = Web3ProxyError::BadRequest("request failed validation".into());
        return Err(Box::new(
            error.into_response_with_id(Some(error_id), None::<RequestForError>),
        ));
    }

    let (status_code, response, rpcs) = app
        .proxy_web3_rpc(proxy_mode, payload, Some(request_id))
        .await
        .map_err(|error| {
            Box::new(error.into_response_with_id(first_id, None::<RequestForError>))
        })?;

    let mut response = (status_code, response).into_response();
    let mut backup_used = false;
    let rpcs = rpcs
        .into_iter()
        .map(|rpc| {
            backup_used |= rpc.backup;
            rpc.name.clone()
        })
        .join(",");

    response.headers_mut().insert(
        "X-W3P-BACKEND-RPCS",
        rpcs.parse().expect("backend RPC names must form a header"),
    );
    response.headers_mut().insert(
        "X-W3P-BACKUP-RPC",
        backup_used
            .to_string()
            .parse()
            .expect("the backup flag must form a header"),
    );

    Ok(response)
}
