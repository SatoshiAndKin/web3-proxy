//! Public HTTP JSON-RPC entrypoints.

use super::authorization::ip_is_authorized;
use super::request_id::RequestId;
use super::rpc_proxy_ws::ProxyMode;
use crate::errors::{RequestForError, Web3ProxyError};
use crate::{app::App, jsonrpc::JsonRpcRequestEnum};
use axum::extract::rejection::JsonRejection;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use axum_client_ip::InsecureClientIp;
use axum_macros::debug_handler;
use itertools::Itertools;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

#[debug_handler]
pub async fn proxy_web3_rpc(
    State(app): State<Arc<App>>,
    InsecureClientIp(ip): InsecureClientIp,
    Extension(RequestId(request_id)): Extension<RequestId>,
    payload: Result<Json<JsonRpcRequestEnum>, JsonRejection>,
) -> Result<Response, Response> {
    proxy(app, &ip, payload, ProxyMode::Best, request_id).await
}

#[debug_handler]
pub async fn fastest_proxy_web3_rpc(
    State(app): State<Arc<App>>,
    InsecureClientIp(ip): InsecureClientIp,
    Extension(RequestId(request_id)): Extension<RequestId>,
    payload: Result<Json<JsonRpcRequestEnum>, JsonRejection>,
) -> Result<Response, Response> {
    proxy(app, &ip, payload, ProxyMode::Fastest(0), request_id).await
}

#[debug_handler]
pub async fn versus_proxy_web3_rpc(
    State(app): State<Arc<App>>,
    InsecureClientIp(ip): InsecureClientIp,
    Extension(RequestId(request_id)): Extension<RequestId>,
    payload: Result<Json<JsonRpcRequestEnum>, JsonRejection>,
) -> Result<Response, Response> {
    proxy(app, &ip, payload, ProxyMode::Versus, request_id).await
}

async fn proxy(
    app: Arc<App>,
    ip: &IpAddr,
    payload: Result<Json<JsonRpcRequestEnum>, JsonRejection>,
    proxy_mode: ProxyMode,
    request_id: String,
) -> Result<Response, Response> {
    let payload = payload
        .map_err(|error| {
            Web3ProxyError::from(error).into_response_with_id(None, None::<RequestForError>)
        })?
        .0;

    let first_id = payload.first_id();
    let authorization = ip_is_authorized(&app, ip, proxy_mode)
        .await
        .map_err(|error| error.into_response_with_id(first_id.clone(), None::<RequestForError>))?;
    let authorization = Arc::new(authorization);

    payload
        .tarpit_invalid(&app, &authorization, Duration::from_secs(5))
        .await?;

    let (status_code, response, rpcs) = app
        .proxy_web3_rpc(authorization, payload, Some(request_id))
        .await
        .map_err(|error| error.into_response_with_id(first_id, None::<RequestForError>))?;

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
