//! Used by admins for health checks and inspecting global statistics.
//!
//! For ease of development, users can currently access these endponts.
//! They will eventually move to another port.

use crate::{
    app::{App, APP_USER_AGENT},
    errors::Web3ProxyError,
};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use axum_client_ip::InsecureClientIp;
use axum_macros::debug_handler;
use hashbrown::HashMap;
use http::HeaderMap;
use moka::future::Cache;
use once_cell::sync::Lazy;
use serde::{ser::SerializeStruct, Serialize};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::time::timeout;

static HEALTH_OK: Lazy<Bytes> = Lazy::new(|| Bytes::from("OK\n"));
static HEALTH_NOT_OK: Lazy<Bytes> = Lazy::new(|| Bytes::from(":(\n"));

static BACKUPS_NEEDED_TRUE: Lazy<Bytes> = Lazy::new(|| Bytes::from("true\n"));
static BACKUPS_NEEDED_FALSE: Lazy<Bytes> = Lazy::new(|| Bytes::from("false\n"));

static CONTENT_TYPE_JSON: &str = "application/json";
static CONTENT_TYPE_PLAIN: &str = "text/plain";

#[debug_handler]
pub async fn debug_request(
    State(app): State<Arc<App>>,
    ip: InsecureClientIp,
    headers: HeaderMap,
) -> impl IntoResponse {
    let (_, _, status) = _status(app).await;

    let status: serde_json::Value = serde_json::from_slice(&status).unwrap();

    let headers: HashMap<_, _> = headers
        .into_iter()
        .filter_map(|(k, v)| {
            if let Some(k) = k {
                let k = k.to_string();

                let v = if let Ok(v) = std::str::from_utf8(v.as_bytes()) {
                    v.to_string()
                } else {
                    format!("{:?}", v)
                };

                Some((k, v))
            } else {
                None
            }
        })
        .collect();

    let x = json!({
        "ip": format!("{:?}", ip),
        "status": status,
        "headers": headers,
    });

    Json(x)
}

/// Health check page for load balancers to use.
#[debug_handler]
pub async fn health(State(app): State<Arc<App>>) -> Result<impl IntoResponse, Web3ProxyError> {
    let (code, content_type, body) = timeout(Duration::from_secs(1), _health(app)).await?;

    let x = Response::builder()
        .status(code)
        .header("content-type", content_type)
        .body(Body::from(body))
        .unwrap();

    Ok(x)
}

#[inline]
async fn _health(app: Arc<App>) -> (StatusCode, &'static str, Bytes) {
    if app.balanced_rpcs.synced() {
        (StatusCode::OK, CONTENT_TYPE_PLAIN, HEALTH_OK.clone())
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            CONTENT_TYPE_PLAIN,
            HEALTH_NOT_OK.clone(),
        )
    }
}

/// Easy alerting if backup servers are in use.
#[debug_handler]
pub async fn backups_needed(
    State(app): State<Arc<App>>,
) -> Result<impl IntoResponse, Web3ProxyError> {
    let (code, content_type, body) = timeout(Duration::from_secs(1), _backups_needed(app)).await?;

    let x = Response::builder()
        .status(code)
        .header("content-type", content_type)
        .body(Body::from(body))
        .unwrap();

    Ok(x)
}

#[inline]
async fn _backups_needed(app: Arc<App>) -> (StatusCode, &'static str, Bytes) {
    let code = {
        let consensus_rpcs = app.balanced_rpcs.watch_ranked_rpcs.borrow().clone();

        if let Some(ref consensus_rpcs) = consensus_rpcs {
            if consensus_rpcs.backups_needed {
                StatusCode::INTERNAL_SERVER_ERROR
            } else {
                StatusCode::OK
            }
        } else {
            // if no consensus, we still "need backups". we just don't have any. which is worse
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };

    if matches!(code, StatusCode::OK) {
        (code, CONTENT_TYPE_PLAIN, BACKUPS_NEEDED_FALSE.clone())
    } else {
        (code, CONTENT_TYPE_PLAIN, BACKUPS_NEEDED_TRUE.clone())
    }
}

/// Very basic status page.
///
/// TODO: replace this with proper stats and monitoring. frontend uses it for their public dashboards though
#[debug_handler]
pub async fn status(State(app): State<Arc<App>>) -> Result<impl IntoResponse, Web3ProxyError> {
    let (code, content_type, body) = timeout(Duration::from_secs(1), _status(app)).await?;

    let x = Response::builder()
        .status(code)
        .header("content-type", content_type)
        .body(Body::from(body))
        .unwrap();

    Ok(x)
}

#[inline]
async fn _status(app: Arc<App>) -> (StatusCode, &'static str, Bytes) {
    // TODO: get out of app.balanced_rpcs instead?
    let head_block = app.watch_consensus_head_receiver.borrow().clone();

    // TODO: what else should we include? CPU load and memory use.
    // TODO: the hostname is probably not going to change. only get once at the start?
    let body = json!({
        "balanced_rpcs": app.balanced_rpcs,
        "bundler_4337_rpcs": app.bundler_4337_rpcs,
        "caches": [
            MokaCacheSerializer(&app.ip_semaphores),
        ],
        "chain_id": app.config.chain_id,
        "head_block_hash": head_block.as_ref().map(|x| x.hash()),
        "head_block_num": head_block.as_ref().map(|x| x.number()),
        "hostname": app.hostname,
        "pending_txid_firehose": app.pending_txid_firehose,
        "private_rpcs": app.protected_rpcs,
        "uptime": app.start.elapsed().as_secs(),
        "version": APP_USER_AGENT,
    });

    let body = body.to_string().into_bytes();

    let body = Bytes::from(body);

    let code = if app.balanced_rpcs.synced() {
        StatusCode::OK
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };

    (code, CONTENT_TYPE_JSON, body)
}

pub struct MokaCacheSerializer<'a, K, V>(pub &'a Cache<K, V>);

impl<'a, K, V> Serialize for MokaCacheSerializer<'a, K, V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("MokaCache", 3)?;

        state.serialize_field("entry_count", &self.0.entry_count())?;
        state.serialize_field("name", &self.0.name())?;
        state.serialize_field("weighted_size", &self.0.weighted_size())?;

        state.end()
    }
}
