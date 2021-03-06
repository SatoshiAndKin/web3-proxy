use axum::{http::StatusCode, response::IntoResponse, Json};
use serde_json::value::RawValue;

use crate::jsonrpc::JsonRpcForwardedResponse;

pub async fn handler_404() -> impl IntoResponse {
    let err = anyhow::anyhow!("nothing to see here");

    handle_anyhow_error(Some(StatusCode::NOT_FOUND), None, err).await
}

/// handle errors by converting them into something that implements `IntoResponse`
/// TODO: use this. i can't get <https://docs.rs/axum/latest/axum/error_handling/index.html> to work
/// TODO: i think we want a custom result type instead. put the anyhow result inside. then `impl IntoResponse for CustomResult`
pub async fn handle_anyhow_error(
    code: Option<StatusCode>,
    id: Option<Box<RawValue>>,
    err: anyhow::Error,
) -> impl IntoResponse {
    // TODO: we might have an id. like if this is for rate limiting, we can use it
    let id = id.unwrap_or_else(|| RawValue::from_string("null".to_string()).unwrap());

    let err = JsonRpcForwardedResponse::from_anyhow_error(err, id);

    // TODO: logs here are too verbose. emit a stat
    // warn!("Responding with error: {:?}", err);

    let code = code.unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    (code, Json(err))
}
