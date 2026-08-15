//! Take a user's WebSocket JSON-RPC requests and either respond from local data or proxy the request to a backend rpc server.
//!
//! WebSockets are the preferred method of receiving requests, but not all clients have good support.

use super::authorization::{ip_is_authorized, Authorization};
use crate::errors::{RequestForError, Web3ProxyError, Web3ProxyResponse};
use crate::jsonrpc::{self, ParsedResponse, ValidatedRequest};
use crate::{app::App, errors::Web3ProxyResult, jsonrpc::SingleRequest};
use alloy_primitives::U64;
use axum::{
    extract::ws::{rejection::WebSocketUpgradeRejection, Message, WebSocket, WebSocketUpgrade},
    extract::State,
    response::{IntoResponse, Redirect},
};
use axum_client_ip::InsecureClientIp;
use axum_macros::debug_handler;
use futures::SinkExt;
use futures::{
    future::AbortHandle,
    stream::{SplitSink, SplitStream, StreamExt},
};
use hashbrown::HashMap;
use serde_json::json;
use std::net::IpAddr;
use std::str::from_utf8;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::select;
use tokio::sync::{broadcast, mpsc, OwnedSemaphorePermit, RwLock as AsyncRwLock};
use tracing::trace;

/// How to select backend servers for a request
#[derive(Copy, Clone, Debug, Default)]
pub enum ProxyMode {
    /// send to the "best" synced server. on error, try the next
    #[default]
    Best,
    /// send to all synced servers and return the fastest non-error response (reverts do not count as errors here)
    Fastest(usize),
    /// send to k servers and return the best response common between at least n servers
    Quorum(usize, usize),
    /// send to all servers for benchmarking. return the fastest non-error response
    Versus,
}

/// Public entrypoint for WebSocket JSON-RPC requests.
/// Queries a single server at a time
#[debug_handler]
pub async fn websocket_handler(
    State(app): State<Arc<App>>,
    InsecureClientIp(ip): InsecureClientIp,
    ws_upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Web3ProxyResponse {
    _websocket_handler(ProxyMode::Best, app, &ip, ws_upgrade).await
}

/// Public entrypoint for WebSocket JSON-RPC requests that uses all synced servers.
/// Queries all synced backends with every request! This might get expensive!
// #[debug_handler]
pub async fn fastest_websocket_handler(
    State(app): State<Arc<App>>,
    InsecureClientIp(ip): InsecureClientIp,
    ws_upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Web3ProxyResponse {
    // TODO: get the fastest number from the url params (default to 0/all)
    // TODO: config to disable this
    _websocket_handler(ProxyMode::Fastest(0), app, &ip, ws_upgrade).await
}

/// Public entrypoint for WebSocket JSON-RPC requests that uses all synced servers.
/// Queries **all** backends with every request! This might get expensive!
#[debug_handler]
pub async fn versus_websocket_handler(
    State(app): State<Arc<App>>,
    InsecureClientIp(ip): InsecureClientIp,
    ws_upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Web3ProxyResponse {
    // TODO: config to disable this
    _websocket_handler(ProxyMode::Versus, app, &ip, ws_upgrade).await
}

async fn _websocket_handler(
    proxy_mode: ProxyMode,
    app: Arc<App>,
    ip: &IpAddr,
    ws_upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Web3ProxyResponse {
    let authorization = ip_is_authorized(&app, ip, proxy_mode).await?;

    let authorization = Arc::new(authorization);

    match ws_upgrade {
        Ok(ws) => Ok(ws
            .on_upgrade(move |socket| proxy_web3_socket(app, authorization, socket))
            .into_response()),
        Err(_) => {
            if let Some(redirect) = &app.config.redirect_public_url {
                // this is not a websocket. redirect to a friendly page
                Ok(Redirect::permanent(redirect).into_response())
            } else {
                Err(Web3ProxyError::WebsocketOnly)
            }
        }
    }
}

async fn proxy_web3_socket(app: Arc<App>, authorization: Arc<Authorization>, socket: WebSocket) {
    // split the websocket so we can read and write concurrently
    let (ws_tx, ws_rx) = socket.split();

    let buffer = authorization.checks.max_concurrent_requests.unwrap_or(2048);

    // create a channel for our reader and writer can communicate. todo: benchmark different channels
    // TODO: this should be bounded. async blocking on too many messages would be fine
    let (response_sender, response_receiver) = mpsc::channel::<Message>(buffer);

    tokio::spawn(write_web3_socket(response_receiver, ws_tx));
    tokio::spawn(read_web3_socket(app, authorization, ws_rx, response_sender));
}

async fn websocket_proxy_web3_rpc(
    app: &Arc<App>,
    authorization: Arc<Authorization>,
    json_request: SingleRequest,
    response_sender: &mpsc::Sender<Message>,
    subscription_count: &AtomicU64,
    subscriptions: &AsyncRwLock<HashMap<U64, AbortHandle>>,
) -> Web3ProxyResult<jsonrpc::Response> {
    match &json_request.method[..] {
        "eth_subscribe" => {
            // todo!(this needs a permit)
            let web3_request = ValidatedRequest::new_with_app(
                app,
                authorization,
                None,
                None,
                json_request.into(),
                None,
                None,
            )
            .await?;

            // TODO: how can we subscribe with proxy_mode?
            match app
                .eth_subscribe(web3_request, subscription_count, response_sender.clone())
                .await
            {
                Ok((handle, response)) => {
                    if let jsonrpc::ResponsePayload::Success {
                        result: ref subscription_id,
                    } = response.payload
                    {
                        let mut x = subscriptions.write().await;

                        let key: U64 = serde_json::from_str(subscription_id.get()).unwrap();

                        x.insert(key, handle);
                    }

                    Ok(response.into())
                }
                Err(err) => Err(err),
            }
        }
        "eth_unsubscribe" => {
            // todo!(this needs a permit)
            let web3_request = ValidatedRequest::new_with_app(
                app,
                authorization,
                None,
                None,
                json_request.into(),
                None,
                None,
            )
            .await?;

            // sometimes we get a list, sometimes we get the id directly
            // check for the list first, then just use the whole thing
            let maybe_id = web3_request
                .inner
                .params()
                .get(0)
                .unwrap_or_else(|| web3_request.inner.params())
                .clone();

            let subscription_id: U64 = match serde_json::from_value::<U64>(maybe_id) {
                Ok(x) => x,
                Err(err) => {
                    return Err(Web3ProxyError::BadRequest(
                        format!("unexpected params given for eth_unsubscribe: {:?}", err).into(),
                    ));
                }
            };

            // TODO: is this the right response?
            let partial_response = {
                let mut x = subscriptions.write().await;
                match x.remove(&subscription_id) {
                    None => false,
                    Some(handle) => {
                        handle.abort();
                        true
                    }
                }
            };

            let response =
                jsonrpc::ParsedResponse::from_value(json!(partial_response), web3_request.id());

            let response = jsonrpc::SingleResponse::Parsed(response);

            web3_request.set_response(&response);
            let response = response.parsed().await.expect("Response already parsed");

            Ok(response.into())
        }
        _ => app
            .proxy_web3_rpc(authorization, json_request.into(), None)
            .await
            .map(|(_, response, _)| response),
    }
}

/// websockets support a few more methods than http clients
async fn handle_socket_payload(
    app: &Arc<App>,
    authorization: &Arc<Authorization>,
    payload: &str,
    response_sender: &mpsc::Sender<Message>,
    subscription_count: &AtomicU64,
    subscriptions: Arc<AsyncRwLock<HashMap<U64, AbortHandle>>>,
) -> Web3ProxyResult<(Message, Option<OwnedSemaphorePermit>)> {
    let (authorization, semaphore) = authorization.check_again(app).await?;

    // TODO: handle batched requests
    let (response_id, response) = match serde_json::from_str::<SingleRequest>(payload) {
        Ok(json_request) => {
            let request_id = json_request.id.clone();

            // TODO: move this to a seperate function so we can use the try operator
            let x = websocket_proxy_web3_rpc(
                app,
                authorization.clone(),
                json_request,
                response_sender,
                subscription_count,
                &subscriptions,
            )
            .await;

            (request_id, x)
        }
        Err(err) => (Default::default(), Err(err.into())),
    };

    let response_str = match response {
        Ok(x) => x.to_json_string().await?,
        Err(err) => {
            let (_, response_data) = err.as_response_parts(None::<RequestForError>);

            let response = ParsedResponse::from_response_data(response_data, response_id);

            serde_json::to_string(&response).expect("to_string should always work here")
        }
    };

    Ok((Message::Text(response_str.into()), semaphore))
}

async fn read_web3_socket(
    app: Arc<App>,
    authorization: Arc<Authorization>,
    mut ws_rx: SplitStream<WebSocket>,
    response_sender: mpsc::Sender<Message>,
) {
    let subscriptions = Arc::new(AsyncRwLock::new(HashMap::new()));
    let subscription_count = Arc::new(AtomicU64::new(1));

    let (close_sender, mut close_receiver) = broadcast::channel(1);

    loop {
        select! {
            msg = ws_rx.next() => {
                if let Some(Ok(msg)) = msg {
                    // clone things so we can handle multiple messages in parallel
                    let close_sender = close_sender.clone();
                    let app = app.clone();
                    let authorization = authorization.clone();
                    let response_sender = response_sender.clone();
                    let subscriptions = subscriptions.clone();
                    let subscription_count = subscription_count.clone();

                    let f = async move {
                        // new message from our client. forward to a backend and then send it through response_sender
                        let (response_msg, _semaphore) = match msg {
                            Message::Text(payload) => {
                                match handle_socket_payload(
                                    &app,
                                    &authorization,
                                    &payload,
                                    &response_sender,
                                    &subscription_count,
                                    subscriptions,
                                )
                                .await {
                                    Ok((m, s)) => (m, Some(s)),
                                    Err(err) => {
                                        // TODO: how can we get the id out of the payload?
                                        let m = err.into_message(None, None::<RequestForError>);
                                        (m, None)
                                    }
                                }
                            }
                            Message::Ping(x) => {
                                trace!("ping: {:?}", x);
                                (Message::Pong(x), None)
                            }
                            Message::Pong(x) => {
                                trace!("pong: {:?}", x);
                                return;
                            }
                            Message::Close(_) => {
                                trace!("closing websocket connection");
                                // TODO: do something to close subscriptions?
                                let _ = close_sender.send(true);
                                return;
                            }
                            Message::Binary(payload) => {
                                let payload = from_utf8(&payload).unwrap();

                                let (m, s) = match handle_socket_payload(
                                    &app,
                                    &authorization,
                                    payload,
                                    &response_sender,
                                    &subscription_count,
                                    subscriptions,
                                )
                                .await {
                                    Ok((m, s)) => (m, Some(s)),
                                    Err(err) => {
                                        // TODO: how can we get the id out of the payload?
                                        let m = err.into_message(None, None::<RequestForError>);
                                        (m, None)
                                    }
                                };

                                // TODO: is this an okay way to convert from text to binary?
                                let m = if let Message::Text(m) = m {
                                    Message::Binary(m.as_bytes().to_vec().into())
                                } else {
                                    unimplemented!();
                                };

                                (m, s)
                            }
                        };

                        if response_sender.send(response_msg).await.is_err() {
                            let _ = close_sender.send(true);
                        };
                    };

                    tokio::spawn(f);
                } else {
                    break;
                }
            }
            _ = close_receiver.recv() => {
                break;
            }
        }
    }
}

async fn write_web3_socket(
    mut response_rx: mpsc::Receiver<Message>,
    mut ws_tx: SplitSink<WebSocket, Message>,
) {
    // TODO: increment counter for open websockets

    while let Some(msg) = response_rx.recv().await {
        // a response is ready

        // we do not check rate limits here. they are checked before putting things into response_sender;

        // forward the response to through the websocket
        if let Err(err) = ws_tx.send(msg).await {
            // this is common. it happens whenever a client disconnects
            trace!("unable to write to websocket: {:?}", err);
            break;
        };
    }

    // TODO: decrement counter for open websockets
}

#[cfg(test)]
mod test {
    #[test]
    fn nulls_and_defaults() {
        let x = serde_json::Value::Null;
        let x = serde_json::to_string(&x).unwrap();

        let y: Box<serde_json::value::RawValue> = Default::default();
        let y = serde_json::to_string(&y).unwrap();

        assert_eq!(x, y);
    }
}
