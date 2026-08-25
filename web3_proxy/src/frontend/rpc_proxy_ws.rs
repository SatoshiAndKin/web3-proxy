//! Take a user's WebSocket JSON-RPC requests and either respond from local data or proxy the request to a backend rpc server.
//!
//! WebSockets are the preferred method of receiving requests, but not all clients have good support.

use crate::errors::{RequestForError, Web3ProxyError, Web3ProxyResponse};
use crate::jsonrpc::{self, ParsedResponse, ValidatedRequest};
use crate::{app::App, errors::Web3ProxyResult, jsonrpc::SingleRequest};
use alloy::primitives::U64;
use axum::{
    extract::ws::{rejection::WebSocketUpgradeRejection, Message, WebSocket, WebSocketUpgrade},
    extract::State,
    response::{IntoResponse, Redirect},
};
use axum_macros::debug_handler;
use futures::SinkExt;
use futures::{
    future::AbortHandle,
    stream::{SplitSink, SplitStream, StreamExt},
};
use hashbrown::HashMap;
use sonic_rs::{json, JsonValueTrait};
use std::str::from_utf8;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::select;
use tokio::sync::{broadcast, mpsc, oneshot, RwLock as AsyncRwLock};
use tracing::trace;

/// How to select backend servers for a request
#[derive(Copy, Clone, Debug, Default)]
pub enum ProxyMode {
    /// send to the "best" synced server. on error, try the next
    #[default]
    Best,
    /// send to all synced servers and return the fastest non-error response (reverts do not count as errors here)
    Fastest(usize),
    /// send to all servers for benchmarking. return the fastest non-error response
    Versus,
}

struct WebsocketRpcResponse {
    response: jsonrpc::Response,
    subscription_start: Option<oneshot::Sender<()>>,
}

struct SocketResponse {
    message: Message,
    subscription_start: Option<oneshot::Sender<()>>,
}

impl From<Message> for SocketResponse {
    fn from(message: Message) -> Self {
        Self {
            message,
            subscription_start: None,
        }
    }
}

/// Public entrypoint for WebSocket JSON-RPC requests.
/// Queries a single server at a time
#[debug_handler]
pub async fn websocket_handler(
    State(app): State<Arc<App>>,
    ws_upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Web3ProxyResponse {
    _websocket_handler(ProxyMode::Best, app, ws_upgrade).await
}

/// Public entrypoint for WebSocket JSON-RPC requests that uses all synced servers.
/// Queries all synced backends with every request! This might get expensive!
// #[debug_handler]
pub async fn fastest_websocket_handler(
    State(app): State<Arc<App>>,
    ws_upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Web3ProxyResponse {
    // TODO: get the fastest number from the url params (default to 0/all)
    // TODO: config to disable this
    _websocket_handler(ProxyMode::Fastest(0), app, ws_upgrade).await
}

/// Public entrypoint for WebSocket JSON-RPC requests that uses all synced servers.
/// Queries **all** backends with every request! This might get expensive!
#[debug_handler]
pub async fn versus_websocket_handler(
    State(app): State<Arc<App>>,
    ws_upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Web3ProxyResponse {
    // TODO: config to disable this
    _websocket_handler(ProxyMode::Versus, app, ws_upgrade).await
}

async fn _websocket_handler(
    proxy_mode: ProxyMode,
    app: Arc<App>,
    ws_upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Web3ProxyResponse {
    match ws_upgrade {
        Ok(ws) => Ok(ws
            .on_upgrade(move |socket| proxy_web3_socket(app, proxy_mode, socket))
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

async fn proxy_web3_socket(app: Arc<App>, proxy_mode: ProxyMode, socket: WebSocket) {
    // split the websocket so we can read and write concurrently
    let (ws_tx, ws_rx) = socket.split();

    let buffer = 2048;

    // create a channel for our reader and writer can communicate. todo: benchmark different channels
    // TODO: this should be bounded. async blocking on too many messages would be fine
    let (response_sender, response_receiver) = mpsc::channel::<Message>(buffer);

    tokio::spawn(write_web3_socket(response_receiver, ws_tx));
    tokio::spawn(read_web3_socket(app, proxy_mode, ws_rx, response_sender));
}

async fn websocket_proxy_web3_rpc(
    app: &Arc<App>,
    proxy_mode: ProxyMode,
    json_request: SingleRequest,
    response_sender: &mpsc::Sender<Message>,
    subscription_count: &AtomicU64,
    subscriptions: &AsyncRwLock<HashMap<U64, AbortHandle>>,
) -> Web3ProxyResult<WebsocketRpcResponse> {
    match &json_request.method[..] {
        "eth_subscribe" => {
            let web3_request = ValidatedRequest::new_with_app(
                app,
                proxy_mode,
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
                Ok(subscription) => {
                    subscriptions
                        .write()
                        .await
                        .insert(subscription.id, subscription.abort_handle);

                    Ok(WebsocketRpcResponse {
                        response: subscription.response.into(),
                        subscription_start: Some(subscription.start_sender),
                    })
                }
                Err(err) => Err(err),
            }
        }
        "eth_unsubscribe" => {
            let web3_request = ValidatedRequest::new_with_app(
                app,
                proxy_mode,
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

            let subscription_id: U64 = match sonic_rs::from_value::<U64>(&maybe_id) {
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

            Ok(WebsocketRpcResponse {
                response: response.into(),
                subscription_start: None,
            })
        }
        _ => app
            .proxy_web3_rpc(proxy_mode, json_request.into(), None)
            .await
            .map(|(_, response, _)| WebsocketRpcResponse {
                response,
                subscription_start: None,
            }),
    }
}

/// websockets support a few more methods than http clients
async fn handle_socket_payload(
    app: &Arc<App>,
    proxy_mode: ProxyMode,
    payload: &str,
    response_sender: &mpsc::Sender<Message>,
    subscription_count: &AtomicU64,
    subscriptions: Arc<AsyncRwLock<HashMap<U64, AbortHandle>>>,
) -> Web3ProxyResult<SocketResponse> {
    // TODO: handle batched requests
    let (response_id, response) = match sonic_rs::from_str::<SingleRequest>(payload) {
        Ok(json_request) => {
            let request_id = json_request.id.clone();

            // TODO: move this to a seperate function so we can use the try operator
            let x = websocket_proxy_web3_rpc(
                app,
                proxy_mode,
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

    let (response_str, subscription_start) = match response {
        Ok(x) => (x.response.to_json_string().await?, x.subscription_start),
        Err(err) => {
            let (_, response_data) = err.as_response_parts(None::<RequestForError>);

            let response = ParsedResponse::from_response_data(response_data, response_id);

            (
                sonic_rs::to_string(&response).expect("to_string should always work here"),
                None,
            )
        }
    };

    Ok(SocketResponse {
        message: Message::Text(response_str.into()),
        subscription_start,
    })
}

async fn read_web3_socket(
    app: Arc<App>,
    proxy_mode: ProxyMode,
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
                    let response_sender = response_sender.clone();
                    let subscriptions = subscriptions.clone();
                    let subscription_count = subscription_count.clone();

                    let f = async move {
                        // new message from our client. forward to a backend and then send it through response_sender
                        let response_msg = match msg {
                            Message::Text(payload) => {
                                match handle_socket_payload(
                                    &app,
                                    proxy_mode,
                                    &payload,
                                    &response_sender,
                                    &subscription_count,
                                    subscriptions,
                                )
                                .await {
                                    Ok(response) => response,
                                    Err(err) => {
                                        // TODO: how can we get the id out of the payload?
                                        err.into_message(None, None::<RequestForError>).into()
                                    }
                                }
                            }
                            Message::Ping(x) => {
                                trace!("ping: {:?}", x);
                                Message::Pong(x).into()
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

                                let mut response = match handle_socket_payload(
                                    &app,
                                    proxy_mode,
                                    payload,
                                    &response_sender,
                                    &subscription_count,
                                    subscriptions,
                                )
                                .await {
                                    Ok(response) => response,
                                    Err(err) => {
                                        // TODO: how can we get the id out of the payload?
                                        err.into_message(None, None::<RequestForError>).into()
                                    }
                                };

                                // TODO: is this an okay way to convert from text to binary?
                                response.message = if let Message::Text(m) = response.message {
                                    Message::Binary(m.as_bytes().to_vec().into())
                                } else {
                                    unimplemented!();
                                };

                                response
                            }
                        };

                        let SocketResponse {
                            message,
                            subscription_start,
                        } = response_msg;

                        if response_sender.send(message).await.is_err() {
                            let _ = close_sender.send(true);
                        } else if let Some(subscription_start) = subscription_start {
                            let _ = subscription_start.send(());
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
    use sonic_rs::{OwnedLazyValue, Value};

    #[test]
    fn nulls_and_defaults() {
        let x = Value::default();
        let x = sonic_rs::to_string(&x).unwrap();

        let y = OwnedLazyValue::default();
        let y = sonic_rs::to_string(&y).unwrap();

        assert_eq!(x, y);
    }
}
