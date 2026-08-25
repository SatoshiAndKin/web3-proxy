//! Websocket-specific functions for the Web3ProxyApp

use super::App;
use crate::errors::{Web3ProxyError, Web3ProxyResult};
use crate::jsonrpc::ResponseData;
use crate::jsonrpc::{self, RequestOrMethod, ValidatedRequest};
use alloy::primitives::U64;
use axum::extract::ws::Message;
use futures::future::AbortHandle;
use futures::future::Abortable;
use futures::stream::StreamExt;
use sonic_rs::{json, JsonValueTrait};
use std::sync::atomic::{self, AtomicU64};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::WatchStream;
use tracing::{error, trace};

pub(crate) struct PreparedSubscription {
    pub(crate) id: U64,
    pub(crate) abort_handle: AbortHandle,
    pub(crate) response: jsonrpc::ParsedResponse,
    pub(crate) start_sender: oneshot::Sender<()>,
}

impl App {
    pub(crate) async fn eth_subscribe<'a>(
        self: &'a Arc<Self>,
        web3_request: Arc<ValidatedRequest>,
        subscription_count: &'a AtomicU64,
        // TODO: taking a sender for Message instead of the exact json we are planning to send feels wrong, but its easier for now
        response_sender: mpsc::Sender<Message>,
    ) -> Web3ProxyResult<PreparedSubscription> {
        let subscribe_to = web3_request
            .inner
            .params()
            .get(0)
            .and_then(|x| x.as_str())
            .ok_or_else(|| {
                Web3ProxyError::BadRequest("unable to subscribe using these params".into())
            })?;

        let (subscription_abort_handle, subscription_registration) = AbortHandle::new_pair();
        // Keep notifications behind the JSON-RPC acknowledgement in the socket queue.
        let (start_sender, start_receiver) = oneshot::channel();

        // TODO: this only needs to be unique per connection. we don't need it globably unique
        // TODO: have a max number of subscriptions per key/ip. have a global max number of subscriptions? how should this be calculated?
        let subscription_id = subscription_count.fetch_add(1, atomic::Ordering::SeqCst);
        let subscription_id = U64::from(subscription_id);

        // TODO: calling `json!` on every request is probably not fast. but it works for now
        // TODO: i think we need a stricter EthSubscribeRequest type that JsonRpcRequest can turn into
        // TODO: DRY This up. lots of duplication between newHeads and newPendingTransactions
        match subscribe_to {
            "newHeads" => {
                // we clone the watch before spawning so that theres less chance of missing anything
                // TODO: watch receivers can miss a block. is that okay?
                let head_block_receiver = self.watch_consensus_head_receiver.clone();
                let app = self.clone();
                let proxy_mode = web3_request.proxy_mode();

                tokio::spawn(async move {
                    if start_receiver.await.is_err() {
                        return;
                    }

                    trace!("newHeads subscription {:?}", subscription_id);

                    let mut head_block_receiver = Abortable::new(
                        WatchStream::new(head_block_receiver),
                        subscription_registration,
                    );

                    while let Some(new_head) = head_block_receiver.next().await {
                        let new_head = if let Some(new_head) = new_head {
                            new_head
                        } else {
                            continue;
                        };

                        let subscription_web3_request = ValidatedRequest::new_with_app(
                            &app,
                            proxy_mode,
                            None,
                            RequestOrMethod::Method("eth_subscribe(newHeads)".into(), 0),
                            Some(new_head),
                            None,
                        )
                        .await;

                        match subscription_web3_request {
                            Err(err) => {
                                error!(?err, "error creating subscription_web3_request");
                                // TODO: send them an error message before closing
                                break;
                            }
                            Ok(subscription_web3_request) => {
                                // TODO: make a response struct for subscription notifications.
                                let response_json = json!({
                                    "jsonrpc": "2.0",
                                    "method":"eth_subscription",
                                    "params": {
                                        "subscription": subscription_id,
                                        "result": subscription_web3_request.head_block.as_ref().map(|x| &x.0),
                                    },
                                });

                                let response_str = sonic_rs::to_string(&response_json)
                                    .expect("this should always be valid json");

                                let response_bytes = response_str.len() as u64;

                                // TODO: do clients support binary messages?
                                // TODO: can we check a content type header?
                                let response_msg = Message::Text(response_str.into());

                                if response_sender.send(response_msg).await.is_err() {
                                    // TODO: increment error_response? i don't think so. i think this will happen once every time a client disconnects.
                                    // TODO: cancel this subscription earlier? select on head_block_receiver.next() and an abort handle?
                                    break;
                                };

                                subscription_web3_request.set_response(response_bytes);
                            }
                        }
                    }

                    let _ = response_sender.send(Message::Close(None)).await;

                    trace!("closed newHeads subscription {:?}", subscription_id);
                });
            }
            // TODO: bring back the other custom subscription types that had the full transaction object
            "newPendingTransactions" => {
                // we subscribe before spawning so that theres less chance of missing anything
                let pending_txid_firehose = self.pending_txid_firehose.subscribe();
                let app = self.clone();
                let proxy_mode = web3_request.proxy_mode();

                tokio::spawn(async move {
                    if start_receiver.await.is_err() {
                        return;
                    }

                    let mut pending_txid_firehose = Abortable::new(
                        BroadcastStream::new(pending_txid_firehose),
                        subscription_registration,
                    );

                    while let Some(maybe_txid) = pending_txid_firehose.next().await {
                        match maybe_txid {
                            Err(err) => {
                                trace!(
                                    ?err,
                                    "error inside newPendingTransactions. probably lagged"
                                );
                                continue;
                            }
                            Ok(new_txid) => {
                                // TODO: include the head_block here?
                                match ValidatedRequest::new_with_app(
                                    &app,
                                    proxy_mode,
                                    None,
                                    RequestOrMethod::Method(
                                        "eth_subscribe(newPendingTransactions)".into(),
                                        0,
                                    ),
                                    None,
                                    None,
                                )
                                .await
                                {
                                    Err(err) => {
                                        error!(?err, "error creating subscription_web3_request");
                                        // what should we do to turn this error into a message for them?
                                        break;
                                    }
                                    Ok(subscription_web3_request) => {
                                        // TODO: make a struct/helper function for this
                                        let response_json = json!({
                                            "jsonrpc": "2.0",
                                            "method":"eth_subscription",
                                            "params": {
                                                "subscription": subscription_id,
                                                "result": new_txid,
                                            },
                                        });

                                        let response_str = sonic_rs::to_string(&response_json)
                                            .expect("this should always be valid json");

                                        let response_bytes = response_str.len() as u64;

                                        subscription_web3_request.set_response(response_bytes);

                                        // TODO: do clients support binary messages?
                                        // TODO: can we check a content type header?
                                        let response_msg = Message::Text(response_str.into());

                                        if response_sender.send(response_msg).await.is_err() {
                                            // TODO: increment error_response? i don't think so. i think this will happen once every time a client disconnects.
                                            // TODO: cancel this subscription earlier? select on head_block_receiver.next() and an abort handle?
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    let _ = response_sender.send(Message::Close(None)).await;

                    trace!(
                        "closed newPendingTransactions subscription {:?}",
                        subscription_id
                    );
                });
            }
            _ => {
                // TODO: make sure this gets a CU cost of unimplemented instead of the normal eth_subscribe cost?
                return Err(Web3ProxyError::MethodNotFound(
                    subscribe_to.to_owned().into(),
                ));
            }
        };

        // TODO: do something with subscription_join_handle?

        let response_data = ResponseData::from(json!(subscription_id));

        let response =
            jsonrpc::ParsedResponse::from_response_data(response_data, web3_request.id());

        // TODO: better way of passing in ParsedResponse
        let response = jsonrpc::SingleResponse::Parsed(response);
        // TODO: this serializes twice
        web3_request.set_response(&response);
        let response = response.parsed().await.expect("Response already parsed");

        Ok(PreparedSubscription {
            id: subscription_id,
            abort_handle: subscription_abort_handle,
            response,
            start_sender,
        })
    }
}
