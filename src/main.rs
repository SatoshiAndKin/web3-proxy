mod block_watcher;
mod provider;
mod provider_tiers;

use futures::future;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use governor::clock::{Clock, QuantaClock};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, RwLock};
use tokio::time::sleep;
use tracing::warn;
use warp::Filter;

// use crate::types::{BlockMap, ConnectionsMap, RpcRateLimiterMap};
use crate::block_watcher::BlockWatcher;
use crate::provider_tiers::{Web3ConnectionMap, Web3ProviderTier};

static APP_USER_AGENT: &str = concat!(
    "satoshiandkin/",
    env!("CARGO_PKG_NAME"),
    "/",
    env!("CARGO_PKG_VERSION"),
);

#[derive(Clone, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: Box<RawValue>,
    id: Box<RawValue>,
    method: String,
    params: Box<RawValue>,
}

#[derive(Clone, Serialize)]
struct JsonRpcForwardedResponse {
    jsonrpc: Box<RawValue>,
    id: Box<RawValue>,
    result: Box<RawValue>,
}

/// The application
// TODO: this debug impl is way too verbose. make something smaller
struct Web3ProxyApp {
    /// clock used for rate limiting
    /// TODO: use tokio's clock (will require a different ratelimiting crate)
    clock: QuantaClock,
    /// Send requests to the best server available
    balanced_rpc_tiers: Arc<Vec<Web3ProviderTier>>,
    /// Send private requests (like eth_sendRawTransaction) to all these servers
    private_rpcs: Option<Arc<Web3ProviderTier>>,
    /// write lock on these when all rate limits are hit
    /// this lock will be held open over an await, so use async locking
    balanced_rpc_ratelimiter_lock: RwLock<()>,
    /// this lock will be held open over an await, so use async locking
    private_rpcs_ratelimiter_lock: RwLock<()>,
}

impl fmt::Debug for Web3ProxyApp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: the default formatter takes forever to write. this is too quiet though
        write!(f, "Web3ProxyApp(...)")
    }
}

impl Web3ProxyApp {
    async fn try_new(
        allowed_lag: u64,
        balanced_rpc_tiers: Vec<Vec<(&str, u32)>>,
        private_rpcs: Vec<(&str, u32)>,
    ) -> anyhow::Result<Web3ProxyApp> {
        let clock = QuantaClock::default();

        let mut rpcs = vec![];
        for balanced_rpc_tier in balanced_rpc_tiers.iter() {
            for rpc_data in balanced_rpc_tier {
                let rpc = rpc_data.0.to_string();

                rpcs.push(rpc);
            }
        }
        for rpc_data in private_rpcs.iter() {
            let rpc = rpc_data.0.to_string();

            rpcs.push(rpc);
        }

        let block_watcher = Arc::new(BlockWatcher::new(rpcs));

        // make a http shared client
        // TODO: how should we configure the connection pool?
        // TODO: 5 minutes is probably long enough. unlimited is a bad idea if something is wrong with the remote server
        let http_client = reqwest::ClientBuilder::new()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(300))
            .user_agent(APP_USER_AGENT)
            .build()?;

        let balanced_rpc_tiers = Arc::new(
            future::join_all(balanced_rpc_tiers.into_iter().map(|balanced_rpc_tier| {
                Web3ProviderTier::try_new(
                    balanced_rpc_tier,
                    Some(http_client.clone()),
                    block_watcher.clone(),
                    &clock,
                )
            }))
            .await
            .into_iter()
            .collect::<anyhow::Result<Vec<Web3ProviderTier>>>()?,
        );

        let private_rpcs = if private_rpcs.is_empty() {
            warn!("No private relays configured. Any transactions will be broadcast to the public mempool!");
            // TODO: instead of None, set it to a list of all the rpcs from balanced_rpc_tiers. that way we broadcast very loudly
            None
        } else {
            Some(Arc::new(
                Web3ProviderTier::try_new(
                    private_rpcs,
                    Some(http_client),
                    block_watcher.clone(),
                    &clock,
                )
                .await?,
            ))
        };

        let (new_block_sender, mut new_block_receiver) = watch::channel::<String>("".to_string());

        {
            // TODO: spawn this later?
            // spawn a future for the block_watcher
            let block_watcher = block_watcher.clone();
            tokio::spawn(async move { block_watcher.run(new_block_sender).await });
        }

        {
            // spawn a future for sorting our synced rpcs
            // TODO: spawn this later?
            let balanced_rpc_tiers = balanced_rpc_tiers.clone();
            let private_rpcs = private_rpcs.clone();
            let block_watcher = block_watcher.clone();

            tokio::spawn(async move {
                let mut tier_map = HashMap::new();
                let mut private_map = HashMap::new();

                for balanced_rpc_tier in balanced_rpc_tiers.iter() {
                    for rpc in balanced_rpc_tier.clone_rpcs() {
                        tier_map.insert(rpc, balanced_rpc_tier);
                    }
                }

                if let Some(private_rpcs) = private_rpcs {
                    for rpc in private_rpcs.clone_rpcs() {
                        private_map.insert(rpc, private_rpcs.clone());
                    }
                }

                while new_block_receiver.changed().await.is_ok() {
                    let updated_rpc = new_block_receiver.borrow().clone();

                    if let Some(tier) = tier_map.get(&updated_rpc) {
                        tier.update_synced_rpcs(block_watcher.clone(), allowed_lag)
                            .unwrap();
                    } else if let Some(tier) = private_map.get(&updated_rpc) {
                        tier.update_synced_rpcs(block_watcher.clone(), allowed_lag)
                            .unwrap();
                    } else {
                        panic!("howd this happen");
                    }
                }
            });
        }

        Ok(Web3ProxyApp {
            clock,
            balanced_rpc_tiers,
            private_rpcs,
            balanced_rpc_ratelimiter_lock: Default::default(),
            private_rpcs_ratelimiter_lock: Default::default(),
        })
    }

    /// send the request to the approriate RPCs
    /// TODO: dry this up
    async fn proxy_web3_rpc(
        self: Arc<Web3ProxyApp>,
        json_body: JsonRpcRequest,
    ) -> anyhow::Result<impl warp::Reply> {
        if self.private_rpcs.is_some() && json_body.method == "eth_sendRawTransaction" {
            let private_rpcs = self.private_rpcs.clone().unwrap();

            // there are private rpcs configured and the request is eth_sendSignedTransaction. send to all private rpcs
            loop {
                let read_lock = self.private_rpcs_ratelimiter_lock.read().await;

                let json_body_clone = json_body.clone();

                match private_rpcs.get_upstream_servers().await {
                    Ok(upstream_servers) => {
                        let (tx, mut rx) =
                            mpsc::unbounded_channel::<anyhow::Result<serde_json::Value>>();

                        let clone = self.clone();
                        let connections = private_rpcs.clone_connections();

                        // check incoming_id before sending any requests
                        let incoming_id = &*json_body.id;

                        tokio::spawn(async move {
                            clone
                                .try_send_requests(
                                    upstream_servers,
                                    connections,
                                    json_body_clone,
                                    tx,
                                )
                                .await
                        });

                        let response = rx
                            .recv()
                            .await
                            .ok_or_else(|| anyhow::anyhow!("no successful response"))?;

                        if let Ok(partial_response) = response {
                            let response = json!({
                                "jsonrpc": "2.0",
                                "id": incoming_id,
                                "result": partial_response
                            });
                            return Ok(warp::reply::json(&response));
                        }
                    }
                    Err(not_until) => {
                        // TODO: move this to a helper function
                        // sleep (with a lock) until our rate limits should be available
                        drop(read_lock);

                        if let Some(not_until) = not_until {
                            let write_lock = self.balanced_rpc_ratelimiter_lock.write().await;

                            let deadline = not_until.wait_time_from(self.clock.now());

                            sleep(deadline).await;

                            drop(write_lock);
                        }
                    }
                };
            }
        } else {
            // this is not a private transaction (or no private relays are configured)
            // try to send to each tier, stopping at the first success
            loop {
                let read_lock = self.balanced_rpc_ratelimiter_lock.read().await;

                // there are multiple tiers. save the earliest not_until (if any). if we don't return, we will sleep until then and then try again
                let mut earliest_not_until = None;

                // check incoming_id before sending any requests
                let incoming_id = &*json_body.id;

                for balanced_rpcs in self.balanced_rpc_tiers.iter() {
                    // TODO: what allowed lag?
                    match balanced_rpcs.next_upstream_server().await {
                        Ok(upstream_server) => {
                            // TODO: better type for this. right now its request (the full jsonrpc object), response (just the inner result)
                            let (tx, mut rx) =
                                mpsc::unbounded_channel::<anyhow::Result<serde_json::Value>>();

                            {
                                // clone things so we can move them into the future and still use them here
                                let clone = self.clone();
                                let connections = balanced_rpcs.clone_connections();
                                let json_body = json_body.clone();
                                let upstream_server = upstream_server.clone();

                                tokio::spawn(async move {
                                    clone
                                        .try_send_requests(
                                            vec![upstream_server],
                                            connections,
                                            json_body,
                                            tx,
                                        )
                                        .await
                                });
                            }

                            let response = rx
                                .recv()
                                .await
                                .ok_or_else(|| anyhow::anyhow!("no successful response"))?;

                            let response = match response {
                                Ok(partial_response) => {
                                    // TODO: trace
                                    // info!("forwarding request from {}", upstream_server);

                                    json!({
                                        "jsonrpc": "2.0",
                                        "id": incoming_id,
                                        "result": partial_response
                                    })
                                }
                                Err(e) => {
                                    // TODO: what is the proper format for an error?
                                    // TODO: use e
                                    json!({
                                        "jsonrpc": "2.0",
                                        "id": incoming_id,
                                        "error": format!("{}", e)
                                    })
                                }
                            };

                            return Ok(warp::reply::json(&response));
                        }
                        Err(None) => {
                            warn!("No servers in sync!");
                        }
                        Err(Some(not_until)) => {
                            // save the smallest not_until. if nothing succeeds, return an Err with not_until in it
                            if earliest_not_until.is_none() {
                                earliest_not_until.replace(not_until);
                            } else {
                                let earliest_possible =
                                    earliest_not_until.as_ref().unwrap().earliest_possible();

                                let new_earliest_possible = not_until.earliest_possible();

                                if earliest_possible > new_earliest_possible {
                                    earliest_not_until = Some(not_until);
                                }
                            }
                        }
                    }
                }

                // we haven't returned an Ok, sleep and try again
                // TODO: move this to a helper function
                drop(read_lock);

                // unwrap should be safe since we would have returned if it wasn't set
                if let Some(earliest_not_until) = earliest_not_until {
                    let write_lock = self.balanced_rpc_ratelimiter_lock.write().await;

                    let deadline = earliest_not_until.wait_time_from(self.clock.now());

                    sleep(deadline).await;

                    drop(write_lock);
                } else {
                    // TODO: how long should we wait?
                    // TODO: max wait time?
                    sleep(Duration::from_millis(500)).await;
                };
            }
        }
    }

    async fn try_send_requests(
        &self,
        rpc_servers: Vec<String>,
        connections: Arc<Web3ConnectionMap>,
        json_request_body: JsonRpcRequest,
        // TODO: better type for this
        tx: mpsc::UnboundedSender<anyhow::Result<serde_json::Value>>,
    ) -> anyhow::Result<()> {
        // {"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}
        let method = json_request_body.method.clone();
        let params = json_request_body.params;

        if rpc_servers.len() == 1 {
            let rpc = rpc_servers.first().unwrap();

            let provider = connections.get(rpc).unwrap().clone_provider();

            let response = provider.request(&method, params).await;

            connections.get(rpc).unwrap().dec_active_requests();

            tx.send(response.map_err(Into::into))?;

            Ok(())
        } else {
            // TODO: lets just use a usize index or something
            let method = Arc::new(method);

            let mut unordered_futures = FuturesUnordered::new();

            for rpc in rpc_servers {
                let connections = connections.clone();
                let method = method.clone();
                let params = params.clone();
                let tx = tx.clone();

                let handle = tokio::spawn(async move {
                    // get the client for this rpc server
                    let provider = connections.get(&rpc).unwrap().clone_provider();

                    let response = provider.request(&method, params).await;

                    connections.get(&rpc).unwrap().dec_active_requests();

                    let response = response?;

                    // TODO: if "no block with that header" or some other jsonrpc errors, skip this response

                    // send the first good response to a one shot channel. that way we respond quickly
                    // drop the result because errors are expected after the first send
                    let _ = tx.send(Ok(response));

                    Ok::<(), anyhow::Error>(())
                });

                unordered_futures.push(handle);
            }

            // TODO: use iterators instead of pushing into a vec
            let mut errs = vec![];
            if let Some(x) = unordered_futures.next().await {
                match x.unwrap() {
                    Ok(_) => {}
                    Err(e) => {
                        // TODO: better errors
                        warn!("Got an error sending request: {}", e);
                        errs.push(e);
                    }
                }
            }

            // get the first error (if any)
            let e: anyhow::Result<serde_json::Value> = if !errs.is_empty() {
                Err(errs.pop().unwrap())
            } else {
                Err(anyhow::anyhow!("no successful responses"))
            };

            // send the error to the channel
            if tx.send(e).is_ok() {
                // if we were able to send an error, then we never sent a success
                return Err(anyhow::anyhow!("no successful responses"));
            } else {
                // if sending the error failed. the other side must be closed (which means we sent a success earlier)
                Ok(())
            }
        }
    }
}

#[tokio::main]
async fn main() {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    // TODO: load the config from yaml instead of hard coding
    // TODO: support multiple chains in one process? then we could just point "chain.stytt.com" at this and caddy wouldn't need anything else
    // TODO: be smart about about using archive nodes? have a set that doesn't use archive nodes since queries to them are more valuable
    let listen_port = 8445;
    // TODO: what should this be? 0 will cause a thundering herd
    let allowed_lag = 0;

    let state = Web3ProxyApp::try_new(
        allowed_lag,
        vec![
            // local nodes
            vec![("ws://10.11.12.16:8545", 0), ("ws://10.11.12.16:8946", 0)],
            // paid nodes
            // TODO: add paid nodes (with rate limits)
            // vec![
            //     // chainstack.com archive
            //     (
            //         "wss://ws-nd-373-761-850.p2pify.com/106d73af4cebc487df5ba92f1ad8dee7",
            //         0,
            //     ),
            // ],
            // free nodes
            // vec![
            //     // ("https://main-rpc.linkpool.io", 0), // linkpool is slow and often offline
            //     ("https://rpc.ankr.com/eth", 0),
            // ],
        ],
        vec![
            // ("https://api.edennetwork.io/v1/beta", 0),
            // ("https://api.edennetwork.io/v1/", 0),
        ],
    )
    .await
    .unwrap();

    let state: Arc<Web3ProxyApp> = Arc::new(state);

    let proxy_rpc_filter = warp::any()
        .and(warp::post())
        .and(warp::body::json())
        .then(move |json_body| state.clone().proxy_web3_rpc(json_body));

    // TODO: filter for displaying connections and their block heights

    // TODO: warp trace is super verbose. how do we make this more readable?
    // let routes = proxy_rpc_filter.with(warp::trace::request());
    let routes = proxy_rpc_filter.map(handle_anyhow_errors);

    warp::serve(routes).run(([0, 0, 0, 0], listen_port)).await;
}

/// convert result into an http response. use this at the end of your warp filter
pub fn handle_anyhow_errors<T: warp::Reply>(res: anyhow::Result<T>) -> Box<dyn warp::Reply> {
    match res {
        Ok(r) => Box::new(r.into_response()),
        Err(e) => Box::new(warp::reply::with_status(
            format!("{}", e),
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}
