mod ws;

use crate::config::{AppConfig, TopConfig};
use crate::errors::{RequestForError, Web3ProxyError, Web3ProxyErrorContext, Web3ProxyResult};
use crate::frontend::rpc_proxy_ws::ProxyMode;
use crate::globals::APP;
use crate::jsonrpc::{
    self, JsonRpcErrorData, JsonRpcRequestEnum, ResponseData, SingleRequest, SingleResponse,
    ValidatedRequest,
};
use crate::rpcs::blockchain::BlockHeader;
use crate::rpcs::consensus::RankedRpcs;
use crate::rpcs::many::{Web3Rpcs, Web3RpcsSpawnConfig};
use crate::rpcs::one::Web3Rpc;
use alloy::consensus::{Transaction as _, TxEnvelope};
use alloy::eips::Decodable2718;
use alloy::primitives::{keccak256, Address, Bytes, TxHash, B256, U256, U64};
use axum::http::StatusCode;
use deduped_broadcast::DedupedBroadcaster;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use hashbrown::HashSet;
use sonic_rs::{json, JsonContainerTrait, JsonValueTrait, OwnedLazyValue};
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::AtomicU16;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch, Semaphore};
use tokio::task::{yield_now, JoinHandle};
use tokio::time::{sleep, sleep_until, timeout_at, Instant};
use tokio::{pin, select};
use tracing::{error, info, trace};

// TODO: make this customizable?
// TODO: include GIT_REF in here. i had trouble getting https://docs.rs/vergen/latest/vergen/ to work with a workspace. also .git is in .dockerignore
pub static APP_USER_AGENT: &str = concat!(
    "ski_",
    env!("CARGO_PKG_NAME"),
    "/v",
    env!("CARGO_PKG_VERSION")
);

/// Convenience type
pub type Web3ProxyJoinHandle<T> = JoinHandle<Web3ProxyResult<T>>;

/// The application
// TODO: i'm sure this is more arcs than necessary, but spawning futures makes references hard
pub struct App {
    /// Send requests to the best server available
    pub balanced_rpcs: Arc<Web3Rpcs>,
    /// Send 4337 Abstraction Bundler requests to one of these servers
    pub bundler_4337_rpcs: Arc<Web3Rpcs>,
    /// application config
    /// TODO: this will need a large refactor to handle reloads while running. maybe use a watch::Receiver and a task_local?
    pub config: AppConfig,
    pub http_client: Option<reqwest::Client>,
    /// rpc clients that subscribe to newHeads use this channel
    /// don't drop this or the sender will stop working
    /// TODO: broadcast channel instead?
    pub watch_consensus_head_receiver: watch::Receiver<Option<BlockHeader>>,
    /// rpc clients that subscribe to newPendingTransactions use this channel
    pub pending_txid_firehose: Arc<DedupedBroadcaster<TxHash>>,
    pub hostname: Option<String>,
    pub frontend_port: Arc<AtomicU16>,
    /// Send private requests (like eth_sendRawTransaction) to all these servers
    pub protected_rpcs: Arc<Web3Rpcs>,
    /// when the app started
    pub start: Instant,
    /// limit the number of tx subscriptions
    pub tx_subscriptions: Semaphore,
}

/// starting an app creates many tasks
pub struct Web3ProxyAppSpawn {
    /// the app. probably clone this to use in other groups of handles
    pub app: Arc<App>,
    /// handle for some rpcs
    pub balanced_handle: Web3ProxyJoinHandle<()>,
    /// handle for some rpcs
    pub private_handle: Web3ProxyJoinHandle<()>,
    /// handle for some rpcs
    pub bundler_4337_rpcs_handle: Web3ProxyJoinHandle<()>,
    /// these are important and must be allowed to finish
    pub background_handles: FuturesUnordered<Web3ProxyJoinHandle<()>>,
    /// config changes are sent here
    pub new_top_config: Arc<watch::Sender<TopConfig>>,
    /// watch this to know when the app is ready to serve requests
    pub ranked_rpcs: watch::Receiver<Option<Arc<RankedRpcs>>>,
}

impl App {
    /// The main entrypoint.
    pub async fn spawn(
        frontend_port: Arc<AtomicU16>,
        mut top_config: TopConfig,
        shutdown_sender: broadcast::Sender<()>,
        watch_consensus_head_sender: watch::Sender<Option<BlockHeader>>,
    ) -> anyhow::Result<Web3ProxyAppSpawn> {
        let mut config_watcher_shutdown_receiver = shutdown_sender.subscribe();
        let mut background_shutdown_receiver = shutdown_sender.subscribe();

        top_config.clean();

        let (new_top_config_sender, mut new_top_config_receiver) =
            watch::channel(top_config.clone());
        new_top_config_receiver.borrow_and_update();

        // we must wait for these to end on their own (and they need to subscribe to shutdown_sender)
        // TODO: is FuturesUnordered what we need? I want to return when the first one returns
        let important_background_handles: FuturesUnordered<Web3ProxyJoinHandle<()>> =
            FuturesUnordered::new();

        // make a http shared client
        // TODO: can we configure the connection pool? should we?
        // TODO: timeouts from config. defaults are hopefully good
        // TODO: is always disabling compression a good idea?
        let http_client = Some(
            reqwest::ClientBuilder::new()
                .connect_timeout(Duration::from_secs(5))
                .no_brotli()
                .no_deflate()
                .no_gzip()
                .timeout(Duration::from_secs(5 * 60 - 2))
                .user_agent(APP_USER_AGENT)
                .build()?,
        );

        let watch_consensus_head_receiver = watch_consensus_head_sender.subscribe();

        let chain_id = top_config.app.chain_id;

        // TODO: deduped_txid_firehose capacity from config
        let deduped_txid_firehose = DedupedBroadcaster::new(100, 20_000);

        // TODO: remove this. it should only be done by apply_top_config
        let (balanced_rpcs, balanced_handle, consensus_connections_watcher) = Web3Rpcs::spawn(
            Web3RpcsSpawnConfig::new(
                chain_id,
                top_config.app.max_head_block_lag,
                top_config.app.min_synced_rpcs,
                top_config.app.min_sum_soft_limit,
                top_config.app.block_cache_max_bytes,
            ),
            "balanced rpcs".into(),
            Some(watch_consensus_head_sender),
            Some(deduped_txid_firehose.clone()),
        )
        .await
        .web3_context("spawning balanced rpcs")?;

        // prepare a Web3Rpcs to hold all our private connections
        // only some chains have this, so this might be empty
        // TODO: set min_sum_soft_limit > 0 if any private rpcs are configured. this way we don't accidently leak to the public mempool if they are all offline
        let (private_rpcs, private_handle, _) = Web3Rpcs::spawn(
            Web3RpcsSpawnConfig::new(
                chain_id,
                // private rpcs don't get subscriptions, so no need for max_head_block_lag
                None,
                0,
                0,
                top_config.app.block_cache_max_bytes,
            ),
            "protected rpcs".into(),
            // subscribing to new heads here won't work well. if they are fast, they might be ahead of balanced_rpcs
            // they also often have low rate limits
            // however, they are well connected to miners/validators. so maybe using them as a safety check would be good
            // TODO: but maybe we could include privates in the "backup" tier
            None,
            None,
        )
        .await
        .web3_context("spawning private_rpcs")?;

        // prepare a Web3Rpcs to hold all our 4337 Abstraction Bundler connections (if any)
        let (bundler_4337_rpcs, bundler_4337_rpcs_handle, _) = Web3Rpcs::spawn(
            Web3RpcsSpawnConfig::new(
                chain_id,
                // bundler_4337_rpcs don't get subscriptions, so no need for max_head_block_lag
                None,
                0,
                0,
                top_config.app.block_cache_max_bytes,
            ),
            "eip4337 rpcs".into(),
            None,
            None,
        )
        .await
        .web3_context("spawning bundler_4337_rpcs")?;

        let hostname = hostname::get()
            .ok()
            .and_then(|x| x.to_str().map(|x| x.to_string()));

        let tx_subscriptions = Semaphore::new(1);

        let app = Self {
            balanced_rpcs,
            bundler_4337_rpcs,
            config: top_config.app.clone(),
            frontend_port: frontend_port.clone(),
            hostname,
            http_client,
            pending_txid_firehose: deduped_txid_firehose,
            protected_rpcs: private_rpcs,
            start: Instant::now(),
            watch_consensus_head_receiver,
            tx_subscriptions,
        };

        let app = Arc::new(app);

        if let Err(app) = APP.set(app.clone()) {
            error!(?app, "global APP can only be set once!");
        };

        // watch for config changes
        // TODO: move this to its own function/struct
        {
            let app = app.clone();
            let config_handle = tokio::spawn(async move {
                loop {
                    let new_top_config = new_top_config_receiver.borrow_and_update().to_owned();

                    // TODO: compare new and old here? the sender should be doing that already but maybe its better here

                    match app.apply_top_config_rpcs(&new_top_config).await {
                        Err(err) => {
                            error!(?err, "unable to apply config! Retrying in 10 seconds (or if the config changes)");

                            select! {
                                _ = config_watcher_shutdown_receiver.recv() => {
                                    break;
                                }
                                _ = sleep(Duration::from_secs(10)) => {}
                                _ = new_top_config_receiver.changed() => {}
                            }
                        }
                        _ => {
                            // configs applied successfully. wait for configs to change or for the app to exit
                            select! {
                                _ = config_watcher_shutdown_receiver.recv() => {
                                    break;
                                }
                                _ = new_top_config_receiver.changed() => {}
                            }
                        }
                    }

                    // TODO: add a min time between config changes
                    // TODO: is this yield actually helpful? i'm not so sure
                    yield_now().await;
                }

                Ok(())
            });

            important_background_handles.push(config_handle);
        }

        if important_background_handles.is_empty() {
            trace!("no important background handles");

            let f = tokio::spawn(async move {
                let _ = background_shutdown_receiver.recv().await;

                Ok(())
            });

            important_background_handles.push(f);
        }

        Ok(Web3ProxyAppSpawn {
            app,
            balanced_handle,
            private_handle,
            bundler_4337_rpcs_handle,
            background_handles: important_background_handles,
            new_top_config: Arc::new(new_top_config_sender),
            ranked_rpcs: consensus_connections_watcher,
        })
    }

    pub async fn apply_top_config(&self, new_top_config: &TopConfig) -> Web3ProxyResult<()> {
        // TODO: update self.config from new_top_config.app (or move it entirely to a global)
        self.apply_top_config_rpcs(new_top_config).await
    }

    async fn apply_top_config_rpcs(&self, new_top_config: &TopConfig) -> Web3ProxyResult<()> {
        info!("applying new config");

        let balanced = self
            .balanced_rpcs
            .apply_server_configs(self, &new_top_config.balanced_rpcs)
            .await
            .web3_context("updating balanced rpcs");

        let protected = self
            .protected_rpcs
            .apply_server_configs(self, &new_top_config.private_rpcs)
            .await
            .web3_context("updating private_rpcs");

        let bundler_4337 = self
            .bundler_4337_rpcs
            .apply_server_configs(self, &new_top_config.bundler_4337_rpcs)
            .await
            .web3_context("updating bundler_4337_rpcs");

        // TODO: log all the errors if there are multiple
        balanced?;
        protected?;
        bundler_4337?;

        Ok(())
    }

    pub fn head_block_receiver(&self) -> watch::Receiver<Option<BlockHeader>> {
        self.watch_consensus_head_receiver.clone()
    }

    /// send the request or batch of requests to the approriate RPCs
    pub async fn proxy_web3_rpc(
        self: &Arc<Self>,
        proxy_mode: ProxyMode,
        request: JsonRpcRequestEnum,
        request_id: Option<String>,
    ) -> Web3ProxyResult<(StatusCode, jsonrpc::Response, Vec<Arc<Web3Rpc>>)> {
        // trace!(?request, "proxy_web3_rpc");

        let response = match request {
            JsonRpcRequestEnum::Single(request) => {
                let (status_code, response, rpcs) = self
                    .proxy_request(request, proxy_mode, None, request_id)
                    .await;

                (status_code, jsonrpc::Response::Single(response), rpcs)
            }
            JsonRpcRequestEnum::Batch(requests) => {
                let (responses, rpcs) = self
                    .proxy_web3_rpc_requests(proxy_mode, requests, request_id)
                    .await?;

                // TODO: real status code. if an error happens, i don't think we are following the spec here
                (StatusCode::OK, jsonrpc::Response::Batch(responses), rpcs)
            }
        };

        Ok(response)
    }

    /// cut up the request and send to potentually different servers
    /// TODO: make sure this isn't a problem
    async fn proxy_web3_rpc_requests(
        self: &Arc<Self>,
        proxy_mode: ProxyMode,
        requests: Vec<SingleRequest>,
        request_id: Option<String>,
    ) -> Web3ProxyResult<(Vec<jsonrpc::ParsedResponse>, Vec<Arc<Web3Rpc>>)> {
        let num_requests = requests.len();

        if num_requests == 0 {
            return Ok((vec![], vec![]));
        }

        // get the head block now so that any requests that need it all use the same block
        // TODO: this still has an edge condition if there is a reorg in the middle of the request!!!
        let head_block: BlockHeader = self
            .balanced_rpcs
            .head_block()
            .ok_or(Web3ProxyError::NoServersSynced)?;

        if matches!(proxy_mode, ProxyMode::Best)
            && requests.len() > 1
            && requests.iter().all(|request| request.method == "eth_call")
        {
            if let Some(result) = self
                .proxy_eth_call_batch(&requests, &head_block, request_id.clone())
                .await
            {
                return result;
            }
        }

        // TODO: use streams and buffers so we don't overwhelm our server
        let responses = join_all(
            requests
                .into_iter()
                .map(|request| {
                    self.proxy_request(
                        request,
                        proxy_mode,
                        Some(head_block.clone()),
                        request_id.clone(),
                    )
                })
                .collect::<Vec<_>>(),
        )
        .await;

        let mut collected: Vec<jsonrpc::ParsedResponse> = Vec::with_capacity(num_requests);
        let mut collected_rpc_names: HashSet<String> = HashSet::new();
        let mut collected_rpcs: Vec<Arc<Web3Rpc>> = vec![];
        for response in responses {
            // TODO: any way to attach the tried rpcs to the error? it is likely helpful
            let (_status_code, response, rpcs) = response;

            // TODO: individual error handling
            collected.push(response.parsed().await?);
            collected_rpcs.extend(rpcs.into_iter().filter(|x| {
                if collected_rpc_names.contains(&x.name) {
                    false
                } else {
                    collected_rpc_names.insert(x.name.clone());
                    true
                }
            }));

            // TODO: what should we do with the status code? check the jsonrpc spec
        }

        Ok((collected, collected_rpcs))
    }

    async fn proxy_eth_call_batch(
        self: &Arc<Self>,
        requests: &[SingleRequest],
        head_block: &BlockHeader,
        request_id: Option<String>,
    ) -> Option<Web3ProxyResult<(Vec<jsonrpc::ParsedResponse>, Vec<Arc<Web3Rpc>>)>> {
        let mut validated_requests = Vec::with_capacity(requests.len());
        for request in requests.iter().cloned() {
            let validated = match ValidatedRequest::new_with_app(
                self,
                ProxyMode::Best,
                None,
                request.into(),
                Some(head_block.clone()),
                request_id.clone(),
            )
            .await
            {
                Ok(validated) => validated,
                Err(_) => return None,
            };
            validated_requests.push(validated);
        }

        if validated_requests
            .windows(2)
            .any(|pair| pair[0].request_blocks != pair[1].request_blocks)
        {
            return None;
        }

        let normalized_requests = validated_requests
            .iter()
            .map(|request| {
                request
                    .inner
                    .jsonrpc_request()
                    .expect("validated external request contains JSON-RPC data")
                    .clone()
            })
            .collect::<Vec<_>>();
        let rpcs = match self
            .balanced_rpcs
            .try_rpcs_for_request(&validated_requests[0])
            .await
        {
            Ok(rpcs) => rpcs,
            Err(error) => return Some(Err(error)),
        };
        let mut handles = rpcs.open_batch_handles().await;
        if handles.is_empty() {
            let stream = rpcs.to_stream();
            pin!(stream);
            let Some(handle) = stream.next().await else {
                return Some(Err(Web3ProxyError::NoServersSynced));
            };
            handles.push(handle);
        }

        let requests_per_backend = normalized_requests.len().div_ceil(handles.len());
        let batches = normalized_requests
            .chunks(requests_per_backend)
            .zip(handles)
            .map(|(requests, handle)| async move {
                let rpc = handle.clone_connection();
                handle
                    .request_batch(requests)
                    .await
                    .map(|responses| (rpc, responses))
            });
        let mut responses = Vec::with_capacity(normalized_requests.len());
        let mut response_rpcs = Vec::with_capacity(normalized_requests.len());
        let mut used_rpcs = Vec::new();
        for result in join_all(batches).await {
            let (rpc, batch_responses) = match result {
                Ok(result) => result,
                Err(error) => return Some(Err(error)),
            };
            response_rpcs.extend(std::iter::repeat_n(rpc.clone(), batch_responses.len()));
            used_rpcs.push(rpc);
            responses.extend(batch_responses);
        }
        for ((request, response), rpc) in validated_requests
            .iter()
            .zip(&responses)
            .zip(&response_rpcs)
        {
            request.response.lock().backend_rpcs.push(rpc.clone());
            let response_bytes = sonic_rs::to_string(response)
                .expect("JSON-RPC response must serialize")
                .len() as u64;
            request.set_response(response_bytes);
        }

        Some(Ok((responses, used_rpcs)))
    }

    /// try to send transactions to the best available rpcs with protected/private mempools
    /// if no protected rpcs are configured (and protected_only is false), then public rpcs are used instead
    /// TODO: should this return a B256 instead of an Arc<OwnedLazyValue>?
    async fn try_send_protected(
        self: &Arc<Self>,
        web3_request: &Arc<ValidatedRequest>,
        protected_only: bool,
    ) -> Web3ProxyResult<ResponseData<Arc<OwnedLazyValue>>> {
        // decode the transaction
        let params = web3_request
            .inner
            .params()
            .as_array()
            .ok_or_else(|| Web3ProxyError::BadRequest("Unable to get array from params".into()))?
            .first()
            .ok_or_else(|| Web3ProxyError::BadRequest("Unable to get item 0 from params".into()))?
            .as_str()
            .ok_or_else(|| {
                Web3ProxyError::BadRequest("Unable to get string from params item 0".into())
            })?;

        let bytes = Bytes::from_str(params)
            .map_err(|_| Web3ProxyError::BadRequest("Unable to parse params as bytes".into()))?;

        if bytes.is_empty() {
            return Err(Web3ProxyError::BadRequest("empty bytes".into()));
        }

        let tx = TxEnvelope::decode_2718_exact(bytes.as_ref()).map_err(|_| {
            Web3ProxyError::BadRequest("failed to parse rlp into transaction".into())
        })?;

        if let Some(chain_id) = tx.chain_id() {
            if self.config.chain_id != chain_id {
                return Err(Web3ProxyError::BadRequest(
                    format!(
                        "unexpected chain_id. {} != {}",
                        chain_id, self.config.chain_id
                    )
                    .into(),
                ));
            }
        }

        // TODO: return now if already confirmed
        // TODO: error if the nonce is way far in the future

        let response = if protected_only {
            if self.protected_rpcs.is_empty() {
                // TODO: different error?
                return Err(Web3ProxyError::NoServersSynced);
            }
            self.protected_rpcs
                .request_with_metadata(web3_request)
                .await
        } else if self.protected_rpcs.is_empty() {
            self.balanced_rpcs.request_with_metadata(web3_request).await
        } else {
            self.protected_rpcs
                .request_with_metadata(web3_request)
                .await
        };

        let mut response: ResponseData<Arc<OwnedLazyValue>> = response?.parsed().await?.into();

        let txid = tx.hash();

        // sometimes we get an error that the transaction is already known by our nodes,
        // that's not really an error. Return the hash like a successful response would.
        // TODO: move this to a helper function. probably part of try_send_protected
        if let ResponseData::RpcError { error_data, .. } = &response {
            let acceptable_error_messages = [
                "already known",
                "ALREADY_EXISTS: already known",
                "INTERNAL_ERROR: existing tx with same hash",
                "",
            ];
            if acceptable_error_messages.contains(&error_data.message.as_ref()) {
                response = ResponseData::from(json!(txid));
            }
        }

        // if successful, send the txid to the pending transaction firehose
        if let ResponseData::Result { value, .. } = &response {
            // no idea how we got an array here, but lets force this to just the txid
            // TODO: think about this more
            if value.is_array() {
                let backend_rpcs = web3_request.backend_rpcs_used();

                let backend_rpcs = backend_rpcs
                    .iter()
                    .map(|x| x.name.as_str())
                    .collect::<Vec<_>>();

                error!(
                    ?value,
                    ?txid,
                    ?backend_rpcs,
                    "unexpected array response from sendRawTransaction"
                );
                response = ResponseData::from(json!(txid));
            }

            self.pending_txid_firehose.send(*txid).await;
        }

        Ok(response)
    }

    /// proxy request with up to 3 tries.
    async fn proxy_request(
        self: &Arc<Self>,
        request: SingleRequest,
        proxy_mode: ProxyMode,
        head_block: Option<BlockHeader>,
        request_id: Option<String>,
    ) -> (StatusCode, jsonrpc::SingleResponse, Vec<Arc<Web3Rpc>>) {
        // TODO: this clone is only for an error response. refactor to not need it
        let error_id = request.id.clone();

        // TODO: think more about how to handle retries without hammering our servers with errors
        let mut ranked_rpcs_recv = self.balanced_rpcs.watch_ranked_rpcs.subscribe();

        let ranked_rpcs = ranked_rpcs_recv.borrow_and_update().clone();

        let head_block = if head_block.is_none() {
            ranked_rpcs.and_then(|x| x.head_block.clone())
        } else {
            head_block
        };

        let web3_request = match ValidatedRequest::new_with_app(
            self,
            proxy_mode,
            None,
            request.into(),
            head_block,
            request_id,
        )
        .await
        {
            Ok(x) => x,
            Err(err) => {
                // TODO: pass the original request into as_json_response_parts
                let (a, b) = err.as_json_response_parts(error_id, None::<RequestForError>);

                let rpcs = vec![];

                return (a, b, rpcs);
            }
        };

        let mut last_success = None;
        let mut last_error = None;

        let latest_start = sleep_until(Instant::now() + Duration::from_secs(3));
        pin!(latest_start);

        // TODO: how many retries?
        loop {
            // TODO: refresh the request here?

            // turn some of the Web3ProxyErrors into Ok results
            match self._proxy_request(&web3_request).await {
                Ok(response_data) => {
                    last_success = Some(response_data);
                    break;
                }
                Err(err) => {
                    last_error = Some(err);
                }
            }

            select! {
                _ = ranked_rpcs_recv.changed() => {
                    // TODO: pass these RankedRpcs to ValidatedRequest::new_with_app
                    ranked_rpcs_recv.borrow_and_update();
                }
                _ = &mut latest_start => {
                    // do not retry if we've already been trying for 3 seconds
                    break;
                }
            }

            // TODO: refresh the request?
        }

        let last_response = if let Some(last_success) = last_success {
            Ok(last_success)
        } else {
            Err(last_error.unwrap_or(anyhow::anyhow!("no success or error").into()))
        };

        let (code, response) = match last_response {
            Ok(response_data) => {
                let user_error_response = response_data.is_jsonrpc_err();

                let mut response_lock = web3_request.response.lock();

                // TODO: i really don't like this logic here. it should be inside add_response
                response_lock.error_response = false;

                // TODO: is it true that all jsonrpc errors are user errors?
                response_lock.user_error_response = user_error_response;

                drop(response_lock);

                (StatusCode::OK, response_data)
            }
            Err(err) => {
                // max tries exceeded. return the error

                let mut response_lock = web3_request.response.lock();

                // TODO: i really don't like this logic here. it should be inside add_error_response
                // TODO: provider errors should have already been handled, but our error types are too broad
                response_lock.error_response = true;
                response_lock.user_error_response = false;

                drop(response_lock);

                err.as_json_response_parts(web3_request.id(), Some(web3_request.as_ref()))
            }
        };

        web3_request.set_response(&response);

        let rpcs = web3_request.backend_rpcs_used();

        (code, response, rpcs)
    }

    /// Main request logic in a dedicated function so the try operator is easy to use.
    /// TODO: how can we make this generic?
    async fn _proxy_request(
        self: &Arc<Self>,
        web3_request: &Arc<ValidatedRequest>,
    ) -> Web3ProxyResult<jsonrpc::SingleResponse> {
        if let Some(request) = web3_request.inner.jsonrpc_request() {
            if let Some(response) = self.balanced_rpcs.cached_block_response(request).await {
                return Ok(response);
            }
        }

        // TODO: serve net_version without querying the backend
        // TODO: don't force OwnedLazyValue
        let response: jsonrpc::SingleResponse = match web3_request.inner.method() {
            // lots of commands are blocked
            method @ ("db_getHex"
            | "db_getString"
            | "db_putHex"
            | "db_putString"
            | "debug_accountRange"
            | "debug_backtraceAt"
            | "debug_blockProfile"
            | "debug_bundler_clearState"
            | "debug_bundler_dumpMempool"
            | "debug_bundler_sendBundleNow"
            | "debug_chaindbCompact"
            | "debug_chaindbProperty"
            | "debug_cpuProfile"
            | "debug_freeOSMemory"
            | "debug_freezeClient"
            | "debug_gcStats"
            | "debug_goTrace"
            | "debug_memStats"
            | "debug_mutexProfile"
            | "debug_setBlockProfileRate"
            | "debug_setGCPercent"
            | "debug_setHead"
            | "debug_setMutexProfileFraction"
            | "debug_standardTraceBadBlockToFile"
            | "debug_standardTraceBlockToFile"
            | "debug_startCPUProfile"
            | "debug_startGoTrace"
            | "debug_stopCPUProfile"
            | "debug_stopGoTrace"
            | "debug_writeBlockProfile"
            | "debug_writeMemProfile"
            | "debug_writeMutexProfile"
            | "erigon_cacheCheck"
            | "eth_compileLLL"
            | "eth_compileSerpent"
            | "eth_compileSolidity"
            | "eth_getCompilers"
            | "eth_sendTransaction"
            | "eth_sign"
            | "eth_signTransaction"
            | "eth_submitHashrate"
            | "eth_submitWork"
            | "les_addBalance"
            | "les_setClientParams"
            | "les_setDefaultParams"
            | "miner_setEtherbase"
            | "miner_setExtra"
            | "miner_setGasLimit"
            | "miner_setGasPrice"
            | "miner_start"
            | "miner_stop"
            | "personal_ecRecover"
            | "personal_importRawKey"
            | "personal_listAccounts"
            | "personal_lockAccount"
            | "personal_newAccount"
            | "personal_sendTransaction"
            | "personal_sign"
            | "personal_unlockAccount"
            | "shh_addToGroup"
            | "shh_getFilterChanges"
            | "shh_getMessages"
            | "shh_hasIdentity"
            | "shh_newFilter"
            | "shh_newGroup"
            | "shh_newIdentity"
            | "shh_post"
            | "shh_uninstallFilter"
            | "shh_version"
            | "wallet_getEthereumChains"
            | "wallet_getSnaps"
            | "wallet_requestSnaps") => {
                return Err(Web3ProxyError::MethodNotFound(method.to_owned().into()));
            }
            // TODO: implement these commands
            method @ ("eth_getFilterChanges"
            | "eth_getFilterLogs"
            | "eth_newBlockFilter"
            | "eth_newFilter"
            | "eth_newPendingTransactionFilter"
            | "eth_pollSubscriptions"
            | "eth_uninstallFilter") => {
                return Err(Web3ProxyError::MethodNotFound(method.to_owned().into()));
            }
            "eth_sendUserOperation"
            | "eth_estimateUserOperationGas"
            | "eth_getUserOperationByHash"
            | "eth_getUserOperationReceipt"
            | "eth_supportedEntryPoints"
            | "web3_bundlerVersion" => self.bundler_4337_rpcs
                        .try_proxy_connection::<Arc<OwnedLazyValue>>(
                            web3_request,
                        )
                        .await?,
            "eth_accounts" => jsonrpc::ParsedResponse::from_value(json!([]), web3_request.id()).into(),
            "eth_blockNumber" => {
                match web3_request.head_block.clone().or(self.balanced_rpcs.head_block()) {
                    Some(head_block) => jsonrpc::ParsedResponse::from_value(json!(head_block.number()), web3_request.id()).into(),
                    None => {
                        return Err(Web3ProxyError::NoServersSynced);
                    }
                }
            }
            "eth_chainId" => jsonrpc::ParsedResponse::from_value(json!(U64::from(self.config.chain_id)), web3_request.id()).into(),
            // TODO: eth_callBundle (https://docs.flashbots.net/flashbots-auction/searchers/advanced/rpc-endpoint#eth_callbundle)
            // TODO: eth_cancelPrivateTransaction (https://docs.flashbots.net/flashbots-auction/searchers/advanced/rpc-endpoint#eth_cancelprivatetransaction, but maybe just reject)
            // TODO: eth_sendPrivateTransaction (https://docs.flashbots.net/flashbots-auction/searchers/advanced/rpc-endpoint#eth_sendprivatetransaction)
            "eth_coinbase" => {
                // no need for serving coinbase
                jsonrpc::ParsedResponse::from_value(json!(Address::ZERO), web3_request.id()).into()
            }
            "eth_estimateGas" => {
                // TODO: timeout
                let mut gas_estimate = self
                    .balanced_rpcs
                    .try_proxy_connection::<U256>(
                        web3_request,
                    )
                    .await?
                    .parsed()
                    .await?
                    .into_result()?;

                let gas_increase = if let Some(gas_increase_percent) =
                    self.config.gas_increase_percent
                {
                    let gas_increase = gas_estimate * gas_increase_percent / U256::from(100);

                    let min_gas_increase = self.config.gas_increase_min.unwrap_or_default();

                    gas_increase.max(min_gas_increase)
                } else {
                    self.config.gas_increase_min.unwrap_or_default()
                };

                gas_estimate += gas_increase;

                let request_id = web3_request.id();

                // TODO: from_serializable?
                jsonrpc::ParsedResponse::from_value(json!(gas_estimate), request_id).into()
            }
            "eth_getTransactionReceipt" | "eth_getTransactionByHash" => {
                // try to get the transaction without specifying a min_block_height
                // TODO: timeout
                // TODO: change this to send serially until we get a success

                // TODO: validate params. we seem to get a lot of spam here of "0x"

                let mut result = self
                    .balanced_rpcs
                    .try_proxy_connection::<Arc<OwnedLazyValue>>(
                        web3_request,
                    )
                    .await;

                // TODO: helper for doing parsed() inside a result?
                if let Ok(SingleResponse::Stream(x)) = result {
                    result = x.read().await.map(SingleResponse::Parsed);
                }

                // if we got "null" or "", it is probably because the tx is old. retry on nodes with old block data
                // TODO: this feels fragile. how should we do this better/
                let try_archive = match &result {
                    Ok(SingleResponse::Parsed(x)) => {
                        match x.result().map(AsRef::as_ref) {
                            Some(value) if value.is_null() => true,
                            Some(value) if value.as_array().is_some_and(|x| x.is_empty()) => true,
                            Some(value) if value.as_str().is_some_and(str::is_empty) => true,
                            None => true,
                            Some(_) => false,
                        }
                    },
                    Ok(SingleResponse::Stream(..)) => unimplemented!(),
                    Err(..) => true,
                };

                if try_archive {
                    {
                        let mut response_lock = web3_request.response.lock();

                        // TODO: this is a hack. we don't usually want an archive
                        // we probably just hit a bug where a server said it had a block but it dosn't yet have all the transactions
                        response_lock
                            .archive_request
                            = true;
                    }

                    // TODO: if the transaction wasn't found, set archive_request back to false?

                    self
                        .balanced_rpcs
                        .try_proxy_connection::<Arc<OwnedLazyValue>>(
                            web3_request,
                        )
                        .await?
                } else {

                    // TODO: if result is an error, return a null instead?

                    result?
                }
            }
            // TODO: eth_gasPrice that does awesome magic to predict the future
            "eth_hashrate" => jsonrpc::ParsedResponse::from_value(json!(U64::ZERO), web3_request.id()).into(),
            "eth_mining" => jsonrpc::ParsedResponse::from_value(json!(false), web3_request.id()).into(),
            "eth_sendRawTransaction" => {
                // TODO: eth_sendPrivateTransaction that only sends private and never to balanced. it has different params though
                let x = self
                    .try_send_protected(
                        web3_request,false,
                    ).await?;

                jsonrpc::ParsedResponse::from_response_data(x, web3_request.id()).into()
            }
            "eth_syncing" => {
                // no stats on this. its cheap
                // TODO: return a real response if all backends are syncing or if no servers in sync
                // TODO: const
                jsonrpc::ParsedResponse::from_value(json!(false), web3_request.id()).into()
            }
            "eth_subscribe" => jsonrpc::ParsedResponse::from_error(JsonRpcErrorData {
                message: "notifications not supported. eth_subscribe is only available over a websocket".into(),
                code: -32601,
                data: None,
            }, web3_request.id()).into(),
            "eth_unsubscribe" => jsonrpc::ParsedResponse::from_error(JsonRpcErrorData {
                message: "notifications not supported. eth_unsubscribe is only available over a websocket".into(),
                code: -32601,
                data: None,
            }, web3_request.id()).into(),
            "net_listening" => {
                // TODO: only true if there are some backends on balanced_rpcs?
                // TODO: const
                jsonrpc::ParsedResponse::from_value(json!(true), web3_request.id()).into()
            }
            "net_peerCount" =>
                jsonrpc::ParsedResponse::from_value(json!(U64::from(self.balanced_rpcs.num_synced_rpcs())), web3_request.id()).into()
            ,
            "web3_clientVersion" =>
                jsonrpc::ParsedResponse::from_value(json!(APP_USER_AGENT), web3_request.id()).into()
            ,
            "web3_sha3" => {
                // returns Keccak-256 (not the standardized SHA3-256) of the given data.
                // TODO: timeout
                match web3_request.inner.params().as_array() {
                    Some(params) => {
                        // TODO: make a struct and use serde conversion to clean this up
                        if params.len() != 1
                            || !params.first().map(|x| x.is_str()).unwrap_or(false)
                        {
                            // TODO: what error code?
                            // TODO: use Web3ProxyError::BadRequest
                            jsonrpc::ParsedResponse::from_error(JsonRpcErrorData {
                                message: "Invalid request".into(),
                                code: -32600,
                                data: None
                            }, web3_request.id()).into()
                        } else {
                            // TODO: BadRequest instead of web3_context
                            let param = Bytes::from_str(
                                params[0]
                                    .as_str()
                                    .ok_or_else(|| {
                                        Web3ProxyError::BadRequest(
                                            "param 0 must contain hex bytes".into(),
                                        )
                                    })
                                    .web3_context("parsing params 0 into str then bytes")?,
                            )
                            .map_err(|x| {
                                trace!("bad request: {:?}", x);
                                Web3ProxyError::BadRequest(
                                    "param 0 could not be read as B256".into(),
                                )
                            })?;

                            let hash = B256::from(keccak256(param));

                            jsonrpc::ParsedResponse::from_value(json!(hash), web3_request.id()).into()
                        }
                    }
                    None => {
                        // TODO: this needs the correct error code in the response
                        // TODO: Web3ProxyError::BadRequest instead?
                        jsonrpc::ParsedResponse::from_error(JsonRpcErrorData {
                            message: "invalid request".into(),
                            code: StatusCode::BAD_REQUEST.as_u16().into(),
                            data: None,
                        }, web3_request.id()).into()
                    }
                }
            }
            "test" => jsonrpc::ParsedResponse::from_error(JsonRpcErrorData {
                message: "The method test does not exist/is not available.".into(),
                code: -32601,
                data: None,
            }, web3_request.id()).into(),
            // Send all other methods to a backend RPC.
            method => {
                if method.starts_with("admin_") {
                    // TODO: emit a stat? will probably just be noise
                    return Err(Web3ProxyError::AccessDenied("admin methods are not allowed".into()));
                }
                if method.starts_with("alchemy_") {
                    return Err(JsonRpcErrorData::from(format!(
                        "the method {} does not exist/is not available",
                        method
                    )).into());
                }
                let mut response = timeout_at(
                    web3_request.expire_at(),
                    self.balanced_rpcs
                        .try_proxy_connection::<Arc<OwnedLazyValue>>(web3_request),
                )
                .await??;

                response.set_id(web3_request.id());
                response
            }
        };

        Ok(response)
    }
}

impl fmt::Debug for App {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: the default formatter takes forever to write. this is too quiet though
        f.debug_struct("Web3ProxyApp").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::SingleRequest;
    use crate::rpcs::blockchain::{BlockResponseCacheKey, CachedBlockResponse};
    use alloy::rpc::types::Header;
    use sonic_rs::OwnedLazyValue;

    async fn app_with_no_backend() -> Arc<App> {
        let (balanced_rpcs, _handle, _) = Web3Rpcs::spawn(
            Web3RpcsSpawnConfig::new(1, None, 0, 0, 1_000_000),
            "cache-only-test".into(),
            None,
            None,
        )
        .await
        .unwrap();
        let (_, watch_consensus_head_receiver) = watch::channel(None);

        Arc::new(App {
            balanced_rpcs: balanced_rpcs.clone(),
            bundler_4337_rpcs: balanced_rpcs.clone(),
            config: AppConfig::default(),
            http_client: None,
            watch_consensus_head_receiver,
            pending_txid_firehose: DedupedBroadcaster::new(4, 16),
            hostname: None,
            frontend_port: Arc::new(AtomicU16::new(0)),
            protected_rpcs: balanced_rpcs,
            start: Instant::now(),
            tx_subscriptions: Semaphore::new(1),
        })
    }

    fn head(number: u64, hash: B256) -> BlockHeader {
        let mut header: Header = Header {
            hash,
            ..Default::default()
        };
        header.inner.number = number;
        BlockHeader::new(Arc::new(header))
    }

    #[tokio::test]
    async fn proxy_serves_all_cached_block_forms_without_a_backend() {
        let app = app_with_no_backend().await;
        let block_hash = B256::with_last_byte(0x42);
        let transaction_hash = B256::with_last_byte(0x11);
        let full_value = serde_json::json!({
            "hash": block_hash,
            "number": "0x2a",
            "transactions": [{"hash": transaction_hash, "providerTx": true}],
            "uncles": [],
            "withdrawals": [],
            "providerBlock": true,
        });
        let raw = Arc::new(sonic_rs::from_str::<OwnedLazyValue>(&full_value.to_string()).unwrap());
        let (full, hashes) = CachedBlockResponse::from_full(raw, block_hash).unwrap();
        app.balanced_rpcs
            .block_responses
            .insert(BlockResponseCacheKey::new(block_hash, true), full)
            .await;
        app.balanced_rpcs
            .block_responses
            .insert(BlockResponseCacheKey::new(block_hash, false), hashes)
            .await;
        app.balanced_rpcs
            .blocks_by_number
            .insert(U64::from(42), block_hash)
            .await;
        let head = head(42, block_hash);

        // TODO: use actual typeds here instead of formatting raw strings
        let requests = [
            format!(
                r#"{{"jsonrpc":"2.0","id":"hash-full","method":"eth_getBlockByHash","params":["{block_hash}",true]}}"#
            ),
            format!(
                r#"{{"jsonrpc":"2.0","id":"hash-hashes","method":"eth_getBlockByHash","params":["{block_hash}",false]}}"#
            ),
            r#"{"jsonrpc":"2.0","id":"number-full","method":"eth_getBlockByNumber","params":["0x2a",true]}"#.into(),
            r#"{"jsonrpc":"2.0","id":"latest-hashes","method":"eth_getBlockByNumber","params":["latest",false]}"#.into(),
        ];

        for request in requests {
            let request: SingleRequest = sonic_rs::from_str(&request).unwrap();
            let expected_id: serde_json::Value =
                serde_json::from_str(&sonic_rs::to_string(&request.id).unwrap()).unwrap();
            let full_transactions = request.params[1].as_bool().unwrap();
            let (status, response, backend_rpcs) = app
                .proxy_request(request, ProxyMode::Best, Some(head.clone()), None)
                .await;
            let response = response.parsed().await.unwrap();
            let response: serde_json::Value =
                serde_json::from_str(&sonic_rs::to_string(&response).unwrap()).unwrap();

            assert_eq!(status, StatusCode::OK);
            assert!(backend_rpcs.is_empty());
            assert_eq!(response["id"], expected_id);
            if full_transactions {
                assert_eq!(response["result"], full_value);
            } else {
                assert_eq!(
                    response["result"]["transactions"],
                    serde_json::json!([transaction_hash])
                );
                assert_eq!(response["result"]["providerBlock"], true);
            }
        }
    }
}
