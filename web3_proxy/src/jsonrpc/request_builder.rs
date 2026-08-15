use super::{JsonRpcParams, LooseId, SingleRequest};
use crate::{
    app::App,
    block_number::RequestBlocks,
    errors::{Web3ProxyError, Web3ProxyResult},
    frontend::{
        authorization::{Authorization, RequestOrMethod, ResponseOrBytes},
        rpc_proxy_ws::ProxyMode,
    },
    globals::APP,
    rpcs::{blockchain::BlockHeader, one::Web3Rpc},
};
use alloy::primitives::U64;
use anyhow::Context;
use chrono::Utc;
use derivative::Derivative;
use parking_lot::Mutex;
use serde::{ser::SerializeStruct, Serialize};
use sonic_rs::{json, OwnedLazyValue};
use std::time::Duration;
use std::{borrow::Cow, sync::Arc};
use std::{
    fmt::{self, Display},
    net::IpAddr,
};
use tokio::{sync::OwnedSemaphorePermit, time::Instant};

#[derive(Derivative)]
#[derivative(Default)]
pub struct RequestBuilder {
    app: Option<Arc<App>>,
    archive_request: bool,
    head_block: Option<BlockHeader>,
    authorization: Option<Arc<Authorization>>,
    request_or_method: RequestOrMethod,
}

impl RequestBuilder {
    pub fn new(app: Arc<App>) -> Self {
        let head_block = app.head_block_receiver().borrow().clone();

        Self {
            app: Some(app),
            head_block,
            ..Default::default()
        }
    }

    pub fn authorize_internal(self) -> Web3ProxyResult<Self> {
        // TODO: allow passing proxy_mode to internal?
        let authorization = Authorization::internal();

        Ok(Self {
            authorization: Some(Arc::new(authorization)),
            ..self
        })
    }

    /// TODO: this takes a lot more things
    pub fn authorize_public(self, ip: &IpAddr, proxy_mode: ProxyMode) -> Web3ProxyResult<Self> {
        let app = self
            .app
            .as_ref()
            .context("app is required for public requests")?;

        let authorization =
            Authorization::external(ip, proxy_mode, app.config.public_max_concurrent_requests);

        let head_block = app.watch_consensus_head_receiver.borrow().clone();

        Ok(Self {
            authorization: Some(Arc::new(authorization)),
            head_block,
            ..self
        })
    }

    pub fn set_archive_request(self, archive_request: bool) -> Self {
        Self {
            archive_request,
            ..self
        }
    }

    /// replace 'latest' in the json and figure out the minimum and maximum blocks.
    /// also tarpit invalid methods.
    pub async fn set_request(self, request: SingleRequest) -> Web3ProxyResult<Self> {
        Ok(Self {
            request_or_method: RequestOrMethod::Request(request),
            ..self
        })
    }

    pub fn set_method(self, method: Cow<'static, str>, size: usize) -> Self {
        Self {
            request_or_method: RequestOrMethod::Method(method, size),
            ..self
        }
    }

    pub async fn build(
        &self,
        max_wait: Option<Duration>,
    ) -> Web3ProxyResult<Arc<ValidatedRequest>> {
        // TODO: make this work without app being set?
        let app = self.app.as_ref().context("app is required")?;

        let authorization = self
            .authorization
            .clone()
            .context("authorization is required")?;

        let permit = authorization.permit(app).await?;

        let x = ValidatedRequest::new_with_app(
            app,
            authorization,
            max_wait,
            permit,
            self.request_or_method.clone(),
            self.head_block.clone(),
            None,
        )
        .await;

        if let Ok(x) = &x {
            if self.archive_request {
                let mut response_lock = x.response.lock();

                response_lock.archive_request = true;
            }
        }

        // todo!(anything else to set?)

        x
    }
}

#[derive(Debug, Default)]
/// todo: better name.
/// the inside bits for ValidatedRequest. It's usually in an Arc, so it's not mutable
pub struct ValidatedResponse {
    /// TODO: set archive_request during the new instead of after
    /// TODO: this is more complex than "requires a block older than X height". different types of data can be pruned differently
    pub archive_request: bool,

    /// RPC servers used by this request.
    pub backend_rpcs: Vec<Arc<Web3Rpc>>,

    /// The number of times the request got stuck waiting because no servers were synced
    pub no_servers: u64,

    /// If handling the request hit an application error
    /// This does not count things like a transcation reverting or a malformed request
    /// TODO: this will need more thought once we support other ProxyMode
    pub error_response: bool,

    /// Size in bytes of the JSON response. Does not include headers or things like that.
    pub response_bytes: u64,

    /// How many milliseconds it took to respond to the request
    pub response_millis: u64,

    /// What time the (first) response was proxied.
    /// TODO: think about how to store response times for ProxyMode::Versus
    pub response_timestamp: i64,

    /// If the request is invalid or received a jsonrpc error response (excluding reverts)
    pub user_error_response: bool,
}

impl ValidatedResponse {
    /// True if the response required querying a backup RPC
    /// RPC aggregators that query multiple providers to compare response may use this header to ignore our response.
    pub fn response_from_backup_rpc(&self) -> bool {
        self.backend_rpcs.last().map(|x| x.backup).unwrap_or(false)
    }
}

/// TODO:
/// TODO: instead of a bunch of atomics, this should probably use a RwLock. need to think more about how parallel requests are going to work though
#[derive(Debug, Derivative)]
#[derivative(Default)]
pub struct ValidatedRequest {
    pub authorization: Arc<Authorization>,

    pub request_blocks: RequestBlocks,

    /// TODO: this should probably be in a global config. although maybe if we run multiple chains in one process this will be useful
    pub chain_id: u64,

    pub head_block: Option<BlockHeader>,

    pub response: Mutex<ValidatedResponse>,

    pub inner: RequestOrMethod,

    // TODO: everything under here should be behind a single lock. all these atomics need to be updated together!
    /// Instant that the request was received (or at least close to it)
    /// We use Instant and not timestamps to avoid problems with leap seconds and similar issues
    #[derivative(Default(value = "Instant::now()"))]
    pub start_instant: Instant,

    /// How long to spend waiting for an rpc that can serve this request
    pub connect_timeout: Duration,
    /// How long to spend waiting for an rpc to respond to this request
    /// TODO: this should start once the connection is established
    pub expire_timeout: Duration,

    /// Limit the number of concurrent requests from a client.
    pub permit: Option<OwnedSemaphorePermit>,

    /// RequestId from x-amzn-trace-id or generated
    pub request_id: Option<String>,
}

impl Display for ValidatedRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.inner.method(),
            sonic_rs::to_string(self.inner.params()).expect("this should always serialize")
        )
    }
}

impl Serialize for ValidatedRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("request", 6)?;

        state.serialize_field("chain_id", &self.chain_id)?;

        state.serialize_field("head_block", &self.head_block)?;
        state.serialize_field("request", &self.inner)?;

        state.serialize_field("elapsed", &self.start_instant.elapsed().as_secs_f32())?;

        let response_lock = self.response.lock();

        state.serialize_field("archive_request", &response_lock.archive_request)?;

        {
            let backend_names = response_lock
                .backend_rpcs
                .iter()
                .map(|x| x.name.as_str())
                .collect::<Vec<_>>();

            state.serialize_field("backend_requests", &backend_names)?;
        }

        state.serialize_field("response_bytes", &response_lock.response_bytes)?;

        drop(response_lock);

        state.end()
    }
}

/// TODO: move all of this onto RequestBuilder
impl ValidatedRequest {
    #[allow(clippy::too_many_arguments)]
    async fn new_with_options(
        app: Option<&App>,
        authorization: Arc<Authorization>,
        chain_id: u64,
        head_block: Option<BlockHeader>,
        max_wait: Option<Duration>,
        permit: Option<OwnedSemaphorePermit>,
        mut request: RequestOrMethod,
        request_id: Option<String>,
    ) -> Web3ProxyResult<Arc<Self>> {
        let start_instant = Instant::now();

        let request_blocks = if head_block.is_none() {
            RequestBlocks::None
        } else {
            // TODO: wait for a future block if one is requested and update head_block too.
            match &mut request {
                RequestOrMethod::Request(x) => {
                    RequestBlocks::new(x, head_block.as_ref(), app).await?
                }
                _ => RequestBlocks::None,
            }
        };

        // TODO: what should we do if we want a really short max_wait?
        let connect_timeout = Duration::from_secs(10);

        let expire_timeout = max_wait
            .unwrap_or_else(|| Duration::from_secs(60))
            .max(connect_timeout);

        let x = Self {
            response: Mutex::new(Default::default()),
            authorization,
            request_blocks,
            chain_id,
            connect_timeout,
            expire_timeout,
            head_block: head_block.clone(),
            inner: request,
            permit,
            start_instant,
            request_id,
        };

        Ok(Arc::new(x))
    }

    /// todo!(this shouldn't be public. use the RequestBuilder)
    pub async fn new_with_app(
        app: &App,
        authorization: Arc<Authorization>,
        max_wait: Option<Duration>,
        permit: Option<OwnedSemaphorePermit>,
        request: RequestOrMethod,
        head_block: Option<BlockHeader>,
        request_id: Option<String>,
    ) -> Web3ProxyResult<Arc<Self>> {
        let chain_id = app.config.chain_id;

        Self::new_with_options(
            Some(app),
            authorization,
            chain_id,
            head_block,
            max_wait,
            permit,
            request,
            request_id,
        )
        .await
    }

    pub async fn new_internal<P: JsonRpcParams>(
        method: Cow<'static, str>,
        params: &P,
        head_block: Option<BlockHeader>,
        max_wait: Option<Duration>,
    ) -> Web3ProxyResult<Arc<Self>> {
        let authorization = Arc::new(Authorization::internal());

        // todo!(we need a real id! increment a counter on the app or websocket-only providers are going to have a problem)
        let id = LooseId::Number(1);

        // TODO: this seems inefficient
        let request = SingleRequest::new(id, method, json!(params)).unwrap();

        if let Some(app) = APP.get() {
            Self::new_with_app(
                app,
                authorization,
                max_wait,
                None,
                request.into(),
                head_block,
                None,
            )
            .await
        } else {
            Self::new_with_options(
                None,
                authorization,
                0,
                head_block,
                max_wait,
                None,
                request.into(),
                None,
            )
            .await
        }
    }

    #[inline]
    pub fn backend_rpcs_used(&self) -> Vec<Arc<Web3Rpc>> {
        let response_lock = self.response.lock();

        response_lock.backend_rpcs.clone()
    }

    #[inline]
    pub fn id(&self) -> OwnedLazyValue {
        self.inner.id()
    }

    #[inline]
    pub fn max_block_needed(&self) -> Option<U64> {
        if let Some(to_block) = self.request_blocks.to_block() {
            Some(to_block.num())
        } else {
            self.head_block
                .as_ref()
                .map(|head_block| head_block.number())
        }
    }

    #[inline]
    pub fn min_block_needed(&self) -> Option<U64> {
        let min_block_needed = self.request_blocks.from_block().map(|x| x.num());

        match min_block_needed {
            Some(x) => Some(x),
            None => {
                let response_lock = self.response.lock();

                if response_lock.archive_request {
                    Some(U64::ZERO)
                } else {
                    None
                }
            }
        }
    }

    #[inline]
    pub fn connect_timeout_at(&self) -> Instant {
        self.start_instant + self.connect_timeout
    }

    #[inline]
    pub fn connect_timeout(&self) -> bool {
        self.connect_timeout_at() <= Instant::now()
    }

    #[inline]
    pub fn expire_at(&self) -> Instant {
        // TODO: get from config
        // erigon's timeout is 5 minutes so we want it shorter than that
        self.start_instant + self.expire_timeout
    }

    #[inline]
    pub fn expired(&self) -> bool {
        self.expire_at() <= Instant::now()
    }

    pub fn set_error_response(&self, _err: &Web3ProxyError) {
        {
            let mut response_lock = self.response.lock();

            response_lock.error_response = true;
            response_lock.user_error_response = false;
        }

        // TODO: add the actual response size
        self.set_response(0);
    }

    pub fn set_response<'a, R: Into<ResponseOrBytes<'a>>>(&'a self, response: R) {
        // TODO: fetch? set? should it be None in a Mutex? or a OnceCell?
        let response = response.into();

        let num_bytes = response.num_bytes();

        let response_millis = self.start_instant.elapsed().as_millis() as u64;

        let now = Utc::now().timestamp();

        {
            let mut response_lock = self.response.lock();

            // TODO: set user_error_response and error_response here instead of outside this function

            response_lock.response_bytes = num_bytes;

            response_lock.response_millis = response_millis;

            response_lock.response_timestamp = now;
        }
    }

    #[inline]
    pub fn proxy_mode(&self) -> ProxyMode {
        self.authorization.checks.proxy_mode
    }

    // TODO: helper function to duplicate? needs to clear request_bytes, and all the atomics tho...
}
