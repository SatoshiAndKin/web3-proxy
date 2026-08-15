//! Authorization and local concurrency limits for public and internal requests.

use super::rpc_proxy_ws::ProxyMode;
use crate::app::App;
use crate::errors::{RequestForError, Web3ProxyError, Web3ProxyResult};
use crate::jsonrpc::{self, SingleRequest};
use derive_more::From;
use serde::Serialize;
use serde_json::value::RawValue;
use std::borrow::Cow;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq)]
pub enum AuthorizationType {
    Internal,
    Remote,
}

#[derive(Clone, Debug, Default, From)]
pub struct AuthorizationChecks {
    pub max_concurrent_requests: Option<usize>,
    pub proxy_mode: ProxyMode,
}

#[derive(Clone, Debug)]
pub struct Authorization {
    pub checks: AuthorizationChecks,
    pub ip: IpAddr,
    pub authorization_type: AuthorizationType,
}

#[derive(Clone, Debug, Default, From, Serialize)]
pub enum RequestOrMethod {
    Request(SingleRequest),
    Method(Cow<'static, str>, usize),
    #[default]
    None,
}

impl RequestOrMethod {
    pub fn id(&self) -> Box<RawValue> {
        match self {
            Self::Request(request) => request.id.clone(),
            Self::Method(_, _) | Self::None => Default::default(),
        }
    }

    pub fn method(&self) -> &str {
        match self {
            Self::Request(request) => request.method.as_ref(),
            Self::Method(method, _) => method,
            Self::None => "unknown",
        }
    }

    pub fn params(&self) -> &serde_json::Value {
        match self {
            Self::Request(request) => &request.params,
            Self::Method(..) | Self::None => &serde_json::Value::Null,
        }
    }

    pub fn jsonrpc_request(&self) -> Option<&SingleRequest> {
        match self {
            Self::Request(request) => Some(request),
            Self::Method(..) | Self::None => None,
        }
    }

    pub fn num_bytes(&self) -> usize {
        match self {
            Self::Request(request) => request.num_bytes(),
            Self::Method(_, num_bytes) => *num_bytes,
            Self::None => 0,
        }
    }
}

#[derive(From)]
pub enum ResponseOrBytes<'a> {
    Json(&'a serde_json::Value),
    Response(&'a jsonrpc::SingleResponse),
    Error(&'a Web3ProxyError),
    Bytes(u64),
}

impl ResponseOrBytes<'_> {
    pub fn num_bytes(&self) -> u64 {
        match self {
            Self::Json(value) => serde_json::to_string(value)
                .expect("JSON values must serialize")
                .len() as u64,
            Self::Response(response) => response.num_bytes(),
            Self::Bytes(num_bytes) => *num_bytes,
            Self::Error(error) => error
                .as_response_parts(None::<RequestForError>)
                .1
                .num_bytes(),
        }
    }
}

impl Default for Authorization {
    fn default() -> Self {
        Self::internal()
    }
}

impl Authorization {
    pub fn internal() -> Self {
        Self {
            checks: AuthorizationChecks::default(),
            ip: IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            authorization_type: AuthorizationType::Internal,
        }
    }

    pub fn external(
        ip: &IpAddr,
        proxy_mode: ProxyMode,
        max_concurrent_requests: Option<usize>,
    ) -> Self {
        Self {
            checks: AuthorizationChecks {
                max_concurrent_requests,
                proxy_mode,
            },
            ip: *ip,
            authorization_type: AuthorizationType::Remote,
        }
    }

    pub async fn permit(&self, app: &App) -> Web3ProxyResult<Option<OwnedSemaphorePermit>> {
        app.permit_public_concurrency(&self.ip).await
    }

    pub async fn check_again(
        &self,
        app: &Arc<App>,
    ) -> Web3ProxyResult<(Arc<Self>, Option<OwnedSemaphorePermit>)> {
        let authorization = ip_is_authorized(app, &self.ip, self.checks.proxy_mode).await?;
        let permit = app.permit_public_concurrency(&self.ip).await?;

        Ok((Arc::new(authorization), permit))
    }
}

pub async fn ip_is_authorized(
    app: &Arc<App>,
    ip: &IpAddr,
    proxy_mode: ProxyMode,
) -> Web3ProxyResult<Authorization> {
    if ip.is_loopback() {
        return Ok(Authorization::internal());
    }

    Ok(Authorization::external(
        ip,
        proxy_mode,
        app.config.public_max_concurrent_requests,
    ))
}

impl App {
    pub async fn permit_public_concurrency(
        &self,
        ip: &IpAddr,
    ) -> Web3ProxyResult<Option<OwnedSemaphorePermit>> {
        let Some(max_concurrent_requests) = self.config.public_max_concurrent_requests else {
            return Ok(None);
        };

        let semaphore = self
            .ip_semaphores
            .get_with_by_ref(ip, async {
                Arc::new(Semaphore::new(max_concurrent_requests))
            })
            .await;

        Ok(Some(semaphore.acquire_owned().await?))
    }
}
