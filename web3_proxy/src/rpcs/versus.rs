//! Compare a fixed set of consensus backends without cancelling slower attempts.
use super::{
    consensus::RankedRpcs,
    many::Web3Rpcs,
    one::Web3Rpc,
    request::{OpenRequestHandle, OpenRequestResult},
};
use crate::{
    errors::{Web3ProxyError, Web3ProxyResult},
    jsonrpc::{JsonRpcResultData, ResponsePayload, SingleResponse, ValidatedRequest},
};
use futures::{stream::FuturesUnordered, StreamExt};
use std::{future::Future, sync::Arc};
use tokio::{
    sync::{oneshot, watch},
    time::{sleep, sleep_until, Duration},
};

fn eligible(
    rankings: &watch::Receiver<Option<Arc<RankedRpcs>>>,
    request: &Arc<ValidatedRequest>,
    rpc: &Arc<Web3Rpc>,
) -> bool {
    rankings
        .borrow()
        .as_ref()
        .and_then(|ranked| ranked.for_request(request))
        .is_some_and(|candidates| {
            candidates
                .connections()
                .iter()
                .any(|node| Arc::ptr_eq(node, rpc))
        })
}

/// Admission can wait and recheck state. Invoke this node's transport once;
/// Alloy may retry internally on the same backend before that invocation finishes.
async fn attempt<R, F, Fut>(
    rpc: Arc<Web3Rpc>,
    request: &Arc<ValidatedRequest>,
    mut rankings: watch::Receiver<Option<Arc<RankedRpcs>>>,
    send: &F,
) -> Web3ProxyResult<SingleResponse<R>>
where
    R: JsonRpcResultData,
    F: Fn(OpenRequestHandle) -> Fut,
    Fut: Future<Output = Web3ProxyResult<SingleResponse<R>>>,
{
    loop {
        if request.expired() {
            return Err(Web3ProxyError::Timeout(None));
        }
        if !eligible(&rankings, request, &rpc) {
            return Err(Web3ProxyError::NoServersSynced);
        }
        let handle = match rpc.try_request_handle(request, None, false).await? {
            OpenRequestResult::Handle(handle) => handle,
            OpenRequestResult::Busy(wait) => {
                tokio::select! {
                    result = wait => result?,
                    changed = rankings.changed() => {
                        changed.map_err(anyhow::Error::from)?;
                        continue;
                    },
                }
            }
            OpenRequestResult::RetryAt(at) => {
                tokio::select! {
                    _ = sleep_until(at) => {},
                    changed = rankings.changed() => { changed.map_err(anyhow::Error::from)?; },
                }
                continue;
            }
            OpenRequestResult::Lagged(_) | OpenRequestResult::Failed => {
                return Err(Web3ProxyError::NoServersSynced);
            }
        };
        if !eligible(&rankings, request, &rpc) {
            return Err(Web3ProxyError::NoServersSynced);
        }
        if !rpc.can_submit(request, false) {
            drop(handle);
            continue;
        }
        // No await separates the eligibility check from the transport submission.
        let response = send(handle).await?;
        let SingleResponse::Parsed(parsed) = &response;
        if let ResponsePayload::Error { error } = &parsed.payload {
            if !error.is_execution_revert() {
                return Err(Web3ProxyError::JsonRpcErrorData(error.clone()));
            }
        }
        return Ok(response);
    }
}

impl Web3Rpcs {
    pub(super) async fn versus_with<R, F, Fut>(
        &self,
        request: &Arc<ValidatedRequest>,
        send: F,
    ) -> Web3ProxyResult<SingleResponse<R>>
    where
        R: JsonRpcResultData,
        F: Fn(OpenRequestHandle) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Web3ProxyResult<SingleResponse<R>>> + Send,
    {
        // Capture membership once. A later ranking update can remove candidates,
        // but it cannot add a new node to this comparison.
        let selected = self.try_rpcs_for_request(request).await?.connections();
        let rankings = self.watch_ranked_rpcs.subscribe();
        let request = request.clone();
        let mut shutdown = self.frontend_shutdown.clone();
        let (reply, response) = oneshot::channel();
        self.frontend_tasks.spawn(async move {
            let mut reply = Some(reply);
            let mut first_error = None;
            let mut pending: FuturesUnordered<_> = selected
                .into_iter()
                .map(|rpc| attempt(rpc, &request, rankings.clone(), &send))
                .collect();
            let drain = async {
                let _ = shutdown.wait_for(|closing| *closing).await;
                sleep(Duration::from_secs(20)).await;
            };
            tokio::pin!(drain);
            while !pending.is_empty() {
                tokio::select! {
                    biased;
                    _ = sleep_until(request.expire_at()) => {
                        first_error = Some(Web3ProxyError::Timeout(None));
                        break;
                    }
                    _ = &mut drain => {
                        first_error = Some(Web3ProxyError::Timeout(None));
                        break;
                    }
                    result = pending.next() => {
                        match result.expect("pending comparison attempt") {
                            Ok(answer) => {
                                if let Some(reply) = reply.take() {
                                    // A disconnected client does not cancel the comparison.
                                    let _ = reply.send(Ok(answer));
                                }
                            }
                            Err(error) => { first_error.get_or_insert(error); }
                        }
                    }
                }
            }
            // Drop outstanding work and its permits before completing task tracking.
            drop(pending);
            if let Some(reply) = reply {
                let error = first_error.unwrap_or(Web3ProxyError::NoServersSynced);
                let _ = reply.send(Err(Web3ProxyError::ExhaustedBackends(Box::new(error))));
            }
        });
        response.await.map_err(|error| {
            Web3ProxyError::ExhaustedBackends(Box::new(anyhow::Error::from(error).into()))
        })?
    }
}
