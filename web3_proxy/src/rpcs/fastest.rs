//! Race the highest-ranked eligible backends using their ordinary transport permits.
use super::{
    many::Web3Rpcs,
    one::Web3Rpc,
    request::{OpenRequestHandle, OpenRequestResult},
};
use crate::{
    errors::{Web3ProxyError, Web3ProxyResult},
    jsonrpc::{JsonRpcResultData, ResponsePayload, SingleResponse, ValidatedRequest},
};
use futures::{stream::FuturesUnordered, StreamExt};
use hashbrown::HashSet;
use std::{future::Future, sync::Arc};
use tokio::time::{sleep_until, Instant};

async fn complete<R, Fut>(
    rpc: Arc<Web3Rpc>,
    response: Fut,
    request: &Arc<ValidatedRequest>,
) -> (Arc<Web3Rpc>, Web3ProxyResult<SingleResponse<R>>)
where
    R: JsonRpcResultData,
    Fut: Future<Output = Web3ProxyResult<SingleResponse<R>>>,
{
    let result = async {
        let response = response.await?.parsed().await?;
        // Compare JSON identities, including escaped string IDs.
        let actual: sonic_rs::Value = sonic_rs::from_str(&sonic_rs::to_string(&response.id)?)?;
        let expected: sonic_rs::Value = sonic_rs::from_str(&sonic_rs::to_string(&request.id())?)?;
        if actual != expected {
            return Err(anyhow::anyhow!("backend response ID mismatch").into());
        }
        if let ResponsePayload::Error { error } = &response.payload {
            if !error.is_execution_revert() {
                return Err(Web3ProxyError::JsonRpcErrorData(error.clone()));
            }
        }
        Ok(response.into())
    }
    .await;
    (rpc, result)
}

impl Web3Rpcs {
    pub(super) async fn fastest_with<R, F, Fut>(
        &self,
        request: &Arc<ValidatedRequest>,
        count: usize,
        send: F,
    ) -> Web3ProxyResult<SingleResponse<R>>
    where
        R: JsonRpcResultData,
        F: Fn(OpenRequestHandle) -> Fut,
        Fut: Future<Output = Web3ProxyResult<SingleResponse<R>>>,
    {
        let limit = if count == 0 { usize::MAX } else { count };
        let mut updates = self.watch_ranked_rpcs.subscribe();
        let mut watching = true;
        let mut pending = FuturesUnordered::new();
        let mut active = HashSet::new();
        let mut exhausted = HashSet::new();
        let mut first_error = None;
        let mut opened_any = !request.backend_rpcs_used().is_empty();
        loop {
            if request.expired() {
                return Err(Web3ProxyError::Timeout(None));
            }
            if !opened_any && request.connect_timeout() {
                break;
            }

            let mut capacity = FuturesUnordered::new();
            let mut sync = FuturesUnordered::new();
            let mut retry_at = None;
            if pending.len() < limit {
                let candidates = self.try_rpcs_for_request(request).await;
                if let Ok(candidates) = candidates {
                    for rpc in candidates.connections() {
                        if pending.len() >= limit {
                            break;
                        }
                        if active.contains(&rpc.name) || exhausted.contains(&rpc.name) {
                            continue;
                        }
                        match rpc.try_request_handle(request, None, false).await {
                            Ok(OpenRequestResult::Handle(handle)) => {
                                opened_any = true;
                                active.insert(rpc.name.clone());
                                pending.push(complete(rpc, send(handle), request));
                            }
                            Ok(OpenRequestResult::Busy(wait)) => {
                                opened_any = true;
                                capacity.push(wait);
                            }
                            Ok(OpenRequestResult::Lagged(wait)) => sync.push(wait),
                            Ok(OpenRequestResult::RetryAt(at)) => {
                                retry_at = Some(retry_at.map_or(at, |old: Instant| old.min(at)));
                            }
                            Ok(OpenRequestResult::Failed) | Err(_) => {}
                        }
                    }
                } else if pending.is_empty() {
                    break;
                }
            }
            if pending.is_empty() && capacity.is_empty() && sync.is_empty() && retry_at.is_none() {
                break;
            }
            let deadline = if opened_any {
                request.expire_at()
            } else {
                request.connect_timeout_at()
            };
            let wake_at = retry_at.unwrap_or(deadline).min(deadline);
            tokio::select! {
                // Prefer a completed response before admitting a replacement.
                biased;
                result = pending.next(), if !pending.is_empty() => {
                    let (rpc, result) = result.expect("pending attempt");
                    active.remove(&rpc.name);
                    match result {
                        Ok(response) => return Ok(response),
                        Err(error) => {
                            // The ordinary admission path handles cooldown retries.
                            // Exhaust terminal errors, but not a stale reserved handle.
                            if rpc.can_submit(request, false) && !matches!(error, Web3ProxyError::NoHandleReady) {
                                exhausted.insert(rpc.name.clone());
                            }
                            first_error.get_or_insert(error);
                        }
                    }
                }
                handle = capacity.next(), if pending.len() < limit && !capacity.is_empty() => {
                    if let Some(Ok(handle)) = handle {
                        let rpc = handle.clone_connection();
                        let eligible = self.try_rpcs_for_request(request).await.ok().is_some_and(|candidates| {
                            candidates.connections().iter().any(|candidate| Arc::ptr_eq(candidate, &rpc))
                        });
                        if eligible && rpc.can_submit(request, false) {
                            active.insert(rpc.name.clone());
                            pending.push(complete(rpc, send(handle), request));
                        }
                    }
                }
                _ = sync.next(), if pending.len() < limit && !sync.is_empty() => {}
                changed = updates.changed(), if watching => { watching = changed.is_ok(); }
                _ = sleep_until(wake_at) => {
                    if Instant::now() >= deadline {
                        return Err(Web3ProxyError::Timeout(None));
                    }
                }
            }
        }
        Err(match first_error {
            Some(error) => Web3ProxyError::ExhaustedBackends(Box::new(error)),
            None => Web3ProxyError::NoServersSynced,
        })
    }
}
