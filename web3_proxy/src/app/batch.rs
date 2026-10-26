use super::{weighted_batch_lengths, App};
use crate::errors::{Web3ProxyError, Web3ProxyResult};
use crate::jsonrpc::{ParsedResponse, ResponsePayload, SingleResponse, ValidatedRequest};
use crate::rpcs::one::Web3Rpc;
use crate::rpcs::request::{OpenRequestHandle, OpenRequestResult};
use futures::{future::select_all, stream::FuturesUnordered, StreamExt};
use hashbrown::HashSet;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::Arc;
use tokio::time::{sleep_until, Instant};

struct BatchCall {
    request: Arc<ValidatedRequest>,
    response: Option<ParsedResponse>,
    last_error: Option<Arc<Web3ProxyError>>,
    // A method/history error exhausts this backend for this call. Transient
    // errors use the backend's shared cooldown and can recover after it ends.
    exhausted: HashSet<String>,
}

impl BatchCall {
    fn finish(&mut self, response: Web3ProxyResult<ParsedResponse>) {
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                self.request.set_error_response(&error);
                match error
                    .as_json_response_parts(self.request.id(), Some(self.request.as_ref()))
                    .1
                {
                    SingleResponse::Parsed(response) => response,
                }
            }
        };
        {
            let mut state = self.request.response.lock();
            state.user_error_response =
                !state.error_response && matches!(response.payload, ResponsePayload::Error { .. });
        }
        self.request.set_response(
            sonic_rs::to_string(&response)
                .expect("response serializes")
                .len() as u64,
        );
        self.response = Some(response);
    }
}

// Membership lets an expired call leave the queue without scanning unrelated
// calls. Deadlines never change on retry: keep one heap entry per original call,
// including submitted calls, until its deadline. Successful packets need no heap
// removals, and a retry returns to the back of the common queue.
struct PendingCalls {
    indices: VecDeque<usize>,
    queued: Vec<bool>,
    deadlines: BinaryHeap<Reverse<(Instant, usize)>>,
    len: usize,
}

impl PendingCalls {
    fn new(calls: &[BatchCall]) -> Self {
        Self {
            indices: (0..calls.len()).collect(),
            queued: vec![true; calls.len()],
            deadlines: calls
                .iter()
                .enumerate()
                .map(|(index, call)| Reverse((call.request.expire_at(), index)))
                .collect(),
            len: calls.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn len(&self) -> usize {
        self.len
    }

    fn iter(&self) -> impl Iterator<Item = &usize> {
        self.indices.iter().filter(|&&index| self.queued[index])
    }

    fn remove(&mut self, index: usize) -> bool {
        if !self.queued[index] {
            return false;
        }
        self.queued[index] = false;
        self.len -= 1;
        true
    }

    fn push_back(&mut self, index: usize) {
        debug_assert!(!self.queued[index]);
        self.queued[index] = true;
        self.len += 1;
        self.indices.push_back(index);
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.deadlines.peek().map(|entry| entry.0 .0)
    }

    fn expire(&mut self, now: Instant, calls: &mut [BatchCall]) {
        while self.next_deadline().is_some_and(|deadline| deadline <= now) {
            let Reverse((_, index)) = self.deadlines.pop().unwrap();
            if self.remove(index) {
                calls[index].finish(Err(Web3ProxyError::Timeout(None)));
            }
        }
    }

    fn take(&mut self, count: usize, mut eligible: impl FnMut(usize) -> bool) -> Vec<usize> {
        let mut selected = Vec::with_capacity(count.min(self.len));
        let mut skipped = Vec::new();
        while selected.len() < count {
            let Some(index) = self.indices.pop_front() else {
                break;
            };
            if !self.queued[index] {
                continue;
            }
            if eligible(index) {
                self.remove(index);
                selected.push(index);
            } else {
                skipped.push(index);
            }
        }
        // Preserve the order of calls that cannot use this backend.
        for index in skipped.into_iter().rev() {
            self.indices.push_front(index);
        }
        selected
    }
}

async fn send_packet(
    handle: OpenRequestHandle,
    indices: Vec<usize>,
    requests: Vec<Arc<ValidatedRequest>>,
) -> (
    Vec<usize>,
    Arc<Web3Rpc>,
    Web3ProxyResult<Vec<Web3ProxyResult<ParsedResponse>>>,
) {
    let rpc = handle.clone_connection();
    let result = handle.request_batch(&requests).await;
    (indices, rpc, result)
}

impl App {
    pub(crate) async fn proxy_eth_call_batch(
        self: &Arc<Self>,
        requests: Vec<Arc<ValidatedRequest>>,
    ) -> Web3ProxyResult<(Vec<ParsedResponse>, Vec<Arc<Web3Rpc>>)> {
        let mut calls = requests
            .into_iter()
            .map(|request| BatchCall {
                request,
                response: None,
                last_error: None,
                exhausted: HashSet::new(),
            })
            .collect::<Vec<_>>();
        let batch_deadline = calls.iter().map(|call| call.request.expire_at()).max();
        let mut pending = PendingCalls::new(&calls);
        let mut exhausted_calls: HashSet<usize> = HashSet::new();
        let mut running = FuturesUnordered::new();
        let mut ready = Vec::new();
        let mut updates = self.balanced_rpcs.watch_ranked_rpcs.subscribe();
        let mut watching = true;
        let mut found_backend = false;

        while !pending.is_empty() || !running.is_empty() {
            pending.expire(Instant::now(), &mut calls);
            if pending.is_empty() && running.is_empty() {
                break;
            }
            let mut wait_for_capacity = Vec::new();
            let mut wait_for_sync = Vec::new();
            let mut wake_at = pending.next_deadline();

            let first = pending.iter().next().copied();
            if let Some(first) = first {
                let request = &calls[first].request;
                if !found_backend && request.connect_timeout() {
                    for index in pending.take(pending.len(), |_| true) {
                        let error = calls[index]
                            .last_error
                            .take()
                            .map(Web3ProxyError::Arc)
                            .unwrap_or(Web3ProxyError::NoServersSynced);
                        calls[index].finish(Err(error));
                    }
                    ready.clear();
                    continue;
                }
                if !found_backend {
                    wake_at = Some(wake_at.unwrap().min(request.connect_timeout_at()));
                }
                // Read current rankings on every completion or readiness event.
                // Pending packets have no fixed backend assignment.
                updates.borrow_and_update();
                let candidates = match self.balanced_rpcs.try_rpcs_for_request(request).await {
                    Ok(rpcs) => Some(rpcs),
                    Err(error) => {
                        let error = Arc::new(error);
                        for &index in pending.iter() {
                            calls[index].last_error = Some(error.clone());
                        }
                        None
                    }
                };
                let connections = candidates
                    .as_ref()
                    .map(|rpcs| rpcs.connections())
                    .unwrap_or_default();
                if !connections.is_empty() {
                    exhausted_calls.retain(|&index| {
                        if !pending.queued[index] {
                            return calls[index].response.is_none();
                        }
                        if connections
                            .iter()
                            .all(|rpc| calls[index].exhausted.contains(&rpc.name))
                        {
                            pending.remove(index);
                            let error = calls[index]
                                .last_error
                                .take()
                                .expect("exhausted backends have an error");
                            calls[index].finish(Err(Web3ProxyError::Arc(error)));
                            false
                        } else {
                            true
                        }
                    });
                }
                for rpc in connections {
                    if ready
                        .iter()
                        .any(|handle: &OpenRequestHandle| handle.connection_name() == rpc.name)
                    {
                        continue;
                    }
                    let Some(&index) = pending
                        .iter()
                        .find(|&&i| !calls[i].exhausted.contains(&rpc.name))
                    else {
                        continue;
                    };
                    if !candidates
                        .as_ref()
                        .expect("connections have candidates")
                        .can_use(
                            &rpc,
                            &calls[index].exhausted,
                            ready.first().map(|handle| handle.clone_connection().backup),
                            None,
                        )
                    {
                        continue;
                    }
                    match rpc
                        .try_request_handle(&calls[index].request, None, false)
                        .await
                    {
                        Ok(OpenRequestResult::Handle(handle)) => {
                            found_backend = true;
                            ready.push(handle);
                        }
                        Ok(OpenRequestResult::Busy(wait)) => {
                            found_backend = true;
                            wait_for_capacity.push(wait);
                        }
                        Ok(OpenRequestResult::RetryAt(at)) => {
                            wake_at = Some(wake_at.unwrap().min(at));
                        }
                        Ok(OpenRequestResult::Lagged(wait)) => wait_for_sync.push(wait),
                        Ok(OpenRequestResult::Failed) | Err(_) => {}
                    }
                }
            }

            let had_reservations = !ready.is_empty();
            ready.retain(|handle| {
                let rpc = handle.clone_connection();
                pending.iter().any(|&i| {
                    self.balanced_rpcs
                        .rpcs_for_request(&calls[i].request)
                        .is_ok_and(|candidates| {
                            candidates.can_use(&rpc, &calls[i].exhausted, None, Some(&rpc))
                                && rpc.can_submit(&calls[i].request, false)
                        })
                })
            });
            if had_reservations && ready.is_empty() {
                continue;
            }
            // Use ready HTTP capacity first. If HTTP cannot take the remaining
            // work, other transports take calls from this same queue, one per slot.
            if ready.iter().any(OpenRequestHandle::supports_batch) {
                ready.retain(OpenRequestHandle::supports_batch);
            }
            let dispatched = !ready.is_empty();
            if dispatched {
                let lengths = weighted_batch_lengths(
                    pending.len(),
                    ready.iter().map(OpenRequestHandle::batch_capacity),
                );
                for (mut handle, mut remaining) in ready.drain(..).zip(lengths) {
                    let rpc = handle.clone_connection();
                    while remaining > 0 {
                        let count = remaining.min(handle.batch_size());
                        let indices = pending.take(count, |index| {
                            !calls[index].exhausted.contains(&rpc.name)
                                && rpc.can_submit(&calls[index].request, false)
                        });
                        if indices.is_empty() {
                            break;
                        }
                        remaining -= indices.len();
                        let requests = indices.iter().map(|&i| calls[i].request.clone()).collect();
                        running.push(send_packet(handle, indices, requests));
                        if remaining == 0 {
                            break;
                        }
                        let Some(&next) = pending
                            .iter()
                            .find(|&&i| !calls[i].exhausted.contains(&rpc.name))
                        else {
                            break;
                        };
                        // Fill this ready backend's weighted share while slots remain.
                        // Unreserved work stays in the common queue for other nodes.
                        match rpc
                            .try_request_handle(&calls[next].request, None, false)
                            .await
                        {
                            Ok(OpenRequestResult::Handle(next_handle)) => handle = next_handle,
                            _ => break,
                        }
                    }
                }
            }
            if pending.is_empty() && running.is_empty() {
                break;
            }
            let deadline = wake_at.or(batch_deadline).expect("unfinished calls exist");
            tokio::select! {
                // Start queued packets before another admission pass. Continue
                // admission without blocking when no response is ready yet.
                biased;
                packet = running.next(), if !running.is_empty() => {
                    let (indices, rpc, result) = packet.expect("running packets exist");
                    let outcomes = match result {
                        Ok(outcomes) => outcomes,
                        Err(error) => {
                            let error = Arc::new(error);
                            indices.iter().map(|_| Err(Web3ProxyError::Arc(error.clone()))).collect()
                        }
                    };
                    for (index, outcome) in indices.into_iter().zip(outcomes) {
                        match outcome {
                            Ok(response) => {
                                exhausted_calls.remove(&index);
                                calls[index].finish(Ok(response));
                            },
                            Err(error) => {
                                if calls[index].request.expired() {
                                    exhausted_calls.remove(&index);
                                    calls[index].finish(Err(Web3ProxyError::Timeout(None)));
                                } else {
                                    let cause = match &error { Web3ProxyError::Arc(error) => error.as_ref(), error => error };
                                    if !matches!(cause, Web3ProxyError::NoHandleReady | Web3ProxyError::Timeout(_))
                                        && rpc.next_available(Instant::now()) <= Instant::now() {
                                        calls[index].exhausted.insert(rpc.name.clone());
                                        exhausted_calls.insert(index);
                                    }
                                    calls[index].last_error = Some(Arc::new(error));
                                    pending.push_back(index);
                                }
                            }
                        }
                    }
                }
                _ = std::future::ready(()), if dispatched => {}
                handle = async { select_all(wait_for_capacity).await.0 }, if !wait_for_capacity.is_empty() => {
                    if let Ok(handle) = handle { ready.push(handle); }
                }
                _ = async { select_all(wait_for_sync).await }, if !wait_for_sync.is_empty() => {}
                changed = updates.changed(), if watching => { watching = changed.is_ok(); }
                _ = sleep_until(deadline) => {}
            }
        }
        let mut names = HashSet::new();
        let mut used_rpcs = Vec::new();
        let responses = calls
            .into_iter()
            .map(|call| {
                used_rpcs.extend(
                    call.request
                        .backend_rpcs_used()
                        .into_iter()
                        .filter(|rpc| names.insert(rpc.name.clone())),
                );
                call.response.expect("all calls completed")
            })
            .collect();
        Ok((responses, used_rpcs))
    }
}
