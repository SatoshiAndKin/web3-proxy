use super::*;
use axum::{
    extract::{
        ws::{Message, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};

async fn heads(State(hash): State<B256>, upgrade: WebSocketUpgrade) -> impl IntoResponse {
    upgrade.on_upgrade(move |mut socket| async move {
        while let Some(Ok(Message::Text(request))) = socket.recv().await {
            let request: sonic_rs::Value = sonic_rs::from_str(request.as_str()).unwrap();
            assert_eq!(request["method"], json!("eth_subscribe"));
            assert_eq!(request["params"], json!(["newHeads"]));
            let reply = json!({"jsonrpc": "2.0", "id": request["id"], "result": "0x1"});
            if socket.send(Message::Text(reply.to_string().into())).await.is_err() { return; }
            let mut header: alloy::rpc::types::Header = alloy::rpc::types::Header { hash, ..Default::default() };
            header.inner.number = 1;
            header.inner.timestamp = 100;
            let event = json!({"jsonrpc": "2.0", "method": "eth_subscription", "params": {"subscription": "0x1", "result": header}});
            if socket.send(Message::Text(event.to_string().into())).await.is_err() { return; }
        }
    })
}

#[tokio::test]
async fn independent_heads_run_in_both_modes_and_identify_preexisting_canonical_work() {
    for mode in [config::Mode::Observe, config::Mode::Inject] {
        let rpc = MockRpc::new();
        let server = Server::rpc(rpc.clone()).await;
        let target = rpc.target(&server.url, "a");
        let worker = Worker::start(target.clone(), mode).await;
        let (sender, mut records) = tokio::sync::mpsc::channel(32);
        worker.stats.lock().recorder = Some(sender);
        let ws = Server::start(
            Router::new()
                .route("/", get(heads))
                .with_state(B256::with_last_byte(1)),
        )
        .await;
        let observe = tokio::spawn(target.clone().observe_heads(
            ws.url.replacen("http", "ws", 1),
            worker.stats.clone(),
            worker.stop.subscribe(),
        ));
        until(|| worker.stats.lock().telemetry.heads.contains_key("a")).await;
        assert!(
            !target.confirmed.contains_key(&B256::with_last_byte(1)),
            "telemetry must not change delivery state"
        );
        worker.tx.send(work(1, 0, 1, mode)).unwrap();
        until(|| {
            let s = worker.stats.lock();
            let Some(target) = s
                .telemetry
                .modes
                .get(&mode)
                .and_then(|m| m.targets.get("execution:a"))
            else {
                return false;
            };
            if mode == config::Mode::Inject {
                target.outcomes.get("valid") == Some(&1)
            } else {
                target.dispositions.get("observe") == Some(&1)
            }
        })
        .await;
        worker.finish().await;
        observe.await.unwrap();
        let mut start = None;
        let mut finish = None;
        while let Ok(record) = records.try_recv() {
            assert_eq!(record.context.mode, mode);
            match record.event {
                recording::Record::SubmissionStarted {
                    attempt_id,
                    canonical_before_call,
                    ..
                } => {
                    let head = canonical_before_call.expect("independent head preceded the call");
                    assert!(head.observed_us <= record.context.monotonic_us);
                    start = Some(attempt_id);
                }
                recording::Record::Submission {
                    attempt_id,
                    outcome,
                    ..
                } => {
                    assert_eq!(outcome, "valid");
                    finish = Some(attempt_id);
                }
                _ => {}
            }
        }
        if mode == config::Mode::Inject {
            assert!(start.is_some());
            assert_eq!(start, finish);
            assert_eq!(rpc.payload_hashes(), vec![B256::with_last_byte(1)]);
        } else {
            assert_eq!(start, None);
            assert!(rpc.payload_hashes().is_empty());
        }
    }
}

#[tokio::test]
async fn failed_head_stream_and_full_recording_queue_do_not_delay_engine_work() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    let target = rpc.target(&server.url, "a");
    let worker = Worker::start(target.clone(), config::Mode::Inject).await;
    let (sender, _records) = tokio::sync::mpsc::channel(1);
    worker.stats.lock().recorder = Some(sender);
    let observe = tokio::spawn(target.observe_heads(
        "invalid://secret".into(),
        worker.stats.clone(),
        worker.stop.subscribe(),
    ));
    until(|| {
        worker
            .stats
            .lock()
            .telemetry
            .head_streams
            .get("a")
            .is_some_and(|s| s.errors > 0)
    })
    .await;
    worker.tx.send(work(1, 0, 1, config::Mode::Inject)).unwrap();
    until(|| worker.stats.lock().execution_targets["a"].valid == 1).await;
    assert!(worker.stats.lock().recording_dropped > 0);
    assert!(!sonic_rs::to_string(&*worker.stats.lock())
        .unwrap()
        .contains("secret"));
    worker.finish().await;
    observe.await.unwrap();
}

#[tokio::test]
async fn submission_finishing_after_mode_change_keeps_its_origin_and_excludes_mixed_latency() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    rpc.state
        .lock()
        .gates
        .insert(B256::with_last_byte(1), gate.clone());
    let worker = Worker::start(rpc.target(&server.url, "a"), config::Mode::Inject).await;
    let (sender, mut records) = tokio::sync::mpsc::channel(8);
    worker.stats.lock().recorder = Some(sender);
    worker.tx.send(work(1, 0, 1, config::Mode::Inject)).unwrap();
    until(|| rpc.payload_hashes().len() == 1).await;
    {
        let mut s = worker.stats.lock();
        s.mode = config::Mode::Observe;
        s.telemetry.mode_epoch = 1;
        worker.mode.send_replace(config::Mode::Observe);
    }
    gate.notify_one();
    until(|| worker.stats.lock().execution_targets["a"].valid == 1).await;
    assert_eq!(worker.stats.lock().telemetry.mixed_mode_samples, 1);
    let start = records.try_recv().unwrap();
    let end = records.try_recv().unwrap();
    assert_eq!(start.context.mode, config::Mode::Inject);
    assert_eq!(end.context.mode, config::Mode::Observe);
    let recording::Record::SubmissionStarted {
        attempt_id: started,
        ..
    } = start.event
    else {
        panic!("start record");
    };
    let recording::Record::Submission {
        attempt_id,
        started_mode,
        started_mode_epoch,
        ..
    } = end.event
    else {
        panic!("end record");
    };
    assert_eq!(started, attempt_id);
    assert_eq!(started_mode, config::Mode::Inject);
    assert_eq!(started_mode_epoch, 0);
    worker.finish().await;
}
