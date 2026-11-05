use super::*;

fn trace_request(id: u64) -> SingleRequest {
    SingleRequest::new(
        id.into(),
        "trace_transaction".into(),
        sonic_rs::json!([format!("0x{id:064x}")]),
    )
    .unwrap()
}

fn start_call(h: &Harness, mode: ProxyMode, call: SingleRequest) -> JoinHandle<Value> {
    let app = h.app.clone();
    super::super::test_support::spawn(async move {
        let (_, response, _) = app
            .proxy_web3_rpc(mode, JsonRpcRequestEnum::Single(call), None)
            .await
            .unwrap();
        serde_json::from_str(&response.to_json_string().await.unwrap()).unwrap()
    })
}

fn missing(call: Incoming, message: &str) {
    let id = call.body["id"].clone();
    call.respond(json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":message}}));
}

fn traced(call: Incoming) {
    let id = call.body["id"].clone();
    call.respond(json!({"jsonrpc":"2.0","id":id,"result":[]}));
}

#[tokio::test]
async fn unrecognized_method_error_retries_another_backend() {
    for mode in [ProxyMode::Best, ProxyMode::Fastest(1)] {
        super::super::test_support::with_cleanup(async {
            let mut geth = Harness::configured("geth", 1, 64, |rpc| {
                rpc.trace_method_support
                    .write()
                    .insert("trace_transaction", true);
            })
            .await;
            let mut reth = Harness::named("reth", 1, 64).await;
            reth.rpc.tier.store(10, Ordering::SeqCst);
            add_backend(&geth.app, vec![geth.rpc.clone(), reth.rpc.clone()]);
            let task = start_call(&geth, mode, trace_request(1));
            // Support changed after startup; wording must not prevent failover.
            missing(geth.next().await, "method not found");
            traced(reth.next().await);
            assert_eq!(
                task.await.unwrap(),
                json!({"jsonrpc":"2.0","id":1,"result":[]})
            );
            geth.idle();
            reth.idle();
        })
        .await;
    }
}

#[tokio::test]
async fn startup_capabilities_route_each_method_without_trying_unsupported_nodes() {
    for backup in [false, true] {
        for mode in [ProxyMode::Best, ProxyMode::Fastest(1), ProxyMode::Versus] {
            super::super::test_support::with_cleanup(async {
                let mut geth = Harness::configured("geth", 2, 64, |rpc| {
                    rpc.trace_method_support
                        .write()
                        .insert("trace_transaction", false);
                    rpc.trace_method_support.write().insert("trace_get", true);
                })
                .await;
                let mut reth = Harness::configured("reth", 2, 64, |rpc| {
                    rpc.backup = backup;
                    rpc.trace_method_support
                        .write()
                        .insert("trace_transaction", true);
                    rpc.trace_method_support.write().insert("trace_get", false);
                })
                .await;
                reth.rpc.tier.store(10, Ordering::SeqCst);
                add_backend(&geth.app, vec![geth.rpc.clone(), reth.rpc.clone()]);
                for id in [1, 2] {
                    let task = start_call(&geth, mode, trace_request(id));
                    let call = reth.next().await;
                    assert_eq!(call.body["params"], json!([format!("0x{id:064x}")]));
                    traced(call);
                    assert_eq!(
                        task.await.unwrap(),
                        json!({"jsonrpc":"2.0","id":id,"result":[]})
                    );
                    geth.quiet().await;
                }
                let request = SingleRequest::new(
                    3.into(),
                    "trace_get".into(),
                    sonic_rs::json!([format!("0x{:064x}", 3), []]),
                )
                .unwrap();
                let task = start_call(&geth, mode, request);
                let call = geth.next().await;
                assert_eq!(call.body["method"], "trace_get");
                traced(call);
                assert_eq!(task.await.unwrap()["result"], json!([]));
                reth.quiet().await;

                // Missing a trace method must not remove Geth from normal RPC routing.
                let task = start_call(&geth, ProxyMode::Best, code_call(4));
                let call = geth.next().await;
                call.respond(json!({"jsonrpc":"2.0","id":4,"result":"0xbeef"}));
                assert_eq!(task.await.unwrap()["result"], "0xbeef");
                geth.idle();
                reth.idle();
            })
            .await;
        }
    }
}

#[tokio::test]
async fn mixed_batch_uses_per_method_capabilities_and_preserves_order() {
    super::super::test_support::with_cleanup(async {
        let mut geth = Harness::configured("geth", 2, 64, |rpc| {
            rpc.trace_method_support
                .write()
                .insert("trace_transaction", false);
        })
        .await;
        let mut reth = Harness::named("reth", 4, 64).await;
        reth.rpc.tier.store(10, Ordering::SeqCst);
        add_backend(&geth.app, vec![geth.rpc.clone(), reth.rpc.clone()]);
        let task = geth.start_requests(vec![trace_request(1), code_call(2), trace_request(3)]);
        let ordinary = geth.next().await;
        assert_eq!(ordinary.body["method"], "eth_getCode");
        ordinary.respond(json!({"jsonrpc":"2.0","id":2,"result":"0xbeef"}));
        for _ in 0..2 {
            let call = reth.next().await;
            assert_eq!(call.body["method"], "trace_transaction");
            traced(call);
        }
        assert_eq!(
            task.await.unwrap(),
            json!([
                {"jsonrpc":"2.0","id":1,"result":[]},
                {"jsonrpc":"2.0","id":2,"result":"0xbeef"},
                {"jsonrpc":"2.0","id":3,"result":[]}
            ])
        );
        geth.quiet().await;
        geth.idle();
        reth.idle();
    })
    .await;
}

#[tokio::test]
async fn all_backends_without_a_method_return_method_not_found_promptly() {
    for mode in [ProxyMode::Best, ProxyMode::Fastest(1), ProxyMode::Versus] {
        super::super::test_support::with_cleanup(async {
            let mut geth = Harness::configured("geth", 1, 64, |rpc| {
                rpc.trace_method_support
                    .write()
                    .insert("trace_transaction", false);
            })
            .await;
            let mut reth = Harness::configured("reth", 1, 64, |rpc| {
                rpc.trace_method_support
                    .write()
                    .insert("trace_transaction", false);
            })
            .await;
            add_backend(&geth.app, vec![geth.rpc.clone(), reth.rpc.clone()]);
            let response = timeout(
                Duration::from_millis(500),
                start_call(&geth, mode, trace_request(1)),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(response["id"], 1);
            assert_eq!(response["error"]["code"], -32601);
            geth.quiet().await;
            reth.quiet().await;
        })
        .await;
    }
}

#[tokio::test]
async fn capability_changes_are_rechecked_before_sending_a_reserved_call() {
    super::super::test_support::with_cleanup(async {
        let mut node = Harness::new(2, 64).await;
        let request = ValidatedRequest::new_internal(
            "trace_transaction".into(),
            &sonic_rs::json!([format!("0x{:064x}", 1)]),
            None,
            None,
        )
        .await
        .unwrap();
        let handle = node
            .rpc
            .wait_for_request_handle(&request, None, false)
            .await
            .unwrap();
        node.rpc
            .trace_method_support
            .write()
            .insert("trace_transaction", false);
        assert!(matches!(
            handle.request::<sonic_rs::OwnedLazyValue>().await,
            Err(Web3ProxyError::NoHandleReady)
        ));
        node.quiet().await;
        node.idle();
    })
    .await;
}

#[tokio::test]
async fn ordinary_trace_errors_and_reverts_keep_the_backend_reply() {
    for (code, message) in [
        (-32602, "invalid parameters"),
        (-32000, "transaction not found"),
        (3, "execution reverted"),
    ] {
        super::super::test_support::with_cleanup(async {
            let mut geth = Harness::named("geth", 1, 64).await;
            let mut reth = Harness::named("reth", 1, 64).await;
            reth.rpc.tier.store(10, Ordering::SeqCst);
            add_backend(&geth.app, vec![geth.rpc.clone(), reth.rpc.clone()]);
            let task = start_call(&geth, ProxyMode::Best, trace_request(1));
            let call = geth.next().await;
            let reply = json!({"jsonrpc":"2.0","id":1,
                "error":{"code":code,"message":message,"data":"0xdead"}});
            call.respond(reply.clone());
            assert_eq!(task.await.unwrap(), reply);
            reth.quiet().await;
        })
        .await;
    }
}
