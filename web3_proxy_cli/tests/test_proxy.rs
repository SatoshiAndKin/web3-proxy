use serde_json::{json, Value};
use std::fmt::Debug;
use tracing::info;
use web3_proxy::prelude::alloy::consensus::{SignableTransaction, TxEip1559};
use web3_proxy::prelude::alloy::eips::Encodable2718;
use web3_proxy::prelude::alloy::network::TxSignerSync;
use web3_proxy::prelude::alloy::primitives::{Address, Bytes, TxKind, B256, U256, U64};
use web3_proxy::prelude::alloy::providers::{Provider, ProviderBuilder, RootProvider, WsConnect};
use web3_proxy::prelude::alloy::rpc::types::{Block, Log, Transaction};
use web3_proxy::prelude::futures::StreamExt;
use web3_proxy::prelude::reqwest::{self, header, StatusCode};
use web3_proxy::prelude::serde::{de::DeserializeOwned, Serialize};
use web3_proxy::prelude::tokio;
use web3_proxy::prelude::tokio::time::{timeout, Duration};
use web3_proxy::rpcs::blockchain::ArcBlock;
use web3_proxy_cli::test_utils::{TestAnvil, TestApp};

async fn request_both<R>(
    anvil_provider: &RootProvider,
    proxy_provider: &RootProvider,
    method: &'static str,
    params: Value,
) -> R
where
    R: Serialize + DeserializeOwned + Debug + PartialEq + Send + Sync + Unpin + 'static,
{
    let anvil_result = anvil_provider
        .raw_request(method.into(), params.clone())
        .await
        .unwrap();
    let proxy_result = proxy_provider
        .raw_request(method.into(), params)
        .await
        .unwrap();

    assert_eq!(anvil_result, proxy_result);
    anvil_result
}

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn it_starts_and_stops() {
    let a = TestAnvil::spawn_chain(31337).await;

    let _: Value = a.provider.raw_request("evm_mine".into(), ()).await.unwrap();

    let x = TestApp::spawn(&a).await;

    let anvil_provider = &a.provider;
    let proxy_provider = &x.proxy_provider;

    // check the /health page
    let proxy_url = &x.proxy_url;
    let health_response = reqwest::get(format!("{}health", proxy_url)).await;
    dbg!(&health_response);
    assert_eq!(health_response.unwrap().status(), StatusCode::OK);

    // check the /status page
    let status_response = reqwest::get(format!("{}status", proxy_url)).await;
    dbg!(&status_response);
    assert_eq!(status_response.unwrap().status(), StatusCode::OK);

    let client = reqwest::Client::new();
    for path in ["", "fastest", "versus"] {
        let response = client
            .post(format!("{}{}", proxy_url, path))
            .header(header::CONTENT_TYPE, "application/json")
            .body(
                sonic_rs::to_vec(&json!({"jsonrpc": "2.0", "method": "eth_chainId", "id": 1}))
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "route /{path}");

        let body: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        assert_eq!(body["result"], json!("0x7a69"), "route /{path}");
    }

    let removed_key_route = client
        .post(format!("{}rpc/removed-key", proxy_url))
        .header(header::CONTENT_TYPE, "application/json")
        .body(
            sonic_rs::to_vec(&json!({"jsonrpc": "2.0", "method": "eth_chainId", "id": 1})).unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(removed_key_route.status(), StatusCode::NOT_FOUND);

    let anvil_result = anvil_provider
        .raw_request::<_, Option<ArcBlock>>("eth_getBlockByNumber".into(), ("latest", false))
        .await
        .unwrap()
        .unwrap();
    let proxy_result = proxy_provider
        .raw_request::<_, Option<ArcBlock>>("eth_getBlockByNumber".into(), ("latest", false))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(anvil_result, proxy_result);

    let status_response = reqwest::get(format!("{}status", proxy_url)).await.unwrap();
    let status: Value = serde_json::from_slice(&status_response.bytes().await.unwrap()).unwrap();
    let backend = &status["balanced_rpcs"]["conns"][0];
    assert!(backend["total_requests"].as_u64().unwrap() > 0);
    assert!(backend.get("internal_requests").is_none());
    assert!(backend.get("external_requests").is_none());

    let first_block_num = anvil_result.number();

    // mine a block
    let _: Value = anvil_provider
        .raw_request("evm_mine".into(), ())
        .await
        .unwrap();

    // make sure the block advanced
    let anvil_result = anvil_provider
        .raw_request::<_, Option<ArcBlock>>("eth_getBlockByNumber".into(), ("latest", false))
        .await
        .unwrap()
        .unwrap();

    let second_block_num = anvil_result.number();

    assert_eq!(first_block_num, second_block_num - 1);

    x.wait_for_block(second_block_num).await;

    let proxy_result = proxy_provider
        .raw_request::<_, Option<ArcBlock>>("eth_getBlockByNumber".into(), ("latest", false))
        .await
        .unwrap();

    assert_eq!(Some(anvil_result), proxy_result);

    // most tests won't need to wait, but we should wait here to be sure all the shutdown logic works properly
    x.wait_for_stop();
}

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn eth_call_batch_stays_batched_at_backend() {
    let anvil = TestAnvil::spawn_chain(31337).await;
    let _: Value = anvil
        .provider
        .raw_request("evm_mine".into(), ())
        .await
        .unwrap();
    let proxy = TestApp::spawn(&anvil).await;
    let client = reqwest::Client::new();

    let status: Value = client
        .get(format!("{}status", proxy.proxy_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let batch_requests_before = status["balanced_rpcs"]["conns"][0]["backend_batch_requests"]
        .as_u64()
        .unwrap_or_default();

    let requests = (0..129)
        .map(|id| {
            json!({
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": Address::ZERO}, "latest"],
                "id": id
            })
        })
        .collect::<Vec<_>>();
    let response = client
        .post(proxy.proxy_url.clone())
        .header(header::CONTENT_TYPE, "application/json")
        .json(&requests)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response: Value = response.json().await.unwrap();
    assert_eq!(response.as_array().unwrap().len(), requests.len());

    let status: Value = client
        .get(format!("{}status", proxy.proxy_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let batch_requests_after = status["balanced_rpcs"]["conns"][0]["backend_batch_requests"]
        .as_u64()
        .unwrap_or_default();

    assert_eq!(batch_requests_after, batch_requests_before + 3);
}

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn eth_call_batch_uses_each_synced_backend() {
    let anvil = TestAnvil::spawn_chain(31337).await;
    let _: Value = anvil
        .provider
        .raw_request("evm_mine".into(), ())
        .await
        .unwrap();
    let proxy = TestApp::spawn_with_balanced_rpc_count(&anvil, 2).await;
    let client = reqwest::Client::new();

    let status_before: Value = client
        .get(format!("{}status", proxy.proxy_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let before = status_before["balanced_rpcs"]["conns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|backend| {
            (
                backend["name"].as_str().unwrap().to_owned(),
                backend["backend_batch_requests"]
                    .as_u64()
                    .unwrap_or_default(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();

    let requests = (0..256)
        .map(|id| {
            json!({
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": Address::ZERO}, "latest"],
                "id": id
            })
        })
        .collect::<Vec<_>>();
    let response = client
        .post(proxy.proxy_url.clone())
        .header(header::CONTENT_TYPE, "application/json")
        .json(&requests)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response: Value = response.json().await.unwrap();
    assert_eq!(response.as_array().unwrap().len(), requests.len());

    let status_after: Value = client
        .get(format!("{}status", proxy.proxy_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let deltas = status_after["balanced_rpcs"]["conns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|backend| {
            let name = backend["name"].as_str().unwrap();
            backend["backend_batch_requests"]
                .as_u64()
                .unwrap_or_default()
                - before[name]
        })
        .collect::<Vec<_>>();

    assert_eq!(deltas.iter().sum::<u64>(), 4);
    assert!(deltas.iter().all(|delta| *delta > 0));
}

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn websocket_new_heads_returns_subscription_and_delivers_block() {
    let anvil = TestAnvil::spawn_chain(31337).await;
    let _: Value = anvil
        .provider
        .raw_request("evm_mine".into(), ())
        .await
        .unwrap();
    let proxy = TestApp::spawn(&anvil).await;

    let mut websocket_url = proxy.proxy_url.clone();
    websocket_url.set_scheme("ws").unwrap();
    let provider: RootProvider = ProviderBuilder::default()
        .connect_ws(WsConnect::new(websocket_url.as_str()))
        .await
        .unwrap();
    let subscription = timeout(Duration::from_secs(5), provider.subscribe_blocks())
        .await
        .expect("proxy did not acknowledge the newHeads subscription")
        .unwrap();
    let mut blocks = subscription.into_stream();

    let _: Value = anvil
        .provider
        .raw_request("evm_mine".into(), ())
        .await
        .unwrap();
    let expected_block: U64 = anvil
        .provider
        .raw_request("eth_blockNumber".into(), ())
        .await
        .unwrap();
    let header = timeout(Duration::from_secs(5), async {
        loop {
            let header = blocks.next().await.expect("subscription stream closed");
            if header.number == expected_block.to::<u64>() {
                return header;
            }
        }
    })
    .await
    .expect("proxy did not deliver the mined block");

    assert_eq!(header.number, expected_block.to::<u64>());
}

/// TODO: have another test that queries mainnet so the state is more interesting
/// TODO: have another test that makes sure error codes match
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn it_matches_anvil() {
    let chain_id = 31337;

    let a = TestAnvil::spawn_chain(chain_id).await;

    let _: Value = a.provider.raw_request("evm_mine".into(), ()).await.unwrap();

    let x = TestApp::spawn(&a).await;

    let anvil_provider = &a.provider;
    let proxy_provider = &x.proxy_provider;

    let chain_id: U64 =
        request_both(anvil_provider, proxy_provider, "eth_chainId", json!([])).await;
    info!(%chain_id);

    let block_number: U64 =
        request_both(anvil_provider, proxy_provider, "eth_blockNumber", json!([])).await;
    info!(%block_number);

    let block_without_tx: Option<Block> = request_both(
        anvil_provider,
        proxy_provider,
        "eth_getBlockByNumber",
        json!([block_number, false]),
    )
    .await;
    info!(?block_without_tx);

    let block_with_tx: Option<Block> = request_both(
        anvil_provider,
        proxy_provider,
        "eth_getBlockByNumber",
        json!([block_number, true]),
    )
    .await;
    info!(?block_with_tx);

    let fee_history: Value = request_both(
        anvil_provider,
        proxy_provider,
        "eth_feeHistory",
        json!([4, "latest", [25, 75]]),
    )
    .await;
    info!(?fee_history);

    let gas_price: U256 =
        request_both(anvil_provider, proxy_provider, "eth_gasPrice", json!([])).await;
    info!(%gas_price);

    let balance: U256 = request_both(
        anvil_provider,
        proxy_provider,
        "eth_getBalance",
        json!([block_with_tx.as_ref().unwrap().header.beneficiary, "latest"]),
    )
    .await;
    info!(%balance);

    let singleton_deploy_from: Address = "0xBb6e024b9cFFACB947A71991E386681B1Cd1477D"
        .parse()
        .unwrap();

    let wallet = a.wallet(0);

    let missing_tx: Option<Transaction> = request_both(
        anvil_provider,
        proxy_provider,
        "eth_getTransactionByHash",
        json!(["0x803351deb6d745e91545a6a3e1c0ea3e9a6a02a1a4193b70edfcd2f40f71a01c"]),
    )
    .await;
    assert!(missing_tx.is_none());

    let gas_price: U256 =
        request_both(anvil_provider, proxy_provider, "eth_gasPrice", json!([])).await;

    let mut fund_tx = TxEip1559 {
        chain_id: chain_id.to::<u64>(),
        to: TxKind::Call(singleton_deploy_from),
        gas_limit: 21_000,
        value: "1024700000000000000".parse().unwrap(),
        max_fee_per_gas: gas_price.to::<u128>() * 2,
        ..Default::default()
    };

    let fund_sig = wallet.sign_transaction_sync(&mut fund_tx).unwrap();

    let fund_tx = Bytes::from(fund_tx.into_signed(fund_sig).encoded_2718());

    // fund singleton deployer
    // Send through the proxy so it can fan the transaction out to its configured providers.
    let fund_tx_hash: B256 = proxy_provider
        .raw_request("eth_sendRawTransaction".into(), [fund_tx])
        .await
        .unwrap();
    info!(%fund_tx_hash);

    // deploy singleton deployer
    // Send through the proxy so it can fan the transaction out to its configured providers.
    let deploy_tx: B256 = proxy_provider.raw_request("eth_sendRawTransaction".into(), ["0xf9016c8085174876e8008303c4d88080b90154608060405234801561001057600080fd5b50610134806100206000396000f3fe6080604052348015600f57600080fd5b506004361060285760003560e01c80634af63f0214602d575b600080fd5b60cf60048036036040811015604157600080fd5b810190602081018135640100000000811115605b57600080fd5b820183602082011115606c57600080fd5b80359060200191846001830284011164010000000083111715608d57600080fd5b91908080601f016020809104026020016040519081016040528093929190818152602001838380828437600092019190915250929550509135925060eb915050565b604080516001600160a01b039092168252519081900360200190f35b6000818351602085016000f5939250505056fea26469706673582212206b44f8a82cb6b156bfcc3dc6aadd6df4eefd204bc928a4397fd15dacf6d5320564736f6c634300060200331b83247000822470"]).await.unwrap();
    assert_eq!(
        deploy_tx,
        "0x803351deb6d745e91545a6a3e1c0ea3e9a6a02a1a4193b70edfcd2f40f71a01c"
            .parse::<B256>()
            .unwrap()
    );

    let deployed_block_number: U64 = anvil_provider
        .raw_request("eth_blockNumber".into(), ())
        .await
        .unwrap();
    x.wait_for_block(deployed_block_number.to::<u64>()).await;

    let code: Bytes = request_both(
        anvil_provider,
        proxy_provider,
        "eth_getCode",
        json!(["0xce0042B868300000d44A59004Da54A005ffdcf9f", "latest"]),
    )
    .await;
    info!(%code);

    let deploy_tx: Transaction = request_both::<Option<Transaction>>(
        anvil_provider,
        proxy_provider,
        "eth_getTransactionByHash",
        json!(["0x803351deb6d745e91545a6a3e1c0ea3e9a6a02a1a4193b70edfcd2f40f71a01c"]),
    )
    .await
    .unwrap();
    info!(?deploy_tx);

    let head_block_num: U64 =
        request_both(anvil_provider, proxy_provider, "eth_blockNumber", json!([])).await;

    let future_block: Option<ArcBlock> = request_both(
        anvil_provider,
        proxy_provider,
        "eth_getBlockByNumber",
        json!([head_block_num + U64::from(1u8), false]),
    )
    .await;
    assert!(future_block.is_none());

    let logs: Vec<Log> =
        request_both(anvil_provider, proxy_provider, "eth_getLogs", json!([{}])).await;
    info!(?logs);

    let logs: Vec<Log> = request_both(
        anvil_provider,
        proxy_provider,
        "eth_getLogs",
        json!([{"fromBlock": U64::ZERO}]),
    )
    .await;
    info!(?logs);

    let logs: Vec<Log> = request_both(
        anvil_provider,
        proxy_provider,
        "eth_getLogs",
        json!([{"fromBlock": U64::ZERO, "toBlock": block_number}]),
    )
    .await;
    info!(?logs);

    let logs: Vec<Log> = request_both(
        anvil_provider,
        proxy_provider,
        "eth_getLogs",
        json!([{"fromBlock": "earliest", "toBlock": "latest"}]),
    )
    .await;
    info!(?logs);

    // // TODO: i prefer our way of erring on this, but we should probably match what everyone else does
    // let logs: Vec<Log> = quorum_provider
    //     .request(
    //         "eth_getLogs",
    //         json!([{"fromBlock": U64::zero(), "toBlock": U64::MAX}]),
    //     )
    //     .await
    //     .unwrap();
    // info!(?logs);

    // todo!("lots more requests");

    // todo!("compare batch requests");
}
