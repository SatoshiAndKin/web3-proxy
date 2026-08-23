use super::{SentrydErrorBuilder, SentrydResult};
use jiff::Timestamp;
use tracing::{debug, warn};
use web3_proxy::jsonrpc::JsonRpcErrorData;
use web3_proxy::prelude::alloy::primitives::B256;
use web3_proxy::prelude::alloy::rpc::types::Block;
use web3_proxy::prelude::anyhow;
use web3_proxy::prelude::anyhow::{anyhow, Context};
use web3_proxy::prelude::futures::{stream::FuturesUnordered, StreamExt};
use web3_proxy::prelude::reqwest;
use web3_proxy::prelude::reqwest::header;
use web3_proxy::prelude::serde::{Deserialize, Serialize};
use web3_proxy::prelude::sonic_rs::{self, json};
use web3_proxy::prelude::tokio;

#[derive(Debug, Deserialize, Serialize)]
struct JsonRpcResponse<V> {
    // pub jsonrpc: String,
    // pub id: OwnedLazyValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<V>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcErrorData>,
}

#[derive(Serialize, Ord, PartialEq, PartialOrd, Eq)]
struct AbbreviatedBlock {
    pub num: u64,
    pub time: Timestamp,
    pub hash: B256,
}

impl From<Block> for AbbreviatedBlock {
    fn from(x: Block) -> Self {
        let timestamp = i64::try_from(x.header.timestamp)
            .expect("block timestamp must fit in a signed 64-bit integer");

        Self {
            num: x.number(),
            hash: x.header.hash,
            time: Timestamp::from_second(timestamp)
                .expect("block timestamp must fit in Jiff's supported range"),
        }
    }
}

pub async fn main(
    error_builder: SentrydErrorBuilder,
    rpc: String,
    others: Vec<String>,
    max_age: i64,
    max_lag: i64,
) -> SentrydResult {
    let client = reqwest::Client::new();

    let block_by_number_request = json!({
        "jsonrpc": "2.0",
        "id": "1",
        "method": "eth_getBlockByNumber",
        "params": ["latest", false],
    });
    let block_by_number_request = sonic_rs::to_vec(&block_by_number_request)
        .context("failed serializing block request")
        .map_err(|x| error_builder.build(x))?;

    let a = client
        .post(&rpc)
        .header(header::CONTENT_TYPE, "application/json")
        .body(block_by_number_request)
        .send()
        .await
        .context(format!("error querying block from {}", rpc))
        .map_err(|x| error_builder.build(x))?;

    if !a.status().is_success() {
        return error_builder.result(anyhow!("bad response from {}: {}", rpc, a.status()));
    }

    // Capture response headers now so errors include them.
    let headers = format!("{:#?}", a.headers());

    let body = a
        .text()
        .await
        .context(format!("failed parsing body from {}", rpc))
        .map_err(|x| error_builder.build(x))?;

    let a: JsonRpcResponse<Block> = sonic_rs::from_str(&body)
        .context(format!("body: {}", body))
        .context(format!("failed parsing json from {}", rpc))
        .map_err(|x| error_builder.build(x))?;

    let a = if let Some(block) = a.result {
        block
    } else if let Some(err) = a.error {
        return error_builder.result(
            anyhow::anyhow!("headers: {:#?}. err: {:#?}", headers, err)
                .context(format!("jsonrpc error from {}: code {}", rpc, err.code)),
        );
    } else {
        return error_builder
            .result(anyhow!("{:#?}", a).context(format!("empty response from {}", rpc)));
    };

    // check the parent because b and c might not be as fast as a
    let parent_hash = a.header.parent_hash;

    let rpc_block = check_rpc(parent_hash, client.clone(), rpc.to_string())
        .await
        .context(format!("Error while querying primary rpc: {}", rpc))
        .map_err(|err| error_builder.build(err))?;

    let fs = FuturesUnordered::new();
    for other in others.iter() {
        let f = check_rpc(parent_hash, client.clone(), other.to_string());

        fs.push(tokio::spawn(f));
    }
    let other_check: Vec<_> = fs.collect().await;

    if other_check.is_empty() {
        return error_builder.result(anyhow::anyhow!("No other RPCs to check!"));
    }

    // TODO: collect into a counter instead?
    let mut newest_other = None;
    for oc in other_check.iter() {
        match oc {
            Ok(Ok(x)) => newest_other = newest_other.max(Some(x)),
            Ok(Err(err)) => warn!("failed checking other rpc: {:?}", err),
            Err(err) => warn!("internal error checking other rpc: {:?}", err),
        }
    }

    if let Some(newest_other) = newest_other {
        let duration_since = newest_other.time.duration_since(rpc_block.time).as_secs();

        match duration_since.abs().cmp(&max_lag) {
            std::cmp::Ordering::Less | std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Greater => match duration_since.cmp(&0) {
                std::cmp::Ordering::Equal => {
                    unimplemented!("we already checked that they are not equal")
                }
                std::cmp::Ordering::Less => {
                    return error_builder.result(anyhow::anyhow!(
                        "Our RPC is too far ahead ({} s)! Something might be wrong.\n{:#}\nvs\n{:#}",
                        duration_since.abs(),
                        json!(rpc_block),
                        json!(newest_other),
                    ).context(format!("{} is too far ahead", rpc)));
                }
                std::cmp::Ordering::Greater => {
                    return error_builder.result(
                        anyhow::anyhow!(
                            "Behind {} s!\n{:#}\nvs\n{:#}",
                            duration_since,
                            json!(rpc_block),
                            json!(newest_other),
                        )
                        .context(format!("{} is too far behind", rpc)),
                    );
                }
            },
        }

        let now = Timestamp::now();

        let block_age = now
            .duration_since(newest_other.max(&rpc_block).time)
            .as_secs();

        match block_age.abs().cmp(&max_age) {
            std::cmp::Ordering::Less | std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Greater => match duration_since.cmp(&0) {
                std::cmp::Ordering::Equal => unimplemented!(),
                std::cmp::Ordering::Less => {
                    return error_builder.result(
                        anyhow::anyhow!(
                            "Clock is behind {}s! Something might be wrong.\n{:#}\nvs\n{:#}",
                            block_age.abs(),
                            json!(now),
                            json!(newest_other),
                        )
                        .context(format!("Clock is too far behind on {}!", rpc)),
                    );
                }
                std::cmp::Ordering::Greater => {
                    return error_builder.result(
                        anyhow::anyhow!(
                            "block is too old ({}s)!\n{:#}\nvs\n{:#}",
                            block_age,
                            json!(now),
                            json!(newest_other),
                        )
                        .context(format!("block is too old on {}!", rpc)),
                    );
                }
            },
        }
    } else {
        return error_builder.result(anyhow::anyhow!("No other RPC times to check!"));
    }

    debug!("rpc comparison ok: {:#}", json!(rpc_block));

    Ok(())
}

// i don't think we need a whole provider. a simple http request is easiest
async fn check_rpc(
    block_hash: B256,
    client: reqwest::Client,
    rpc: String,
) -> anyhow::Result<AbbreviatedBlock> {
    let block_by_hash_request = json!({
        "jsonrpc": "2.0",
        "id": "1",
        "method": "eth_getBlockByHash",
        "params": [block_hash, false],
    });

    let response = client
        .post(&rpc)
        .header(header::CONTENT_TYPE, "application/json")
        .body(sonic_rs::to_vec(&block_by_hash_request)?)
        .send()
        .await
        .context(format!("awaiting response from {}", rpc))?;

    anyhow::ensure!(
        response.status().is_success(),
        "bad response from {}: {}",
        rpc,
        response.status(),
    );

    let body = response
        .text()
        .await
        .context(format!("failed parsing body from {}", rpc))?;

    let response_json: JsonRpcResponse<Block> = sonic_rs::from_str(&body)
        .context(format!("body: {}", body))
        .context(format!("failed parsing json from {}", rpc))?;

    if let Some(result) = response_json.result {
        let abbreviated = AbbreviatedBlock::from(result);

        debug!("{} has {:?}@{}", rpc, abbreviated.hash, abbreviated.num);

        Ok(abbreviated)
    } else if let Some(result) = response_json.error {
        Err(anyhow!(
            "jsonrpc error during check_rpc from {}: {:#}",
            rpc,
            json!(result),
        ))
    } else {
        Err(anyhow!(
            "empty result during check_rpc from {}: {:#}",
            rpc,
            json!(response_json)
        ))
    }
}
