use super::strict::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::Path;
use web3_proxy::prelude::anyhow::{self, ensure, Context};

#[derive(Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Comparison {
    #[default]
    Exact,
    Observation,
}

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Expect {
    #[default]
    NonNull,
    Result,
    Revert,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Case {
    pub name: String,
    pub method: String,
    pub params: Value,
    #[serde(default)]
    pub comparison: Comparison,
    #[serde(default)]
    pub expect: Expect,
    #[serde(skip)]
    pub unavailable: Option<String>,
}

pub(super) struct ChainContext {
    pub number: String,
    pub hash: String,
    pub transactions: Vec<String>,
}

pub(super) fn quantity(value: &str) -> anyhow::Result<u64> {
    let hex = value
        .strip_prefix("0x")
        .context("block must be a hexadecimal quantity")?;
    ensure!(
        !hex.is_empty() && (hex.len() == 1 || !hex.starts_with('0')),
        "block must use canonical hexadecimal quantity encoding"
    );
    u64::from_str_radix(hex, 16).map_err(|_| anyhow::anyhow!("invalid block quantity"))
}

pub(super) fn is_hash(value: &str) -> bool {
    value.len() == 66
        && value.starts_with("0x")
        && value[2..].bytes().all(|b| b.is_ascii_hexdigit())
}

pub(super) fn read_cases(path: Option<&Path>) -> anyhow::Result<Vec<Case>> {
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let bytes = std::fs::read(path).context("cannot read request file")?;
    ensure!(bytes.len() <= 4 * 1024 * 1024, "request file exceeds 4 MiB");
    Json::parse(&bytes).map_err(|_| anyhow::anyhow!("invalid request JSON"))?;
    let cases: Vec<Case> = serde_json::from_slice(&bytes)
        .map_err(|_| anyhow::anyhow!("invalid request cases; see docs/rpc-comparison.md"))?;
    ensure!(
        cases.len() <= 256,
        "at most 256 additional cases are allowed"
    );
    Ok(cases)
}

fn case(name: &str, method: &str, params: Value) -> Case {
    Case {
        name: name.to_owned(),
        method: method.to_owned(),
        params,
        comparison: Comparison::Exact,
        expect: Expect::NonNull,
        unavailable: None,
    }
}

pub(super) fn build_cases(context: &ChainContext, extra: Vec<Case>) -> anyhow::Result<Vec<Case>> {
    let state = json!({"blockHash":context.hash,"requireCanonical":true});
    let zero = "0x0000000000000000000000000000000000000000";
    let fixture = "0x0000000000000000000000000000000000001000";
    let call = json!({"from":zero,"to":zero,"gas":"0x186a0","value":"0x0","data":"0x"});
    let fixture_call = json!({"from":zero,"to":fixture,"gas":"0x186a0","value":"0x0","data":"0x"});
    let mut cases = vec![
        case(
            "block_number_hashes",
            "eth_getBlockByNumber",
            json!([context.number, false]),
        ),
        case(
            "block_number_full",
            "eth_getBlockByNumber",
            json!([context.number, true]),
        ),
        case(
            "block_hash_hashes",
            "eth_getBlockByHash",
            json!([context.hash, false]),
        ),
        case(
            "block_hash_full",
            "eth_getBlockByHash",
            json!([context.hash, true]),
        ),
        case("balance", "eth_getBalance", json!([zero, state])),
        case("nonce", "eth_getTransactionCount", json!([zero, state])),
        case("code", "eth_getCode", json!([zero, state])),
        case("storage", "eth_getStorageAt", json!([zero, "0x0", state])),
        case("logs", "eth_getLogs", json!([{"blockHash":context.hash}])),
        case("call", "eth_call", json!([call, state])),
        case(
            "call_override_result",
            "eth_call",
            json!([fixture_call,state,{fixture:{"code":"0x602a60005260206000f3"}}]),
        ),
        case(
            "call_override_revert",
            "eth_call",
            json!([fixture_call,state,{fixture:{"code":"0x60006000fd"}}]),
        ),
        case(
            "trace_call",
            "trace_call",
            json!([call, ["trace"], context.number]),
        ),
        case(
            "trace_call_many",
            "trace_callMany",
            json!([[[call, ["trace"]]], context.number]),
        ),
        case("trace_block", "trace_block", json!([context.number])),
        case(
            "trace_filter",
            "trace_filter",
            json!([{"fromBlock":context.number,"toBlock":context.number,"count":1000}]),
        ),
        case(
            "trace_replay_block",
            "trace_replayBlockTransactions",
            json!([context.number, ["trace"]]),
        ),
        case(
            "transaction",
            "eth_getTransactionByHash",
            json!(["$TX_HASH"]),
        ),
        case("receipt", "eth_getTransactionReceipt", json!(["$TX_HASH"])),
        case(
            "trace_transaction",
            "trace_transaction",
            json!(["$TX_HASH"]),
        ),
        case("trace_get", "trace_get", json!(["$TX_HASH", []])),
        case(
            "trace_replay_transaction",
            "trace_replayTransaction",
            json!(["$TX_HASH", ["trace"]]),
        ),
    ];
    cases
        .iter_mut()
        .find(|c| c.name == "call_override_revert")
        .unwrap()
        .expect = Expect::Revert;
    let mut estimate = case(
        "estimate_gas_policy",
        "eth_estimateGas",
        json!([call, context.number]),
    );
    estimate.comparison = Comparison::Observation;
    cases.push(estimate);
    let mut head = case("head_observation", "eth_blockNumber", json!([]));
    head.comparison = Comparison::Observation;
    cases.push(head);
    cases.extend(extra);
    let mut names = std::collections::HashSet::new();
    for case in &mut cases {
        ensure!(
            !case.name.is_empty() && names.insert(case.name.clone()),
            "case names must be nonempty and unique"
        );
        bind(&mut case.params, context, &mut case.unavailable);
        validate(case, context)?;
    }
    Ok(cases)
}

fn bind(value: &mut Value, context: &ChainContext, unavailable: &mut Option<String>) {
    match value {
        Value::String(text) => match text.as_str() {
            "$BLOCK_NUMBER" => *value = json!(context.number),
            "$BLOCK_HASH" => *value = json!(context.hash),
            "$STATE_BLOCK" => *value = json!({"blockHash":context.hash,"requireCanonical":true}),
            "$TX_HASH" => match context.transactions.first() {
                Some(hash) => *value = json!(hash),
                None => *unavailable = Some("anchor block has no transaction for this case".into()),
            },
            _ => {}
        },
        Value::Array(values) => {
            for value in values {
                bind(value, context, unavailable);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                bind(value, context, unavailable);
            }
        }
        _ => {}
    }
}

fn validate(case: &Case, context: &ChainContext) -> anyhow::Result<()> {
    let params = case
        .params
        .as_array()
        .context("case params must be an array")?;
    // An explicit allowlist prevents supplied corpora from submitting a transaction
    // or invoking debug/admin methods with side effects.
    let block_index = match case.method.as_str() {
        "eth_getBlockByNumber" | "trace_block" | "trace_replayBlockTransactions" => Some(0),
        "eth_getBalance"
        | "eth_getTransactionCount"
        | "eth_getCode"
        | "eth_call"
        | "eth_estimateGas"
        | "trace_callMany" => Some(1),
        "eth_getStorageAt" | "eth_getProof" | "trace_call" => Some(2),
        "eth_getBlockByHash"
        | "eth_getTransactionByHash"
        | "eth_getTransactionReceipt"
        | "trace_transaction"
        | "trace_get"
        | "trace_replayTransaction"
        | "eth_getLogs"
        | "trace_filter"
        | "eth_blockNumber"
        | "eth_chainId" => None,
        _ => anyhow::bail!("request method is outside the read-only comparison allowlist"),
    };
    if case.comparison == Comparison::Observation
        && !case.method.starts_with("trace_")
        && case.method != "eth_getLogs"
    {
        return Ok(());
    }
    ensure!(
        !matches!(case.method.as_str(), "eth_estimateGas" | "eth_blockNumber"),
        "gas estimates and moving heads must use observation comparison"
    );
    if let Some(index) = block_index {
        let block = params
            .get(index)
            .context("exact case requires an explicit anchor block")?;
        ensure!(
            block == &json!(context.number)
                || block == &json!({"blockHash":context.hash,"requireCanonical":true}),
            "exact case must use the selected anchor block"
        );
    }
    match case.method.as_str() {
        "eth_getBlockByHash" => ensure!(
            params.first() == Some(&json!(context.hash)),
            "block case must use the anchor hash"
        ),
        "eth_getTransactionByHash"
        | "eth_getTransactionReceipt"
        | "trace_transaction"
        | "trace_get"
        | "trace_replayTransaction" => {
            if case.unavailable.is_none() {
                ensure!(
                    params
                        .first()
                        .and_then(Value::as_str)
                        .is_some_and(|hash| context.transactions.iter().any(|t| t == hash)),
                    "transaction case must use a transaction in the anchor block"
                );
            }
        }
        "eth_getLogs" => {
            let filter = params.first().context("missing log filter")?;
            ensure!(
                filter.get("blockHash") == Some(&json!(context.hash))
                    && filter.get("fromBlock").is_none()
                    && filter.get("toBlock").is_none(),
                "exact logs must use only the anchor blockHash"
            );
        }
        "trace_filter" => {
            let filter = params.first().context("missing trace filter")?;
            ensure!(
                filter.get("fromBlock") == Some(&json!(context.number))
                    && filter.get("toBlock") == Some(&json!(context.number)),
                "trace filter must stay within the anchor block"
            );
            ensure!(
                filter
                    .get("count")
                    .and_then(Value::as_u64)
                    .is_some_and(|count| (1..=1000).contains(&count)),
                "trace filter requires count between 1 and 1000"
            );
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn request(case: &Case, id: usize) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":case.method,"params":case.params})
}

pub(super) fn expected(case: &Case, reply: &Json) -> bool {
    match case.expect {
        Expect::NonNull => reply.field("result").is_some_and(|v| v != &Json::Null),
        Expect::Result => reply.field("result").is_some(),
        Expect::Revert => reply.field("error").is_some_and(super::strict::is_revert),
    }
}
