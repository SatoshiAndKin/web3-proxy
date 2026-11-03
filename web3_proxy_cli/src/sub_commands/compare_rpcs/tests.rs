use super::{
    corpus::{self, Case, ChainContext, Comparison, Expect},
    runner::Runner,
    strict::{self, Json},
    *,
};
use serde_json::{json, Value};
use std::{
    fs,
    sync::{Arc, Mutex},
    time::Duration,
};
use web3_proxy::prelude::tokio::{
    self,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn parse(text: &str) -> Json {
    Json::parse(text.as_bytes()).unwrap()
}
fn hash(digit: char) -> String {
    format!("0x{}", digit.to_string().repeat(64))
}

#[test]
fn comparison_preserves_fields_types_hex_array_order_and_large_numbers() {
    let a = parse(
        r#"{"result":{"unknown":90071992547409930000000000000001,"hex":"0x0","items":[1,2],"field":null},"id":1,"jsonrpc":"2.0"}"#,
    );
    let reordered = parse(
        r#"{ "jsonrpc":"2.0", "id":1, "result":{"field":null,"items":[1,2],"hex":"0x0","unknown":90071992547409930000000000000001} }"#,
    );
    assert_eq!(a, reordered);
    for (from, to, path) in [
        (
            "90071992547409930000000000000001",
            "90071992547409930000000000000002",
            "/result/unknown",
        ),
        ("\"0x0\"", "\"0x00\"", "/result/hex"),
        ("[1,2]", "[2,1]", "/result/items/0"),
        ("\"field\":null", "\"field\":false", "/result/field"),
    ] {
        let value = serde_json::to_string(&a.value()).unwrap().replace(from, to);
        let b = parse(&value);
        assert_ne!(a, b);
        assert!(strict::differences(&a, &b).iter().any(|d| d.path == path));
    }
    let missing = parse(r#"{"result":{"a":null}}"#);
    let empty = parse(r#"{"result":{}}"#);
    let diff = strict::differences(&missing, &empty);
    assert_eq!(diff[0].left, Some(Value::Null));
    assert_eq!(diff[0].right, None);
    let serialized = serde_json::to_value(&diff[0]).unwrap();
    assert_eq!(serialized["left"], Value::Null);
    assert!(serialized.get("right").is_none());
    // Numeric tokens remain exact; strings compare after JSON escape decoding.
    assert_ne!(parse("1"), parse("1.0"));
    assert_eq!(parse(r#""\u0061""#), parse(r#""a""#));
}

#[test]
fn empty_revert_data_normalization_keeps_other_error_differences() {
    fn compared(value: Value) -> Json {
        let mut reply = parse(&value.to_string());
        strict::normalize_empty_revert_data(&mut reply);
        reply
    }
    let revert = json!({"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}});
    let expected = compared(revert.clone());
    let mut empty = revert.clone();
    empty["error"]["data"] = json!("0x");
    assert_eq!(expected, compared(empty.clone()));
    for code in [3, -32000] {
        let mut missing = revert.clone();
        missing["error"]["code"] = json!(code);
        let mut empty = missing.clone();
        empty["error"]["data"] = json!("0x");
        assert_eq!(compared(missing), compared(empty));
    }
    for data in [
        Value::Null,
        json!(""),
        json!("0x00"),
        json!("0xdead"),
        json!({"data":"0x"}),
    ] {
        let mut changed = revert.clone();
        changed["error"]["data"] = data;
        assert_ne!(expected, compared(changed));
    }
    for (field, value) in [
        ("code", json!(-32000)),
        ("message", json!("execution reverted: reason")),
        ("extra", json!("0x")),
    ] {
        let mut changed = empty.clone();
        changed["error"][field] = value;
        assert_ne!(expected, compared(changed));
    }
    let other = json!({"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid params"}});
    let mut other_empty = other.clone();
    other_empty["error"]["data"] = json!("0x");
    assert_ne!(compared(other), compared(other_empty));
    let result = json!({"jsonrpc":"2.0","id":1,"result":{"data":"0x"}});
    assert_eq!(compared(result.clone()), parse(&result.to_string()));
}

#[test]
fn decoder_rejects_malformed_and_duplicate_keys_at_every_depth() {
    for value in [
        r#"{"id":1,"id":2}"#,
        r#"{"result":{"x":1,"\u0078":2}}"#,
        "[1,]",
        "{}{}",
        "NaN",
    ] {
        assert!(Json::parse(value.as_bytes()).is_err(), "{value}");
    }
    for reply in [
        r#"{"jsonrpc":"2.0","id":1,"result":null,"error":{"code":3,"message":"x"}}"#,
        r#"{"jsonrpc":"2.0","id":2,"result":null}"#,
        r#"{"jsonrpc":"1.0","id":1,"result":null}"#,
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":"3","message":"x"}}"#,
    ] {
        assert!(strict::envelope(&parse(reply), &parse("1")).is_err());
    }
}

fn context() -> ChainContext {
    ChainContext {
        number: "0x10".into(),
        hash: hash('1'),
        transactions: vec![hash('2')],
    }
}

#[test]
fn corpus_is_pinned_bounded_and_read_only() {
    let context = context();
    let cases = corpus::build_cases(&context, vec![], 32).unwrap();
    assert!(cases.iter().any(|c| c.method == "trace_filter"));
    assert!(cases
        .iter()
        .any(|c| c.method == "eth_call" && matches!(c.expect, Expect::Revert)));
    let override_call = cases
        .iter()
        .find(|c| c.name == "call_override_result")
        .unwrap();
    assert_eq!(
        override_call.params[2]["0x0000000000000000000000000000000000001000"]["code"],
        "0x602a60005260206000f3"
    );
    for (method, params) in [
        ("eth_sendRawTransaction", json!(["0x"])),
        ("debug_setHead", json!(["0x1"])),
        ("eth_call", json!([{}, "latest"])),
        (
            "eth_getLogs",
            json!([{"fromBlock":"0x0","toBlock":"latest"}]),
        ),
        (
            "trace_filter",
            json!([{"fromBlock":"0x0","toBlock":"0x10","count":1000}]),
        ),
    ] {
        let case = Case {
            name: "custom".into(),
            method: method.into(),
            params,
            comparison: Comparison::Exact,
            expect: Expect::NonNull,
            unavailable: None,
        };
        assert!(corpus::build_cases(&context, vec![case], 32).is_err());
    }
    let mut empty = context;
    empty.transactions.clear();
    let cases = corpus::build_cases(&empty, vec![], 32).unwrap();
    assert!(cases
        .iter()
        .find(|c| c.name == "receipt")
        .unwrap()
        .unavailable
        .is_some());
    let bounded = corpus::build_cases(
        &ChainContext {
            transactions: vec![hash('2')],
            ..empty
        },
        vec![],
        0,
    )
    .unwrap();
    assert!(bounded
        .iter()
        .find(|c| c.method == "trace_block")
        .unwrap()
        .unavailable
        .is_some());
}

#[test]
fn matching_errors_are_not_successful_coverage() {
    let cases = corpus::build_cases(&context(), vec![], 32).unwrap();
    let unsupported =
        parse(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"unsupported"}}"#);
    assert!(!corpus::expected(&cases[0], &unsupported));
    let revert_case = cases
        .iter()
        .find(|c| matches!(c.expect, Expect::Revert))
        .unwrap();
    assert!(!corpus::expected(revert_case, &unsupported));
    assert!(corpus::expected(
        revert_case,
        &parse(r#"{"error":{"code":3,"message":"execution reverted","data":"0x"}}"#)
    ));
}

struct Mock {
    url: reqwest::Url,
    task: tokio::task::JoinHandle<()>,
    seen: Arc<Mutex<Vec<Value>>>,
}
impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

type Handler = Arc<dyn Fn(Value) -> (u16, String) + Send + Sync>;
impl Mock {
    async fn start(handler: Handler) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = seen.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buffer = Vec::new();
                let header_end = loop {
                    let mut chunk = [0; 4096];
                    let n = socket.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        break None;
                    }
                    buffer.extend_from_slice(&chunk[..n]);
                    if let Some(i) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                        break Some(i + 4);
                    }
                };
                let Some(header_end) = header_end else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&buffer[..header_end]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .and_then(|v| v.trim().parse::<usize>().ok())
                    })
                    .unwrap();
                while buffer.len() < header_end + length {
                    let mut chunk = [0; 4096];
                    let n = socket.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    buffer.extend_from_slice(&chunk[..n]);
                }
                let request: Value =
                    serde_json::from_slice(&buffer[header_end..header_end + length]).unwrap();
                captured.lock().unwrap().push(request.clone());
                let (status, body) = handler(request);
                let header = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(header.as_bytes()).await;
                let _ = socket.write_all(body.as_bytes()).await;
            }
        });
        Self { url, task, seen }
    }
}

fn answer(request: &Value) -> Value {
    let method = request["method"].as_str().unwrap();
    let params = &request["params"];
    let result = match method {
        "eth_chainId" => json!("0x1"),
        "web3_clientVersion" => json!("test/v1"),
        "eth_blockNumber" => json!("0x11"),
        "eth_getBlockByNumber" | "eth_getBlockByHash" => {
            if params[0] == "0x0" {
                json!({"number":"0x0","hash":hash('0'),"transactions":[]})
            } else {
                json!({"number":"0x10","hash":hash('1'),"transactions":[hash('2')],"unknownProviderField":90071992547409930000000000000001u128})
            }
        }
        "eth_call" if params.to_string().contains("0x60006000fd") => {
            return json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":3,"message":"execution reverted","data":"0x"}});
        }
        "eth_getTransactionByHash" => {
            json!({"hash":hash('2'),"blockHash":hash('1'),"blockNumber":"0x10"})
        }
        "eth_getTransactionReceipt" => {
            json!({"transactionHash":hash('2'),"blockHash":hash('1'),"blockNumber":"0x10","logs":[]})
        }
        "eth_getLogs"
        | "trace_block"
        | "trace_filter"
        | "trace_transaction"
        | "trace_replayBlockTransactions" => json!([]),
        "trace_callMany" => json!([{"output":"0x","trace":[]}]),
        "trace_call" | "trace_get" | "trace_replayTransaction" => json!({"output":"0x","trace":[]}),
        _ => json!("0x0"),
    };
    json!({"jsonrpc":"2.0","id":request["id"],"result":result})
}

fn normal(request: Value) -> (u16, String) {
    if let Some(requests) = request.as_array() {
        // Legal batch reordering must not become a content difference.
        (
            200,
            Value::Array(requests.iter().rev().map(answer).collect()).to_string(),
        )
    } else {
        (200, answer(&request).to_string())
    }
}

async fn run_mocks(handlers: Vec<Handler>) -> (Value, Vec<Mock>, tempfile::TempDir) {
    let mut mocks = Vec::new();
    let mut endpoints = Vec::new();
    for (i, handler) in handlers.into_iter().enumerate() {
        let mock = Mock::start(handler).await;
        endpoints.push(Endpoint {
            name: format!("node{i}"),
            url: mock.url.clone(),
            client: reqwest::Client::builder()
                .retry(reqwest::retry::never())
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap(),
            next_start: tokio::time::Instant::now(),
        });
        mocks.push(mock);
    }
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("capture");
    let settings = Settings {
        interval: Duration::ZERO,
        timeout: Duration::from_secs(2),
        max_response_bytes: 1_000_000,
        max_trace_transactions: 32,
        batch_size: 4,
    };
    let mut runner = Runner::new(endpoints, settings, vec![], &output).unwrap();
    let report = runner
        .run(None, vec![], Some("operator-supplied-test-version".into()))
        .await
        .unwrap();
    runner.save_report(&report).unwrap();
    let mut value = serde_json::to_value(&report).unwrap();
    value["exit_code"] = json!(report.exit_code());
    (value, mocks, directory)
}

#[tokio::test]
async fn complete_http_run_compares_singles_batches_repeats_and_keeps_raw_evidence() {
    let (report, mocks, directory) =
        run_mocks(vec![Arc::new(normal), Arc::new(normal), Arc::new(normal)]).await;
    assert_eq!(report["exit_code"], 0);
    assert!(report["agreement"].as_u64().unwrap() > 100);
    assert_eq!(report["incomplete"], 0);
    assert_eq!(report["anchor_stable"], true);
    assert_eq!(report["client_versions"]["node1"], "test/v1");
    assert!(directory
        .path()
        .join("capture/000001-request.json")
        .exists());
    let journal = fs::read_to_string(directory.path().join("capture/exchanges.jsonl")).unwrap();
    assert!(journal.contains("body_file"));
    assert!(journal.lines().count() > 80);
    let first: Vec<_> = mocks[0]
        .seen
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r["method"] == "eth_getCode")
        .cloned()
        .collect();
    let direct: Vec<_> = mocks[1]
        .seen
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r["method"] == "eth_getCode")
        .cloned()
        .collect();
    assert_eq!(first, direct);
    assert_eq!(first.len(), 3);
    let requests = mocks[0].seen.lock().unwrap();
    let call_batches: Vec<_> = requests
        .iter()
        .filter_map(Value::as_array)
        .filter(|batch| batch.len() == 3 && batch.iter().all(|call| call["method"] == "eth_call"))
        .collect();
    assert_eq!(
        call_batches.len(),
        3,
        "exercise the optimized all-eth_call batch path in every round"
    );
    let persisted: Value =
        serde_json::from_slice(&fs::read(directory.path().join("capture/report.json")).unwrap())
            .unwrap();
    assert!(persisted["results"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["name"] == "call" && row["form"] == "method_batch"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(directory.path().join("capture"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }
}

fn changed_code(request: Value) -> (u16, String) {
    fn changed(request: &Value) -> Value {
        let mut reply = answer(request);
        if request["method"] == "eth_getCode" {
            reply["result"] = json!("0x00");
        }
        reply
    }
    let response = if let Some(requests) = request.as_array() {
        Value::Array(requests.iter().map(changed).collect())
    } else {
        changed(&request)
    };
    (200, response.to_string())
}

#[tokio::test]
async fn empty_revert_data_is_equal_across_backends_batches_and_repeats() {
    fn handler(omit: bool, alternate: bool) -> Handler {
        let count = Mutex::new(0);
        Arc::new(move |request| {
            let (status, body) = normal(request);
            let mut reply: Value = serde_json::from_str(&body).unwrap();
            let replies = match &mut reply {
                Value::Array(replies) => replies.as_mut_slice(),
                reply => std::slice::from_mut(reply),
            };
            for reply in replies {
                if reply.get("error").is_some() {
                    let mut count = count.lock().unwrap();
                    *count += 1;
                    if omit || (alternate && *count % 2 == 0) {
                        reply["error"].as_object_mut().unwrap().remove("data");
                    }
                }
            }
            (status, reply.to_string())
        })
    }
    let (report, _, directory) = run_mocks(vec![
        handler(false, true),
        handler(true, false),
        handler(false, false),
    ])
    .await;
    assert_eq!(report["exit_code"], 0);
    for counter in [
        "backend_differences",
        "proxy_differences",
        "repeat_differences",
        "incomplete",
    ] {
        assert_eq!(report[counter], 0, "{counter}");
    }
    let capture = directory.path().join("capture");
    let journal = fs::read_to_string(capture.join("exchanges.jsonl")).unwrap();
    let mut variants = [false; 2];
    for line in journal.lines() {
        let exchange: Value = serde_json::from_str(line).unwrap();
        for observation in exchange["observations"].as_array().unwrap() {
            if observation["endpoint"] != "node0" {
                continue;
            }
            let body = fs::read(capture.join(observation["body_file"].as_str().unwrap())).unwrap();
            let reply: Value = serde_json::from_slice(&body).unwrap();
            let replies = reply
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or(std::slice::from_ref(&reply));
            for reply in replies {
                if let Some(error) = reply.get("error") {
                    variants[usize::from(error.get("data").is_some())] = true;
                }
            }
        }
    }
    assert_eq!(
        variants,
        [true, true],
        "preserve both original response forms"
    );
}

#[tokio::test]
async fn distinguishes_backend_disagreement_from_proxy_only_difference() {
    let (backend, _, _) = run_mocks(vec![
        Arc::new(normal),
        Arc::new(normal),
        Arc::new(changed_code),
    ])
    .await;
    assert_eq!(backend["exit_code"], 1);
    assert_eq!(backend["backend_differences"], 6);
    assert_eq!(backend["proxy_differences"], 0);
    let rows = backend["results"].as_array().unwrap();
    let row = rows.iter().find(|r| r["name"] == "code").unwrap();
    assert_eq!(row["matching_backends"], json!(["node1"]));
    assert_eq!(row["differences"][0]["differences"][0]["path"], "/result");
    let (proxy, _, _) = run_mocks(vec![
        Arc::new(changed_code),
        Arc::new(normal),
        Arc::new(normal),
    ])
    .await;
    assert_eq!(proxy["exit_code"], 1);
    assert_eq!(proxy["proxy_differences"], 6);
    assert_eq!(proxy["backend_differences"], 0);
}

#[tokio::test]
async fn equal_unsupported_trace_replies_leave_coverage_incomplete() {
    let handler: Handler = Arc::new(|request| {
        fn unsupported(request: &Value) -> Value {
            if request["method"].as_str().unwrap().starts_with("trace_") {
                json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32601,"message":"method not found"}})
            } else {
                answer(request)
            }
        }
        let value = match request.as_array() {
            Some(requests) => Value::Array(requests.iter().map(unsupported).collect()),
            None => unsupported(&request),
        };
        (200, value.to_string())
    });
    let (report, _, _) = run_mocks(vec![handler.clone(), handler]).await;
    assert_eq!(report["exit_code"], 2);
    assert!(report["incomplete"].as_u64().unwrap() > 0);
    assert_eq!(report["backend_differences"], 0);
    assert_eq!(report["proxy_differences"], 0);
}

#[tokio::test]
async fn changed_anchor_invalidates_equality_verdicts() {
    let count = Arc::new(Mutex::new(0));
    let handler: Handler = Arc::new(move |request| {
        let (status, body) = normal(request.clone());
        if request["method"] == "eth_getBlockByNumber"
            && request["params"] == json!(["0x10", false])
            && request["id"] == 0
        {
            let mut count = count.lock().unwrap();
            *count += 1;
            if *count == 2 {
                let mut body: Value = serde_json::from_str(&body).unwrap();
                body["result"]["hash"] = json!(hash('3'));
                return (status, body.to_string());
            }
        }
        (status, body)
    });
    let (report, _, _) = run_mocks(vec![handler, Arc::new(normal)]).await;
    assert_eq!(report["exit_code"], 2);
    assert_eq!(report["anchor_stable"], false);
    assert_eq!(report["agreement"], 0);
}

#[tokio::test]
async fn repeat_changes_fail_even_when_all_endpoints_agree_in_each_round() {
    fn changing() -> Handler {
        let count = Arc::new(Mutex::new(0));
        Arc::new(move |request| {
            fn edit(request: &Value, count: &Mutex<usize>) -> Value {
                let mut response = answer(request);
                if request["method"] == "eth_getCode" {
                    let mut n = count.lock().unwrap();
                    *n += 1;
                    if *n > 1 {
                        response["result"] = json!("0x00");
                    }
                }
                response
            }
            let reply = if let Some(requests) = request.as_array() {
                Value::Array(requests.iter().map(|r| edit(r, &count)).collect())
            } else {
                edit(&request, &count)
            };
            (200, reply.to_string())
        })
    }
    let (report, _, _) = run_mocks(vec![changing(), changing(), changing()]).await;
    assert_eq!(report["exit_code"], 1);
    assert_eq!(report["repeat_differences"], 5);
    assert_eq!(report["backend_differences"], 0);
    assert_eq!(report["proxy_differences"], 0);
}

#[tokio::test]
async fn credentials_are_withheld_and_existing_evidence_is_preserved() {
    let secret = "fixture-private-credential";
    let mock = Mock::start(Arc::new(move |request| {
        (
            200,
            json!({"jsonrpc":"2.0","id":request["id"],"result":secret})
                .to_string()
                .replace("fixture", r"\u0066ixture"),
        )
    }))
    .await;
    let endpoint = Endpoint {
        name: "test".into(),
        url: mock.url.clone(),
        client: reqwest::Client::new(),
        next_start: tokio::time::Instant::now(),
    };
    let settings = Settings {
        interval: Duration::ZERO,
        timeout: Duration::from_secs(1),
        max_response_bytes: 1024,
        max_trace_transactions: 32,
        batch_size: 4,
    };
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("capture");
    let mut runner = Runner::new(vec![endpoint], settings, vec![secret.into()], &output).unwrap();
    assert!(runner.run(Some("0x10".into()), vec![], None).await.is_err());
    let journal = fs::read_to_string(output.join("exchanges.jsonl")).unwrap();
    assert!(journal.contains("withheld"));
    assert!(!journal.contains(secret));
    assert!(!output.join("000001-test.body").exists());
    let settings = Settings {
        interval: Duration::ZERO,
        timeout: Duration::from_secs(1),
        max_response_bytes: 1024,
        max_trace_transactions: 32,
        batch_size: 4,
    };
    assert!(Runner::new(vec![], settings, vec![], &output).is_err());
    assert_eq!(
        fs::read_to_string(output.join("exchanges.jsonl")).unwrap(),
        journal
    );
}

#[tokio::test]
async fn detects_proxy_changes_confined_to_same_method_batches() {
    let proxy: Handler = Arc::new(|request| {
        let method_batch = request.as_array().is_some_and(|batch| {
            batch.len() > 1 && batch.iter().all(|call| call["method"] == "eth_call")
        });
        let (status, body) = normal(request);
        if !method_batch {
            return (status, body);
        }
        let mut replies: Value = serde_json::from_str(&body).unwrap();
        for reply in replies.as_array_mut().unwrap() {
            if reply.get("result").is_some() {
                reply["result"] = json!("0x1234");
            }
        }
        (status, replies.to_string())
    });
    let (report, _, _) = run_mocks(vec![proxy, Arc::new(normal), Arc::new(normal)]).await;
    assert_eq!(report["exit_code"], 1);
    assert_eq!(report["proxy_differences"], 6);
    assert_eq!(report["backend_differences"], 0);
    assert!(report["results"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["classification"] == "proxy_only_difference")
        .all(|row| row["form"] == "method_batch"));
}
