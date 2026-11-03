use super::{
    corpus::{self, Case, ChainContext, Comparison},
    strict::{self, Difference, Json},
    Endpoint, Settings,
};
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
use web3_proxy::prelude::{
    anyhow::{self, ensure, Context},
    futures::future::join_all,
    jiff::Timestamp,
    reqwest, tokio,
};

#[derive(Serialize)]
struct Observation {
    endpoint: String,
    status: Option<u16>,
    elapsed_ms: u128,
    headers: BTreeMap<String, String>,
    error: Option<String>,
    body_file: Option<String>,
    #[serde(skip)]
    body: Vec<u8>,
    #[serde(skip)]
    parsed: Option<Json>,
}

#[derive(Serialize)]
struct PairDifference {
    left_endpoint: String,
    right_endpoint: String,
    differences: Vec<Difference>,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum Form {
    Individual,
    MixedBatch,
    MethodBatch,
    Skipped,
}

impl Form {
    fn is_batch(self) -> bool {
        matches!(self, Self::MixedBatch | Self::MethodBatch)
    }
}

#[derive(Serialize)]
struct CaseResult {
    name: String,
    round: usize,
    form: Form,
    classification: &'static str,
    matching_backends: Vec<String>,
    coverage_gaps: Vec<String>,
    differences: Vec<PairDifference>,
    repeat_differences: Vec<PairDifference>,
}

#[derive(Serialize)]
pub(super) struct Report {
    schema_version: u8,
    started_at: String,
    finished_at: Option<String>,
    endpoints: Vec<String>,
    proxy_version_supplied: Option<String>,
    client_versions: BTreeMap<String, Option<String>>,
    anchor: Value,
    limits: Value,
    anchor_stable: bool,
    cases: Vec<Case>,
    results: Vec<CaseResult>,
    pub agreement: usize,
    pub backend_differences: usize,
    pub proxy_differences: usize,
    pub repeat_differences: usize,
    pub incomplete: usize,
    pub observations: usize,
}

impl Report {
    pub fn exit_code(&self) -> u8 {
        if self.backend_differences + self.proxy_differences + self.repeat_differences > 0 {
            1
        } else if self.incomplete > 0 || self.agreement == 0 || !self.anchor_stable {
            2
        } else {
            0
        }
    }
}

pub(super) struct Runner {
    endpoints: Vec<Endpoint>,
    settings: Settings,
    secrets: Vec<String>,
    output: PathBuf,
    journal: File,
    sequence: usize,
    started_at: String,
}

fn private_file(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

impl Runner {
    pub fn new(
        endpoints: Vec<Endpoint>,
        settings: Settings,
        secrets: Vec<String>,
        output: &Path,
    ) -> anyhow::Result<Self> {
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(output).context(
            "cannot create new evidence directory (parent must exist; output must not exist)",
        )?;
        let journal = private_file(&output.join("exchanges.jsonl"))?;
        Ok(Self {
            endpoints,
            settings,
            secrets,
            output: output.to_owned(),
            journal,
            sequence: 0,
            started_at: Timestamp::now().to_string(),
        })
    }

    fn reflects_secret(&self, bytes: &[u8]) -> bool {
        self.secrets.iter().any(|secret| {
            !secret.is_empty()
                && bytes
                    .windows(secret.len())
                    .any(|part| part == secret.as_bytes())
        })
    }

    fn json_reflects_secret(&self, value: &Json) -> bool {
        match value {
            Json::String(text) => self.reflects_secret(text.as_bytes()),
            Json::Array(values) => values.iter().any(|v| self.json_reflects_secret(v)),
            Json::Object(fields) => fields
                .iter()
                .any(|(k, v)| self.reflects_secret(k.as_bytes()) || self.json_reflects_secret(v)),
            _ => false,
        }
    }

    fn save_json(&self, name: &str, value: &impl Serialize) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec_pretty(value)?;
        ensure!(
            !self.reflects_secret(&bytes),
            "evidence withheld because it reflects endpoint credentials"
        );
        private_file(&self.output.join(name))?.write_all(&bytes)?;
        Ok(())
    }

    pub fn save_report(&self, report: &Report) -> anyhow::Result<()> {
        let mut value = serde_json::to_value(report)?;
        value["exit_code"] = json!(report.exit_code());
        self.save_json("report.json", &value)
    }
    pub fn save_failure(&self, reason: &str) -> anyhow::Result<()> {
        self.save_json("report.json", &json!({"schema_version":1,"started_at":self.started_at,"finished_at":Timestamp::now().to_string(),"exit_code":2,"failure":reason,"exchanges":self.sequence}))
    }

    async fn exchange(
        &mut self,
        request: &Value,
        indices: &[usize],
    ) -> anyhow::Result<Vec<Observation>> {
        let bytes = serde_json::to_vec(request)?;
        ensure!(
            !self.reflects_secret(&bytes),
            "request withheld because it contains endpoint credentials"
        );
        self.sequence += 1;
        let prefix = format!("{:06}", self.sequence);
        private_file(&self.output.join(format!("{prefix}-request.json")))?.write_all(&bytes)?;
        let settings = &self.settings;
        let mut replies = join_all(
            self.endpoints
                .iter_mut()
                .enumerate()
                .filter(|(i, _)| indices.contains(i))
                .map(|(_, endpoint)| fetch(endpoint, bytes.clone(), settings)),
        )
        .await;
        for reply in &mut replies {
            let parsed = Json::parse(&reply.body);
            let decoded_secret = parsed
                .as_ref()
                .is_ok_and(|value| self.json_reflects_secret(value));
            // Check both raw bytes and decoded strings. Preserve exact bodies
            // unless the server reflects endpoint credentials; then withhold.
            let header_bytes = serde_json::to_vec(&reply.headers)?;
            if self.reflects_secret(&reply.body)
                || self.reflects_secret(&header_bytes)
                || decoded_secret
            {
                reply.body.clear();
                reply.headers.clear();
                reply.error = Some("response withheld: reflected endpoint credentials".into());
            } else {
                let file = format!("{prefix}-{}.body", reply.endpoint);
                private_file(&self.output.join(&file))?.write_all(&reply.body)?;
                reply.body_file = Some(file);
                if reply.error.is_none() {
                    match parsed {
                        Ok(parsed) => reply.parsed = Some(parsed),
                        Err(error) => reply.error = Some(error.into()),
                    }
                }
            }
            reply.body.clear();
        }
        let record = json!({"recorded_at":Timestamp::now().to_string(),"exchange":self.sequence,"request_file":format!("{prefix}-request.json"),"observations":replies});
        let record = serde_json::to_vec(&record)?;
        self.journal.write_all(&record)?;
        self.journal.write_all(b"\n")?;
        self.journal.flush()?;
        Ok(replies)
    }

    async fn rpc(
        &mut self,
        method: &str,
        params: Value,
        indices: &[usize],
    ) -> anyhow::Result<Vec<Json>> {
        let request = json!({"jsonrpc":"2.0","id":0,"method":method,"params":params});
        let replies = self.exchange(&request, indices).await?;
        replies
            .into_iter()
            .map(|reply| {
                let parsed = reply.parsed.context(format!(
                    "preflight {} failed on {} (see exchanges.jsonl)",
                    method, reply.endpoint
                ))?;
                strict::envelope(&parsed, &Json::Number("0".into())).map_err(|error| {
                    anyhow::anyhow!("preflight {} on {}: {}", method, reply.endpoint, error)
                })?;
                ensure!(
                    parsed.field("result").is_some_and(|v| v != &Json::Null),
                    "preflight {} returned no result on {}",
                    method,
                    reply.endpoint
                );
                Ok(parsed.field("result").unwrap().clone())
            })
            .collect()
    }

    async fn verify_anchor(&mut self, context: &ChainContext, all: &[usize]) -> anyhow::Result<()> {
        let blocks = self
            .rpc("eth_getBlockByNumber", json!([context.number, false]), all)
            .await?;
        ensure!(
            blocks
                .iter()
                .all(
                    |block| block.field("hash").and_then(Json::string) == Some(&context.hash)
                        && block.field("number").and_then(Json::string) == Some(&context.number)
                ),
            "anchor number/hash changed or is unavailable on an endpoint"
        );
        Ok(())
    }

    pub async fn run(
        &mut self,
        explicit_block: Option<String>,
        extra: Vec<Case>,
        proxy_version: Option<String>,
    ) -> anyhow::Result<Report> {
        let all: Vec<_> = (0..self.endpoints.len()).collect();
        let direct: Vec<_> = (1..self.endpoints.len()).collect();
        let chain_ids = self.rpc("eth_chainId", json!([]), &all).await?;
        ensure!(
            chain_ids.iter().all(|id| id == &chain_ids[0]),
            "chain IDs differ"
        );
        corpus::quantity(chain_ids[0].string().context("invalid chain ID")?)?;
        let genesis = self
            .rpc("eth_getBlockByNumber", json!(["0x0", false]), &all)
            .await?;
        let genesis_hash = genesis[0]
            .field("hash")
            .and_then(Json::string)
            .context("genesis hash missing")?;
        ensure!(
            corpus::is_hash(genesis_hash)
                && genesis
                    .iter()
                    .all(|block| block.field("hash").and_then(Json::string) == Some(genesis_hash)),
            "genesis hashes differ or are invalid"
        );
        let number = if let Some(number) = explicit_block {
            number
        } else {
            // Query actual finalized blocks on direct nodes, not the proxy's tag
            // resolution. A chain without finalized support needs --block.
            let finalized = self
                .rpc("eth_getBlockByNumber", json!(["finalized", false]), &direct)
                .await?;
            let numbers = finalized
                .iter()
                .map(|block| {
                    corpus::quantity(
                        block
                            .field("number")
                            .and_then(Json::string)
                            .context("finalized block number missing")?,
                    )
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            format!(
                "0x{:x}",
                numbers.into_iter().min().context("no finalized block")?
            )
        };
        let blocks = self
            .rpc("eth_getBlockByNumber", json!([number, false]), &all)
            .await?;
        let block = &blocks[1];
        let hash = block
            .field("hash")
            .and_then(Json::string)
            .context("anchor hash missing")?
            .to_owned();
        ensure!(corpus::is_hash(&hash), "invalid anchor hash");
        let transactions = match block.field("transactions") {
            Some(Json::Array(txs)) => txs
                .iter()
                .map(|tx| {
                    let hash = tx.string().context("invalid anchor transaction hash")?;
                    ensure!(corpus::is_hash(hash), "invalid anchor transaction hash");
                    Ok(hash.to_owned())
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
            _ => anyhow::bail!("anchor transaction list missing"),
        };
        let context = ChainContext {
            number,
            hash,
            transactions,
        };
        ensure!(
            blocks
                .iter()
                .all(
                    |block| block.field("hash").and_then(Json::string) == Some(&context.hash)
                        && block.field("number").and_then(Json::string) == Some(&context.number)
                ),
            "anchor number/hash differs across endpoints"
        );
        let mut client_versions = BTreeMap::new();
        let versions = self
            .exchange(
                &json!({"jsonrpc":"2.0","id":0,"method":"web3_clientVersion","params":[]}),
                &direct,
            )
            .await?;
        for reply in versions {
            let version = reply
                .parsed
                .filter(|p| strict::envelope(p, &Json::Number("0".into())).is_ok())
                .and_then(|p| p.field("result").and_then(Json::string).map(str::to_owned));
            client_versions.insert(reply.endpoint, version);
        }
        let cases = corpus::build_cases(&context, extra, self.settings.max_trace_transactions)?;
        let mut report = Report {
            schema_version: 1,
            started_at: self.started_at.clone(),
            finished_at: None,
            endpoints: self.endpoints.iter().map(|e| e.name.clone()).collect(),
            proxy_version_supplied: proxy_version,
            client_versions,
            anchor: json!({"chain_id":chain_ids[0].value(),"genesis_hash":genesis_hash,"number":context.number,"hash":context.hash}),
            limits: json!({"interval_ms":self.settings.interval.as_millis(),"timeout_ms":self.settings.timeout.as_millis(),"max_response_bytes":self.settings.max_response_bytes,"max_trace_transactions":self.settings.max_trace_transactions,"batch_size":self.settings.batch_size,"rounds":3}),
            anchor_stable: true,
            cases,
            results: Vec::new(),
            agreement: 0,
            backend_differences: 0,
            proxy_differences: 0,
            repeat_differences: 0,
            incomplete: 0,
            observations: 0,
        };
        for case in &report.cases {
            if let Some(reason) = &case.unavailable {
                report.incomplete += 1;
                report.results.push(CaseResult {
                    name: case.name.clone(),
                    round: 0,
                    form: Form::Skipped,
                    classification: "incomplete",
                    matching_backends: vec![],
                    coverage_gaps: vec![reason.clone()],
                    differences: vec![],
                    repeat_differences: vec![],
                });
            }
        }
        println!(
            "Fixed anchor {} {}. Starting three rounds of individual and batch requests.",
            context.number, context.hash
        );
        let indices: Vec<_> = report
            .cases
            .iter()
            .enumerate()
            .filter(|(_, c)| c.unavailable.is_none())
            .map(|(i, _)| i)
            .collect();
        // Mixed batches and same-method batches can take different application
        // paths. In particular, web3-proxy has an optimized all-eth_call path.
        let mut method_groups = BTreeMap::<String, Vec<usize>>::new();
        for &index in &indices {
            method_groups
                .entry(report.cases[index].method.clone())
                .or_default()
                .push(index);
        }
        let mut baseline = BTreeMap::new();
        for round in 1..=3 {
            println!("Comparison round {round}/3");
            for &index in &indices {
                self.measure(
                    &mut report,
                    &[index],
                    round,
                    Form::Individual,
                    &all,
                    &mut baseline,
                )
                .await?;
            }
            for chunk in indices.chunks(self.settings.batch_size) {
                self.measure(
                    &mut report,
                    chunk,
                    round,
                    Form::MixedBatch,
                    &all,
                    &mut baseline,
                )
                .await?;
            }
            for group in method_groups.values() {
                for chunk in group
                    .chunks(self.settings.batch_size)
                    .filter(|chunk| chunk.len() > 1)
                {
                    self.measure(
                        &mut report,
                        chunk,
                        round,
                        Form::MethodBatch,
                        &all,
                        &mut baseline,
                    )
                    .await?;
                }
            }
        }
        if self.verify_anchor(&context, &all).await.is_err() {
            report.anchor_stable = false;
            report.agreement = 0;
            report.backend_differences = 0;
            report.proxy_differences = 0;
            report.repeat_differences = 0;
            for result in &mut report.results {
                if result.classification != "observation" {
                    result.classification = "incomplete";
                    result.coverage_gaps.push(
                        "anchor could not be verified after capture; equality verdict invalidated"
                            .into(),
                    );
                }
            }
            report.incomplete = report
                .results
                .iter()
                .filter(|r| r.classification == "incomplete")
                .count();
        }
        report.finished_at = Some(Timestamp::now().to_string());
        Ok(report)
    }

    async fn measure(
        &mut self,
        report: &mut Report,
        indices: &[usize],
        round: usize,
        form: Form,
        all: &[usize],
        baseline: &mut BTreeMap<(usize, usize), Json>,
    ) -> anyhow::Result<()> {
        let requests: Vec<_> = indices
            .iter()
            .map(|i| corpus::request(&report.cases[*i], *i + 1))
            .collect();
        let request = if form.is_batch() {
            Value::Array(requests)
        } else {
            requests[0].clone()
        };
        let packets = self.exchange(&request, all).await?;
        let replies: Vec<_> = packets
            .iter()
            .map(|packet| unpack(packet, indices, form.is_batch()))
            .collect();
        for (position, &index) in indices.iter().enumerate() {
            let case = &report.cases[index];
            let mut coverage_gaps = Vec::new();
            let mut values = Vec::new();
            for (endpoint, reply) in replies.iter().enumerate() {
                match reply {
                    Ok(values_by_id) => {
                        let value = &values_by_id[position];
                        if !corpus::expected(case, value) {
                            coverage_gaps.push(format!(
                                "{}: required result/revert unavailable (error or unexpected null)",
                                self.endpoints[endpoint].name
                            ));
                        }
                        values.push(Some(value));
                    }
                    Err(error) => {
                        coverage_gaps.push(format!("{}: {error}", self.endpoints[endpoint].name));
                        values.push(None);
                    }
                }
            }
            let mut differences = Vec::new();
            let mut matching_backends = Vec::new();
            let reference = values
                .iter()
                .enumerate()
                .skip(1)
                .find_map(|(i, value)| value.map(|v| (i, v)));
            let mut backend_difference = false;
            let mut proxy_difference = false;
            if let Some((reference_index, reference)) = reference {
                for (i, value) in values.iter().enumerate().skip(1) {
                    if let Some(value) = value {
                        if *value != reference {
                            backend_difference = true;
                            differences.push(pair(
                                &self.endpoints[reference_index].name,
                                &self.endpoints[i].name,
                                reference,
                                value,
                            ));
                        }
                    }
                }
                if let Some(proxy) = values[0] {
                    for (i, value) in values.iter().enumerate().skip(1) {
                        if value.is_some_and(|v| v == proxy) {
                            matching_backends.push(self.endpoints[i].name.clone());
                        }
                    }
                    if matching_backends.is_empty() {
                        proxy_difference = true;
                        differences.push(pair(
                            &self.endpoints[reference_index].name,
                            &self.endpoints[0].name,
                            reference,
                            proxy,
                        ));
                    }
                }
            }
            let mut repeat_differences = Vec::new();
            if case.comparison == Comparison::Exact {
                for (endpoint, value) in values.iter().enumerate() {
                    if let Some(value) = value {
                        match baseline.get(&(index, endpoint)) {
                            Some(first) if first != *value => {
                                repeat_differences.push(pair(
                                    "first_individual_reply",
                                    &self.endpoints[endpoint].name,
                                    first,
                                    value,
                                ));
                            }
                            None => {
                                baseline.insert((index, endpoint), (*value).clone());
                            }
                            _ => {}
                        }
                    }
                }
            }
            let classification = if case.comparison == Comparison::Observation {
                report.observations += 1;
                "observation"
            } else {
                if !repeat_differences.is_empty() {
                    report.repeat_differences += 1;
                }
                if backend_difference {
                    report.backend_differences += 1;
                }
                if proxy_difference {
                    report.proxy_differences += 1;
                }
                if !coverage_gaps.is_empty() {
                    report.incomplete += 1;
                }
                if proxy_difference {
                    "proxy_only_difference"
                } else if backend_difference {
                    "backend_disagreement"
                } else if !repeat_differences.is_empty() {
                    "repeat_difference"
                } else if !coverage_gaps.is_empty() {
                    "incomplete"
                } else {
                    report.agreement += 1;
                    "full_agreement"
                }
            };
            report.results.push(CaseResult {
                name: case.name.clone(),
                round,
                form,
                classification,
                matching_backends,
                coverage_gaps,
                differences,
                repeat_differences,
            });
        }
        Ok(())
    }
}

fn pair(left: &str, right: &str, a: &Json, b: &Json) -> PairDifference {
    PairDifference {
        left_endpoint: left.to_owned(),
        right_endpoint: right.to_owned(),
        differences: strict::differences(a, b),
    }
}

fn unpack(packet: &Observation, indices: &[usize], batch: bool) -> Result<Vec<Json>, String> {
    if let Some(error) = &packet.error {
        return Err(error.clone());
    }
    let parsed = packet.parsed.as_ref().ok_or("no parsed response")?;
    let values = if batch {
        match parsed {
            Json::Array(values) => values.iter().collect::<Vec<_>>(),
            _ => return Err("batch response is not an array".into()),
        }
    } else {
        vec![parsed]
    };
    if values.len() != indices.len() {
        return Err("missing or extra batch replies".into());
    }
    let mut ordered = BTreeMap::new();
    for value in values {
        let id = value
            .field("id")
            .and_then(Json::number)
            .and_then(|v| v.parse::<usize>().ok())
            .ok_or("invalid response ID")?;
        if !indices.iter().any(|i| *i + 1 == id) {
            return Err("unexpected response ID".into());
        }
        strict::envelope(value, &Json::Number(id.to_string())).map_err(str::to_owned)?;
        if ordered.insert(id, value.clone()).is_some() {
            return Err("duplicate response ID".into());
        }
    }
    indices
        .iter()
        .map(|index| {
            ordered
                .remove(&(index + 1))
                .ok_or_else(|| "missing response ID".into())
        })
        .collect()
}

async fn fetch(endpoint: &mut Endpoint, request: Vec<u8>, settings: &Settings) -> Observation {
    tokio::time::sleep_until(endpoint.next_start).await;
    let start = tokio::time::Instant::now();
    endpoint.next_start = start + settings.interval;
    let mut observation = Observation {
        endpoint: endpoint.name.clone(),
        status: None,
        elapsed_ms: 0,
        headers: BTreeMap::new(),
        error: None,
        body_file: None,
        body: Vec::new(),
        parsed: None,
    };
    let response = endpoint
        .client
        .post(endpoint.url.clone())
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(request)
        .send()
        .await;
    match response {
        Ok(mut response) => {
            observation.status = Some(response.status().as_u16());
            for name in ["x-w3p-backend-rpcs", "x-w3p-backup-rpc", "content-type"] {
                if let Some(value) = response.headers().get(name).and_then(|v| v.to_str().ok()) {
                    observation
                        .headers
                        .insert(name.to_owned(), value.to_owned());
                }
            }
            loop {
                match response.chunk().await {
                    Ok(Some(chunk)) => {
                        let remaining = settings
                            .max_response_bytes
                            .saturating_sub(observation.body.len());
                        observation
                            .body
                            .extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                        if chunk.len() > remaining {
                            observation.error =
                                Some("response exceeds byte limit (saved body is partial)".into());
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(error) => {
                        observation.error = Some(
                            if error.is_timeout() {
                                "response body timed out (saved body is partial)"
                            } else {
                                "response body read failed (saved body is partial)"
                            }
                            .into(),
                        );
                        break;
                    }
                }
            }
            if !response.status().is_success() && observation.error.is_none() {
                observation.error = Some("HTTP status is not successful".into());
            }
        }
        Err(error) => {
            observation.error = Some(
                if error.is_timeout() {
                    "HTTP request timed out"
                } else {
                    "HTTP request failed"
                }
                .into(),
            );
        }
    }
    observation.elapsed_ms = start.elapsed().as_millis();
    observation
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn packet(text: &str) -> Observation {
        Observation {
            endpoint: "test".into(),
            status: Some(200),
            elapsed_ms: 0,
            headers: BTreeMap::new(),
            error: None,
            body_file: None,
            body: vec![],
            parsed: Some(Json::parse(text.as_bytes()).unwrap()),
        }
    }

    #[test]
    fn batches_reject_missing_extra_duplicate_wrong_and_string_ids() {
        let ordered = unpack(&packet(r#"[{"jsonrpc":"2.0","id":2,"result":false},{"jsonrpc":"2.0","id":1,"result":null}]"#), &[0,1], true).unwrap();
        assert_eq!(ordered[0].field("id"), Some(&Json::Number("1".into())));
        for text in [
            r#"[{"jsonrpc":"2.0","id":1,"result":null}]"#,
            r#"[{"jsonrpc":"2.0","id":1,"result":null},{"jsonrpc":"2.0","id":1,"result":null}]"#,
            r#"[{"jsonrpc":"2.0","id":1,"result":null},{"jsonrpc":"2.0","id":3,"result":null}]"#,
            r#"[{"jsonrpc":"2.0","id":1,"result":null},{"jsonrpc":"2.0","id":"2","result":null}]"#,
            r#"{"jsonrpc":"2.0","id":1,"result":null}"#,
        ] {
            assert!(unpack(&packet(text), &[0, 1], true).is_err(), "{text}");
        }
    }

    async fn one_response(
        response: &'static [u8],
        delay: Duration,
        max_bytes: usize,
        timeout: Duration,
    ) -> Observation {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url: reqwest::Url = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 4096];
            let _ = socket.read(&mut buffer).await;
            tokio::time::sleep(delay).await;
            let _ = socket.write_all(response).await;
        });
        let client = reqwest::Client::builder()
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .build()
            .unwrap();
        let mut endpoint = Endpoint {
            name: "node".into(),
            url,
            client,
            next_start: tokio::time::Instant::now(),
        };
        let settings = Settings {
            interval: Duration::ZERO,
            timeout,
            max_response_bytes: max_bytes,
            max_trace_transactions: 32,
            batch_size: 4,
        };
        let observation = fetch(&mut endpoint, b"{}".to_vec(), &settings).await;
        server.abort();
        observation
    }

    #[tokio::test]
    async fn captures_status_size_truncation_incomplete_bodies_and_deadlines_without_retries() {
        let too_large = one_response(
            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n0123456789",
            Duration::ZERO,
            5,
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(too_large.body, b"01234");
        assert!(too_large.error.unwrap().contains("byte limit"));
        let partial = one_response(
            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n0123",
            Duration::ZERO,
            20,
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(partial.body, b"0123");
        assert!(partial.error.unwrap().contains("partial"));
        let status = one_response(
            b"HTTP/1.1 429 Rate Limited\r\nContent-Length: 2\r\n\r\n{}",
            Duration::ZERO,
            20,
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(status.status, Some(429));
        assert!(status.error.unwrap().contains("HTTP status"));
        let timeout =
            one_response(b"", Duration::from_secs(1), 20, Duration::from_millis(30)).await;
        assert!(timeout.error.unwrap().contains("timed out"));
    }
}
