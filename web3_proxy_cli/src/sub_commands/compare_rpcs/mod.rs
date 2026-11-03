mod corpus;
mod runner;
mod strict;
#[cfg(test)]
mod tests;

use serde::Deserialize;
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
use web3_proxy::prelude::{
    anyhow::{self, ensure, Context},
    argh::{self, FromArgs},
    reqwest, tokio, toml,
};

#[derive(FromArgs, PartialEq, Debug)]
/// Compare live HTTP RPC replies at fixed chain state. Never submits transactions.
#[argh(subcommand, name = "compare-rpcs")]
pub struct CompareRpcsSubCommand {
    /// private TOML file with [proxy] and [[backends]] endpoint environment references
    #[argh(option)]
    endpoints: PathBuf,
    /// new evidence directory; existing directories are never overwritten
    #[argh(option)]
    output: PathBuf,
    /// additional JSON request cases; see docs/rpc-comparison.md
    #[argh(option)]
    requests: Option<PathBuf>,
    /// explicit hexadecimal block number; otherwise use a common finalized block
    #[argh(option)]
    block: Option<String>,
    /// milliseconds between starts on each endpoint (default 1000)
    #[argh(option, default = "1000")]
    interval_ms: u64,
    /// complete HTTP request timeout in seconds (default 30)
    #[argh(option, default = "30")]
    timeout_seconds: u64,
    /// maximum complete response size in bytes (default 33554432)
    #[argh(option, default = "33554432")]
    max_response_bytes: usize,
    /// requests per batch (default 4)
    #[argh(option, default = "4")]
    batch_size: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    proxy: EndpointConfig,
    backends: Vec<EndpointConfig>,
    /// Operator-supplied evidence reference, not a measured version.
    proxy_version: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EndpointConfig {
    name: String,
    url_env: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
}

struct Endpoint {
    name: String,
    url: reqwest::Url,
    client: reqwest::Client,
    next_start: tokio::time::Instant,
}

struct Settings {
    interval: Duration,
    timeout: Duration,
    max_response_bytes: usize,
    batch_size: usize,
}

impl CompareRpcsSubCommand {
    pub async fn main(self) -> u8 {
        match self.execute().await {
            Ok(code) => code,
            Err(error) => {
                // All external parser/transport errors are replaced at their
                // boundary: neither a URL nor an authorization value may escape.
                eprintln!("RPC comparison could not complete: {error:#}");
                2
            }
        }
    }

    async fn execute(self) -> anyhow::Result<u8> {
        ensure!(self.timeout_seconds > 0, "timeout must be positive");
        ensure!(
            self.max_response_bytes > 0,
            "response size limit must be positive"
        );
        ensure!(
            (1..=32).contains(&self.batch_size),
            "batch size must be between 1 and 32"
        );
        if let Some(block) = &self.block {
            corpus::quantity(block)?;
        }
        let contents =
            std::fs::read_to_string(&self.endpoints).context("cannot read endpoint file")?;
        let config: Config =
            toml::from_str(&contents).map_err(|_| anyhow::anyhow!("invalid endpoint TOML"))?;
        ensure!(
            !config.backends.is_empty(),
            "at least one direct backend is required"
        );
        let settings = Settings {
            interval: Duration::from_millis(self.interval_ms),
            timeout: Duration::from_secs(self.timeout_seconds),
            max_response_bytes: self.max_response_bytes,
            batch_size: self.batch_size,
        };
        let mut secrets = Vec::new();
        let mut endpoints = Vec::new();
        for endpoint in std::iter::once(&config.proxy).chain(&config.backends) {
            ensure!(
                !endpoint.name.is_empty()
                    && endpoint.name.len() <= 64
                    && endpoint
                        .name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
                "endpoint names must use 1-64 ASCII letters, digits, hyphens, or underscores"
            );
            ensure!(
                !endpoints.iter().any(|e: &Endpoint| e.name == endpoint.name),
                "duplicate endpoint name"
            );
            let url_text = std::env::var(&endpoint.url_env).map_err(|_| {
                anyhow::anyhow!("missing URL environment variable for {}", endpoint.name)
            })?;
            let url = reqwest::Url::parse(&url_text)
                .map_err(|_| anyhow::anyhow!("invalid URL for {}", endpoint.name))?;
            ensure!(
                matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
                "endpoint {} must use HTTP or HTTPS",
                endpoint.name
            );
            ensure!(
                !endpoints.iter().any(|e| e.url == url),
                "endpoint URLs must be distinct"
            );
            secrets.push(url_text);
            if let Some(password) = url.password() {
                secrets.push(password.to_string());
            }
            secrets.extend(
                url.query_pairs()
                    .map(|(_, v)| v.into_owned())
                    .filter(|v| !v.is_empty()),
            );
            if let Some(segments) = url.path_segments() {
                secrets.extend(segments.filter(|v| v.len() >= 16).map(str::to_string));
            }
            let mut headers = reqwest::header::HeaderMap::new();
            for (name, variable) in &endpoint.headers {
                let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                    .map_err(|_| anyhow::anyhow!("invalid header name for {}", endpoint.name))?;
                ensure!(
                    name != reqwest::header::HOST
                        && name != reqwest::header::CONTENT_LENGTH
                        && name != reqwest::header::CONTENT_TYPE,
                    "endpoint headers cannot override Host, Content-Length, or Content-Type"
                );
                let value = std::env::var(variable).map_err(|_| {
                    anyhow::anyhow!("missing header environment variable for {}", endpoint.name)
                })?;
                ensure!(
                    !value.is_empty(),
                    "empty credential header for {}",
                    endpoint.name
                );
                secrets.push(value.clone());
                if let Some(token) = value
                    .strip_prefix("Bearer ")
                    .or_else(|| value.strip_prefix("Basic "))
                {
                    secrets.push(token.to_string());
                }
                let mut value = reqwest::header::HeaderValue::from_str(&value)
                    .map_err(|_| anyhow::anyhow!("invalid header value for {}", endpoint.name))?;
                value.set_sensitive(true);
                headers.insert(name, value);
            }
            let client = reqwest::Client::builder()
                .default_headers(headers)
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .timeout(settings.timeout)
                .build()
                .map_err(|_| anyhow::anyhow!("cannot build HTTP client"))?;
            endpoints.push(Endpoint {
                name: endpoint.name.clone(),
                url,
                client,
                next_start: tokio::time::Instant::now(),
            });
        }
        let extra = corpus::read_cases(self.requests.as_deref())?;
        let mut runner = runner::Runner::new(endpoints, settings, secrets, &self.output)?;
        let result = runner.run(self.block, extra, config.proxy_version).await;
        match result {
            Ok(report) => {
                let code = report.exit_code();
                runner.save_report(&report)?;
                println!("RPC comparison: {} agreement, {} backend differences, {} proxy-only differences, {} repeat differences, {} incomplete, {} observations. Exit {code}.", report.agreement, report.backend_differences, report.proxy_differences, report.repeat_differences, report.incomplete, report.observations);
                println!("Evidence: {}", self.output.display());
                Ok(code)
            }
            Err(error) => {
                runner.save_failure(&format!("{error:#}"))?;
                Err(error)
            }
        }
    }
}
