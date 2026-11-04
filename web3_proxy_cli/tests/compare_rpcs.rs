//! Executable boundary: this command must not load a server config or leak URLs.
use serde_json::{json, Value};
use std::{
    io::{Read, Write},
    net::TcpListener,
    process::Command,
    thread,
};

#[test]
fn comparison_help_does_not_require_a_server_config() {
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_web3_proxy_cli"))
        .current_dir(directory.path())
        .args(["compare-rpcs", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8(output.stdout)
        .unwrap()
        .contains("--endpoints"));
}

#[test]
fn setup_failure_uses_exit_two_without_loading_the_default_server_config() {
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_web3_proxy_cli"))
        .current_dir(directory.path())
        .args([
            "compare-rpcs",
            "--endpoints",
            "missing.toml",
            "--output",
            "capture",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains("cannot read endpoint file"));
    assert!(!error.contains("development.toml"));
}

fn failed_preflight(
    endpoint_options: &str,
    query: &str,
    environment: &[(&str, &str)],
    response_body: Option<String>,
) -> (std::process::Output, tempfile::TempDir, usize) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let endpoint_file = directory.path().join("endpoints.toml");
    std::fs::write(&endpoint_file, format!("[proxy]\nname = 'proxy'\nurl_env = 'TEST_COMPARE_PROXY'\n{endpoint_options}\n[[backends]]\nname = 'direct'\nurl_env = 'TEST_COMPARE_DIRECT'\n{endpoint_options}\n")).unwrap();
    let (stop, stopped) = std::sync::mpsc::channel();
    let server = thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut count = 0;
        // Fail the genesis preflight on both endpoints with a non-block result.
        while count < 4 && std::time::Instant::now() < deadline && stopped.try_recv().is_err() {
            let (mut stream, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(std::time::Duration::from_millis(5));
                    continue;
                }
                Err(error) => panic!("{error}"),
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(1)))
                .unwrap();
            let mut bytes = Vec::new();
            let header_end = loop {
                let mut buffer = [0; 4096];
                let n = stream.read(&mut buffer).unwrap();
                assert_ne!(n, 0);
                bytes.extend_from_slice(&buffer[..n]);
                if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    break i + 4;
                }
            };
            let headers = String::from_utf8_lossy(&bytes[..header_end]);
            let length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|v| v.trim().parse::<usize>().ok())
                })
                .unwrap();
            while bytes.len() < header_end + length {
                let mut buffer = [0; 4096];
                let n = stream.read(&mut buffer).unwrap();
                assert_ne!(n, 0);
                bytes.extend_from_slice(&buffer[..n]);
            }
            let request: Value =
                serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
            let body = response_body.clone().unwrap_or_else(|| {
                json!({"jsonrpc":"2.0","id":request["id"],"result":"0x1"}).to_string()
            });
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
            count += 1;
        }
        count
    });
    let output = Command::new(env!("CARGO_BIN_EXE_web3_proxy_cli"))
        .current_dir(directory.path())
        .args([
            "compare-rpcs",
            "--endpoints",
            "endpoints.toml",
            "--output",
            "capture",
            "--interval-ms",
            "0",
        ])
        .env(
            "TEST_COMPARE_PROXY",
            format!("http://{address}/proxy?key=fixture-proxy-secret{query}"),
        )
        .env(
            "TEST_COMPARE_DIRECT",
            format!("http://{address}/direct?key=fixture-direct-secret{query}"),
        )
        .envs(environment.iter().copied())
        .output()
        .unwrap();
    let _ = stop.send(());
    let requests = server.join().unwrap();
    (output, directory, requests)
}

#[test]
fn executable_reads_private_environment_and_saves_failed_preflight_without_credentials() {
    let (output, directory, requests) = failed_preflight("", "", &[], None);
    assert_eq!(requests, 4);
    assert_eq!(output.status.code(), Some(2));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("fixture-"));
    let capture = directory.path().join("capture");
    let report: Value =
        serde_json::from_slice(&std::fs::read(capture.join("report.json")).unwrap()).unwrap();
    assert_eq!(report["exit_code"], 2);
    assert!(report["failure"]
        .as_str()
        .unwrap()
        .contains("genesis hash missing"));
    for entry in std::fs::read_dir(capture).unwrap() {
        let body = std::fs::read(entry.unwrap().path()).unwrap();
        assert!(!String::from_utf8_lossy(&body).contains("fixture-"));
    }
}

#[test]
fn ordinary_query_and_header_values_do_not_withhold_evidence() {
    for (options, query, environment) in [
        ("", "&network=1", vec![]),
        (
            "headers = { Accept = 'TEST_COMPARE_ACCEPT' }",
            "",
            vec![("TEST_COMPARE_ACCEPT", "application/json")],
        ),
    ] {
        let (output, directory, requests) = failed_preflight(options, query, &environment, None);
        assert_eq!(requests, 4);
        assert_eq!(output.status.code(), Some(2));
        let report: Value = serde_json::from_slice(
            &std::fs::read(directory.path().join("capture/report.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(report["failure"], "genesis hash missing");
        assert!(directory.path().join("capture/000001-proxy.body").exists());
        assert!(directory.path().join("capture/000001-direct.body").exists());
    }
}

#[test]
fn explicit_secrets_protect_custom_headers_and_url_components() {
    let secret = "fixture-explicit-api-key";
    let body = json!({"jsonrpc":"2.0","id":0,"result":secret}).to_string();
    for options in [
        "secret_env = ['TEST_COMPARE_KEY']",
        "secret_env = ['TEST_COMPARE_KEY']\nheaders = { 'X-API-Key' = 'TEST_COMPARE_KEY' }",
    ] {
        let (output, directory, requests) = failed_preflight(
            options,
            &format!("&api_key={secret}"),
            &[("TEST_COMPARE_KEY", secret)],
            Some(body.clone()),
        );
        assert_eq!(requests, 2);
        assert_eq!(output.status.code(), Some(2));
        assert!(!directory.path().join("capture/000001-proxy.body").exists());
        assert!(!directory.path().join("capture/000001-direct.body").exists());
        let journal =
            std::fs::read_to_string(directory.path().join("capture/exchanges.jsonl")).unwrap();
        assert!(journal.contains("withheld"));
        assert!(!journal.contains(secret));
    }
}

#[test]
fn empty_explicit_secret_stops_setup() {
    let (output, directory, requests) = failed_preflight(
        "secret_env = ['TEST_COMPARE_EMPTY_SECRET']",
        "",
        &[("TEST_COMPARE_EMPTY_SECRET", "")],
        None,
    );
    assert_eq!(requests, 0);
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("empty secret for proxy"));
    assert!(!directory.path().join("capture").exists());
}

#[test]
fn authorization_credentials_are_automatically_protected() {
    for scheme in ["Bearer", "bearer", "Basic"] {
        let token = "fixture-authorization-token";
        let header = format!("{scheme} {token}");
        let body = json!({"jsonrpc":"2.0","id":0,"result":token}).to_string();
        let (output, directory, requests) = failed_preflight(
            "headers = { Authorization = 'TEST_COMPARE_AUTHORIZATION' }",
            "",
            &[("TEST_COMPARE_AUTHORIZATION", &header)],
            Some(body),
        );
        assert_eq!(requests, 2);
        assert_eq!(output.status.code(), Some(2));
        assert!(!directory.path().join("capture/000001-proxy.body").exists());
        assert!(!directory.path().join("capture/000001-direct.body").exists());
    }
}

#[test]
fn escaped_credentials_in_duplicate_fields_are_never_saved() {
    let secret = "fixture-hidden-credential";
    let encoded: String = secret
        .chars()
        .map(|c| format!("\\u{:04x}", c as u32))
        .collect();
    for body in [
        format!(r#"{{"jsonrpc":"2.0","id":0,"id":0,"result":"{encoded}"}}"#),
        format!(r#"{{"jsonrpc":"2.0","id":0,"result":"{encoded}","result":"public"}}"#),
        format!(
            r#"{{"jsonrpc":"2.0","id":0,"result":"public","result":{{"nested":["{encoded}"]}}}}"#
        ),
    ] {
        let (output, directory, requests) = failed_preflight(
            "headers = { Authorization = 'TEST_COMPARE_AUTHORIZATION' }",
            "",
            &[("TEST_COMPARE_AUTHORIZATION", &format!("Bearer {secret}"))],
            Some(body),
        );
        assert_eq!(requests, 2);
        assert_eq!(output.status.code(), Some(2));
        assert!(!directory.path().join("capture/000001-proxy.body").exists());
        assert!(!directory.path().join("capture/000001-direct.body").exists());
        for entry in std::fs::read_dir(directory.path().join("capture")).unwrap() {
            let bytes = std::fs::read(entry.unwrap().path()).unwrap();
            let text = String::from_utf8_lossy(&bytes);
            assert!(!text.contains(secret));
            assert!(!text.contains(&encoded));
        }
        assert!(!String::from_utf8_lossy(&output.stderr).contains(secret));
    }
}

#[test]
fn incomplete_json_and_non_json_responses_are_withheld() {
    for body in [
        "<html>upstream unavailable</html>",
        r#"{"jsonrpc":"2.0","id":0,"result":"\u0066ixture-credential"#,
    ] {
        let (output, directory, requests) = failed_preflight(
            "headers = { Authorization = 'TEST_COMPARE_AUTHORIZATION' }",
            "",
            &[("TEST_COMPARE_AUTHORIZATION", "Bearer fixture-credential")],
            Some(body.into()),
        );
        assert_eq!(requests, 2);
        assert_eq!(output.status.code(), Some(2));
        assert!(!directory.path().join("capture/000001-proxy.body").exists());
        assert!(!directory.path().join("capture/000001-direct.body").exists());
        let journal =
            std::fs::read_to_string(directory.path().join("capture/exchanges.jsonl")).unwrap();
        assert!(journal.contains("credential scan could not decode JSON"));
        assert!(!journal.contains("fixture-credential"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("fixture-credential"));
    }
}

#[test]
fn duplicate_public_fields_keep_raw_evidence_without_becoming_valid_replies() {
    let body = r#"{"jsonrpc":"2.0","id":0,"id":0,"result":"0x1"}"#;
    let (output, directory, requests) = failed_preflight(
        "headers = { Authorization = 'TEST_COMPARE_AUTHORIZATION' }",
        "",
        &[("TEST_COMPARE_AUTHORIZATION", "Bearer fixture-credential")],
        Some(body.into()),
    );
    assert_eq!(requests, 2);
    assert_eq!(output.status.code(), Some(2));
    for endpoint in ["proxy", "direct"] {
        let bytes = std::fs::read(
            directory
                .path()
                .join(format!("capture/000001-{endpoint}.body")),
        )
        .unwrap();
        assert_eq!(bytes, body.as_bytes());
    }
    let journal =
        std::fs::read_to_string(directory.path().join("capture/exchanges.jsonl")).unwrap();
    assert!(journal.contains("invalid or duplicate JSON object key"));
    assert!(!journal.contains("withheld"));
}
