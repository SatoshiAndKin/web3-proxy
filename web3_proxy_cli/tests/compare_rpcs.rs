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

#[test]
fn executable_reads_private_environment_and_saves_failed_preflight_without_credentials() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let endpoint_file = directory.path().join("endpoints.toml");
    std::fs::write(&endpoint_file, "[proxy]\nname = 'proxy'\nurl_env = 'TEST_COMPARE_PROXY'\n[[backends]]\nname = 'direct'\nurl_env = 'TEST_COMPARE_DIRECT'\n").unwrap();
    let server = thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut count = 0;
        // Fail the genesis preflight on both endpoints with a non-block result.
        while count < 4 && std::time::Instant::now() < deadline {
            let (mut stream, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(std::time::Duration::from_millis(5));
                    continue;
                }
                Err(error) => panic!("{error}"),
            };
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
            let body = json!({"jsonrpc":"2.0","id":request["id"],"result":"0x1"}).to_string();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
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
            format!("http://{address}/proxy?key=fixture-proxy-secret"),
        )
        .env(
            "TEST_COMPARE_DIRECT",
            format!("http://{address}/direct?key=fixture-direct-secret"),
        )
        .output()
        .unwrap();
    assert_eq!(server.join().unwrap(), 4);
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
