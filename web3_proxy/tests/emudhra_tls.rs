//! Live certificate checks from https://emudhra.we-have-a-problem.com/.
//! Run explicitly with `cargo nextest run --test emudhra_tls --run-ignored only`.

#[tokio::test]
#[ignore = "requires the public advisory endpoints"]
async fn rejects_compromised_emudhra_intermediates() {
    let client = web3_proxy::block_relay::transport::client().unwrap();
    let control = client
        .get("https://emudhra.we-have-a-problem.com/")
        .send()
        .await
        .expect("the valid control endpoint must connect");
    assert!(control.status().is_success());
    for host in [
        "dv-g3b-emudhra.we-have-a-problem.com",
        "dv-g3b-aaa-emudhra.we-have-a-problem.com",
    ] {
        let error = client
            .get(format!("https://{host}/"))
            .send()
            .await
            .expect_err("compromised intermediates must be rejected");
        assert!(
            !error.is_timeout(),
            "timeout is not TLS rejection: {error:?}"
        );
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        let mut certificate_error = false;
        while let Some(current) = cause {
            certificate_error |= current
                .to_string()
                .to_ascii_lowercase()
                .contains("certificate");
            cause = current.source();
        }
        assert!(
            certificate_error,
            "expected certificate validation failure: {error:?}"
        );
    }
}
