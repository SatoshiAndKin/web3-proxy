use super::*;

#[tokio::test]
async fn structured_spec_values_do_not_block_network_validation() {
    let network = network();
    let beacon = MockBeacon::new(network.clone());
    let server = Server::beacon(beacon.clone()).await;
    let http = transport::BeaconHttp::new(&server.url, &BTreeMap::new()).unwrap();

    assert_eq!(
        source::validate_network(&http, &network)
            .await
            .map_err(|e| e.to_string()),
        Ok(())
    );

    let mut wrong_genesis = network.clone();
    wrong_genesis.genesis_validators_root = B256::ZERO;
    assert_eq!(
        source::validate_network(&http, &wrong_genesis)
            .await
            .unwrap_err()
            .to_string(),
        "Beacon genesis mismatch"
    );

    beacon.state.lock().network.forks.push(config::Fork {
        name: "future".into(),
        version: "0x07000000".parse().unwrap(),
        epoch: 100,
    });
    assert_eq!(
        source::validate_network(&http, &network)
            .await
            .unwrap_err()
            .to_string(),
        "unconfigured Beacon fork; update relay schedule"
    );
}

#[tokio::test]
async fn required_spec_fields_still_reject_wrong_types_or_missing_values() {
    let network = network();
    let beacon = MockBeacon::new(network.clone());
    let server = Server::beacon(beacon.clone()).await;
    let http = transport::BeaconHttp::new(&server.url, &BTreeMap::new()).unwrap();

    for spec in [
        json!({"SECONDS_PER_SLOT": "12"}),
        json!({"PRESET_BASE": "mainnet"}),
        json!({"PRESET_BASE": null, "SECONDS_PER_SLOT": "12"}),
        json!({"PRESET_BASE": ["mainnet"], "SECONDS_PER_SLOT": "12"}),
        json!({"PRESET_BASE": "mainnet", "SECONDS_PER_SLOT": 12}),
        json!({"PRESET_BASE": "mainnet", "SECONDS_PER_SLOT": null}),
        json!({"PRESET_BASE": "mainnet", "SECONDS_PER_SLOT": ["12"]}),
    ] {
        beacon.state.lock().spec = spec.clone();
        assert_eq!(
            source::validate_network(&http, &network)
                .await
                .unwrap_err()
                .to_string(),
            "invalid Beacon response",
            "accepted or misclassified required spec fields: {spec}"
        );
    }
}

#[tokio::test]
async fn required_spec_values_still_enforce_mainnet_preset_and_slot_duration() {
    let network = network();
    let beacon = MockBeacon::new(network.clone());
    let server = Server::beacon(beacon.clone()).await;
    let http = transport::BeaconHttp::new(&server.url, &BTreeMap::new()).unwrap();
    let valid_spec = beacon.state.lock().spec.clone();

    for (field, value, expected) in [
        ("PRESET_BASE", "minimal", "unsupported Beacon preset"),
        ("PRESET_BASE", "", "unsupported Beacon preset"),
        ("SECONDS_PER_SLOT", "0", "slot duration mismatch"),
        ("SECONDS_PER_SLOT", "6", "slot duration mismatch"),
        ("SECONDS_PER_SLOT", "twelve", "slot duration mismatch"),
        ("SECONDS_PER_SLOT", "", "slot duration mismatch"),
        (
            "SECONDS_PER_SLOT",
            "18446744073709551616",
            "slot duration mismatch",
        ),
    ] {
        let mut spec = valid_spec.clone();
        spec[field] = json!(value);
        beacon.state.lock().spec = spec;
        assert_eq!(
            source::validate_network(&http, &network)
                .await
                .unwrap_err()
                .to_string(),
            expected,
            "accepted or misclassified {field}={value}"
        );
    }
}
