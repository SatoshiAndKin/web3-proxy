# Consensus vector

`electra.json` comes from the Ethereum consensus-spec-tests repository at
`bc5c1a7fb2a8871aaffd4b16ee4dd9c72bb81908`, under
`tests/mainnet/electra/ssz_static/BeaconBlock/ssz_random/case_0/value.yaml`.
The fixture retains all fields. YAML integers become quoted decimal JSON strings,
as required by the Beacon API. The independently supplied root is
`0x853653c6733c3275753665c3e1269f9f1fed274a67809dd9dec8245097f1aa7b`.

This randomly generated SSZ vector tests the complete block-root schema. It does
not describe an executable Ethereum block or a valid proposer signature.

Source: https://github.com/ethereum/consensus-spec-tests/tree/bc5c1a7fb2a8871aaffd4b16ee4dd9c72bb81908

The fixture uses the upstream MIT license. See [LICENSE](LICENSE).

`test-jwt.hex` is a public, fixed test value. Never use it for an actual node.
