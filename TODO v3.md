# TODO

- [x] migrate from old web3 stuff to alloy
- [x] replace serde_json with sonic_rs
- [ ] is "active_requests" actually included in the proxy choice? or just ewma latency?
- [x] upgrade+update all deps once we have moved to alloy. i think that will free up a lot
- [ ] just commands
    - wrk
    - the app
    - tests
    - linting
    - formatting
- [ ] do we actually want parkinglot?
- [ ] make sure we are using fast modes for hashbrown (hash algos have changed over the years)
- [ ] inspect streamed JSON-RPC envelopes before forwarding bytes. stream successful results and route large JSON-RPC errors through retry and failover
- [ ] make response streaming transport-neutral so HTTP, IPC, and WebSocket backends can stream large responses without buffering complete messages
