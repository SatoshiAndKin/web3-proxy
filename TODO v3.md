# TODO

- [x] migrate from old web3 stuff to alloy
- [x] replace serde_json with sonic_rs
- [ ] is "active_requests" actually included in the proxy choice? or just ewma latency?
- [x] fix lint warnings around Web3Proxy error being too big. 
- [x] upgrade+update all deps once we have moved to alloy. i think that will free up a lot
- [ ] just commands
    - wrk
    - check config
    - the app
    - tests
    - linting
    - formatting
- [ ] upgrade everything to edition = "2024"
- [ ] config for setting the default to /fastest /one /quorum /anythingelse
- [ ] do we actually want parkinglot?
- [ ] make sure we are using fast modes for hashbrown (hash algos have changed over the years)
- [ ] inspect streamed JSON-RPC envelopes before forwarding bytes. stream successful results and route large JSON-RPC errors through retry and failover
- [ ] make response streaming transport-neutral so HTTP, IPC, and WebSocket backends can stream large responses without buffering complete messages
