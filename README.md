# web3_proxy

Web3_proxy is a fast load-balancing proxy for web3 (Ethereum or similar) JSON-RPC servers.

**Under construction!** Please note that the code is currently under active development. If you wish to run the proxy yourself, please send me a public cast on [Farcaster](https://farcaster.xyz/flashprofits.eth) (not a DM, I barely check those).

Signed transactions `(eth_sendRawTransaction)` are sent in parallel to the configured private RPCs (Flashbots, etc.).

The normal `/` route uses Best mode. It selects eligible RPC servers with the existing latency tiers and load-aware ranking. Eligible primary nodes precede backups. Best retries malformed responses, wrong response IDs, and `-32603 Internal error` on another eligible node within the original deadline. Its other RPC error rules remain in place.

HTTP backend responses are read completely into memory before the proxy accepts or returns them. This includes responses larger than 128 KiB. The proxy validates the JSON-RPC envelope and decoded response ID before recording a successful latency sample. The backend permit stays held through the body read and validation. Large replies therefore use memory proportional to their complete size; an incomplete or stalled body cannot become a client response.

Use `/fastest` for HTTP or WebSocket tests that should race multiple fast nodes. Set `app.fastest_rpcs` in the TOML config (default `2`). Fastest uses the same latency tiers, then load-weighted latency within each tier. It starts requests on the best available synced nodes. With one synced node, it sends one request. With three synced nodes and a count of two, it selects the top two. Set the count to `1` for one node, or `0` for all synced eligible nodes.

Fastest returns the first complete valid result, including `null` or an execution revert. A transport failure, malformed response, or other RPC error does not beat a valid response. Fastest fills failed attempt slots from the remaining eligible nodes, within the original deadline. The count limits concurrent attempts, not the total number of retries. It cancels losing attempts and releases their request slots. Requests that already entered transport still count as backend attempts.

Config reloads apply the new count to new HTTP requests and new messages on existing WebSocket connections. Active requests keep their original count. Fastest preserves local and cached responses, transaction broadcast rules, subscriptions, client IDs, and batch response order.

Use `http://127.0.0.1:8544/fastest` or `ws://127.0.0.1:8544/fastest` as the default RPC URL in selected tests. Expect more RPC traffic and possible provider charges. The race can reduce latency, but it cannot guarantee the fastest answer across all nodes and network conditions.

Use `/versus` over HTTP or WebSocket to compare the eligible consensus nodes. Each comparison captures its node set and uses the same validated request and pinned block on every selected node. It waits for normal backend permits and cooldowns, then rechecks eligibility before submission. A Versus attempt is one pipeline invocation of a selected node's transport. Alloy can retry that call on the same backend, including resending it after a WebSocket reconnect. The Versus pipeline does not invoke a selected node again, replace failed nodes, add new nodes, or restart the comparison. This restriction applies to Versus; Best failover and Fastest retries retain their contracts above.

Versus returns the first complete success, including `null`, or execution revert. Other RPC errors, transport failures, and invalid responses cannot win. After handing the answer to the client, the same tracked task continues the remaining selected work, including queued nodes. Client disconnection does not cancel it. The original request deadline still applies. Shutdown allows at most 20 seconds to drain request work. Each attempt holds its permit until it finishes or expires.

Later successful completions update the existing median and peak backend latency metrics. They do not change the client answer or its recorded response time. RPC errors, including accepted reverts, do not produce successful latency samples. Local replies and cache hits start no comparison and add no backend timing samples. Transaction submission rules, subscriptions, client IDs, and batch order stay in place. A completed comparison cannot restart through application retries, gas-estimate error handling, or transaction/receipt archive fallback. If every node fails, Versus returns the first completed failure; if the deadline expires, it reports timeout.

Backend WebSocket calls keep Alloy's existing transport, response parsing, and reconnect behavior. Alloy handles backend response IDs, and the proxy returns the original client ID. HTTP backends, including requests from WebSocket clients, use the complete-response validation path described above.

Each server has different limits that can be configured. The `soft_limit` is the number of parallel active requests where a server starts to slow down, while the `hard_limit` is where a server starts giving rate limits or other errors.

An optional [block relay](docs/block-relay.md) monitors local and external Beacon
APIs outside the balanced RPC pool. It can run alone against existing Geth, Reth,
and other Engine V4 clients. It starts in observe mode; speed gains need a measured
comparison before enabling injection.

## Quick development

1. Copy `.env.example` to `.env` and set local secrets.
2. Copy `./config/example.toml` to `./config/development.toml` and change settings to match your setup. Config values can use `${VARIABLE_NAME}` references from `.env`.
3. Run `cargo` commands:

```
$ cargo run --release -- --help
```
```
   Compiling web3_proxy v0.1.0 (/home/bryan/src/web3_proxy/web3_proxy)
    Finished release [optimized + debuginfo] target(s) in 17.69s
     Running `target/release/web3_proxy --help`
Usage: web3_proxy [--port <port>] [--workers <workers>] [--config <config>]

web3_proxy is a fast load-balancing proxy for web3 (Ethereum or similar) JSON-RPC servers.

Options:
  --port            what port the proxy should listen on
  --workers         number of worker threads
  --config          path to a toml of rpc servers
  --help            display usage information
```

Start the server with the defaults. It listens on `http://localhost:8544` and uses `./config/development.toml`:

```
cargo run --release -- proxyd
```

Quickly run tests:

```
RUST_BACKTRACE=1 RUST_LOG=web3_proxy=trace,info cargo nextest run
```

## Common commands

Check that the proxy is working:

```
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"web3_clientVersion","id":1}' 127.0.0.1:8544
```
```
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}' 127.0.0.1:8544
```
```
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"eth_getBlockByNumber", "params": ["latest", false],"id":1}' 127.0.0.1:8544
```
```
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"eth_getBalance", "params": ["0x0000000000000000000000000000000000000000", "latest"],"id":1}' 127.0.0.1:8544
```

Check that the websocket is working:

```
$ websocat ws://127.0.0.1:8544

{"jsonrpc":"2.0","method":"web3_clientVersion","id":1}

{"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}

{"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newPendingTransactions"]}
```

You can copy `config/example.toml` to `config/production-$CHAINNAME.toml` and then run `docker compose -f docker-compose.prod.yml up --build -d` to start proxies for many chains.

### Production TCP backlog

The proxy requests the largest positive TCP accept queue backlog that Tokio can pass to `listen(2)`. The operating system selects the effective size and can silently cap the request. Linux uses `net.core.somaxconn` as the cap. See the [Linux `listen(2)` documentation](https://man7.org/linux/man-pages/man2/listen.2.html).

The common Compose service sets `net.core.somaxconn` to 4096, so all services in `docker-compose.prod.yml` inherit that value. Docker applies this network setting inside each container network namespace. It does not change the host value. See the [Docker Compose `sysctls` documentation](https://docs.docker.com/reference/compose-file/services/#sysctls).

Inspect the container and Linux host values:

    docker compose -f docker-compose.prod.yml exec eth sysctl net.core.somaxconn
    sysctl net.core.somaxconn

The startup log shows the backlog that the proxy requests. If `listenfd` supplies a socket, the socket owner controls its backlog instead.

Compare 3 RPCs:

```
web3_proxy_cli health_compass https://eth.llamarpc.com https://eth-ski.llamarpc.com https://rpc.ankr.com/eth
```

### Health compass

Health check 3 servers and error if the first one doesn't match the others.

```
web3_proxy_cli health_compass https://eth.llamarpc.com/ https://rpc.ankr.com/eth https://cloudflare-eth.com
```

## Flame Graphs

Flame graphs make a developer's join of finding slow code painless:

    $ cat /proc/sys/kernel/kptr_restrict
    1
    $ echo 0 | sudo tee /proc/sys/kernel/kptr_restrict
    0
    $ cat /proc/sys/kernel/perf_event_paranoid
    4
    $ echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
    -1
    $ CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph --bin web3_proxy_cli --no-inline -- proxyd

Be sure to use `--no-inline` or perf will be VERY slow

## GDB

Developers can run the proxy under gdb for advanced debugging:

    cargo build --release && RUST_LOG=info,web3_proxy=debug,alloy_transport=error rust-gdb --args target/debug/web3_proxy --listen-port 7503 --rpc-config-path ./config/production-eth.toml

TODO: also enable debug symbols in the release build by modifying the root Cargo.toml

## Load Testing

Test the proxy:

    wrk -t12 -c400 -d1s --latency http://127.0.0.1:8544/health
    wrk -t12 -c400 -d30s --latency http://127.0.0.1:8544/health
    wrk -t12 -c400 -d30s --latency http://127.0.0.1:8544/status
    wrk -s ./wrk/getBlockNumber.lua -t12 -c400 -d30s --latency http://127.0.0.1:8544/
    wrk -s ./wrk/getLatestBlockByNumber.lua -t12 -c400 -d30s --latency http://127.0.0.1:8544/

Before a high-concurrency test, make sure that the load generator can open enough file descriptors:

    ulimit -n 4096

If `wrk` has a soft limit of 256, `-c400` can report about 155 connect errors because `wrk` also needs descriptors for its threads and control files. An error count that stays constant when test duration increases is consistent with this client limit.

Connect errors can also mean that the initial burst filled the server's TCP accept queue. Inspect the operating-system cap and the active queue before you investigate request handling. On macOS, use:

    sysctl kern.ipc.somaxconn
    netstat -Lan -p tcp

A successful run has zero connect, read, write, and timeout errors in both the one-second and 30-second tests.

Test geth (assuming it is on 8545):

    wrk -s ./wrk/getBlockNumber.lua -t12 -c400 -d30s --latency http://127.0.0.1:8545
    wrk -s ./wrk/getLatestBlockByNumber.lua -t12 -c400 -d30s --latency http://127.0.0.1:8545

Test erigon (assuming it is on 8945):

    wrk -s ./wrk/getBlockNumber.lua -t12 -c400 -d30s --latency http://127.0.0.1:8945
    wrk -s ./wrk/getLatestBlockByNumber.lua -t12 -c400 -d30s --latency http://127.0.0.1:8945

Note: Testing with `getLatestBlockByNumber.lua` is not great because the latest block changes and so one run is likely to be very different than another.

Run [ethspam](https://github.com/shazow/ethspam) and [versus](https://github.com/INFURA/versus) for a more realistic load test. This command keeps up to 200 requests in flight and sends 20,000 `eth_call` requests through the proxy. These requests reach a backend:

    ethspam --rpc=http://127.0.0.1:8544/ --method=eth_call:1 | versus --concurrency=200 --stop-after=20000 http://127.0.0.1:8544/

Give `--stop-after` a total request count. The duration form, such as `--stop-after=10s`, has a timer bug in the current `versus` release. It exits with `context deadline exceeded` and does not print the test report.

The `ethspam` `--rpc` endpoint supplies current chain data for generated requests. The final `versus` URL is the load-test target. To keep the initial `eth_getBlockByNumber` request out of the proxy results, give `ethspam` a direct RPC endpoint for the same chain. The `--method` option replaces the default method map, so specify each method that you want with a positive weight. Do not set `--ratelimit` for a throughput test because it can prevent `versus` from keeping all workers busy.
