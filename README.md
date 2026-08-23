# web3_proxy

Web3_proxy is a fast load-balancing proxy for web3 (Ethereum or similar) JSON-RPC servers.

**Under construction!** Please note that the code is currently under active development. If you wish to run the proxy yourself, please send me a public cast on [Farcaster](https://farcaster.xyz/flashprofits.eth) (not a DM, I barely check those).

Signed transactions `(eth_sendRawTransaction)` are sent in parallel to the configured private RPCs (Flashbots, etc.).

All other requests are sent to an RPC server that is currently on the latest block (Alchemy, your own node, or one of many other providers). If multiple servers are in sync, we prioritize servers based on their `active_requests` and request latency. Please keep in mind that this means that the fastest server is most likely to serve requests, while slower servers are unlikely to ever receive any requests.

Each server has different limits that can be configured. The `soft_limit` is the number of parallel active requests where a server starts to slow down, while the `hard_limit` is where a server starts giving rate limits or other errors.

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

The proxy requests a TCP accept queue backlog of 4096 for sockets that it creates. The operating system can silently cap this request. Linux uses `net.core.somaxconn` as the cap. See the [Linux `listen(2)` documentation](https://man7.org/linux/man-pages/man2/listen.2.html).

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

Connect errors that occur only during the initial burst usually mean that the burst filled the TCP accept queue. Check the proxy's requested backlog and the operating-system cap before you investigate request handling. A successful run has zero connect, read, write, and timeout errors in both the one-second and 30-second tests.

On the current macOS development host, `sysctl kern.ipc.somaxconn` reports 128. The same local `wrk -c400` test can have initial connect errors until an administrator increases this limit. Inspect it with:

    sysctl kern.ipc.somaxconn

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
