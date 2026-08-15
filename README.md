# web3_proxy

Web3_proxy is a fast load-balancing proxy for web3 (Ethereum or similar) JSON-RPC servers.

**Under construction!** Please note that the code is currently under active development. If you wish to run the proxy yourself, please send us a message on Discord, and we can explain things that aren't documented yet. Most RPC methods are currently supported, though filters will be added soon. Additionally, more tests are always needed.

Signed transactions `(eth_sendRawTransaction)` are sent in parallel to the configured private RPCs (NeoC, Eden, BloxRoute, Flashbots, etc.).

All other requests are sent to an RPC server that is currently on the latest block (LlamaNodes, Alchemy, Moralis, Rivet, your node, or one of many other providers). If multiple servers are in sync, we prioritize servers based on their `active_requests` and request latency. Please keep in mind that this means that the fastest server is most likely to serve requests, while slower servers are unlikely to ever receive any requests.

Each server has different limits that can be configured. The `soft_limit` is the number of parallel active requests where a server starts to slow down, while the `hard_limit` is where a server starts giving rate limits or other errors.

## Quick development

1. `brew install librdkafka` or `sudo apt-get install librdkafka-dev`
2. Run `docker-compose up -d` to start the optional Kafka service. See `docker-compose.yml` for details.
3. Copy `./config/example.toml` to `./config/development.toml` and change settings to match your setup.
4. Run `cargo` commands:

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

Run more tests:

```
RUST_BACKTRACE=1 RUST_LOG=web3_proxy=trace,info cargo nextest run --features tests-needing-docker
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

You can copy `config/example.toml` to `config/production-$CHAINNAME.toml` and then run `docker-compose up --build -d` start proxies for many chains.

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

    cargo build --release && RUST_LOG=info,web3_proxy=debug,ethers_providers::rpc=off rust-gdb --args target/debug/web3_proxy --listen-port 7503 --rpc-config-path ./config/production-eth.toml

TODO: also enable debug symbols in the release build by modifying the root Cargo.toml

## Load Testing

Test the proxy:

    wrk -t12 -c400 -d30s --latency http://127.0.0.1:8544/health
    wrk -t12 -c400 -d30s --latency http://127.0.0.1:8544/status
    wrk -s ./wrk/getBlockNumber.lua -t12 -c400 -d30s --latency http://127.0.0.1:8544/
    wrk -s ./wrk/getLatestBlockByNumber.lua -t12 -c400 -d30s --latency http://127.0.0.1:8544/

Test geth (assuming it is on 8545):

    wrk -s ./wrk/getBlockNumber.lua -t12 -c400 -d30s --latency http://127.0.0.1:8545
    wrk -s ./wrk/getLatestBlockByNumber.lua -t12 -c400 -d30s --latency http://127.0.0.1:8545

Test erigon (assuming it is on 8945):

    wrk -s ./wrk/getBlockNumber.lua -t12 -c400 -d30s --latency http://127.0.0.1:8945
    wrk -s ./wrk/getLatestBlockByNumber.lua -t12 -c400 -d30s --latency http://127.0.0.1:8945

Note: Testing with `getLatestBlockByNumber.lua` is not great because the latest block changes and so one run is likely to be very different than another.

Run [ethspam](https://github.com/INFURA/versus) and [versus](https://github.com/shazow/ethspam) for a more realistic load test:

    ethspam --rpc http://127.0.0.1:8544 | versus --concurrency=10 --stop-after=1000 http://127.0.0.1:8544

    ethspam --rpc http://127.0.0.1:8544/ | versus --concurrency=100 --stop-after=10000 http://127.0.0.1:8544/
