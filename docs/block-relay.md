# Block relay

The objective is to reduce the time until **every execution node returns a new
block through its direct RPC**. This feature is an experiment, not a proven speed
improvement. It can also add execution contention and make a node slower.

Use the existing Geth, Reth, and Lighthouse instances. Do not start more
Ethereum clients for this feature. No node configuration or peer
topology changes form part of this change.

## Cross-client contract

The relay submits complete execution payloads through the private, JWT-protected
`engine_newPayloadV4` method. It does not use a Reth-specific block import method.
The Engine specification permits additional payload producers when only one
consensus client controls fork choice. This relay never calls
`engine_forkchoiceUpdated`, requests payload builds, or publishes unverified
blocks to consensus gossip. Each existing Lighthouse instance keeps sole control
of its execution node's chain head.
[Engine API common definitions](https://github.com/ethereum/execution-apis/blob/main/src/engine/common.md#load-balancing-and-advanced-configurations).

This implementation supports Electra/Fulu blocks with the mainnet SSZ preset.
It checks network genesis, slot duration, configured fork versions and epochs,
both execution chain IDs, and target support for Engine V4. It rejects unknown
block versions. A newly advertised fork requires a configuration review; a new
block schema also requires code and tests. Do not add its name to the config and
assume it works. The relay does not support PoW or arbitrary EVM chains.

## Sources and delivery

```text
Existing local Lighthouse instances + external Beacon APIs
                   │ block_gossip / block events
                   ▼
  Race full-block reads → verify roots → encode one Engine V4 body
                   │
         ┌─────────┼─────────┐
         ▼         ▼         ▼
       Geth      Reth     next client
       Engine    Engine     Engine
         │         │         │
       Direct RPC readiness probes, independent of delivery
```

Sources never enter `balanced_rpcs`, `private_rpcs`, or the proxy's readiness
quorum. Local and external sources use the same path. A local first arrival can
therefore reach the other configured targets. “Peers” here means configured
Engine targets, not arbitrary Ethereum P2P peers.

Each source must provide genesis, fork schedule, spec, full-block, header, and
event endpoints from the standard Beacon API. Alchemy documents a separate
[Beacon event endpoint](https://www.alchemy.com/docs/chains/ethereum/ethereum-beacon-api-endpoints/ethereum-beacon-api-endpoints/v-1-events).
Use that service, not its execution `newHeads` URL. Provider access and current
endpoint behavior still need a check with the account's credentials.

A private transaction RPC or a MEV-Boost relay URL is not automatically a block
feed. MEV-Boost holds a payload until the selected proposer supplies a signed
blinded block. Do not request or invent validator signatures to obtain it.
Only add a provider with an authorized, complete block feed. Proprietary feeds
need a reviewed adapter; this change does not claim support for every private
relay. [MEV-Boost design](https://boost.flashbots.net/).

The source requests both `block_gossip` and `block`. It falls back to `block` if
the server rejects the combined topics. A gossip event can precede full-block
availability. An imported event wakes the pending fetch after a 404. Reads race
all verified sources; the first **verified full block**, not the first HTTP
response, wins. A reconnect reconciles up to eight recent ancestors by root.

The relay checks the complete Beacon root, execution block hash, payload
timestamp, and transaction/blob-commitment order. It includes the parent Beacon
root and SSZ-encoded, type-prefixed execution requests. Empty request types stay
absent. These checks bind the data to the announced root; they do **not** verify
the proposer signature, consensus state, or blob availability. Configure trusted
sources. The execution clients still validate the payload.
[Engine V4 parameters](https://github.com/ethereum/execution-apis/blob/main/src/engine/prague.md#engine_newpayloadv4).

Each target has one delivery worker. It prefers recent slots, retains competing
hashes at the same slot, and tracks delivery by hash. One target cannot block
another. Readiness probes run separately; they do not add a read round trip before
each submission. The worker skips a block when a completed probe has already
confirmed it. Concurrent arrival can still cause a duplicate submission. Measure
that cost, especially when the local Lighthouse was the first source.

`VALID`, `ACCEPTED`, `SYNCING`, and `INVALID` remain distinct. `VALID` must name the
submitted hash. It is not evidence that direct RPC or canonical queries are ready.
After `SYNCING`, the worker can repair up to eight cached ancestors. It retries a
child after new parent evidence, not on a timer. Native sync handles larger gaps
and missing state. Invalid ancestors prevent descendant submissions.

The Engine response deadline is eight seconds. A timeout or malformed response
does not prove that the node stopped execution. The worker suspends new submissions
until direct RPC confirms that block. This state survives configuration reloads
for a retained Engine URL. Removing the relay or restarting its process clears
in-memory state; first resolve any unknown imports. Never run two injecting relay
processes against the same targets.
Do not list one physical Engine endpoint through multiple DNS or URL aliases.
Pair each Engine URL with the direct RPC URL of that same execution instance.

## Run against the existing stack

Use [the example config](block-relay.example.toml). Set `GETH_JWT_PATH` and
`RETH_JWT_PATH` to the **existing** secret files, as seen by this process. The
process needs read access. Keep Engine URLs on the private network. Do not expose
JWT files, put their contents in config, or commit account keys.
The example uses loopback addresses. Adjust ports to match the existing clients;
use private host addresses if the relay runs on a different machine.

Run only the monitor:

```sh
cargo run --release -p web3_proxy_cli -- --config docs/block-relay.example.toml block_relay
```

The command starts neither an RPC proxy nor an Ethereum node. It reports status
every 30 seconds and checks the config file every second. `Ctrl-C` or `SIGTERM`
stops intake and lets an outstanding Engine request finish or reach its deadline.

Alternatively, add `[block_relay]` to the existing proxy config and use `proxyd`.
The same service then appears under `block_relay` in `/status`. Its failure does
not change proxy readiness. The proxy's existing config watcher uses its normal
reload interval. Use only one deployment path for a given target fleet.

Omit the section to disable the service. `mode = "observe"` is the default. It
checks capabilities, fetches and verifies blocks, and probes direct RPCs, but
never submits a payload. Change only `mode` to `"inject"` for an approved trial.
Mode-only reloads retain streams and warm caches. Switching back to observe
prevents queued imports and ancestor repairs; an import already sent may finish.
Other config changes drain the previous workers before they start replacements.
Invalid local config leaves the previous config active.

No deployment or injection against the existing fleet occurred during implementation.
A read-only check verified a real Fulu Beacon root and execution hash against
Geth 1.17.5 and Reth 2.5.1, using the existing Lighthouse 8.2.2 API. Repeat it with:

```sh
cargo run -p web3_proxy --example block_relay_check -- \
  http://127.0.0.1:5062 http://127.0.0.1:8545 http://127.0.0.1:8547
```

This check verifies conversion and existing RPC availability. It does not test
Engine injection or establish a speed gain. Tests use HTTP/SSE fixtures and an
independent consensus-spec root vector. They never launch a node.

## Measure the primary objective

Measure the whole fleet, not only the first node or Engine response time. Use the
same target list, source list, process location, and probe load in both modes.
Do not change peer topology during the comparison.

Private diagnostic records use the tracing target
`web3_proxy::block_relay::samples`. Enable its debug level and retain those logs
privately. The service also retains the last 1,024 records in memory for callers
of `BlockRelay::samples()`. Public `/status` omits those records, URLs, headers,
JWT paths, and JWT contents. Its histograms are cumulative since the last full
config reload and mix modes; do not use them as the trial comparison.

Each record includes the block hash and slot, announcement source, successful
fetch source, mode, acquisition time, target, last confirmed absence, first RPC
readiness, and canonical readiness. Times use one process's monotonic clock and
start at its first Beacon announcement. No clock synchronization between nodes
is needed. A null readiness value is an incomplete observation, not a zero.

Probes begin after acquisition. Thus a target already ready on its first probe
has no measured arrival time: its observation is left-censored. Otherwise, the
true availability time lies between the last missing query and first successful
response. RPC errors do not count as proof of absence. Queries can take up to two
seconds; a final response can cross the one-slot observation deadline.

For each block, calculate the maximum target readiness time and the difference
between the last and first target. Report p50, p95, and p99 for both. Also report
canonical readiness, incomplete observations, left-censoring, dropped work,
source outages, and Engine outcomes. Never discard timeouts to improve a percentile.

Use a preselected, randomized sequence of epoch-sized observe/inject windows.
Exclude the first two slots after each mode transition from latency comparisons
to reduce carryover. Keep those slots in the safety report. Exclude reconnect
reconciliation events from latency comparisons. Retain same-slot competitors by
hash; report canonical blocks separately. Collect at least 10,000 eligible blocks
per mode and report confidence intervals by resampling whole epoch windows.
Keep all targets in each block's denominator. Count a block as incomplete if any
target is missing. Independently reconcile the observed slot/block set against
Lighthouse so source/acquisition failures cannot silently disappear from the trial.

Enable injection only if the all-node readiness tail improves without a material
increase in incomplete blocks, Engine errors, RPC latency, or Lighthouse import
latency. If the interval is inconclusive, keep observe mode. If contention or
canonical readiness dominates, payload relay is not the proven fix; inspect
the existing Lighthouse gossip path and node execution timings first.

## Bounds

The payload cache defaults to 128 MiB with a two-epoch TTL. It is not a total
process memory cap: requests, queued work, and observations can retain payloads.
The process allows 16 sources, 64 targets, eight concurrent acquisitions, four
concurrent decoding tasks, two full reads per source, and four probe sessions per
target. Announcement intake holds 256 events. Target delivery and pending queues
hold 128 blocks each. Observations hold at most 128 block sessions. HTTP responses
have a 32 MiB limit. SSE connections recycle after 1 MiB to bound parser memory.
Overflow counters are visible. Use modest source/target counts, private monitoring,
and normal process memory limits; do not treat this relay as a public submission API.
