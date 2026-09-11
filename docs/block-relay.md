# Block relay

The objective is to reduce the time until every execution and consensus node
imports a new block, and every stack exposes the canonical head. This feature is
an experiment, not a proven speed improvement. Duplicate execution and consensus
work can make a node slower. Measure the whole fleet while forwarding runs.

Use the existing Geth, Reth, and Lighthouse instances. Do not start more
Ethereum clients for this feature. Keep their existing native peer connections.
Full blob reconstruction may require a measured data-custody change on an
existing consensus node. That change is a separate experiment from injection.

## Cross-client contract

The relay submits complete execution payloads through the private, JWT-protected
`engine_newPayloadV4` method. It does not use a Reth-specific block import method.
The Engine specification permits additional payload producers when only one
consensus client controls fork choice. This relay never calls
`engine_forkchoiceUpdated` or requests payload builds. Each existing Lighthouse instance keeps sole control
of its execution node's chain head.
[Engine API common definitions](https://github.com/ethereum/execution-apis/blob/main/src/engine/common.md#load-balancing-and-advanced-configurations).

Consensus targets receive the original signed Beacon block and complete blob
contents through `POST /eth/v2/beacon/blocks?broadcast_validation=gossip`. The
`Eth-Consensus-Version` header selects the fork. Electra uses blob proofs; Fulu
uses cell proofs. The relay checks each blob against its commitment and generates
the proofs with Ethereum's trusted KZG setup. Lighthouse still checks the proposer
signature and consensus rules. The relay does not sign blocks or need validator keys.

This implementation supports Electra/Fulu blocks with the mainnet SSZ preset.
It checks network genesis, slot duration, configured fork versions and epochs,
the Engine chain ID, and target support for Engine V4. It rejects unknown
block versions. A newly advertised fork requires a configuration review; a new
block schema also requires code and tests. Do not add its name to the config and
assume it works. The relay does not support PoW or arbitrary EVM chains.

## Sources and delivery

```text
Existing local Lighthouse instances + external Beacon APIs
                   │ block_gossip / block events
                   ▼
  Race full-block reads → verify roots and execution hash
                   │
         ┌─────────┴─────────────────┐
         ▼                           ▼
  Engine V4 payload          Race blobs → verify commitments
         │                           │ generate fork-correct proofs
         ▼                           ▼
  All execution targets       All consensus targets
         │                           │
  Direct RPC probes           Non-optimistic Beacon header probes
         └─────────────┬─────────────┘
                Full-fleet readiness
```

Sources never enter `balanced_rpcs`, `private_rpcs`, or the proxy's readiness
quorum. Local and external sources use the same path. A local first arrival can
therefore reach the other configured targets. “Peers” here means configured
execution and consensus destinations, not arbitrary Ethereum P2P peers.

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
response, wins. Retry ready sources while slower requests remain pending. A
reconnect reconciles up to eight recent ancestors by root.

The relay checks the complete Beacon root, execution block hash, payload
timestamp, and transaction/blob-commitment order. It includes the parent Beacon
root and SSZ-encoded, type-prefixed execution requests. Empty request types stay
absent. These checks bind the data to the announced root; they do **not** verify
the proposer signature or consensus state. Execution delivery does not wait for
blobs. Consensus delivery waits for complete, checked blobs and locally generated
proofs. Configure trusted sources. Destination clients retain native validation.
[Engine V4 parameters](https://github.com/ethereum/execution-apis/blob/main/src/engine/prague.md#engine_newpayloadv4).

Each target has one delivery worker. It prefers recent slots, retains competing
hashes at the same slot, and tracks delivery by hash. One target cannot block
another. Readiness probes run separately; they do not add a read round trip before
each submission. The worker skips a block when a completed probe has already
confirmed it. Concurrent arrival can still cause a duplicate submission. Measure
that cost, especially when the local Lighthouse was the first source.

Multiple forwarders can submit to the same complete destination list. Each sender
keeps its own bounded queues and duplicate suppression. There is no cross-host
leader, target partition, exclusion lock, or delay for the second sender.

Consensus publication responses do not establish readiness. In particular, HTTP
202 can mean the block was broadcast but not imported. Confirm the requested
header root and non-optimistic execution status. Track canonical availability
separately. Repair at most eight cached Beacon ancestors and bound child retries.
Keep competing roots at the same slot. Native sync handles larger gaps.

`VALID`, `ACCEPTED`, `SYNCING`, and `INVALID` remain distinct. `VALID` must name the
submitted hash. It is not evidence that direct RPC or canonical queries are ready.
After `SYNCING`, the worker can repair up to eight cached ancestors. It retries a
child after new parent evidence, not on a timer. Native sync handles larger gaps
and missing state. Invalid ancestors prevent descendant submissions.

The Engine response deadline is eight seconds. A timeout or malformed response
leaves that import **unknown**. It does not prove that execution stopped or
succeeded. After the deadline, the target worker proceeds with eligible newer
work. Duplicate suppression prevents repeated sends of the unknown hash. RPC
confirmation can establish that the block is available and wake waiting children.
Workers retain delivery history, including invalid ancestry, across worker
restarts and config reloads with the same normalized Engine endpoint.

There is no runtime Engine journal. Existing journal files remain untouched as
historical data. Storage faults cannot prevent Engine submissions. Each source
and target restarts independently after an exit or panic. JWT files are read
within that target's request deadline and retried independently. Invalid endpoint
settings appear in status; other endpoints continue. Network validation remains
required for each source and target. Direct RPC probes do not gate Engine sends.

Set `state_dir` to an absolute path for private measurement files. Different
forwarders use their own directories. The directory should survive container
replacement. Changing it requires a process restart. Do not list one physical
Engine endpoint through multiple DNS aliases. Pair each Engine URL with the
direct RPC URL of that execution instance.

## Run against the existing stack

Use [the example config](block-relay.example.toml). Set `GETH_JWT_PATH` and
`RETH_JWT_PATH` to the **existing** secret files, as seen by this process. The
process needs read access. Configure `execution_targets` and `consensus_targets`
separately. These replace the old, unreleased execution-only target configuration;
there is no alias for that format. Keep Engine and Beacon publication URLs on the private network. Do not expose
JWT files, put their contents in config, or commit account keys.
The example uses loopback addresses. Adjust ports to match the existing clients;
use private host addresses if the relay runs on a different machine.

Run only the monitor:

```sh
cargo run --release -p web3_proxy_cli -- --config docs/block-relay.example.toml block_relay
```

The command starts neither an RPC proxy nor an Ethereum node. It reports status
every 30 seconds and checks the config file every second. `Ctrl-C` or `SIGTERM`
stops intake and drains current imports within a 25-second application deadline.
Its private status listener defaults to `127.0.0.1:18550`. Use `--status-address`
to set the container listener address; keep the host port private.

- `/live` confirms that the relay supervisor is running. Use it for container startup.
- `/health` reports recent useful acquisition and forwarding on at least one
  configured layer. One working source and target can pass. It never gates forwarding.
- `/status` reports `healthy`, `degraded`, or `unavailable` operation, with current
  source, target, acquisition, probe, and recording conditions plus historical
  counters. Recovery clears degradation without erasing errors. Silent sources
  and missing blobs remain visible. The endpoint accepts no control writes.

The service writes private JSONL files below `state_dir/observations`. These
include source events, acquisition failures, blob readiness, and per-target
observations with their mode, slot, root, and missing/ready bounds. Durations use
a monotonic clock. Wall-clock timestamps only help join observer records.
The writer has a bounded queue, syncs each second, rotates at 16 MiB, and caps
storage at 512 MiB total. It never erases evidence. Full storage, queue overflow,
and disk errors increment visible error/drop counters. The writer retries storage
at a bounded rate while forwarding continues. The last unsynced records can be
lost on a crash. Reconcile recorded roots against the chain before evaluating a trial.

Alternatively, add `[block_relay]` to the existing proxy config and use `proxyd`.
The same service then appears under `block_relay` in `/status`. Its failure does
not change proxy readiness. The proxy's existing config watcher uses its normal
reload interval. Do not enable this embedded service on a proxy host that already
runs a standalone forwarder. Independent forwarders on different hosts can each
submit to the full target fleet.

Omit the section to disable the service. `mode = "observe"` is the default. It
checks capabilities, fetches and verifies blocks, and probes direct RPCs, but
never sends Engine payloads or Beacon publications. Change only `mode` to
`"inject"` for an approved trial.
Mode-only reloads retain streams and warm caches. Switching back to observe
prevents queued imports and ancestor repairs; an import already sent may finish.
Other config changes drain the previous workers before they start replacements.
Changing the proof-worker count requires a process restart. KZG setup runs on the
blocking pool during preparation, before source intake. Blob decoding uses
heap-backed bytes; proof work has its own bounded pool and cannot occupy the
execution decoder pool or its source-read permits.
Invalid local config leaves the previous config active.

The initial read-only check verified a real Fulu Beacon root and execution hash against
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
JWT paths, and JWT contents. Its histograms are cumulative for the process lifetime and mix modes; do not use them as the trial comparison.

Each record includes its execution/consensus layer, Beacon root, execution hash and slot, announcement source, successful
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
canonical readiness across all targets, incomplete observations, left-censoring,
dropped work, source outages, Engine outcomes, and Beacon publication responses.
Never discard timeouts to improve a percentile. A successful POST is not a speed result.

Observe at least 32 current canonical blocks during functional acceptance. Check
complete block and blob acquisition, submission results, direct RPC and Beacon
imports, and destination coverage. Keep execution forwarding active if complete
blobs are unavailable; report the consensus limitation and do not claim complete
acceptance. Distinguish RPC-confirmed already-known skips from duplicate
suppression and failed delivery.

A speed claim requires comparable full-fleet data and uncertainty estimates.
Use the same sources, targets, probe load, and observer location across trials.
Include incomplete blocks and errors in the results. Larger randomized trials
can improve the estimate; they are not a prerequisite for broadcast. Roll back
an affected application only for a confirmed release defect. A source, target,
or recording outage alone is not a reason to disable healthy forwarding.

## Bounds

The payload cache defaults to 128 MiB with a two-epoch TTL. It is not a total
process memory cap: requests, queued work, and observations can retain payloads.
The process allows 16 sources, 64 targets, eight concurrent acquisitions, four
concurrent execution decoding tasks, a separate bounded proof pool, two block
reads and two blob reads per source, and four probe sessions per target.
Consensus contents use a separate cache with the same configured byte bound.
Announcement intake holds 256 events. Target delivery and pending queues
hold 128 blocks each. Observations hold at most 128 block sessions. HTTP responses
have a 32 MiB limit. SSE connections recycle after 1 MiB to bound parser memory.
Overflow counters are visible. Use modest source/target counts, private monitoring,
and normal process memory limits; do not treat this relay as a public submission API.
