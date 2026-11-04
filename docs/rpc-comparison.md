# Compare live HTTP RPC responses

`compare-rpcs` sends the same requests to the normal proxy URL and each named
backend. It compares responses without using the proxy's response decoder.
It does not start clients, change proxy settings, clear caches, or submit transactions.

## Configure endpoints

Keep this private TOML file outside Git. List every backend, including backups.
The runner uses each endpoint URL exactly as supplied. Use the normal proxy route
(`/`) to test the path that applications use.

```toml
# Optional operator-supplied reference. The runner does not verify this version.
proxy_version = "deployment image digest or commit reference"

[proxy]
name = "proxy"
url_env = "COMPARE_PROXY_URL"
# Optional: each header value names an environment variable, not a literal secret.
# headers = { Authorization = "COMPARE_PROXY_AUTHORIZATION" }
# For API keys in URL paths/query parameters or custom headers, explicitly name
# variables containing the secret values (not the complete URL).
# secret_env = ["COMPARE_PROXY_API_KEY"]

[[backends]]
name = "erigon"
url_env = "COMPARE_ERIGON_URL"

[[backends]]
name = "geth_backup"
url_env = "COMPARE_GETH_URL"
```

Set these variables in your private environment. Do not put credentials in command
arguments. The URL variables must contain distinct HTTP or HTTPS URLs. Header
variables contain complete values, such as `Bearer ...`. Do not supply credentials
that could appear as ordinary response values.

The runner automatically protects complete endpoint URLs, URL passwords, and
`Authorization`/`Proxy-Authorization` header values and their credential portions.
List environment variables containing API keys or other sensitive URL components
and custom header values in that endpoint's `secret_env`. Missing or empty secret
variables stop setup. Ordinary query parameters, paths, and headers such as
`Accept: application/json` are not classified as credentials. The runner does not
guess whether a query value or path segment is a secret.

Run the command with a **new** output directory whose parent already exists:

```sh
cargo run --locked -p web3_proxy_cli -- compare-rpcs \
  --endpoints /path/to/private/endpoints.toml \
  --output /Volumes/Sabrent4TB/agent-work/web3-proxy/rpc-run-001
```

Use the repository's external build and cache directories for development. The
command also works as `web3_proxy_cli compare-rpcs ...` from a built binary. It
needs no proxy server config and does not initialize Sentry.

## Chain state and coverage

The runner checks chain ID and genesis hash on all endpoints. It queries actual
`finalized` blocks on the direct backends, selects the lowest finalized height,
and requires all endpoints to return the same hash at that height. It records
direct-node client versions when available. If the chain does not support
`finalized`, supply `--block 0x...` with an explicit hexadecimal block number.

State queries use EIP-1898 `{blockHash, requireCanonical: true}`. Methods that use
block numbers receive the fixed height. The runner checks the anchor number and
hash again after capture. A failed final check invalidates all strict verdicts;
the raw observations remain available.

The built-in suite covers:

- Blocks by hash and number, with full transactions and transaction hashes.
- Balance, nonce, code, storage, logs, calls, a transaction, and its receipt.
- Successful and reverting `eth_call` cases with state-override bytecode. These
  require state-override support. An unsupported override is a coverage gap.
- `trace_call`, `trace_callMany`, `trace_block`, `trace_filter`, `trace_transaction`,
  `trace_get`, `trace_replayTransaction`, and `trace_replayBlockTransactions`.
  Trace options request the `trace` output family. `stateDiff`, `vmTrace`, and
  `trace_rawTransaction` need separate cases/work; this suite does not claim their coverage.

Transaction cases use the first transaction in the anchor block. An empty block
leaves those cases incomplete. Block-wide trace cases run regardless of the
anchor's transaction count. The same request timeout and response-size limits
apply to them. `trace_filter` stays within that block and requests at most 1,000
entries; this can truncate its coverage.

Every available case runs three times individually and three times in mixed
batches of up to four requests. Methods with multiple cases also run in batches
grouped by method. This includes all-`eth_call` batches, which use a separate
proxy execution path. The runner also checks replies against their first
individual observation to detect changes between repeats or batch forms.
It labels `eth_blockNumber` and `eth_estimateGas` as observations. Moving heads,
client estimation rules, and configured proxy estimate increases can differ.
They do not enter the strict equality pass count.

Defaults: one active request per endpoint, 1,000 ms between starts per endpoint,
a 30-second complete-request timeout, and a 32 MiB response limit. Endpoints run
in parallel. Override these with `--interval-ms`, `--timeout-seconds`,
`--max-response-bytes`, and `--batch-size` (1–32). There are no application retries,
HTTP retries, or redirect following. A failed observation stays failed.

## Add real application requests

Supply `--requests /path/to/cases.json` to add a JSON array of cases. Names must
be unique, including built-in names. The file limit is 4 MiB and 256 cases.
For example:

```json
[
  {
    "name": "application_balance",
    "method": "eth_getBalance",
    "params": ["0x0000000000000000000000000000000000000000", "$STATE_BLOCK"]
  },
  {
    "name": "application_trace_call",
    "method": "trace_call",
    "params": [
      {"to": "0x0000000000000000000000000000000000000000", "data": "0x", "gas": "0x186a0"},
      ["trace", "stateDiff"],
      "$BLOCK_NUMBER"
    ]
  }
]
```

Replace addresses and call data with the actual application inputs. Exact string
placeholders bind to the selected anchor: `$BLOCK_NUMBER`, `$BLOCK_HASH`,
`$STATE_BLOCK`, and `$TX_HASH`. Other values remain as supplied. Transaction-based
exact cases must refer to a transaction in the anchor block. Exact log queries
must use its `blockHash`. Trace filters must stay within the anchor block and
include a `count` from 1 to 1,000.

`expect` defaults to `non_null`: the response must contain a non-null result.
Use `result` when null is an intended successful outcome, or `revert` for an
expected execution revert. Matching unsupported-method or missing-state errors
are incomplete coverage, not successful method tests.

`comparison` defaults to `exact`. Use `observation` for moving state or intended
policy checks. Trace and log bounds still apply to observation cases. The method
allowlist excludes submissions and administrative actions. Invalid cases stop
the run; the runner does not silently rewrite them to another block.

## Read the evidence

`report.json` contains the anchor, cases, endpoint labels, client versions,
per-case verdicts, counts, and exit code. `exchanges.jsonl` links each exact
request body to endpoint observations, HTTP status, elapsed time, relevant proxy
headers, and raw `.body` files. A transport or size failure can leave a partial
body when the credential scan completes; the observation marks it as partial.
An undecodable body is withheld when configured URLs or secrets need protection.
JSON differences use JSON Pointer paths; an omitted difference side means missing,
while a present `null` means null.
Each pair lists at most 100 differences; inspect the raw bodies for the complete
difference.

Comparison ignores JSON spacing and object-key order. It compares decoded string
values, preserves number tokens without floating-point conversion, and keeps
hex text, array order, field presence, types, and error objects exact, with one
exception: for execution reverts, missing `error.data` and `error.data: "0x"`
compare as equal. This applies across backends, batch forms, and repeats. Raw
captures retain the original fields. Non-empty data, `null`, error codes, error
messages, and data on other errors still compare exactly.
Thus `"0x0"` differs from `"0x00"`, and numeric `1` differs from `1.0`.
Duplicate JSON keys and invalid JSON-RPC envelopes are incomplete observations.
Batch replies can arrive in any order, but must contain exactly the requested IDs.

- **Full agreement:** Every endpoint returns the same expected response.
- **Backend disagreement:** Direct backends differ. The report lists those whose
  replies match the proxy.
- **Proxy-only difference:** The proxy differs from all captured direct replies.
- **Repeat difference:** A reply differs from that endpoint's first individual
  response. This can identify a batch-dependent or unstable response.
- **Incomplete:** A timeout, unsupported method, missing state, unexpected null,
  invalid reply, skipped case, or unstable anchor prevents full coverage.
- **Observation:** A moving-state or policy case outside strict equality counts.

Counts can overlap: a case can show both a difference and incomplete coverage.
Exit **0** requires complete agreement in every required exact case. Exit **1**
means observed content or repeat differences. Exit **2** means incomplete coverage
or setup failure without a content difference. An invalidated anchor yields 2.

The runner creates a private directory (0700) and private files (0600) on Unix.
It does not save endpoint URLs or request authorization headers. If a reply
reflects a configured URL or credential, it withholds that reply and marks a gap
instead of changing the raw evidence. The scan checks every JSON key and value,
including duplicate fields, before strict response validation. If it cannot
finish decoding a body, it withholds that body and keeps the HTTP metadata and
failure reason. Treat all evidence as private: requests and response bodies can
contain application data.

`X-W3P-BACKEND-RPCS` and `X-W3P-BACKUP-RPC` describe backend attempts. They do not
identify the source of a cache entry or prove which retry supplied the reply.
First and repeat captures do not prove cold or warm cache state. A proxy-only
difference can come from cached client-specific fields; it is an observation to
investigate, not proof of accidental rewriting. A match with one disagreeing
backend likewise does not prove that every backend or every proxy path agrees.

A passing run proves agreement only for the recorded endpoints, requests, chain
state, and capture period. It does not prove Ethereum correctness, WebSocket
behavior, all cache states, forced backup delivery, reorg handling, or all inputs.
Controlled proxy/cache tests and WebSocket coverage remain separate work.

For further cases, use the designs in [trace-interop](https://github.com/banteg/trace-interop)
and [execution-apis](https://github.com/ethereum/execution-apis). Their generated
chain fixtures need adaptation for live production chains. This runner does not
start Hive or build execution clients.
