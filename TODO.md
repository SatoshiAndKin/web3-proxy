# Todo

This is stale. Now that there is more than one dev, important things are tracked in GitHub Issues and Pull Requests.

One day I'll go through this and make sure everything is done, moved to an issue, or otherwise handled.

## MVP

These are roughly in order of completition

- [x] simple proxy
- [x] better locking. when lots of requests come in, we seem to be in the way of block updates
- [x] load balance between multiple RPC servers
- [x] support more than just ETH
- [x] option to disable private rpc and send everything to primary
- [x] support websocket clients
  - we support websockets for the backends already, but we need them for the frontend too
- [x] health check nodes by block height
- [x] Dockerfile
- [x] docker-compose.yml
- [x] after connecting to a server, check that it gives the expected chainId
- [x] the ethermine rpc is usually fastest. but its in the private tier. since we only allow synced rpcs, we are going to not have an rpc a lot of the time
- [x] if not backends. return a 502 instead of delaying?
- [x] move from warp to axum
- [x] handle websocket disconnect and reconnect
- [x] eth_sendRawTransaction should return the most common result, not the first
- [x] it works for a few seconds and then gets stuck on something.
  - [x] its working with one backend node, but multiple breaks. something to do with pending transactions
  - [x] dashmap entry api is easy to deadlock! be careful with it!
- [x] the web3proxyapp object gets cloned for every call. why do we need any arcs inside that? shouldn't they be able to connect to the app's? can we just use static lifetimes
- [x] refactor Connection::spawn. have it return a handle to the spawned future of it running with block and transaction subscriptions
- [x] refactor Connections::spawn. have it return a handle that is selecting on those handles?
- [x] some production configs are occassionally stuck waiting at 100% cpu
  - they stop processing new blocks. i'm guessing 2 blocks arrive at the same time, but i thought our locks would handle that
  - even after removing a bunch of the locks, the deadlock still happens. i can't reliably reproduce. i just let it run for awhile and it happens.
  - running gdb shows the thread at tokio tungstenite thread is spinning near 100% cpu and none of the rest of the program is proceeding
  - fixed by an upstream WebSocket transport patch
- [x] when sending with private relays, brownie's tx.wait can think the transaction was dropped. smarter retry on eth_getTransactionByHash and eth_getTransactionReceipt (maybe only if we sent the transaction ourselves)
- [x] if web3 proxy gets an http error back, retry another node
- [x] endpoint for health checks. if no synced servers, give a 502 error
- [x] rpc errors propagate too far. one subscription failing ends the app. isolate the providers more (might already be fixed)
- [x] automatically route to archive server when necessary
  - originally, no processing was done to params; they were raw JSON. this is probably fastest, but we need to look for "latest" and count elements, so we have to use sonic_rs::Value
  - when getting the next server, filtering on "archive" isn't going to work well. need to check inner instead
- [x] if the requested block is ahead of the best block, return without querying any backend servers
- [x] http servers should check block at the very start
- [x] subscription id should be per connection, not global
- [x] when under load, i'm seeing "http interval lagging!". sometimes it happens when not loaded.
  - we were skipping our delay interval when block hash wasn't changed. so if a block was ever slow, the http provider would get the same hash twice and then would try eth_getBlockByNumber a ton of times
- [x] inspect any jsonrpc errors. if its something like "header not found" or "block with id $x not found" retry on another node (and add a negative score to that server)
  - this error seems to happen when we use load balanced backend rpcs like pokt and ankr
- [x] web3_sha3 rpc command
- [x] test that launches anvil and connects the proxy to it and does some basic queries
  - [x] need to have some sort of shutdown signaling. doesn't need to be graceful at this point, but should be eventually
- [x] if the fastest server has hit rate limits, we won't be able to serve any traffic until another server is synced.
  - thundering herd problem if we only allow a lag of 0 blocks
  - we can improve this by only publishing the synced connections once a threshold of total available soft limits is passed.
  - [x] instead of tracking `pending_synced_connections`, have a mapping of where all connections are individually. then each change, re-check for consensus.
- [x] synced connections swap threshold set to 1 so that it always serves something
- [x] sort forked blocks by total difficulty like geth does
- [x] refactor result type on active handlers to use a cleaner success/error so we can use the try operator
- [x] Add a "weight" key to the servers. Sort on that after block. keep most requests local
- [x] allow blocking public requests
- [x] Got warning: "WARN subscribe_new_heads:send_block: web3_proxy::connection: unable to get block from https://rpc.ethermine.org: Deserialization Error: expected value at line 1 column 1. Response: error code: 1015". this is cloudflare rate limiting on fetching a block, but this is a private rpc. why is there a block subscription?
- [x] im seeing ethspam occasionally try to query a future block. something must be setting the head block too early
  - [x] we were sorting best block the wrong direction. i flipped a.cmp(b) to b.cmp(a) so that the largest would be first, but then i used 'max_by' which looks at the end of the list
- [x] HTTP GET to the websocket endpoints should redirect instead of giving an ugly error
- [x] load the redirected page from config
- [x] attach a request id to every web request
- [x] fantom_1    | 2022-08-10T22:19:43.522465Z  WARN web3_proxy::jsonrpc: forwarding error err=missing field `jsonrpc` at line 1 column 60
  - [x] i think the server isn't following the spec. we need a context attached to more errors so we know which one
  - [x] make jsonrpc default to "2.0" (including the custom deserializer that handles the RawValues)
- [x] if the eth_call (or similar) params include a block, we can cache for that
- [x] when block subscribers receive blocks, store them in a block_map
- [x] eth_blockNumber without a backend request
- [x] if we send a transaction to private rpcs and then people query it on public rpcs things, some interfaces might think the transaction is dropped (i saw this happen in a brownie script of mine). how should we handle this?
  - [x] send getTransaction rpc requests to the private rpc tier
- [x] I'm hitting infura rate limits very quickly. I feel like that means something is very inefficient
  - whenever blocks were slow, we started checking as fast as possible
- [x] improve consensus block selection. Our goal is to find the highest work chain with a block over a minimum threshold of sum_soft_limit.
  - [x] i saw a fork of like 300 blocks. probably just because a node was restarted and had fallen behind. need some checks to ignore things that are far behind. this improvement should fix this problem
  - [x] A new block arrives at a connection.
  - [x] It checks that it isn't the same that it already has (which is a problem with polling nodes)
  - [x] If its new to this node...
    - [x] if the block does not have total work, check our cache. otherwise, query the node
    - [x] save the block num and hash so that http polling doesn't send duplicates
    - [x] send the deduped block through a channel to be handled by the connections grouping.
  - [x] The connections group...
    - [x] input = rpc, new_block
    - [x] adds the block and rpc to it's internal maps
      - [x] connection_heads: HashMap<rpc_name, blockhash>
      - [x] block_map: DashMap<blockhash, Arc<Block>>
      - [x] block_num: DashMap<U64, H256>
      - [x] blockchain: DiGraphMap<blockhash, ?>
    - [x] iterate the rpc_map to find the highest_work_block
    - [x] update synced connections
    - [x] send the block through new head_block_sender
  - [x] rewrite cannonical_block to work as long as there are no forks
  - [x] rewrite cannonical_block (again) and related functions to handle forks
    - [x] got a very large number of possible heads here. i think maybe a server was very far out of sync. we should drop servers behind by too much
    eth_1       | 2022-08-10T23:26:06.377129Z  WARN web3_proxy::connections: chain is forked! 261 possible heads. 1/2/5/5 rpcs have 0xd403…3c5d
    eth_1       | 2022-08-10T23:26:08.917603Z  WARN web3_proxy::connections: chain is forked! 262 possible heads. 1/2/5/5 rpcs have 0x0538…bfff
    eth_1       | 2022-08-10T23:26:10.195014Z  WARN web3_proxy::connections: chain is forked! 262 possible heads. 1/2/5/5 rpcs have 0x0538…bfff
    eth_1       | 2022-08-10T23:26:10.195658Z  WARN web3_proxy::connections: chain is forked! 262 possible heads. 2/3/5/5 rpcs have 0x0538…bfff
    - [x] todo!("handle equal") and also less and greater
    - [x] "chain is forked" message is wrong. it includes nodes just being on different heights of the same chain. need a smarter check
      - i think there is also a bug because i've seen "server not synced" a couple times
- [x] bug around eth_getBlockByHash sometimes causes tokio to lock up
  - i keep a mapping of blocks so that i can go from hash -> block. it has some consistent hashing it does to split them up across multiple maps each with their own lock. so a lot of the time reads dont block writes because they are in different internal maps. this was fine. but after changing my fork detection logic to use the same rules as erigon, i discovered that when you get blocks from a websocket subscription in erigon and geth, theres a missing field (https://github.com/ledgerwatch/erigon/issues/5190). so i added a query to get the block that includes the missing field.
  - but i did this in a way where i was holding the write lock open while doing the query. the "new" block that has the missing field ends up in the same bucket and it also wants a write lock. oops. entry api has very sharp edges. don't ever await inside a match on DashMap::entry
- [x] requests for "Get transactions receipts" are routed to the private_rpcs and not the balanced_rpcs. do this better.
  - [x] quick fix, send to balanced_rpcs for now. we will just live with errors on new transactions.
  - this was intentional so that recently confirmed transactions go to a server that is more likely to have the tx.
  - but under heavy load, we hit their rate limits. need a "retry_until_success" function that goes to balanced_rpcs.
- [x] some of the DashMaps grow unbounded! Make/find a "SizedDashMap" that cleans up old rows with some garbage collection task
  - moka has all the features that we need and more
- [x] if block data limit is 0, say Unknown in Debug output
- [x] refactor from_anyhow_error to have consistent error codes and http codes. maybe implement the Error trait
- [x] improve rpc weights. i think theres still a potential thundering herd
- [x] improved logging with useful instrumentation
- [x] synced connections swap threshold should come from config
- [x] right now we send too many getTransaction queries to the private rpc tier and i are being rate limited by some of them. change to be serial and weight by hard/soft limit.  
- [x] ip blocking gives a 500 and not the proper error code
- [x] need a reconnect that doesn't unwrap
- [x] need a retrying_reconnect that is used everywhere reconnect is. have exponential backoff here
- [x] it looks like our reconnect logic is not always firing. we need to make reconnect more robust!
  - i am pretty sure that this is actually servers that fail to connect on initial setup (maybe the rpcs that are on the wrong chain are just timing out and they aren't set to reconnect?)
- [x] chain rolled back 1/1/1 con_head=15510065 (0xa4a3…d2d8) rpc_head=15510065 (0xa4a3…d2d8) rpc=local_erigon_archive
  - include the old head number and block in the log
- [x] exponential backoff when reconnecting a connection
- [x] once the merge happens, we don't want to use total difficulty and instead just care about the number
- [x] web3_proxy_error_count{path = "backend_rpc/request"} is inflated by a bunch of reverts. do not log reverts as warn. 
  - erigon gives `method=eth_call reqid=986147 t=1.151551ms err="execution reverted"`
- [x] in /status, block hashes has a lower count than block numbers. how is that possible?
  - we weren't calling sync. now we are
- [x] ip blocking logs a warn. we don't need that
- [x] get to /, when not serving a websocket, should have a simple welcome page. maybe with a button to update your wallet. 
- [x] improve `web3_proxy_cli check_config`
  - print out warnings if important settings are missing
- [x] if unknown config items, error
  - unknown configs are almost always a mistake. usually from me changing config parsing on my side and old fields not being updated to the new way
  - [x] also need to change how we disable rpcs since i was using an unknown field
- [x] graceful shutdown. stop taking new requests and don't stop until all outstanding queries are handled
  - need a tokio::sync::watch on unflushed stats that we can subscribe to. wait for it to flip to true
- [x] don't use unix timestamps for response_millis since leap seconds will confuse it
- [x] config to allow origins even on the anonymous endpoints
- [x] send logs to sentry
- [x] add config for concurrent requests from public requests
- [x] document url params with examples
- [x] improve "docs/http routes.txt"
- [x] instruments are missing. maybe that is why sentry had broken traces
- [x] description should default to an empty string instead of being nullable
- [x] include if archive query or not in the stats
- [x] fix test not shutting down
- [x] /status should include the server weights
- [x] test that runs check_config against example.toml
- [x] improve sorting servers by weight. don't force to lower weights, still have a probability that smaller weights might be 
- [x] flamegraphs show 52% of the time to be in tracing. replace with simpler logging
- [x] add optional display name to rpc configs
- [x] cli tool for checking config
- [x] cache the status page for a second
- [x] test that sets up a Web3Rpc and asks "has_block" for old and new blocks
- [x] test that sets up Web3Rpcs with 2 nodes. one behind by several blocks. and see what the "next" server shows as
- [x] ethspam on bsc and polygon gives 1/4 errors. fix whatever is causing this
  - bugfix! we were using the whole connection list instead of just the synced connection list when picking servers. oops!
- [x] smarter reconnection logic
- [x] if a websocket connection hasn't received a new block in a while, do a reconnect or just query the block. its possible that the node was syncing when the proxy started
- [x] on web3-proxy start, if a node fails to connect, it can hold up listening on 8544
    - need to do all the connections in parallel with spawns
- [x] add block timestamp to the /status page
  - [x] be sure to save the timestamp in a way that our request routing logic can make use of it
- [x] node selection still needs improvements. we still send to syncing nodes if they are close
    - try consensus heads first! only if that is empty should we try others. and we should try them sorted by block height and then randomly chosen from there
- [x] logging of "bad response!" is way too verbose
- [x] i think our "best" server picking is incorrect somehow.
    - we upgraded erigon to a version with a broken websocket
    - that made it clear we still route to the lagged server sometimes. this is bad, but retries keep it from giving users bad data.
- [x] more trace logging
- [x] on ETH, we no longer need total difficulty
- [x] benchmarks of the different Cache implementations (futures vs dash)
  - futures is better
- [x] if archive servers are added to the rotation while they are still syncing, they might get requests too soon. keep archive servers out of the configs until they are done syncing. full nodes should be fine to add to the configs even while syncing, though its a wasted connection
- [x] subscribing to transactions should be configurable per server. listening to paid servers can get expensive
- [x] status page leaks our urls which contain secrets. change that to use names
- [x] for easier errors in the axum code, i think we need to have our own type that wraps anyhow::Result+Error
- [x] hit counts seem wrong. how are we hitting the backend so much more than the frontend? retries on disconnect don't seem to fit that
  web3_proxy_hit_count{path = "app/proxy_web3_rpc_request"} 857270
  web3_proxy_hit_count{path = "backend_rpc/request"}       1396127
  - this was because backend server ordering was including servers that were still syncing from too long ago
## V1

These are not yet ordered. There might be duplicates. We might not actually need all of these.

- [x] put display name into our prod configs
- [x] sometimes when fetching a txid through the proxy it fails, but fetching from the backends works fine
  - check flashprofits logs for examples
  - we were caching too aggressively
- [x] BUG! if sending transactions gets "INTERNAL_ERROR: existing tx with same hash", create a success message
  - we just want to be sure that the server has our tx and in this case, it does.
- [x] serde collect unknown fields in config instead of crash
- [x] all_backend_connections skips syncing servers
- [x] change weight back to tier
- [x] fix multiple origin and referer checks
- [x] ip detection needs work so that everything doesnt show up as 172.x.x.x
  - i think this was done, but am not positive.
- [x] if private txs are disabled, only send trasactions to some of our servers. we were DOSing ourselves with transactions and slowing down sync
- [x] retry if we get "the method X is not available"
- [x] remove weight. we don't use it anymore. tiers are what we use now
- [x] make deadlock feature optional
- [x] standalone healthcheck daemon (sentryd)
- [x] status page should show version
- [x] combine the proxy and cli into one bin
- [x] retry another server if we get a jsonrpc response error about rate limits
- [x] major refactor to only use backup servers when absolutely necessary
- [x] remove allowed lag
- [x] configurable gas buffer. default to the larger of 25k or 25% on polygon to work around erigon bug
- [x] public is 3900, but free is 360. free should be at least 3900 but probably more
- [x] add --max-wait to wait_for_sync
- [x] add automatic compare urls to wait_for_sync
- [x] enable lto on release builds
- [x] less logs for backup servers
- [x] use channels instead of arcswap
  - this will let us easily wait for a new head or a new synced connection
- [x] broadcast transactions to more servers
- [x] improve handling of unknown methods
- [x] improve waiting for sync when rate limited
- [x] short lived cache on /health
- [x] cache /status for longer
- [x] sort connections during eth_sendRawTransaction
- [x] block all admin_ rpc commands
- [x] remove the "metered" crate now that we save aggregate queries?
- [x] add archive depth to app config
- [x] use from_block and to_block so that eth_getLogs is routed correctly
- [x] improve eth_sendRawTransaction server selection
- [x] don't cache methods that are usually very large
- [x] use http provider when available
- [x] per-chain rpc rate limits
- [x] canonical block checks giving weird errors. change healthcheck to use block number
    [2023-02-21T02:58:06Z DEBUG web3_proxy::rpcs::request] error response from blastapi! method=eth_getCode params=(0xa9a8760b8333efae8c9c751e6695a11938ae4b90, 0x73a627f588338804e6dc880154728484f7e0373c29057408c6674d75bdc29d12) err=JsonRpcClientError(JsonRpcError(JsonRpcError { code: -32603, message: "hash 73a627f588338804e6dc880154728484f7e0373c29057408c6674d75bdc29d12 is not currently canonical", data: None }))
    [2023-02-21T02:58:06Z DEBUG web3_proxy::rpcs::one] blastapi failed health check query! Error {
            context: "ProviderError from the backend",
            source: JsonRpcClientError(
                JsonRpcError(
                    JsonRpcError {
                        code: -32603,
                        message: "hash 73a627f588338804e6dc880154728484f7e0373c29057408c6674d75bdc29d12 is not currently canonical",
                        data: None,
                    },
                ),
            ),
        }
- [x] add a "failover" tier that is only used if balanced_rpcs has "no servers synced"
  - use this tier (and private tier) to check timestamp on latest block. if we are behind that by more than a few seconds, something is wrong
- [x] eth_getLogs is going to unsynced nodes because it only checks start block and not the end block
- [x] have multiple providers on each backend rpc. one websocket for newHeads. and then http providers for handling requests
  - erigon only streams the JSON over HTTP. that code isn't enabled for websockets. so this should save memory on the erigon servers
  - i think this also means we don't need to worry about changing the id that the user gives us.
- [x] eth_getLogs is going to unsynced nodes because it only checks start block and not the end block
- [x] fix caching getLogs with blockhash
- [x] fix trying to send signed transactions to an empty list of private_rpcs
- [x] improve logging around consensus head.
  - it was "num in best synced tier"/num rpc connected/num rpc known.
  - it should be "num with best head in best synced tier/num with best head in any tier/num rpcs connected/num rpcs known
- [x] refactor so configs can change while running
  - this will probably be a rather large change, but is necessary when we have autoscaling
  - create the app without applying any config to it
  - have a blocking future watching the config file and calling app.apply_config() on first load and on change
  - work started on this in the "config_reloads" branch. because of how we pass channels around during spawn, this requires a larger refactor.
- [-] if we subscribe to a server that is syncing, it gives us null block_data_limit. when it catches up, we don't ever send queries to it. we need to recheck block_data_limit
- [ ] don't use new_head_provider anywhere except new head subscription
- [x] remove the "metered" crate now that we save aggregate queries?
- [x] don't use systemtime. use Jiff
- [x] graceful shutdown
  - [x] frontend needs to shut down first. this will stop serving requests on /health and so new requests should quickly stop being routed to us
  - [x] when frontend has finished, tell all the other tasks to stop
- [x] period_datetime should always round to the start of the minute. this will ensure aggregations use as few rows as possible
- [x] weighted random choice should still prioritize non-archive servers
    - maybe shuffle randomly and then sort by (block_limit, random_index)?
    - maybe sum available_requests grouped by archive/non-archive. only limit to non-archive if they have enough?
- [x] if we subscribe to a server that is syncing, it gives us null block_data_limit. when it catches up, we don't ever send queries to it. we need to recheck block_data_limit
- [x] add a "backup" tier that is only used if balanced_rpcs has "no servers synced"
  - use this tier to check timestamp on latest block. if we are behind that by more than a few seconds, something is wrong
- [x] config parsing is strict right now. this makes it hard to deploy on git push since configs need to change along with it
  - changed to only emit a warning if there is an unknown configuration key
- [x] make the "not synced" error more verbose
- [x] short lived cache on /health
- [x] cache /status for longer
- [x] sort connections during eth_sendRawTransaction
- [x] block all admin_ rpc commands
- [x] remove the "metered" crate now that we save aggregate queries?
- [x] add archive depth to app config
- [x] improve "archive_needed" boolean. change to "block_depth"
- [x] keep score of new_head timings for all rpcs
- [x] having the whole block in /status is very verbose. trim it down
- [x] maybe we shouldn't route eth_getLogs to syncing nodes. serving queries slows down sync significantly
  - change the send_best function to only include servers that are at least close to fully synced
- [-] proxy mode for benchmarking all backends
- [-] proxy mode for sending to multiple backends
- [-] add configurable size limits to all the Caches
  - instead of configuring each cache with MB sizes, have one value for total memory footprint and then percentages for each cache
  - https://github.com/moka-rs/moka/issues/201
- [x] all anyhow::Results need to be replaced with FrontendErrorResponse. 
    - [x] rename FrontendErrorResponse to Web3ProxyError
    - [x] almost all the anyhows should be Web3ProxyError::BadRequest
    - as is, these errors are seen as 500 errors and so haproxy keeps retrying them
- [ ] have the healthcheck get the block over http. if it errors, or doesn't match what the websocket says, something is wrong (likely a deadlock in the websocket code)
- [ ] has_block_data is too simple. it needs to know what kind of data is being requested
  - all nodes have all blocks
  - most nodes have all receipts
  - only archives have old state
- [x] don't use new_head_provider anywhere except new head subscription
- [x] add support for http basic auth
- [ ] a **lot** got done that wasn't included in this todo list. go through commits and update this
- [ ] eth_sendRawTransaction should only forward if the chain_id matches what we are running
- [ ] rename "private" to "mev protected" to avoid confusion about private transactions being public once they are mined
- [-] writes to median_request_latency should be handled by a background task so they don't slow down the request
- [ ] keep re-broadcasting transactions until they are confirmed
- [ ] if mev protection is disabled, we should send to *both* balanced_rpcs *and* private_rps
- [x] if mev protection is enabled, we should sent to *only* private_rpcs
- [ ] web3rpc configs should have a max_concurrent_requests
    - will probably want a tool for calculating a safe value for this. too low and we could kill our performance
- [ ] rename "concurrent" requests to "parallel" requests
- [ ] setting request limits to None is broken. it does maxu64 and then internal deferred rate limiter counts try to *99/100
- [ ] during shutdown, mark the proxy unhealthy and send unsubscribe responses for any open websocket subscriptions
- [ ] setting request limits to None is broken. it does maxu64 and then internal deferred rate limiter counts overflows when it does to `x*99/100`
- [ ] during shutdown, send unsubscribe responses for any open websocket subscriptions
- [ ] some chains still use total_difficulty. have total_difficulty be used only if the chain needs it
  - if total difficulty is not on the block and we aren't on ETH, fetch the full block instead of just the header
  - if total difficulty is set and non-zero, use it for consensus instead of just the number
- [ ] need debounce on reconnect. websockets are closing on us and then we reconnect twice. locks on ProviderState need more thought
- [ ] having the whole block in /status is very verbose. trim it down
- [ ] don't use systemtime. use Jiff
- [ ] soft limit needs more thought
    - it should be the min of total_sum_soft_limit (from only non-lagged servers) and min_sum_soft_limit
    - otherwise it won't track anything and will just give errors.
    - but if web3 proxy has just started, we should give some time otherwise we will thundering herd the first server that responds
- [ ] connection pool for websockets. use tokio-tungstenite directly. sonic_rs is enough for our raw requests
    - this should also get us closer to being able to do our own streaming json parser where we can 
- [ ] figure out if "could not get block from params" is a problem worth logging
    - maybe it was an ots request?
- [ ] implement filters
- [ ] implement remaining subscriptions
    - would be nice if our subscriptions had better gaurentees than geth/erigon do, but maybe simpler to just setup a broadcast channel and proxy all the respones to a backend instead
- [ ] tests should use `test-env-log = "0.2.8"`
- [ ] eth_sendRawTransaction should only forward if the chain_id matches what we are running
- [ ] weighted random choice should still prioritize non-archive servers
    - maybe shuffle randomly and then sort by (block_limit, random_index)?
    - maybe sum available_requests grouped by archive/non-archive. only limit to non-archive if they have enough?
- [ ] flamegraphs show 25% of the time to be in moka-housekeeper. tune that
- [ ] config parsing is strict right now. this makes it hard to deploy on git push since configs need to change along with it
- [ ] refactor so configs can change while running
  - this will probably be a rather large change, but is necessary when we have autoscaling
  - create the app without applying any config to it
  - have a blocking future watching the config file and calling app.apply_config() on first load and on change
  - work started on this in the "config_reloads" branch. because of how we pass channels around during spawn, this requires a larger refactor.
- [ ] have a test that runs ethspam and versus
- [ ] status page show git hash of running version
- [ ] unbounded queues are risky. add limits
- [ ] after running for a while, https://eth-ski.llamanodes.com/status is only at 157 blocks and hashes. i thought they would be near 10k after running for a while
    - adding uptime to the status should help
    - i think this is already in our todo list
- [ ] emit stdandard deviation?
- [ ] emit global stat on retry
- [ ] emit global stat on no servers synced
- [ ] emit global stat on error (maybe just use sentry, but graphs are handy)
  - if we wait until the error handler to emit the stat, i don't think we have access to the authorized_request
- [ ] somehow the proxy thought latest was hours behind. need internal health check that forces reconnect if this happens
- [ ] BUG: i think if all backend servers stop, the server doesn't properly reconnect. It appears to stop listening on 8854, but not shut down.
- [ ] from what i thought, /status should show hashes > numbers!
  - but block numbers count is maxed out (10k)
  - and block hashes count is tiny (83)
  - what is going on? when the server fist launches they are in sync
  - [ ] related BUG? WARN web3_proxy::rpcs::blockchain: Missing connection_head_block in block_hashes. Fetching now connection_head_hash=0x4b7a…14b5 conn_name=local_erigon_alpha_archive rpc=local_erigon_alpha_archive
  - i see this a lot more than expected. why is it happening so much? better logs needed
- [ ] after adding semaphores (or maybe something else), CPU load seems a lot higher. investigate
- [ ] proper support for Finalized and Safe block queries
- [ ] geth sometimes gives an empty response instead of an error response. figure out a good way to catch this and not serve it
- [ ] Limited throughput during high traffic
- [ ] instead of Option<...> in our frontend function signatures, use result and then the try operator so that we get our errors wrapped in json
- [ ] script that looks at config and estimates max memory used by caches
- [ ] favicon
  - eth_1       | 2022-09-07T17:10:48.431536Z  WARN web3_proxy::jsonrpc: forwarding error err=nothing to see here
  - use the one on https://staging.llamanodes.com/
- [ ] warn if no servers have transaction subscriptions
    - [ ] if no servers have transaction subscriptions, and a user tries to subscribe, make sure the error is user friendly
- [ ] eth_subscribe logs (https://geth.ethereum.org/docs/rpc/pubsub)
- [ ] write a function for receipts that tries balanced_rpcs and only if they all error should it try private relays
  - [ ] automatic retries with a timeout or until all the servers have been tried.
    - i had the websocket die on me in the middle of a long test. only one in-flight request failed because of it. the rest delayed. figure out how to catch these ones since websocket fails sadly seem common
- [ ] nice output when cargo doc is run
- [ ] cache more block metadata locally
- [ ] stats when forks are resolved (and what chain they were on?)
- [ ] Only subscribe to transactions when someone is listening and if the server has opted in to it
- [ ] When sending eth_sendRawTransaction, retry errors
- [ ] If we need an archive server and no servers in sync, exit immediately with an error instead of waiting 60 seconds
- [ ] when handling errors from axum parsing the Json...Enum in the function signature, the errors don't get wrapped in json. i think we need a axum::Layer
- [ ] don't "unwrap" anywhere. give proper errors
- [ ] handle log subscriptions
- [ ] relevant erigon changelogs: add pendingTransactionWithBody subscription method (#5675)
- [ ] make sure all our responses follow the spec: https://www.jsonrpc.org/specification#examples
- [ ] min_sum_soft_limit should be automatic based on the app's average rps plus a buffer.
  - [ ] add a rate counter to the balanced_rpcs
  - [ ] every time a block is found, update min_sum_soft_limit
  - [ ] add a min_sum_soft_limit_safety
      - keeps the automaticly calculated limit from going so high that we stop serving requests
  - [ ] add a min_sum_soft_limit_max_wait that advances the consensus block even if mins not met yet
- [ ] a script for load testing a server and calculating its hard and soft limits
- [ ] use https://github.com/dherman/esprit or similar to parse https://github.com/DefiLlama/chainlist/blob/main/constants/extraRpcs.js
- [ ] update example.toml
- [ ] i'm seeing a bunch of errors with eth_getLogs.
    - i think maybe my block number rewriting is causing problems. but maybe its just a user doing bad queries
- [ ] Use "is_fresh" instead of our atomic bool
    - moka 0.10 - Add entry and entry_by_ref APIs to sync and future caches (#193):
        They allow users to perform more complex operations on a cache entry. At this point, the following operations (methods) are provided:
            or_default
            or_insert
            or_insert_with
            or_insert_with_if
            or_optionally_insert_with
            or_try_insert_with
        The above methods return Entry type, which provides is_fresh method to check if the value was freshly computed or already existed in the cache.
- [ ] lag message always shows on first response
    - http interval on blastapi lagging by 1!
- [ ] change scoring for rpcs again. "p2c ewma"
  - [ ] weighted random sort: (soft_limit - ewma active requests * num web3_proxy servers)
    - 2. soft_limit
  - [ ] pick 2 servers from the random sort.
    - [ ] exponential weighted moving average for block subscriptions of time behind the first server (works well for ws but not http)

## V2

These are not ordered. I think some rows also accidently got deleted here. Check git history.

- [ ] less Arc (and more pin?). we use arcs on a lot of things where i think a &self should work fine.
- [ ] if a rpc fails to connect at start, retry later instead of skipping it forever (need config hot reloads first)
- [ ] automated soft limit
  - look at average request time for getBlock? i'm not sure how good a proxy that will be for serving eth_call, but its a start
  - https://crates.io/crates/histogram-sampler
- [ ] interval for http subscriptions should be based on block time. load from config is easy, but better to query. currently hard coded to 13 seconds
- [ ] check code to keep us from going backwards. maybe that is causing outages
- [ ] min_backup_rpcs seperate from min_synced_rpcs

in another repo: event subscriber
  - [ ] cli tool that support can run to manually check and submit a transaction

## "Maybe some day" and other Miscellaneous Things

- [ ] eth_getBlockByNumber and similar calls served from the block map
  - will need all Block<TxHash> **and** Block<TransactionReceipt> in caches or fetched efficiently
  - after looking at my request logs, i think its worth doing this. no point hitting the backends with requests for blocks multiple times. will also help with cache hit rates since we can keep recent blocks in a separate cache
- [ ] Public bsc server got “0” for block data limit (ninicoin)
- [ ] Advanced load testing scripts so we can find optimal cost servers 
  - [ ] benchmarks from https://github.com/llamafolio/llamafolio-api/
  - [ ] benchmarks from ethspam and versus
  - [ ] benchmarks from other things
  - [ ] quick script that calls all the curve-api endpoints once and checks for success, then calls wrk to hammer it
    - [ ] https://github.com/curvefi/curve-api
    - [ ] test /api/getGaugesmethod
        - usually times out after vercel's 60 second timeout
        - one time got: Error invalid Json response ""
- [ ] page that prints a graphviz dotfile of the blockchain
- [ ] search for all the "TODO" and `todo!(...)` items in the code and move them here
- [ ] add the backend server to the header?
- [ ] have a low-latency option that always tries at least two servers in parallel and then returns the first success?
  - this doubles our request load though. maybe only if the first one doesn't respond very quickly? 
- [ ] zero downtime deploys
- [ ] are we using Acquire/Release/AcqRel properly? or do we need other modes?
- [ ] use https://github.com/ledgerwatch/interfaces to talk to erigon directly instead of through erigon's rpcdaemon (possible example code which uses ledgerwatch/interfaces: https://github.com/akula-bft/akula/tree/master)
- [ ] subscribe to pending transactions and build an intelligent gas estimator
- [ ] flashbots specific methods
  - [ ] flashbots protect fast mode or not? probably fast matches most user's needs, but no reverts is nice.
- [ ] i saw "WebSocket connection closed unexpectedly" but no log about reconnecting
  - need better logs on this because afaict it did reconnect
- [ ] better document load tests: docker run --rm --name spam shazow/ethspam --rpc http://$LOCAL_IP:8544 | versus --concurrency=100 --stop-after=10000 http://$LOCAL_IP:8544; docker stop spam
- [ ] if the call is something simple like "symbol" or "decimals", cache that too. though i think this could bite us.
- [ ] add a subscription that returns the head block number and hash but nothing else
- [ ] if chain split detected, what should we do? don't send transactions?
- [ ] archive check works well for local servers, but public nodes (especially on other chains) seem to give unreliable results. likely because of load balancers.
  - [x] configurable block data limit until better checks
- [ ] https://docs.rs/derive_builder/latest/derive_builder/
- [ ] Detect orphaned transactions
- [ ] https://crates.io/crates/reqwest-middleware easy retry with exponential back off
  - Though I think we want retries that go to other backends instead
- [ ] Some of the pub things should probably be "pub(crate)"
- [ ] Maybe storing pending txs on receipt in a dashmap is wrong. We want to store in a timer_heap (or similar) when we actually send. This way there's no lock contention until the race is over.
- [ ] Support "safe" block height. It's planned for eth2 but we can kind of do it now but just doing head block num-3
- [ ] Archive check on BSC gave “archive” when it isn’t. and FTM gave 90k for all servers even though they should be archive
- [ ] stats for "read amplification". how many backend requests do we send compared to frontend requests we received?
- [ ] fully test retrying when "header not found"
  - i saw "header not found" on a simple eth_getCode query to a public load balanced bsc archive node on block 1
- [ ] weird flapping fork could have more useful logs. like, howd we get to 1/1/4 and fork. geth changed its mind 3 times?
  - should we change our code to follow the same consensus rules as geth? our first seen still seems like a reasonable choice
  -  other chains might change all sorts of things about their fork choice rules
    2022-07-22T23:52:18.593956Z  WARN block_receiver: web3_proxy::connections: chain is forked! 1 possible heads. 1/1/4 rpcs have 0xa906…5bc1 rpc=Web3Rpc { url: "ws://127.0.0.1:8546", data: 64, .. } new_block_num=15195517
    2022-07-22T23:52:18.983441Z  WARN block_receiver: web3_proxy::connections: chain is forked! 1 possible heads. 1/1/4 rpcs have 0x70e8…48e0 rpc=Web3Rpc { url: "ws://127.0.0.1:8546", data: 64, .. } new_block_num=15195517
    2022-07-22T23:52:19.350720Z  WARN block_receiver: web3_proxy::connections: chain is forked! 2 possible heads. 1/2/4 rpcs have 0x70e8…48e0 rpc=Web3Rpc { url: "ws://127.0.0.1:8549", data: "archive", .. } new_block_num=15195517
    2022-07-22T23:52:26.041140Z  WARN block_receiver: web3_proxy::connections: chain is forked! 2 possible heads. 2/4/4 rpcs have 0x70e8…48e0 rpc=Web3Rpc { url: "http://127.0.0.1:8549", data: "archive", .. } new_block_num=15195517
  - [ ] threshold should check actual available request limits (if any) instead of just the soft limit
- [ ] better error handling. we warn too often for validation errors and use the same error code for most every request
- [ ] use &str more instead of String. lifetime annotations get really annoying though
- [ ] tarpit instead of reject requests (unless theres a lot)
- [ ] archive servers should be lowest priority
- [ ] docker build context is really big. we must be including target or something
- [ ] this query always times out, but erigon can serve it quickly: `curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"debug_traceBlockByNumber","params":["latest"],"id":1}' 127.0.0.1:8544' 127.0.0.1:8544`
  {"jsonrpc":"2.0","id":null,"error":{"code":-32099,"message":"deadline has elapsed"}}
  - [ ] figure out rate limits for private rpcs. eden v1 gives 500 error instead of a code for rate limits
- [ ] https://gitlab.com/moka-labs/tiered-cache-example
- [ ] web3connection3.block(...) might wait forever. be sure to do it safely
- [ ] search for all "todo!"
- [ ] when using a bunch of slow public servers, i see "no servers in sync" even when things should be right
  - maybe iterate connection heads by total weight? i still think we need to include parent hashes
- [ ] i see "No block found" sometimes for a single server's block. Not sure why since reads should happen after writes
- [ ] better handling for offline http servers
  - if we get a connection refused, we should remove the server's block info so it is taken out of rotation
- [ ] how should we handle reverting transactions? they won't confirm for a while after we send them
- [ ] Wrapping extractors in Result makes them optional and gives you the reason the extraction failed
- [ ] need a status page for your wallet's rpc. show head block information with age
- [ ] replace sonic_rs::Value with https://lib.rs/crates/ijson (more memory efficient)
- [ ] failsafe. if no blocks or transactions in some time, warn and reset the connection
- [ ] having tons of worker threads can actually make us slower if they keep waking to steal work from eachother. need benchmarks
- [ ] change the wrk data to log requests and errors to a file
- [ ] sentry profiling
- [ ] support alchemy_minedTransactions
- [ ] we need to use docker-compose's proper environment variable handling. because now if someone tries to start dev containers in their prod, remove orphans stops and removes them
- [ ] some third party rpcs have limits on the size of eth_getLogs. include those limits in server config
- [ ] request timeout messages should include the request id
- [ ] have an upgrade tier that queries multiple backends at once. returns on first Ok result, collects errors. if no Ok, find the most common error and then respond with that
- [ ] include tier in the head block logs?
- [x] i think i use FuturesUnordered when a try_join_all might be better
- [ ] since we are read-heavy on our configs, maybe we should use a cache
  - "using a thread local storage and explicit types" https://docs.rs/arc-swap/latest/arc_swap/cache/struct.Cache.html
- [ ] tests for config reloading
- [ ] use pin instead of arc for a bunch of things?
  - https://fasterthanli.me/articles/pin-and-suffering
- [ ] calculate archive depth automatically based on block_data_limits 
- `[2023-04-11T05:40:33Z ERROR websocket backend] Failed to deserialize message e=invalid type: null, expected u64 at line 1 column 26`
  - "Post http://127.0.0.1:8544: net/http: request canceled (Client.Timeout exceeded while awaiting headers)"
  - probably need to have a max count on how long we wait for a response
- [ ] do we need lto = true? is that the default on release?
- [x] we want rate limits based on request latency instead of head latency. low head latency already increases the chance that the server will be seen
- [x] server selection isn't picking lagged archive servers correctly
- [x] sending an empty block on disconnect is bad. the rpc name is used as a key instead of the arc. so the new connection's block is cleared
