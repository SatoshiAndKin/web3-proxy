# Wallet-compatible RPC through Tailscale Funnel

An optional, standalone HTTP JSON-RPC gateway for an existing private web3-proxy
instance. It is not part of any live deployment. Funnel makes its listener
internet-reachable; the gateway (not Tailscale identity or Tailnet Lock)
authenticates callers. Leave the normal RPC URL, transaction path, and tailnet
policy unchanged. Start with one chain.

## Deploy on the Funnel node

1. Choose the existing private **HTTP JSON-RPC** upstream on this node. Set
   `RPC_UPSTREAM` in a local `.env` file. For a host endpoint already reachable
   from Docker's bridge, the URL can use `host.docker.internal:<private-port>`.
   The host alias does not make a host's 127.0.0.1 listener reachable from the
   container. If the upstream is in Docker, attach the gateway to its private
   network and use the service address instead. Do not rebind an existing
   private upstream to the public interface to make this work.
2. Generate a separate 256-bit token for each person/device in a trusted terminal:

   ```sh
   node --input-type=module -e '
     import { randomBytes, createHash } from "node:crypto";
     const token = randomBytes(32).toString("base64url");
     console.log("Client token:", token);
     console.log("Server SHA-256:", createHash("sha256").update(token).digest("hex"));
   '
   ```

   Put only the hex digests in `rpc-token-hashes.json` (a JSON array of strings).
   The container user must be able to read this mounted file. Never commit the
   client token or send it in a public channel. Give each recipient only their
   own URL or token through a secure channel. For revocation, remove the digest
   and run `docker compose up -d --force-recreate` from this directory.
3. For browser clients that send an Origin header, configure the exact
   allowed origins in `RPC_ORIGINS` as a comma-separated list. Leave unset for
   native clients that do not send Origin. Use the wallet's actual origin,
   which may be an extension origin rather than the dapp origin.
4. With a current Docker Engine (28+ for reliable loopback-only port
   publishing), run `docker compose up -d --build`. Verify the gateway port
   is bound on host loopback only. First verify an unauthenticated local
   `POST /rpc` returns 401, then make an authenticated `eth_chainId` request
   and confirm its value. No public listener is enabled by these steps.
5. Have the Tailnet admin check the node's existing Serve/Funnel config for
   a listener conflict and authorize Funnel for the intended node tag only.
   With the default local port and an otherwise unused external HTTPS port:

   ```sh
   tailscale funnel --bg --https=443 http://127.0.0.1:18745
   tailscale funnel status
   ```

   Use the HTTPS hostname printed by Tailscale. For rollback, turn off this
   listener with `tailscale funnel --bg --https=443 off`, then stop the gateway.
   Do not use a blanket reset on a node with other listeners.

## Client access

- Header-capable clients: `https://<node>.<tailnet>.ts.net/rpc`, with
  `Authorization: Bearer <token>`.
- URL-only wallets: `https://<node>.<tailnet>.ts.net/rpc/<token>`. The URL is
  itself a bearer credential. Anyone who sees it can use it until revoked.
  Wallet exports, screen recordings and client logs can leak it. Prefer the
  header for scripts and never add the token to query parameters. The gateway
  does not log requests; do not enable access logging of request URLs, headers,
  or bodies in other local layers.
- HTTPS POST with JSON only; no WebSocket, remote signing, trace/debug/admin,
  or transaction broadcast, even through batches. Wallets can read balances
  and prepare transaction fees, but must use a different RPC endpoint to
  submit. Heavy queries and bursts are deliberately bounded.
- Use the same endpoint for both arms of any cloud A/B benchmark. Funnel
  has a bandwidth cap and is not a measurement of local RPC latency. Retain
  a private upstream path for production and throughput measurements. By
  default each credential gets 120 weighted units per minute (48 burst),
  with at most eight simultaneous upstream requests across all credentials.
  If a dedicated upstream has room, tune the rate, burst, and concurrency
  in the local `.env` via `RPC_RATE_UNITS_PER_MINUTE`,
  `RPC_BURST_UNITS`, and `RPC_MAX_INFLIGHT`. Raise them cautiously: the
  limits protect the existing upstream and Funnel has its own ceiling.

See [Tailscale Funnel requirements and limitations](https://tailscale.com/docs/features/tailscale-funnel)
and [the Funnel command reference](https://tailscale.com/docs/reference/tailscale-cli/funnel).
Run the local integration suite with `node --test gateway.test.mjs`.
