# Production application image

Use `docker/production.Dockerfile` for the RPC proxy and standalone block relay.
It starts no Ethereum clients. Its checks compile all targets but run only the
mock-only library tests. Do not use the root development Dockerfile for this
deployment; that build runs node-launching integration tests.

The workflow publishes Linux AMD64 images tagged with the source commit and
records their immutable digest. Use the digest in private Compose files. Do not
use a moving tag for production or rollback. The image uses portable x86-64
instructions, a non-root user, and an exec-form entrypoint.

The build context allows only source, manifests, the toolchain, and public test
fixtures. It excludes endpoint lists, `.env`, credentials, and Git metadata.
Supply runtime configuration and JWT files through read-only mounts. Give each
relay a persistent, private `state_dir` owned by UID 10001. Keep proxy and relay
containers separate; neither service depends on the other's readiness.

The proxy listens on container port 8544. The relay's status listener defaults
to loopback port 18550; set `block_relay --status-address 0.0.0.0:18550` inside
its container and publish that host port on loopback only. Never expose Engine
ports, relay status, or private measurements on the public Internet.

Use `stop_grace_period: 30s`. Both commands have a 25-second application drain
and two-second runtime cleanup budget. Proxy shutdown marks readiness false,
stops listeners, drains active HTTP requests, and closes WebSockets with code
1001. Clients must reconnect after a host update. An unresolved Engine request remains unknown. It does not suspend newer work.
Use `/live` for forwarder container startup and report dependency health separately.

Validate the selected image on each host before changing traffic. Keep the
previous digest and private configuration for rollback. A built image does not
prove that block delivery is faster. Measure full-fleet latency during broadcast.
