# Security and operational boundaries

## Implemented controls

- Parameterized asyncpg statements separate SQL structure from untrusted values. Even quotes and SQL-looking text in item names remain data. Dynamic filter SQL uses fixed column/operator constants only; no API accepts raw SQL.
- Input schemas forbid unknown properties, bound lengths/IDs, require integer quantities, reject negative/nonfinite/overprecision prices, and restrict SKU characters. Model errors omit submitted input and internal exception contexts.
- Pure ASGI middleware enforces a body-size limit on every chunk, including missing or misleading Content-Length, and a ten-second receive deadline. CLI Uvicorn also bounds WebSocket messages and queues.
- Per-IP rate limits protect HTTP routes and WebSocket handshakes; WebSocket connection capacity and send deadlines bound resources. Probes remain available under load.
- Optional bearer authentication protects application HTTP/WebSocket traffic in development; production requires distinct random API/admin secrets of at least 32 characters. Cache administration is denied unless an admin token is configured and supplied. Constant-time byte comparisons avoid token timing comparisons and reject non-ASCII header garbage safely.
- Request IDs are character/length bounded; paths use escaped log formatting. Bodies, tokens, database URLs, and exception tracebacks are not emitted in API error responses. Credentials are not logged by CLI error handling.
- Every command is transactional; SQL constraints, unique keys, and event immutability triggers protect data integrity. The application container runs as a non-root user. Compose only publishes the API on loopback and keeps PostgreSQL on its private network.
- Runtime dependencies are pinned in `requirements.txt`; development dependencies are pinned separately. CI checks tests, formatting, lint, and a vulnerability audit. Audit findings reflect the advisory database at the time of the check, not a guarantee against undiscovered issues.

## Credentials and database roles

`.env` and runtime secret files are ignored. `.env.example` contains only blank tokens and a password-free local DSN. Never commit secrets: repository history, forks, build logs, and caches can retain them after deletion. If exposed, revoke/rotate first, then remove them from retained artifacts.

Migrate using an owner role, then use a restricted runtime login for API and worker. Run `scripts/grant_runtime.sql` as the database owner to create a NOLOGIN permission group. Create the actual login with your secret manager or `createuser --pwprompt`, then grant it that group. Point runtime `DATABASE_URL` at that login. The runtime role gets no event UPDATE/DELETE/TRUNCATE, no schema DDL, and no migration writes. Do not run production processes as a PostgreSQL superuser/owner: those roles can bypass triggers and grants. Migrations themselves are trusted, checksum-verified repository files.

For production connections require verified TLS according to the driver/server deployment configuration (`sslmode=verify-full` and a trusted server CA). Terminate HTTPS/WSS at a managed ingress. The sample Compose configuration is local development infrastructure, not an internet-facing deployment manifest.

## Deployment responsibilities

The service-token model identifies a trusted application client, not individual users or tenants. Add OIDC/service identities, item/tenant permissions, token rotation, and audit access controls before exposing sensitive inventory to independent customers. WebSocket authentication uses the Authorization header; native browser WebSocket clients cannot set it. A browser deployment needs a carefully scoped cookie or short-lived handshake-token design; long-lived tokens in query strings are intentionally unsupported.

The in-memory limiter cannot enforce a global quota across multiple API processes: one client could receive approximately N times its limit across N processes. NAT users share a quota. Untrusted forwarded IP headers must never become the identity source; CLI Uvicorn disables proxy-header interpretation. Configure exact trusted ingress addresses when deploying behind a proxy. Add distributed rate limits and edge DDoS/body/header/time limits for production.

Keep PostgreSQL private, use separate databases for tests/benchmarks, encrypt backups, enable PITR, and rehearse restores. Restrict metrics/admin endpoints at ingress. Enforce connection budgets, maximum HTTP concurrency, and file-descriptor limits appropriate to your host. Benchmark failure injection is prohibited by production settings and disabled by default.

## Test evidence

The suite checks SQL-looking values, invalid quantities/prices/IDs, oversized streamed bodies, admin/API authorization, rolling rate limits, WebSocket authorization/capacity, actual PostgreSQL statement failure, both transaction fault boundaries, event mutation rejection, worker rollback, and replay drift detection. These tests demonstrate specific safeguards; they are not a penetration test or formal security proof.
