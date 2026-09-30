# EventVault

An asynchronous inventory backend built with Python 3.11+, FastAPI, asyncpg, and PostgreSQL. Every accepted command atomically changes inventory, appends an immutable event, queues projection work, and records its idempotent response. It includes replay, WebSockets, a version-validated LRU/TTL cache, rate limiting, metrics, a CLI, fault injection, and reproducible benchmarks.

## Quick start without Docker

Requirements: Python 3.11+ and PostgreSQL 16+ (tested with Python 3.12 and PostgreSQL 16). Docker is optional.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install --no-deps -e .
cp .env.example .env
```

Create a PostgreSQL database and role using your system's PostgreSQL administration tools. For example, on a local machine with PostgreSQL already running:

```bash
createuser --pwprompt eventvault_owner
createdb --owner=eventvault_owner eventvault
```

Use an administrator connection if your OS user cannot create roles/databases. Set `DATABASE_URL` in `.env` to your PostgreSQL URL, or export it from a secret store. With local Unix-socket authentication, `postgresql:///eventvault` can avoid a password. For TCP, use `postgresql://USER:URL_ENCODED_PASSWORD@HOST:5432/eventvault`. Configure the server's `pg_hba.conf` for SCRAM authentication; do not enable network trust authentication. All application timestamps use UTC; the seed's warehouse is in Jaipur (`Asia/Kolkata`).

```bash
eventvault migrate
eventvault seed
eventvault run                         # http://127.0.0.1:8000/docs
```

An embedded worker runs by default. To operate it separately, set `WORKER_ENABLED=false` for the API and run `eventvault worker` in another terminal. API startup deliberately does not perform privileged schema migrations. `/ready` must pass before routing traffic.

## Docker Compose

Compose creates a private bridge network; API/worker connect to hostname `db`. PostgreSQL has **no published host port**. API binds to host loopback. No host-network or device-specific networking configuration is required.

Generate local secrets, then start:

```bash
export POSTGRES_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_hex(24))')"
export DATABASE_URL="postgresql://eventvault:${POSTGRES_PASSWORD}@db:5432/eventvault"
export ADMIN_TOKEN="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
docker compose up -d --build
docker compose run --rm api eventvault seed
curl http://127.0.0.1:8000/ready
```

Keep the generated values in a private `.env` or secret manager for restarts; changing the password environment variable does not change an existing database volume's password. Use `docker compose down` to stop while retaining data. `docker compose down -v` destroys the database and is only suitable for disposable local data. Set `API_PORT` if host port 8000 is occupied.

If an older Buildx plugin prevents Compose from building, use the verified fallback:

```bash
docker build -t eventvault:local .
docker compose up -d --no-build
```

All three application services share this image. The migration service must succeed before API/worker startup. The container runs as a non-root user. Docker daemon and bridge networking must work on your host; see [setup notes](docs/SETUP.md) for the cloud sandbox setup verified during development.

## API examples

```bash
curl -i http://127.0.0.1:8000/items \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: laptop-create-1' \
  -d '{"sku":"LAPTOP-DELL-001","name":"Engineering Laptop","quantity":100,"unit_price":"74999.00"}'

curl -i http://127.0.0.1:8000/items/1
curl -i http://127.0.0.1:8000/items/1/reserve \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: order-123-reserve' \
  -H 'If-Match-Version: 1' -d '{"quantity":5}'

curl -i -X PATCH http://127.0.0.1:8000/items/1 \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: laptop-update-1' \
  -d '{"description":"16GB RAM laptop"}'

curl -i 'http://127.0.0.1:8000/events?item_id=1&limit=50'
curl -i http://127.0.0.1:8000/items/1 -H 'If-None-Match: "item-1-v3"'
```

When `API_TOKEN` is configured, include `Authorization: Bearer <your token>` on HTTP and WebSocket requests. Interactive OpenAPI docs describe parameter bounds and models. Monetary values are JSON decimal **strings** to avoid precision loss; input accepts exact JSON numbers or decimal strings.

| Endpoint | Purpose |
|---|---|
| `POST /items` | Create item; requires `Idempotency-Key` |
| `GET /items/{id}` | Read authoritative version; ETag/304 |
| `PATCH /items/{id}` | Update name, description, price |
| `POST /items/{id}/reserve` | Reserve positive units |
| `POST /items/{id}/release` | Release reserved units |
| `GET /events` | Filtered cursor pagination (`item_id`, `event_type`, `from`, `to`, `limit`, `cursor`) |
| `WS /ws/items/{id}?after_version=N` | Replay then live events after N |
| `GET /health`, `GET /ready` | Liveness and dependency readiness |
| `GET /metrics`, `GET /stats` | Process metrics and database totals |
| `GET /admin/cache`, `DELETE /admin/cache` | Cache administration; requires `X-Admin-Token` |

Mutations return 409 for insufficient inventory, stale versions, duplicate SKU, or conflicting key reuse; 404 for missing items; 422 for invalid inputs; 429 plus `Retry-After` on overload. Database/injected transient errors return safe 503 responses. Every HTTP response has an `X-Request-ID`, including failures and rate limiting. If a request ID contains unsafe characters or exceeds 128 characters, a new ID is generated.

## CLI

Global flags precede the command. Environment/.env settings apply to every command. For example:

```bash
eventvault --help
eventvault --cache-capacity 1024 --cache-ttl 15 --rate-limit 200 run
eventvault migrate
eventvault seed
eventvault worker
eventvault stats
eventvault cache-stats                   # set ADMIN_TOKEN to match running server
eventvault clear-cache
eventvault events --item-id 42            # streams JSON lines, bounded DB batches
eventvault replay --item-id 42
eventvault verify-item --item-id 42       # same strict replay/invariant verification
eventvault retry-dead                    # explicit recovery after fixing a poison event cause
eventvault benchmark --requests 10000 --concurrency 100
eventvault benchmark --mode reserve --requests 100 --concurrency 100 --failure-rate 0.05
```

`--api-url` selects the process for stats/cache commands. These call the running API because a new CLI process cannot inspect another process's in-memory cache. `--database-url` is available, but prefer `DATABASE_URL` to keep credentials out of process listings/history. Exit codes: 0 success; 1 operational/configuration failure; 2 inconsistent replay or invalid CLI syntax; 130 interrupted. Errors omit credential-bearing exception text. Replay prints reconstructed and actual states, field differences, and `CONSISTENT`/`INCONSISTENT`.

## Configuration

See [.env.example](.env.example) for all settings. Core defaults:

| Setting | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql://localhost/eventvault` | PostgreSQL connection URL; no built-in credentials |
| `APP_ENV` | `development` | `development`, `test`, `production` |
| `LOG_LEVEL` | `INFO` | Request and worker logging verbosity |
| `CACHE_CAPACITY`, `CACHE_TTL` | `512`, `30` | Entries and seconds; capacity 0 disables cache |
| `RATE_LIMIT`, `RATE_LIMIT_WINDOW` | `100`, `60` | Per-IP rolling window |
| `WORKER_BATCH_SIZE`, `WORKER_INTERVAL` | `100`, `0.2` | Claim size and idle polling seconds |
| `WORKER_MAX_ATTEMPTS`, `WORKER_RETRY_BASE` | `5`, `0.5` | Retry budget and exponential delay base |
| `DB_POOL_MIN`, `DB_POOL_MAX` | `2`, `20` | Connections per process |
| `MAX_BODY_BYTES` | `16384` | HTTP request body bound, including streamed bodies |
| `WS_POLL_INTERVAL`, `WS_HEARTBEAT` | `0.1`, `15` | Durable polling and heartbeat seconds |
| `WS_SEND_TIMEOUT`, `WS_MAX_CONNECTIONS` | `5`, `1000` | Slow-client timeout and capacity |
| `API_TOKEN`, `ADMIN_TOKEN` | empty | Optional API auth in development; admin actions disabled without token |
| `FAILURE_RATE` | `0` | Test/development fault probability |

Production requires distinct API/admin tokens of at least 32 characters and forbids fault injection. TLS termination, tenant-level authorization, database role separation, and ingress limits are deployment responsibilities; see [security](SECURITY.md).

## How correctness works

- **Idempotency:** PostgreSQL transaction-scoped advisory lock on a hash of the key, followed by an exact key/fingerprint lookup. The response, item, event, and job commit together. Same key/body/operation returns the original response even after later changes. Different request with the same key returns 409. Failed transactions do not consume keys. Keys are global across the service; do not reuse them between operations. Records are retained indefinitely.
- **Concurrency:** mutations lock the item with `FOR UPDATE`; constraints enforce nonnegative stock and reservations. `If-Match-Version` also detects decisions based on old reads. Database serialization alone does not know which version the client saw.
- **Cache:** bounded LRU with monotonic TTL and single flight. Hits validate the authoritative DB version, including writes by another API process. Cached values are copied for callers. This guarantees freshness for non-overlapping reads/writes but costs a small DB query on a hit. ETags reduce transfer/serialization; they do not bypass freshness validation.
- **Event processing:** workers claim mutable job rows using `FOR UPDATE SKIP LOCKED`. Projection changes and job completion commit in one transaction. A savepoint rolls back failed projection changes, then persists a retry with exponential backoff. Exhausted events become dead jobs; `retry-dead` requeues them. Business events cannot be updated, deleted, or truncated.
- **WebSocket replay:** a per-connection cursor reads events in aggregate version order. The same loop drains historical batches and polls for new events, so there is no separate replay/live handoff race. Send timeouts isolate slow sockets. Clients persist the last applied version and deduplicate after reconnect. No end-to-end exactly-once network delivery is claimed.
- **Failures:** a failed command rolls back all durable effects. Workers retry durable pending jobs after a crash. Cache injection falls back to DB. Socket send failure closes that connection; reconnect recovers events. Broken DB connections make readiness fail and return safe service errors. Request IDs and bounded metrics support diagnosis.

## Tests

Use a dedicated disposable PostgreSQL cluster with an administrative test role; tests create and drop isolated randomly named databases and verify a restricted NOLOGIN permission group. **Do not use production credentials.** Unit tests run without PostgreSQL; integration tests explicitly skip without `TEST_DATABASE_URL`.

```bash
pip install -r requirements-dev.txt
pip install --no-deps -e .
export TEST_DATABASE_URL='postgresql:///eventvault_test_admin'
pytest -q
ruff check eventvault tests scripts
ruff format --check eventvault tests scripts
pip-audit -r requirements.txt --no-deps --disable-pip
```

Create the `eventvault_test_admin` database beforehand (its name is arbitrary). The URL must name a database your test role can connect to; the test databases are separate. The suite exercises real PostgreSQL locking, atomic rollback, concurrent idempotency, oversubscription, releases, optimistic conflicts, cache expiry/stampedes/stale data, worker rollback/retry/dead jobs, concurrent workers, cursor bounds, security, and real Uvicorn WebSocket connections. CI uses an isolated PostgreSQL service.

## Seed and benchmarks

`seed` produces 1,000 items and 11,000 consistent events: 1,000 creations, 3,000 updates, 4,000 reservations of two units, and 3,000 releases of one unit. Every item ends with five reserved units and version 11. Business inputs and idempotency keys are deterministic; timestamps reflect execution time. Repeat execution resumes missing steps and adds no duplicate records. It does not reset later user operations.

```bash
eventvault seed
LOG_LEVEL=ERROR python scripts/verify_dataset.py
LOG_LEVEL=ERROR python scripts/run_experiments.py
```

Run benchmarks in a disposable, migrated database. They create identifiable `BENCH-*` fixtures, start a temporary loopback API, disable its embedded worker, and raise its rate limit to isolate inventory/cache/socket performance. They use actual HTTP and WebSocket connections. `--failure-rate` applies faults inside that temporary server. Existing servers' metrics/settings are unaffected. Raw results are in [benchmarks/](benchmarks/); methodology and measured trade-offs are in [PERFORMANCE.md](PERFORMANCE.md).

## Limitations and next steps

The cache, limiter, WebSocket capacity, and metrics are per process. A single hot item serializes writes. The global event cursor lock also serializes the final portion of all mutations; it preserves lossless pagination at a measurable throughput cost. Socket readers poll PostgreSQL per connection. Projection reads are eventually consistent; dead jobs require an explicit repair/requeue. There is no inventory deletion/restocking endpoint, multi-tenant model, external delivery integration, automated event archival, or idempotency retention policy in the requested scope. The default auth is a service token, not user-level access control.

Before large-scale deployment, implement the concrete changes in [PRODUCTION_DESIGN.md](PRODUCTION_DESIGN.md): partitioned ordering, durable broker, shared cache/rate limits, dedicated WebSocket gateways, capacity testing, backups/PITR, distributed tracing, TLS/identity controls, and operational SLOs. [ARCHITECTURE.md](ARCHITECTURE.md) explains the transaction boundaries and lifecycle designs.
