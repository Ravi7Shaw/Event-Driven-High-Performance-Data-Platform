# Requirement and verification map

| Assignment areas | Implementation | Evidence |
|---|---|---|
| 1–5, 42, 44: stack, inventory, schema, indexes, configuration | `config.py`, `models.py`, `db.py`, SQL migration, package manifests | PostgreSQL integration tests, repeated transactional migration, container startup |
| 6–13: idempotency, commands, versions, atomic events | `repository.py`, API routes, DB constraints | concurrent duplicate/create/reserve/release tests; both fault boundaries and actual DB error rollback |
| 14–15, 35: durable worker, retries, projection | `worker.py`, `event_jobs`, `inventory_activity` | concurrent workers, crash rollback, poison jobs/requeue, out-of-order processing, 11,000-event drain |
| 16–17: cursor API and replay | `pagination.py`, `replay.py`, repository queries | snapshot pagination/filter validation, 1,000-item replay, drift detection |
| 18–20, 33: LRU/TTL, stampedes, stale detection, ETags | `cache.py`, GET route | TTL/LRU/invalidation tests, cross-process-style stale update, 1,000 misses/one fetch, weak/list/star ETags |
| 21–23, 34: WebSockets and reconnect replay | `websocket.py` | real server multi-subscriber/replay/live race tests, heartbeat, disconnect, injected broken socket, slow-send isolation, auth/capacity |
| 24–27, 43: rate limits, correlation, metrics, probes, logs | middleware, limiter, metrics, worker logging | concurrent429 checks, request-ID errors, readiness outage, observable counters |
| 28–30, 45–46: CLI and deterministic data | `cli.py`, `seed.py` | CLI help/errors, run/migrate/seed/worker/replay/events/stats/cache/benchmark operations; repeat seed |
| 31–36: validation, concurrency, failure tests | `tests/`, `faults.py` | real PostgreSQL suite, failure-rate benchmark |
| 37–39: measured load and analysis | `benchmark.py`, scripts, `benchmarks/`, `PERFORMANCE.md` | required A–D plus injected E; raw measurements and load-generator profile |
| 40, 47–50, 52: architecture, organization, operating docs | README, ARCHITECTURE, docs | separate responsibilities, Docker/native instructions, limitations and engineering decisions |
| 41: security | strict models, ASGI controls, parameterized SQL, protected admin API, runtime role grants, SECURITY | SQL-looking payload, validation/body bounds, auth, permission tests, dependency audit |
| 51: production challenge | `PRODUCTION_DESIGN.md` | concrete migration path for this implementation, scaling/consistency/recovery trade-offs |

Optional Docker support, fault injection, the JSON metrics alternative, and the production design challenge are included. Tests require a disposable PostgreSQL administrative test connection for the full suite. The specification defines create/read/update/reserve/release; no item deletion endpoint is added because it would conflict with retained immutable history without an additional lifecycle design.
