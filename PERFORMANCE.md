# Measured performance and engineering analysis

## Environment and method

Measured on 2026-09-30 in an Amazon Linux 2023 cloud sandbox with four available CPU cores and an 8 GiB memory limit. Python 3.12.14, PostgreSQL 16.14, asyncpg 0.31.0, HTTPX 0.28.1; exact runtime pins are in `requirements.txt`. The database used a local Unix socket with normal PostgreSQL durability settings. The API and load generator used actual loopback HTTP/WebSocket connections, sharing one Python asyncio loop; PostgreSQL ran separately. This is a reproducible functional load experiment, not an isolated production capacity certification.

Commands and machine-readable results are in `benchmarks/`. Run `python scripts/run_experiments.py` against a migrated, disposable database to reproduce A–E. Each experiment creates a fresh `BENCH-*` item. A/B target one hot item, begin with a cold application cache, use 10,000 requests and 100 concurrent clients, and have identical rate limits and fixtures. Each simulated client owns one persistent HTTP connection. HTTP client construction and fixture creation occur before timing; requests include connection establishment and response reading. The embedded worker is disabled in each temporary benchmark API. The rate limit is raised to 1,000,000/60 seconds so 429s do not mask inventory/cache performance.

Other lightweight task processes existed in this sandbox. Results are single runs, not confidence intervals. The hotspot workload maximizes coalescing and is not representative of a uniform million-item working set. The native preview's worker could drain committed fixture events independently; this does not change command correctness but is another reason not to infer isolated peak throughput.

## Required experiments

| Experiment | Requests / clients | Success / expected conflict | Duration | RPS | P50 | P95 | P99 | Item/version DB queries |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| A: cache disabled | 10,000 / 100 | 10,000 / 0 | 14.331 s | 697.80 | 139.68 ms | 191.26 ms | 222.88 ms | 10,000 |
| B: cache enabled (512 entries, TTL 30s) | 10,000 / 100 | 10,000 / 0 | 9.740 s | 1,026.74 | 90.74 ms | 128.26 ms | 139.37 ms | 100 |
| C: reserve two units from stock 100 | 100 / 100 | 50 / 50 | 0.258 s | 387.11 | 198.01 ms | 228.28 ms | 229.64 ms | 0* |

`db_queries` counts physical item/version SELECTs, not all SQL in a command. C's zero therefore **does not** mean zero database operations: each mutation attempts a transaction, idempotency lookup, and locked item read. The dedicated counter is intended to compare GET experiments.

**D — WebSocket fan-out:** 100 clients connected before 100 item updates. All **10,000/10,000** expected event deliveries arrived in version order, with **0 failed connections**, in **5.220 seconds** including connection establishment and event generation. Heartbeats are excluded from message counts. The fixture's creation event was excluded with `after_version=1`. Every benchmark fixture passed independent replay verification afterward.

**E — injected failures:** 100 concurrent two-unit reservations with a 0.05 probability at each transaction fault point produced 50 successful responses, 7 safe 503 responses, and 43 expected 409 conflicts. Final reserved stock was exactly 100; replay was consistent. The 0.05 setting is per fault point, not a promise that exactly 5% of requests fail. Failed requests did not consume inventory or emit orphan events. The benchmark counts failures and does not automatically retry requests; idempotent retry is verified separately by tests.

## Cache behavior

B delivered about **47.1% more RPS**, reduced P95 by about **32.9%**, and reduced physical item reads by **99%**. The 10,000 requests formed 100 coalesced flights: one miss/full fetch followed by 99 hits/version checks. Its reported hit ratio is 99% of flights; it is not a claim that every HTTP request performed an independent cache lookup. There were no TTL expirations or evictions in this short, single-item run.

The cache deliberately pays for primary-version validation on hits. This catches stale entries produced by another process or a racing old fill. TTL-only caching would lower database load further while weakening correctness. On disjoint keys or sequential reads, single flight provides much less benefit; a uniform-key benchmark and longer expiry/churn test are required before sizing a real cache. LRU/TTL correctness and 1,000 simultaneous cold requests causing exactly one DB read are covered independently by tests.

## Finding and correcting a measurement bottleneck

The first A/B runs shared one HTTPX client across 100 request tasks. They measured only 141.83/209.08 RPS with very long tails. Those results are preserved under `benchmarks/baseline/`. Increasing the shared pool's keepalive capacity produced 168.60/203.63 RPS (`benchmarks/shared_pool/`): cache-enabled performance was slightly **worse**, so increasing retained connections was not a reliable optimization.

A diagnostic cProfile run of 1,000 cached GETs recorded roughly 21.4 million function calls. HTTPX/httpcore connection assignment consumed 5.36 seconds cumulatively out of 10.07 seconds, with over 3 million idle checks. See `benchmarks/profile_summary.txt`. The profile changes timing and is evidence of where this harness spent CPU, not an additional server-throughput measurement.

Giving each simulated client its own persistent connection removed this shared-pool scanning bottleneck; the final A/B values above use that harness. This improves the validity of the measurement rather than changing inventory semantics. Final timings still include Python HTTP processing, JSON serialization, cache copies, pool waits, and client work on the same loop. Separate load-generator hosts and server-side latency/CPU profiles would be needed to isolate production capacity. Small run-to-run differences in the earlier experiments may also reflect sandbox background activity.

## Database contention and event ordering

C proves bounded inventory under a hotspot; its RPS includes expected 409 rejections and is not sustained write throughput. Item row locks serialize reservations on the same SKU. Increasing request concurrency cannot remove that serialization and instead raises lock/pool wait. Unique idempotency keys avoid duplicating effects; concurrent duplicate keys queue behind the same transaction-scoped advisory lock and return the stored response.

Every mutation also locks the global event clock briefly before inserting its event/job/response. This solves the allocated-before-commit cursor problem, but creates contention even for different items. A distributed production feed needs partition-local commit positions/CDC; simply replacing this clock with a sequence would sacrifice correctness. No cross-item write-saturation claim is made from the reservation benchmark.

## Projection throughput

A separate full-seed verification processed **11,000 events in 2.734 seconds**, approximately **4,024 events/second**, with worker batches of 100. Afterwards all 1,000 item replays were consistent and SQL comparison found zero projection discrepancies. See `benchmarks/seed_verification.json`.

This local worker only updates a PostgreSQL projection; it does no network side effect and benefits from batching. Ten workers are not expected to multiply throughput by ten: item projection locks, WAL/fsync, connection limits, and batch deadlocks can dominate. Poison events consume bounded attempts and move to dead jobs. Tests verify savepoint rollback, cancellation rollback, retry/requeue, out-of-order convergence, and concurrent workers without duplicate projection application.

## WebSocket behavior

Per-connection durable polling simplifies replay/live correctness and isolates slow clients, but polling 100 sockets at 0.1s intervals can produce around1,000 idle event queries/second. The benchmark's `db_queries` value 100 only counts handshake version lookups; it excludes these event queries. Fan-out completion time is bounded by polling, sequential fixture generation, database query work, and socket scheduling. D measures successful delivery rather than a per-message latency distribution.

A slow socket gets its own send timeout instead of blocking other clients. Replay batches are capped at 100, and the database holds the backlog. At larger scale, use one shared item feed per gateway or a durable broker/gateway design; retain version cursors and reconnect deduplication. The current implementation prioritizes durable replay correctness over minimizing idle DB polling.

## Rate limiter overhead and metrics

A 100,000-decision in-process microbenchmark measured approximately **0.454 microseconds per allowed decision** on one IP (`benchmarks/rate_limiter.json`). It excludes HTTP overhead, multiple-IP churn, and rejection logging. The limiter remains enabled in A–E; its high configured quota avoids rejections. Exact rolling-window cleanup is amortized and memory-bounded. With multiple API processes, quotas multiply unless enforcement is centralized.

Latency metrics use a 10,000-observation bounded sample for process p95 and cumulative totals for the average. Benchmark percentiles use measured client latencies and a sorted nearest-index method. The `/metrics` values therefore have a different observation boundary from benchmark values. Process counters are diagnostic; durable job state is authoritative after restarts or rolled-back worker batches.

## Practical next measurements

Before deployment, measure a mixed read/write workload over a working set larger than the cache, longer TTL churn, multiple API processes, cross-item write saturation, worker backlog recovery after downtime, real network latency, and prolonged slow-client reconnect storms. Add PostgreSQL wait-event/pool instrumentation and distributed latency histograms. Use multiple repeated runs with isolated load generators before setting a performance SLO. The current results demonstrate the requested experiments and correctness checks; they do not establish 100,000 requests/second capacity.
