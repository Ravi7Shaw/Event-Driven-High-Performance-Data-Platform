# EventVault at 20 API processes, 10 workers, and multiple regions

## Start with capacity and consistency contracts

100,000 requests/second and 10 million events/hour are different rates: the latter is about 2,778 durable mutations/second before burst headroom. Establish the read/write mix, number of hot SKUs, payload size, region latency, reconnect rate, retention, and acceptable staleness. The measured local benchmark is not evidence of that target capacity. A single globally hot item cannot safely accept arbitrary concurrent reservations without an ordering/ownership mechanism.

The current implementation has three explicit scale limits: an authoritative DB version read on every cache flight, a global commit-order clock for events, and polling per WebSocket connection. Address them before adding replicas indiscriminately. Twenty APIs with the default 20-connection pools would already request 400 database connections, plus ten worker pools and administration. Budget active database work explicitly; use PgBouncer transaction pooling only after verifying asyncpg prepared-statement behavior and transaction/advisory-lock compatibility.

## Command ownership and PostgreSQL

Keep a strongly consistent owner for each item/warehouse. A reservation's item row, idempotency result, and outbox event must remain in one database transaction on one shard. Route writes by warehouse/item; retain a stable ownership map and fence old owners during moves. Shard idempotency with the command owner and include tenant/operation namespaces, otherwise retries routed to a different shard could duplicate stock changes. Keep optimistic versions for stale client decisions even with distributed infrastructure.

The global `event_clock` cannot span all shards at target scale. Replace the global feed with partition-local commit positions or CDC/WAL positions and explicit per-partition cursors. Promise per-item order, not a fictitious cheap total order across regions. A multi-partition historical cursor is a vector of positions or a stable time-window snapshot, with documented late-arrival handling. Do not replace the clock with BIGSERIAL and silently reintroduce the commit-order pagination bug.

Scale PostgreSQL vertically first, measure lock waits/WAL/fsync/IOPS/pool wait, and partition the large event/outbox tables by time and possibly warehouse. Partitioning complicates global UUID uniqueness and `(aggregate_id, version)` constraints; implement uniqueness in an appropriate unpartitioned registry or route all of an aggregate's events to one partition key strategy. Test query plans for item replay rather than assuming time partitioning helps it. Read replicas can serve lag-tolerant historical queries and projections. They must not validate authoritative ETags or stock availability without a causal/LSN fence because replication lag would permit stale reads.

## Redis caching and distributed rate limiting

A shared Redis cache can reduce repeated reads across 20 APIs, but cache invalidation remains a correctness problem. Publish versioned invalidations only after command commit through the durable outbox/CDC path. Use compare-and-set versions to prevent late fills from overwriting newer values, bounded TTLs, and per-key coalescing/leases. A crash between commit and invalidation still creates a stale window. Choose and advertise a contract: retain primary version validation for strict reads, or offer explicitly bounded-stale reads with primary reads after mutations and on version-sensitive operations. Reservations always validate against primary state.

Move IP/identity quotas to an atomic Redis Lua/scripted rolling-window or token-bucket decision, with edge limits absorbing hostile traffic before application memory is consumed. Partition by tenant/identity plus IP safeguards; NAT alone is not an appropriate customer quota. Decide fail-open for low-risk reads versus fail-closed for mutations/admin when the rate-limit service is unavailable. Redis loss must not lose durable inventory or idempotency state.

## Durable broker and ten workers

Use Kafka (or an equivalently durable partitioned log) for event distribution, analytics consumers, and WebSocket routing. PostgreSQL remains the command source of truth. Publish committed outbox rows with CDC or a retrying relay; no request handler should dual-write the database and broker independently. Broker records carry event UUID, item ID, aggregate version, schema version, and trace context. Partition by item/warehouse to preserve per-aggregate order. Parallelism depends on distinct partition keys; one hot SKU still serializes.

The current worker's completion and projection update are atomic in PostgreSQL. A broker cannot atomically commit its consumer offset and an arbitrary external database transaction. Make delivery at least once and implement a durable consumer inbox keyed by `(consumer, event_id)`; insert inbox marker and update projection in the same transaction, then acknowledge the broker. If acknowledgement fails, redelivery sees the marker. This gives an exactly-once *projection effect* under those assumptions, not exactly-once end-to-end messaging. Use bounded exponential backoff with jitter, poison-event quarantine, alerting, and an authenticated replay/requeue tool. Keep retries off hot partitions when business ordering permits; otherwise explicitly block the affected aggregate.

Add versioned projection rebuilds. Replay into a new table/version, catch up to a known cursor, compare aggregates, then atomically switch readers. The current additive projection tolerates reordering; future stock/balance projections must enforce contiguous versions or buffer gaps. Never reuse a consumer inbox when intentionally rebuilding into a new projection generation.

## WebSockets and load balancing

Replace per-socket PostgreSQL polling with dedicated connection gateways consuming shared item channels from broker-backed distribution. Gateways maintain bounded per-client queues, send deadlines, heartbeat, connection quotas, and backpressure. Fan out one item event locally to all interested clients. Sticky routing can improve locality but correctness must not require it: every client reconnects with its last applied version to any gateway.

A replay service provides retained event ranges; a subscription/replay barrier plus version deduplication bridges historical replay to broker live delivery. A gateway crash loses only connections and ephemeral buffers, not history. Slow clients are disconnected with a resumable cursor; excessive history triggers HTTP bulk replay/snapshot plus a fenced stream restart. Restrict item subscriptions by tenant authorization, validate origins for browser sessions, and terminate WSS at regional ingress. Autoscale on connections, queued bytes, send lag, and event fan-out rather than CPU alone.

## Event retention and storage

At 10 million events/hour, retention must be explicit: 240 million/day before indexes, JSON snapshots, WAL, replicas, and backups. Measure bytes/event using this schema; current full-state snapshots in every event amplify storage. Introduce versioned payload schemas and periodic aggregate snapshots, keeping sufficient deltas/checksums for audit and replay. Archive immutable time partitions to encrypted object storage with manifests, checksums, retention/legal-hold policies, and a tested historical replay path. Expired history needs a clear cursor-too-old response and snapshot recovery API.

Idempotency retention is a separate correctness contract. Deleting old keys permits old retries to run again. Negotiate a retry window and reject expired keys or persist a compact tombstone ledger. Coordinate item/event/idempotency retention and consumer deduplication windows; do not delete an event while a live consumer can still require it.

## Multiple geographic regions

Prefer a home region for each warehouse/item and synchronously consistent writes within that region. Route reservations to its home; remote read replicas/projections can be stale under an explicit policy. Cross-region synchronous writes trade availability and latency for stronger durability. For active-active reservations, preallocate fenced regional stock quotas/escrow and make transfers explicit commands; independent regional counters are unsafe. Never resolve conflicting reservations with last-write-wins.

During a network partition, choose safety over accepting unbounded reservations on both sides. Failover requires fencing the old primary, determining committed WAL/outbox positions, and handling idempotent retries against the new owner. Define RPO/RTO before selecting asynchronous versus synchronous replication. Inventory correctness and “always accept writes everywhere” cannot both be assumed.

## Observability and deployment

Export durable backlog age, retry/dead counts, item lock wait, pool wait, DB latency, cache stale detections, broker lag, replay lag, and per-gateway queue pressure. Aggregate process counters in Prometheus/OpenTelemetry; the current `/metrics` JSON is per process and resets at restart. Histograms need fleet-level bucket aggregation; averaging per-process p95s is invalid. Propagate W3C trace context from HTTP command through event/outbox, worker, and delivery, while keeping `X-Request-ID` for user support. Avoid event/item/request IDs as metric labels because cardinality explodes.

Deploy immutable images, managed secrets, restricted identities, TLS, infrastructure-as-code, and rolling/canary releases. Use expand/migrate/contract schema changes and backward-compatible event envelopes so old and new workers can coexist. Drain HTTP requests and socket connections gracefully; do not let all reconnects hit one region simultaneously. Separate liveness from readiness, and avoid restart storms during database outages.

Rehearse PostgreSQL PITR, broker retention loss, replica failover, Redis outage, poison-event recovery, and rolling gateway failures. Validate recovery with replay comparison against source state and application-level invariants. Define SLOs for command success/latency, event-to-projection delay, WebSocket delivery delay, and replay consistency; use representative load and failure tests before claiming the stated future throughput.
