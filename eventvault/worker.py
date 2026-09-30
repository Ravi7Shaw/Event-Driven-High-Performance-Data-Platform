import asyncio
import logging

logger = logging.getLogger("eventvault.worker")


class Worker:
    def __init__(self, pool, settings, metrics, faults):
        self.pool, self.settings, self.metrics, self.faults = pool, settings, metrics, faults
        self.stop = asyncio.Event()

    async def batch(self):
        processed = 0
        async with self.pool.acquire() as connection, connection.transaction():
            events = await connection.fetch(
                "SELECT e.*, j.attempts FROM event_jobs j JOIN events e ON e.id=j.event_id "
                "WHERE j.processed_at IS NULL AND NOT j.dead AND j.available_at<=clock_timestamp() "
                "ORDER BY j.event_id LIMIT $1 FOR UPDATE OF j SKIP LOCKED",
                self.settings.worker_batch_size,
            )
            for event in events:
                attempt = event["attempts"] + 1
                try:
                    # Savepoint: a failed projection cannot leak a partial update.
                    async with connection.transaction():
                        kind = event["event_type"]
                        quantity = event["payload"]["changes"].get("quantity", 0)
                        await connection.execute(
                            "INSERT INTO inventory_activity(item_id,total_created,total_updated,"
                            "total_reserved,total_released,last_event_at) "
                            "VALUES($1,$2,$3,$4,$5,$6) "
                            "ON CONFLICT(item_id) DO UPDATE SET "
                            "total_created=inventory_activity.total_created+EXCLUDED.total_created,"
                            "total_updated=inventory_activity.total_updated+EXCLUDED.total_updated,"
                            "total_reserved=inventory_activity.total_reserved+EXCLUDED.total_reserved,"
                            "total_released=inventory_activity.total_released+EXCLUDED.total_released,"
                            "last_event_at=GREATEST(inventory_activity.last_event_at,EXCLUDED.last_event_at)",
                            event["aggregate_id"],
                            int(kind == "ITEM_CREATED"),
                            int(kind == "ITEM_UPDATED"),
                            quantity if kind == "INVENTORY_RESERVED" else 0,
                            quantity if kind == "INVENTORY_RELEASED" else 0,
                            event["created_at"],
                        )
                        self.faults.check("worker_after_projection")
                        await connection.execute(
                            "UPDATE event_jobs SET "
                            "attempts=$2,processed_at=clock_timestamp(),last_error=NULL "
                            "WHERE event_id=$1",
                            event["id"],
                            attempt,
                        )
                    processed += 1
                    result = "processed"
                except Exception as exc:
                    # Safe diagnostic only; SQL values and connection secrets are not logged.
                    dead = attempt >= self.settings.worker_max_attempts
                    delay = min(60, self.settings.worker_retry_base * 2 ** min(attempt - 1, 10))
                    await connection.execute(
                        "UPDATE event_jobs SET attempts=$2,dead=$3,last_error=$4,"
                        "available_at=clock_timestamp()+$5::double precision*interval '1 second' "
                        "WHERE event_id=$1",
                        event["id"],
                        attempt,
                        dead,
                        type(exc).__name__,
                        delay,
                    )
                    self.metrics.inc("events_failed")
                    result = "dead" if dead else "retry_scheduled"
                if attempt > 1:
                    self.metrics.inc("events_retried")
                logger.info(
                    "event_id=%s event_type=%s attempt=%s result=%s",
                    event["event_id"],
                    event["event_type"],
                    attempt,
                    result,
                )
        self.metrics.inc("events_processed", processed)
        return len(events)

    async def run(self):
        while not self.stop.is_set():
            try:
                count = await self.batch()
            except Exception as exc:
                logger.error("worker_batch_error=%s", type(exc).__name__)
                count = 0
            if not count:
                try:
                    await asyncio.wait_for(self.stop.wait(), self.settings.worker_interval)
                except TimeoutError:
                    continue

    async def retry_dead(self):
        return await self.pool.execute(
            "UPDATE event_jobs SET "
            "dead=false,attempts=0,available_at=clock_timestamp(),last_error=NULL "
            "WHERE dead AND processed_at IS NULL"
        )
