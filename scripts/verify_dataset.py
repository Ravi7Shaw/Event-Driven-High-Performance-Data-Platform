"""Verify every seeded item, drain worker, and check projection against event sums."""

import asyncio
import json
import time

from eventvault.config import Settings
from eventvault.db import create_pool
from eventvault.faults import Faults
from eventvault.metrics import Metrics
from eventvault.replay import replay
from eventvault.repository import Repository
from eventvault.seed import seed
from eventvault.worker import Worker


async def main():
    settings = Settings()
    pool = await create_pool(settings)
    try:
        metrics = Metrics()
        repository = Repository(pool, metrics, Faults())
        before = await repository.stats()
        await seed(repository)
        after = await repository.stats()
        assert before == after, "Repeat seed changed database totals; run seed once first"
        items = await pool.fetch("SELECT id FROM items WHERE sku LIKE 'SKU-%' ORDER BY id")
        assert len(items) == 1000
        for item in items:
            result = await replay(pool, item["id"])
            assert result["result"] == "CONSISTENT", result
        worker = Worker(pool, settings, metrics, Faults())
        started = time.perf_counter()
        while await worker.batch():
            continue
        seconds = time.perf_counter() - started
        differences = await pool.fetchval(
            "SELECT count(*) FROM (SELECT e.aggregate_id FROM events e "
            "LEFT JOIN inventory_activity a ON a.item_id=e.aggregate_id "
            "GROUP BY e.aggregate_id,a.total_created,a.total_updated,"
            "a.total_reserved,a.total_released "
            "HAVING count(*) FILTER (WHERE event_type='ITEM_CREATED') "
            "IS DISTINCT FROM a.total_created "
            "OR count(*) FILTER (WHERE event_type='ITEM_UPDATED') IS DISTINCT FROM a.total_updated "
            "OR COALESCE(sum((payload->'changes'->>'quantity')::bigint) FILTER "
            "(WHERE event_type='INVENTORY_RESERVED'),0) IS DISTINCT FROM a.total_reserved "
            "OR COALESCE(sum((payload->'changes'->>'quantity')::bigint) FILTER "
            "(WHERE event_type='INVENTORY_RELEASED'),0) IS DISTINCT FROM a.total_released) checks"
        )
        assert differences == 0
        print(
            json.dumps(
                {
                    "seed_repeatable": True,
                    "items_verified": len(items),
                    "projection_differences": differences,
                    "database": await repository.stats(),
                    "worker_events": metrics.counts["events_processed"],
                    "worker_seconds": seconds,
                    "worker_events_per_second": metrics.counts["events_processed"] / seconds,
                },
                indent=2,
            )
        )
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
