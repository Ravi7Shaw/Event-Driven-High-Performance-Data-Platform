import asyncio

from eventvault.faults import Faults
from eventvault.replay import replay
from eventvault.seed import seed
from eventvault.worker import Worker
from tests.conftest import key


async def test_multiple_workers_exact_projection(client, app, item):
    for _ in range(20):
        await client.post(f"/items/{item['id']}/reserve", headers=key(), json={"quantity": 2})
    app.state.settings.worker_batch_size = 3
    workers = [
        Worker(app.state.pool, app.state.settings, app.state.metrics, Faults()) for _ in range(4)
    ]

    async def drain(worker):
        while await worker.batch():
            continue

    await asyncio.gather(*(drain(worker) for worker in workers))
    projection = await app.state.pool.fetchrow("SELECT * FROM inventory_activity")
    assert projection["total_created"] == 1
    assert projection["total_reserved"] == 40
    assert (
        await app.state.pool.fetchval(
            "SELECT count(*) FROM event_jobs WHERE attempts=1 AND processed_at IS NOT NULL"
        )
        == 21
    )
    assert await workers[0].batch() == 0


async def test_failure_retry_dead_and_requeue(app, item):
    worker = app.state.worker
    app.state.faults.rate = 1
    assert await worker.batch() == 1
    assert await app.state.pool.fetchval("SELECT count(*) FROM inventory_activity") == 0
    await asyncio.sleep(0.005)
    app.state.faults.rate = 0
    await worker.batch()
    assert await app.state.pool.fetchval("SELECT attempts FROM event_jobs") == 2
    assert await app.state.pool.fetchval("SELECT total_created FROM inventory_activity") == 1
    await app.state.repository.mutate("reserve", item["id"], {"quantity": 2}, "later")
    app.state.faults.rate = 1
    for _ in range(3):
        await worker.batch()
        await asyncio.sleep(0.005)
    assert await app.state.pool.fetchval("SELECT count(*) FROM event_jobs WHERE dead") == 1
    assert await worker.batch() == 0
    assert await app.state.pool.fetchval("SELECT total_reserved FROM inventory_activity") == 0
    await worker.retry_dead()
    app.state.faults.rate = 0
    await worker.batch()
    assert await app.state.pool.fetchval("SELECT total_reserved FROM inventory_activity") == 2
    assert app.state.metrics.counts["events_retried"] == 3


async def test_out_of_order_projection(app, item):
    await app.state.repository.mutate("reserve", item["id"], {"quantity": 3}, "r")
    await app.state.repository.mutate("release", item["id"], {"quantity": 1}, "l")
    await app.state.pool.execute(
        "UPDATE event_jobs SET available_at=now()+interval '1 hour' WHERE event_id<3"
    )
    await app.state.worker.batch()
    assert await app.state.pool.fetchval("SELECT total_released FROM inventory_activity") == 1
    latest = await app.state.pool.fetchval("SELECT last_event_at FROM inventory_activity")
    await app.state.pool.execute("UPDATE event_jobs SET available_at=now()")
    await app.state.worker.batch()
    row = await app.state.pool.fetchrow("SELECT * FROM inventory_activity")
    assert row["total_created"] == 1 and row["total_reserved"] == 3
    assert row["last_event_at"] == latest


async def test_worker_cancellation_rolls_back(app, item):
    # Lock projection row; worker claims event then blocks. Simulate process death.
    await app.state.pool.execute(
        "INSERT INTO inventory_activity(item_id,last_event_at) VALUES($1,now())", item["id"]
    )
    async with app.state.pool.acquire() as connection, connection.transaction():
        await connection.execute("SELECT * FROM inventory_activity FOR UPDATE")
        task = asyncio.create_task(app.state.worker.batch())
        await asyncio.sleep(0.03)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert await app.state.pool.fetchval("SELECT attempts FROM event_jobs") == 0
    await app.state.worker.batch()
    assert await app.state.pool.fetchval("SELECT total_created FROM inventory_activity") == 1


async def test_repeatable_seed_and_replay_detects_drift(app):
    await seed(app.state.repository, count=5)
    await seed(app.state.repository, count=5)
    assert await app.state.pool.fetchval("SELECT count(*) FROM events") == 55
    for item_id in await app.state.pool.fetch("SELECT id FROM items"):
        assert (await replay(app.state.pool, item_id["id"]))["result"] == "CONSISTENT"
    await app.state.pool.execute("UPDATE items SET reserved_quantity=0 WHERE id=1")
    result = await replay(app.state.pool, 1)
    assert result["result"] == "INCONSISTENT"
    assert any("reserved_quantity" in diff for diff in result["differences"])


async def test_restricted_runtime_database_permissions(app, item, settings):
    from pathlib import Path

    import asyncpg
    import pytest

    # NOLOGIN group only; SET ROLE tests the exact production permission set.
    source = await asyncio.to_thread(Path("scripts/grant_runtime.sql").read_text)
    await app.state.pool.execute(source)
    async with app.state.pool.acquire() as connection, connection.transaction():
        await connection.execute("SET LOCAL ROLE eventvault_runtime")
        assert await connection.fetchval("SELECT count(*) FROM items") == 1
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with connection.transaction():
                await connection.execute("DELETE FROM events")
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with connection.transaction():
                await connection.execute("UPDATE schema_migrations SET checksum='bad'")

    from eventvault.db import init_connection
    from eventvault.repository import Repository

    restricted = await asyncpg.create_pool(
        settings.database_url.get_secret_value(),
        min_size=1,
        max_size=2,
        init=init_connection,
        server_settings={"role": "eventvault_runtime"},
    )
    try:
        repo = Repository(restricted, app.state.metrics, Faults())
        result, status, _ = await repo.mutate("reserve", item["id"], {"quantity": 2}, "restricted")
        assert status == 200 and result["reserved_quantity"] == 2
        worker = Worker(restricted, settings, app.state.metrics, Faults())
        assert await worker.batch() == 2
        assert await restricted.fetchval("SELECT total_reserved FROM inventory_activity") == 2
    finally:
        await restricted.close()
