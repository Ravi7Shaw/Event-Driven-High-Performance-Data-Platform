import asyncio

import pytest

from eventvault.cache import ItemCache
from eventvault.metrics import Metrics


async def test_lru_ttl_invalidation_and_staleness():
    now = [0]
    metrics = Metrics()
    cache = ItemCache(2, 5, metrics, clock=lambda: now[0])
    versions = {1: 1, 2: 1, 3: 1}
    calls = []

    async def get(key):
        async def load():
            calls.append(key)
            return {"version": versions[key], "value": key}

        async def version():
            return versions[key]

        return await cache.get(key, load, version)

    await get(1)
    await get(2)
    await get(1)
    await get(3)
    assert list(cache.entries) == [1, 3]
    assert metrics.counts["cache_hits"] == 1
    assert metrics.counts["cache_evictions"] == 1
    now[0] = 6
    await get(1)
    assert metrics.counts["cache_expirations"] == 1
    versions[1] = 2
    assert (await get(1))["version"] == 2
    assert metrics.counts["cache_stale"] == 1
    cache.invalidate(1)
    await get(1)
    cache.clear()
    assert not cache.entries


async def test_thousand_misses_single_flight_and_cancellation():
    cache = ItemCache(512, 30, Metrics())
    gate = asyncio.Event()
    calls = 0

    async def load():
        nonlocal calls
        calls += 1
        await gate.wait()
        return {"version": 1, "quantity": 5}

    async def version():
        return 1

    tasks = [asyncio.create_task(cache.get(1, load, version)) for _ in range(1000)]
    await asyncio.sleep(0.01)
    tasks[0].cancel()
    gate.set()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert isinstance(results[0], asyncio.CancelledError)
    assert all(result == {"version": 1, "quantity": 5} for result in results[1:])
    assert calls == 1
    results[1]["quantity"] = 100
    assert (await cache.get(1, load, version))["quantity"] == 5


async def test_failed_loader_retries_and_disabled_cache():
    metrics = Metrics()
    cache = ItemCache(1, 1, metrics)
    calls = 0

    async def load():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("database down")
        return {"version": 1}

    async def version():
        return 1

    with pytest.raises(RuntimeError):
        await cache.get(1, load, version)
    assert await cache.get(1, load, version) == {"version": 1}
    disabled = ItemCache(0, 1, metrics)
    await asyncio.gather(*(disabled.get(1, load, version) for _ in range(10)))
    assert calls == 12


async def test_http_stampede(client, app, item):
    before = app.state.metrics.counts["db_read_queries"]
    responses = await asyncio.gather(*(client.get(f"/items/{item['id']}") for _ in range(1000)))
    assert all(response.status_code == 200 for response in responses)
    assert app.state.metrics.counts["db_read_queries"] - before == 1
