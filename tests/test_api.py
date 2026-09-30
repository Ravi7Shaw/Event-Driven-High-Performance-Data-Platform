import asyncio

import pytest

from eventvault.db import migrate
from eventvault.replay import replay
from tests.conftest import key


async def test_operations_etag_replay(client, app, item):
    item_id = item["id"]
    read = await client.get(f"/items/{item_id}")
    assert read.json()["available_quantity"] == 100
    tag = read.headers["etag"]
    for condition in (tag, "W/" + tag, '"other", ' + tag, "*"):
        response = await client.get(f"/items/{item_id}", headers={"If-None-Match": condition})
        assert response.status_code == 304 and not response.content
    response = await client.post(f"/items/{item_id}/reserve", headers=key(), json={"quantity": 5})
    assert response.json()["reserved_quantity"] == 5
    assert response.json()["version"] == 2
    response = await client.post(f"/items/{item_id}/release", headers=key(), json={"quantity": 2})
    assert response.json()["reserved_quantity"] == 3
    response = await client.patch(
        f"/items/{item_id}",
        headers={**key(), "If-Match-Version": "3"},
        json={"name": "New", "description": "Updated", "unit_price": "10.00"},
    )
    assert response.status_code == 200
    assert response.json()["version"] == 4
    stale = await client.patch(
        f"/items/{item_id}", headers={**key(), "If-Match-Version": "3"}, json={"name": "Lost"}
    )
    assert stale.status_code == 409
    assert stale.json()["detail"]["current_version"] == 4
    fresh = await client.get(f"/items/{item_id}", headers={"If-None-Match": tag})
    assert fresh.status_code == 200 and fresh.headers["etag"] != tag
    assert (await replay(app.state.pool, item_id))["result"] == "CONSISTENT"
    bad_release = await client.post(
        f"/items/{item_id}/release", headers=key(), json={"quantity": 4}
    )
    assert bad_release.status_code == 409


@pytest.mark.parametrize(
    "patch",
    [
        {"quantity": -1},
        {"quantity": True},
        {"quantity": "2"},
        {"unit_price": -1},
        {"unit_price": "NaN"},
        {"unit_price": "1.001"},
        {"sku": "bad sku"},
        {"sku": "x'*"},
        {"sku": ""},
        {"name": "  "},
        {"surprise": 1},
    ],
)
async def test_validation(client, patch):
    body = {"sku": "valid", "name": "Good", "quantity": 1, "unit_price": "1.00", **patch}
    response = await client.post("/items", json=body, headers=key())
    assert response.status_code == 422


async def test_missing_fields_ids_and_patches(client, item):
    assert (await client.post("/items", json={}, headers=key())).status_code == 422
    assert (await client.post("/items", json={})).status_code == 422
    for identifier in ("0", "-1", "abc", str(2**63)):
        assert (await client.get(f"/items/{identifier}")).status_code == 422
    assert (await client.get("/items/9999")).status_code == 404
    for patch in ({}, {"name": None}, {"unit_price": None}, {"quantity": 1}):
        assert (
            await client.patch(f"/items/{item['id']}", json=patch, headers=key())
        ).status_code == 422
    assert (
        await client.patch(f"/items/{item['id']}", json={"description": None}, headers=key())
    ).status_code == 200


async def test_cursor_snapshot_filters(client, app, item):
    for _ in range(4):
        await client.post(f"/items/{item['id']}/reserve", json={"quantity": 1}, headers=key())
    params = {"item_id": item["id"], "limit": 2}
    page = (await client.get("/events", params=params)).json()
    ids = [event["id"] for event in page["events"]]
    cursor = page["next_cursor"]
    await client.post(f"/items/{item['id']}/release", json={"quantity": 1}, headers=key())
    while cursor:
        page = (await client.get("/events", params={**params, "cursor": cursor})).json()
        ids += [event["id"] for event in page["events"]]
        cursor = page["next_cursor"]
    assert ids == list(range(1, 6))
    assert (await client.get("/events", params={"cursor": "invalid"})).status_code == 422
    for params in (
        {"from": "2026-01-01"},
        {"from": "2026-02-01T00:00:00Z", "to": "2026-01-01T00:00:00Z"},
    ):
        assert (await client.get("/events", params=params)).status_code == 422
    filtered = (await client.get("/events", params={"event_type": "INVENTORY_RESERVED"})).json()
    assert len(filtered["events"]) == 4
    changed = await client.get("/events", params={"limit": 1})
    cursor = changed.json()["next_cursor"]
    assert (
        await client.get("/events", params={"cursor": cursor, "item_id": 123})
    ).status_code == 422


async def test_health_stats_admin_body_and_request_id(client, app, item):
    assert (await client.get("/health")).json() == {"status": "alive"}
    assert (await client.get("/ready")).status_code == 200
    response = await client.get("/stats", headers={"X-Request-ID": "trace-123"})
    assert response.headers["x-request-id"] == "trace-123"
    assert response.json()["database"]["items"] == 1
    assert (await client.delete("/admin/cache")).status_code == 403
    assert (
        await client.delete("/admin/cache", headers={"X-Admin-Token": "test-admin"})
    ).status_code == 200

    async def chunks():
        for _ in range(20):
            yield b"x" * 1024

    assert (await client.post("/items", content=chunks(), headers=key())).status_code == 413
    metrics = (await client.get("/metrics")).json()
    assert metrics["requests_total"] >= 7
    assert metrics["requests_failed"] >= 2


async def test_atomic_rollback_safe_errors_and_retry(client, app, item):
    app.state.faults.rate = 1
    response = await client.post(
        f"/items/{item['id']}/reserve", json={"quantity": 3}, headers=key("fault")
    )
    assert response.status_code == 503
    assert "x-request-id" in response.headers
    assert "InjectedFailure" not in response.text
    assert await app.state.pool.fetchval("SELECT count(*) FROM events") == 1
    assert await app.state.pool.fetchval("SELECT count(*) FROM idempotency") == 1
    assert await app.state.pool.fetchval("SELECT reserved_quantity FROM items") == 0
    app.state.faults.rate = 0
    assert (
        await client.post(
            f"/items/{item['id']}/reserve", json={"quantity": 3}, headers=key("fault")
        )
    ).status_code == 200
    # Cache failures fall through to the authoritative DB.
    app.state.faults.rate = 1
    assert (await client.get(f"/items/{item['id']}")).status_code == 200
    assert app.state.metrics.counts["cache_errors"] == 1


async def test_migration_repeat_and_immutable_events(settings, app, item):
    import asyncpg

    await asyncio.gather(migrate(settings), migrate(settings))
    for sql in ("UPDATE events SET payload='{}'", "DELETE FROM events", "TRUNCATE events CASCADE"):
        with pytest.raises(asyncpg.CheckViolationError):
            await app.state.pool.execute(sql)
    assert await app.state.pool.fetchval("SELECT count(*) FROM events") == 1


async def test_external_write_cache_stale(client, app, item):
    path = f"/items/{item['id']}"
    await client.get(path)
    await app.state.repository.mutate("update", item["id"], {"name": "Other process"}, "external")
    response = await client.get(path)
    assert response.json()["name"] == "Other process"
    assert app.state.metrics.counts["cache_stale"] == 1


@pytest.mark.parametrize("point", ["transaction_after_item_update", "transaction_before_commit"])
async def test_each_transaction_failure_boundary(client, app, item, point):
    from eventvault.faults import InjectedFailure

    def fail(target):
        if target == point:
            raise InjectedFailure(point)

    app.state.faults.check = fail
    response = await client.post(
        f"/items/{item['id']}/reserve", headers=key("boundary"), json={"quantity": 2}
    )
    assert response.status_code == 503
    assert await app.state.pool.fetchval("SELECT count(*) FROM events") == 1
    assert await app.state.pool.fetchval("SELECT value FROM event_clock") == 1
    assert await app.state.pool.fetchval("SELECT version FROM items") == 1
    assert await app.state.pool.fetchval("SELECT count(*) FROM event_jobs") == 1


async def test_actual_database_error_rolls_back_command(client, app, item):
    await app.state.pool.execute(
        "CREATE FUNCTION reject_test_event() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "BEGIN RAISE EXCEPTION 'test database failure'; END; $$"
    )
    await app.state.pool.execute(
        "CREATE TRIGGER reject_test_event BEFORE INSERT ON events "
        "FOR EACH ROW EXECUTE FUNCTION reject_test_event()"
    )
    response = await client.post(
        f"/items/{item['id']}/reserve", headers=key(), json={"quantity": 2}
    )
    assert response.status_code == 503
    assert "database failure" not in response.text
    assert await app.state.pool.fetchval("SELECT reserved_quantity FROM items") == 0
    assert await app.state.pool.fetchval("SELECT count(*) FROM idempotency") == 1


async def test_readiness_on_database_outage(client, app):
    class Unavailable:
        async def fetchval(self, *args, **kwargs):
            raise OSError("DB not reachable")

    app.state.pool = Unavailable()
    assert (await client.get("/health")).status_code == 200
    assert (await client.get("/ready")).status_code == 503
