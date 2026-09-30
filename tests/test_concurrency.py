import asyncio

import pytest

from tests.conftest import key


@pytest.mark.parametrize("requests,quantity,stock,expected", [(50, 3, 100, 33), (100, 1, 10, 10)])
async def test_concurrent_reservations(client, app, requests, quantity, stock, expected):
    response = await client.post(
        "/items",
        headers=key(),
        json={"sku": "limited", "name": "Limited", "quantity": stock, "unit_price": 1},
    )
    item_id = response.json()["id"]
    responses = await asyncio.gather(
        *(
            client.post(f"/items/{item_id}/reserve", headers=key(), json={"quantity": quantity})
            for _ in range(requests)
        )
    )
    assert sum(r.status_code == 200 for r in responses) == expected
    assert sum(r.status_code == 409 for r in responses) == requests - expected
    row = await app.state.repository.item(item_id)
    assert row["reserved_quantity"] == expected * quantity <= stock
    assert await app.state.pool.fetchval("SELECT count(*) FROM events") == expected + 1


async def test_concurrent_idempotency_and_conflict(client, app, item):
    path = f"/items/{item['id']}/reserve"
    responses = await asyncio.gather(
        *(client.post(path, json={"quantity": 7}, headers=key("duplicate")) for _ in range(50))
    )
    assert all(r.status_code == 200 for r in responses)
    assert all(r.json() == responses[0].json() for r in responses)
    assert await app.state.pool.fetchval("SELECT count(*) FROM events") == 2
    assert (
        await client.post(path, json={"quantity": 8}, headers=key("duplicate"))
    ).status_code == 409
    # A replay after later changes returns the original body/version.
    await client.post(path, json={"quantity": 1}, headers=key())
    assert (
        await client.post(path, json={"quantity": 7}, headers=key("duplicate"))
    ).json() == responses[0].json()
    response = await client.post(
        f"/items/{item['id']}/release", json={"quantity": 7}, headers=key("duplicate")
    )
    assert response.status_code == 409


async def test_concurrent_create_release_and_optimistic(client, app, item):
    body = {"sku": "concurrent", "name": "Concurrent", "quantity": 10, "unit_price": "1.00"}
    responses = await asyncio.gather(
        *(client.post("/items", headers=key("same-create"), json=body) for _ in range(20))
    )
    assert all(r.status_code == 201 and r.json() == responses[0].json() for r in responses)
    path = f"/items/{item['id']}"
    await client.post(path + "/reserve", headers=key(), json={"quantity": 10})
    releases = await asyncio.gather(
        *(client.post(path + "/release", headers=key(), json={"quantity": 1}) for _ in range(30))
    )
    assert sum(r.status_code == 200 for r in releases) == 10
    current = (await client.get(path)).json()["version"]
    updates = await asyncio.gather(
        *(
            client.patch(
                path, headers={**key(), "If-Match-Version": str(current)}, json={"name": str(n)}
            )
            for n in range(10)
        )
    )
    assert sum(r.status_code == 200 for r in updates) == 1
