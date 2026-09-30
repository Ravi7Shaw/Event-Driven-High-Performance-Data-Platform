import asyncio
import json

import httpx
import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from eventvault.benchmark import benchmark_server
from eventvault.websocket import WebSocketManager
from tests.conftest import key


async def next_event(socket):
    async with asyncio.timeout(5):
        while True:
            message = json.loads(await socket.recv())
            if message["type"] != "heartbeat":
                return message


async def test_live_heartbeat_multiple_disconnect_replay(settings):
    async with (
        benchmark_server(settings) as (app, base),
        httpx.AsyncClient(base_url=base) as client,
    ):
        response = await client.post(
            "/items",
            headers=key(),
            json={"sku": "WS", "name": "Stream", "quantity": 100, "unit_price": 1},
        )
        item_id = response.json()["id"]
        url = base.replace("http", "ws") + f"/ws/items/{item_id}"
        async with connect(url) as first, connect(url) as second:
            assert json.loads(await asyncio.wait_for(first.recv(), 2))["type"] == "heartbeat"
            await client.post(f"/items/{item_id}/reserve", headers=key(), json={"quantity": 2})
            assert (await next_event(first))["version"] == 2
            assert (await next_event(second))["version"] == 2
            await first.close()
            await client.post(f"/items/{item_id}/release", headers=key(), json={"quantity": 1})
            assert (await next_event(second))["version"] == 3
            async with connect(url + "?after_version=1") as replay:
                # New writes race replay; each version must arrive once and in order.
                pending = asyncio.create_task(
                    client.patch(f"/items/{item_id}", headers=key(), json={"name": "Live"})
                )
                versions = [(await next_event(replay))["version"] for _ in range(3)]
                await pending
                assert versions == [2, 3, 4]
        await asyncio.sleep(0.03)
        assert app.state.metrics.counts["active_websockets"] == 0
        assert not app.state.sockets.connections
        with pytest.raises(InvalidStatus):
            async with connect(url + "?after_version=999"):
                pytest.fail("Future cursor accepted")
        app.state.faults.rate = 1
        async with connect(url + "?after_version=0") as failed:
            with pytest.raises(ConnectionClosed):
                await failed.recv()
        assert app.state.metrics.counts["websocket_messages_failed"] >= 1


async def test_slow_send_deadline_does_not_block_fast(app):
    class FakeSocket:
        def __init__(self, slow):
            self.slow, self.messages = slow, []

        async def send_json(self, message):
            if self.slow:
                await asyncio.Event().wait()
            self.messages.append(message)

    manager = WebSocketManager(
        app.state.repository, app.state.settings, app.state.metrics, app.state.faults, None
    )
    slow, fast = FakeSocket(True), FakeSocket(False)
    results = await asyncio.gather(
        manager.send(slow, {"version": 1}, True),
        manager.send(fast, {"version": 1}, True),
        return_exceptions=True,
    )
    assert isinstance(results[0], TimeoutError)
    assert fast.messages == [{"version": 1}]


async def test_websocket_auth_and_limit(settings):
    from pydantic import SecretStr

    settings.api_token = SecretStr("ws-secret")
    settings.ws_max_connections = 1
    async with benchmark_server(settings) as (app, base):
        item, _, _ = await app.state.repository.mutate(
            "create",
            None,
            {
                "sku": "AUTH",
                "name": "Auth",
                "description": None,
                "quantity": 1,
                "unit_price": "1.00",
            },
            "auth",
        )
        url = base.replace("http", "ws") + f"/ws/items/{item['id']}"
        with pytest.raises(InvalidStatus):
            async with connect(url):
                pytest.fail("Unauthenticated socket accepted")
        async with connect(url, additional_headers={"Authorization": "Bearer ws-secret"}):
            with pytest.raises(InvalidStatus):
                async with connect(url, additional_headers={"Authorization": "Bearer ws-secret"}):
                    pytest.fail("Capacity exceeded")
