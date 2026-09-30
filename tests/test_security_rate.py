import asyncio

import httpx
import pytest

from eventvault.config import Settings
from eventvault.main import create_app
from eventvault.metrics import Metrics
from eventvault.pagination import decode_cursor, encode_cursor
from eventvault.rate_limit import RateLimiter
from eventvault.repository import DomainError
from tests.conftest import key


def test_rate_window_and_capacity():
    now = [0]
    metrics = Metrics()
    limiter = RateLimiter(2, 10, metrics, max_ips=2, clock=lambda: now[0])
    assert limiter.allow("a")[0]
    assert limiter.allow("a")[0]
    assert not limiter.allow("a")[0]
    assert limiter.allow("b")[0]
    assert not limiter.allow("c")[0]
    now[0] = 10
    assert limiter.allow("c")[0]
    assert metrics.counts["rate_limit_rejections"] == 2


async def test_http_rate_limit_and_auth(settings):
    settings.rate_limit = 10
    settings.api_token = "private-token"
    # Revalidate assignment before constructing app.
    settings = Settings(**settings.model_dump(), _env_file=None)
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        assert (await client.get("/stats")).status_code == 401
        responses = await asyncio.gather(
            *(
                client.get("/metrics", headers={"Authorization": "Bearer private-token"})
                for _ in range(20)
            )
        )
        assert sum(r.status_code == 200 for r in responses) == 9
        assert sum(r.status_code == 429 for r in responses) == 11
        assert all("x-request-id" in r.headers for r in responses)
        assert (await client.get("/health")).status_code == 200
        assert (await client.get("/ready")).status_code == 200


def test_production_config():
    with pytest.raises(ValueError):
        Settings(app_env="production", _env_file=None)
    with pytest.raises(ValueError):
        Settings(
            app_env="production",
            api_token="x" * 32,
            admin_token="y" * 32,
            failure_rate=0.1,
            _env_file=None,
        )
    assert Settings(app_env="production", api_token="x" * 32, admin_token="y" * 32, _env_file=None)


async def test_parameterized_injection_is_data(client, app):
    name = "Robert'); DROP TABLE items; --"
    response = await client.post(
        "/items", headers=key(), json={"sku": "SAFE", "name": name, "quantity": 0, "unit_price": 0}
    )
    assert response.status_code == 201
    assert await app.state.pool.fetchval("SELECT name FROM items") == name


def test_cursor_invalid_payloads():
    import base64
    import json

    for data in (
        {},
        [],
        1,
        {"v": 1, "filters": "bad"},
        {"v": 1, "after": -1, "upper": 1, "filters": "bad"},
    ):
        cursor = base64.urlsafe_b64encode(json.dumps(data).encode()).decode()
        with pytest.raises(DomainError):
            decode_cursor(cursor, {})
    assert decode_cursor(encode_cursor(1, 9, {}), {}) == (1, 9)
