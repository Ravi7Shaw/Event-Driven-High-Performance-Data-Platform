import os
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import httpx
import pytest

from eventvault.config import Settings
from eventvault.db import migrate
from eventvault.main import create_app


@pytest.fixture
async def settings():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL required (dedicated PostgreSQL role with CREATEDB)")
    parts = urlsplit(url)
    name = "eventvault_test_" + uuid4().hex
    admin = await asyncpg.connect(url)
    await admin.execute(f'CREATE DATABASE "{name}"')
    test_url = urlunsplit((parts.scheme, parts.netloc, "/" + name, parts.query, parts.fragment))
    config = Settings(
        database_url=test_url,
        app_env="test",
        worker_enabled=False,
        rate_limit=100000,
        worker_retry_base=0.001,
        worker_max_attempts=3,
        ws_poll_interval=0.01,
        ws_heartbeat=0.05,
        ws_send_timeout=0.1,
        api_token="",
        admin_token="test-admin",
        failure_rate=0,
        _env_file=None,
    )
    try:
        await migrate(config)
        yield config
    finally:
        await admin.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
        await admin.close()


@pytest.fixture
async def app(settings):
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        yield app


@pytest.fixture
async def client(app):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


@pytest.fixture
async def item(client):
    response = await client.post(
        "/items",
        headers={"Idempotency-Key": "create"},
        json={
            "sku": "SKU-1",
            "name": "Laptop",
            "description": None,
            "quantity": 100,
            "unit_price": "12.50",
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


def key(value=None):
    return {"Idempotency-Key": value or str(uuid4())}
