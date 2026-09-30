import os
import subprocess
import sys

import pytest

from eventvault.benchmark import benchmark


def test_cli_help_and_invalid_arguments():
    result = subprocess.run(
        [sys.executable, "-m", "eventvault.cli", "--help"], capture_output=True, text=True
    )
    assert result.returncode == 0
    for command in (
        "seed",
        "run",
        "migrate",
        "cache-stats",
        "clear-cache",
        "stats",
        "replay",
        "events",
        "verify-item",
        "worker",
        "benchmark",
    ):
        assert command in result.stdout
    result = subprocess.run(
        [sys.executable, "-m", "eventvault.cli", "benchmark", "--concurrency", "0"],
        capture_output=True,
    )
    assert result.returncode == 2


async def test_cli_migrate_seed_events_replay_exit_codes(settings, app):
    env = {
        **os.environ,
        "DATABASE_URL": settings.database_url.get_secret_value(),
        "LOG_LEVEL": "ERROR",
    }
    # Run subprocess off-loop so PostgreSQL fixtures remain responsive.
    import asyncio

    async def run(*args):
        return await asyncio.to_thread(
            subprocess.run,
            [sys.executable, "-m", "eventvault.cli", *args],
            capture_output=True,
            text=True,
            env=env,
        )

    assert (await run("migrate")).returncode == 0
    item, _, _ = await app.state.repository.mutate(
        "create",
        None,
        {"sku": "CLI", "name": "CLI", "description": None, "quantity": 1, "unit_price": "1.00"},
        "cli",
    )
    for command in ("replay", "verify-item", "events"):
        result = await run(command, "--item-id", str(item["id"]))
        assert result.returncode == 0
    assert (await run("replay", "--item-id", "999999")).returncode == 1
    await app.state.pool.execute("UPDATE items SET name='drift'")
    assert (await run("verify-item", "--item-id", str(item["id"]))).returncode == 2


@pytest.mark.parametrize("mode", ["get", "reserve", "fanout"])
async def test_network_benchmark_without_auth(settings, mode):
    result = await benchmark(settings, requests=3, concurrency=2, mode=mode)
    assert result["replay"] == "CONSISTENT"
    if mode == "fanout":
        assert result["messages_delivered"] == 6
    else:
        assert result["successful"] == 3


async def test_worker_handles_sigterm(settings, app, item):
    import asyncio
    import signal

    env = {
        **os.environ,
        "DATABASE_URL": settings.database_url.get_secret_value(),
        "LOG_LEVEL": "ERROR",
    }
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "eventvault.cli",
        "worker",
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        # A completed durable job proves the standalone worker is ready.
        async with asyncio.timeout(5):
            while not await app.state.pool.fetchval(  # noqa: ASYNC110 -- external DB readiness
                "SELECT count(*) FROM event_jobs WHERE processed_at IS NOT NULL"
            ):
                await asyncio.sleep(0.02)
        process.send_signal(signal.SIGTERM)
        await asyncio.wait_for(process.communicate(), 5)
        assert process.returncode == 0
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()


async def test_cli_operates_running_cache(settings):
    import asyncio

    import httpx

    from eventvault.benchmark import benchmark_server

    async with benchmark_server(settings) as (app, base):
        env = {
            **os.environ,
            "ADMIN_TOKEN": settings.admin_token.get_secret_value(),
            "API_TOKEN": "",
            "LOG_LEVEL": "ERROR",
        }

        async def run(command):
            return await asyncio.to_thread(
                subprocess.run,
                [sys.executable, "-m", "eventvault.cli", "--api-url", base, command],
                capture_output=True,
                text=True,
                env=env,
            )

        item, _, _ = await app.state.repository.mutate(
            "create",
            None,
            {
                "sku": "CACHE-CLI",
                "name": "Cache",
                "description": None,
                "quantity": 1,
                "unit_price": "1.00",
            },
            "cache-cli",
        )
        async with httpx.AsyncClient(base_url=base) as client:
            assert (await client.get(f"/items/{item['id']}")).status_code == 200
        assert app.state.cache.stats()["size"] == 1
        assert (await run("cache-stats")).returncode == 0
        assert (await run("clear-cache")).returncode == 0
        assert app.state.cache.stats()["size"] == 0
