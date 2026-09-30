import asyncio
import json
import socket
import statistics
import time
from contextlib import asynccontextmanager
from uuid import uuid4

import httpx
import uvicorn
from websockets.asyncio.client import connect

from eventvault.main import create_app
from eventvault.replay import replay


def percentiles(values):
    values = sorted(values)
    return {
        f"p{p}_ms": values[min(len(values) - 1, int((len(values) - 1) * p / 100))] * 1000
        if values
        else 0
        for p in (50, 95, 99)
    }


@asynccontextmanager
async def benchmark_server(settings):
    app = create_app(settings)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(2048)
    port = listener.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(app, log_level="error", access_log=False, ws_max_size=16384, ws_max_queue=16)
    )
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(30):
            while not server.started:
                if task.done():
                    task.result()
                    raise RuntimeError("Benchmark server failed to start")
                await asyncio.sleep(0.01)
        yield app, f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(task, 15)
        finally:
            listener.close()


async def measure(client, method, path, requests, concurrency, body=None):
    latencies, statuses = [], {}
    next_request = 0

    async def worker(worker_client):
        nonlocal next_request
        while next_request < requests:
            next_request += 1
            start = time.perf_counter()
            try:
                response = await worker_client.request(
                    method, path, json=body, headers={"Idempotency-Key": str(uuid4())}
                )
                status = str(response.status_code)
            except httpx.HTTPError:
                status = "transport_error"
            latencies.append(time.perf_counter() - start)
            statuses[status] = statuses.get(status, 0) + 1

    # A shared HTTPX pool scans all connections for every queued request. One
    # persistent client per simulated user avoids generator-side O(N) contention.
    clients = [
        httpx.AsyncClient(
            base_url=client.base_url,
            headers=client.headers,
            timeout=client.timeout,
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
        )
        for _ in range(min(concurrency, requests))
    ]
    start = time.perf_counter()
    try:
        await asyncio.gather(*(worker(worker_client) for worker_client in clients))
        duration = time.perf_counter() - start
    finally:
        await asyncio.gather(*(worker_client.aclose() for worker_client in clients))
    successful = sum(count for code, count in statuses.items() if code.startswith("2"))
    return {
        "requests": requests,
        "load_generator": "one persistent HTTP client per concurrent worker",
        "successful": successful,
        "failed": requests - successful,
        "duration_seconds": duration,
        "rps": requests / duration,
        "average_latency_ms": statistics.mean(latencies) * 1000,
        **percentiles(latencies),
        "statuses": statuses,
    }


async def benchmark(settings, requests=10000, concurrency=100, mode="get", failure_rate=0):
    if not 0 <= failure_rate <= 1 or (settings.app_env == "production" and failure_rate):
        raise ValueError("Failure injection is restricted to development/test with rate in [0,1]")
    # A dedicated local HTTP server makes configuration and measurement explicit.
    settings = settings.model_copy(
        update={
            "worker_enabled": False,
            "rate_limit": max(requests * 100, 1_000_000),
            "failure_rate": 0,
        }
    )
    async with benchmark_server(settings) as (app, base):
        token = settings.api_token.get_secret_value()
        headers = {"Authorization": "Bearer " + token} if token else {}
        async with httpx.AsyncClient(
            base_url=base,
            headers=headers,
            timeout=60,
            limits=httpx.Limits(
                max_connections=max(concurrency, 100),
                max_keepalive_connections=max(concurrency, 100),
            ),
        ) as client:
            response = await client.post(
                "/items",
                headers={"Idempotency-Key": str(uuid4())},
                json={
                    "sku": "BENCH-" + uuid4().hex,
                    "name": "Benchmark fixture",
                    "quantity": 100,
                    "unit_price": "1.00",
                },
            )
            response.raise_for_status()
            item_id = response.json()["id"]
            app.state.faults.rate = failure_rate
            before = app.state.metrics.snapshot()
            if mode == "fanout":
                result = await fanout(client, base, headers, item_id, concurrency, requests)
            else:
                path = f"/items/{item_id}" + ("/reserve" if mode == "reserve" else "")
                result = await measure(
                    client,
                    "POST" if mode == "reserve" else "GET",
                    path,
                    requests,
                    concurrency,
                    {"quantity": 2} if mode == "reserve" else None,
                )
            result.update(
                {
                    "mode": mode,
                    "concurrency": concurrency,
                    "cache_capacity": settings.cache_capacity,
                    "failure_rate": failure_rate,
                    "rate_limit": settings.rate_limit,
                    "db_queries": app.state.metrics.counts["db_read_queries"]
                    - before["db_read_queries"],
                    "cache": app.state.cache.stats(),
                }
            )
            state = await app.state.repository.item(item_id)
            result["final_reserved_quantity"] = state["reserved_quantity"]
            result["replay"] = (await replay(app.state.pool, item_id))["result"]
            return result


async def fanout(client, base, headers, item_id, clients, events):
    sockets, failures = [], 0
    start = time.perf_counter()
    url = base.replace("http://", "ws://") + f"/ws/items/{item_id}?after_version=1"
    for _ in range(clients):
        try:
            sockets.append(await connect(url, additional_headers=headers, max_queue=events + 10))
        except Exception:
            failures += 1

    async def consume(ws):
        received, last = 0, 1
        try:
            async with asyncio.timeout(60):
                while received < events:
                    event = json.loads(await ws.recv())
                    if event["type"] == "heartbeat":
                        continue
                    if event["version"] != last + 1:
                        raise RuntimeError("Non-contiguous WebSocket delivery")
                    last = event["version"]
                    received += 1
        except (TimeoutError, OSError):
            return received, False
        return received, True

    tasks = [asyncio.create_task(consume(ws)) for ws in sockets]
    generated = 0
    try:
        for index in range(events):
            response = await client.patch(
                f"/items/{item_id}",
                json={"name": f"Fanout {index}"},
                headers={"Idempotency-Key": str(uuid4())},
            )
            if response.is_success:
                generated += 1
        results = await asyncio.gather(*tasks, return_exceptions=True)
        delivered = sum(result[0] for result in results if isinstance(result, tuple))
        failures += sum(not isinstance(result, tuple) or not result[1] for result in results)
        return {
            "messages_generated": generated,
            "expected_deliveries": events * clients,
            "messages_delivered": delivered,
            "failed_connections": failures,
            "duration_seconds": time.perf_counter() - start,
        }
    finally:
        await asyncio.gather(*(ws.close() for ws in sockets), return_exceptions=True)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
