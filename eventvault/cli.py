import argparse
import asyncio
import json
import logging
import signal
import sys

import httpx
import uvicorn

from eventvault.benchmark import benchmark
from eventvault.config import Settings
from eventvault.db import create_pool, migrate
from eventvault.faults import Faults
from eventvault.metrics import Metrics
from eventvault.replay import replay
from eventvault.repository import Repository
from eventvault.seed import seed
from eventvault.worker import Worker


def positive(value):
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError("Must be positive")
    return result


def parser():
    root = argparse.ArgumentParser(description="EventVault inventory and event operations")
    root.add_argument(
        "--database-url", help="PostgreSQL URL (prefer DATABASE_URL to avoid shell history)"
    )
    root.add_argument(
        "--api-url",
        default="http://127.0.0.1:8000",
        help="Running API for process-local stats/cache",
    )
    root.add_argument("--cache-capacity", type=int)
    root.add_argument("--cache-ttl", type=float)
    root.add_argument("--rate-limit", type=positive)
    sub = root.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="Start the API and optional embedded worker")
    run.add_argument("--host", default="127.0.0.1")
    run.add_argument("--port", type=positive, default=8000)
    for command, help_text in {
        "migrate": "Apply checked, transactional SQL migrations",
        "seed": "Create/resume 1000 deterministic items and 11000 events",
        "cache-stats": "Inspect the running API cache (ADMIN_TOKEN required)",
        "clear-cache": "Clear the running API cache (ADMIN_TOKEN required)",
        "stats": "Inspect running API metrics and database totals",
        "worker": "Run an independent durable projection worker",
        "retry-dead": "Explicitly requeue permanently failed events",
    }.items():
        sub.add_parser(command, help=help_text)
    for command in ("replay", "verify-item", "events"):
        cmd = sub.add_parser(command, help=f"{command} for an item")
        cmd.add_argument("--item-id", required=True, type=positive)
    bench = sub.add_parser(
        "benchmark", help="Run real HTTP load on a temporary local API; writes fixtures"
    )
    bench.add_argument("--requests", type=positive, default=10000)
    bench.add_argument("--concurrency", type=positive, default=100)
    bench.add_argument("--mode", choices=("get", "reserve", "fanout"), default="get")
    bench.add_argument("--failure-rate", type=float, default=0)
    return root


async def execute(args, settings):
    if args.command == "migrate":
        await migrate(settings)
        return {"migrations": "applied"}
    if args.command in {"stats", "cache-stats", "clear-cache"}:
        headers = {
            "Authorization": "Bearer " + settings.api_token.get_secret_value(),
            "X-Admin-Token": settings.admin_token.get_secret_value(),
        }
        if not settings.api_token.get_secret_value():
            headers.pop("Authorization")
        async with httpx.AsyncClient(base_url=args.api_url, headers=headers, timeout=30) as client:
            path = "/stats" if args.command == "stats" else "/admin/cache"
            response = await client.request(
                "DELETE" if args.command == "clear-cache" else "GET", path
            )
            response.raise_for_status()
            return response.json()
    if args.command == "benchmark":
        if not 0 <= args.failure_rate <= 1 or (
            settings.app_env == "production" and args.failure_rate
        ):
            raise ValueError("Failure rate must be in [0,1] and zero in production")
        return await benchmark(
            settings, args.requests, args.concurrency, args.mode, args.failure_rate
        )
    pool = await create_pool(settings)
    try:
        metrics, faults = Metrics(), Faults(settings.failure_rate)
        repository = Repository(pool, metrics, faults)
        if args.command == "seed":
            return await seed(repository)
        if args.command in {"replay", "verify-item"}:
            return await replay(pool, args.item_id)
        if args.command == "events":
            cursor = 0
            while rows := await repository.after_version(args.item_id, cursor, 500):
                for row in rows:
                    print(json.dumps(row))
                cursor = rows[-1]["aggregate_version"]
            if await repository.version(args.item_id) is None:
                raise ValueError("Item not found")
            return None
        worker = Worker(pool, settings, metrics, faults)
        if args.command == "retry-dead":
            return {"requeued": await worker.retry_dead()}
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(signum, worker.stop.set)
        try:
            await worker.run()
        finally:
            for signum in (signal.SIGTERM, signal.SIGINT):
                loop.remove_signal_handler(signum)
    finally:
        await pool.close()


def main():
    args = parser().parse_args()
    overrides = {
        key: getattr(args, key)
        for key in ("database_url", "cache_capacity", "cache_ttl", "rate_limit")
        if getattr(args, key) is not None
    }
    try:
        settings = Settings(**overrides)
        logging.basicConfig(
            level=settings.log_level.upper(),
            format="%(asctime)s level=%(levelname)s %(name)s %(message)s",
        )
        if args.command == "run":
            from eventvault.main import create_app

            uvicorn.run(
                create_app(settings),
                host=args.host,
                port=args.port,
                access_log=False,
                proxy_headers=False,
                ws_max_size=16384,
                ws_max_queue=16,
                ws_ping_interval=15,
                ws_ping_timeout=15,
            )
            return
        result = asyncio.run(execute(args, settings))
        if result is not None:
            print(json.dumps(result, indent=2, default=str))
        if isinstance(result, dict) and result.get("result") == "INCONSISTENT":
            sys.exit(2)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as exc:
        # Never print database URLs, validation inputs, or credential-bearing HTTP URLs.
        print(
            f"EventVault failed ({type(exc).__name__}); check configuration and service health.",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
