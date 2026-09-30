import asyncio
import hmac
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated

from fastapi import FastAPI, Header, Path, Query, Request, WebSocket
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse, Response

from eventvault.cache import ItemCache
from eventvault.config import Settings
from eventvault.db import create_pool
from eventvault.faults import Faults, InjectedFailure
from eventvault.metrics import Metrics
from eventvault.middleware import RequestMiddleware
from eventvault.models import CreateItem, InventoryChange, UpdateItem
from eventvault.pagination import decode_cursor, encode_cursor
from eventvault.rate_limit import RateLimiter
from eventvault.repository import DomainError, Repository
from eventvault.websocket import WebSocketManager
from eventvault.worker import Worker

ItemID = Annotated[int, Path(gt=0, le=2**63 - 1)]
Key = Annotated[
    str, Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"^[!-~]+$")
]
Version = Annotated[int | None, Header(alias="If-Match-Version", gt=0, le=2**63 - 1)]


def create_app(settings=None):
    settings = settings or Settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)sZ level=%(levelname)s logger=%(name)s %(message)s",
    )
    logging.Formatter.converter = __import__("time").gmtime
    metrics = Metrics()
    faults = Faults(settings.failure_rate)
    cache = ItemCache(settings.cache_capacity, settings.cache_ttl, metrics)
    limiter = RateLimiter(settings.rate_limit, settings.rate_limit_window, metrics)

    @asynccontextmanager
    async def lifespan(app):
        pool = await create_pool(settings)
        app.state.pool = pool
        app.state.repository = Repository(pool, metrics, faults)
        app.state.worker = Worker(pool, settings, metrics, faults)
        app.state.sockets = WebSocketManager(
            app.state.repository, settings, metrics, faults, limiter
        )
        task = asyncio.create_task(app.state.worker.run()) if settings.worker_enabled else None
        try:
            yield
        finally:
            if task:
                app.state.worker.stop.set()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            # Complete outstanding shared reads before closing connections.
            await asyncio.gather(*cache.flights.values(), return_exceptions=True)
            await pool.close()

    app = FastAPI(
        title="EventVault",
        version="1.0.0",
        lifespan=lifespan,
        description="Transactional inventory, immutable events, replay, and durable processing.",
    )
    app.state.settings, app.state.metrics, app.state.cache = settings, metrics, cache
    app.state.faults = faults
    app.add_middleware(RequestMiddleware, settings=settings, metrics=metrics, limiter=limiter)

    @app.exception_handler(DomainError)
    async def domain_error(request, exc):
        return JSONResponse({"detail": exc.detail}, exc.status)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        # Pydantic input/context can contain user secrets or unserializable objects.
        errors = [{"loc": e["loc"], "msg": e["msg"], "type": e["type"]} for e in exc.errors()]
        return JSONResponse({"detail": errors}, 422)

    @app.get("/", include_in_schema=False)
    async def index():
        return RedirectResponse("/docs")

    @app.get("/health")
    async def health():
        return {"status": "alive"}

    @app.get("/ready")
    async def ready():
        try:
            await app.state.pool.fetchval("SELECT 1 FROM schema_migrations LIMIT 1", timeout=2)
        except Exception:
            return JSONResponse({"status": "not_ready"}, 503)
        return {"status": "ready"}

    async def mutate(operation, item_id, model, key, version=None):
        response, status, created = await app.state.repository.mutate(
            operation,
            item_id,
            model.model_dump(mode="json", exclude_unset=operation == "update"),
            key,
            version,
        )
        if created:
            cache.invalidate(response["id"])
        return JSONResponse(
            response, status, headers={"ETag": f'"item-{response["id"]}-v{response["version"]}"'}
        )

    @app.post("/items", status_code=201)
    async def create_item(body: CreateItem, idempotency_key: Key):
        return await mutate("create", None, body, idempotency_key)

    @app.get("/items/{item_id}")
    async def get_item(item_id: ItemID, if_none_match: Annotated[str | None, Header()] = None):
        repo = app.state.repository
        try:
            faults.check("cache_read")
            item = await cache.get(
                item_id, lambda: repo.item(item_id), lambda: repo.version(item_id)
            )
        except InjectedFailure:
            metrics.inc("cache_errors")
            item = await repo.item(item_id)
        if item is None:
            raise DomainError(404, "Item not found")
        etag = f'"item-{item_id}-v{item["version"]}"'
        headers = {"ETag": etag, "Cache-Control": "private, no-cache"}
        tags = [tag.strip().removeprefix("W/") for tag in (if_none_match or "").split(",")]
        if etag in tags or "*" in tags:
            return Response(status_code=304, headers=headers)
        return JSONResponse(item, headers=headers)

    @app.patch("/items/{item_id}")
    async def update_item(
        item_id: ItemID, body: UpdateItem, idempotency_key: Key, if_match_version: Version = None
    ):
        return await mutate("update", item_id, body, idempotency_key, if_match_version)

    @app.post("/items/{item_id}/reserve")
    async def reserve(
        item_id: ItemID,
        body: InventoryChange,
        idempotency_key: Key,
        if_match_version: Version = None,
    ):
        return await mutate("reserve", item_id, body, idempotency_key, if_match_version)

    @app.post("/items/{item_id}/release")
    async def release(
        item_id: ItemID,
        body: InventoryChange,
        idempotency_key: Key,
        if_match_version: Version = None,
    ):
        return await mutate("release", item_id, body, idempotency_key, if_match_version)

    @app.get("/events")
    async def events(
        item_id: Annotated[int | None, Query(gt=0, le=2**63 - 1)] = None,
        event_type: Annotated[
            str | None,
            Query(pattern=r"^(ITEM_CREATED|ITEM_UPDATED|INVENTORY_RESERVED|INVENTORY_RELEASED)$"),
        ] = None,
        start: Annotated[datetime | None, Query(alias="from")] = None,
        end: Annotated[datetime | None, Query(alias="to")] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
        cursor: Annotated[str | None, Query(max_length=512)] = None,
    ):
        if any(value is not None and value.tzinfo is None for value in (start, end)):
            raise DomainError(422, "Timestamps must include a timezone")
        if start and end and start > end:
            raise DomainError(422, "from must precede to")
        filters = dict(item_id=item_id, event_type=event_type, start=start, end=end)
        if cursor:
            after, upper = decode_cursor(cursor, filters)
        else:
            after, upper = (
                0,
                await app.state.pool.fetchval("SELECT value FROM event_clock WHERE singleton"),
            )
        rows = await app.state.repository.events(
            **filters, after=after, upper=upper, limit=limit + 1
        )
        more = len(rows) > limit
        rows = rows[:limit]
        return {
            "events": rows,
            "next_cursor": encode_cursor(rows[-1]["id"], upper, filters) if more else None,
            "snapshot_upper": upper,
        }

    @app.get("/metrics")
    async def get_metrics():
        return metrics.snapshot()

    @app.get("/stats")
    async def stats():
        return {
            "database": await app.state.repository.stats(),
            "cache": cache.stats(),
            "metrics": metrics.snapshot(),
        }

    @app.get("/admin/cache")
    async def cache_stats(request: Request):
        authorize_admin(request)
        return cache.stats()

    def authorize_admin(request):
        token = settings.admin_token.get_secret_value()
        if not token or not hmac.compare_digest(
            request.headers.get("X-Admin-Token", "").encode(), token.encode()
        ):
            raise DomainError(403, "Admin authorization required")

    @app.delete("/admin/cache")
    async def clear_cache(request: Request):
        authorize_admin(request)
        cache.clear()
        return {"cleared": True}

    @app.websocket("/ws/items/{item_id}")
    async def websocket(
        socket: WebSocket,
        item_id: ItemID,
        after_version: Annotated[int | None, Query(ge=0, le=2**63 - 1)] = None,
    ):
        await app.state.sockets.serve(socket, item_id, after_version)

    return app
