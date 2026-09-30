import asyncio
import hmac
import logging
import math
import re
import time
from uuid import uuid4

import asyncpg
from starlette.responses import JSONResponse

from eventvault.faults import InjectedFailure

logger = logging.getLogger("eventvault.request")


class RequestMiddleware:
    """Pure ASGI: bounded body buffering before parsing, even without Content-Length."""

    def __init__(self, app, settings, metrics, limiter):
        self.app, self.settings, self.metrics, self.limiter = app, settings, metrics, limiter

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        started, status = time.perf_counter(), 500
        response_started = False
        headers = dict(scope["headers"])
        request_id = headers.get(b"x-request-id", b"").decode("latin1")
        if not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", request_id):
            request_id = str(uuid4())
        self.metrics.inc("requests_total")

        async def wrapped_send(message):
            nonlocal status, response_started
            if message["type"] == "http.response.start":
                status = message["status"]
                response_started = True
                message["headers"] = [
                    *message["headers"],
                    (b"x-request-id", request_id.encode()),
                    (b"x-content-type-options", b"nosniff"),
                ]
            await send(message)

        async def reject(code, detail, extra=None):
            await JSONResponse({"detail": detail}, code, headers=extra)(
                scope, receive, wrapped_send
            )

        try:
            # Probes remain available during overload and do not expose business data.
            if scope["path"] not in {"/health", "/ready"}:
                ip = scope.get("client", ("unknown", 0))[0]
                allowed, retry = self.limiter.allow(ip)
                if not allowed:
                    return await reject(
                        429, "Rate limit exceeded", {"Retry-After": str(math.ceil(retry))}
                    )
                token = self.settings.api_token.get_secret_value()
                if token:
                    supplied = headers.get(b"authorization", b"").decode("latin1")
                    if not hmac.compare_digest(supplied.encode(), ("Bearer " + token).encode()):
                        return await reject(401, "Unauthorized", {"WWW-Authenticate": "Bearer"})
            body = bytearray()
            async with asyncio.timeout(10):
                while True:
                    message = await receive()
                    if message["type"] == "http.disconnect":
                        return
                    chunk = message.get("body", b"")
                    if len(body) + len(chunk) > self.settings.max_body_bytes:
                        return await reject(413, "Request body too large")
                    body.extend(chunk)
                    if not message.get("more_body", False):
                        break
            consumed = False

            async def replay_receive():
                nonlocal consumed
                if not consumed:
                    consumed = True
                    return {"type": "http.request", "body": bytes(body), "more_body": False}
                return await receive()

            await self.app(scope, replay_receive, wrapped_send)
        except Exception as exc:
            logger.error("request_id=%s error=%s", request_id, type(exc).__name__)
            if response_started:
                raise
            code = (
                503
                if isinstance(exc, (asyncpg.PostgresError, OSError, TimeoutError, InjectedFailure))
                else 500
            )
            await reject(
                code, "Service temporarily unavailable" if code == 503 else "Internal server error"
            )
        finally:
            duration = time.perf_counter() - started
            self.metrics.observe(duration)
            if status >= 400:
                self.metrics.inc("requests_failed")
            # repr escapes attacker-controlled path characters/newlines.
            logger.info(
                "request_id=%s method=%s path=%r status=%s duration=%.6f",
                request_id,
                scope["method"],
                scope["path"],
                status,
                duration,
            )
