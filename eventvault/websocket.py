import asyncio
import hmac
import logging
import time

from starlette.websockets import WebSocket, WebSocketDisconnect

logger = logging.getLogger("eventvault.websocket")


class WebSocketManager:
    """Durable ordered per-connection readers; bounded batches and send deadlines.

    The database is the queue. A slow reader cannot hold a broadcast lock or grow
    an in-memory event backlog. Replay and live delivery use the same cursor.
    """

    def __init__(self, repository, settings, metrics, faults, limiter):
        self.repository, self.settings, self.metrics = repository, settings, metrics
        self.faults, self.limiter = faults, limiter
        self.connections = set()

    async def send(self, socket, message, event=False):
        self.faults.check("websocket_send")
        await asyncio.wait_for(socket.send_json(message), self.settings.ws_send_timeout)
        if event:
            self.metrics.inc("websocket_messages_sent")

    async def serve(self, socket: WebSocket, item_id, after_version):
        token = self.settings.api_token.get_secret_value()
        if token and not hmac.compare_digest(
            socket.headers.get("authorization", "").encode(), ("Bearer " + token).encode()
        ):
            await socket.close(code=1008)
            return
        allowed, _ = self.limiter.allow(socket.client.host if socket.client else "unknown")
        if not allowed or len(self.connections) >= self.settings.ws_max_connections:
            self.metrics.inc("websocket_connections_rejected")
            await socket.close(code=1013)
            return
        # Reserve a slot before awaiting DB, so simultaneous handshakes obey capacity.
        self.connections.add(socket)
        receiver = None
        try:
            version = await self.repository.version(item_id)
            if version is None or (after_version is not None and after_version > version):
                await socket.close(code=1008)
                return
            cursor = version if after_version is None else after_version
            await socket.accept()
            self.metrics.inc("active_websockets")
            receiver = asyncio.create_task(self.receive(socket))
            heartbeat = time.monotonic()
            while not receiver.done():
                events = await self.repository.after_version(item_id, cursor)
                for event in events:
                    await self.send(
                        socket,
                        {
                            "type": event["event_type"],
                            "event_id": event["event_id"],
                            "item_id": item_id,
                            "version": event["aggregate_version"],
                            "payload": event["payload"],
                            "created_at": event["created_at"],
                        },
                        True,
                    )
                    cursor = event["aggregate_version"]
                now = time.monotonic()
                if now - heartbeat >= self.settings.ws_heartbeat:
                    await self.send(socket, {"type": "heartbeat", "after_version": cursor})
                    heartbeat = now
                if len(events) < 100:
                    await asyncio.wait({receiver}, timeout=self.settings.ws_poll_interval)
            receiver.result()
        except (WebSocketDisconnect, OSError):
            logger.info("item_id=%s result=disconnected", item_id)
        except Exception as exc:
            self.metrics.inc("websocket_messages_failed")
            logger.warning("item_id=%s result=failed error=%s", item_id, type(exc).__name__)
        finally:
            self.connections.discard(socket)
            if receiver is not None:
                self.metrics.inc("active_websockets", -1)
                receiver.cancel()
                await asyncio.gather(receiver, return_exceptions=True)
            try:
                await asyncio.wait_for(socket.close(code=1001), self.settings.ws_send_timeout)
            except (RuntimeError, OSError, TimeoutError):
                logger.debug("item_id=%s result=already_closed", item_id)

    async def receive(self, socket):
        # Transport ping/pong is handled by uvicorn; application messages are
        # consumed solely to detect disconnect, with uvicorn's size/queue bounds.
        while True:
            message = await socket.receive()
            if message["type"] == "websocket.disconnect":
                return
