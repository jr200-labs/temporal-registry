"""ASGI middleware for rejecting oversized HTTP request bodies."""

from __future__ import annotations

from starlette.types import ASGIApp, Message, Receive, Scope, Send

DEFAULT_MAX_BODY_BYTES = 1 * 1024 * 1024  # 1 MiB


class BodyLimitMiddleware:
    def __init__(
        self, app: ASGIApp, *, max_bytes: int = DEFAULT_MAX_BODY_BYTES
    ) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        limit = self.max_bytes

        # Fast path: an honest, oversized Content-Length is rejected without
        # consuming the body at all.
        for name, value in scope.get("headers", []):
            if name == b"content-length":
                try:
                    declared = int(value)
                except ValueError:
                    await _too_large(send, 0, limit)
                    return
                if declared > limit:
                    await _too_large(send, declared, limit)
                    return
                break

        # Streaming path: tally bytes as they arrive; once over limit we drop
        # whatever response the app is trying to produce and emit a 413 once.
        state = {"received": 0, "over": False, "responded": False}

        async def limited_receive() -> Message:
            msg = await receive()
            if msg.get("type") == "http.request":
                body = msg.get("body") or b""
                state["received"] += len(body)
                if state["received"] > limit:
                    state["over"] = True
            return msg

        async def guarded_send(msg: Message) -> None:
            if state["over"]:
                if not state["responded"]:
                    state["responded"] = True
                    await _too_large(send, state["received"], limit)
                return  # drop subsequent messages from the app
            await send(msg)

        await self.app(scope, limited_receive, guarded_send)


async def _too_large(send: Send, received: int, limit: int) -> None:
    body = f"request body exceeds {limit} bytes (received >= {received})".encode(
        "utf-8"
    )
    await send(
        {
            "type": "http.response.start",
            "status": 413,
            "headers": [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"content-length", str(len(body)).encode("ascii")),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body, "more_body": False})
