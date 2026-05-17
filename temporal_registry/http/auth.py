"""Optional shared-secret auth for the registry HTTP API.

This is not a substitute for a real authenticating proxy; it's a safety net so
the registry service refuses requests when the deployer has not explicitly opted
out via `auth.enabled: false` and not configured a credential.
"""

from __future__ import annotations

import hmac
import logging

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

log = logging.getLogger("temporal_registry.http.auth")

# Routes that must always be reachable so liveness probes / scrape targets
# don't require credentials. Add health endpoints here if/when they exist.
_PUBLIC_ROUTES: frozenset[str] = frozenset({"/health", "/ready"})


def constant_time_eq(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def extract_bearer(header_value: str) -> str:
    if not header_value:
        return ""
    parts = header_value.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return ""
    return parts[1].strip()


class SharedSecretAuthMiddleware(BaseHTTPMiddleware):
    """Authenticate requests against a configured shared secret.

    `token` empty + `enabled=True` → every request is rejected (fail-closed):
    the deployer asked for auth but forgot to wire a credential.
    `enabled=False` → middleware is a no-op (caller is expected to front the
    registry service with an authenticating proxy).
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        enabled: bool,
        token: str,
        metrics_path: str = "",
    ) -> None:
        super().__init__(app)
        self._enabled = enabled
        self._token = token
        self._metrics_path = metrics_path

    async def dispatch(self, request: Request, call_next):
        if not self._enabled:
            return await call_next(request)
        path = request.url.path
        if path in _PUBLIC_ROUTES or (self._metrics_path and path == self._metrics_path):
            return await call_next(request)
        if not self._token:
            log.warning("auth enabled but no token configured; rejecting %s %s", request.method, path)
            return Response("unauthorized", status_code=401)
        presented = extract_bearer(request.headers.get("authorization", ""))
        if not presented or not constant_time_eq(presented, self._token):
            return Response("unauthorized", status_code=401)
        return await call_next(request)
