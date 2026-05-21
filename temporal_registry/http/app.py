"""FastAPI application assembly for registry HTTP routes and middleware."""

from __future__ import annotations

import logging

from fastapi import FastAPI

from ..config_schemas import RegistryServiceConfig
from .auth import SharedSecretAuthMiddleware
from .body_limit import BodyLimitMiddleware, DEFAULT_MAX_BODY_BYTES
from .observability import (
    ObservabilityMiddleware,
    configure_otel,
    metrics_endpoint,
)
from .routers import health, registry, run, schedules, slug_counters
from .schemas.responses import install_openapi_schema

log = logging.getLogger("temporal_registry.http")


def build_app(
    lifespan=None,
    *,
    access_log: bool = False,
    metrics_enabled: bool = False,
    metrics_path: str = "/metrics",
    otel_enabled: bool = False,
    otel_service_name: str = "temporal-registry",
    otel_endpoint: str = "",
    otel_insecure: bool = True,
    auth_enabled: bool = False,
    auth_token: str = "",
    max_request_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
    config: RegistryServiceConfig | None = None,
    temporal_client=None,
) -> FastAPI:
    app = FastAPI(
        title="temporal-registry",
        summary="Temporal-backed workflow registry, scheduler, and HTTP dispatch gateway.",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(health.router)
    app.include_router(run.router)
    app.include_router(registry.router)
    app.include_router(registry.workflow_router)
    app.include_router(schedules.router)
    app.include_router(slug_counters.router)
    if metrics_enabled:
        app.add_api_route(
            metrics_path, metrics_endpoint, methods=["GET"], include_in_schema=False
        )
    install_openapi_schema(app)
    if config is not None:
        app.state.registry_config = config
    if temporal_client is not None:
        app.state.temporal_client = temporal_client
    app.add_middleware(ObservabilityMiddleware, access_log=access_log)
    # Body limit must run before everything else so over-sized payloads are
    # rejected without burning auth/observability work.
    app.add_middleware(BodyLimitMiddleware, max_bytes=max_request_body_bytes)
    if auth_enabled or auth_token:
        # Add auth INSIDE observability so 401s are still counted/logged.
        app.add_middleware(
            SharedSecretAuthMiddleware,
            enabled=auth_enabled,
            token=auth_token,
            metrics_path=metrics_path if metrics_enabled else "",
        )
    if otel_enabled:
        configure_otel(
            app,
            service_name=otel_service_name,
            endpoint=otel_endpoint,
            insecure=otel_insecure,
        )
    return app


# Module-level HTTP-only app. The full registry service runtime is started via
# `python -m temporal_registry`.
app = build_app()
