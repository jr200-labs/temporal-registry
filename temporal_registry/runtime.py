"""Runtime bootstrap for the registry HTTP API and Temporal worker."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager, suppress

import uvicorn
from fastapi import FastAPI
from temporalio.worker import Worker

from .config import (
    load_registry_config,
    parse_registry_args,
)
from .http.app import build_app
from .logging_config import configure_json_logging

log = logging.getLogger("temporal_registry")
REGISTRY_SERVICE_HEARTBEAT_SECONDS = 300


@asynccontextmanager
async def registry_lifespan(app: FastAPI):
    from .temporal_client import (
        connect as _temporal_connect,
        resolve as _temporal_resolve,
    )

    config = app.state.registry_config
    address, namespace, tls, _ = _temporal_resolve(config)
    app.state.temporal_client = await _temporal_connect(config)
    from .temporal.registry.client import (
        ensure_registry_workflow,
        mark_registry_service_started,
        registry_service_heartbeat_loop,
    )
    from .temporal.registry.workflow import WorkerRegistry

    registry_worker = Worker(
        app.state.temporal_client,
        task_queue=config.registry.task_queue,
        workflows=[WorkerRegistry],
    )
    registry_task = asyncio.create_task(registry_worker.run())
    await ensure_registry_workflow(app.state.temporal_client, config)
    process_id = f"{os.uname().nodename}:{os.getpid()}"
    await mark_registry_service_started(
        app.state.temporal_client,
        process_id,
        config,
        interval_seconds=REGISTRY_SERVICE_HEARTBEAT_SECONDS,
    )
    heartbeat_task = asyncio.create_task(
        registry_service_heartbeat_loop(
            app.state.temporal_client,
            process_id,
            REGISTRY_SERVICE_HEARTBEAT_SECONDS,
            config,
        )
    )
    log.info(
        "registry service bootstrap: temporal=%s ns=%s tls=%s registry_task_queue=%s",
        address,
        namespace,
        tls,
        config.registry.task_queue,
    )
    try:
        yield
    finally:
        heartbeat_task.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat_task
        registry_task.cancel()
        with suppress(asyncio.CancelledError):
            await registry_task


def _warn_if_public_bind_no_auth(host: str) -> None:
    if host in ("127.0.0.1", "localhost", "::1"):
        return
    log.warning(
        "registry service bound to %s with no built-in auth; place it behind an authenticated proxy",
        host,
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_registry_args(sys.argv[1:] if argv is None else argv)
    config = load_registry_config(args.config)
    configure_json_logging(config.observability.logging.level)
    host = config.server.host
    port = config.server.port
    _warn_if_public_bind_no_auth(host)
    log.info("registry service listening on %s:%d", host, port)
    if config.auth.enabled and not config.auth.token:
        log.warning(
            "auth.enabled=true but auth.token is empty; the registry service "
            "will reject every request until a token is configured"
        )
    app = build_app(
        lifespan=registry_lifespan,
        access_log=config.observability.logging.access,
        metrics_enabled=config.observability.metrics.enabled,
        metrics_path=config.observability.metrics.path,
        otel_enabled=config.observability.otel.enabled,
        otel_service_name=config.observability.otel.service_name,
        otel_endpoint=config.observability.otel.endpoint,
        otel_insecure=config.observability.otel.insecure,
        auth_enabled=config.auth.enabled,
        auth_token=config.auth.token,
        max_request_body_bytes=config.server.max_request_body_bytes,
        config=config,
    )
    uvicorn.run(app, host=host, port=port, log_level=config.observability.logging.level.lower())


if __name__ == "__main__":
    main()
