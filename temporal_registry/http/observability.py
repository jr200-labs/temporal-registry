"""HTTP observability middleware, metrics, and OpenTelemetry setup."""

from __future__ import annotations

import logging
import time
import uuid
from collections import defaultdict
from collections.abc import Awaitable, Callable

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from fastapi import Request, Response
from fastapi.responses import PlainTextResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from ..logging_config import log_context
from .schemas.observability import MetricsSnapshot

log = logging.getLogger("temporal_registry.http.access")


_DEFAULT_LATENCY_BUCKETS = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)


class MetricsStore:
    """Wraps prometheus_client collectors so we get correct exposition format
    + histograms (real percentiles) without giving up the snapshot/test API."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self._build(registry)

    def _build(self, registry: CollectorRegistry | None) -> None:
        self._registry = registry or CollectorRegistry()
        self._requests_total = Counter(
            "omp_http_requests_total",
            "Total HTTP requests.",
            registry=self._registry,
        )
        self._errors_total = Counter(
            "omp_http_errors_total",
            "Total HTTP 5xx responses.",
            registry=self._registry,
        )
        self._in_flight = Gauge(
            "omp_http_in_flight",
            "In-flight HTTP requests.",
            registry=self._registry,
        )
        self._latency = Histogram(
            "omp_http_latency_seconds",
            "Request latency seconds.",
            buckets=_DEFAULT_LATENCY_BUCKETS,
            registry=self._registry,
        )
        self._route_requests = Counter(
            "omp_http_route_requests_total",
            "Total HTTP requests by route.",
            labelnames=("route",),
            registry=self._registry,
        )
        # Snapshot-friendly bookkeeping for tests and ad-hoc inspection.
        self.requests_total = 0
        self.errors_total = 0
        self.in_flight = 0
        self.latency_seconds_sum = 0.0
        self.latency_seconds_count = 0
        self.by_route: defaultdict[str, int] = defaultdict(int)

    def reset(self) -> None:
        """Recreate all collectors against a fresh registry. Use between tests
        or for one-shot benchmarks; not safe under concurrent traffic."""
        self._build(None)

    def record_start(self) -> None:
        self._requests_total.inc()
        self._in_flight.inc()
        self.requests_total += 1
        self.in_flight += 1

    def record_end(self, route: str, status_code: int, latency_seconds: float) -> None:
        latency = max(0.0, latency_seconds)
        self._in_flight.dec()
        self._latency.observe(latency)
        self._route_requests.labels(route=route).inc()
        if status_code >= 500:
            self._errors_total.inc()
        self.in_flight = max(0, self.in_flight - 1)
        self.latency_seconds_sum += latency
        self.latency_seconds_count += 1
        self.by_route[route] += 1
        if status_code >= 500:
            self.errors_total += 1

    def snapshot(self) -> MetricsSnapshot:
        return MetricsSnapshot(
            requests_total=self.requests_total,
            errors_total=self.errors_total,
            in_flight=self.in_flight,
            latency_seconds_sum=self.latency_seconds_sum,
            latency_seconds_count=self.latency_seconds_count,
            by_route=dict(sorted(self.by_route.items())),
        )

    def render_prometheus(self) -> bytes:
        return generate_latest(self._registry)

    @property
    def content_type(self) -> str:
        return CONTENT_TYPE_LATEST


METRICS = MetricsStore()


async def metrics_endpoint(_request: Request) -> Response:
    return PlainTextResponse(
        METRICS.render_prometheus(), media_type=METRICS.content_type
    )


def route_name(request: Request) -> str:
    route = request.scope.get("route")
    path = getattr(route, "path", "")
    return str(path or "unknown")


def observability_middleware(
    access_log: bool,
) -> Callable[[Request, Callable[[Request], Awaitable[Response]]], Awaitable[Response]]:
    async def middleware(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        started = time.perf_counter()
        METRICS.record_start()
        status_code = 500
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        try:
            with log_context(request_id=request_id):
                response = await call_next(request)
            status_code = response.status_code
            response.headers.setdefault("x-request-id", request_id)
            return response
        finally:
            latency = time.perf_counter() - started
            route = route_name(request)
            METRICS.record_end(route, status_code, latency)
            if access_log:
                with log_context(request_id=request_id):
                    log.info(
                        "http request",
                        extra={
                            "method": request.method,
                            "route": route,
                            "status": status_code,
                            "duration_ms": round(latency * 1000, 2),
                        },
                    )

    return middleware


class ObservabilityMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, access_log: bool = False) -> None:
        super().__init__(app)
        self.access_log = access_log

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        return await observability_middleware(access_log=self.access_log)(
            request, call_next
        )


def configure_otel(
    app: ASGIApp, service_name: str, endpoint: str, insecure: bool
) -> None:
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError as e:
        logging.getLogger("temporal_registry.otel").warning(
            "otel disabled because dependencies are unavailable: %s",
            e,
        )
        return

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    if endpoint:
        exporter = OTLPSpanExporter(endpoint=endpoint, insecure=insecure)
        provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app)
