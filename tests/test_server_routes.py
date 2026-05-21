"""Unit tests for registry HTTP route handlers and OpenAPI wiring."""

import asyncio
import json
from types import SimpleNamespace
from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi.responses import PlainTextResponse
from fastapi.testclient import TestClient

from temporal_registry.http.app import build_app
from temporal_registry.http.observability import METRICS
from temporal_registry.config_schemas import RegistryServiceConfig
from temporal_registry.temporal.registry.registry_schemas import (
    InputWarning,
    SearchAttributeReconcileReport,
    RegistryWorkflowSpec,
    SearchAttributeSpec,
    SearchAttributeSummary,
    TemporalSearchAttribute,
)
from temporal_registry.http.routers import registry as routes_registry
from temporal_registry.http.routers import run as routes_run
from temporal_registry.http.routers import schedules as routes_schedules
from temporal_registry.http.routers import slug_counters as routes_slug
from temporal_registry.temporal.registry.registry_schemas import (
    ClaimSlugIdResponse,
    ResetSlugResponse,
    SlugCounterSummary,
)


def _config() -> RegistryServiceConfig:
    return RegistryServiceConfig.model_validate(
        {
            "temporal": {
                "address": "127.0.0.1:7233",
                "namespace": "default",
                "tls": False,
                "api_key": "",
            },
            "server": {"host": "127.0.0.1", "port": 8080},
            "registry": {
                "workflow_id": "registry",
                "workflow_type": "registry.workflow",
                "task_queue": "registry-task-queue",
            },
            "observability": {
                "logging": {"level": "INFO", "access": False},
                "metrics": {"enabled": True, "path": "/metrics"},
                "otel": {
                    "enabled": False,
                    "service_name": "test",
                    "endpoint": "",
                    "insecure": True,
                },
            },
        }
    )


class _Handle:
    id = "run-agent-hello-world-test"
    first_execution_run_id = "run-id"


class _Client:
    def __init__(self) -> None:
        self.started: dict = {}
        self.created_schedule: dict = {}
        self.schedule_handle = _ScheduleHandle()

    async def start_workflow(self, workflow_type, payload, **kwargs):
        self.started = {"workflow_type": workflow_type, "payload": payload, **kwargs}
        return _Handle()

    async def create_schedule(self, schedule_id, schedule):
        self.created_schedule = {"schedule_id": schedule_id, "schedule": schedule}

    def get_schedule_handle(self, schedule_id):
        self.schedule_handle.schedule_id = schedule_id
        return self.schedule_handle


class _ScheduleHandle:
    def __init__(self) -> None:
        self.schedule_id = ""
        self.triggered = False

    async def trigger(self, **_kwargs):
        self.triggered = True


@dataclass
class _Target:
    task_queue: str = "agent-harness"
    input_schema: dict | None = None
    schedule_input_warnings: list[InputWarning] | None = None
    search_attributes: list[SearchAttributeSpec] | None = None

    def __post_init__(self) -> None:
        if self.search_attributes is None:
            self.search_attributes = [
                SearchAttributeSpec(name="agent_id", type="Keyword"),
                SearchAttributeSpec(name="agent_acp_provider", type="Keyword"),
                SearchAttributeSpec(name="agent_event_types", type="KeywordList"),
                SearchAttributeSpec(name="tools_used", type="KeywordList"),
            ]
        if self.schedule_input_warnings is None:
            self.schedule_input_warnings = []


class _Request:
    client = None

    def __init__(self, body: dict, path_params: dict | None = None) -> None:
        self._body = body
        self.path_params = path_params or {}
        self.app = SimpleNamespace(
            state=SimpleNamespace(
                temporal_client=self.client,
                registry_config=_config(),
            )
        )

    async def json(self) -> dict:
        return self._body


def _workflow_spec_payload(**overrides):
    payload = {
        "workflow_type": "example.workflow.v1",
        "version": "1",
        "task_queue": "example-task-queue",
        "description": "Example workflow.",
        "input_schema": {"type": "object"},
        "schedule_input_warnings": [],
        "search_attributes": [],
        "enabled": True,
        "labels": {"component": "example"},
    }
    payload.update(overrides)
    return payload


def test_metrics_endpoint_is_configurable_and_records_requests() -> None:
    METRICS.reset()

    async def ok(_request):
        return PlainTextResponse("ok")

    app = build_app(metrics_enabled=True, metrics_path="/internal/metrics")
    app.add_route("/ok", ok, methods=["GET"])
    client = TestClient(app)

    assert client.get("/ok").status_code == 200
    response = client.get("/internal/metrics")

    assert response.status_code == 200
    assert "omp_http_requests_total" in response.text
    assert "omp_http_latency_seconds_count 1" in response.text
    assert 'omp_http_route_requests_total{route="unknown"} 1' in response.text


def test_metrics_endpoint_can_be_disabled() -> None:
    app = build_app(metrics_enabled=False, metrics_path="/internal/metrics")
    client = TestClient(app)

    assert client.get("/internal/metrics").status_code == 404


def test_openapi_documents_registry_routes() -> None:
    app = build_app()
    client = TestClient(app)

    response = client.get("/openapi.json")

    assert response.status_code == 200
    spec = response.json()
    assert spec["info"]["title"] == "temporal-registry"
    assert "/run" in spec["paths"]
    assert "/health" in spec["paths"]
    assert "/ready" in spec["paths"]
    assert "/registry/workflows" in spec["paths"]
    assert "/registry/status" in spec["paths"]
    assert "/registry/temporal/search-attributes" in spec["paths"]
    assert "post" in spec["paths"]["/registry/temporal/search-attributes"]
    assert "/workflows/{workflow_type}/start" in spec["paths"]
    assert "/schedules/{schedule_id}" in spec["paths"]
    assert "RunRequest" in spec["components"]["schemas"]
    assert "RegistryWorkflowSpec" in spec["components"]["schemas"]
    assert "RegistryStatus" in spec["components"]["schemas"]
    assert "ScheduleStartRequest" in spec["components"]["schemas"]
    assert "SearchAttributeReconcileRequest" in spec["components"]["schemas"]
    assert "SearchAttributeReconcileReport" in spec["components"]["schemas"]


def test_health_endpoint_does_not_require_dependencies() -> None:
    app = build_app()
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_post_run_starts_run_agent_workflow() -> None:
    old_client = _Request.client
    client = _Client()
    _Request.client = client
    old_resolve = routes_run.resolve_workflow

    async def fake_resolve(_client, _workflow_type, _config):
        return _Target()

    routes_run.resolve_workflow = fake_resolve
    try:
        response = asyncio.run(
            routes_run.post_run(
                _Request(
                    {
                        "agent_id": "hello-world",
                        "workspace": "/tmp/work",
                        "prompt": "List files.",
                        "chain": [],
                    }
                )
            )
        )
    finally:
        _Request.client = old_client
        routes_run.resolve_workflow = old_resolve

    assert response.status_code == 200
    assert client.started["workflow_type"] == "agent.run.v1"
    assert client.started["payload"]["agent_id"] == "hello-world"
    assert client.started["payload"]["workspace"] == "/tmp/work"


def test_post_run_uses_request_workflow_type() -> None:
    old_client = _Request.client
    client = _Client()
    _Request.client = client
    old_resolve = routes_run.resolve_workflow

    async def fake_resolve(_client, workflow_type: str, _config):
        assert workflow_type == "custom.agent.v1"
        return _Target()

    routes_run.resolve_workflow = fake_resolve
    try:
        response = asyncio.run(
            routes_run.post_run(
                _Request(
                    {
                        "workflow_type": "custom.agent.v1",
                        "agent_id": "hello-world",
                        "workspace": "/tmp/work",
                        "prompt": "List files.",
                    }
                )
            )
        )
    finally:
        _Request.client = old_client
        routes_run.resolve_workflow = old_resolve

    assert response.status_code == 200
    assert client.started["workflow_type"] == "custom.agent.v1"


def test_post_run_requires_chain_mode_when_chain_is_set() -> None:
    response = asyncio.run(
        routes_run.post_run(
            _Request(
                {
                    "agent_id": "hello-world",
                    "workspace": "/tmp/work",
                    "prompt": "List files.",
                    "chain": [{"agent_id": "reviewer", "prompt": "Review."}],
                }
            )
        )
    )

    assert response.status_code == 400


def test_delete_registry_workflow_signals_unregister() -> None:
    old_client = _Request.client
    client = object()
    _Request.client = client
    calls: list[tuple[object, str]] = []
    old_unregister = routes_registry.unregister_workflow

    async def fake_unregister(c, workflow_type: str, _config) -> None:
        calls.append((c, workflow_type))

    routes_registry.unregister_workflow = fake_unregister
    try:
        response = asyncio.run(
            routes_registry.delete_registry_workflow(
                _Request({}, {"workflow_type": "agent.run.v1"}), "agent.run.v1"
            )
        )
    finally:
        _Request.client = old_client
        routes_registry.unregister_workflow = old_unregister

    assert response.status_code == 204
    assert calls == [(client, "agent.run.v1")]


def test_post_registry_workflow_creates_when_absent() -> None:
    old_client = _Request.client
    client = object()
    _Request.client = client
    old_get = routes_registry.get_workflow
    old_put = routes_registry.put_workflow
    calls: list[tuple[object, RegistryWorkflowSpec, bool]] = []

    async def fake_get(c, workflow_type: str, _config):
        assert c is client
        assert workflow_type == "example.workflow.v1"
        return None

    async def fake_put(
        c, spec: RegistryWorkflowSpec, _config, *, overwrite: bool
    ) -> None:
        calls.append((c, spec, overwrite))

    routes_registry.get_workflow = fake_get
    routes_registry.put_workflow = fake_put
    try:
        response = asyncio.run(
            routes_registry.post_registry_workflow(_Request(_workflow_spec_payload()))
        )
    finally:
        _Request.client = old_client
        routes_registry.get_workflow = old_get
        routes_registry.put_workflow = old_put

    assert response.status_code == 201
    assert calls[0][0] is client
    assert calls[0][1].workflow_type == "example.workflow.v1"
    assert calls[0][2] is False


def test_post_registry_workflow_rejects_existing() -> None:
    old_client = _Request.client
    client = object()
    _Request.client = client
    old_get = routes_registry.get_workflow
    old_put = routes_registry.put_workflow
    calls: list[object] = []

    async def fake_get(_client, _workflow_type: str, _config):
        return object()

    async def fake_put(*args, **kwargs) -> None:
        calls.append((args, kwargs))

    routes_registry.get_workflow = fake_get
    routes_registry.put_workflow = fake_put
    try:
        response = asyncio.run(
            routes_registry.post_registry_workflow(_Request(_workflow_spec_payload()))
        )
    finally:
        _Request.client = old_client
        routes_registry.get_workflow = old_get
        routes_registry.put_workflow = old_put

    assert response.status_code == 409
    assert calls == []


def test_put_registry_workflow_replaces_full_spec() -> None:
    old_client = _Request.client
    client = object()
    _Request.client = client
    old_put = routes_registry.put_workflow
    calls: list[tuple[object, RegistryWorkflowSpec, bool]] = []

    async def fake_put(
        c, spec: RegistryWorkflowSpec, _config, *, overwrite: bool
    ) -> None:
        calls.append((c, spec, overwrite))

    routes_registry.put_workflow = fake_put
    try:
        response = asyncio.run(
            routes_registry.put_registry_workflow(
                _Request(
                    _workflow_spec_payload(description="Updated."),
                    {"workflow_type": "example.workflow.v1"},
                ),
                "example.workflow.v1",
            )
        )
    finally:
        _Request.client = old_client
        routes_registry.put_workflow = old_put

    assert response.status_code == 200
    assert calls[0][0] is client
    assert calls[0][1].description == "Updated."
    assert calls[0][2] is True


def test_put_registry_workflow_requires_path_body_match() -> None:
    response = asyncio.run(
        routes_registry.put_registry_workflow(
            _Request(
                _workflow_spec_payload(workflow_type="different.workflow.v1"),
                {"workflow_type": "example.workflow.v1"},
            ),
            "example.workflow.v1",
        )
    )

    assert response.status_code == 400


def test_post_registry_shutdown_signals_shutdown() -> None:
    old_client = _Request.client
    client = object()
    _Request.client = client
    calls: list[object] = []
    old_shutdown = routes_registry.shutdown_registry

    async def fake_shutdown(c, _config) -> None:
        calls.append(c)

    routes_registry.shutdown_registry = fake_shutdown
    try:
        response = asyncio.run(routes_registry.post_registry_shutdown(_Request({})))
    finally:
        _Request.client = old_client
        routes_registry.shutdown_registry = old_shutdown

    assert response.status_code == 202
    assert calls == [client]


def test_get_registry_search_attributes_returns_aggregate_metadata() -> None:
    old_client = _Request.client
    client = object()
    _Request.client = client
    old_list = routes_registry.list_search_attributes

    async def fake_list(c, _config):
        assert c is client
        return [
            SearchAttributeSummary(
                name="agent_id",
                type="Keyword",
                description="Agent identifier.",
                workflows=["agent.run.v1", "slack.poll.v1"],
            )
        ]

    routes_registry.list_search_attributes = fake_list
    try:
        response = asyncio.run(
            routes_registry.get_registry_search_attributes(_Request({}))
        )
    finally:
        _Request.client = old_client
        routes_registry.list_search_attributes = old_list

    assert response.status_code == 200
    assert json.loads(response.body) == {
        "search_attributes": [
            {
                "name": "agent_id",
                "type": "Keyword",
                "description": "Agent identifier.",
                "workflows": ["agent.run.v1", "slack.poll.v1"],
            }
        ]
    }


def test_get_temporal_search_attributes_returns_namespace_attributes() -> None:
    old_client = _Request.client
    client = object()
    _Request.client = client
    old_list = routes_registry.list_temporal_search_attributes

    async def fake_list(c, namespace: str):
        assert c is client
        assert namespace == "default"
        return [
            TemporalSearchAttribute(
                name="agent_id",
                type="Keyword",
                source="custom",
            )
        ]

    routes_registry.list_temporal_search_attributes = fake_list
    try:
        response = asyncio.run(
            routes_registry.get_temporal_search_attributes(_Request({}))
        )
    finally:
        _Request.client = old_client
        routes_registry.list_temporal_search_attributes = old_list

    assert response.status_code == 200
    assert json.loads(response.body) == {
        "search_attributes": [
            {"name": "agent_id", "type": "Keyword", "source": "custom"}
        ]
    }


def test_post_temporal_search_attributes_reconcile_runs_requested_mode() -> None:
    old_client = _Request.client
    client = object()
    _Request.client = client
    old_reconcile = routes_registry.reconcile_search_attributes
    calls: list[dict] = []

    async def fake_reconcile(c, _config, **kwargs):
        assert c is client
        calls.append(kwargs)
        return SearchAttributeReconcileReport(mode=kwargs["mode"])

    routes_registry.reconcile_search_attributes = fake_reconcile
    try:
        response = asyncio.run(
            routes_registry.post_temporal_search_attributes_reconcile(
                _Request(
                    {
                        "mode": "replace",
                        "attributes": ["agent_id"],
                        "confirm": True,
                    }
                )
            )
        )
    finally:
        _Request.client = old_client
        routes_registry.reconcile_search_attributes = old_reconcile

    assert response.status_code == 200
    assert calls == [{"mode": "replace", "attributes": ["agent_id"], "confirm": True}]


def test_post_schedule_creates_temporal_schedule_from_registry_target() -> None:
    old_client = _Request.client
    client = _Client()
    _Request.client = client
    old_resolve = routes_schedules.resolve_workflow

    async def fake_resolve(_client, workflow_type: str, _config):
        assert workflow_type == "slack.poll.v1"
        return _Target(task_queue="agent-harness")

    routes_schedules.resolve_workflow = fake_resolve
    try:
        response = asyncio.run(
            routes_schedules.post_schedule(
                _Request(
                    {
                        "workflow_type": "slack.poll.v1",
                        "workflow_id": "slack-poll",
                        "input": {"target_user_id": "U123"},
                        "interval_seconds": 60,
                        "overlap_policy": "skip",
                        "search_attributes": {"agent_id": "slack-bridge"},
                        "note": "polls Slack",
                    },
                    {"schedule_id": "slack-bridge"},
                ),
                "slack-bridge",
            )
        )
    finally:
        _Request.client = old_client
        routes_schedules.resolve_workflow = old_resolve

    assert response.status_code == 201
    assert client.created_schedule["schedule_id"] == "slack-bridge"
    schedule = client.created_schedule["schedule"]
    assert schedule.action.workflow == "slack.poll.v1"
    assert schedule.action.args == [{"target_user_id": "U123"}]
    assert schedule.action.id == "slack-poll"
    assert schedule.action.task_queue == "agent-harness"
    assert schedule.spec.intervals[0].every.total_seconds() == 60


def test_post_schedule_accepts_timezone_aware_datetimes() -> None:
    old_client = _Request.client
    client = _Client()
    _Request.client = client
    old_resolve = routes_schedules.resolve_workflow

    async def fake_resolve(_client, workflow_type: str, _config):
        assert workflow_type == "agent.run.v1"
        return _Target(task_queue="agent-harness")

    routes_schedules.resolve_workflow = fake_resolve
    try:
        response = asyncio.run(
            routes_schedules.post_schedule(
                _Request(
                    {
                        "workflow_type": "agent.run.v1",
                        "start_at": "2026-05-16T10:00:00+09:00",
                        "end_at": "2026-05-16T11:00:00+09:00",
                    },
                    {"schedule_id": "agent-run-window"},
                ),
                "agent-run-window",
            )
        )
    finally:
        _Request.client = old_client
        routes_schedules.resolve_workflow = old_resolve

    assert response.status_code == 201
    schedule = client.created_schedule["schedule"]
    assert schedule.spec.start_at.isoformat() == "2026-05-16T01:00:00+00:00"
    assert schedule.spec.end_at.isoformat() == "2026-05-16T02:00:00+00:00"


def test_post_schedule_rejects_naive_datetimes() -> None:
    old_client = _Request.client
    client = _Client()
    _Request.client = client
    old_resolve = routes_schedules.resolve_workflow

    async def fake_resolve(_client, _workflow_type: str, _config):
        return _Target(task_queue="agent-harness")

    routes_schedules.resolve_workflow = fake_resolve
    try:
        response = asyncio.run(
            routes_schedules.post_schedule(
                _Request(
                    {
                        "workflow_type": "agent.run.v1",
                        "start_at": "2026-05-16T10:00:00",
                    },
                    {"schedule_id": "agent-run-naive"},
                ),
                "agent-run-naive",
            )
        )
    finally:
        _Request.client = old_client
        routes_schedules.resolve_workflow = old_resolve

    assert response.status_code == 400
    assert b"timezone" in response.body


def test_post_schedule_supports_relative_fire_offsets() -> None:
    old_client = _Request.client
    client = _Client()
    _Request.client = client
    old_resolve = routes_schedules.resolve_workflow

    async def fake_resolve(_client, workflow_type: str, _config):
        assert workflow_type == "agent.run.v1"
        return _Target(task_queue="agent-harness")

    routes_schedules.resolve_workflow = fake_resolve
    try:
        response = asyncio.run(
            routes_schedules.post_schedule(
                _Request(
                    {
                        "workflow_type": "agent.run.v1",
                        "workflow_id": "agent-run-offsets",
                        "input": {"agent_id": "hello-world"},
                        "fire_offsets_seconds": [0, 10, 20, 30],
                        "search_attributes": {"agent_id": "hello-world"},
                    },
                    {"schedule_id": "agent-run-offsets"},
                ),
                "agent-run-offsets",
            )
        )
    finally:
        _Request.client = old_client
        routes_schedules.resolve_workflow = old_resolve

    assert response.status_code == 201
    assert client.schedule_handle.triggered is True
    assert client.schedule_handle.schedule_id == "agent-run-offsets"
    schedule = client.created_schedule["schedule"]
    assert len(schedule.spec.calendars) == 3
    assert not schedule.spec.intervals
    assert schedule.spec.start_at is not None
    assert schedule.spec.end_at is not None
    assert schedule.spec.start_at.tzinfo is not None
    assert schedule.spec.end_at.tzinfo is not None
    assert schedule.spec.time_zone_name == "UTC"


def test_post_schedule_returns_generic_missing_input_warning() -> None:
    old_client = _Request.client
    client = _Client()
    _Request.client = client
    old_resolve = routes_schedules.resolve_workflow

    async def fake_resolve(_client, workflow_type: str, _config):
        assert workflow_type == "agent.run.v1"
        return _Target(
            task_queue="agent-harness",
            schedule_input_warnings=[
                InputWarning(
                    field="agent_acp_provider",
                    message="agent_acp_provider is missing; this will only work if the agent.yaml provider is set.",
                )
            ],
        )

    routes_schedules.resolve_workflow = fake_resolve
    try:
        response = asyncio.run(
            routes_schedules.post_schedule(
                _Request(
                    {
                        "workflow_type": "agent.run.v1",
                        "workflow_id": "agent-run-offsets",
                        "input": {"agent_id": "hello-world"},
                        "fire_offsets_seconds": [10],
                        "search_attributes": {"agent_id": "hello-world"},
                    },
                    {"schedule_id": "agent-run-offsets"},
                ),
                "agent-run-offsets",
            )
        )
    finally:
        _Request.client = old_client
        routes_schedules.resolve_workflow = old_resolve

    assert response.status_code == 201
    assert json.loads(response.body) == {
        "warnings": [
            {
                "field": "agent_acp_provider",
                "message": "agent_acp_provider is missing; this will only work if the agent.yaml provider is set.",
            }
        ]
    }


def test_post_schedule_validates_registered_input_schema() -> None:
    old_client = _Request.client
    client = _Client()
    _Request.client = client
    old_resolve = routes_schedules.resolve_workflow

    async def fake_resolve(_client, workflow_type: str, _config):
        assert workflow_type == "agent.run.v1"
        return _Target(
            task_queue="agent-harness",
            input_schema={
                "type": "object",
                "required": ["agent_id", "workspace", "prompt"],
                "properties": {
                    "agent_id": {"type": "string"},
                    "workspace": {"type": "string"},
                    "prompt": {"type": "string"},
                },
            },
        )

    routes_schedules.resolve_workflow = fake_resolve
    try:
        response = asyncio.run(
            routes_schedules.post_schedule(
                _Request(
                    {
                        "workflow_type": "agent.run.v1",
                        "input": {"agent_id": "hello-world"},
                        "fire_offsets_seconds": [10],
                    },
                    {"schedule_id": "agent-run-offsets"},
                ),
                "agent-run-offsets",
            )
        )
    finally:
        _Request.client = old_client
        routes_schedules.resolve_workflow = old_resolve

    assert response.status_code == 400
    payload = json.loads(response.body)
    assert payload["error"] == "invalid workflow input"
    assert {tuple(item["loc"]) for item in payload["details"]} == {
        ("workspace",),
        ("prompt",),
    }


def test_post_schedule_suppresses_warning_when_input_field_is_present() -> None:
    old_client = _Request.client
    client = _Client()
    _Request.client = client
    old_resolve = routes_schedules.resolve_workflow

    async def fake_resolve(_client, workflow_type: str, _config):
        assert workflow_type == "agent.run.v1"
        return _Target(
            task_queue="agent-harness",
            schedule_input_warnings=[
                InputWarning(
                    field="agent_acp_provider",
                    message="agent_acp_provider is missing; this will only work if the agent.yaml provider is set.",
                )
            ],
        )

    routes_schedules.resolve_workflow = fake_resolve
    try:
        response = asyncio.run(
            routes_schedules.post_schedule(
                _Request(
                    {
                        "workflow_type": "agent.run.v1",
                        "workflow_id": "agent-run-offsets",
                        "input": {
                            "agent_id": "hello-world",
                            "agent_acp_provider": "opencode",
                        },
                        "fire_offsets_seconds": [10],
                        "search_attributes": {
                            "agent_id": "hello-world",
                            "agent_acp_provider": "opencode",
                        },
                    },
                    {"schedule_id": "agent-run-offsets"},
                ),
                "agent-run-offsets",
            )
        )
    finally:
        _Request.client = old_client
        routes_schedules.resolve_workflow = old_resolve

    assert response.status_code == 201
    assert response.body == b""


def test_schedule_input_warnings_are_generic_for_multiple_fields() -> None:
    warnings = routes_schedules._schedule_input_warnings(
        {"required_one": "present", "required_two": ""},
        [
            InputWarning(field="required_one", message="required_one missing"),
            InputWarning(field="required_two", message="required_two missing"),
            InputWarning(field="required_three", message="required_three missing"),
        ],
    )

    assert warnings == [
        {"field": "required_two", "message": "required_two missing"},
        {"field": "required_three", "message": "required_three missing"},
    ]


def test_schedule_datetime_rendering_normalizes_to_aware_utc() -> None:
    naive = datetime(2026, 5, 16, 1, 2, 3)
    aware = routes_schedules._aware_utc(naive)

    assert aware.tzinfo is timezone.utc
    assert routes_schedules._aware_utc_isoformat(naive) == "2026-05-16T01:02:03+00:00"


def test_post_schedule_rejects_unknown_search_attribute() -> None:
    old_client = _Request.client
    client = _Client()
    _Request.client = client
    old_resolve = routes_schedules.resolve_workflow

    async def fake_resolve(_client, _workflow_type: str, _config):
        return _Target(task_queue="agent-harness")

    routes_schedules.resolve_workflow = fake_resolve
    try:
        response = asyncio.run(
            routes_schedules.post_schedule(
                _Request(
                    {
                        "workflow_type": "slack.poll.v1",
                        "search_attributes": {"unsupported": "value"},
                    },
                    {"schedule_id": "slack-bridge"},
                ),
                "slack-bridge",
            )
        )
    finally:
        _Request.client = old_client
        routes_schedules.resolve_workflow = old_resolve

    assert response.status_code == 400
    assert json.loads(response.body) == {
        "error": "unsupported search attribute for slack.poll.v1: unsupported; supported: agent_acp_provider, agent_event_types, agent_id, tools_used"
    }


# ---------- slug counter routes -------------------------------------------


def test_post_run_with_name_claims_slug_and_uses_it() -> None:
    """When the run request carries `name`, the route must claim a
    slug counter via the registry workflow and use the returned
    `workflow_id` instead of the random hex fallback."""
    old_client = _Request.client
    client = _Client()
    _Request.client = client
    old_resolve = routes_run.resolve_workflow
    old_claim = routes_run.claim_slug_id

    async def fake_resolve(_client, _workflow_type, _config):
        return _Target()

    async def fake_claim(_client, name: str, _config):
        return ClaimSlugIdResponse(slug=name, counter=7, workflow_id=f"{name}-r7")

    routes_run.resolve_workflow = fake_resolve
    routes_run.claim_slug_id = fake_claim
    try:
        response = asyncio.run(
            routes_run.post_run(
                _Request(
                    {
                        "agent_id": "hello-world",
                        "workspace": "/tmp/work",
                        "prompt": "x",
                        "name": "smoke",
                    }
                )
            )
        )
    finally:
        _Request.client = old_client
        routes_run.resolve_workflow = old_resolve
        routes_run.claim_slug_id = old_claim

    assert response.status_code == 200
    assert client.started["id"] == "smoke-r7"
    # `name` must NOT be passed through to the workflow input.
    assert "name" not in client.started["payload"]


def test_post_workflow_start_rejects_both_workflow_id_and_name() -> None:
    """The model validator on WorkflowStartRequest rejects requests
    that try to set both fields; the route returns 400 with the
    pydantic-formatted error rather than crashing."""
    response = asyncio.run(
        routes_registry.post_workflow_start(
            _Request(
                {
                    "input": {},
                    "workflow_id": "explicit-id",
                    "name": "claimed",
                },
                {"workflow_type": "agent.run.v1"},
            ),
            "agent.run.v1",
        )
    )
    assert response.status_code == 400
    body = json.loads(response.body)
    assert body["error"] == "invalid request"


def test_workflow_ids_claim_route_returns_claimed_id() -> None:
    old_client = _Request.client
    client = _Client()
    _Request.client = client
    old_claim = routes_slug.claim_slug_id

    async def fake_claim(_client, name: str, _config):
        return ClaimSlugIdResponse(slug=name, counter=42, workflow_id=f"{name}-r42")

    routes_slug.claim_slug_id = fake_claim
    try:
        response = asyncio.run(
            routes_slug.post_claim(_Request({"name": "tui-build"}))
        )
    finally:
        _Request.client = old_client
        routes_slug.claim_slug_id = old_claim

    assert response.status_code == 200
    body = json.loads(response.body)
    assert body == {"slug": "tui-build", "counter": 42, "workflow_id": "tui-build-r42"}


def test_workflow_ids_reset_route_returns_previous_counter() -> None:
    old_client = _Request.client
    client = _Client()
    _Request.client = client
    old_reset = routes_slug.reset_slug

    async def fake_reset(_client, name: str, _config):
        return ResetSlugResponse(slug=name, previous_counter=9, reset_to=0)

    routes_slug.reset_slug = fake_reset
    try:
        response = asyncio.run(
            routes_slug.post_reset(_Request({"name": "tui-build"}))
        )
    finally:
        _Request.client = old_client
        routes_slug.reset_slug = old_reset

    assert response.status_code == 200
    body = json.loads(response.body)
    assert body == {"slug": "tui-build", "previous_counter": 9, "reset_to": 0}


def test_workflow_ids_list_route_returns_sorted_counters() -> None:
    old_client = _Request.client
    client = _Client()
    _Request.client = client
    old_list = routes_slug.list_slug_counters

    async def fake_list(_client, _config):
        return [
            SlugCounterSummary(slug="alpha", counter=3, last_claimed_epoch=1.0),
            SlugCounterSummary(slug="beta", counter=1, last_claimed_epoch=2.0),
        ]

    routes_slug.list_slug_counters = fake_list
    try:
        response = asyncio.run(routes_slug.get_list(_Request({})))
    finally:
        _Request.client = old_client
        routes_slug.list_slug_counters = old_list

    assert response.status_code == 200
    body = json.loads(response.body)
    assert [c["slug"] for c in body["slug_counters"]] == ["alpha", "beta"]


def test_workflow_ids_claim_route_rejects_empty_name() -> None:
    response = asyncio.run(routes_slug.post_claim(_Request({"name": ""})))
    assert response.status_code == 400
