from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from temporalio.api.enums.v1.common_pb2 import (
    INDEXED_VALUE_TYPE_KEYWORD,
    INDEXED_VALUE_TYPE_KEYWORD_LIST,
)

from temporal_registry.config_schemas import RegistryServiceConfig
from temporal_registry.temporal.registry.client import (
    ensure_search_attributes,
    reconcile_search_attributes,
)
from temporal_registry.temporal.registry.registry_schemas import (
    SearchAttributeSpec,
    SearchAttributeSummary,
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
                "metrics": {"enabled": False, "path": "/metrics"},
                "otel": {
                    "enabled": False,
                    "service_name": "test",
                    "endpoint": "",
                    "insecure": True,
                },
            },
        }
    )


class _OperatorService:
    def __init__(self, custom_attributes: dict[str, int] | None = None) -> None:
        self.custom_attributes = custom_attributes or {}
        self.added: list[dict[str, int]] = []
        self.removed: list[list[str]] = []

    async def list_search_attributes(self, req):
        self.namespace = req.namespace
        return SimpleNamespace(
            custom_attributes=self.custom_attributes,
            system_attributes={},
        )

    async def add_search_attributes(self, req):
        self.added.append(dict(req.search_attributes))
        self.custom_attributes.update(dict(req.search_attributes))

    async def remove_search_attributes(self, req):
        names = list(req.search_attributes)
        self.removed.append(names)
        for name in names:
            self.custom_attributes.pop(name, None)


class _Handle:
    async def query(self, name: str):
        assert name == "list_search_attributes"
        return [
            SearchAttributeSummary(
                name="agent_id",
                type="Keyword",
                workflows=["agent.run.v1"],
            ).model_dump(mode="json"),
            SearchAttributeSummary(
                name="tools_used",
                type="KeywordList",
                workflows=["agent.run.v1"],
            ).model_dump(mode="json"),
        ]


class _Client:
    def __init__(self, operator_service: _OperatorService) -> None:
        self.operator_service = operator_service

    def get_workflow_handle(self, _workflow_id: str):
        return _Handle()


def test_ensure_search_attributes_adds_missing_attributes() -> None:
    operator = _OperatorService({"agent_id": INDEXED_VALUE_TYPE_KEYWORD})
    client = _Client(operator)

    asyncio.run(
        ensure_search_attributes(
            client,  # type: ignore[arg-type]
            "default",
            [
                SearchAttributeSpec(name="agent_id", type="Keyword"),
                SearchAttributeSpec(name="tools_used", type="KeywordList"),
            ],
        )
    )

    assert operator.namespace == "default"
    assert operator.added == [{"tools_used": INDEXED_VALUE_TYPE_KEYWORD_LIST}]


def test_ensure_search_attributes_skips_existing_attributes() -> None:
    operator = _OperatorService({"agent_id": INDEXED_VALUE_TYPE_KEYWORD})
    client = _Client(operator)

    asyncio.run(
        ensure_search_attributes(
            client,  # type: ignore[arg-type]
            "default",
            [SearchAttributeSpec(name="agent_id", type="Keyword")],
        )
    )

    assert operator.added == []


def test_reconcile_search_attributes_validate_reports_missing_and_conflicts() -> None:
    operator = _OperatorService({"agent_id": INDEXED_VALUE_TYPE_KEYWORD_LIST})
    client = _Client(operator)

    report = asyncio.run(
        reconcile_search_attributes(
            client,  # type: ignore[arg-type]
            _config(),
            mode="validate",
        )
    )

    assert [attr.name for attr in report.missing] == ["tools_used"]
    assert report.conflicts == [
        {
            "name": "agent_id",
            "existing_type": "KeywordList",
            "desired_type": "Keyword",
            "source": "custom",
        }
    ]
    assert operator.added == []
    assert operator.removed == []


def test_reconcile_search_attributes_ensure_adds_missing_without_conflicts() -> None:
    operator = _OperatorService({"agent_id": INDEXED_VALUE_TYPE_KEYWORD})
    client = _Client(operator)

    report = asyncio.run(
        reconcile_search_attributes(
            client,  # type: ignore[arg-type]
            _config(),
            mode="ensure",
        )
    )

    assert [attr.name for attr in report.added] == ["tools_used"]
    assert operator.added == [{"tools_used": INDEXED_VALUE_TYPE_KEYWORD_LIST}]
    assert operator.removed == []


def test_reconcile_search_attributes_replace_requires_confirmation() -> None:
    operator = _OperatorService({"agent_id": INDEXED_VALUE_TYPE_KEYWORD_LIST})
    client = _Client(operator)

    with pytest.raises(ValueError, match="confirm=true"):
        asyncio.run(
            reconcile_search_attributes(
                client,  # type: ignore[arg-type]
                _config(),
                mode="replace",
                attributes=["agent_id"],
            )
        )


def test_reconcile_search_attributes_replace_removes_and_recreates_conflicts() -> None:
    operator = _OperatorService({"agent_id": INDEXED_VALUE_TYPE_KEYWORD_LIST})
    client = _Client(operator)

    report = asyncio.run(
        reconcile_search_attributes(
            client,  # type: ignore[arg-type]
            _config(),
            mode="replace",
            attributes=["agent_id"],
            confirm=True,
        )
    )

    assert [attr.name for attr in report.replaced] == ["agent_id"]
    assert operator.removed == [["agent_id"]]
    assert operator.added == [{"agent_id": INDEXED_VALUE_TYPE_KEYWORD}]


def test_ensure_search_attributes_rejects_type_conflicts() -> None:
    operator = _OperatorService({"agent_id": INDEXED_VALUE_TYPE_KEYWORD_LIST})
    client = _Client(operator)

    with pytest.raises(ValueError, match="agent_id"):
        asyncio.run(
            ensure_search_attributes(
                client,  # type: ignore[arg-type]
                "default",
                [SearchAttributeSpec(name="agent_id", type="Keyword")],
            )
        )

    assert operator.added == []
