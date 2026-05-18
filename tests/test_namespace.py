from __future__ import annotations

import asyncio

from temporalio.api.workflowservice.v1.request_response_pb2 import (
    DescribeNamespaceResponse,
)
from temporalio.service import RPCError, RPCStatusCode

from temporal_registry.temporal.registry.client import ensure_namespace


class _WorkflowService:
    def __init__(self, describe_status: RPCStatusCode | None = None) -> None:
        self.describe_status = describe_status
        self.described: list[str] = []
        self.registered: list[tuple[str, int]] = []

    async def describe_namespace(self, req):
        self.described.append(req.namespace)
        if self.describe_status is not None:
            raise RPCError("describe failed", self.describe_status, b"")
        return DescribeNamespaceResponse()

    async def register_namespace(self, req):
        self.registered.append(
            (req.namespace, req.workflow_execution_retention_period.seconds)
        )


class _AlreadyExistsWorkflowService(_WorkflowService):
    async def register_namespace(self, req):
        await super().register_namespace(req)
        raise RPCError("already exists", RPCStatusCode.ALREADY_EXISTS, b"")


class _Client:
    def __init__(self, workflow_service: _WorkflowService) -> None:
        self.workflow_service = workflow_service


def test_ensure_namespace_skips_existing_namespace() -> None:
    service = _WorkflowService()

    asyncio.run(
        ensure_namespace(
            _Client(service),  # type: ignore[arg-type]
            "default",
            retention_days=30,
        )
    )

    assert service.described == ["default"]
    assert service.registered == []


def test_ensure_namespace_registers_missing_namespace() -> None:
    service = _WorkflowService(describe_status=RPCStatusCode.NOT_FOUND)

    asyncio.run(
        ensure_namespace(
            _Client(service),  # type: ignore[arg-type]
            "agents",
            retention_days=30,
        )
    )

    assert service.described == ["agents"]
    assert service.registered == [("agents", 30 * 24 * 60 * 60)]


def test_ensure_namespace_tolerates_create_race() -> None:
    service = _AlreadyExistsWorkflowService(describe_status=RPCStatusCode.NOT_FOUND)

    asyncio.run(
        ensure_namespace(
            _Client(service),  # type: ignore[arg-type]
            "agents",
            retention_days=30,
        )
    )

    assert service.registered == [("agents", 30 * 24 * 60 * 60)]
