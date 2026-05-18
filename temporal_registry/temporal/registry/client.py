"""Client helpers for signaling and querying the registry workflow."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any, Literal, cast

from google.protobuf.duration_pb2 import Duration
from temporalio.api.enums.v1 import common_pb2
from temporalio.api.operatorservice.v1.request_response_pb2 import (
    AddSearchAttributesRequest,
    ListSearchAttributesRequest,
    RemoveSearchAttributesRequest,
)
from temporalio.api.workflowservice.v1.request_response_pb2 import (
    DescribeNamespaceRequest,
    RegisterNamespaceRequest,
)
from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from ...config_schemas import RegistryServiceConfig
from .registry_schemas import (
    RegistryStatus,
    RegistryServiceHeartbeatSignal,
    RegistryWorkflowInfo,
    RegistryWorkflowSpec,
    ResolvedWorkflowTarget,
    SearchAttributeReconcileReport,
    SearchAttributeSpec,
    SearchAttributeSummary,
    TemporalSearchAttribute,
    WorkerIdSignal,
    WorkflowSpecSignal,
    WorkflowTypeSignal,
)

log = logging.getLogger("temporal_registry.temporal.registry")

_SEARCH_ATTRIBUTE_TYPES: dict[str, common_pb2.IndexedValueType.ValueType] = {
    "Keyword": common_pb2.INDEXED_VALUE_TYPE_KEYWORD,
    "KeywordList": common_pb2.INDEXED_VALUE_TYPE_KEYWORD_LIST,
}
_TEMPORAL_SEARCH_ATTRIBUTE_TYPE_NAMES = {
    common_pb2.INDEXED_VALUE_TYPE_UNSPECIFIED: "Unspecified",
    common_pb2.INDEXED_VALUE_TYPE_TEXT: "Text",
    common_pb2.INDEXED_VALUE_TYPE_KEYWORD: "Keyword",
    common_pb2.INDEXED_VALUE_TYPE_INT: "Int",
    common_pb2.INDEXED_VALUE_TYPE_DOUBLE: "Double",
    common_pb2.INDEXED_VALUE_TYPE_BOOL: "Bool",
    common_pb2.INDEXED_VALUE_TYPE_DATETIME: "Datetime",
    common_pb2.INDEXED_VALUE_TYPE_KEYWORD_LIST: "KeywordList",
}
TemporalSearchAttributeType = Literal[
    "Keyword", "KeywordList", "Text", "Int", "Double", "Bool", "Datetime", "Unspecified"
]
TemporalSearchAttributeSource = Literal["custom", "system"]
SearchAttributeReconcileMode = Literal["validate", "ensure", "replace"]


async def ensure_namespace(
    client: Client,
    namespace: str,
    *,
    retention_days: int,
) -> None:
    try:
        await client.workflow_service.describe_namespace(
            DescribeNamespaceRequest(namespace=namespace)
        )
        return
    except RPCError as e:
        if e.status != RPCStatusCode.NOT_FOUND:
            raise

    retention = Duration()
    retention.FromTimedelta(timedelta(days=retention_days))
    try:
        await client.workflow_service.register_namespace(
            RegisterNamespaceRequest(
                namespace=namespace,
                workflow_execution_retention_period=retention,
            )
        )
    except RPCError as e:
        if e.status == RPCStatusCode.ALREADY_EXISTS:
            return
        raise


def _search_attribute_type(
    attr: SearchAttributeSpec,
) -> common_pb2.IndexedValueType.ValueType:
    return _SEARCH_ATTRIBUTE_TYPES[attr.type]


def _search_attributes_from_workflows(
    workflows: list[RegistryWorkflowSpec],
) -> list[SearchAttributeSpec]:
    attrs: dict[str, SearchAttributeSpec] = {}
    for workflow in workflows:
        for attr in workflow.search_attributes:
            existing = attrs.get(attr.name)
            if existing is not None and existing.type != attr.type:
                raise ValueError(
                    f"search attribute {attr.name} registered with conflicting "
                    f"types: {existing.type} and {attr.type}"
                )
            attrs[attr.name] = attr
    return list(attrs.values())


async def ensure_search_attributes(
    client: Client,
    namespace: str,
    attrs: list[SearchAttributeSpec],
    *,
    _retry_already_exists: bool = True,
) -> None:
    if not attrs:
        return

    listed = await client.operator_service.list_search_attributes(
        ListSearchAttributesRequest(namespace=namespace)
    )
    existing = dict(listed.system_attributes)
    existing.update(dict(listed.custom_attributes))

    missing: dict[str, common_pb2.IndexedValueType.ValueType] = {}
    for attr in attrs:
        expected_type = _search_attribute_type(attr)
        existing_type = existing.get(attr.name)
        if existing_type is None:
            missing[attr.name] = expected_type
            continue
        if existing_type != expected_type:
            raise ValueError(
                f"Temporal search attribute {attr.name} exists with type "
                f"{existing_type}, but registry expected {attr.type}"
            )
    if not missing:
        return

    try:
        await client.operator_service.add_search_attributes(
            AddSearchAttributesRequest(namespace=namespace, search_attributes=missing)
        )
    except RPCError as e:
        if e.status == RPCStatusCode.ALREADY_EXISTS and _retry_already_exists:
            await ensure_search_attributes(
                client, namespace, attrs, _retry_already_exists=False
            )
            return
        raise


async def list_temporal_search_attributes(
    client: Client,
    namespace: str,
) -> list[TemporalSearchAttribute]:
    listed = await client.operator_service.list_search_attributes(
        ListSearchAttributesRequest(namespace=namespace)
    )
    attrs = [
        TemporalSearchAttribute(
            name=name,
            type=cast(
                TemporalSearchAttributeType,
                _TEMPORAL_SEARCH_ATTRIBUTE_TYPE_NAMES.get(type_value, "Unspecified"),
            ),
            source=cast(TemporalSearchAttributeSource, source),
        )
        for source, values in (
            ("system", listed.system_attributes),
            ("custom", listed.custom_attributes),
        )
        for name, type_value in values.items()
    ]
    return sorted(attrs, key=lambda attr: (attr.source, attr.name))


async def reconcile_search_attributes(
    client: Client,
    config: RegistryServiceConfig,
    *,
    mode: SearchAttributeReconcileMode,
    attributes: list[str] | None = None,
    confirm: bool = False,
) -> SearchAttributeReconcileReport:
    desired = _desired_search_attributes(
        await list_search_attributes(client, config),
        attributes or [],
    )
    temporal_attrs = await list_temporal_search_attributes(
        client, config.temporal.namespace
    )
    existing = {attr.name: attr for attr in temporal_attrs}
    missing: list[SearchAttributeSpec] = []
    conflicts: list[dict[str, str]] = []
    unchanged: list[SearchAttributeSpec] = []

    for attr in desired:
        current = existing.get(attr.name)
        if current is None:
            missing.append(attr)
        elif current.type == attr.type:
            unchanged.append(attr)
        else:
            conflicts.append(
                {
                    "name": attr.name,
                    "existing_type": current.type,
                    "desired_type": attr.type,
                    "source": current.source,
                }
            )

    report = SearchAttributeReconcileReport(
        mode=mode,
        desired=desired,
        existing=temporal_attrs,
        missing=missing,
        conflicts=conflicts,
        unchanged=unchanged,
    )
    if mode == "validate":
        return report
    if mode == "ensure":
        if conflicts:
            return report
        await ensure_search_attributes(client, config.temporal.namespace, missing)
        report.added = missing
        return report
    if mode != "replace":
        raise ValueError(f"unsupported reconcile mode: {mode}")
    if not confirm:
        raise ValueError("replace mode requires confirm=true")
    if not attributes:
        raise ValueError("replace mode requires an explicit attributes list")

    replace_names = {item["name"] for item in conflicts}
    system_conflicts = [
        item["name"] for item in conflicts if item["source"] == "system"
    ]
    if system_conflicts:
        raise ValueError(
            "cannot replace system search attributes: " + ", ".join(system_conflicts)
        )
    if replace_names:
        await client.operator_service.remove_search_attributes(
            RemoveSearchAttributesRequest(
                namespace=config.temporal.namespace,
                search_attributes=sorted(replace_names),
            )
        )
    add_attrs = [attr for attr in desired if attr.name in replace_names] + missing
    await ensure_search_attributes(client, config.temporal.namespace, add_attrs)
    report.replaced = [attr for attr in desired if attr.name in replace_names]
    report.added = missing
    return report


def _desired_search_attributes(
    summaries: list[SearchAttributeSummary],
    attributes: list[str],
) -> list[SearchAttributeSpec]:
    desired_by_name = {
        item.name: SearchAttributeSpec(
            name=item.name,
            type=item.type,
            description=item.description,
        )
        for item in summaries
    }
    if not attributes:
        return [desired_by_name[name] for name in sorted(desired_by_name)]
    unknown = sorted(set(attributes) - set(desired_by_name))
    if unknown:
        raise ValueError("unknown registered search attributes: " + ", ".join(unknown))
    return [desired_by_name[name] for name in sorted(set(attributes))]


async def ensure_registry_workflow(
    client: Client, config: RegistryServiceConfig
) -> None:
    # ALLOW_DUPLICATE so that a `shutdown_registry` followed by registry service
    # restart can spin up a fresh registry instance under the same workflow id.
    try:
        await client.start_workflow(
            config.registry.workflow_type,
            id=config.registry.workflow_id,
            task_queue=config.registry.task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        )
    except WorkflowAlreadyStartedError:
        return


async def register_worker(
    client: Client, registration: Any, config: RegistryServiceConfig
) -> None:
    await ensure_registry_workflow(client, config)
    await ensure_search_attributes(
        client,
        config.temporal.namespace,
        _search_attributes_from_workflows(registration.workflows),
    )
    handle = client.get_workflow_handle(config.registry.workflow_id)
    await handle.signal("register_worker", registration.model_dump(mode="json"))


async def heartbeat_worker(
    client: Client, worker_id: str, config: RegistryServiceConfig
) -> None:
    handle = client.get_workflow_handle(config.registry.workflow_id)
    await handle.signal(
        "heartbeat_worker", WorkerIdSignal(worker_id=worker_id).model_dump(mode="json")
    )


async def unregister_workflow(
    client: Client, workflow_type: str, config: RegistryServiceConfig
) -> None:
    handle = client.get_workflow_handle(config.registry.workflow_id)
    await handle.signal(
        "unregister_workflow",
        WorkflowTypeSignal(workflow_type=workflow_type).model_dump(mode="json"),
    )


async def put_workflow(
    client: Client,
    workflow: RegistryWorkflowSpec,
    config: RegistryServiceConfig,
    *,
    overwrite: bool,
) -> None:
    await ensure_registry_workflow(client, config)
    await ensure_search_attributes(
        client,
        config.temporal.namespace,
        workflow.search_attributes,
    )
    handle = client.get_workflow_handle(config.registry.workflow_id)
    await handle.signal(
        "put_workflow",
        WorkflowSpecSignal(workflow=workflow, overwrite=overwrite).model_dump(
            mode="json"
        ),
    )


async def shutdown_registry(client: Client, config: RegistryServiceConfig) -> None:
    handle = client.get_workflow_handle(config.registry.workflow_id)
    await handle.signal("shutdown_registry")


async def mark_registry_service_started(
    client: Client,
    process_id: str,
    config: RegistryServiceConfig,
) -> None:
    await ensure_registry_workflow(client, config)
    handle = client.get_workflow_handle(config.registry.workflow_id)
    await handle.signal(
        "registry_service_started",
        RegistryServiceHeartbeatSignal(
            process_id=process_id,
            event="started",
        ).model_dump(mode="json"),
    )


async def heartbeat_loop(
    client: Client, worker_id: str, interval_seconds: int, config: RegistryServiceConfig
) -> None:
    while True:
        try:
            await heartbeat_worker(client, worker_id, config)
        except Exception as e:  # noqa: BLE001
            log.warning("registry heartbeat failed: %s", e)
        await asyncio.sleep(max(1, interval_seconds))


async def list_workflows(
    client: Client, config: RegistryServiceConfig
) -> list[RegistryWorkflowInfo]:
    handle = client.get_workflow_handle(config.registry.workflow_id)
    result = await handle.query("list_workflows")
    return [RegistryWorkflowInfo.model_validate(item) for item in list(result or [])]


async def get_status(client: Client, config: RegistryServiceConfig) -> RegistryStatus:
    handle = client.get_workflow_handle(config.registry.workflow_id)
    result = await handle.query("get_status")
    return RegistryStatus.model_validate(result or {})


async def get_workflow(
    client: Client, workflow_type: str, config: RegistryServiceConfig
) -> RegistryWorkflowInfo | None:
    handle = client.get_workflow_handle(config.registry.workflow_id)
    result = await handle.query("get_workflow", workflow_type)
    return RegistryWorkflowInfo.model_validate(result) if result else None


async def list_search_attributes(
    client: Client, config: RegistryServiceConfig
) -> list[SearchAttributeSummary]:
    handle = client.get_workflow_handle(config.registry.workflow_id)
    result = await handle.query("list_search_attributes")
    return [SearchAttributeSummary.model_validate(item) for item in list(result or [])]


async def resolve_workflow(
    client: Client, workflow_type: str, config: RegistryServiceConfig
) -> ResolvedWorkflowTarget | None:
    handle = client.get_workflow_handle(config.registry.workflow_id)
    result = await handle.query("resolve_workflow", workflow_type)
    return ResolvedWorkflowTarget.model_validate(result) if result else None
