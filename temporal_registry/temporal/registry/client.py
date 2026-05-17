"""Client helpers for signaling and querying the registry workflow."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from ...config_schemas import RegistryServiceConfig
from .registry_schemas import (
    RegistryStatus,
    RegistryServiceHeartbeatSignal,
    RegistryWorkflowInfo,
    RegistryWorkflowSpec,
    ResolvedWorkflowTarget,
    SearchAttributeSummary,
    WorkerIdSignal,
    WorkflowSpecSignal,
    WorkflowTypeSignal,
)

log = logging.getLogger("temporal_registry.temporal.registry")


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
    *,
    interval_seconds: int = 0,
) -> None:
    await ensure_registry_workflow(client, config)
    handle = client.get_workflow_handle(config.registry.workflow_id)
    await handle.signal(
        "registry_service_started",
        RegistryServiceHeartbeatSignal(
            process_id=process_id,
            event="started",
            interval_seconds=max(0, interval_seconds),
        ).model_dump(mode="json"),
    )


async def heartbeat_registry_service(
    client: Client,
    process_id: str,
    config: RegistryServiceConfig,
    *,
    interval_seconds: int = 0,
    failed_attempts_since_last_success: int = 0,
) -> None:
    handle = client.get_workflow_handle(config.registry.workflow_id)
    await handle.signal(
        "registry_service_heartbeat",
        RegistryServiceHeartbeatSignal(
            process_id=process_id,
            event="heartbeat",
            interval_seconds=max(0, interval_seconds),
            failed_attempts_since_last_success=max(
                0, failed_attempts_since_last_success
            ),
        ).model_dump(mode="json"),
    )


async def registry_service_heartbeat_loop(
    client: Client,
    process_id: str,
    interval_seconds: int,
    config: RegistryServiceConfig,
) -> None:
    failed_attempts = 0
    while True:
        try:
            await heartbeat_registry_service(
                client,
                process_id,
                config,
                interval_seconds=interval_seconds,
                failed_attempts_since_last_success=failed_attempts,
            )
            failed_attempts = 0
        except Exception as e:  # noqa: BLE001
            failed_attempts += 1
            log.warning("registry service heartbeat failed: %s", e)
        await asyncio.sleep(max(1, interval_seconds))


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
