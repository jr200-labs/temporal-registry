"""Durable Temporal workflow that stores registry metadata and worker health."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from pydantic import TypeAdapter, ValidationError
from temporalio import workflow
from temporalio.common import RawValue, RetryPolicy

from .activity_types import ACTIVITY_ENSURE_SEARCH_ATTRIBUTES

from .registry_schemas import (
    RegistryWorker,
    RegistryWorkerInfo,
    RegistryWorkflowInfo,
    RegistryWorkflowSpec,
    RegistryServiceHeartbeatSignal,
    RegistryStatus,
    ResolvedWorkflowTarget,
    SearchAttributeSpec,
    SearchAttributeSummary,
    WorkerIdSignal,
    WorkerRegistrationSignal,
    WorkflowEnabledSignal,
    WorkflowSpecSignal,
    WorkflowTypeSignal,
)


GC_INTERVAL_SECONDS = 60
GC_STALE_FACTOR = 6
ENSURE_SEARCH_ATTRIBUTES_TIMEOUT_SECONDS = 30
SEARCH_ATTRIBUTE_PROVISIONING_PATCH = "search-attribute-provisioning-v1"
log = logging.getLogger("temporal_registry.temporal.registry.workflow")


@workflow.defn(name="temporal-registry.v1", dynamic=True)  # type: ignore[call-overload]
class WorkerRegistry:
    def __init__(self) -> None:
        self._workflows: dict[str, RegistryWorkflowSpec] = {}
        self._workers: dict[str, RegistryWorker] = {}
        self._shutdown = False
        self._started_at_epoch = 0.0
        self._last_registry_service_started_epoch = 0.0
        self._registry_service_process_id = ""

    @workflow.signal(name="register_worker")
    async def register_worker(
        self, payload: dict[str, Any] | WorkerRegistrationSignal
    ) -> None:
        try:
            registration = TypeAdapter(WorkerRegistrationSignal).validate_python(
                payload
            )
        except ValidationError as e:
            workflow.logger.warning("register_worker rejected invalid payload: %s", e)
            return

        if workflow.patched(SEARCH_ATTRIBUTE_PROVISIONING_PATCH):
            attrs = self._search_attributes_from_workflows(registration.workflows)
            if attrs is None or not await self._ensure_search_attributes(attrs):
                return

        worker = RegistryWorker(
            worker_id=registration.worker_id,
            task_queue=registration.task_queue,
            ttl_seconds=registration.ttl_seconds,
            last_seen_epoch=self._utc_now().timestamp(),
            environment=registration.environment,
            labels=registration.labels,
            workflows=[],
        )

        for spec in registration.workflows:
            self._register_workflow_spec(
                spec, overwrite=False, source=f"worker {registration.worker_id}"
            )
            worker.workflows.append(spec.workflow_type)

        self._workers[worker.worker_id] = worker

    @workflow.signal(name="put_workflow")
    async def put_workflow(self, payload: dict[str, Any] | WorkflowSpecSignal) -> None:
        try:
            signal = TypeAdapter(WorkflowSpecSignal).validate_python(payload)
        except ValidationError as e:
            workflow.logger.warning("put_workflow rejected invalid payload: %s", e)
            return
        if workflow.patched(SEARCH_ATTRIBUTE_PROVISIONING_PATCH):
            if not await self._ensure_search_attributes(
                signal.workflow.search_attributes
            ):
                return
        self._register_workflow_spec(
            signal.workflow, overwrite=signal.overwrite, source="api"
        )

    @workflow.signal(name="heartbeat_worker")
    async def heartbeat_worker(self, payload: dict[str, Any] | WorkerIdSignal) -> None:
        try:
            signal = TypeAdapter(WorkerIdSignal).validate_python(payload)
        except ValidationError as e:
            workflow.logger.warning("heartbeat_worker rejected invalid payload: %s", e)
            return
        worker = self._workers.get(signal.worker_id)
        if worker is not None:
            worker.last_seen_epoch = self._utc_now().timestamp()
        else:
            workflow.logger.info(
                "heartbeat for unknown worker %s; the worker should re-register",
                signal.worker_id,
            )

    @workflow.signal(name="unregister_worker")
    async def unregister_worker(self, payload: dict[str, Any] | WorkerIdSignal) -> None:
        try:
            signal = TypeAdapter(WorkerIdSignal).validate_python(payload)
        except ValidationError as e:
            workflow.logger.warning("unregister_worker rejected invalid payload: %s", e)
            return
        self._workers.pop(signal.worker_id, None)

    @workflow.signal(name="set_workflow_enabled")
    async def set_workflow_enabled(
        self, payload: dict[str, Any] | WorkflowEnabledSignal
    ) -> None:
        try:
            signal = TypeAdapter(WorkflowEnabledSignal).validate_python(payload)
        except ValidationError as e:
            workflow.logger.warning(
                "set_workflow_enabled rejected invalid payload: %s", e
            )
            return
        spec = self._workflows.get(signal.workflow_type)
        if spec is not None:
            spec.enabled = signal.enabled

    @workflow.signal(name="unregister_workflow")
    async def unregister_workflow(
        self, payload: dict[str, Any] | WorkflowTypeSignal
    ) -> None:
        try:
            signal = TypeAdapter(WorkflowTypeSignal).validate_python(payload)
        except ValidationError as e:
            workflow.logger.warning(
                "unregister_workflow rejected invalid payload: %s", e
            )
            return
        self._workflows.pop(signal.workflow_type, None)
        for worker in self._workers.values():
            worker.workflows = [
                wf for wf in worker.workflows if wf != signal.workflow_type
            ]

    @workflow.signal(name="shutdown_registry")
    async def shutdown_registry(self) -> None:
        self._shutdown = True

    @workflow.signal(name="registry_service_started")
    async def registry_service_started(
        self, payload: dict[str, Any] | RegistryServiceHeartbeatSignal
    ) -> None:
        signal = self._registry_service_signal(payload, event="started")
        self._registry_service_process_id = signal.process_id
        self._last_registry_service_started_epoch = self._utc_now().timestamp()

    @workflow.signal(name="registry_service_heartbeat")
    async def registry_service_heartbeat(
        self, payload: dict[str, Any] | RegistryServiceHeartbeatSignal
    ) -> None:
        # Legacy handler for workflows that already have heartbeat signals in
        # history. Runtime no longer emits these high-churn liveness signals.
        self._registry_service_signal(payload, event="heartbeat")

    @workflow.query(name="list_workflows")
    def list_workflows(self) -> list[dict[str, Any]]:
        return [
            self._workflow_info(wf).model_dump(mode="json")
            for wf in sorted(self._workflows)
        ]

    @workflow.query(name="get_workflow")
    def get_workflow(self, workflow_type: str) -> dict[str, Any] | None:
        if workflow_type not in self._workflows:
            return None
        return self._workflow_info(workflow_type).model_dump(mode="json")

    @workflow.query(name="resolve_workflow")
    def resolve_workflow(self, workflow_type: str) -> dict[str, Any] | None:
        spec = self._workflows.get(workflow_type)
        if spec is None or not spec.enabled:
            return None
        healthy = self._healthy_workers_for(workflow_type)
        if not healthy:
            return None
        # Queries MUST be side-effect free (they aren't recorded in history),
        # so we don't keep a round-robin cursor here. Workers all advertise the
        # same task_queue today; if they ever diverge, route via task-queue-side
        # load balancing or a signal-driven cursor instead.
        worker = healthy[0]
        return ResolvedWorkflowTarget(
            workflow_type=workflow_type,
            task_queue=worker.task_queue,
            input_schema=spec.input_schema,
            schedule_input_warnings=spec.schedule_input_warnings,
            search_attributes=spec.search_attributes,
        ).model_dump(mode="json")

    @workflow.query(name="list_search_attributes")
    def list_search_attributes(self) -> list[dict[str, Any]]:
        aggregate: dict[str, SearchAttributeSummary] = {}
        for workflow_type, spec in self._workflows.items():
            for attr in spec.search_attributes:
                existing = aggregate.setdefault(
                    attr.name,
                    SearchAttributeSummary(
                        name=attr.name,
                        type=attr.type,
                        description=attr.description,
                        workflows=[],
                    ),
                )
                if workflow_type not in existing.workflows:
                    existing.workflows.append(workflow_type)
                    existing.workflows.sort()
        return [aggregate[name].model_dump(mode="json") for name in sorted(aggregate)]

    @workflow.query(name="list_workers")
    def list_workers(self) -> list[dict[str, Any]]:
        return [
            self._worker_info(worker).model_dump(mode="json")
            for worker in self._workers.values()
        ]

    @workflow.query(name="get_status")
    def get_status(self) -> dict[str, Any]:
        return RegistryStatus(
            workflow_count=len(self._workflows),
            worker_count=len(self._workers),
            healthy_worker_count=sum(
                1
                for worker in self._workers.values()
                if self._is_worker_healthy(worker)
            ),
            started_at_epoch=self._started_at_epoch,
            last_registry_service_started_epoch=self._last_registry_service_started_epoch,
            registry_service_process_id=self._registry_service_process_id,
        ).model_dump(mode="json")

    @workflow.run
    async def run(self, _args: Sequence[RawValue]) -> None:
        self._started_at_epoch = self._utc_now().timestamp()
        # Periodic GC keeps history bounded when workers churn (containers die,
        # IPs roll, etc.) so the workflow doesn't accumulate dead entries.
        while not self._shutdown:
            try:
                await workflow.wait_condition(
                    lambda: self._shutdown,
                    timeout=timedelta(seconds=GC_INTERVAL_SECONDS),
                )
            except TimeoutError:
                pass
            self._gc_stale_workers()

    def _gc_stale_workers(self) -> None:
        now = self._utc_now().timestamp()
        stale: list[str] = []
        for worker_id, worker in self._workers.items():
            age = now - (worker.last_seen_epoch or 0)
            if age > worker.ttl_seconds * GC_STALE_FACTOR:
                stale.append(worker_id)
        for worker_id in stale:
            workflow.logger.info("gc removing stale worker %s", worker_id)
            self._workers.pop(worker_id, None)

    def _registry_service_signal(
        self,
        payload: dict[str, Any] | RegistryServiceHeartbeatSignal,
        *,
        event: Literal["started", "heartbeat"],
    ) -> RegistryServiceHeartbeatSignal:
        try:
            signal = TypeAdapter(RegistryServiceHeartbeatSignal).validate_python(
                payload
            )
        except ValidationError as e:
            workflow.logger.warning(
                "registry service %s rejected invalid payload: %s", event, e
            )
            return RegistryServiceHeartbeatSignal(process_id="unknown", event=event)
        return signal

    async def _ensure_search_attributes(self, attrs: list[SearchAttributeSpec]) -> bool:
        if not attrs:
            return True
        try:
            await workflow.execute_activity(
                ACTIVITY_ENSURE_SEARCH_ATTRIBUTES,
                [attr.model_dump(mode="json") for attr in attrs],
                start_to_close_timeout=timedelta(
                    seconds=ENSURE_SEARCH_ATTRIBUTES_TIMEOUT_SECONDS
                ),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
        except Exception as e:  # noqa: BLE001 - keep bad admin state from advertising unavailable workflows.
            workflow.logger.error("search attribute provisioning failed: %s", e)
            return False
        return True

    def _search_attributes_from_workflows(
        self, workflows: list[RegistryWorkflowSpec]
    ) -> list[SearchAttributeSpec] | None:
        attrs: dict[str, SearchAttributeSpec] = {}
        for spec in workflows:
            for attr in spec.search_attributes:
                existing = attrs.get(attr.name)
                if existing is not None and existing.type != attr.type:
                    workflow.logger.error(
                        "search attribute %s registered with conflicting types: %s and %s",
                        attr.name,
                        existing.type,
                        attr.type,
                    )
                    return None
                attrs[attr.name] = attr
        return list(attrs.values())

    def _workflow_info(self, workflow_type: str) -> RegistryWorkflowInfo:
        spec = self._workflows[workflow_type]
        return RegistryWorkflowInfo(
            **spec.model_dump(mode="python"),
            workers=[
                self._worker_info(worker)
                for worker in self._workers.values()
                if workflow_type in worker.workflows
            ],
        )

    def _register_workflow_spec(
        self, spec: RegistryWorkflowSpec, *, overwrite: bool, source: str
    ) -> None:
        existing = self._workflows.get(spec.workflow_type)
        if existing is None or overwrite:
            self._workflows[spec.workflow_type] = spec
            return
        if existing.model_dump(mode="python") == spec.model_dump(mode="python"):
            return
        message = "ignoring conflicting workflow spec registration: workflow_type=%s source=%s"
        try:
            workflow.logger.warning(message, spec.workflow_type, source)
        except Exception:  # noqa: BLE001 - fallback for direct unit tests outside Temporal runtime.
            log.warning(message, spec.workflow_type, source)

    def _worker_info(self, worker: RegistryWorker) -> RegistryWorkerInfo:
        return RegistryWorkerInfo(
            **worker.model_dump(mode="python"),
            healthy=self._is_worker_healthy(worker),
        )

    def _healthy_workers_for(self, workflow_type: str) -> list[RegistryWorker]:
        return [
            worker
            for worker in self._workers.values()
            if workflow_type in worker.workflows and self._is_worker_healthy(worker)
        ]

    def _is_worker_healthy(self, worker: RegistryWorker) -> bool:
        if not worker.last_seen_epoch:
            return False
        age = self._utc_now().timestamp() - worker.last_seen_epoch
        return age <= worker.ttl_seconds

    def _parse_aware_utc(self, value: str) -> datetime:
        """Accept ISO strings (test helper). Internal state uses epoch floats."""
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _normalize_input_warnings(self, values: Any) -> list[dict[str, Any]]:
        """Filter caller-supplied input-warning lists down to the well-formed
        entries (test helper for input sanitization)."""
        from .registry_schemas import InputWarning

        warnings: list[dict[str, Any]] = []
        for value in list(values or []):
            try:
                warning = InputWarning.model_validate(value)
            except ValidationError:
                continue
            warnings.append(warning.model_dump(mode="json"))
        return warnings

    def _utc_now(self) -> datetime:
        now = workflow.now()
        if now.tzinfo is None:
            return now.replace(tzinfo=timezone.utc)
        return now.astimezone(timezone.utc)
