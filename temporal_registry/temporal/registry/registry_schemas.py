"""Pydantic schemas shared by registry workflow signals, queries, and API responses."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SearchAttributeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    type: Literal["Keyword", "KeywordList"]
    description: str = ""

    @field_validator("name")
    @classmethod
    def _strip_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value


class SearchAttributeSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: Literal["Keyword", "KeywordList"]
    description: str = ""
    workflows: list[str] = Field(default_factory=list)


class TemporalSearchAttribute(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: Literal[
        "Keyword",
        "KeywordList",
        "Text",
        "Int",
        "Double",
        "Bool",
        "Datetime",
        "Unspecified",
    ]
    source: Literal["custom", "system"]


class SearchAttributeReconcileReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["validate", "ensure", "replace"]
    desired: list[SearchAttributeSpec] = Field(default_factory=list)
    existing: list[TemporalSearchAttribute] = Field(default_factory=list)
    missing: list[SearchAttributeSpec] = Field(default_factory=list)
    conflicts: list[dict[str, str]] = Field(default_factory=list)
    added: list[SearchAttributeSpec] = Field(default_factory=list)
    replaced: list[SearchAttributeSpec] = Field(default_factory=list)
    unchanged: list[SearchAttributeSpec] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class InputWarning(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str = Field(min_length=1)
    message: str = Field(min_length=1)

    @field_validator("field", "message")
    @classmethod
    def _strip_required(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value


class ResolvedWorkflowTarget(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_type: str
    task_queue: str
    input_schema: dict[str, Any] = Field(default_factory=dict)
    schedule_input_warnings: list[InputWarning] = Field(default_factory=list)
    search_attributes: list[SearchAttributeSpec] = Field(default_factory=list)


class RegistryWorkflowSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_type: str = Field(min_length=1)
    version: str = ""
    task_queue: str = Field(min_length=1)
    description: str = ""
    input_schema: dict[str, Any] = Field(default_factory=dict)
    schedule_input_warnings: list[InputWarning] = Field(default_factory=list)
    search_attributes: list[SearchAttributeSpec] = Field(default_factory=list)
    enabled: bool = True
    labels: dict[str, str] = Field(default_factory=dict)

    @field_validator("workflow_type", "task_queue")
    @classmethod
    def _strip_required(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value


class RegistryWorker(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: str = Field(min_length=1)
    task_queue: str = Field(min_length=1)
    ttl_seconds: int = Field(default=60, ge=5, le=3600)
    last_seen_epoch: float = 0.0
    environment: str = "local"
    labels: dict[str, str] = Field(default_factory=dict)
    workflows: list[str] = Field(default_factory=list)

    @field_validator("worker_id", "task_queue")
    @classmethod
    def _strip_required(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value


class RegistryWorkerInfo(RegistryWorker):
    healthy: bool = False


class RegistryWorkflowInfo(RegistryWorkflowSpec):
    workers: list[RegistryWorkerInfo] = Field(default_factory=list)


class RegistryServiceHeartbeatSignal(BaseModel):
    model_config = ConfigDict(extra="forbid")

    process_id: str = Field(min_length=1)
    event: Literal["started", "heartbeat"] = "heartbeat"
    interval_seconds: int = Field(default=0, ge=0)
    failed_attempts_since_last_success: int = Field(default=0, ge=0)


class RegistryStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_count: int = 0
    worker_count: int = 0
    healthy_worker_count: int = 0
    started_at_epoch: float = 0.0
    last_registry_service_started_epoch: float = 0.0
    registry_service_process_id: str = ""


class WorkerRegistrationSignal(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: str = Field(min_length=1)
    task_queue: str = Field(min_length=1)
    workflows: list[RegistryWorkflowSpec] = Field(default_factory=list)
    ttl_seconds: int = Field(default=60, ge=5, le=3600)
    environment: str = "local"
    labels: dict[str, str] = Field(default_factory=dict)


class WorkflowSpecSignal(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow: RegistryWorkflowSpec
    overwrite: bool = False


class WorkerIdSignal(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: str = Field(min_length=1)


class WorkflowTypeSignal(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_type: str = Field(min_length=1)


class WorkflowEnabledSignal(WorkflowTypeSignal):
    enabled: bool = True


# ---------------------------------------------------------------------------
# Slug counter for human-readable workflow IDs.
#
# Workflows submitted with an optional `name` get a sequential per-slug
# counter ("tui-build" → r1, r2, …). The registry workflow holds the counter
# map in its own durable state, so the values survive worker restarts and
# concurrent claims are serialised through Temporal.
# ---------------------------------------------------------------------------


class SlugCounter(BaseModel):
    model_config = ConfigDict(extra="forbid")

    counter: int = Field(ge=0)
    last_claimed_epoch: float = Field(ge=0)


class ClaimSlugIdRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # The raw human name. Registry slugifies before lookup; "Tui_Build" and
    # "tui-build" hit the same counter.
    name: str = Field(min_length=1, max_length=128)


class ClaimSlugIdResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    slug: str
    counter: int
    workflow_id: str


class ResetSlugRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=128)


class ResetSlugResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    slug: str
    previous_counter: int
    reset_to: int = 0


class SlugCounterSummary(BaseModel):
    """Returned by the list endpoint for debugging."""

    model_config = ConfigDict(extra="forbid")

    slug: str
    counter: int
    last_claimed_epoch: float
