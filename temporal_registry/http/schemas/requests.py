"""Pydantic request schemas for registry HTTP endpoints."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


AGENT_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


def _validate_agent_id(value: str, *, allow_empty: bool) -> str:
    if not value:
        if allow_empty:
            return value
        raise ValueError("agent_id is required")
    if not AGENT_ID_PATTERN.fullmatch(value):
        raise ValueError(
            f'invalid agent_id "{value}": must match {AGENT_ID_PATTERN.pattern}'
        )
    return value


class ChainStepRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_id: str = ""
    prompt: str = ""

    @field_validator("agent_id")
    @classmethod
    def _check_agent_id(cls, value: str) -> str:
        return _validate_agent_id(value, allow_empty=True)


_FORBIDDEN_WORKSPACE_ROOTS = frozenset(
    {"/", "/root", "/home", "/etc", "/var", "/usr", "/bin", "/sbin"}
)


def _validate_workspace(value: str) -> str:
    """Workspace must be a non-empty, absolute, normalised path that isn't a
    system root. The activity does not implicitly resolve relative paths, so
    accepting "" would silently land at the worker's cwd."""
    if not value:
        raise ValueError("workspace is required and must be an absolute path")
    if not os.path.isabs(value):
        raise ValueError(f"workspace must be absolute: {value!r}")
    normalized = os.path.normpath(value)
    if normalized in _FORBIDDEN_WORKSPACE_ROOTS:
        raise ValueError(f"workspace must not be a system root: {value!r}")
    return normalized


class RunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_type: str = "agent.run.v1"
    agent_id: str = Field(min_length=1)
    workspace: str = Field(min_length=1)
    prompt: str = ""
    agent_acp_provider: str = ""
    chain: list[ChainStepRequest] = Field(default_factory=list)
    chain_mode: Literal["", "override", "append"] = ""

    @field_validator("agent_id")
    @classmethod
    def _check_agent_id(cls, value: str) -> str:
        return _validate_agent_id(value, allow_empty=False)

    @field_validator("workspace")
    @classmethod
    def _check_workspace(cls, value: str) -> str:
        return _validate_workspace(value)

    @model_validator(mode="after")
    def _validate_chain_mode(self) -> RunRequest:
        if self.chain and not self.chain_mode:
            raise ValueError(
                'chain_mode is required when chain is set; use "override" or "append"'
            )
        return self


class ScheduleStartRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_type: str = Field(min_length=1)
    workflow_id: str = ""
    task_queue: str = ""
    input: dict[str, Any] = Field(default_factory=dict)
    interval_seconds: int = Field(default=60, ge=1)
    fire_offsets_seconds: list[int] = Field(default_factory=list)
    start_at: datetime | None = None
    end_at: datetime | None = None
    overlap_policy: Literal[
        "skip",
        "buffer_one",
        "buffer_all",
        "cancel_other",
        "terminate_other",
        "allow_all",
    ] = "skip"
    search_attributes: dict[str, str | list[str]] = Field(default_factory=dict)
    note: str = ""

    @field_validator("fire_offsets_seconds")
    @classmethod
    def _validate_fire_offsets(cls, value: list[int]) -> list[int]:
        if any(offset < 0 for offset in value):
            raise ValueError("fire_offsets_seconds must be non-negative")
        return value

    @model_validator(mode="before")
    @classmethod
    def _reject_explicit_empty_fire_offsets(cls, data: Any) -> Any:
        if isinstance(data, dict) and "fire_offsets_seconds" in data:
            value = data["fire_offsets_seconds"]
            if isinstance(value, list) and not value:
                raise ValueError(
                    "fire_offsets_seconds must be omitted or contain at least one offset"
                )
        return data

    @field_validator("start_at", "end_at")
    @classmethod
    def _validate_aware_datetime(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("schedule datetimes must include timezone information")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _validate_datetime_mode(self) -> ScheduleStartRequest:
        if self.fire_offsets_seconds and (
            self.start_at is not None or self.end_at is not None
        ):
            raise ValueError(
                "start_at/end_at cannot be combined with fire_offsets_seconds"
            )
        if (
            self.start_at is not None
            and self.end_at is not None
            and self.end_at <= self.start_at
        ):
            raise ValueError("end_at must be after start_at")
        return self


class WorkflowStartRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input: dict[str, Any] = Field(default_factory=dict)
    workflow_id: str = ""
