"""Pydantic response schemas and OpenAPI helpers for the HTTP API."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel, Field

from ...temporal.registry.registry_schemas import (
    RegistryWorkflowInfo,
    RegistryWorkflowSpec,
    SearchAttributeReconcileReport,
    SearchAttributeSummary,
    TemporalSearchAttribute,
)
from .requests import (
    RunRequest,
    ScheduleStartRequest,
    SearchAttributeReconcileRequest,
    WorkflowStartRequest,
)


class ErrorResponse(BaseModel):
    error: str
    details: list[dict[str, Any]] = Field(default_factory=list)


class HealthResponse(BaseModel):
    status: str


class WorkflowListResponse(BaseModel):
    workflows: list[RegistryWorkflowInfo]


class SearchAttributeListResponse(BaseModel):
    search_attributes: list[SearchAttributeSummary]


class TemporalSearchAttributeListResponse(BaseModel):
    search_attributes: list[TemporalSearchAttribute]


class RunStartResponse(BaseModel):
    workflow_id: str
    run_id: str


class WorkflowStartResponse(RunStartResponse):
    task_queue: str
    workflow_type: str


class RegistryShutdownResponse(BaseModel):
    status: str


class ScheduleExistsResponse(BaseModel):
    status: str
    id: str


class ScheduleWarning(BaseModel):
    field: str
    message: str


class ScheduleWarningsResponse(BaseModel):
    warnings: list[ScheduleWarning]


class RunningScheduleAction(BaseModel):
    workflow_id: str
    first_execution_run_id: str


class RecentScheduleAction(BaseModel):
    scheduled_at: str
    started_at: str


class ScheduleDescriptionResponse(BaseModel):
    id: str
    paused: bool
    next_fires: list[str]
    running: list[RunningScheduleAction]
    recent: list[RecentScheduleAction]


def request_body(model: type[BaseModel]) -> dict[str, Any]:
    return {
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": {"$ref": f"#/components/schemas/{model.__name__}"},
                }
            },
        }
    }


def error_responses(*codes: int) -> dict[int | str, dict[str, Any]]:
    return {code: {"model": ErrorResponse} for code in codes}


def install_openapi_schema(app: FastAPI) -> None:
    def custom_openapi() -> dict[str, Any]:
        if app.openapi_schema:
            return app.openapi_schema
        spec = get_openapi(
            title=app.title,
            version=app.version,
            summary=app.summary,
            routes=app.routes,
        )
        schemas = spec.setdefault("components", {}).setdefault("schemas", {})
        for model in (
            RunRequest,
            RegistryWorkflowSpec,
            ScheduleStartRequest,
            SearchAttributeReconcileRequest,
            SearchAttributeReconcileReport,
            WorkflowStartRequest,
        ):
            schema = model.model_json_schema(
                ref_template="#/components/schemas/{model}"
            )
            schemas.update(schema.pop("$defs", {}))
            schemas[model.__name__] = schema
        app.openapi_schema = spec
        return app.openapi_schema

    app.openapi = custom_openapi  # type: ignore[method-assign]
