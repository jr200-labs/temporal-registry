"""Registry administration and workflow dispatch HTTP endpoints."""

from __future__ import annotations

import json
import os

from fastapi import APIRouter
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from ...temporal.registry.client import (
    claim_slug_id,
    get_status,
    get_workflow,
    list_search_attributes,
    list_temporal_search_attributes,
    list_workflows,
    put_workflow,
    reconcile_search_attributes,
    resolve_workflow,
    shutdown_registry,
    unregister_workflow,
)
from ...temporal.registry.registry_schemas import (
    RegistryStatus,
    RegistryWorkflowInfo,
    RegistryWorkflowSpec,
)
from ..dependencies import registry_config, temporal_client
from ..schemas.requests import SearchAttributeReconcileRequest, WorkflowStartRequest
from ..schemas.responses import (
    RegistryShutdownResponse,
    SearchAttributeReconcileReport,
    SearchAttributeListResponse,
    TemporalSearchAttributeListResponse,
    WorkflowListResponse,
    WorkflowStartResponse,
    error_responses,
    request_body,
)
from .payloads import validate_schema


router = APIRouter(tags=["registry"])
workflow_router = APIRouter(tags=["workflows"])


@router.get(
    "/registry/workflows",
    response_model=WorkflowListResponse,
    responses=error_responses(503),
)
async def get_registry_workflows(request: Request) -> Response:
    try:
        workflows = await list_workflows(
            temporal_client(request), registry_config(request)
        )
        return JSONResponse(
            {"workflows": [item.model_dump(mode="json") for item in workflows]}
        )
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=503)


@router.get(
    "/registry/search-attributes",
    response_model=SearchAttributeListResponse,
    responses=error_responses(503),
)
async def get_registry_search_attributes(request: Request) -> Response:
    try:
        attrs = await list_search_attributes(
            temporal_client(request), registry_config(request)
        )
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=503)
    return JSONResponse(
        {"search_attributes": [attr.model_dump(mode="json") for attr in attrs]}
    )


@router.get(
    "/registry/temporal/search-attributes",
    response_model=TemporalSearchAttributeListResponse,
    responses=error_responses(503),
)
async def get_temporal_search_attributes(request: Request) -> Response:
    try:
        attrs = await list_temporal_search_attributes(
            temporal_client(request), registry_config(request).temporal.namespace
        )
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=503)
    return JSONResponse(
        {"search_attributes": [attr.model_dump(mode="json") for attr in attrs]}
    )


@router.post(
    "/registry/temporal/search-attributes",
    response_model=SearchAttributeReconcileReport,
    responses=error_responses(400, 503),
    openapi_extra=request_body(SearchAttributeReconcileRequest),
)
async def post_temporal_search_attributes_reconcile(request: Request) -> Response:
    try:
        body = await request.json()
    except json.JSONDecodeError as e:
        return Response(str(e), status_code=400)
    try:
        req = SearchAttributeReconcileRequest.model_validate(body)
    except ValidationError as e:
        return JSONResponse(
            {"error": "invalid request", "details": e.errors(include_context=False)},
            status_code=400,
        )
    try:
        report = await reconcile_search_attributes(
            temporal_client(request),
            registry_config(request),
            mode=req.mode,
            attributes=req.attributes,
            confirm=req.confirm,
        )
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=503)
    return JSONResponse(report.model_dump(mode="json"))


@router.get(
    "/registry/status", response_model=RegistryStatus, responses=error_responses(503)
)
async def get_registry_status(request: Request) -> Response:
    try:
        status = await get_status(temporal_client(request), registry_config(request))
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=503)
    return JSONResponse(status.model_dump(mode="json"))


@router.get(
    "/registry/workflows/{workflow_type}",
    response_model=RegistryWorkflowInfo,
    responses=error_responses(404, 503),
)
async def get_registry_workflow(request: Request, workflow_type: str) -> Response:
    try:
        info = await get_workflow(
            temporal_client(request), workflow_type, registry_config(request)
        )
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=503)
    if not info:
        return Response("workflow not registered", status_code=404)
    return JSONResponse(info.model_dump(mode="json"))


@router.post(
    "/registry/workflows",
    response_model=RegistryWorkflowSpec,
    status_code=201,
    responses=error_responses(400, 409, 503),
    openapi_extra=request_body(RegistryWorkflowSpec),
)
async def post_registry_workflow(request: Request) -> Response:
    try:
        body = await request.json()
    except json.JSONDecodeError as e:
        return Response(str(e), status_code=400)
    try:
        spec = RegistryWorkflowSpec.model_validate(body)
    except ValidationError as e:
        return JSONResponse(
            {"error": "invalid request", "details": e.errors(include_context=False)},
            status_code=400,
        )

    client = temporal_client(request)
    config = registry_config(request)
    try:
        existing = await get_workflow(client, spec.workflow_type, config)
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=503)
    if existing is not None:
        return JSONResponse({"error": "workflow already registered"}, status_code=409)
    try:
        await put_workflow(client, spec, config, overwrite=False)
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=503)
    return JSONResponse(spec.model_dump(mode="json"), status_code=201)


@router.put(
    "/registry/workflows/{workflow_type}",
    response_model=RegistryWorkflowSpec,
    responses=error_responses(400, 503),
    openapi_extra=request_body(RegistryWorkflowSpec),
)
async def put_registry_workflow(request: Request, workflow_type: str) -> Response:
    if not workflow_type:
        return Response("workflow_type is required", status_code=400)
    try:
        body = await request.json()
    except json.JSONDecodeError as e:
        return Response(str(e), status_code=400)
    try:
        spec = RegistryWorkflowSpec.model_validate(body)
    except ValidationError as e:
        return JSONResponse(
            {"error": "invalid request", "details": e.errors(include_context=False)},
            status_code=400,
        )
    if spec.workflow_type != workflow_type:
        return JSONResponse(
            {"error": "workflow_type in path and body must match"}, status_code=400
        )
    try:
        await put_workflow(
            temporal_client(request), spec, registry_config(request), overwrite=True
        )
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=503)
    return JSONResponse(spec.model_dump(mode="json"), status_code=200)


@router.delete(
    "/registry/workflows/{workflow_type}",
    status_code=204,
    responses=error_responses(400, 503),
)
async def delete_registry_workflow(request: Request, workflow_type: str) -> Response:
    if not workflow_type:
        return Response("workflow_type is required", status_code=400)
    try:
        await unregister_workflow(
            temporal_client(request), workflow_type, registry_config(request)
        )
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=503)
    return Response(status_code=204)


@router.post(
    "/registry/shutdown",
    response_model=RegistryShutdownResponse,
    status_code=202,
    responses=error_responses(503),
)
async def post_registry_shutdown(request: Request) -> Response:
    try:
        await shutdown_registry(temporal_client(request), registry_config(request))
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=503)
    return JSONResponse({"status": "shutdown signaled"}, status_code=202)


@workflow_router.post(
    "/workflows/{workflow_type}/start",
    response_model=WorkflowStartResponse,
    responses=error_responses(400, 404, 500, 503),
    openapi_extra=request_body(WorkflowStartRequest),
)
async def post_workflow_start(request: Request, workflow_type: str) -> Response:
    try:
        body = await request.json()
    except json.JSONDecodeError as e:
        return Response(str(e), status_code=400)
    try:
        req = WorkflowStartRequest.model_validate(body)
    except ValidationError as e:
        return JSONResponse(
            {"error": "invalid request", "details": e.errors(include_context=False)},
            status_code=400,
        )

    try:
        client = temporal_client(request)
        target = await resolve_workflow(client, workflow_type, registry_config(request))
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=503)
    if target is None:
        return Response("workflow unavailable", status_code=404)

    validation_errors = validate_schema(req.input, target.input_schema)
    if validation_errors:
        return JSONResponse(
            {"error": "invalid workflow input", "details": validation_errors},
            status_code=400,
        )

    if req.name:
        try:
            claimed = await claim_slug_id(client, req.name, registry_config(request))
        except Exception as e:  # noqa: BLE001
            return Response(f"slug claim failed: {e}", status_code=503)
        workflow_id = claimed.workflow_id
    elif req.workflow_id:
        workflow_id = req.workflow_id
    else:
        workflow_id = f"{workflow_type.replace('.', '-')}-{os.urandom(4).hex()}"
    try:
        handle = await client.start_workflow(
            workflow_type,
            req.input,
            id=workflow_id,
            task_queue=target.task_queue,
        )
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=500)
    return JSONResponse(
        {
            "workflow_id": handle.id,
            "run_id": handle.first_execution_run_id or "",
            "task_queue": target.task_queue,
            "workflow_type": workflow_type,
        }
    )
