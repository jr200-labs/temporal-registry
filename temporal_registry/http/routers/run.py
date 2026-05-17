"""Convenience HTTP endpoint for starting registered run workflows."""

from __future__ import annotations

import json
import os

from fastapi import APIRouter
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from temporalio import common as tcommon

from ...temporal.registry.client import resolve_workflow
from ..dependencies import registry_config, temporal_client
from ..schemas.requests import RunRequest
from ..schemas.responses import RunStartResponse, error_responses, request_body
from .payloads import validate_schema
from .search_attributes import SA_KEY_AGENT_ACP_PROVIDER, SA_KEY_AGENT_ID


router = APIRouter(tags=["runs"])


@router.post(
    "/run",
    response_model=RunStartResponse,
    responses=error_responses(400, 404, 500),
    openapi_extra=request_body(RunRequest),
)
async def post_run(request: Request) -> Response:
    try:
        body = await request.json()
    except json.JSONDecodeError as e:
        return Response(str(e), status_code=400)
    try:
        req = RunRequest.model_validate(body)
    except ValidationError as e:
        return JSONResponse(
            {"error": "invalid request", "details": e.errors(include_context=False)}, status_code=400
        )

    client = temporal_client(request)
    config = registry_config(request)
    target = await resolve_workflow(client, req.workflow_type, config)
    if target is None:
        return Response("workflow unavailable", status_code=404)

    pairs = [tcommon.SearchAttributePair(SA_KEY_AGENT_ID, req.agent_id)]
    if req.agent_acp_provider:
        pairs.append(tcommon.SearchAttributePair(SA_KEY_AGENT_ACP_PROVIDER, req.agent_acp_provider))
    sa = tcommon.TypedSearchAttributes(pairs)

    inp = req.model_dump(mode="json", exclude={"workflow_type"})
    validation_errors = validate_schema(inp, target.input_schema)
    if validation_errors:
        return JSONResponse(
            {"error": "invalid workflow input", "details": validation_errors},
            status_code=400,
        )
    try:
        handle = await client.start_workflow(
            req.workflow_type,
            inp,
            id=f"run-agent-{req.agent_id}-{os.urandom(4).hex()}",
            task_queue=target.task_queue,
            search_attributes=sa,
        )
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=500)

    return JSONResponse({"workflow_id": handle.id, "run_id": handle.first_execution_run_id or ""})
