"""HTTP routes for the slug counter feature.

Most callers will use the integrated path: pass `name` to
`POST /run` or `POST /workflows/<type>/start` and the registry
claims a counter atomically alongside the workflow start. These
endpoints exist for direct inspection and operator-driven reset.
"""

from __future__ import annotations

import json

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ...temporal.registry.client import (
    claim_slug_id,
    list_slug_counters,
    reset_slug,
)
from ...temporal.registry.registry_schemas import (
    ClaimSlugIdResponse,
    ResetSlugResponse,
    SlugCounterSummary,
)
from ..dependencies import registry_config, temporal_client
from ..schemas.responses import error_responses, request_body


router = APIRouter(tags=["slug-counters"])


class _NameBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=128)


class _SlugCountersListResponse(BaseModel):
    slug_counters: list[SlugCounterSummary]


@router.post(
    "/workflow-ids/claim",
    response_model=ClaimSlugIdResponse,
    responses=error_responses(400, 503),
    openapi_extra=request_body(_NameBody),
)
async def post_claim(request: Request) -> Response:
    try:
        body = await request.json()
    except json.JSONDecodeError as e:
        return Response(str(e), status_code=400)
    try:
        req = _NameBody.model_validate(body)
    except ValidationError as e:
        return JSONResponse(
            {"error": "invalid request", "details": e.errors(include_context=False)},
            status_code=400,
        )
    try:
        claimed = await claim_slug_id(
            temporal_client(request), req.name, registry_config(request)
        )
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=503)
    return JSONResponse(claimed.model_dump(mode="json"))


@router.post(
    "/workflow-ids/reset",
    response_model=ResetSlugResponse,
    responses=error_responses(400, 503),
    openapi_extra=request_body(_NameBody),
)
async def post_reset(request: Request) -> Response:
    try:
        body = await request.json()
    except json.JSONDecodeError as e:
        return Response(str(e), status_code=400)
    try:
        req = _NameBody.model_validate(body)
    except ValidationError as e:
        return JSONResponse(
            {"error": "invalid request", "details": e.errors(include_context=False)},
            status_code=400,
        )
    try:
        reset = await reset_slug(
            temporal_client(request), req.name, registry_config(request)
        )
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=503)
    return JSONResponse(reset.model_dump(mode="json"))


@router.get(
    "/workflow-ids",
    response_model=_SlugCountersListResponse,
    responses=error_responses(503),
)
async def get_list(request: Request) -> Response:
    try:
        counters = await list_slug_counters(
            temporal_client(request), registry_config(request)
        )
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=503)
    return JSONResponse(
        {"slug_counters": [c.model_dump(mode="json") for c in counters]}
    )
