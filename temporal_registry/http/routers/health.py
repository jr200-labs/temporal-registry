"""Health and readiness endpoints for the registry HTTP API."""

from __future__ import annotations

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse

from ...temporal.registry.client import get_status
from ..dependencies import registry_config, temporal_client
from ..schemas.responses import HealthResponse, error_responses


router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def get_health(_request: Request) -> Response:
    return JSONResponse({"status": "ok"})


@router.get("/ready", response_model=HealthResponse, responses=error_responses(503))
async def get_ready(request: Request) -> Response:
    try:
        await get_status(temporal_client(request), registry_config(request))
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"status": "not_ready", "error": str(e)}, status_code=503)
    return JSONResponse({"status": "ready"})
