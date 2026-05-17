"""HTTP endpoints for creating, describing, and deleting Temporal schedules."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from temporalio import common as tcommon
from temporalio.client import (
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleCalendarSpec,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleRange,
    ScheduleSpec,
    ScheduleState,
)
from temporalio.service import RPCError, RPCStatusCode

from ...temporal.registry.client import resolve_workflow
from ...temporal.registry.registry_schemas import InputWarning, SearchAttributeSpec
from ..dependencies import registry_config, temporal_client
from ..schemas.requests import ScheduleStartRequest
from ..schemas.responses import (
    ScheduleDescriptionResponse,
    ScheduleExistsResponse,
    ScheduleWarningsResponse,
    error_responses,
    request_body,
)
from .search_attributes import SEARCH_ATTRIBUTE_KEYS


router = APIRouter(tags=["schedules"])


@router.post(
    "/schedules/{schedule_id}",
    status_code=201,
    responses={
        200: {"model": ScheduleExistsResponse},
        201: {"model": ScheduleWarningsResponse},
        **error_responses(400, 404, 500, 503),
    },
    openapi_extra=request_body(ScheduleStartRequest),
)
async def post_schedule(request: Request) -> Response:
    schedule_id = str(request.path_params.get("schedule_id") or "")
    if not schedule_id:
        return Response("schedule_id is required", status_code=400)
    try:
        body = await request.json()
    except json.JSONDecodeError as e:
        return Response(str(e), status_code=400)
    try:
        req = ScheduleStartRequest.model_validate(body)
    except ValidationError as e:
        return JSONResponse(
            {"error": "invalid request", "details": e.errors(include_context=False)},
            status_code=400,
        )

    try:
        client = temporal_client(request)
        config = registry_config(request)
        target = await resolve_workflow(client, req.workflow_type, config)
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=503)
    if target is None:
        return Response("workflow unavailable", status_code=404)

    task_queue = req.task_queue or target.task_queue

    try:
        typed_search_attributes = _typed_search_attributes(
            req.search_attributes,
            req.workflow_type,
            target.search_attributes,
        )
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    warnings = _schedule_input_warnings(req.input, target.schedule_input_warnings)

    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            req.workflow_type,
            req.input,
            id=req.workflow_id or schedule_id,
            task_queue=task_queue,
            typed_search_attributes=typed_search_attributes,
        ),
        spec=_schedule_spec(req),
        policy=SchedulePolicy(overlap=_overlap_policy(req.overlap_policy)),
        state=ScheduleState(note=req.note),
    )
    try:
        await client.create_schedule(schedule_id, schedule)
        if 0 in req.fire_offsets_seconds:
            handle = client.get_schedule_handle(schedule_id)
            await handle.trigger(overlap=_overlap_policy(req.overlap_policy))
        positive_offsets = [o for o in req.fire_offsets_seconds if o > 0]
        if req.fire_offsets_seconds and not positive_offsets:
            handle = client.get_schedule_handle(schedule_id)
            try:
                await handle.delete()
            except Exception as e:  # noqa: BLE001
                _ = e
    except ScheduleAlreadyRunningError:
        return JSONResponse(
            {"status": "already_exists", "id": schedule_id}, status_code=200
        )
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=500)
    if warnings:
        return JSONResponse({"warnings": warnings}, status_code=201)
    return Response(status_code=201)


@router.delete(
    "/schedules/{schedule_id}", status_code=204, responses=error_responses(500)
)
async def delete_schedule(request: Request) -> Response:
    schedule_id = str(request.path_params.get("schedule_id") or "")
    handle = temporal_client(request).get_schedule_handle(schedule_id)
    try:
        await handle.delete()
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=500)
    return Response(status_code=204)


@router.get(
    "/schedules/{schedule_id}",
    response_model=ScheduleDescriptionResponse,
    responses=error_responses(403, 404, 500, 503),
)
async def get_schedule(request: Request) -> Response:
    schedule_id = str(request.path_params.get("schedule_id") or "")
    handle = temporal_client(request).get_schedule_handle(schedule_id)
    try:
        desc = await handle.describe()
    except RPCError as e:
        if e.status == RPCStatusCode.NOT_FOUND:
            return Response(str(e), status_code=404)
        if e.status in (RPCStatusCode.PERMISSION_DENIED, RPCStatusCode.UNAUTHENTICATED):
            return Response(str(e), status_code=403)
        if e.status in (RPCStatusCode.DEADLINE_EXCEEDED, RPCStatusCode.UNAVAILABLE):
            return Response(str(e), status_code=503)
        return Response(str(e), status_code=500)
    except Exception as e:  # noqa: BLE001
        return Response(str(e), status_code=500)
    paused = bool(getattr(desc.schedule.state, "paused", False))
    info = desc.info
    payload: dict[str, Any] = {
        "id": schedule_id,
        "paused": paused,
        "next_fires": [_aware_utc_isoformat(t) for t in (info.next_action_times or [])],
        "running": [
            {
                "workflow_id": getattr(w, "workflow_id", ""),
                "first_execution_run_id": getattr(w, "first_execution_run_id", ""),
            }
            for w in (info.running_actions or [])
        ],
        "recent": [
            {
                "scheduled_at": _aware_utc_isoformat(a.scheduled_at),
                "started_at": _aware_utc_isoformat(a.started_at)
                if getattr(a, "started_at", None)
                else "",
            }
            for a in (info.recent_actions or [])
        ],
    }
    return JSONResponse(payload)


def _typed_search_attributes(
    values: dict[str, str | list[str]],
    workflow_type: str,
    supported_attrs: list[SearchAttributeSpec],
) -> tcommon.TypedSearchAttributes:
    pairs: list[tcommon.SearchAttributePair] = []
    supported = {attr.name for attr in supported_attrs}
    for name, value in values.items():
        if name not in supported:
            supported_list = ", ".join(sorted(supported)) or "none"
            raise ValueError(
                f"unsupported search attribute for {workflow_type}: {name}; supported: {supported_list}"
            )
        key = SEARCH_ATTRIBUTE_KEYS.get(name)
        if key is None:
            raise ValueError(f"search attribute has no registry typed key: {name}")
        pairs.append(tcommon.SearchAttributePair(key, value))
    return tcommon.TypedSearchAttributes(pairs)


def _schedule_input_warnings(
    values: dict[str, Any], input_warnings: list[InputWarning]
) -> list[dict[str, str]]:
    warnings: list[dict[str, str]] = []
    for warning in input_warnings:
        value = values.get(warning.field)
        if value is None or value == "" or value == [] or value == {}:
            warnings.append({"field": warning.field, "message": warning.message})
    return warnings


def _schedule_spec(req: ScheduleStartRequest) -> ScheduleSpec:
    if not req.fire_offsets_seconds:
        return ScheduleSpec(
            intervals=[
                ScheduleIntervalSpec(every=timedelta(seconds=req.interval_seconds))
            ],
            start_at=req.start_at,
            end_at=req.end_at,
        )

    now = _ceil_to_second(datetime.now(timezone.utc))
    positive_offsets = sorted(
        {offset for offset in req.fire_offsets_seconds if offset > 0}
    )
    if not positive_offsets:
        far_future = now + timedelta(days=3650)
        return ScheduleSpec(
            intervals=[ScheduleIntervalSpec(every=timedelta(days=3650))],
            start_at=far_future,
            end_at=far_future + timedelta(seconds=1),
        )

    calendars = []
    for offset in positive_offsets:
        target = now + timedelta(seconds=offset)
        calendars.append(
            ScheduleCalendarSpec(
                second=[ScheduleRange(target.second)],
                minute=[ScheduleRange(target.minute)],
                hour=[ScheduleRange(target.hour)],
                day_of_month=[ScheduleRange(target.day)],
                month=[ScheduleRange(target.month)],
                year=[ScheduleRange(target.year)],
                comment=f"relative offset {offset}s",
            )
        )
    max_offset = positive_offsets[-1]
    return ScheduleSpec(
        calendars=calendars,
        start_at=now,
        end_at=now + timedelta(seconds=max_offset + 2),
        time_zone_name="UTC",
    )


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _ceil_to_second(value: datetime) -> datetime:
    if value.microsecond == 0:
        return value
    return value.replace(microsecond=0) + timedelta(seconds=1)


def _aware_utc_isoformat(value: datetime) -> str:
    return _aware_utc(value).isoformat()


def _overlap_policy(policy: str) -> ScheduleOverlapPolicy:
    return {
        "skip": ScheduleOverlapPolicy.SKIP,
        "buffer_one": ScheduleOverlapPolicy.BUFFER_ONE,
        "buffer_all": ScheduleOverlapPolicy.BUFFER_ALL,
        "cancel_other": ScheduleOverlapPolicy.CANCEL_OTHER,
        "terminate_other": ScheduleOverlapPolicy.TERMINATE_OTHER,
        "allow_all": ScheduleOverlapPolicy.ALLOW_ALL,
    }[policy]
