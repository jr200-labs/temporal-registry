"""Logging setup helpers for structured registry service logs."""

from __future__ import annotations

import contextvars
import json
import logging
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from types import TracebackType
from typing import Any, Iterator


_workflow_id = contextvars.ContextVar[str]("workflow_id", default="")
_run_id = contextvars.ContextVar[str]("run_id", default="")
_request_id = contextvars.ContextVar[str]("request_id", default="")


_STANDARD_RECORD_ATTRS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        workflow_id = str(getattr(record, "workflow_id", "") or _workflow_id.get())
        run_id = str(getattr(record, "run_id", "") or _run_id.get())
        request_id = str(getattr(record, "request_id", "") or _request_id.get())
        if workflow_id:
            data["workflow_id"] = workflow_id
        if run_id:
            data["run_id"] = run_id
        if request_id:
            data["request_id"] = request_id
        for key, value in record.__dict__.items():
            if key in _STANDARD_RECORD_ATTRS or key.startswith("_"):
                continue
            if key in data:
                continue
            data[key] = _jsonable(value)
        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            data["stack"] = self.formatStack(record.stack_info)
        return json.dumps(data, separators=(",", ":"), sort_keys=True)


def configure_json_logging(level: str | int = logging.INFO) -> None:
    root = logging.getLogger()
    resolved_level = _resolve_level(level)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonLogFormatter())
    root.handlers = [handler]
    root.setLevel(resolved_level)


def temporal_log_extra(
    workflow_id: str = "", run_id: str = "", **values: Any
) -> dict[str, Any]:
    extra = {key: value for key, value in values.items() if value is not None}
    if workflow_id:
        extra["workflow_id"] = workflow_id
    if run_id:
        extra["run_id"] = run_id
    return extra


@contextmanager
def log_context(
    *,
    workflow_id: str = "",
    run_id: str = "",
    request_id: str = "",
) -> Iterator[None]:
    workflow_token = _workflow_id.set(workflow_id) if workflow_id else None
    run_token = _run_id.set(run_id) if run_id else None
    request_token = _request_id.set(request_id) if request_id else None
    try:
        yield
    finally:
        if request_token is not None:
            _request_id.reset(request_token)
        if run_token is not None:
            _run_id.reset(run_token)
        if workflow_token is not None:
            _workflow_id.reset(workflow_token)


def _resolve_level(level: str | int) -> int:
    if isinstance(level, int):
        return level
    return getattr(logging, level.upper(), logging.INFO)


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, BaseException):
        return str(value)
    if isinstance(value, type):
        return value.__name__
    if isinstance(value, TracebackType):
        return repr(value)
    return str(value)
