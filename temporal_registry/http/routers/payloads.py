"""JSON Schema validation helpers for workflow input payloads."""

from __future__ import annotations

import re
from typing import Any

import jsonschema
from jsonschema import Draft202012Validator

_REQUIRED_FIELD_RE = re.compile(r"^'(?P<field>[^']+)' is a required property$")


def validate_schema(
    payload: dict[str, Any], schema: dict[str, Any]
) -> list[dict[str, Any]]:
    """Validate `payload` against a JSON Schema. Returns a list of error dicts
    shaped like pydantic's ValidationError.errors() output so existing
    consumers and tests can keep their assertions stable."""
    if not schema:
        return []
    try:
        Draft202012Validator.check_schema(schema)
    except jsonschema.SchemaError as e:
        return [
            {"loc": (), "msg": f"invalid schema: {e.message}", "type": "schema_error"}
        ]
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(payload), key=lambda e: list(e.absolute_path))
    return [_format_error(err) for err in errors]


def _format_error(err: jsonschema.ValidationError) -> dict[str, Any]:
    loc: tuple[Any, ...] = tuple(err.absolute_path)
    if err.validator == "required":
        m = _REQUIRED_FIELD_RE.match(err.message)
        if m:
            loc = loc + (m.group("field"),)
    return {
        "loc": loc,
        "msg": err.message,
        "type": _classify(err),
    }


def _classify(err: jsonschema.ValidationError) -> str:
    validator = err.validator or ""
    if validator == "required":
        return "missing"
    if validator == "type":
        expected = err.validator_value if isinstance(err.validator_value, str) else ""
        return {
            "integer": "int_parsing",
            "number": "float_parsing",
            "string": "string_type",
            "boolean": "bool_parsing",
            "array": "list_type",
            "object": "dict_type",
        }.get(expected, f"{expected}_type" if expected else "type_error")
    if validator == "enum":
        return "enum"
    if validator == "pattern":
        return "string_pattern"
    return validator or "validation_error"
