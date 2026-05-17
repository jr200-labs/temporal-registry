"""Pydantic response schemas for HTTP metrics snapshots."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class MetricsSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requests_total: int = Field(ge=0)
    errors_total: int = Field(ge=0)
    in_flight: int = Field(ge=0)
    latency_seconds_sum: float = Field(ge=0)
    latency_seconds_count: int = Field(ge=0)
    by_route: dict[str, int]
