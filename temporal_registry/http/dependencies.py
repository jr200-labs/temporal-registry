"""Request dependency helpers for shared registry state."""

from __future__ import annotations

from fastapi import Request
from temporalio.client import Client

from ..config_schemas import RegistryServiceConfig


def temporal_client(request: Request) -> Client:
    return request.app.state.temporal_client


def registry_config(request: Request) -> RegistryServiceConfig:
    return request.app.state.registry_config
