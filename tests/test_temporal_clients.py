"""Unit tests for Temporal client configuration resolution."""

from __future__ import annotations

import pytest

from temporal_registry import temporal_client as registry_client
from temporal_registry.config_schemas import RegistryServiceConfig


def _registry_config(address: str, tls: bool, namespace: str, api_key: str) -> RegistryServiceConfig:
    return RegistryServiceConfig.model_validate(
        {
            "temporal": {"address": address, "namespace": namespace, "tls": tls, "api_key": api_key},
            "server": {"host": "127.0.0.1", "port": 8080},
            "registry": {
                "workflow_id": "registry",
                "workflow_type": "registry.workflow",
                "task_queue": "registry-task-queue",
            },
            "observability": {
                "logging": {"level": "INFO", "access": False},
                "metrics": {"enabled": True, "path": "/metrics"},
                "otel": {"enabled": False, "service_name": "test", "endpoint": "", "insecure": True},
            },
        }
    )


def test_registry_resolve_passes_api_key_with_tls() -> None:
    assert registry_client.resolve(
        _registry_config("temporal.example.com:443", True, "prod", "shared-key")
    ) == (
        "temporal.example.com:443",
        "prod",
        True,
        "shared-key",
    )


def test_api_key_requires_tls() -> None:
    with pytest.raises(ValueError, match="api_key requires"):
        registry_client.resolve(_registry_config("127.0.0.1:7233", False, "default", "shared-key"))
