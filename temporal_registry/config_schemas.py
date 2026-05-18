"""Pydantic schemas for registry service configuration."""

from __future__ import annotations

import os
import re

from pydantic import BaseModel, ConfigDict, Field, field_validator


_ENV_REF = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)(?::-([^}]*))?\}")


def _expand_env_refs(value: str) -> str:
    if not value or "${" not in value:
        return value
    return _ENV_REF.sub(lambda m: os.environ.get(m.group(1), m.group(2) or ""), value)


class ServerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str = "127.0.0.1"
    port: int = Field(default=8080, ge=1, le=65535)
    # Hard cap on request body size, applied before route handlers run. 1 MiB
    # is comfortably above any realistic /run or /schedules payload.
    max_request_body_bytes: int = Field(
        default=1024 * 1024, ge=1024, le=64 * 1024 * 1024
    )


class AuthConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # When enabled, every request must carry `Authorization: Bearer <token>`.
    # Defaults to enabled so that a misconfigured ingress fails closed; set
    # `enabled: false` only when an authenticated proxy sits in front.
    enabled: bool = True
    token: str = ""

    @field_validator("enabled", mode="before")
    @classmethod
    def _expand_enabled(cls, value: object) -> object:
        if isinstance(value, str):
            return _expand_env_refs(value)
        return value

    @field_validator("token")
    @classmethod
    def _expand_token(cls, value: str) -> str:
        return _expand_env_refs(value)


class RegistryConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_id: str = Field(min_length=1)
    workflow_type: str = Field(min_length=1)
    task_queue: str = Field(min_length=1)


class TemporalConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    address: str = Field(min_length=1)
    namespace: str = Field(min_length=1)
    namespace_retention_days: int = Field(default=30, ge=1, le=3650)
    tls: bool
    api_key: str = ""

    @field_validator("api_key", "address", "namespace")
    @classmethod
    def _expand_secret(cls, value: str) -> str:
        return _expand_env_refs(value)

    @field_validator("tls", mode="before")
    @classmethod
    def _expand_tls(cls, value: object) -> object:
        if isinstance(value, str):
            return _expand_env_refs(value)
        return value


class LoggingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    level: str = Field(default="INFO", min_length=1)
    access: bool = False

    @field_validator("level")
    @classmethod
    def _expand_level(cls, value: str) -> str:
        return _expand_env_refs(value)

    @field_validator("access", mode="before")
    @classmethod
    def _expand_access(cls, value: object) -> object:
        if isinstance(value, str):
            return _expand_env_refs(value)
        return value


class MetricsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    path: str = Field(default="/metrics", min_length=1)

    @field_validator("enabled", mode="before")
    @classmethod
    def _expand_enabled(cls, value: object) -> object:
        if isinstance(value, str):
            return _expand_env_refs(value)
        return value

    @field_validator("path")
    @classmethod
    def _starts_with_slash(cls, value: str) -> str:
        value = _expand_env_refs(value)
        if not value.startswith("/"):
            raise ValueError("metrics.path must start with '/'")
        return value


class OTelConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    service_name: str = Field(default="temporal-registry", min_length=1)
    endpoint: str = ""
    insecure: bool = False

    @field_validator("enabled", "insecure", mode="before")
    @classmethod
    def _expand_bool(cls, value: object) -> object:
        if isinstance(value, str):
            return _expand_env_refs(value)
        return value

    @field_validator("service_name", "endpoint")
    @classmethod
    def _expand_string(cls, value: str) -> str:
        return _expand_env_refs(value)


class ObservabilityConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    otel: OTelConfig = Field(default_factory=OTelConfig)


class RegistryServiceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    temporal: TemporalConfig
    server: ServerConfig = Field(default_factory=ServerConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    registry: RegistryConfig
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
