"""Temporal client connection helpers for registry services."""

from __future__ import annotations

from temporalio.client import Client, TLSConfig

from .config_schemas import RegistryServiceConfig


def resolve(config: RegistryServiceConfig) -> tuple[str, str, bool, str | None]:
    address = config.temporal.address.strip()
    tls = config.temporal.tls
    namespace = config.temporal.namespace
    api_key = config.temporal.api_key or None
    if api_key and not tls:
        raise ValueError("temporal.api_key requires temporal.tls")
    return address, namespace, tls, api_key


async def connect(config: RegistryServiceConfig) -> Client:
    address, namespace, tls, api_key = resolve(config)
    kwargs: dict = {"namespace": namespace}
    if tls:
        kwargs["tls"] = TLSConfig()
    if api_key:
        kwargs["api_key"] = api_key
    return await Client.connect(address, **kwargs)
