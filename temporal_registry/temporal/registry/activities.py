from __future__ import annotations

from collections.abc import Callable
from typing import Any

from temporalio import activity
from temporalio.client import Client

from .client import ensure_search_attributes
from .activity_types import ACTIVITY_ENSURE_SEARCH_ATTRIBUTES
from .registry_schemas import SearchAttributeSpec


def make_registry_activities(
    temporal_client: Client,
    namespace: str,
) -> list[Callable[..., Any]]:
    @activity.defn(name=ACTIVITY_ENSURE_SEARCH_ATTRIBUTES)
    async def _ensure_search_attributes(payload: list[dict[str, Any]]) -> None:
        attrs = [SearchAttributeSpec.model_validate(item) for item in payload]
        await ensure_search_attributes(temporal_client, namespace, attrs)

    return [_ensure_search_attributes]
