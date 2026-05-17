"""Typed Temporal search attribute keys used by registry routes."""

from __future__ import annotations

from typing import Any

from temporalio.common import SearchAttributeKey


SA_KEY_AGENT_ID = SearchAttributeKey.for_keyword("agent_id")
SA_KEY_AGENT_EVENT_TYPES = SearchAttributeKey.for_keyword_list("agent_event_types")
SA_KEY_TOOLS_USED = SearchAttributeKey.for_keyword_list("tools_used")
SA_KEY_AGENT_ACP_PROVIDER = SearchAttributeKey.for_keyword("agent_acp_provider")


SEARCH_ATTRIBUTE_KEYS: dict[str, SearchAttributeKey[Any]] = {
    SA_KEY_AGENT_ID.name: SA_KEY_AGENT_ID,
    SA_KEY_AGENT_ACP_PROVIDER.name: SA_KEY_AGENT_ACP_PROVIDER,
    SA_KEY_AGENT_EVENT_TYPES.name: SA_KEY_AGENT_EVENT_TYPES,
    SA_KEY_TOOLS_USED.name: SA_KEY_TOOLS_USED,
}
