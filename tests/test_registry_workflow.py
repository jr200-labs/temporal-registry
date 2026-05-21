"""Unit tests for registry workflow state and helper methods."""

from __future__ import annotations

from datetime import timezone

from temporal_registry.temporal.registry.registry_schemas import RegistryWorkflowSpec
from temporal_registry.temporal.registry.workflow import WorkerRegistry


def test_registry_parses_naive_timestamps_as_aware_utc() -> None:
    registry = WorkerRegistry()

    parsed = registry._parse_aware_utc("2026-05-16T01:02:03")

    assert parsed.tzinfo is timezone.utc
    assert parsed.isoformat() == "2026-05-16T01:02:03+00:00"


def test_registry_normalizes_aware_timestamps_to_utc() -> None:
    registry = WorkerRegistry()

    parsed = registry._parse_aware_utc("2026-05-16T10:02:03+09:00")

    assert parsed.tzinfo is timezone.utc
    assert parsed.isoformat() == "2026-05-16T01:02:03+00:00"


def test_registry_normalizes_input_warnings() -> None:
    registry = WorkerRegistry()

    warnings = registry._normalize_input_warnings(
        [
            {"field": " agent_acp_provider ", "message": " provider missing "},
            {"field": "", "message": "ignored"},
            {"field": "ignored", "message": ""},
            "not-a-dict",
        ]
    )

    assert warnings == [{"field": "agent_acp_provider", "message": "provider missing"}]


def test_registry_worker_registration_does_not_overwrite_conflicting_workflow_spec() -> (
    None
):
    registry = WorkerRegistry()
    original = RegistryWorkflowSpec(
        workflow_type="example.workflow.v1",
        version="1",
        task_queue="queue-a",
        description="original",
    )
    conflicting = RegistryWorkflowSpec(
        workflow_type="example.workflow.v1",
        version="1",
        task_queue="queue-b",
        description="changed",
    )

    registry._register_workflow_spec(original, overwrite=False, source="test")
    registry._register_workflow_spec(conflicting, overwrite=False, source="test")

    assert registry._workflows["example.workflow.v1"].description == "original"
    assert registry._workflows["example.workflow.v1"].task_queue == "queue-a"


def test_registry_put_workflow_can_overwrite_workflow_spec() -> None:
    registry = WorkerRegistry()
    original = RegistryWorkflowSpec(
        workflow_type="example.workflow.v1",
        version="1",
        task_queue="queue-a",
        description="original",
    )
    replacement = RegistryWorkflowSpec(
        workflow_type="example.workflow.v1",
        version="2",
        task_queue="queue-b",
        description="replacement",
    )

    registry._register_workflow_spec(original, overwrite=False, source="test")
    registry._register_workflow_spec(replacement, overwrite=True, source="test")

    assert registry._workflows["example.workflow.v1"].version == "2"
    assert registry._workflows["example.workflow.v1"].description == "replacement"


def test_registry_status_reports_registry_service_state() -> None:
    registry = WorkerRegistry()
    registry._registry_service_process_id = "host:123"
    registry._last_registry_service_started_epoch = 1.0

    status = registry.get_status()

    assert status["workflow_count"] == 0
    assert status["worker_count"] == 0
    assert status["registry_service_process_id"] == "host:123"
    assert status["last_registry_service_started_epoch"] == 1.0


# ---------- slug counter tests --------------------------------------------


def test_slugify_normalises_case_and_separators() -> None:
    from temporal_registry.temporal.registry.workflow import slugify

    assert slugify("Tui_Build") == "tui-build"
    assert slugify("  TUI Build!! ") == "tui-build"
    assert slugify("a/b\\c") == "a-b-c"
    assert slugify("-foo--bar-") == "foo--bar"
    assert slugify("@@@") == ""


def test_gc_slug_counters_drops_expired_entries() -> None:
    from temporal_registry.temporal.registry.registry_schemas import SlugCounter

    registry = WorkerRegistry()
    registry._slug_ttl_seconds = 100
    registry._slug_counters = {
        "old": SlugCounter(counter=5, last_claimed_epoch=0),
        "fresh": SlugCounter(counter=2, last_claimed_epoch=500),
    }

    registry._gc_slug_counters(now_epoch=600)

    # "old" was 600s stale (> 100s ttl) -> dropped; "fresh" was 100s stale,
    # exactly at the boundary, age > ttl is strict so it survives.
    assert "old" not in registry._slug_counters
    assert "fresh" in registry._slug_counters


def test_gc_slug_counters_caps_map_size_lru() -> None:
    from temporal_registry.temporal.registry.registry_schemas import SlugCounter

    registry = WorkerRegistry()
    registry._slug_ttl_seconds = 0  # disable TTL so we only test the cap leg
    registry._slug_max_entries = 3
    registry._slug_counters = {
        "a": SlugCounter(counter=1, last_claimed_epoch=1.0),
        "b": SlugCounter(counter=1, last_claimed_epoch=2.0),
        "c": SlugCounter(counter=1, last_claimed_epoch=3.0),
        "d": SlugCounter(counter=1, last_claimed_epoch=4.0),
        "e": SlugCounter(counter=1, last_claimed_epoch=5.0),
    }

    registry._gc_slug_counters(now_epoch=10.0)

    assert set(registry._slug_counters) == {"c", "d", "e"}


def test_gc_slug_counters_noop_when_empty() -> None:
    registry = WorkerRegistry()
    registry._slug_counters = {}
    registry._gc_slug_counters(now_epoch=10.0)
    assert registry._slug_counters == {}
