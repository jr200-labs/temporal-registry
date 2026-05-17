# temporal-registry Agent Notes

- This project owns only the HTTP dispatch API, schedule API, registry workflow, observability, and Temporal client startup.
- Do not add worker-specific execution, provider selection, agent specs, bridge execution, or worker activities here; those belong in the worker implementation that registers with the registry.
- Keep structured data as Pydantic `BaseModel` or Pydantic dataclasses, with schema definitions in `*_schemas.py` files.
- Schedule datetimes supplied by users must include timezone information. Relative offsets are relative to the registry service server's current instant.
- `temporal.api_key` is optional for local dev but must only be used with TLS-enabled Temporal connections.
- OTEL dependencies are optional; guard imports so the base package works without the `otel` extra.
- Wire payloads must stay compatible with registered worker implementations; update affected worker repositories when changing registry signals, run inputs, or search attributes.

## Terminology

- Temporal frontend means the Temporal gRPC API endpoint (`TEMPORAL_ADDRESS`), not the browser UI.
- Temporal UI is only for humans inspecting workflows, schedules, and histories; `temporal-registry` does not call it.
- `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_TLS`, and `TEMPORAL_API_KEY` configure how the registry process connects to Temporal.
- `TEMPORAL_REGISTRY_URL` and `TEMPORAL_REGISTRY_TOKEN` are for clients calling the registry HTTP API.
- `TEMPORAL_REGISTRY_TOKEN` is not the Temporal API key; it maps to the registry HTTP `Authorization: Bearer ...` token.
- The registry workflow is the durable Temporal workflow storing workflow registrations, task queues, schemas, workers, and heartbeat state.
- The registry worker is the worker process started by `temporal-registry` to execute the registry workflow itself.
- The API exposes `/health` for process liveness, `/ready` for Temporal/registry workflow readiness, and `/registry/status` for workflow state.
- Runtime startup sends a `registry_service_started` signal and then low-frequency `registry_service_heartbeat` signals so the Temporal UI shows registry service activity without high-volume history churn.
