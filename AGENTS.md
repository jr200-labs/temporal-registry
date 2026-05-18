# temporal-registry Agent Notes

- This project owns only the HTTP dispatch API, schedule API, registry workflow, observability, and Temporal client startup.
- Do not add worker-specific execution, provider selection, agent specs, bridge execution, or worker activities here; those belong in the worker implementation that registers with the registry.
- Keep structured data as Pydantic `BaseModel` or Pydantic dataclasses, with schema definitions in `*_schemas.py` files.
- Schedule datetimes supplied by users must include timezone information. Relative offsets are relative to the registry service server's current instant.
- `temporal.api_key` is optional for local dev but must only be used with TLS-enabled Temporal connections.
- OTEL dependencies are optional; guard imports so the base package works without the `otel` extra.
- Wire payloads must stay compatible with registered worker implementations; update affected worker repositories when changing registry signals, run inputs, or search attributes.
- The registry owns namespace-level Temporal search attribute provisioning for registered workflows. Normal registration may create missing custom attributes, but must never silently replace same-name/different-type attributes.
- Search attribute type replacement is an explicit admin operation through `POST /registry/temporal/search-attributes` with `mode=replace`, an explicit `attributes` list, and `confirm=true`. Never put replacement behavior in normal worker registration or workflow `PUT` paths.
- Keep Temporal search attribute admin operations in registry-owned activities/helpers so worker implementations do not need Temporal operator privileges.

## Terminology

- Temporal frontend means the Temporal gRPC API endpoint (`TEMPORAL_ADDRESS`), not the browser UI.
- Temporal UI is only for humans inspecting workflows, schedules, and histories; `temporal-registry` does not call it.
- `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_TLS`, and `TEMPORAL_API_KEY` configure how the registry process connects to Temporal.
- `TEMPORAL_REGISTRY_URL` and `TEMPORAL_REGISTRY_TOKEN` are for clients calling the registry HTTP API.
- `TEMPORAL_REGISTRY_TOKEN` is not the Temporal API key; it maps to the registry HTTP `Authorization: Bearer ...` token.
- The registry service ensures `TEMPORAL_NAMESPACE` exists at startup before starting the registry workflow. Namespace creation belongs in startup/bootstrap, not worker registration, because the registry workflow itself needs the namespace first.
- The registry workflow is the durable Temporal workflow storing workflow registrations, task queues, schemas, workers, and worker heartbeat state.
- The registry worker is the worker process started by `temporal-registry` to execute the registry workflow itself.
- The API exposes `/health` for process liveness, `/ready` for Temporal/registry workflow readiness, and `/registry/status` for workflow state.
- The API exposes `/registry/search-attributes` for registry-declared attributes and `/registry/temporal/search-attributes` for actual Temporal namespace attributes plus reconcile operations.
- Runtime startup sends one `registry_service_started` signal. Do not add periodic registry service heartbeat signals; use `/health` and `/ready` for API process liveness so workflow history stays bounded.
