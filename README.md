# temporal-registry

Temporal-backed workflow registry, scheduler, and HTTP dispatch gateway.

## The Problem

Temporal gives you durable workflow execution, but clients still need to know a
lot before they can safely start work:

- Which workflow types exist.
- Which task queue can run each workflow.
- What input shape each workflow expects.
- Which search attributes are supported.
- Whether any worker that can run the workflow is currently alive.

Without a registry, that information tends to get copied into clients,
deployment config, docs, and worker code. Those copies drift. Clients start
hard-coding task queues, old workflow names stay around, schedule creation
depends on tribal knowledge, and independent worker deployments become tightly
coupled to every caller.

The operational failure mode is simple: the worker knows what it can run, but
the client does not have a reliable, current way to discover that before it
starts or schedules a workflow.

## The Solution

`temporal-registry` is a small always-on registry service for Temporal workflow
workers. It serves two purposes:

- It hosts an HTTP API for clients to discover, start, schedule, and administer
  registered workflows.
- It also runs as a Temporal worker for the registry workflow, which stores
  workflow registrations, task queues, schemas, workers, and heartbeat state.

Other workers register their runnable workflow types, task queues, input schemas,
search attributes, labels, and heartbeat TTLs. The registry stores that metadata
inside the durable registry workflow, then exposes it through the HTTP API.

Clients use the registry instead of hard-coding worker details. A caller can:

- List registered workflow types.
- Validate workflow input against the registered schema.
- Start a workflow on the currently registered task queue.
- Create schedules without knowing the worker deployment layout.
- Query supported search attributes for a workflow type.

Worker heartbeats keep availability current. If a worker stops heartbeating, the
registry can stop advertising that worker as a healthy dispatch target, while the
registry state itself remains durable because it is backed by Temporal.

This keeps ownership clean: workers own execution and capability registration;
clients own requests; `temporal-registry` owns discovery, validation, dispatch,
and scheduling. It does not execute workflow activities directly; registered
Temporal workers host the actual workflow code.

Use it when workers are deployed independently, workflow types change over time,
or multiple runtimes need to advertise capabilities into one shared Temporal
namespace.

## How To Use It

1. Start `temporal-registry` against your Temporal namespace.
2. Have each worker register the workflow types it can run, including task queue,
   input schema, labels, and supported search attributes.
3. Keep workers heartbeating so the registry knows which dispatch targets are
   currently healthy.
4. Point clients at the registry API to list workflows, validate inputs, start
   runs, or create schedules without hard-coding worker task queues.

## Terms And Configuration

`temporal-registry` sits between HTTP clients and Temporal. It hosts both the
registry HTTP API and the registry worker. The HTTP API talks to Temporal over
gRPC, while clients talk to `temporal-registry` over HTTP.

```text
client / curl / docs
        |
        | HTTP
        v
temporal-registry API
        |
        | Temporal gRPC
        v
Temporal frontend
        |
        v
registry workflow state
```

Temporal terms:

- `TEMPORAL_ADDRESS` is the Temporal frontend gRPC endpoint. Despite the word
  "frontend", this is not the browser UI.
- `TEMPORAL_NAMESPACE` is the Temporal namespace where the registry workflow and
  dispatched workflows run.
- `TEMPORAL_NAMESPACE` is created by the registry service at startup if it is
  missing. The default retention is 30 days unless the config's
  `temporal.namespace_retention_days` is changed.
- `TEMPORAL_TLS` controls whether the registry connects to Temporal using TLS.
- `TEMPORAL_API_KEY` authenticates the registry process to Temporal, if your
  Temporal deployment requires API-key auth.
- Temporal UI is the browser UI for humans to inspect workflow histories,
  schedules, and runs. It is separate from this service.

Registry terms:

- `TEMPORAL_REGISTRY_URL` is the HTTP URL for the `temporal-registry` API, for
  example `http://127.0.0.1:8080`.
- `TEMPORAL_REGISTRY_TOKEN` is the bearer token for the registry HTTP API when
  registry auth is enabled. It is not the Temporal API key.
- The registry workflow is the durable Temporal workflow that stores registered
  workflow types, task queues, schemas, workers, and heartbeat state.
- The registry worker is the worker process started by `temporal-registry` to
  execute the registry workflow itself.

## Quick Start

```bash
uv sync --frozen
uv run temporal-registry -f temporal_registry/config.yaml
```

The HTTP API publishes an OpenAPI spec at `/openapi.json` and interactive docs
at `/docs`.

Health and visibility endpoints:

- `/health` returns `ok` when the HTTP API process is alive.
- `/ready` checks that the API can query the registry workflow through Temporal.
- `/registry/status` returns registry workflow counts and the latest
  registry service startup marker.

The registry records a startup signal in workflow history. Runtime liveness stays
on `/health` and `/ready` so the registry workflow history does not grow from
periodic service heartbeat signals.

## Human-readable workflow IDs (slug counters)

By default the registry generates workflow IDs that include random hex
suffixes (`run-agent-hello-world-eb3bd463`). That keeps submissions
collision-free but the IDs are hard to scan, share, or group in the
Temporal UI.

The registry maintains a durable per-slug counter so callers can opt
in to predictable IDs:

```bash
# Submit a run with a human name. The registry slugifies the name,
# claims the next counter, and uses `<slug>-r<N>` as the workflow id.
curl -X POST "$TEMPORAL_REGISTRY_URL/run" \
  -H 'content-type: application/json' \
  -d '{
    "agent_id":  "hello-world",
    "workspace": "/tmp/work",
    "prompt":    "say hi",
    "name":      "tui-build"
  }'
# -> {"workflow_id": "tui-build-r1", ...}

# Same for graph workflows via the generic start endpoint.
curl -X POST "$TEMPORAL_REGISTRY_URL/workflows/agent.graph.run.v1/start" \
  -H 'content-type: application/json' \
  -d '{"input": {...}, "name": "tui-build"}'
# -> {"workflow_id": "tui-build-r2", ...}
```

Direct admin endpoints:

```bash
# Claim a counter without starting a workflow (useful for client-side
# id construction before submitting).
curl -X POST "$TEMPORAL_REGISTRY_URL/workflow-ids/claim" \
  -H 'content-type: application/json' \
  -d '{"name": "tui-build"}'
# -> {"slug": "tui-build", "counter": 3, "workflow_id": "tui-build-r3"}

# Reset a slug back to 0 (next claim returns r1).
curl -X POST "$TEMPORAL_REGISTRY_URL/workflow-ids/reset" \
  -H 'content-type: application/json' \
  -d '{"name": "tui-build"}'

# Inspect all current counters.
curl "$TEMPORAL_REGISTRY_URL/workflow-ids"
```

**Durability.** Counters live in the registry's own Temporal workflow
state (the singleton `workflow-registry`). Each claim is a Temporal
Workflow Update, serialised on the workflow and persisted to the same
Postgres backing store that holds the rest of Temporal's workflow
history. Multiple registry HTTP replicas are safe — they all signal
the same workflow. Worker / pod restarts preserve the state. The only
clobber risk is running two registries with the same `workflow_id`
against the same namespace, which is a deployment misconfiguration.

**Retention.** Counter entries are time-bounded (`SLUG_DEFAULT_TTL_SECONDS`,
30 days) and size-bounded (`SLUG_DEFAULT_MAX_ENTRIES`, 10 000 slugs).
The periodic GC sweep drops entries older than the TTL, then evicts
the LRU-oldest if the map still exceeds the cap. Both bounds keep the
workflow's history from growing unboundedly when callers spam unique
slugs.

**Slug normalisation.** Names are lowercased and any non-`[a-z0-9-]`
run is collapsed to `-` (`Tui_Build`, `tui build!`, `TUI-Build` all
hit the same counter). Leading/trailing dashes are stripped. A name
that slugifies to empty (e.g. `@@@`) is rejected with HTTP 400.

## Search Attribute Administration

Registered workers declare the custom search attributes their workflows use. The
registry provisions those attributes in the configured Temporal namespace before
advertising the worker or workflow. Missing attributes are created automatically;
existing attributes with the same type are left unchanged.

Type conflicts are not overwritten during normal registration. A same-name,
different-type attribute is a namespace-level admin concern, so the registry
fails provisioning and keeps the workflow unavailable until an operator reconciles
it explicitly.

List attributes declared by registered workflows:

```bash
curl "$TEMPORAL_REGISTRY_URL/registry/search-attributes"
```

List attributes that actually exist in the Temporal namespace:

```bash
curl "$TEMPORAL_REGISTRY_URL/registry/temporal/search-attributes"
```

Reconcile registered declarations against Temporal:

```bash
curl -X POST "$TEMPORAL_REGISTRY_URL/registry/temporal/search-attributes" \
  -H 'content-type: application/json' \
  -d '{"mode":"validate"}'
```

Reconcile modes:

- `validate` reports missing and conflicting attributes without changing Temporal.
- `ensure` creates missing attributes and refuses type conflicts.
- `replace` removes conflicting custom attributes and recreates them with the
  registered type. It requires an explicit `attributes` list and `confirm: true`.

Example explicit replacement:

```bash
curl -X POST "$TEMPORAL_REGISTRY_URL/registry/temporal/search-attributes" \
  -H 'content-type: application/json' \
  -d '{"mode":"replace","attributes":["agent_id"],"confirm":true}'
```

`replace` never removes system search attributes. Use it only as an admin
migration step because changing an attribute type can invalidate existing
visibility queries and assumptions.

Useful targets:

```bash
make test
make lint
make docker-build
```

## Contributing

Set up the project with `uv sync --frozen`, then run `make check` before opening
a change. Use `make fmt` to apply Ruff formatting and safe fixes.

For local git hooks:

```bash
make hooks-install
```

The hooks run shared config sync, Ruff, mypy, and commit message linting. Commit
messages should follow Conventional Commits because releases are managed by
release-please.

## License

MIT. See [LICENSE](LICENSE).
