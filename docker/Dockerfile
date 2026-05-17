# syntax=docker/dockerfile:1.7
# Build from the repo root so the build context includes pyproject.toml,
# uv.lock and temporal_registry/:
#   docker build -f docker/Dockerfile -t temporal-registry:dev .

ARG PYTHON_VERSION=3.14
FROM python:${PYTHON_VERSION}-slim AS build

WORKDIR /build

# uv resolves and installs from the locked dep graph; pin uv itself so build
# reproducibility doesn't slide as new uv releases ship.
ARG UV_VERSION=0.5.10
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir uv==${UV_VERSION}

# Order matters: lockfile + pyproject first so dep-only layers cache.
COPY pyproject.toml uv.lock README.md ./
COPY temporal_registry ./temporal_registry

# `uv sync --frozen` reads uv.lock and installs into a project-local .venv;
# `--no-dev` keeps lint/test deps out of the image.
RUN uv sync --frozen --no-dev


FROM python:${PYTHON_VERSION}-slim AS runtime

ARG APP_USER=app
ARG APP_UID=10001
ARG APP_GID=10001

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid ${APP_GID} ${APP_USER} \
    && useradd  --system --uid ${APP_UID} --gid ${APP_GID} --home /app --shell /sbin/nologin ${APP_USER}

WORKDIR /app

# Copy the resolved venv and the application source so console scripts work.
COPY --from=build /build/.venv /app/.venv
COPY --from=build /build/temporal_registry /app/temporal_registry

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1

USER ${APP_USER}

EXPOSE 8080

LABEL org.opencontainers.image.title="temporal-registry" \
      org.opencontainers.image.source="https://github.com/jr200-labs/temporal-registry"

CMD ["temporal-registry", "-f", "/etc/temporal-registry/config.yaml"]
