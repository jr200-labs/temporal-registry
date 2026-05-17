SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
CONFIG ?= $(ROOT_DIR)/temporal_registry/config.yaml

export TEMPORAL_ADDRESS ?= localhost:7233
export TEMPORAL_NAMESPACE ?= default
export TEMPORAL_TLS ?= 0
export TEMPORAL_API_KEY ?=
export TEMPORAL_REGISTRY_AUTH_ENABLED ?= false

RUN_DIR := $(ROOT_DIR)/.run
PID_FILE := $(RUN_DIR)/temporal-registry.pid
LOG_FILE := $(RUN_DIR)/temporal-registry.log
TEMPORAL_REGISTRY_URL ?= http://127.0.0.1:8080
TEMPORAL_REGISTRY_TOKEN ?=

.PHONY: help init test lint fmt check run up down docker-build hooks-install hooks-run clean

.DEFAULT_GOAL := help

## help: show this help
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /'

## init: install dependencies from uv.lock
init:
	@uv sync --frozen

## test: run tests
test:
	@uv run pytest

## lint: run Ruff
lint:
	@uv run ruff check .
	@uv run ruff format --check .
	@uv run mypy .

## check: run lint and tests
check: lint test

## fmt: apply Ruff fixes
fmt:
	@uv run ruff check --fix temporal_registry tests
	@uv run ruff format temporal_registry tests

## run: start the registry server
run:
	@uv run temporal-registry -f $(CONFIG)

## up: start the registry server in the background
up:
	@mkdir -p $(RUN_DIR)
	@if [ -f $(PID_FILE) ] && kill -0 "$$(cat $(PID_FILE))" 2>/dev/null; then \
		echo "temporal-registry already running: pid $$(cat $(PID_FILE))"; \
	else \
		uv run temporal-registry -f $(CONFIG) > $(LOG_FILE) 2>&1 & \
		echo $$! > $(PID_FILE); \
		echo "temporal-registry started: pid $$(cat $(PID_FILE)), log $(LOG_FILE)"; \
	fi

## down: shutdown the registry and stop the background server
down:
	@auth_args=(); \
	if [ -n "$(TEMPORAL_REGISTRY_TOKEN)" ]; then \
		auth_args=(-H "Authorization: Bearer $(TEMPORAL_REGISTRY_TOKEN)"); \
	fi; \
	if curl -fsS -X POST "$${auth_args[@]}" "$(TEMPORAL_REGISTRY_URL)/registry/shutdown" >/dev/null 2>&1; then \
		echo "temporal-registry shutdown signaled"; \
	else \
		echo "temporal-registry shutdown signal skipped or failed"; \
	fi
	@if [ -f $(PID_FILE) ]; then \
		pid="$$(cat $(PID_FILE))"; \
		if kill -0 "$$pid" 2>/dev/null; then \
			kill "$$pid"; \
			echo "temporal-registry stopped: pid $$pid"; \
		else \
			echo "temporal-registry not running: stale pid $$pid"; \
		fi; \
		rm -f $(PID_FILE); \
	else \
		echo "temporal-registry not running"; \
	fi

## docker-build: build the runtime image
docker-build:
	@docker build -f docker/Dockerfile -t temporal-registry:dev .

## hooks-install: install pre-commit hooks
hooks-install:
	@pre-commit install
	@pre-commit install --hook-type commit-msg

## hooks-run: run all pre-commit hooks
hooks-run:
	@pre-commit run --all-files

## clean: remove local caches
clean:
	@rm -rf .pytest_cache .ruff_cache .venv .pycache .run *.egg-info
