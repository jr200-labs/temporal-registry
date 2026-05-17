"""Load and validate registry service configuration from YAML."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import yaml

from .config_schemas import RegistryServiceConfig


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument(
        "-f",
        "--config",
        default="",
        help="Path to registry service YAML config.",
    )
    return parser


def parse_registry_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = build_arg_parser()
    return parser.parse_args(argv)


def _default_config_path() -> Path:
    return Path(__file__).with_name("config.yaml")


def load_registry_config(path: str | Path = "") -> RegistryServiceConfig:
    path = Path(path or _default_config_path())
    with path.open(encoding="utf-8") as f:
        raw: Any = yaml.safe_load(f) or {}
    return RegistryServiceConfig.model_validate(raw)
