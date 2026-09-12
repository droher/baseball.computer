from __future__ import annotations

import importlib.util
import uuid
from pathlib import Path
from typing import Any

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "preload_sources.py"


@pytest.fixture(scope="module")
def script() -> Any:
    spec = importlib.util.spec_from_file_location(
        f"preload_sources_test_{uuid.uuid4().hex}", SCRIPT_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_no_overrides_returns_the_defaults_unchanged(script: Any) -> None:
    roots = script.source_roots_from_overrides([])
    assert roots == script.DEFAULT_SOURCE_ROOTS
    assert roots is not script.DEFAULT_SOURCE_ROOTS


def test_override_replaces_only_the_named_schema(script: Any) -> None:
    defaults = script.DEFAULT_SOURCE_ROOTS
    schema = next(iter(defaults))
    roots = script.source_roots_from_overrides([f"{schema}=file:///tmp/parquet/"])
    assert roots[schema] == "file:///tmp/parquet"
    for other in defaults:
        if other != schema:
            assert roots[other] == defaults[other]


def test_malformed_or_unknown_overrides_are_rejected(script: Any) -> None:
    with pytest.raises(ValueError, match="schema=root"):
        _ = script.source_roots_from_overrides(["event"])
    with pytest.raises(ValueError, match="unknown source schema"):
        _ = script.source_roots_from_overrides(["nope=file:///x"])


def test_env_roots_replace_only_the_named_schema(script: Any) -> None:
    defaults = script.DEFAULT_SOURCE_ROOTS
    schema = next(iter(defaults))
    roots = script.source_roots_from_env(
        {f"BC_SOURCE_ROOT_{schema.upper()}": "file:///tmp/parquet/"}
    )
    assert roots[schema] == "file:///tmp/parquet"
    for other in defaults:
        if other != schema:
            assert roots[other] == defaults[other]
    assert script.source_roots_from_env({}) == defaults


def test_cli_override_beats_env(script: Any) -> None:
    defaults = script.DEFAULT_SOURCE_ROOTS
    schema = next(iter(defaults))
    env_roots = script.source_roots_from_env(
        {f"BC_SOURCE_ROOT_{schema.upper()}": "file:///env"}
    )
    roots = script.source_roots_from_overrides([f"{schema}=file:///cli"], env_roots)
    assert roots[schema] == "file:///cli"
