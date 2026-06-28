from __future__ import annotations

import importlib.util
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"


@contextmanager
def _temporary_env(values: dict[str, str | None]) -> Iterator[None]:
    previous = {key: os.environ.get(key) for key in values}
    try:
        for key, value in values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _load(name: str) -> Any:
    sys.path.insert(0, str(SCRIPTS_DIR))
    try:
        module_name = f"{name}_test_{uuid4().hex}"
        spec = importlib.util.spec_from_file_location(
            module_name, SCRIPTS_DIR / f"{name}.py"
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise
        return module
    finally:
        sys.path.remove(str(SCRIPTS_DIR))


def test_retarget_rewrites_ledger_schema_only() -> None:
    baseline = _load("baseline_data_coverage")
    queries = {
        "x": "SELECT COUNT(*) FROM main_models.event_observation_pitch",
        "y": "SELECT COUNT(*) FROM main_models.event_states_full",
    }
    with _temporary_env({baseline.ENV_LEDGER_SCHEMA: "main_models__myenv"}):
        out = baseline._retarget_ledger_queries(queries)
    assert out["x"] == "SELECT COUNT(*) FROM main_models__myenv.event_observation_pitch"
    assert out["y"] == "SELECT COUNT(*) FROM main_models.event_states_full"


def test_retarget_passthrough_when_unset() -> None:
    baseline = _load("baseline_data_coverage")
    queries = {"x": "SELECT COUNT(*) FROM main_models.event_observation_pitch"}
    with _temporary_env({baseline.ENV_LEDGER_SCHEMA: None}):
        out = baseline._retarget_ledger_queries(queries)
    assert out == queries


def test_scalar_deltas_skip_equal() -> None:
    compare = _load("compare_baseline")
    left = {"k": {"result": 5, "error": None}}
    right = {"k": {"result": 5, "error": None}}
    assert compare._scalar_deltas(left, right) == []


def test_scalar_deltas_report_change() -> None:
    compare = _load("compare_baseline")
    left = {"k": {"result": 5, "error": None}}
    right = {"k": {"result": 7, "error": None}}
    diffs = compare._scalar_deltas(left, right)
    assert len(diffs) == 1
    assert diffs[0]["label"] == "k"
    assert diffs[0]["left"] == 5
    assert diffs[0]["right"] == 7


def test_scalar_deltas_report_new_key() -> None:
    compare = _load("compare_baseline")
    left: dict[str, dict[str, Any]] = {}
    right = {"k": {"result": 7, "error": None}}
    diffs = compare._scalar_deltas(left, right)
    assert len(diffs) == 1
    assert diffs[0]["left"] is None
    assert diffs[0]["right"] == 7


def test_grouped_deltas_match_by_dimension_keys() -> None:
    compare = _load("compare_baseline")
    left = {
        "by_dim": {
            "result": [
                {"dim": "a", "row_count": 10},
                {"dim": "b", "row_count": 20},
            ],
            "error": None,
        }
    }
    right = {
        "by_dim": {
            "result": [
                {"dim": "a", "row_count": 10},
                {"dim": "b", "row_count": 21},
            ],
            "error": None,
        }
    }
    diffs = compare._grouped_deltas(left, right)
    assert len(diffs) == 1
    assert diffs[0]["label"] == "by_dim"
    rows = diffs[0]["diffs"]
    assert len(rows) == 1
    assert rows[0]["row_key"] == {"dim": "b"}
    assert rows[0]["left_count"] == 20
    assert rows[0]["right_count"] == 21


def test_grouped_deltas_use_team_season_count_alias() -> None:
    compare = _load("compare_baseline")
    left = {
        "tsc": {
            "result": [
                {"least_granular_source_type": "BoxScore", "team_season_count": 50}
            ],
            "error": None,
        }
    }
    right = {
        "tsc": {
            "result": [
                {"least_granular_source_type": "BoxScore", "team_season_count": 51}
            ],
            "error": None,
        }
    }
    diffs = compare._grouped_deltas(left, right)
    assert len(diffs) == 1
    assert diffs[0]["diffs"][0]["left_count"] == 50
    assert diffs[0]["diffs"][0]["right_count"] == 51


def test_parity_retarget_replaces_ledger_only() -> None:
    parity = _load("rollup_parity_checks")
    sql = """
        SELECT * FROM main_models.event_observation_pitch
        JOIN main_models.event_completeness_pitches USING (event_key)
    """
    with _temporary_env({parity.ENV_LEDGER_SCHEMA: "main_models__envx"}):
        out = parity._retarget_ledgers(sql)
    assert "main_models__envx.event_observation_pitch" in out
    assert "main_models.event_completeness_pitches" in out
