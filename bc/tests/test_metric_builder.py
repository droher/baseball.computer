"""Unit tests for the metric SQL + columns builders.

The diff harness in scripts/diff_models.py validates row-level output
against cached baselines. These tests catch regressions at the builder
layer, before they need full backfills to surface.
"""

from __future__ import annotations

import pytest

from python_models.metrics.builders import build_metric_sql
from python_models.metrics.columns import metric_columns, metric_column_descriptions

_KINDS = ("offense", "pitching", "fielding")
_SCOPES_AND_GRAIN = (
    ("player_career", ["player_id"]),
    ("player_season_league", ["player_id", "season", "league"]),
    ("team_season", ["team_id", "season"]),
)


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("scope,grain", _SCOPES_AND_GRAIN)
def test_build_metric_sql_returns_grouped_query(kind, scope, grain):
    sql = build_metric_sql(kind, grain)
    assert "SELECT" in sql.upper()
    assert "GROUP BY" in sql.upper()
    for g in grain:
        assert g in sql, f"grain column {g!r} missing from {kind}/{scope} SQL"


def test_build_metric_sql_rejects_unknown_kind():
    with pytest.raises(ValueError, match="Invalid kind"):
        build_metric_sql("baserunning", ["player_id"])  # type: ignore[arg-type]


def test_build_metric_sql_rejects_empty_grain():
    with pytest.raises(ValueError, match="grouping key"):
        build_metric_sql("offense", [])


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("scope,grain", _SCOPES_AND_GRAIN)
def test_metric_columns_includes_grain_and_event_coverage(kind, scope, grain):
    cols = metric_columns(kind, grain)
    for g in grain:
        assert g in cols, f"missing grain column {g!r} for {kind}/{scope}"
    assert "event_coverage_rate" in cols
    assert cols["event_coverage_rate"] == "DOUBLE"


@pytest.mark.parametrize("kind", _KINDS)
def test_metric_columns_typed_correctly(kind):
    """Counting stats are INTEGER; ratio metrics default to DOUBLE."""
    cols = metric_columns(kind, ["player_id"])
    int_count = sum(1 for v in cols.values() if v == "INTEGER")
    double_count = sum(1 for v in cols.values() if v == "DOUBLE")
    assert int_count > 0, "expected at least one INTEGER counting stat"
    assert double_count > 0, "expected at least one DOUBLE ratio metric"


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("scope,grain", _SCOPES_AND_GRAIN)
def test_descriptions_are_subset_of_columns(kind, scope, grain):
    """Every described column must exist in the schema (event_coverage_rate
    + the 3 fielding ratio metrics intentionally have no doc entry)."""
    cols = set(metric_columns(kind, grain))
    descs = set(metric_column_descriptions(kind, grain))
    assert descs.issubset(cols)


@pytest.mark.parametrize(
    "kind,source,name",
    [
        ("offense", "season", "on_base_plus_slugging"),
        ("offense", "event", "known_trajectory_out_hit_ratio"),
    ],
)
def test_derived_metrics_present_in_metrics_for(kind, source, name):
    from python_models.metrics import _metric_registrations  # noqa: F401
    from python_models.metrics.registry import metrics_for

    found = next(m for m in metrics_for(kind, source) if m.name == name)
    assert found.derived is not None, f"{name} should be a derived Metric"
