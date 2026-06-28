"""Synthetic-fixture tests for ``compute_marginal_linear_weights``."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import math

import polars as pl

from python_models.statistical.models.linear_weights import (
    compute_marginal_linear_weights,
)

SEASON = 2019
LEAGUE = "NL"


def _re_summary() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "state": ["0_0", "1_0", "2_0", "0_1"],
            "season": [SEASON] * 4,
            "league": [LEAGUE] * 4,
            "re_value_mean": [0.50, 0.27, 0.10, 0.90],
        }
    )


def _start_key(state: str) -> str:
    return f"{SEASON}_{LEAGUE}_{state}"


def test_single_play_type_arithmetic() -> None:
    events = pl.DataFrame(
        {
            "run_expectancy_start_key": [_start_key("0_0")],
            "run_expectancy_end_key": [_start_key("0_1")],
            "runs_on_play": [0],
            "result_family": ["single"],
            "season": [SEASON],
            "league": [LEAGUE],
        }
    )
    out = compute_marginal_linear_weights(_re_summary(), events)
    assert out.height == 1
    row = out.row(0, named=True)
    expected = 0 + 0.90 - 0.50
    assert math.isclose(row["linear_weight"], expected, rel_tol=0, abs_tol=1e-9)
    assert row["n_events"] == 1
    assert row["result_family"] == "single"


def test_inning_end_contributes_zero_v_end() -> None:
    events = pl.DataFrame(
        {
            "run_expectancy_start_key": [_start_key("2_0")],
            "run_expectancy_end_key": [f"{SEASON}_{LEAGUE}_3_0"],
            "runs_on_play": [1],
            "result_family": ["out_in_play"],
            "season": [SEASON],
            "league": [LEAGUE],
        }
    )
    out = compute_marginal_linear_weights(_re_summary(), events)
    row = out.row(0, named=True)
    expected = 1 + 0.0 - 0.10
    assert math.isclose(row["linear_weight"], expected, rel_tol=0, abs_tol=1e-9)


def test_mean_over_multiple_events_of_same_type() -> None:
    events = pl.DataFrame(
        {
            "run_expectancy_start_key": [_start_key("0_0"), _start_key("0_0")],
            "run_expectancy_end_key": [_start_key("1_0"), _start_key("2_0")],
            "runs_on_play": [0, 2],
            "result_family": ["out_in_play", "out_in_play"],
            "season": [SEASON, SEASON],
            "league": [LEAGUE, LEAGUE],
        }
    )
    out = compute_marginal_linear_weights(_re_summary(), events)
    assert out.height == 1
    row = out.row(0, named=True)
    d1 = 0 + 0.27 - 0.50
    d2 = 2 + 0.10 - 0.50
    expected = (d1 + d2) / 2
    assert math.isclose(row["linear_weight"], expected, rel_tol=0, abs_tol=1e-9)
    assert row["n_events"] == 2


def test_distinct_play_types_split_into_rows() -> None:
    events = pl.DataFrame(
        {
            "run_expectancy_start_key": [_start_key("0_0"), _start_key("0_0")],
            "run_expectancy_end_key": [_start_key("0_1"), _start_key("1_0")],
            "runs_on_play": [0, 0],
            "result_family": ["single", "out_in_play"],
            "season": [SEASON, SEASON],
            "league": [LEAGUE, LEAGUE],
        }
    )
    out = compute_marginal_linear_weights(_re_summary(), events)
    assert out.height == 2
    assert set(out.get_column("result_family").to_list()) == {"single", "out_in_play"}


def test_weights_finite_and_event_counts_positive() -> None:
    events = pl.DataFrame(
        {
            "run_expectancy_start_key": [
                _start_key("0_0"),
                _start_key("1_0"),
                _start_key("2_0"),
            ],
            "run_expectancy_end_key": [
                _start_key("0_1"),
                _start_key("2_0"),
                f"{SEASON}_{LEAGUE}_3_0",
            ],
            "runs_on_play": [0, 0, 0],
            "result_family": ["single", "out_in_play", "out_in_play"],
            "season": [SEASON, SEASON, SEASON],
            "league": [LEAGUE, LEAGUE, LEAGUE],
        }
    )
    out = compute_marginal_linear_weights(_re_summary(), events)
    assert out.height >= 1
    for w in out.get_column("linear_weight").to_list():
        assert math.isfinite(w)
    for n in out.get_column("n_events").to_list():
        assert n > 0


def test_lookup_keys_on_grouped_season_league_from_key_not_raw_columns() -> None:
    summary = pl.DataFrame(
        {
            "state": ["0_0", "0_1"],
            "season": [1914, 1914],
            "league": ["Other", "Other"],
            "re_value_mean": [0.50, 0.90],
        }
    )
    events = pl.DataFrame(
        {
            "run_expectancy_start_key": ["1914_Other_0_0"],
            "run_expectancy_end_key": ["1914_Other_0_1"],
            "runs_on_play": [0],
            "result_family": ["single"],
            "season": [1910],
            "league": ["FL"],
        }
    )
    out = compute_marginal_linear_weights(summary, events)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["season"] == 1914
    assert row["league"] == "Other"
    assert row["n_events"] == 1
    assert math.isclose(row["linear_weight"], 0.90 - 0.50, rel_tol=0, abs_tol=1e-9)


def test_event_with_unknown_start_state_is_dropped() -> None:
    events = pl.DataFrame(
        {
            "run_expectancy_start_key": [_start_key("0_7"), _start_key("0_0")],
            "run_expectancy_end_key": [_start_key("0_1"), _start_key("0_1")],
            "runs_on_play": [0, 0],
            "result_family": ["single", "single"],
            "season": [SEASON, SEASON],
            "league": [LEAGUE, LEAGUE],
        }
    )
    out = compute_marginal_linear_weights(_re_summary(), events)
    row = out.row(0, named=True)
    assert row["n_events"] == 1
