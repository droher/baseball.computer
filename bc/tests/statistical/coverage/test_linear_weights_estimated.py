"""Tests for the estimated linear-weights draw-propagation helper.

The SQLMesh ``@model`` file imports the project dialect (``SMALLINT`` etc.)
which stock sqlglot can't parse without a SQLMesh context, so we exercise the
pure ``propagate_linear_weights_draws`` helper directly, mirroring the other
coverage manifest-ingest tests.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from python_models.statistical.linear_weights_estimated import (
    RUN_VALUE_SUMMARY_SCHEMA,
    propagate_linear_weights_draws,
)


def _re_draws() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    cells = {
        ("0_0", 2021, "NL"): (0.50, 0.60),
        ("1_1", 2021, "NL"): (0.90, 1.00),
        ("2_0", 2021, "NL"): (0.10, 0.20),
    }
    for (state, season, league), (v0, v1) in cells.items():
        for chain, draw, value in ((0, 0, v0), (0, 1, v1)):
            rows.append(
                {
                    "state": state,
                    "season": season,
                    "league": league,
                    "outcome": "runs_to_end",
                    "value": value,
                    "chain": chain,
                    "draw": draw,
                }
            )
    return pl.DataFrame(rows).select(
        pl.col("state").cast(pl.Utf8),
        pl.col("season").cast(pl.Int16),
        pl.col("league").cast(pl.Utf8),
        pl.col("outcome").cast(pl.Utf8),
        pl.col("value").cast(pl.Float64),
        pl.col("chain").cast(pl.Int16),
        pl.col("draw").cast(pl.Int32),
    )


def _transition_counts() -> pl.DataFrame:
    rows = [
        {
            "season": 2021,
            "league": "NL",
            "play": "Single",
            "play_category": "BATTING",
            "run_expectancy_start_key": "2021_NL_0_0",
            "run_expectancy_end_key": "2021_NL_1_1",
            "runs_on_play": 0,
            "n": 3,
        },
        {
            "season": 2021,
            "league": "NL",
            "play": "InPlayOut",
            "play_category": "BATTING",
            "run_expectancy_start_key": "2021_NL_1_1",
            "run_expectancy_end_key": "2021_NL_2_0",
            "runs_on_play": 1,
            "n": 2,
        },
        {
            "season": 2021,
            "league": "NL",
            "play": "InPlayOut",
            "play_category": "BATTING",
            "run_expectancy_start_key": "2021_NL_2_0",
            "run_expectancy_end_key": "2021_NL_3_0",
            "runs_on_play": 0,
            "n": 5,
        },
    ]
    return pl.DataFrame(rows).select(
        pl.col("season").cast(pl.Int16),
        pl.col("league").cast(pl.Utf8),
        pl.col("play").cast(pl.Utf8),
        pl.col("play_category").cast(pl.Utf8),
        pl.col("run_expectancy_start_key").cast(pl.Utf8),
        pl.col("run_expectancy_end_key").cast(pl.Utf8),
        pl.col("runs_on_play").cast(pl.Int64),
        pl.col("n").cast(pl.Int64),
    )


def _reference_centered() -> dict[tuple[str, int], float]:
    re = {0: {"0_0": 0.50, "1_1": 0.90, "2_0": 0.10}, 1: {"0_0": 0.60, "1_1": 1.00, "2_0": 0.20}}
    out: dict[tuple[str, int], float] = {}
    for d in (0, 1):
        single_erc = 0 + re[d]["1_1"] - re[d]["0_0"]
        ipo1_erc = 1 + re[d]["2_0"] - re[d]["1_1"]
        ipo2_erc = 0 + 0.0 - re[d]["2_0"]
        single_w = single_erc
        ipo_w = (2 * ipo1_erc + 5 * ipo2_erc) / (2 + 5)
        all_play_w = (3 * single_erc + 2 * ipo1_erc + 5 * ipo2_erc) / (3 + 2 + 5)
        out[("Single", d)] = single_w - all_play_w
        out[("InPlayOut", d)] = ipo_w - all_play_w
    return out


def test_empty_inputs_yield_typed_empty_frame() -> None:
    empty_counts = _transition_counts().clear()
    out = propagate_linear_weights_draws(empty_counts, _re_draws())
    assert out.height == 0
    assert dict(out.schema) == RUN_VALUE_SUMMARY_SCHEMA

    out2 = propagate_linear_weights_draws(_transition_counts(), _re_draws().clear())
    assert out2.height == 0
    assert dict(out2.schema) == RUN_VALUE_SUMMARY_SCHEMA


def test_draw_propagation_and_centering_matches_hand_computation() -> None:
    out = propagate_linear_weights_draws(_transition_counts(), _re_draws())

    assert dict(out.schema) == RUN_VALUE_SUMMARY_SCHEMA
    assert out.height == 2
    assert set(out.get_column("play").to_list()) == {"Single", "InPlayOut"}
    assert set(out.get_column("season").to_list()) == {2021}
    assert set(out.get_column("league").to_list()) == {"NL"}

    ref = _reference_centered()
    by_play = {row["play"]: row for row in out.iter_rows(named=True)}

    for play in ("Single", "InPlayOut"):
        per_draw = np.array([ref[(play, 0)], ref[(play, 1)]], dtype=np.float64)
        row = by_play[play]
        assert row["run_value_mean"] == pytest.approx(per_draw.mean())
        assert row["run_value_sd"] == pytest.approx(per_draw.std(ddof=1))
        assert row["run_value_hdi_lower"] == pytest.approx(per_draw.min())
        assert row["run_value_hdi_upper"] == pytest.approx(per_draw.max())
        assert row["play_category"] == "BATTING"


def test_n_events_equals_summed_occurrence_count_per_cell() -> None:
    counts = _transition_counts()
    out = propagate_linear_weights_draws(counts, _re_draws())

    expected = {
        (row["season"], row["league"], row["play"]): row["n_events"]
        for row in (
            counts.group_by(["season", "league", "play"])
            .agg(pl.col("n").sum().alias("n_events"))
            .iter_rows(named=True)
        )
    }
    assert expected == {(2021, "NL", "Single"): 3, (2021, "NL", "InPlayOut"): 7}

    for row in out.iter_rows(named=True):
        key = (row["season"], row["league"], row["play"])
        assert row["n_events"] == expected[key]
        assert isinstance(row["n_events"], int)


def test_centering_sums_to_zero_across_plays_per_draw() -> None:
    ref = _reference_centered()
    for d in (0, 1):
        weighted = 3 * ref[("Single", d)] + (2 + 5) * ref[("InPlayOut", d)]
        assert abs(weighted) < 1e-9
