"""Tests for the estimated linear-weights draw-propagation helper.

The SQLMesh ``@model`` file imports the project dialect (``SMALLINT`` etc.)
which stock sqlglot can't parse without a SQLMesh context, so we exercise the
pure ``propagate_linear_weights_draws`` helper directly, mirroring the other
coverage manifest-ingest tests.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical.linear_weights_estimated import (
    DETERMINISTIC_LINEAR_WEIGHTS_SQL,
    DETERMINISTIC_PLAY_FLOOR,
    RUN_VALUE_SUMMARY_SCHEMA,
    propagate_linear_weights_draws,
)

_PLAY_FLOOR_PATTERN = re.compile(
    r"QUALIFY\s+COUNT\(\*\)\s+OVER\s+result\s*>\s*(\d+)", re.IGNORECASE
)


def _sql_play_floor(sql_path: Path) -> int:
    matches = _PLAY_FLOOR_PATTERN.findall(sql_path.read_text(encoding="utf-8"))
    if len(matches) != 1:
        raise AssertionError(
            f"{sql_path} must carry exactly one 'QUALIFY COUNT(*) OVER result > N' "
            f"floor, found {len(matches)}"
        )
    return int(matches[0])


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
    re = {
        0: {"0_0": 0.50, "1_1": 0.90, "2_0": 0.10},
        1: {"0_0": 0.60, "1_1": 1.00, "2_0": 0.20},
    }
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
    out = propagate_linear_weights_draws(
        _transition_counts(), _re_draws(), dirichlet_alpha=None, min_events_per_play=0
    )

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


def _re_draws_many(n_draws: int, *, seed: int) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    means = {"0_0": 0.50, "1_1": 0.90, "2_0": 0.10}
    rows: list[dict[str, object]] = []
    for state, m in means.items():
        values = m + rng.normal(0.0, 0.05, size=n_draws)
        for draw in range(n_draws):
            rows.append(
                {
                    "state": state,
                    "season": 2021,
                    "league": "NL",
                    "outcome": "runs_to_end",
                    "value": float(values[draw]),
                    "chain": 0,
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


def _transition_counts_scaled(scale: int) -> pl.DataFrame:
    return _transition_counts().with_columns(
        (pl.col("n") * scale).cast(pl.Int64).alias("n")
    )


def _band_widths(out: pl.DataFrame) -> dict[str, float]:
    return {
        row["play"]: row["run_value_hdi_upper"] - row["run_value_hdi_lower"]
        for row in out.iter_rows(named=True)
    }


def test_dense_cells_match_fixed_weights_to_first_order() -> None:
    re_draws = _re_draws_many(600, seed=11)
    counts = _transition_counts_scaled(1_000_000)

    fixed = propagate_linear_weights_draws(
        counts, re_draws, dirichlet_alpha=None, min_events_per_play=0
    )
    dirichlet = propagate_linear_weights_draws(counts, re_draws, min_events_per_play=0)

    fixed_w = _band_widths(fixed)
    dirichlet_w = _band_widths(dirichlet)
    assert set(fixed_w) == set(dirichlet_w)
    for play, width in fixed_w.items():
        assert dirichlet_w[play] == pytest.approx(width, rel=0.02, abs=2e-3)


def test_sparse_cells_widen_bands_strictly() -> None:
    re_draws = _re_draws_many(800, seed=13)
    counts = _transition_counts()

    fixed = propagate_linear_weights_draws(
        counts, re_draws, dirichlet_alpha=None, min_events_per_play=0
    )
    dirichlet = propagate_linear_weights_draws(counts, re_draws, min_events_per_play=0)

    fixed_w = _band_widths(fixed)
    dirichlet_w = _band_widths(dirichlet)
    for play, width in fixed_w.items():
        assert dirichlet_w[play] > width


def _constant_erc_fixture(n_draws: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    rng = np.random.default_rng(5)
    rows: list[dict[str, object]] = []
    per_draw = rng.normal(0.4, 0.1, size=n_draws)
    for state in ("0_0", "1_0"):
        for draw in range(n_draws):
            rows.append(
                {
                    "state": state,
                    "season": 2021,
                    "league": "NL",
                    "outcome": "runs_to_end",
                    "value": float(per_draw[draw]),
                    "chain": 0,
                    "draw": draw,
                }
            )
    re_draws = pl.DataFrame(rows).select(
        pl.col("state").cast(pl.Utf8),
        pl.col("season").cast(pl.Int16),
        pl.col("league").cast(pl.Utf8),
        pl.col("outcome").cast(pl.Utf8),
        pl.col("value").cast(pl.Float64),
        pl.col("chain").cast(pl.Int16),
        pl.col("draw").cast(pl.Int32),
    )
    combos = [
        ("A", "2021_NL_0_0", "2021_NL_1_0"),
        ("B", "2021_NL_1_0", "2021_NL_0_0"),
        ("B", "2021_NL_0_0", "2021_NL_1_0"),
    ]
    counts = pl.DataFrame(
        [
            {
                "season": 2021,
                "league": "NL",
                "play": play,
                "play_category": "BATTING",
                "run_expectancy_start_key": start,
                "run_expectancy_end_key": end,
                "runs_on_play": 0,
                "n": n,
            }
            for (play, start, end), n in zip(combos, (2, 3, 5))
        ]
    ).select(
        pl.col("season").cast(pl.Int16),
        pl.col("league").cast(pl.Utf8),
        pl.col("play").cast(pl.Utf8),
        pl.col("play_category").cast(pl.Utf8),
        pl.col("run_expectancy_start_key").cast(pl.Utf8),
        pl.col("run_expectancy_end_key").cast(pl.Utf8),
        pl.col("runs_on_play").cast(pl.Int64),
        pl.col("n").cast(pl.Int64),
    )
    return counts, re_draws


def test_centering_holds_per_draw_under_dirichlet() -> None:
    counts, re_draws = _constant_erc_fixture(400)
    out = propagate_linear_weights_draws(counts, re_draws, min_events_per_play=0)

    for row in out.iter_rows(named=True):
        assert row["run_value_mean"] == pytest.approx(0.0, abs=1e-12)
        assert row["run_value_sd"] == pytest.approx(0.0, abs=1e-12)
        assert row["run_value_hdi_lower"] == pytest.approx(0.0, abs=1e-12)
        assert row["run_value_hdi_upper"] == pytest.approx(0.0, abs=1e-12)


def test_dirichlet_draws_are_deterministic() -> None:
    re_draws = _re_draws_many(300, seed=17)
    counts = _transition_counts()

    first = propagate_linear_weights_draws(counts, re_draws, min_events_per_play=0)
    second = propagate_linear_weights_draws(counts, re_draws, min_events_per_play=0)
    assert first.equals(second)


def test_base_seed_changes_the_draws() -> None:
    re_draws = _re_draws_many(300, seed=19)
    counts = _transition_counts()

    default = propagate_linear_weights_draws(counts, re_draws, min_events_per_play=0)
    reseeded = propagate_linear_weights_draws(
        counts, re_draws, base_seed=99, min_events_per_play=0
    )
    assert not default.equals(reseeded)


def test_row_order_does_not_affect_output() -> None:
    re_draws = _re_draws_many(300, seed=29)
    counts = _transition_counts_scaled(50)

    baseline = propagate_linear_weights_draws(counts, re_draws, min_events_per_play=0)

    shuffled_counts = counts[
        np.random.default_rng(7).permutation(counts.height).tolist()
    ]
    shuffled = propagate_linear_weights_draws(
        shuffled_counts, re_draws, min_events_per_play=0
    )

    baseline_sorted = baseline.sort(["season", "league", "play"])
    shuffled_sorted = shuffled.sort(["season", "league", "play"])
    assert baseline_sorted.equals(shuffled_sorted)


def test_does_not_disturb_global_numpy_state() -> None:
    re_draws = _re_draws_many(200, seed=23)
    counts = _transition_counts()

    before = np.random.get_state(legacy=False)
    _ = propagate_linear_weights_draws(counts, re_draws, min_events_per_play=0)
    after = np.random.get_state(legacy=False)

    assert before["bit_generator"] == after["bit_generator"]
    assert before["state"]["pos"] == after["state"]["pos"]
    assert np.array_equal(before["state"]["key"], after["state"]["key"])
    assert before["has_gauss"] == after["has_gauss"]


def test_every_row_below_the_pinned_floor_is_observed() -> None:
    out = propagate_linear_weights_draws(
        _transition_counts(), _re_draws(), min_events_per_play=0
    )
    assert "is_imputed" in out.columns
    assert out.get_column("is_imputed").to_list() == [False] * out.height


def test_pinned_floor_equals_the_deterministic_sql_floor() -> None:
    assert DETERMINISTIC_LINEAR_WEIGHTS_SQL.exists()
    assert DETERMINISTIC_PLAY_FLOOR == _sql_play_floor(DETERMINISTIC_LINEAR_WEIGHTS_SQL)
    assert DETERMINISTIC_PLAY_FLOOR > 0


def test_sql_floor_parse_fails_loudly_without_a_floor_clause(tmp_path: Path) -> None:
    bare = tmp_path / "linear_weights.sql"
    bare.write_text("SELECT 1", encoding="utf-8")
    with pytest.raises(AssertionError, match="floor"):
        _ = _sql_play_floor(bare)


def test_default_floor_is_the_pinned_constant() -> None:
    floor = DETERMINISTIC_PLAY_FLOOR
    counts = _transition_counts().with_columns(pl.lit(floor).cast(pl.Int64).alias("n"))
    defaulted = propagate_linear_weights_draws(
        counts, _re_draws(), dirichlet_alpha=None
    )
    explicit = propagate_linear_weights_draws(
        counts, _re_draws(), dirichlet_alpha=None, min_events_per_play=floor
    )
    assert defaulted.equals(explicit)
    by_play = {row["play"]: row for row in defaulted.iter_rows(named=True)}
    assert by_play["Single"]["n_events"] == floor
    assert by_play["Single"]["is_imputed"]
    assert by_play["InPlayOut"]["n_events"] == 2 * floor
    assert not by_play["InPlayOut"]["is_imputed"]

    above = propagate_linear_weights_draws(
        counts, _re_draws(), dirichlet_alpha=None, min_events_per_play=floor - 1
    )
    assert above.get_column("is_imputed").to_list() == [False, False]


def _two_cell_counts() -> pl.DataFrame:
    dense = _transition_counts_scaled(200)
    sparse = _transition_counts().with_columns(pl.lit("AL").alias("league"))
    return pl.concat([dense, sparse])


def _two_cell_draws() -> pl.DataFrame:
    nl = _re_draws()
    al = nl.with_columns(
        pl.lit("AL").alias("league"), (pl.col("value") * 2.0).alias("value")
    )
    return pl.concat([nl, al])


def test_cells_at_or_below_floor_take_pooled_value_and_flag() -> None:
    out = propagate_linear_weights_draws(
        _two_cell_counts(),
        _two_cell_draws(),
        dirichlet_alpha=None,
        min_events_per_play=100,
    )
    rows = {(r["league"], r["play"]): r for r in out.iter_rows(named=True)}
    assert not rows[("NL", "Single")]["is_imputed"]
    assert not rows[("NL", "InPlayOut")]["is_imputed"]
    assert rows[("AL", "Single")]["is_imputed"]
    assert rows[("AL", "InPlayOut")]["is_imputed"]

    counts = _two_cell_counts().with_columns(pl.col("n").cast(pl.Float64))
    draws = _two_cell_draws()
    pooled_expected: dict[str, list[float]] = {}
    for d in (0, 1):
        re = {
            (row["league"], row["state"]): row["value"]
            for row in draws.filter(pl.col("draw") == d).iter_rows(named=True)
        }
        erc = []
        for row in counts.iter_rows(named=True):
            start = re[
                (
                    row["league"],
                    "_".join(row["run_expectancy_start_key"].split("_")[-2:]),
                )
            ]
            end_state = "_".join(row["run_expectancy_end_key"].split("_")[-2:])
            end = 0.0 if end_state.startswith("3_") else re[(row["league"], end_state)]
            erc.append((row["play"], row["n"], row["runs_on_play"] + end - start))
        total_w = sum(n for _p, n, _e in erc)
        all_mean = sum(n * e for _p, n, e in erc) / total_w
        for play in ("Single", "InPlayOut"):
            w = sum(n for p, n, _e in erc if p == play)
            m = sum(n * e for p, n, e in erc if p == play) / w
            pooled_expected.setdefault(play, []).append(m - all_mean)
    for play in ("Single", "InPlayOut"):
        per_draw = np.array(pooled_expected[play])
        assert rows[("AL", play)]["run_value_mean"] == pytest.approx(per_draw.mean())
        assert rows[("AL", play)]["run_value_sd"] == pytest.approx(per_draw.std(ddof=1))


def test_imputed_rows_keep_their_own_event_count() -> None:
    out = propagate_linear_weights_draws(
        _two_cell_counts(), _two_cell_draws(), min_events_per_play=100
    )
    sparse = out.filter(pl.col("league") == "AL")
    assert dict(zip(sparse.get_column("play"), sparse.get_column("n_events"))) == {
        "Single": 3,
        "InPlayOut": 7,
    }


def test_rows_whose_start_state_has_no_posterior_cell_are_dropped() -> None:
    counts = pl.concat(
        [
            _transition_counts(),
            pl.DataFrame(
                [
                    {
                        "season": 2021,
                        "league": "NL",
                        "play": "Walk",
                        "play_category": "BATTING",
                        "run_expectancy_start_key": "2021_NL_0_7",
                        "run_expectancy_end_key": "2021_NL_0_7",
                        "runs_on_play": 1,
                        "n": 4,
                    }
                ]
            ).select(
                pl.col("season").cast(pl.Int16),
                pl.col("league").cast(pl.Utf8),
                pl.col("play").cast(pl.Utf8),
                pl.col("play_category").cast(pl.Utf8),
                pl.col("run_expectancy_start_key").cast(pl.Utf8),
                pl.col("run_expectancy_end_key").cast(pl.Utf8),
                pl.col("runs_on_play").cast(pl.Int64),
                pl.col("n").cast(pl.Int64),
            ),
        ]
    )
    with_orphan = propagate_linear_weights_draws(
        counts, _re_draws(), dirichlet_alpha=None, min_events_per_play=0
    )
    without_orphan = propagate_linear_weights_draws(
        _transition_counts(), _re_draws(), dirichlet_alpha=None, min_events_per_play=0
    )
    assert "Walk" not in with_orphan.get_column("play").to_list()
    assert with_orphan.sort("play").equals(without_orphan.sort("play"))


def test_posterior_join_uses_row_league_not_key_group() -> None:
    counts = _transition_counts().with_columns(
        pl.lit("NN2").alias("league"),
        pl.col("run_expectancy_start_key").str.replace("NL", "Other"),
        pl.col("run_expectancy_end_key").str.replace("NL", "Other"),
    )
    draws = _re_draws().with_columns(pl.lit("NN2").alias("league"))
    out = propagate_linear_weights_draws(
        counts, draws, dirichlet_alpha=None, min_events_per_play=0
    )
    reference = propagate_linear_weights_draws(
        _transition_counts(), _re_draws(), dirichlet_alpha=None, min_events_per_play=0
    )
    assert out.get_column("league").unique().to_list() == ["NN2"]
    assert np.allclose(
        out.sort("play").get_column("run_value_mean").to_numpy(),
        reference.sort("play").get_column("run_value_mean").to_numpy(),
    )

    grouped_only = _re_draws().with_columns(pl.lit("Other").alias("league"))
    assert (
        propagate_linear_weights_draws(
            counts, grouped_only, dirichlet_alpha=None, min_events_per_play=0
        ).height
        == 0
    )
