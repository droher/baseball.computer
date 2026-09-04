"""Tests for the offset-at-bound inversion of the single-class sensitivity sweep.

Invariants only, no DB. The offset at which a class's corrected marginal reaches
a target is monotone in the target, reproduces a forward evaluation, is zero at
the MAR baseline, and is `None` for unreachable targets; the per-era table
agrees with the per-era marginal ribbon and flags whether the grid contains it.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import math

import polars as pl
import pytest

from python_models.statistical.sensitivity import (
    DEFAULT_GRID,
    OFFSET_SEARCH_TOLERANCE_NATS,
    bound_offset_table,
    era_sensitivity_ribbon,
    marginal_shares,
    offset_reaching_share,
    sensitivity_ribbon,
)

FOCAL = "GroundBall"
CLASSES = ("Bunt", "Fly", "GroundBall", "LineDrive", "PopUp")


def _export(per_event: dict[int, tuple[str, dict[str, float]]]) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for event_key, (era, shares) in per_event.items():
        for class_index, (label, share) in enumerate(sorted(shares.items())):
            rows.append(
                {
                    "event_key": event_key,
                    "era_bucket": era,
                    "geometry_dimension": "trajectory",
                    "class_index": class_index,
                    "class_label": label,
                    "expected_share": share,
                }
            )
    return pl.DataFrame(rows)


@pytest.fixture
def export() -> pl.DataFrame:
    return _export(
        {
            1: (
                "pre-1950",
                {
                    "Bunt": 0.05,
                    "Fly": 0.30,
                    "GroundBall": 0.25,
                    "LineDrive": 0.20,
                    "PopUp": 0.20,
                },
            ),
            2: (
                "pre-1950",
                {
                    "Bunt": 0.10,
                    "Fly": 0.25,
                    "GroundBall": 0.30,
                    "LineDrive": 0.25,
                    "PopUp": 0.10,
                },
            ),
            3: (
                "pre-1950",
                {
                    "Bunt": 0.02,
                    "Fly": 0.40,
                    "GroundBall": 0.18,
                    "LineDrive": 0.30,
                    "PopUp": 0.10,
                },
            ),
            4: (
                "1988+",
                {
                    "Bunt": 0.08,
                    "Fly": 0.28,
                    "GroundBall": 0.40,
                    "LineDrive": 0.14,
                    "PopUp": 0.10,
                },
            ),
            5: (
                "1988+",
                {
                    "Bunt": 0.06,
                    "Fly": 0.22,
                    "GroundBall": 0.44,
                    "LineDrive": 0.18,
                    "PopUp": 0.10,
                },
            ),
        }
    )


def _era(export: pl.DataFrame, era: str) -> pl.DataFrame:
    return export.filter(pl.col("era_bucket") == era)


def test_offset_at_baseline_is_zero(export: pl.DataFrame) -> None:
    era_df = _era(export, "pre-1950")
    baseline = marginal_shares(era_df, offset={})[FOCAL]
    assert (
        offset_reaching_share(era_df, class_label=FOCAL, target_share=baseline) == 0.0
    )


def test_offset_reproduces_forward_evaluation(export: pl.DataFrame) -> None:
    era_df = _era(export, "pre-1950")
    for delta in (-1.3, -0.4, 0.2, 0.75, 1.6):
        target = marginal_shares(era_df, offset={FOCAL: delta})[FOCAL]
        found = offset_reaching_share(era_df, class_label=FOCAL, target_share=target)
        assert found is not None
        assert found == pytest.approx(delta, abs=1e-7)
        assert marginal_shares(era_df, offset={FOCAL: found})[FOCAL] == pytest.approx(
            target
        )


def test_offset_monotone_in_target(export: pl.DataFrame) -> None:
    era_df = _era(export, "pre-1950")
    baseline = marginal_shares(era_df, offset={})[FOCAL]
    targets = [
        baseline * 0.5,
        baseline * 0.9,
        baseline,
        baseline + 0.1,
        baseline + 0.3,
        0.9,
    ]
    deltas = [
        offset_reaching_share(era_df, class_label=FOCAL, target_share=t)
        for t in targets
    ]
    assert all(d is not None for d in deltas)
    finite = [d for d in deltas if d is not None]
    assert finite == sorted(finite)
    assert finite[0] < 0.0 < finite[-1]


def test_unreachable_target_is_none(export: pl.DataFrame) -> None:
    era_df = _era(export, "pre-1950")
    assert offset_reaching_share(era_df, class_label=FOCAL, target_share=1.0) is None
    assert offset_reaching_share(era_df, class_label=FOCAL, target_share=0.0) is None
    with pytest.raises(ValueError, match="not in export classes"):
        offset_reaching_share(era_df, class_label="Rocket", target_share=0.5)


def test_bound_table_matches_ribbon_and_flags_grid(export: pl.DataFrame) -> None:
    ribbon = era_sensitivity_ribbon(export, dimension="trajectory")
    pre = _era(export, "pre-1950")
    modern = _era(export, "1988+")
    pre_at_high = marginal_shares(pre, offset={FOCAL: max(DEFAULT_GRID)})[FOCAL]
    modern_baseline = marginal_shares(modern, offset={})[FOCAL]
    targets = {"pre-1950": pre_at_high + 0.05, "1988+": modern_baseline * 0.8}
    table = bound_offset_table(export, class_label=FOCAL, target_by_era=targets)
    assert table.get_column("era_bucket").to_list() == ["1988+", "pre-1950"]

    for row in table.iter_rows(named=True):
        era_ribbon = ribbon.filter(
            (pl.col("era_bucket") == row["era_bucket"])
            & (pl.col("class_label") == FOCAL)
        )
        by_delta = dict(
            zip(
                era_ribbon.get_column("delta_logodds").to_list(),
                era_ribbon.get_column("marginal_share").to_list(),
                strict=True,
            )
        )
        assert row["baseline_share"] == pytest.approx(by_delta[0.0])
        assert row["grid_low_share"] == pytest.approx(by_delta[min(DEFAULT_GRID)])
        assert row["grid_high_share"] == pytest.approx(by_delta[max(DEFAULT_GRID)])
        assert row["grid_low_delta"] == min(DEFAULT_GRID)
        assert row["grid_high_delta"] == max(DEFAULT_GRID)
        assert row["target_share"] == pytest.approx(targets[row["era_bucket"]])
        assert row["delta_at_target"] is not None
        assert row["target_in_grid"] == (
            min(DEFAULT_GRID) <= row["delta_at_target"] <= max(DEFAULT_GRID)
        )

    pre_row = table.filter(pl.col("era_bucket") == "pre-1950").row(0, named=True)
    assert pre_row["delta_at_target"] > max(DEFAULT_GRID)
    assert pre_row["target_in_grid"] is False
    modern_row = table.filter(pl.col("era_bucket") == "1988+").row(0, named=True)
    assert modern_row["delta_at_target"] < 0.0
    assert modern_row["target_in_grid"] is True


def test_bound_table_skips_eras_without_target_and_records_unreachable(
    export: pl.DataFrame,
) -> None:
    table = bound_offset_table(export, class_label=FOCAL, target_by_era={"1988+": 1.0})
    assert table.height == 1
    row = table.row(0, named=True)
    assert row["era_bucket"] == "1988+"
    assert row["delta_at_target"] is None
    assert row["target_in_grid"] is False


def test_era_ribbon_zero_row_is_mar_identity(export: pl.DataFrame) -> None:
    ribbon = era_sensitivity_ribbon(export, dimension="trajectory")
    for era in ("pre-1950", "1988+"):
        mar = marginal_shares(_era(export, era), offset={})
        zero = ribbon.filter(
            (pl.col("era_bucket") == era) & (pl.col("delta_logodds") == 0.0)
        )
        assert zero.height == len(CLASSES)
        for row in zero.iter_rows(named=True):
            assert row["marginal_share"] == pytest.approx(mar[row["class_label"]])
            assert row["marginal_share"] == pytest.approx(row["baseline_share"])
        plain = sensitivity_ribbon(_era(export, era), dimension="trajectory")
        assert (
            zero.select("class_label", "marginal_share")
            .sort("class_label")
            .equals(
                plain.filter(pl.col("delta_logodds") == 0.0)
                .select("class_label", "marginal_share")
                .sort("class_label")
            )
        )


def test_tolerance_is_tight() -> None:
    assert 0.0 < OFFSET_SEARCH_TOLERANCE_NATS < 1e-6
    assert math.isfinite(OFFSET_SEARCH_TOLERANCE_NATS)
