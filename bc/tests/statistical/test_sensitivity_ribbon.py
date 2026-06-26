"""Tests for the MNAR pattern-mixture sensitivity ribbon.

Invariants only — `delta = 0` reproduces the published marginal, every grid
point is a valid distribution, a class's share moves monotonically with its own
offset, and the band brackets the baseline. One closed-form reweight pins the
math.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import math

import polars as pl
import pytest

from python_models.statistical.sensitivity import (
    DEFAULT_GRID,
    marginal_shares,
    offset_recovers_target,
    ribbon_band,
    sensitivity_ribbon,
)


def _export(per_event: dict[int, dict[str, float]]) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for event_key, shares in per_event.items():
        for class_index, (label, share) in enumerate(sorted(shares.items())):
            rows.append(
                {
                    "event_key": event_key,
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
            1: {"A": 0.6, "B": 0.3, "C": 0.1},
            2: {"A": 0.2, "B": 0.5, "C": 0.3},
            3: {"A": 0.1, "B": 0.1, "C": 0.8},
        }
    )


def test_zero_offset_is_identity(export: pl.DataFrame) -> None:
    marg = marginal_shares(export, offset={})
    expected = {
        "A": (0.6 + 0.2 + 0.1) / 3,
        "B": (0.3 + 0.5 + 0.1) / 3,
        "C": (0.1 + 0.3 + 0.8) / 3,
    }
    for label, value in expected.items():
        assert marg[label] == pytest.approx(value)


def test_marginal_is_a_distribution_under_offset(export: pl.DataFrame) -> None:
    marg = marginal_shares(export, offset={"A": 0.7, "C": -0.4})
    assert sum(marg.values()) == pytest.approx(1.0)
    assert all(0.0 <= v <= 1.0 for v in marg.values())


def test_closed_form_single_offset(export: pl.DataFrame) -> None:
    w = math.exp(0.5)
    per_event_a = []
    for shares in (
        {"A": 0.6, "B": 0.3, "C": 0.1},
        {"A": 0.2, "B": 0.5, "C": 0.3},
        {"A": 0.1, "B": 0.1, "C": 0.8},
    ):
        denom = shares["A"] * w + shares["B"] + shares["C"]
        per_event_a.append(shares["A"] * w / denom)
    marg = marginal_shares(export, offset={"A": 0.5})
    assert marg["A"] == pytest.approx(sum(per_event_a) / 3)


def test_ribbon_zero_row_matches_baseline(export: pl.DataFrame) -> None:
    ribbon = sensitivity_ribbon(export, dimension="trajectory")
    zero = ribbon.filter(pl.col("delta_logodds") == 0.0)
    assert zero.height == export["class_label"].n_unique()
    for row in zero.iter_rows(named=True):
        assert row["marginal_share"] == pytest.approx(row["baseline_share"])


def test_class_share_monotonic_in_own_offset(export: pl.DataFrame) -> None:
    ribbon = sensitivity_ribbon(export, dimension="trajectory")
    for label in export["class_label"].unique():
        own = (
            ribbon.filter(pl.col("class_label") == label)
            .sort("delta_logodds")
            .get_column("marginal_share")
            .to_list()
        )
        assert own == sorted(own)


def test_band_brackets_baseline(export: pl.DataFrame) -> None:
    band = ribbon_band(sensitivity_ribbon(export, dimension="trajectory"))
    for row in band.iter_rows(named=True):
        assert row["share_low"] <= row["baseline_share"] <= row["share_high"]


def test_offset_recovers_self_target(export: pl.DataFrame) -> None:
    offset = {"A": 0.6, "B": -0.2}
    target = marginal_shares(export, offset=offset)
    assert offset_recovers_target(export, offset=offset, target=target) == pytest.approx(
        0.0, abs=1e-9
    )
    assert offset_recovers_target(
        export, offset={}, target=marginal_shares(export, offset={})
    ) == pytest.approx(0.0, abs=1e-9)


def test_default_grid_is_symmetric_and_zero_centered() -> None:
    assert 0.0 in DEFAULT_GRID
    assert sorted(DEFAULT_GRID) == list(DEFAULT_GRID)
    assert sorted(-g for g in DEFAULT_GRID) == list(DEFAULT_GRID)
