"""Tests for the joint anchored MNAR sensitivity ribbon.

Invariants only, no DB, no monkeypatching. `t = 0` reproduces the MAR marginal;
the focal (anchored) class share is monotone in `t`; the ±perturbation rows
bracket the unperturbed `t = 1` mix; the ribbon is invariant to adding a
constant to the whole offset vector (softmax identifiability); and the whole
thing is deterministic.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import polars as pl
import pytest

from python_models.statistical.sensitivity import (
    JOINT_ANCHOR_T,
    JOINT_T_GRID,
    SWEEP_JOINT_ANCHOR,
    SWEEP_JOINT_ANCHOR_PERTURBED,
    SWEEP_MARGINAL,
    joint_ribbon_band,
    joint_sensitivity_ribbon,
    marginal_shares,
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


def _shares(gb: float, other: float) -> dict[str, float]:
    rest = (1.0 - gb) / 4.0
    out = {c: rest for c in CLASSES}
    out[FOCAL] = gb
    out["Fly"] = other
    out["LineDrive"] = 1.0 - gb - other - out["Bunt"] - out["PopUp"]
    return out


@pytest.fixture
def export() -> pl.DataFrame:
    return _export(
        {
            1: ("pre-1950", {"Bunt": 0.05, "Fly": 0.30, "GroundBall": 0.25, "LineDrive": 0.20, "PopUp": 0.20}),
            2: ("pre-1950", {"Bunt": 0.10, "Fly": 0.25, "GroundBall": 0.30, "LineDrive": 0.25, "PopUp": 0.10}),
            3: ("pre-1950", {"Bunt": 0.02, "Fly": 0.40, "GroundBall": 0.18, "LineDrive": 0.30, "PopUp": 0.10}),
            4: ("1988+", {"Bunt": 0.08, "Fly": 0.28, "GroundBall": 0.40, "LineDrive": 0.14, "PopUp": 0.10}),
            5: ("1988+", {"Bunt": 0.06, "Fly": 0.22, "GroundBall": 0.44, "LineDrive": 0.18, "PopUp": 0.10}),
        }
    )


@pytest.fixture
def anchor() -> dict[str, dict[str, float]]:
    return {"pre-1950": {FOCAL: 1.24}, "1988+": {FOCAL: 0.83}}


def _ribbon(export: pl.DataFrame, anchor: dict[str, dict[str, float]]) -> pl.DataFrame:
    return joint_sensitivity_ribbon(
        export, dimension="trajectory", anchor_offset_by_era=anchor, focal_class=FOCAL
    )


def test_t0_reproduces_mar_marginal(export: pl.DataFrame, anchor: dict[str, dict[str, float]]) -> None:
    ribbon = _ribbon(export, anchor)
    for era in ("pre-1950", "1988+"):
        era_df = export.filter(pl.col("era_bucket") == era)
        mar = marginal_shares(era_df, offset={})
        t0 = ribbon.filter(
            (pl.col("era_bucket") == era)
            & (pl.col("sweep_kind") == SWEEP_JOINT_ANCHOR)
            & (pl.col("t") == 0.0)
        )
        assert t0.height == len(CLASSES)
        for row in t0.iter_rows(named=True):
            assert row["marginal_share"] == pytest.approx(mar[row["class_label"]])
            assert row["marginal_share"] == pytest.approx(row["baseline_share"])


def test_marginal_rows_preserved(export: pl.DataFrame, anchor: dict[str, dict[str, float]]) -> None:
    ribbon = _ribbon(export, anchor)
    marg = ribbon.filter(pl.col("sweep_kind") == SWEEP_MARGINAL)
    assert marg.height > 0
    assert marg.filter(pl.col("t").is_not_null()).height == 0
    assert marg.filter(pl.col("delta_logodds").is_null()).height == 0
    zero = marg.filter(pl.col("delta_logodds") == 0.0)
    for row in zero.iter_rows(named=True):
        assert row["marginal_share"] == pytest.approx(row["baseline_share"])


def test_focal_share_monotone_in_t(export: pl.DataFrame, anchor: dict[str, dict[str, float]]) -> None:
    ribbon = _ribbon(export, anchor)
    for era in ("pre-1950", "1988+"):
        focal = (
            ribbon.filter(
                (pl.col("era_bucket") == era)
                & (pl.col("sweep_kind") == SWEEP_JOINT_ANCHOR)
                & (pl.col("class_label") == FOCAL)
            )
            .sort("t")
            .get_column("marginal_share")
            .to_list()
        )
        assert focal == sorted(focal)
        assert focal[0] < focal[-1]


def test_perturbations_bracket_unperturbed_t1(export: pl.DataFrame, anchor: dict[str, dict[str, float]]) -> None:
    ribbon = _ribbon(export, anchor)
    band = joint_ribbon_band(ribbon)
    assert band.height == 2 * len(CLASSES)
    for row in band.iter_rows(named=True):
        assert row["share_low"] <= row["anchor_share"] <= row["share_high"]
        assert row["share_low"] < row["share_high"]


def test_perturbed_rows_only_touch_non_focal(export: pl.DataFrame, anchor: dict[str, dict[str, float]]) -> None:
    ribbon = _ribbon(export, anchor)
    perturbed = ribbon.filter(pl.col("sweep_kind") == SWEEP_JOINT_ANCHOR_PERTURBED)
    assert FOCAL not in perturbed.get_column("perturbed_class").unique().to_list()
    assert set(perturbed.get_column("perturbation_nats").unique().to_list()) == {0.25, -0.25}
    assert perturbed.filter(pl.col("t") != JOINT_ANCHOR_T).height == 0


def test_identifiability_constant_shift_is_invariant(
    export: pl.DataFrame, anchor: dict[str, dict[str, float]]
) -> None:
    shifted = {
        era: {c: offsets.get(c, 0.0) + 0.9 for c in CLASSES}
        for era, offsets in anchor.items()
    }
    base = _ribbon(export, anchor).sort("era_bucket", "sweep_kind", "class_label", "t", "perturbation_nats")
    other = _ribbon(export, shifted).sort("era_bucket", "sweep_kind", "class_label", "t", "perturbation_nats")
    assert base.get_column("marginal_share").to_list() == pytest.approx(
        other.get_column("marginal_share").to_list()
    )


def test_marginal_shares_constant_shift_invariance(export: pl.DataFrame) -> None:
    era_df = export.filter(pl.col("era_bucket") == "pre-1950")
    single = marginal_shares(era_df, offset={FOCAL: 1.24})
    shifted = marginal_shares(era_df, offset={c: (1.24 if c == FOCAL else 0.0) + 0.5 for c in CLASSES})
    for c in CLASSES:
        assert single[c] == pytest.approx(shifted[c])


def test_deterministic(export: pl.DataFrame, anchor: dict[str, dict[str, float]]) -> None:
    first = _ribbon(export, anchor)
    second = _ribbon(export, anchor)
    assert first.equals(second)


def test_t_grid_spans_anchor_and_beyond() -> None:
    assert JOINT_T_GRID[0] == 0.0
    assert JOINT_ANCHOR_T in JOINT_T_GRID
    assert max(JOINT_T_GRID) > JOINT_ANCHOR_T
    assert list(JOINT_T_GRID) == sorted(JOINT_T_GRID)
