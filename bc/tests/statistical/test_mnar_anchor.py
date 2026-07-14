"""Anchored MNAR offset estimator on synthetic frames with known selection."""

from __future__ import annotations

import math
from typing import Any

import polars as pl
from polars.testing import assert_frame_equal

from python_models.statistical.mnar_anchor import (
    PARTIAL_TRUTH_ASSUMPTION,
    anchor_offsets,
    compute_anchor,
    decade_bucket,
    paper_era_bucket,
)


def _rows(season: int, status: str, value: str, n: int) -> pl.DataFrame:
    raw = value if status == "observed" else "Unknown"
    deduced = value if status == "derived" else None
    return pl.DataFrame(
        {
            "season": [season] * n,
            "observed_status": [status] * n,
            "raw_value": [raw] * n,
            "deduced_value": [deduced] * n,
        },
        schema={
            "season": pl.Int64,
            "observed_status": pl.Utf8,
            "raw_value": pl.Utf8,
            "deduced_value": pl.Utf8,
        },
    )


def _frame(*specs: tuple[int, str, str, int]) -> pl.DataFrame:
    return pl.concat([_rows(*spec) for spec in specs])


def _offset_row(offsets: pl.DataFrame, era: str, cls: str) -> dict[str, Any]:
    match = offsets.filter(
        (pl.col("era_bucket") == era) & (pl.col("class_label") == cls)
    )
    assert match.height == 1, f"expected one row for ({era}, {cls}), got {match.height}"
    return match.to_dicts()[0]


def test_delta_recovered_exactly_for_covered_classes() -> None:
    frame = _frame(
        (2000, "observed", "A", 60),
        (2000, "observed", "B", 30),
        (2000, "observed", "C", 10),
        (2000, "derived", "A", 20),
        (2000, "derived", "B", 50),
        (2000, "derived", "C", 30),
    )
    offsets = anchor_offsets(frame, era_bucketing=paper_era_bucket)

    p_obs = {"A": 0.6, "B": 0.3, "C": 0.1}
    p_masked = {"A": 0.2, "B": 0.5, "C": 0.3}
    raw = {c: math.log(p_masked[c] / p_obs[c]) for c in p_obs}
    center = sum(raw.values()) / len(raw)
    expected = {c: raw[c] - center for c in raw}

    for cls in ("A", "B", "C"):
        row = _offset_row(offsets, "1988+", cls)
        assert row["covered"] is True
        assert math.isclose(float(row["p_obs"]), p_obs[cls], abs_tol=1e-12)
        assert math.isclose(float(row["p_masked"]), p_masked[cls], abs_tol=1e-12)
        assert math.isclose(float(row["delta_raw"]), raw[cls], abs_tol=1e-12)
        assert math.isclose(float(row["delta"]), expected[cls], abs_tol=1e-12)


def test_centering_zero_mean_per_era_over_covered_classes() -> None:
    frame = _frame(
        (1930, "observed", "A", 40),
        (1930, "observed", "B", 60),
        (1930, "derived", "A", 70),
        (1930, "derived", "B", 30),
        (2010, "observed", "A", 25),
        (2010, "observed", "B", 75),
        (2010, "derived", "A", 55),
        (2010, "derived", "B", 45),
    )
    offsets = anchor_offsets(frame, era_bucketing=paper_era_bucket)

    for era in ("pre-1950", "1988+"):
        covered = offsets.filter((pl.col("era_bucket") == era) & pl.col("covered"))
        assert covered.height == 2
        mean_delta = covered.select(pl.col("delta").mean()).item()
        assert math.isclose(float(mean_delta), 0.0, abs_tol=1e-12)


def test_uncovered_class_is_nan_not_inf() -> None:
    frame = _frame(
        (2000, "observed", "GroundBall", 40),
        (2000, "observed", "Fly", 60),
        (2000, "derived", "GroundBall", 100),
    )
    offsets = anchor_offsets(frame, era_bucketing=paper_era_bucket)

    ground = _offset_row(offsets, "1988+", "GroundBall")
    assert ground["covered"] is True
    assert math.isfinite(float(ground["delta_raw"]))
    assert math.isclose(float(ground["delta"]), 0.0, abs_tol=1e-12)

    fly = _offset_row(offsets, "1988+", "Fly")
    assert fly["covered"] is False
    assert math.isclose(float(fly["p_masked"]), 0.0, abs_tol=1e-12)
    for key in ("delta_raw", "delta"):
        value = float(fly[key])
        assert math.isnan(value)
        assert not math.isinf(value)


def test_partial_truth_degenerate_single_covered_class_centers_to_zero() -> None:
    frame = _frame(
        (1930, "observed", "GroundBall", 30),
        (1930, "observed", "Fly", 40),
        (1930, "observed", "LineDrive", 30),
        (1930, "derived", "GroundBall", 500),
    )
    result = compute_anchor(frame, dimension="trajectory", era_bucketing_name="paper")
    ground = _offset_row(result.offsets, "pre-1950", "GroundBall")
    assert ground["covered"] is True
    assert math.isclose(float(ground["delta"]), 0.0, abs_tol=1e-12)
    assert math.isclose(
        float(ground["delta_raw"]), math.log(1.0 / (30 / 100)), abs_tol=1e-12
    )
    assert result.summary.slice_row_counts["pre-1950"] == {
        "observed": 100,
        "derived": 500,
    }
    assert result.summary.partial_truth_assumption == PARTIAL_TRUTH_ASSUMPTION
    assert result.summary.n_observed == 100
    assert result.summary.n_derived == 500


def test_paper_buckets_partition_all_seasons() -> None:
    seasons = list(range(1871, 2026))
    labels = {s: paper_era_bucket(s) for s in seasons}
    assert set(labels.values()) == {"pre-1950", "1950-1987", "1988+"}
    assert paper_era_bucket(1949) == "pre-1950"
    assert paper_era_bucket(1950) == "1950-1987"
    assert paper_era_bucket(1987) == "1950-1987"
    assert paper_era_bucket(1988) == "1988+"
    for season in seasons:
        assert isinstance(labels[season], str) and labels[season]


def test_decade_buckets_partition_all_seasons() -> None:
    seasons = list(range(1871, 2026))
    labels = {s: decade_bucket(s) for s in seasons}
    assert decade_bucket(1871) == "1870s"
    assert decade_bucket(1879) == "1870s"
    assert decade_bucket(1880) == "1880s"
    assert decade_bucket(2020) == "2020s"
    assert decade_bucket(2025) == "2020s"
    starts = sorted({int(label[:-1]) for label in labels.values()})
    assert starts == list(range(1870, 2021, 10))
    for start in starts:
        for offset in range(10):
            season = start + offset
            if 1871 <= season <= 2025:
                assert decade_bucket(season) == f"{start}s"


def test_determinism() -> None:
    frame = _frame(
        (1930, "observed", "A", 33),
        (1930, "observed", "B", 67),
        (1930, "derived", "A", 80),
        (1930, "derived", "B", 20),
        (2005, "observed", "A", 50),
        (2005, "observed", "B", 50),
        (2005, "derived", "A", 10),
        (2005, "derived", "B", 90),
    )
    first = anchor_offsets(frame, era_bucketing=decade_bucket)
    second = anchor_offsets(frame, era_bucketing=decade_bucket)
    assert_frame_equal(first, second)


def test_empty_frame_returns_typed_empty() -> None:
    empty = pl.DataFrame(
        schema={
            "season": pl.Int64,
            "observed_status": pl.Utf8,
            "raw_value": pl.Utf8,
            "deduced_value": pl.Utf8,
        }
    )
    offsets = anchor_offsets(empty)
    assert offsets.height == 0
    assert offsets.columns[0] == "era_bucket"
