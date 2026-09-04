"""Derived-slice bounds on synthetic slice frames with a hand-built MAR export."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from python_models.statistical import config as cfg
from python_models.statistical.manifests import write_published_pointer
from python_models.statistical.mnar_anchor import (
    IDENTIFICATION_STATEMENT,
    LOG_LOSS_CLIP,
    compute_bounds,
    decade_bucket,
    derived_slice_bounds,
    paper_era_bucket,
    resolve_published_geometry_inputs,
)
from python_models.statistical.schemas import PublishedPointer

CLASSES = ("Fly", "GroundBall", "LineDrive")

_SLICE_SCHEMA = {
    "event_key": pl.Int64,
    "season": pl.Int64,
    "observed_status": pl.Utf8,
    "raw_value": pl.Utf8,
    "deduced_value": pl.Utf8,
}


def _slice_frame(
    rows: list[tuple[int, int, str, str | None, str | None]],
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "event_key": [r[0] for r in rows],
            "season": [r[1] for r in rows],
            "observed_status": [r[2] for r in rows],
            "raw_value": [r[3] for r in rows],
            "deduced_value": [r[4] for r in rows],
        },
        schema=_SLICE_SCHEMA,
    )


def _export(shares: dict[int, dict[str, float]]) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for event_key, per_class in shares.items():
        for label, share in per_class.items():
            rows.append(
                {"event_key": event_key, "class_label": label, "expected_share": share}
            )
    return pl.DataFrame(rows)


def _row(bounds: pl.DataFrame, era: str, cls: str) -> dict[str, Any]:
    match = bounds.filter(
        (pl.col("era_bucket") == era) & (pl.col("class_label") == cls)
    )
    assert match.height == 1, f"expected one row for ({era}, {cls}), got {match.height}"
    return match.to_dicts()[0]


@pytest.fixture
def synthetic() -> tuple[pl.DataFrame, pl.DataFrame]:
    slice_frame = _slice_frame(
        [
            (1, 1930, "observed", "GroundBall", None),
            (2, 1930, "observed", "Fly", None),
            (3, 1930, "observed", "Fly", None),
            (4, 1930, "observed", "LineDrive", None),
            (10, 1930, "derived", None, "GroundBall"),
            (11, 1930, "derived", None, "GroundBall"),
            (12, 1930, "unknown_code", None, None),
            (13, 1930, "unknown_code", None, None),
            (14, 1930, "missing", None, None),
            (20, 2000, "observed", "GroundBall", None),
            (21, 2000, "observed", "GroundBall", None),
            (30, 2000, "derived", None, "GroundBall"),
            (31, 2000, "unknown_code", None, None),
        ]
    )
    export = _export(
        {
            10: {"Fly": 0.5, "GroundBall": 0.3, "LineDrive": 0.2},
            11: {"Fly": 0.2, "GroundBall": 0.7, "LineDrive": 0.1},
            12: {"Fly": 0.4, "GroundBall": 0.4, "LineDrive": 0.2},
            13: {"Fly": 0.1, "GroundBall": 0.6, "LineDrive": 0.3},
            14: {"Fly": 0.3, "GroundBall": 0.2, "LineDrive": 0.5},
            30: {"Fly": 0.5, "GroundBall": 0.25, "LineDrive": 0.25},
            31: {"Fly": 0.2, "GroundBall": 0.5, "LineDrive": 0.3},
        }
    )
    return slice_frame, export


def test_bound_and_point_estimate_by_hand(
    synthetic: tuple[pl.DataFrame, pl.DataFrame],
) -> None:
    slice_frame, export = synthetic
    bounds = derived_slice_bounds(slice_frame, export, era_bucketing=paper_era_bucket)

    gb = _row(bounds, "pre-1950", "GroundBall")
    n_unrecorded = 5
    n_derived = 2
    unknown_mass = 0.4 + 0.6 + 0.2
    assert gb["n_unrecorded"] == n_unrecorded
    assert gb["n_derived"] == n_derived
    assert gb["n_unknown"] == 3
    assert gb["n_derived_class"] == n_derived
    assert gb["covered"] is True
    assert math.isclose(gb["share_lower_bound"], n_derived / n_unrecorded)
    assert math.isclose(
        gb["share_point_estimate"], (n_derived + unknown_mass) / n_unrecorded
    )
    assert math.isclose(gb["mar_share_unknown"], unknown_mass / 3)
    assert math.isclose(
        gb["mar_share_unrecorded"], (0.3 + 0.7 + unknown_mass) / n_unrecorded
    )
    assert math.isclose(gb["mar_share_on_derived"], (0.3 + 0.7) / 2)
    assert math.isclose(
        gb["mar_log_loss_on_derived"], (-math.log(0.3) - math.log(0.7)) / 2
    )
    assert gb["n_observed"] == 4
    assert math.isclose(gb["observed_share"], 1 / 4)

    fly = _row(bounds, "pre-1950", "Fly")
    assert fly["covered"] is False
    assert fly["n_derived_class"] == 0
    assert math.isclose(fly["share_lower_bound"], 0.0)
    assert math.isclose(fly["share_point_estimate"], (0.4 + 0.1 + 0.3) / n_unrecorded)
    assert fly["mar_share_on_derived"] is None
    assert fly["mar_log_loss_on_derived"] is None
    assert math.isclose(fly["observed_share"], 2 / 4)

    modern = _row(bounds, "1988+", "GroundBall")
    assert modern["n_unrecorded"] == 2
    assert math.isclose(modern["share_lower_bound"], 1 / 2)
    assert math.isclose(modern["share_point_estimate"], (1 + 0.5) / 2)
    assert math.isclose(modern["observed_share"], 1.0)


def test_bound_below_point_below_one_and_points_sum_to_one(
    synthetic: tuple[pl.DataFrame, pl.DataFrame],
) -> None:
    slice_frame, export = synthetic
    bounds = derived_slice_bounds(slice_frame, export, era_bucketing=paper_era_bucket)
    for row in bounds.iter_rows(named=True):
        assert 0.0 <= row["share_lower_bound"] <= row["share_point_estimate"] <= 1.0
    for era in bounds.get_column("era_bucket").unique().to_list():
        era_rows = bounds.filter(pl.col("era_bucket") == era)
        assert era_rows.get_column("share_point_estimate").sum() == pytest.approx(1.0)
        assert era_rows.get_column("mar_share_unrecorded").sum() == pytest.approx(1.0)
        n_derived = era_rows.get_column("n_derived").first()
        n_unrecorded = era_rows.get_column("n_unrecorded").first()
        assert isinstance(n_derived, int) and isinstance(n_unrecorded, int)
        assert era_rows.get_column("share_lower_bound").sum() == pytest.approx(
            n_derived / n_unrecorded
        )


def test_derived_rows_count_as_truth_and_unknown_rows_as_mar_share() -> None:
    slice_frame = _slice_frame(
        [
            (1, 1960, "derived", None, "GroundBall"),
            (2, 1960, "derived", None, "GroundBall"),
            (3, 1960, "unknown_code", None, None),
        ]
    )
    low_mar = _export(
        {
            1: {"Fly": 0.95, "GroundBall": 0.05},
            2: {"Fly": 0.95, "GroundBall": 0.05},
            3: {"Fly": 0.9, "GroundBall": 0.1},
        }
    )
    high_mar = _export(
        {
            1: {"Fly": 0.05, "GroundBall": 0.95},
            2: {"Fly": 0.05, "GroundBall": 0.95},
            3: {"Fly": 0.9, "GroundBall": 0.1},
        }
    )
    low = _row(derived_slice_bounds(slice_frame, low_mar), "1950-1987", "GroundBall")
    high = _row(derived_slice_bounds(slice_frame, high_mar), "1950-1987", "GroundBall")
    assert low["share_point_estimate"] == pytest.approx((2 + 0.1) / 3)
    assert high["share_point_estimate"] == low["share_point_estimate"]
    assert low["share_lower_bound"] == high["share_lower_bound"] == pytest.approx(2 / 3)
    assert low["mar_share_on_derived"] == pytest.approx(0.05)
    assert high["mar_share_on_derived"] == pytest.approx(0.95)
    assert low["mar_log_loss_on_derived"] > high["mar_log_loss_on_derived"]

    unknown_shifted = _export(
        {
            1: {"Fly": 0.95, "GroundBall": 0.05},
            2: {"Fly": 0.95, "GroundBall": 0.05},
            3: {"Fly": 0.3, "GroundBall": 0.7},
        }
    )
    shifted = _row(
        derived_slice_bounds(slice_frame, unknown_shifted), "1950-1987", "GroundBall"
    )
    assert shifted["share_point_estimate"] == pytest.approx((2 + 0.7) / 3)


def test_class_remap_aligns_labels_to_export_vocabulary() -> None:
    slice_frame = _slice_frame(
        [
            (1, 2000, "observed", "GroundBallBunt", None),
            (2, 2000, "observed", "GroundBall", None),
            (3, 2000, "observed", "FoulBunt", None),
            (4, 2000, "derived", None, "GroundBall"),
        ]
    )
    export = _export({4: {"Bunt": 0.5, "GroundBall": 0.5}})
    bounds = derived_slice_bounds(
        slice_frame, export, class_remap={"GroundBallBunt": "Bunt"}
    )
    bunt = _row(bounds, "1988+", "Bunt")
    assert bunt["n_observed"] == 2
    assert bunt["observed_share"] == pytest.approx(0.5)
    assert _row(bounds, "1988+", "GroundBall")["observed_share"] == pytest.approx(0.5)


def test_log_loss_is_finite_at_zero_share() -> None:
    slice_frame = _slice_frame([(1, 2000, "derived", None, "GroundBall")])
    export = _export({1: {"Fly": 1.0, "GroundBall": 0.0}})
    row = _row(derived_slice_bounds(slice_frame, export), "1988+", "GroundBall")
    assert math.isfinite(row["mar_log_loss_on_derived"])
    assert row["mar_log_loss_on_derived"] == pytest.approx(-math.log(LOG_LOSS_CLIP))


def test_unscored_unrecorded_event_raises() -> None:
    slice_frame = _slice_frame(
        [
            (1, 2000, "derived", None, "GroundBall"),
            (2, 2000, "unknown_code", None, None),
        ]
    )
    export = _export({1: {"Fly": 0.5, "GroundBall": 0.5}})
    with pytest.raises(ValueError, match="no row in the MAR export"):
        derived_slice_bounds(slice_frame, export)


def test_export_missing_a_class_for_an_event_raises() -> None:
    slice_frame = _slice_frame(
        [
            (1, 2000, "derived", None, "GroundBall"),
            (2, 2000, "unknown_code", None, None),
        ]
    )
    export = _export({1: {"Fly": 0.5, "GroundBall": 0.5}, 2: {"GroundBall": 1.0}})
    with pytest.raises(ValueError, match="every class for every event"):
        derived_slice_bounds(slice_frame, export)


def test_derived_class_outside_vocabulary_raises() -> None:
    slice_frame = _slice_frame([(1, 2000, "derived", None, "Rocket")])
    export = _export({1: {"Fly": 0.5, "GroundBall": 0.5}})
    with pytest.raises(ValueError, match="outside the export vocabulary"):
        derived_slice_bounds(slice_frame, export)


def test_compute_bounds_summary(synthetic: tuple[pl.DataFrame, pl.DataFrame]) -> None:
    slice_frame, export = synthetic
    result = compute_bounds(
        slice_frame, export, dimension="trajectory", era_bucketing_name="paper"
    )
    assert result.summary.identification_statement == IDENTIFICATION_STATEMENT
    assert result.summary.n_observed == 6
    assert result.summary.n_unrecorded == 7
    assert result.summary.n_derived == 3
    assert result.summary.n_unknown == 4
    assert result.summary.buckets == ["1988+", "pre-1950"]
    assert result.summary.classes == list(CLASSES)
    assert result.summary.slice_row_counts["pre-1950"] == {
        "observed": 4,
        "derived": 2,
        "unknown_code": 2,
        "missing": 1,
    }
    assert result.bounds.height == 2 * len(CLASSES)


def test_paper_buckets_partition_all_seasons() -> None:
    seasons = list(range(1871, 2026))
    labels = {s: paper_era_bucket(s) for s in seasons}
    assert set(labels.values()) == {"pre-1950", "1950-1987", "1988+"}
    assert paper_era_bucket(1949) == "pre-1950"
    assert paper_era_bucket(1950) == "1950-1987"
    assert paper_era_bucket(1987) == "1950-1987"
    assert paper_era_bucket(1988) == "1988+"


def test_decade_buckets_partition_all_seasons() -> None:
    seasons = list(range(1871, 2026))
    labels = {s: decade_bucket(s) for s in seasons}
    starts = sorted({int(label[:-1]) for label in labels.values()})
    assert starts == list(range(1870, 2021, 10))
    for season in seasons:
        assert decade_bucket(season) == f"{(season // 10) * 10}s"


def test_determinism(synthetic: tuple[pl.DataFrame, pl.DataFrame]) -> None:
    slice_frame, export = synthetic
    first = derived_slice_bounds(slice_frame, export, era_bucketing=decade_bucket)
    second = derived_slice_bounds(slice_frame, export, era_bucketing=decade_bucket)
    assert_frame_equal(first, second)


def test_empty_frame_returns_typed_empty() -> None:
    empty = pl.DataFrame(schema=_SLICE_SCHEMA)
    export = _export({1: {"Fly": 0.5, "GroundBall": 0.5}})
    bounds = derived_slice_bounds(empty, export)
    assert bounds.height == 0
    assert bounds.columns[0] == "era_bucket"


def test_era_with_only_observed_rows_emits_zero_counts_and_a_zero_floor() -> None:
    slice_frame = _slice_frame(
        [
            (1, 1930, "observed", "GroundBall", None),
            (2, 1930, "derived", None, "GroundBall"),
            (3, 1930, "unknown_code", None, None),
            (20, 2000, "observed", "Fly", None),
            (21, 2000, "observed", "GroundBall", None),
        ]
    )
    export = _export(
        {
            2: {"Fly": 0.2, "GroundBall": 0.7, "LineDrive": 0.1},
            3: {"Fly": 0.4, "GroundBall": 0.4, "LineDrive": 0.2},
        }
    )
    bounds = derived_slice_bounds(slice_frame, export, era_bucketing=paper_era_bucket)

    modern = bounds.filter(pl.col("era_bucket") == "1988+")
    assert modern.height == len(CLASSES)
    assert set(modern.get_column("class_label").to_list()) == set(CLASSES)
    for column in ("n_unrecorded", "n_derived", "n_unknown", "n_derived_class"):
        assert modern.get_column(column).to_list() == [0] * modern.height
    assert modern.get_column("covered").to_list() == [False] * modern.height
    assert modern.get_column("share_lower_bound").null_count() == 0
    assert modern.get_column("share_lower_bound").to_list() == [0.0] * modern.height
    assert modern.get_column("share_point_estimate").null_count() == modern.height
    assert modern.get_column("n_observed").to_list() == [2] * modern.height
    assert _row(bounds, "1988+", "Fly")["observed_share"] == pytest.approx(0.5)
    assert _row(bounds, "1988+", "GroundBall")["observed_share"] == pytest.approx(0.5)

    early = bounds.filter(pl.col("era_bucket") == "pre-1950")
    assert early.get_column("share_point_estimate").null_count() == 0
    assert early.get_column("share_point_estimate").sum() == pytest.approx(1.0)
    assert all(float(v) >= 0.0 for v in bounds.get_column("share_lower_bound"))


def _published_geometry_fit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    with_dataset: bool,
    with_export: bool,
    relative_pointer: bool,
) -> tuple[Path, Path]:
    artifacts_root = tmp_path / "canonical"
    artifact_dir = artifacts_root / "bayes" / "geometry_trajectory" / "fit-1"
    artifact_dir.mkdir(parents=True)
    manifest_path = artifact_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps({"artifact_id": "fit-1", "dataset_artifact_id": "ds-1"}),
        encoding="utf-8",
    )
    datasets_root = artifacts_root / "datasets"
    dataset_path = datasets_root / "model_input_geometry" / "ds-1" / "dataset.parquet"
    export_path = artifact_dir / "exports" / "geometry_probabilities.parquet"
    if with_dataset:
        dataset_path.parent.mkdir(parents=True)
        pl.DataFrame({"event_key": [1]}).write_parquet(dataset_path)
    if with_export:
        export_path.parent.mkdir(parents=True)
        pl.DataFrame({"event_key": [1]}).write_parquet(export_path)
    monkeypatch.setenv(cfg.ENV_ARTIFACTS_ROOT, str(artifacts_root))
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(tmp_path / "published"))
    monkeypatch.setattr(cfg, "DATASETS_ROOT", datasets_root)
    pointer = PublishedPointer(
        model_name="geometry_trajectory",
        artifact_id="fit-1",
        published_at=datetime.now(tz=timezone.utc),
        manifest_path=manifest_path,
    )
    _ = write_published_pointer(
        pointer, root=tmp_path / "published", relative=relative_pointer
    )
    return dataset_path, export_path


def test_resolver_follows_a_relative_pointer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset_path, export_path = _published_geometry_fit(
        tmp_path,
        monkeypatch,
        with_dataset=True,
        with_export=True,
        relative_pointer=True,
    )
    stored = json.loads(
        (tmp_path / "published" / "geometry_trajectory.json").read_text("utf-8")
    )
    assert not Path(stored["manifest_path"]).is_absolute()

    inputs = resolve_published_geometry_inputs("geometry_trajectory")

    assert inputs.artifact_id == "fit-1"
    assert inputs.dataset_artifact_id == "ds-1"
    assert inputs.dataset_path == dataset_path
    assert inputs.export_path == export_path
    assert inputs.export_path.is_absolute()


@pytest.mark.parametrize(
    ("with_dataset", "with_export", "require_dataset", "require_export", "ok"),
    [
        (True, True, True, True, True),
        (False, True, True, True, False),
        (False, True, False, True, True),
        (True, False, True, True, False),
        (True, False, True, False, True),
        (False, False, False, False, True),
    ],
)
def test_resolver_requires_only_the_requested_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    with_dataset: bool,
    with_export: bool,
    require_dataset: bool,
    require_export: bool,
    ok: bool,
) -> None:
    dataset_path, export_path = _published_geometry_fit(
        tmp_path,
        monkeypatch,
        with_dataset=with_dataset,
        with_export=with_export,
        relative_pointer=False,
    )
    if not ok:
        with pytest.raises(FileNotFoundError):
            _ = resolve_published_geometry_inputs(
                "geometry_trajectory",
                require_dataset=require_dataset,
                require_export=require_export,
            )
        return
    inputs = resolve_published_geometry_inputs(
        "geometry_trajectory",
        require_dataset=require_dataset,
        require_export=require_export,
    )
    assert inputs.dataset_path == dataset_path
    assert inputs.export_path == export_path
    assert inputs.dataset_path.exists() == with_dataset
    assert inputs.export_path.exists() == with_export
