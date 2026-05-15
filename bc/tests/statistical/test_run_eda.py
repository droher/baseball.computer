"""End-to-end exercise of ``run_eda`` against a synthetic dataset snapshot."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest

from python_models.statistical.dataset_registry import DatasetSpec
from python_models.statistical.eda import run_eda
from python_models.statistical.manifests import read_manifest
from python_models.statistical.schemas import (
    ArtifactManifest,
    EdaReport,
)


def _spec() -> DatasetSpec:
    return DatasetSpec(
        name="model_input_test_eda",
        sqlmesh_table="model_input_test_eda",
        dataset_version="0.0.1",
        grain=("event_key", "dimension"),
        categorical_columns=(
            "dimension",
            "observed_status",
            "sentinel_type",
            "data_error_risk",
            "primary_fold",
            "league",
            "source_family",
            "scorer",
            "park_id",
        ),
        slice_columns=(
            "season",
            "league",
            "source_family",
            "sentinel_type",
            "dimension",
        ),
        target_columns=("is_observed", "observed_status", "raw_value", "deduced_value"),
    )


_DATASET_COLUMNS: tuple[str, ...] = (
    "event_key",
    "dimension",
    "is_observed",
    "observed_status",
    "sentinel_type",
    "raw_value",
    "deduced_value",
    "source_acquisition_status",
    "data_error_risk",
    "model_input_eligible",
    "season",
    "league",
    "source_family",
    "park_id",
    "scorer",
    "batter_id",
    "pitcher_id",
    "batter_hand",
    "alignment_regime",
    "result_family",
    "target_population_status",
    "training_weight",
    "primary_fold",
    "holdout_flags",
    "source_snapshot_id",
)


def _seed_parquet(
    parquet_path: Path,
    *,
    rows: int = 6000,
    inject_data_error_truth: bool = True,
    inject_dominant_scorer: bool = True,
    inject_test_only_category: bool = True,
) -> None:
    seasons = [2020, 2021, 2022]
    leagues = ["AL", "NL"]
    source_families = ["pbp", "box_score", "gamelog"]
    parks = ["PARK_A", "PARK_B", "PARK_C"]
    scorers = ["scorer_1", "scorer_2", "scorer_3"]
    sentinel_types = [
        "valid_value",
        "null",
        "unknown",
        "default",
        "zero",
        "empty_sequence",
        "not_applicable",
    ]
    dimensions = ["trajectory", "location_side", "location_depth"]
    result_families = ["single", "double", "out"]
    folds = ["TRAIN", "VALIDATE", "TEST"]

    records: list[dict[str, object]] = []
    for i in range(rows):
        season = seasons[i % len(seasons)]
        league = leagues[i % len(leagues)]
        source_family = source_families[i % len(source_families)]
        park = parks[i % len(parks)]
        scorer = scorers[i % len(scorers)]
        sentinel = sentinel_types[i % len(sentinel_types)]
        dimension = dimensions[i % len(dimensions)]
        result_family = result_families[i % len(result_families)]
        fold = folds[i % len(folds)]
        is_obs = sentinel == "valid_value"
        data_error = "none" if i % 17 != 0 else "high"
        records.append(
            {
                "event_key": i + 1,
                "dimension": dimension,
                "is_observed": is_obs,
                "observed_status": "observed" if is_obs else "unknown",
                "sentinel_type": sentinel,
                "raw_value": "v" if is_obs else None,
                "deduced_value": None,
                "source_acquisition_status": "acquired"
                if i % 31 != 0
                else "not_acquired",
                "data_error_risk": data_error,
                "model_input_eligible": is_obs,
                "season": season,
                "league": league,
                "source_family": source_family,
                "park_id": park,
                "scorer": scorer,
                "batter_id": f"b{i % 50}",
                "pitcher_id": f"p{i % 50}",
                "batter_hand": "R" if i % 2 == 0 else "L",
                "alignment_regime": "standard",
                "result_family": result_family,
                "target_population_status": "event_level",
                "training_weight": 1.0 if data_error == "none" else 0.0,
                "primary_fold": fold,
                "holdout_flags": {
                    "is_heldout_scorer": False,
                    "is_heldout_park": False,
                    "is_heldout_alignment_regime": False,
                    "is_heldout_source_acquisition_block": False,
                    "is_heldout_season_block": False,
                    "is_heldout_aggregate_total": False,
                    "is_heldout_player_group": False,
                },
                "source_snapshot_id": "snap-test",
            }
        )

    if inject_data_error_truth:
        records.append(
            {
                "event_key": 10_000_001,
                "dimension": "trajectory",
                "is_observed": True,
                "observed_status": "observed",
                "sentinel_type": "valid_value",
                "raw_value": "ed",
                "deduced_value": None,
                "source_acquisition_status": "acquired",
                "data_error_risk": "high",
                "model_input_eligible": True,
                "season": 2020,
                "league": "AL",
                "source_family": "pbp",
                "park_id": "PARK_A",
                "scorer": "scorer_1",
                "batter_id": "b0",
                "pitcher_id": "p0",
                "batter_hand": "R",
                "alignment_regime": "standard",
                "result_family": "single",
                "target_population_status": "event_level",
                "training_weight": 1.0,
                "primary_fold": "TRAIN",
                "holdout_flags": {
                    "is_heldout_scorer": False,
                    "is_heldout_park": False,
                    "is_heldout_alignment_regime": False,
                    "is_heldout_source_acquisition_block": False,
                    "is_heldout_season_block": False,
                    "is_heldout_aggregate_total": False,
                    "is_heldout_player_group": False,
                },
                "source_snapshot_id": "snap-test",
            }
        )

    if inject_dominant_scorer:
        for i in range(500):
            records.append(
                {
                    "event_key": 20_000_001 + i,
                    "dimension": "trajectory",
                    "is_observed": True,
                    "observed_status": "observed",
                    "sentinel_type": "valid_value",
                    "raw_value": "v",
                    "deduced_value": None,
                    "source_acquisition_status": "acquired",
                    "data_error_risk": "none",
                    "model_input_eligible": True,
                    "season": 2023,
                    "league": "AL",
                    "source_family": "pbp",
                    "park_id": "PARK_DOM",
                    "scorer": "scorer_dom",
                    "batter_id": f"b{i}",
                    "pitcher_id": f"p{i}",
                    "batter_hand": "R",
                    "alignment_regime": "standard",
                    "result_family": "single",
                    "target_population_status": "event_level",
                    "training_weight": 1.0,
                    "primary_fold": "TRAIN",
                    "holdout_flags": {
                        "is_heldout_scorer": False,
                        "is_heldout_park": False,
                        "is_heldout_alignment_regime": False,
                        "is_heldout_source_acquisition_block": False,
                        "is_heldout_season_block": False,
                        "is_heldout_aggregate_total": False,
                        "is_heldout_player_group": False,
                    },
                    "source_snapshot_id": "snap-test",
                }
            )

    if inject_test_only_category:
        for i in range(5):
            records.append(
                {
                    "event_key": 30_000_001 + i,
                    "dimension": "trajectory",
                    "is_observed": True,
                    "observed_status": "observed",
                    "sentinel_type": "valid_value",
                    "raw_value": "v",
                    "deduced_value": None,
                    "source_acquisition_status": "acquired",
                    "data_error_risk": "none",
                    "model_input_eligible": True,
                    "season": 2024,
                    "league": "AL",
                    "source_family": "only_in_test_source",
                    "park_id": "PARK_A",
                    "scorer": "scorer_1",
                    "batter_id": f"b{i}",
                    "pitcher_id": f"p{i}",
                    "batter_hand": "R",
                    "alignment_regime": "standard",
                    "result_family": "single",
                    "target_population_status": "event_level",
                    "training_weight": 1.0,
                    "primary_fold": "TEST",
                    "holdout_flags": {
                        "is_heldout_scorer": False,
                        "is_heldout_park": False,
                        "is_heldout_alignment_regime": False,
                        "is_heldout_source_acquisition_block": False,
                        "is_heldout_season_block": False,
                        "is_heldout_aggregate_total": False,
                        "is_heldout_player_group": False,
                    },
                    "source_snapshot_id": "snap-test",
                }
            )

    schema_overrides: dict[str, pl.DataType] = {
        "event_key": pl.UInt32(),
        "season": pl.Int16(),
        "training_weight": pl.Float64(),
        "is_observed": pl.Boolean(),
        "model_input_eligible": pl.Boolean(),
    }
    df = pl.DataFrame(records, schema_overrides=schema_overrides)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(parquet_path)


def _write_dataset_artifact(
    *,
    base_root: Path,
    spec: DatasetSpec,
    dataset_artifact_id: str,
    **seed_kwargs: bool,
) -> Path:
    dataset_dir = base_root / spec.name / dataset_artifact_id
    parquet_path = dataset_dir / "dataset.parquet"
    _seed_parquet(parquet_path, **seed_kwargs)
    return dataset_dir


def test_run_eda_writes_full_artifact(tmp_path: Path) -> None:
    spec = _spec()
    datasets_root = tmp_path / "datasets"
    eda_root = tmp_path / "eda"
    _write_dataset_artifact(
        base_root=datasets_root,
        spec=spec,
        dataset_artifact_id="ds-1",
    )

    manifest = run_eda(
        spec,
        dataset_artifact_id="ds-1",
        artifact_id="eda-1",
        output_root=eda_root,
        dataset_artifact_root=datasets_root,
        artifact_versions={"duckdb": "test"},
    )

    artifact_dir = eda_root / spec.name / "eda-1"
    assert (artifact_dir / "manifest.json").exists()
    assert (artifact_dir / "report.json").exists()
    assert (artifact_dir / "eda.md").exists()
    expected_module_files = (
        "missingness_by_slice.parquet",
        "target_distribution.parquet",
        "source_family_block_missingness.parquet",
        "data_error_concentration.parquet",
        "connectivity_edges.parquet",
        "collinearity_report.parquet",
        "candidate_interactions.parquet",
        "weak_identification_flags.parquet",
        "split_leakage_report.parquet",
    )
    for fname in expected_module_files:
        assert (artifact_dir / fname).exists(), fname

    assert manifest.kind == "eda"
    assert manifest.dataset_artifact_id == "ds-1"
    assert manifest.source_snapshot_id == "snap-test"
    assert manifest.metadata["row_count"] > 0

    report = EdaReport.model_validate_json(
        (artifact_dir / "report.json").read_text(encoding="utf-8")
    )
    assert report.dataset_name == spec.name
    assert report.dataset_artifact_id == "ds-1"
    assert report.row_count == int(manifest.metadata["row_count"])
    assert report.module_paths.keys() == {
        "missingness_by_slice",
        "target_distribution",
        "source_family_block_missingness",
        "data_error_concentration",
        "connectivity_edges",
        "collinearity_report",
        "candidate_interactions",
        "weak_identification_flags",
        "split_leakage_report",
    }


def test_run_eda_idempotent_rerun(tmp_path: Path) -> None:
    spec = _spec()
    datasets_root = tmp_path / "datasets"
    eda_root = tmp_path / "eda"
    _write_dataset_artifact(
        base_root=datasets_root,
        spec=spec,
        dataset_artifact_id="ds-2",
    )

    first = run_eda(
        spec,
        dataset_artifact_id="ds-2",
        artifact_id="eda-2",
        output_root=eda_root,
        dataset_artifact_root=datasets_root,
        artifact_versions={"duckdb": "test"},
    )
    manifest_path = eda_root / spec.name / "eda-2" / "manifest.json"
    mtime = manifest_path.stat().st_mtime_ns

    second = run_eda(
        spec,
        dataset_artifact_id="ds-2",
        artifact_id="eda-2",
        output_root=eda_root,
        dataset_artifact_root=datasets_root,
        artifact_versions={"duckdb": "test"},
    )

    assert first.artifact_id == second.artifact_id
    assert first.query_hash == second.query_hash
    assert manifest_path.stat().st_mtime_ns == mtime


def test_run_eda_fires_data_error_truth_finding(tmp_path: Path) -> None:
    spec = _spec()
    datasets_root = tmp_path / "datasets"
    _write_dataset_artifact(
        base_root=datasets_root,
        spec=spec,
        dataset_artifact_id="ds-derr",
        inject_data_error_truth=True,
        inject_dominant_scorer=False,
        inject_test_only_category=False,
    )
    manifest = run_eda(
        spec,
        dataset_artifact_id="ds-derr",
        artifact_id="eda-derr",
        output_root=tmp_path / "eda",
        dataset_artifact_root=datasets_root,
        artifact_versions={"duckdb": "test"},
    )
    assert "data_error_rows_train_as_truth" in manifest.blocking_findings


def test_run_eda_fires_dominant_scorer_finding(tmp_path: Path) -> None:
    spec = _spec()
    datasets_root = tmp_path / "datasets"
    _write_dataset_artifact(
        base_root=datasets_root,
        spec=spec,
        dataset_artifact_id="ds-dom",
        inject_data_error_truth=False,
        inject_dominant_scorer=True,
        inject_test_only_category=False,
    )
    manifest = run_eda(
        spec,
        dataset_artifact_id="ds-dom",
        artifact_id="eda-dom",
        output_root=tmp_path / "eda",
        dataset_artifact_root=datasets_root,
        artifact_versions={"duckdb": "test"},
    )
    assert "dominant_single_scorer_park_team" in manifest.blocking_findings


def test_run_eda_fires_test_only_category_finding(tmp_path: Path) -> None:
    spec = _spec()
    datasets_root = tmp_path / "datasets"
    _write_dataset_artifact(
        base_root=datasets_root,
        spec=spec,
        dataset_artifact_id="ds-cat",
        inject_data_error_truth=False,
        inject_dominant_scorer=False,
        inject_test_only_category=True,
    )
    manifest = run_eda(
        spec,
        dataset_artifact_id="ds-cat",
        artifact_id="eda-cat",
        output_root=tmp_path / "eda",
        dataset_artifact_root=datasets_root,
        artifact_versions={"duckdb": "test"},
    )
    assert "category_absent_in_train_present_in_test" in manifest.blocking_findings


def test_run_eda_markdown_lists_findings(tmp_path: Path) -> None:
    spec = _spec()
    datasets_root = tmp_path / "datasets"
    eda_root = tmp_path / "eda"
    _write_dataset_artifact(
        base_root=datasets_root,
        spec=spec,
        dataset_artifact_id="ds-md",
        inject_data_error_truth=True,
        inject_dominant_scorer=True,
        inject_test_only_category=False,
    )
    manifest = run_eda(
        spec,
        dataset_artifact_id="ds-md",
        artifact_id="eda-md",
        output_root=eda_root,
        dataset_artifact_root=datasets_root,
        artifact_versions={"duckdb": "test"},
    )
    md = (eda_root / spec.name / "eda-md" / "eda.md").read_text(encoding="utf-8")
    assert spec.name in md
    for code in manifest.blocking_findings:
        assert code in md


def test_run_eda_rejects_dataset_artifact_drift(tmp_path: Path) -> None:
    spec = _spec()
    datasets_root = tmp_path / "datasets"
    eda_root = tmp_path / "eda"
    _write_dataset_artifact(
        base_root=datasets_root,
        spec=spec,
        dataset_artifact_id="ds-A",
    )
    _write_dataset_artifact(
        base_root=datasets_root,
        spec=spec,
        dataset_artifact_id="ds-B",
    )
    _ = run_eda(
        spec,
        dataset_artifact_id="ds-A",
        artifact_id="eda-drift",
        output_root=eda_root,
        dataset_artifact_root=datasets_root,
        artifact_versions={"duckdb": "test"},
    )
    with pytest.raises(ValueError, match="different dataset"):
        _ = run_eda(
            spec,
            dataset_artifact_id="ds-B",
            artifact_id="eda-drift",
            output_root=eda_root,
            dataset_artifact_root=datasets_root,
            artifact_versions={"duckdb": "test"},
        )


def test_run_eda_missing_parquet_errors(tmp_path: Path) -> None:
    spec = _spec()
    datasets_root = tmp_path / "datasets"
    with pytest.raises(FileNotFoundError, match="dataset Parquet missing"):
        _ = run_eda(
            spec,
            dataset_artifact_id="does-not-exist",
            artifact_id="eda-noop",
            output_root=tmp_path / "eda",
            dataset_artifact_root=datasets_root,
        )


def test_run_eda_manifest_is_artifact_manifest(tmp_path: Path) -> None:
    spec = _spec()
    datasets_root = tmp_path / "datasets"
    eda_root = tmp_path / "eda"
    _write_dataset_artifact(
        base_root=datasets_root,
        spec=spec,
        dataset_artifact_id="ds-am",
    )
    manifest = run_eda(
        spec,
        dataset_artifact_id="ds-am",
        artifact_id="eda-am",
        output_root=eda_root,
        dataset_artifact_root=datasets_root,
        artifact_versions={"duckdb": "test"},
    )
    persisted = read_manifest(eda_root / spec.name / "eda-am" / "manifest.json")
    assert isinstance(persisted, ArtifactManifest)
    assert persisted.kind == "eda"
    assert persisted.artifact_id == manifest.artifact_id
    assert persisted.dataset_artifact_id == "ds-am"
    assert "report" in persisted.output_paths
    assert "markdown" in persisted.output_paths


def test_run_eda_fires_split_leakage_detected(tmp_path: Path) -> None:
    spec = _spec()
    datasets_root = tmp_path / "datasets"
    dataset_dir = datasets_root / spec.name / "ds-leak"
    parquet_path = dataset_dir / "dataset.parquet"
    _seed_parquet(
        parquet_path,
        rows=600,
        inject_data_error_truth=False,
        inject_dominant_scorer=False,
        inject_test_only_category=False,
    )
    base = pl.read_parquet(parquet_path)
    base_row = {col: base[col][0] for col in base.columns if col != "holdout_flags"}
    leak_rows = pl.DataFrame(
        [
            {
                **base_row,
                "event_key": 9_999_001,
                "scorer": "scorer_split",
                "holdout_flags": {
                    "is_heldout_scorer": True,
                    "is_heldout_park": False,
                    "is_heldout_alignment_regime": False,
                    "is_heldout_source_acquisition_block": False,
                    "is_heldout_season_block": False,
                    "is_heldout_aggregate_total": False,
                    "is_heldout_player_group": False,
                },
            },
            {
                **base_row,
                "event_key": 9_999_002,
                "scorer": "scorer_split",
                "holdout_flags": {
                    "is_heldout_scorer": False,
                    "is_heldout_park": False,
                    "is_heldout_alignment_regime": False,
                    "is_heldout_source_acquisition_block": False,
                    "is_heldout_season_block": False,
                    "is_heldout_aggregate_total": False,
                    "is_heldout_player_group": False,
                },
            },
        ],
        schema=base.schema,
    )
    combined = pl.concat([base, leak_rows], how="vertical_relaxed")
    combined.write_parquet(parquet_path)

    manifest = run_eda(
        spec,
        dataset_artifact_id="ds-leak",
        artifact_id="eda-leak",
        output_root=tmp_path / "eda",
        dataset_artifact_root=datasets_root,
        artifact_versions={"duckdb": "test"},
    )
    assert "split_leakage_detected" in manifest.blocking_findings


def test_run_eda_module_parquet_schemas_are_consistent(tmp_path: Path) -> None:
    spec = _spec()
    datasets_root = tmp_path / "datasets"
    eda_root = tmp_path / "eda"
    _write_dataset_artifact(
        base_root=datasets_root,
        spec=spec,
        dataset_artifact_id="ds-sch",
    )
    _ = run_eda(
        spec,
        dataset_artifact_id="ds-sch",
        artifact_id="eda-sch",
        output_root=eda_root,
        dataset_artifact_root=datasets_root,
        artifact_versions={"duckdb": "test"},
    )
    artifact_dir = eda_root / spec.name / "eda-sch"
    for name in ("missingness_by_slice", "target_distribution", "connectivity_edges"):
        table = pq.read_table(artifact_dir / f"{name}.parquet")
        assert table.num_rows >= 0
        assert len(table.column_names) > 0
