"""End-to-end validate_artifact() against synthetic deep artifacts."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from python_models.statistical import config as cfg
from python_models.statistical.manifests import (
    package_versions,
    write_manifest,
)
from python_models.statistical.schemas import ArtifactManifest
from python_models.statistical.validate import validate_artifact


def _write_probabilities(
    artifact_dir: Path,
    *,
    p_class_rows: list[list[float]],
    grain_values: list[int] | None = None,
    partitions: list[str] | None = None,
    extra_columns: dict[str, Any] | None = None,
) -> Path:
    exports = artifact_dir / "exports"
    exports.mkdir(parents=True, exist_ok=True)
    n = len(p_class_rows)
    grain = grain_values if grain_values is not None else list(range(n))
    parts = partitions if partitions is not None else ["OOF"] * n
    columns: dict[str, object] = {
        "event_key": pl.Series(grain, dtype=pl.UInt32),
        "partition": pl.Series(parts, dtype=pl.Utf8),
        "dl_p_class": pl.Series("dl_p_class", p_class_rows, dtype=pl.List(pl.Float64)),
    }
    if extra_columns:
        for k, v in extra_columns.items():
            columns[k] = v
    df = pl.DataFrame(columns)
    path = exports / "probabilities.parquet"
    df.write_parquet(path)
    return path


def _write_deep_manifest(
    deep_root: Path,
    *,
    target: str,
    artifact_id: str,
) -> Path:
    artifact_dir = deep_root / target / artifact_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    manifest = ArtifactManifest(
        artifact_id=artifact_id,
        kind="deep",
        name=target,
        version="0.1.0",
        created_at=datetime.now(tz=timezone.utc),
        source_snapshot_id="src-validate-1",
        query_hash="deadbeef",
        dataset_artifact_id="ds-1",
        output_paths={"artifact_dir": artifact_dir},
        package_versions=package_versions(),
    )
    write_manifest(manifest, artifact_dir / "manifest.json")
    return artifact_dir


def test_validate_deep_passes_with_normalized_probs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    artifact_dir = _write_deep_manifest(
        tmp_path / "deep", target="dl_proposal_trajectory", artifact_id="aid-pass"
    )
    _ = _write_probabilities(
        artifact_dir,
        p_class_rows=[[0.6, 0.3, 0.1], [0.4, 0.4, 0.2], [0.1, 0.1, 0.8]],
    )
    report = validate_artifact("aid-pass")
    assert report.status == "passed"
    block_codes = {f.code for f in report.findings if f.severity == "block"}
    assert block_codes == set()
    assert report.metrics["probability_normalization_violations"] == 0
    assert report.metrics["grain_uniqueness_violations"] == 0


def test_validate_deep_flags_unnormalized_probs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    artifact_dir = _write_deep_manifest(
        tmp_path / "deep", target="dl_proposal_trajectory", artifact_id="aid-bad"
    )
    _ = _write_probabilities(
        artifact_dir,
        p_class_rows=[[0.5, 0.3, 0.1], [0.4, 0.4, 0.2]],
    )
    report = validate_artifact("aid-bad")
    assert report.status == "failed"
    codes = {f.code for f in report.findings}
    assert "deep_probability_not_normalized" in codes


def test_validate_deep_blocks_argmax_column(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    artifact_dir = _write_deep_manifest(
        tmp_path / "deep", target="dl_proposal_trajectory", artifact_id="aid-argmax"
    )
    _ = _write_probabilities(
        artifact_dir,
        p_class_rows=[[0.6, 0.3, 0.1]],
        extra_columns={"predicted_class": pl.Series(["A"], dtype=pl.Utf8)},
    )
    report = validate_artifact("aid-argmax")
    codes = {f.code for f in report.findings}
    assert "deep_argmax_column_exposed" in codes
    assert report.status == "failed"


def test_validate_deep_flags_duplicate_grain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    artifact_dir = _write_deep_manifest(
        tmp_path / "deep", target="dl_proposal_trajectory", artifact_id="aid-dup"
    )
    _ = _write_probabilities(
        artifact_dir,
        p_class_rows=[[0.6, 0.4], [0.5, 0.5]],
        grain_values=[1, 1],
        partitions=["OOF", "OOF"],
    )
    report = validate_artifact("aid-dup")
    codes = {f.code for f in report.findings}
    assert "deep_grain_not_unique_per_partition" in codes
    assert report.status == "failed"


def test_validate_deep_missing_probabilities_blocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    _ = _write_deep_manifest(
        tmp_path / "deep", target="dl_proposal_trajectory", artifact_id="aid-empty"
    )
    report = validate_artifact("aid-empty")
    codes = {f.code for f in report.findings}
    assert "deep_missing_probabilities" in codes
    assert report.status == "failed"


def test_validate_deep_oof_fold_integrity_passes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    artifact_dir = _write_deep_manifest(
        tmp_path / "deep", target="dl_proposal_trajectory", artifact_id="aid-fold-ok"
    )
    _ = _write_probabilities(
        artifact_dir,
        p_class_rows=[[0.6, 0.4], [0.5, 0.5], [0.3, 0.7], [0.2, 0.8]],
        partitions=["OOF", "OOF", "VALIDATE", "TEST"],
        extra_columns={"fold_id": pl.Series([0, 1, None, None], dtype=pl.Int32)},
    )
    report = validate_artifact("aid-fold-ok")
    assert report.status == "passed"
    codes = {f.code for f in report.findings}
    assert "deep_oof_fold_provenance_unverifiable" not in codes
    assert report.metrics["oof_row_count"] == 2
    assert report.metrics["oof_distinct_fold_count"] == 2
    assert report.metrics["oof_null_fold_id_rows"] == 0
    assert report.metrics["non_oof_fold_id_rows"] == 0
    assert report.metrics["cross_partition_grain_violations"] == 0


def test_validate_deep_single_fold_oof_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    artifact_dir = _write_deep_manifest(
        tmp_path / "deep", target="dl_proposal_trajectory", artifact_id="aid-one-fold"
    )
    _ = _write_probabilities(
        artifact_dir,
        p_class_rows=[[0.6, 0.4], [0.5, 0.5], [0.3, 0.7]],
        partitions=["OOF", "OOF", "OOF"],
        extra_columns={"fold_id": pl.Series([0, 0, 0], dtype=pl.Int32)},
    )
    report = validate_artifact("aid-one-fold")
    assert report.status == "failed"
    block_codes = {f.code for f in report.findings if f.severity == "block"}
    assert "deep_oof_single_fold" in block_codes


def test_validate_deep_missing_fold_id_warns_not_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    artifact_dir = _write_deep_manifest(
        tmp_path / "deep", target="dl_proposal_trajectory", artifact_id="aid-no-fold"
    )
    _ = _write_probabilities(
        artifact_dir,
        p_class_rows=[[0.6, 0.4], [0.5, 0.5]],
    )
    report = validate_artifact("aid-no-fold")
    assert report.status == "passed"
    warn_codes = {f.code for f in report.findings if f.severity == "warn"}
    assert "deep_oof_fold_provenance_unverifiable" in warn_codes


def test_validate_deep_null_fold_id_on_oof_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    artifact_dir = _write_deep_manifest(
        tmp_path / "deep", target="dl_proposal_trajectory", artifact_id="aid-null-oof"
    )
    _ = _write_probabilities(
        artifact_dir,
        p_class_rows=[[0.6, 0.4], [0.5, 0.5], [0.3, 0.7]],
        partitions=["OOF", "OOF", "OOF"],
        extra_columns={"fold_id": pl.Series([0, 1, None], dtype=pl.Int32)},
    )
    report = validate_artifact("aid-null-oof")
    assert report.status == "failed"
    block_codes = {f.code for f in report.findings if f.severity == "block"}
    assert "deep_oof_fold_id_null" in block_codes


def test_validate_deep_fold_id_outside_oof_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    artifact_dir = _write_deep_manifest(
        tmp_path / "deep", target="dl_proposal_trajectory", artifact_id="aid-leaky-val"
    )
    _ = _write_probabilities(
        artifact_dir,
        p_class_rows=[[0.6, 0.4], [0.5, 0.5], [0.3, 0.7]],
        partitions=["OOF", "OOF", "VALIDATE"],
        extra_columns={"fold_id": pl.Series([0, 1, 2], dtype=pl.Int32)},
    )
    report = validate_artifact("aid-leaky-val")
    assert report.status == "failed"
    block_codes = {f.code for f in report.findings if f.severity == "block"}
    assert "deep_fold_id_outside_oof" in block_codes


def test_validate_deep_grain_spanning_partitions_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    artifact_dir = _write_deep_manifest(
        tmp_path / "deep", target="dl_proposal_trajectory", artifact_id="aid-spanning"
    )
    _ = _write_probabilities(
        artifact_dir,
        p_class_rows=[[0.6, 0.4], [0.5, 0.5], [0.3, 0.7]],
        grain_values=[1, 2, 1],
        partitions=["OOF", "OOF", "TEST"],
        extra_columns={"fold_id": pl.Series([0, 1, None], dtype=pl.Int32)},
    )
    report = validate_artifact("aid-spanning")
    assert report.status == "failed"
    block_codes = {f.code for f in report.findings if f.severity == "block"}
    assert "deep_grain_spans_partitions" in block_codes


def test_validate_unknown_artifact_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    monkeypatch.setattr(cfg, "BAYES_ROOT", tmp_path / "bayes")
    monkeypatch.setattr(cfg, "DATASETS_ROOT", tmp_path / "datasets")
    monkeypatch.setattr(cfg, "EDA_ROOT", tmp_path / "eda")
    with pytest.raises(FileNotFoundError):
        _ = validate_artifact("does-not-exist")
