"""Validate library hooks for the optional baseline-predictions comparison."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from python_models.statistical import config as cfg
from python_models.statistical.manifests import package_versions, write_manifest
from python_models.statistical.schemas import ArtifactManifest
from python_models.statistical.validate import validate_artifact


def _setup_deep_artifact(
    deep_root: Path, *, with_baseline: bool, baseline_has_dl_p_class: bool
) -> Path:
    artifact_dir = deep_root / "dl_proposal_trajectory" / "aid-baseline"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    exports = artifact_dir / "exports"
    exports.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "event_key": pl.Series([1, 2, 3], dtype=pl.UInt32),
            "partition": pl.Series(["OOF", "OOF", "OOF"], dtype=pl.Utf8),
            "dl_p_class": pl.Series(
                "dl_p_class",
                [[0.5, 0.5], [0.7, 0.3], [0.2, 0.8]],
                dtype=pl.List(pl.Float64),
            ),
        }
    ).write_parquet(exports / "probabilities.parquet")
    (exports / "class_labels.json").write_text(
        '{"labels":["class_0","class_1"]}', encoding="utf-8"
    )

    if with_baseline:
        baseline_cols: dict[str, object] = {
            "event_key": pl.Series([1, 2, 3], dtype=pl.UInt32),
            "partition": pl.Series(["OOF", "OOF", "OOF"], dtype=pl.Utf8),
            "target_class": pl.Series(["class_0", "class_0", "class_1"], dtype=pl.Utf8),
        }
        if baseline_has_dl_p_class:
            baseline_cols["dl_p_class"] = pl.Series(
                "dl_p_class",
                [[0.5, 0.5], [0.5, 0.5], [0.5, 0.5]],
                dtype=pl.List(pl.Float64),
            )
        pl.DataFrame(baseline_cols).write_parquet(
            exports / "baseline_predictions.parquet"
        )

    write_manifest(
        ArtifactManifest(
            artifact_id="aid-baseline",
            kind="deep",
            name="dl_proposal_trajectory",
            version="0.1.0",
            created_at=datetime.now(tz=timezone.utc),
            source_snapshot_id="src-bl",
            query_hash="bd001",
            dataset_artifact_id="ds-1",
            output_paths={"artifact_dir": artifact_dir},
            package_versions=package_versions(),
            metadata={"validation_partition": "OOF"},
        ),
        artifact_dir / "manifest.json",
    )
    return artifact_dir


def test_validate_blocks_when_baseline_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    _ = _setup_deep_artifact(
        tmp_path / "deep", with_baseline=False, baseline_has_dl_p_class=False
    )
    report = validate_artifact("aid-baseline")
    codes = {f.code for f in report.findings if f.severity == "block"}
    assert "deep_baseline_absent" in codes
    assert report.status == "failed"


def test_validate_records_baseline_row_count_when_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    _ = _setup_deep_artifact(
        tmp_path / "deep", with_baseline=True, baseline_has_dl_p_class=True
    )
    report = validate_artifact("aid-baseline")
    assert report.metrics["baseline_row_count"] == 3
    assert report.metrics["deep_evaluated_rows"] == 3
    assert report.status == "passed"


def test_validate_blocks_when_baseline_missing_p_class(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    _ = _setup_deep_artifact(
        tmp_path / "deep", with_baseline=True, baseline_has_dl_p_class=False
    )
    report = validate_artifact("aid-baseline")
    codes = {f.code for f in report.findings if f.severity == "block"}
    assert "deep_baseline_contract_missing" in codes
    assert report.status == "failed"
