"""Driver for the derived-slice bounds: published inputs resolve lazily."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl
import pytest

from python_models.statistical import config as cfg
from python_models.statistical.manifests import write_published_pointer
from python_models.statistical.models._geometry_data import GEOMETRY_DIMENSIONS
from python_models.statistical.schemas import PublishedPointer

SCRIPTS_DIR = Path(__file__).resolve().parents[3] / "scripts"
MODEL_NAME = "geometry_trajectory"
_TRAJECTORY_LABELS = GEOMETRY_DIMENSIONS["trajectory"].class_labels
assert _TRAJECTORY_LABELS is not None
CLASSES: tuple[str, ...] = _TRAJECTORY_LABELS


def _load() -> Any:
    sys.path.insert(0, str(SCRIPTS_DIR))
    try:
        module_name = f"mnar_anchor_script_test_{uuid4().hex}"
        spec = importlib.util.spec_from_file_location(
            module_name, SCRIPTS_DIR / "mnar_anchor.py"
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        finally:
            sys.modules.pop(module_name, None)
        return module
    finally:
        sys.path.remove(str(SCRIPTS_DIR))


def _slice_frame() -> pl.DataFrame:
    rows = [
        (1, 1930, "observed", "Fly", None),
        (2, 1930, "observed", "GroundBall", None),
        (3, 1930, "derived", None, "GroundBall"),
        (4, 1930, "unknown_code", None, None),
        (5, 2001, "observed", "LineDrive", None),
        (6, 2001, "derived", None, "GroundBall"),
    ]
    return pl.DataFrame(
        {
            "geometry_dimension": ["trajectory"] * len(rows),
            "event_key": [r[0] for r in rows],
            "season": [r[1] for r in rows],
            "observed_status": [r[2] for r in rows],
            "raw_value": [r[3] for r in rows],
            "deduced_value": [r[4] for r in rows],
        },
        schema={
            "geometry_dimension": pl.Utf8,
            "event_key": pl.Int64,
            "season": pl.Int64,
            "observed_status": pl.Utf8,
            "raw_value": pl.Utf8,
            "deduced_value": pl.Utf8,
        },
    )


def _export_frame() -> pl.DataFrame:
    unrecorded = (3, 4, 6)
    share = 1.0 / len(CLASSES)
    return pl.DataFrame(
        {
            "event_key": [e for e in unrecorded for _ in CLASSES],
            "geometry_dimension": ["trajectory"] * (len(unrecorded) * len(CLASSES)),
            "class_label": [c for _ in unrecorded for c in CLASSES],
            "expected_share": [share] * (len(unrecorded) * len(CLASSES)),
        }
    )


def _publish_fit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    with_dataset: bool,
    with_export: bool,
) -> tuple[Path, Path]:
    artifacts_root = tmp_path / "canonical"
    artifact_dir = artifacts_root / "bayes" / MODEL_NAME / "fit-1"
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
        _slice_frame().write_parquet(dataset_path)
    if with_export:
        export_path.parent.mkdir(parents=True)
        _export_frame().write_parquet(export_path)
    monkeypatch.setenv(cfg.ENV_ARTIFACTS_ROOT, str(artifacts_root))
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(tmp_path / "published"))
    monkeypatch.setattr(cfg, "DATASETS_ROOT", datasets_root)
    _ = write_published_pointer(
        PublishedPointer(
            model_name=MODEL_NAME,
            artifact_id="fit-1",
            published_at=datetime.now(tz=timezone.utc),
            manifest_path=manifest_path,
        ),
        root=tmp_path / "published",
    )
    return dataset_path, export_path


def _override(tmp_path: Path, name: str, frame: pl.DataFrame) -> Path:
    path = tmp_path / "overrides" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path)
    return path


def _run(
    module: Any,
    tmp_path: Path,
    *,
    dataset_path: Path | None = None,
    export_path: Path | None = None,
) -> Path:
    return module.run(
        source="dataset",
        dataset_path=dataset_path,
        db_path=tmp_path / "absent.db",
        export_path=export_path,
        out_root=tmp_path / "anchors",
        run_id="run-1",
    )


def _assert_run_outputs(run_dir: Path) -> dict[str, Any]:
    bounds = pl.read_parquet(run_dir / "derived_slice_bounds.parquet")
    assert set(bounds.get_column("bucketing").unique().to_list()) == {"paper", "decade"}
    assert bounds.get_column("share_lower_bound").null_count() == 0
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["trajectory_artifact_id"] == "fit-1"
    assert summary["dataset_artifact_id"] == "ds-1"
    return summary


def test_dataset_override_bypasses_a_missing_published_dataset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load()
    dataset_path, export_path = _publish_fit(
        tmp_path, monkeypatch, with_dataset=False, with_export=True
    )
    assert not dataset_path.exists()
    override = _override(tmp_path, "slice.parquet", _slice_frame())

    run_dir = _run(module, tmp_path, dataset_path=override)

    summary = _assert_run_outputs(run_dir)
    assert summary["slice_path"] == str(override)
    assert summary["export_path"] == str(export_path)


def test_export_override_bypasses_a_missing_published_export(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load()
    dataset_path, export_path = _publish_fit(
        tmp_path, monkeypatch, with_dataset=True, with_export=False
    )
    assert not export_path.exists()
    override = _override(tmp_path, "export.parquet", _export_frame())

    run_dir = _run(module, tmp_path, export_path=override)

    summary = _assert_run_outputs(run_dir)
    assert summary["slice_path"] == str(dataset_path)
    assert summary["export_path"] == str(override)


def test_missing_published_input_without_an_override_still_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load()
    _ = _publish_fit(tmp_path, monkeypatch, with_dataset=False, with_export=True)
    with pytest.raises(FileNotFoundError, match="dataset.parquet"):
        _ = _run(module, tmp_path)


def test_published_inputs_run_end_to_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load()
    dataset_path, export_path = _publish_fit(
        tmp_path, monkeypatch, with_dataset=True, with_export=True
    )
    run_dir = _run(module, tmp_path)
    summary = _assert_run_outputs(run_dir)
    assert summary["slice_path"] == str(dataset_path)
    assert summary["export_path"] == str(export_path)
