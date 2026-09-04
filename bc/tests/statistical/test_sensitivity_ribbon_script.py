"""Driver for the sensitivity ribbon: pointer resolution and anchor-run provenance."""

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
from python_models.statistical.schemas import PublishedPointer

SCRIPTS_DIR = Path(__file__).resolve().parents[3] / "scripts"
MODEL_NAME = "geometry_trajectory"


def _load() -> Any:
    sys.path.insert(0, str(SCRIPTS_DIR))
    try:
        module_name = f"sensitivity_ribbon_script_test_{uuid4().hex}"
        spec = importlib.util.spec_from_file_location(
            module_name, SCRIPTS_DIR / "sensitivity_ribbon.py"
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


def _publish_fit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    artifact_id: str,
    relative_pointer: bool,
) -> Path:
    artifacts_root = tmp_path / "canonical"
    artifact_dir = artifacts_root / "bayes" / MODEL_NAME / artifact_id
    export_path = artifact_dir / "exports" / "geometry_probabilities.parquet"
    export_path.parent.mkdir(parents=True)
    manifest_path = artifact_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps({"artifact_id": artifact_id, "dataset_artifact_id": "ds-1"}),
        encoding="utf-8",
    )
    pl.DataFrame(
        {
            "event_key": [1],
            "geometry_dimension": ["trajectory"],
            "class_label": ["GroundBall"],
            "expected_share": [1.0],
        }
    ).write_parquet(export_path)
    monkeypatch.setenv(cfg.ENV_ARTIFACTS_ROOT, str(artifacts_root))
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(tmp_path / "published"))
    monkeypatch.setattr(cfg, "DATASETS_ROOT", artifacts_root / "datasets")
    _ = write_published_pointer(
        PublishedPointer(
            model_name=MODEL_NAME,
            artifact_id=artifact_id,
            published_at=datetime.now(tz=timezone.utc),
            manifest_path=manifest_path,
        ),
        root=tmp_path / "published",
        relative=relative_pointer,
    )
    return export_path


def _anchor_run(tmp_path: Path, summary: dict[str, object] | None) -> Path:
    anchor_dir = tmp_path / "anchors" / "trajectory" / "run-1"
    anchor_dir.mkdir(parents=True)
    if summary is not None:
        (anchor_dir / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
    return anchor_dir


@pytest.mark.parametrize("relative_pointer", [True, False])
def test_published_export_resolves_either_pointer_form(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, relative_pointer: bool
) -> None:
    module = _load()
    export_path = _publish_fit(
        tmp_path, monkeypatch, artifact_id="fit-1", relative_pointer=relative_pointer
    )
    stored = json.loads(
        (tmp_path / "published" / f"{MODEL_NAME}.json").read_text(encoding="utf-8")
    )
    assert Path(stored["manifest_path"]).is_absolute() is not relative_pointer

    resolved = module._published_export(MODEL_NAME)

    assert resolved is not None
    artifact_id, resolved_export = resolved
    assert artifact_id == "fit-1"
    assert resolved_export == export_path
    assert resolved_export.is_absolute()


def test_anchor_artifact_id_is_read_from_the_run_summary(tmp_path: Path) -> None:
    module = _load()
    anchor_dir = _anchor_run(tmp_path, {"trajectory_artifact_id": "fit-1"})
    assert module.anchor_run_artifact_id(anchor_dir) == "fit-1"
    module.assert_anchor_matches_published(anchor_dir, "fit-1")


def test_anchor_from_a_different_fit_raises_naming_both_ids(tmp_path: Path) -> None:
    module = _load()
    anchor_dir = _anchor_run(tmp_path, {"trajectory_artifact_id": "fit-old"})
    with pytest.raises(ValueError, match="fit-old") as excinfo:
        module.assert_anchor_matches_published(anchor_dir, "fit-new")
    assert "fit-new" in str(excinfo.value)


@pytest.mark.parametrize(
    "summary",
    [None, {}, {"trajectory_artifact_id": ""}, {"trajectory_artifact_id": None}],
)
def test_anchor_without_a_recorded_fit_raises(
    tmp_path: Path, summary: dict[str, object] | None
) -> None:
    module = _load()
    anchor_dir = _anchor_run(tmp_path, summary)
    with pytest.raises((FileNotFoundError, ValueError)):
        _ = module.anchor_run_artifact_id(anchor_dir)


def test_publish_bound_ribbons_refuses_a_stale_anchor_before_reading_bounds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load()
    export_path = _publish_fit(
        tmp_path, monkeypatch, artifact_id="fit-new", relative_pointer=True
    )
    anchor_dir = _anchor_run(tmp_path, {"trajectory_artifact_id": "fit-old"})
    assert not (anchor_dir / "derived_slice_bounds.parquet").exists()

    with pytest.raises(ValueError, match="fit-old"):
        _ = module.publish_bound_ribbons(
            anchor_dir,
            source="dataset",
            dataset_path=tmp_path / "unused.parquet",
            db_path=tmp_path / "absent.db",
        )
    assert not (export_path.parent / "sensitivity_ribbon_by_era.parquet").exists()
