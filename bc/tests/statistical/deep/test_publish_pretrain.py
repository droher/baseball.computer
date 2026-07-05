"""publish-pretrain handler + offset-artifact resolution: rglob ambiguity guard."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from python_models.statistical import config as cfg
from python_models.statistical.cli import _run_publish_pretrain
from python_models.statistical.manifests import (
    PRETRAIN_POINTER_SUBDIR,
    package_versions,
    write_manifest,
)
from python_models.statistical.schemas import ArtifactManifest


def _fake_pretrain_manifest(root: Path, spec_name: str, artifact_id: str) -> Path:
    artifact_dir = root / spec_name / artifact_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    manifest = ArtifactManifest(
        artifact_id=artifact_id,
        kind="pretrain",
        name=spec_name,
        version="0.1.0",
        created_at=datetime.now(tz=timezone.utc),
        source_snapshot_id="src-0001",
        query_hash="dead0001",
        dataset_artifact_id="ds-0001",
        output_paths={"artifact_dir": artifact_dir},
        package_versions=package_versions(),
    )
    manifest_path = artifact_dir / "manifest.json"
    write_manifest(manifest, manifest_path)
    return manifest_path


@pytest.fixture
def deep_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "deep"
    monkeypatch.setattr(cfg, "DEEP_ROOT", root)
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(tmp_path / "published"))
    return root


def test_publish_pretrain_single_match_writes_pointer(
    deep_root: Path, tmp_path: Path
) -> None:
    _ = _fake_pretrain_manifest(deep_root, "event_universe_context", "pre-0001")

    rc = _run_publish_pretrain(argparse.Namespace(artifact_id="pre-0001"))

    assert rc == 0
    pointer_path = (
        tmp_path / "published" / PRETRAIN_POINTER_SUBDIR / "event_universe_context.json"
    )
    assert pointer_path.exists()
    payload = json.loads(pointer_path.read_text(encoding="utf-8"))
    assert payload["artifact_id"] == "pre-0001"


def test_publish_pretrain_ambiguous_artifact_id_raises(deep_root: Path) -> None:
    shared_id = "pre-0002"
    _ = _fake_pretrain_manifest(deep_root, "event_universe", shared_id)
    _ = _fake_pretrain_manifest(deep_root, "event_universe_context", shared_id)

    with pytest.raises(ValueError, match="ambiguous"):
        _ = _run_publish_pretrain(argparse.Namespace(artifact_id=shared_id))


def test_publish_pretrain_missing_artifact_id_raises(deep_root: Path) -> None:
    with pytest.raises(FileNotFoundError, match="no manifest.json"):
        _ = _run_publish_pretrain(argparse.Namespace(artifact_id="absent"))


@pytest.mark.slow
def test_load_offset_artifact_ambiguous_artifact_id_raises(tmp_path: Path) -> None:
    from python_models.statistical.deep.pretrain.training import _load_offset_artifact

    shared_id = "stage1-0001"
    for spec_name in ("event_universe_context", "event_universe"):
        _ = _fake_pretrain_manifest(tmp_path, spec_name, shared_id)

    with pytest.raises(ValueError, match="ambiguous"):
        _ = _load_offset_artifact(
            offset_artifact_id=shared_id, artifact_root=tmp_path
        )


@pytest.mark.slow
def test_load_offset_artifact_missing_artifact_id_raises(tmp_path: Path) -> None:
    from python_models.statistical.deep.pretrain.training import _load_offset_artifact

    with pytest.raises(FileNotFoundError, match="manifest not found"):
        _ = _load_offset_artifact(
            offset_artifact_id="absent", artifact_root=tmp_path
        )
