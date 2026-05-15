"""publish-manifest CLI handler: discover artifact, write PublishedPointer."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from python_models.statistical import config as cfg
from python_models.statistical.cli import _run_publish_manifest
from python_models.statistical.manifests import (
    find_published_manifest,
    package_versions,
    write_manifest,
)
from python_models.statistical.schemas import ArtifactManifest


def _fake_deep_manifest(deep_root: Path, target: str, artifact_id: str) -> Path:
    artifact_dir = deep_root / target / artifact_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    manifest = ArtifactManifest(
        artifact_id=artifact_id,
        kind="deep",
        name=target,
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


def test_publish_manifest_roundtrip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    deep_root = tmp_path / "deep"
    published_root = tmp_path / "published"
    monkeypatch.setattr(cfg, "DEEP_ROOT", deep_root)
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))

    artifact_id = "pr1-smoke-fit"
    target_name = "dl_proposal_trajectory"
    _ = _fake_deep_manifest(deep_root, target_name, artifact_id)

    args = argparse.Namespace(
        model=target_name,
        artifact_id=artifact_id,
    )
    rc = _run_publish_manifest(args)
    assert rc == 0

    pointer_path = published_root / f"{target_name}.json"
    assert pointer_path.exists()
    payload = json.loads(pointer_path.read_text(encoding="utf-8"))
    assert payload["model_name"] == target_name
    assert payload["artifact_id"] == artifact_id

    resolved = find_published_manifest(target_name)
    assert resolved is not None
    assert resolved == pointer_path


def test_publish_manifest_raises_when_artifact_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    monkeypatch.setattr(cfg, "BAYES_ROOT", tmp_path / "bayes")
    monkeypatch.setattr(cfg, "DATASETS_ROOT", tmp_path / "datasets")
    monkeypatch.setattr(cfg, "EDA_ROOT", tmp_path / "eda")
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(tmp_path / "published"))

    args = argparse.Namespace(model="nope", artifact_id="missing")
    with pytest.raises(FileNotFoundError):
        _ = _run_publish_manifest(args)
