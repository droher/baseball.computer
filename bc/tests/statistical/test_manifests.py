"""Manifest IDs, serialization, atomic writes, published-pointer lookup."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from python_models.statistical import config as cfg
from python_models.statistical.manifests import (
    find_published_manifest,
    new_artifact_id,
    new_run_id,
    package_versions,
    query_hash,
    read_manifest,
    read_published_pointer,
    stable_model_version,
    write_manifest,
    write_published_pointer,
)
from python_models.statistical.schemas import ArtifactManifest, PublishedPointer


def _sample_manifest(tmp_path: Path) -> ArtifactManifest:
    return ArtifactManifest(
        artifact_id=new_artifact_id(),
        kind="dataset",
        name="model_input_observation_batted_ball",
        version=stable_model_version(0, 1, 0),
        created_at=datetime.now(tz=timezone.utc),
        source_snapshot_id="src-0001",
        query_hash=query_hash("SELECT 1"),
        schema_hash=None,
        dataset_artifact_id=None,
        input_artifact_ids=(),
        output_paths={"parquet": tmp_path / "dataset.parquet"},
        package_versions=package_versions(),
        random_seed=20260513,
    )


def test_new_artifact_id_is_uuid4() -> None:
    art_id = new_artifact_id()
    parsed = UUID(art_id)
    assert parsed.version == 4


def test_new_artifact_id_unique_across_calls() -> None:
    ids = {new_artifact_id() for _ in range(50)}
    assert len(ids) == 50


def test_query_hash_deterministic() -> None:
    assert query_hash("SELECT 1") == query_hash("SELECT 1")
    assert query_hash("SELECT 1") != query_hash("SELECT 2")


def test_new_run_id_prefixed() -> None:
    rid = new_run_id()
    assert rid.startswith("run-")


def test_manifest_round_trip(tmp_path: Path) -> None:
    manifest = _sample_manifest(tmp_path)
    target = tmp_path / "manifest.json"
    write_manifest(manifest, target)
    assert target.exists()
    loaded = read_manifest(target)
    assert loaded.artifact_id == manifest.artifact_id
    assert loaded.query_hash == manifest.query_hash
    assert loaded.output_paths["parquet"] == manifest.output_paths["parquet"]


def test_manifest_write_atomic_no_temp_residue(tmp_path: Path) -> None:
    manifest = _sample_manifest(tmp_path)
    target = tmp_path / "manifest.json"
    write_manifest(manifest, target)
    residue = [p for p in tmp_path.iterdir() if p.name.startswith(".manifest.")]
    assert residue == []


def test_published_pointer_round_trip(tmp_path: Path) -> None:
    pointer = PublishedPointer(
        model_name="fielding_credit",
        artifact_id=new_artifact_id(),
        published_at=datetime.now(tz=timezone.utc),
        manifest_path=tmp_path / "manifest.json",
    )
    written = write_published_pointer(pointer, root=tmp_path)
    assert written == tmp_path / "fielding_credit.json"
    loaded = read_published_pointer(written)
    assert loaded.artifact_id == pointer.artifact_id


def test_find_published_manifest_branch_overrides_global(tmp_path: Path) -> None:
    branch_root = tmp_path / "branch"
    global_root = tmp_path / "global"
    branch_root.mkdir()
    global_root.mkdir()
    (branch_root / "fielding_credit.json").write_text("{}", encoding="utf-8")
    (global_root / "fielding_credit.json").write_text("{}", encoding="utf-8")

    previous_branch = os.environ.get(cfg.ENV_PUBLISHED_ROOT)
    previous_global = cfg.GLOBAL_PUBLISHED_ROOT
    os.environ[cfg.ENV_PUBLISHED_ROOT] = str(branch_root)
    cfg.GLOBAL_PUBLISHED_ROOT = global_root  # type: ignore[misc]
    try:
        path = find_published_manifest("fielding_credit")
    finally:
        cfg.GLOBAL_PUBLISHED_ROOT = previous_global  # type: ignore[misc]
        if previous_branch is None:
            _ = os.environ.pop(cfg.ENV_PUBLISHED_ROOT, None)
        else:
            os.environ[cfg.ENV_PUBLISHED_ROOT] = previous_branch
    assert path == branch_root / "fielding_credit.json"


def test_find_published_manifest_falls_back_to_global(tmp_path: Path) -> None:
    branch_root = tmp_path / "branch"
    global_root = tmp_path / "global"
    branch_root.mkdir()
    global_root.mkdir()
    (global_root / "fielding_credit.json").write_text("{}", encoding="utf-8")

    previous_branch = os.environ.get(cfg.ENV_PUBLISHED_ROOT)
    previous_global = cfg.GLOBAL_PUBLISHED_ROOT
    os.environ[cfg.ENV_PUBLISHED_ROOT] = str(branch_root)
    cfg.GLOBAL_PUBLISHED_ROOT = global_root  # type: ignore[misc]
    try:
        path = find_published_manifest("fielding_credit")
    finally:
        cfg.GLOBAL_PUBLISHED_ROOT = previous_global  # type: ignore[misc]
        if previous_branch is None:
            _ = os.environ.pop(cfg.ENV_PUBLISHED_ROOT, None)
        else:
            os.environ[cfg.ENV_PUBLISHED_ROOT] = previous_branch
    assert path == global_root / "fielding_credit.json"


def test_find_published_manifest_missing_returns_none(tmp_path: Path) -> None:
    previous_branch = os.environ.get(cfg.ENV_PUBLISHED_ROOT)
    previous_global = cfg.GLOBAL_PUBLISHED_ROOT
    os.environ[cfg.ENV_PUBLISHED_ROOT] = str(tmp_path / "branch")
    cfg.GLOBAL_PUBLISHED_ROOT = tmp_path / "global"  # type: ignore[misc]
    try:
        path = find_published_manifest("never_published_model")
    finally:
        cfg.GLOBAL_PUBLISHED_ROOT = previous_global  # type: ignore[misc]
        if previous_branch is None:
            _ = os.environ.pop(cfg.ENV_PUBLISHED_ROOT, None)
        else:
            os.environ[cfg.ENV_PUBLISHED_ROOT] = previous_branch
    assert path is None


def test_package_versions_subset_includes_pydantic() -> None:
    versions = package_versions()
    assert "pydantic" in versions
