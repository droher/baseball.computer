"""Ingestion-helper round-trip for the dl_proposal_manifest @model.

Mirrors ``bc/tests/statistical/test_sqlmesh_ingestion_fixture.py``: writes
a fake artifact + published pointer under a tmp root, calls the regular
helper backing the SQLMesh ``@model``, and asserts the yielded frames
carry the expected per-target rows. The ``@model`` decorator itself is
not exercised — its `execute` body is a thin wrapper that delegates to
``aggregate_proposal_manifest_frames``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from python_models.statistical import config as cfg
from python_models.statistical.deep import targets as _targets  # noqa: F401
from python_models.statistical.deep.manifest_ingest import (
    PROPOSAL_MANIFEST_SCHEMA,
    aggregate_proposal_manifest_frames,
)
from python_models.statistical.deep.registry import get_target
from python_models.statistical.manifests import (
    package_versions,
    write_manifest,
    write_published_pointer,
)
from python_models.statistical.schemas import ArtifactManifest, PublishedPointer


def _write_artifact(
    deep_root: Path,
    *,
    target_name: str,
    artifact_id: str,
    event_keys: list[int],
    dl_p_class: list[list[float]],
) -> Path:
    artifact_dir = deep_root / target_name / artifact_id
    exports = artifact_dir / "exports"
    exports.mkdir(parents=True, exist_ok=True)
    probabilities = exports / "probabilities.parquet"
    pl.DataFrame(
        {
            "event_key": pl.Series(event_keys, dtype=pl.UInt32),
            "partition": pl.Series(["OOF"] * len(event_keys), dtype=pl.Utf8),
            "dl_p_class": pl.Series(
                "dl_p_class", dl_p_class, dtype=pl.List(pl.Float64)
            ),
        }
    ).write_parquet(probabilities)

    manifest_path = artifact_dir / "manifest.json"
    write_manifest(
        ArtifactManifest(
            artifact_id=artifact_id,
            kind="deep",
            name=target_name,
            version="0.2.0",
            created_at=datetime.now(tz=timezone.utc),
            source_snapshot_id="src-ingest-1",
            query_hash="bb1",
            dataset_artifact_id="ds-1",
            output_paths={
                "artifact_dir": artifact_dir,
                "probabilities": probabilities,
            },
            package_versions=package_versions(),
        ),
        manifest_path,
    )
    return manifest_path


def _publish_pointer(
    published_root: Path,
    *,
    target: str,
    artifact_id: str,
    manifest_path: Path,
) -> Path:
    spec = get_target(target)
    pointer = PublishedPointer(
        model_name=spec.published_manifest_name(),
        artifact_id=artifact_id,
        published_at=datetime.now(tz=timezone.utc),
        manifest_path=manifest_path,
    )
    return write_published_pointer(pointer, root=published_root)


def test_empty_when_no_targets_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(tmp_path / "published"))

    frames = list(aggregate_proposal_manifest_frames("dl_proposal_manifest"))
    assert len(frames) == 1
    only = frames[0]
    assert only.height == 0
    assert only.schema == PROPOSAL_MANIFEST_SCHEMA


def test_aggregates_published_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    deep_root = tmp_path / "deep"
    published_root = tmp_path / "published"
    monkeypatch.setattr(cfg, "DEEP_ROOT", deep_root)
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))

    manifest_path = _write_artifact(
        deep_root,
        target_name="geometry_trajectory",
        artifact_id="aid-traj-1",
        event_keys=[10, 11, 12],
        dl_p_class=[[0.7, 0.2, 0.1], [0.4, 0.4, 0.2], [0.1, 0.1, 0.8]],
    )
    _ = _publish_pointer(
        published_root,
        target="geometry_trajectory",
        artifact_id="aid-traj-1",
        manifest_path=manifest_path,
    )

    frames = list(aggregate_proposal_manifest_frames("dl_proposal_manifest"))
    assert len(frames) == 1
    df = frames[0]
    assert df.height == 3
    assert set(df.columns) == {"event_key", "dimension", "dl_artifact_id", "dl_p_class"}
    assert df["dimension"].unique().to_list() == ["trajectory"]
    assert df["dl_artifact_id"].unique().to_list() == ["aid-traj-1"]


def test_skips_target_with_no_published_pointer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    deep_root = tmp_path / "deep"
    published_root = tmp_path / "published"
    monkeypatch.setattr(cfg, "DEEP_ROOT", deep_root)
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))

    manifest_path = _write_artifact(
        deep_root,
        target_name="geometry_location_edge",
        artifact_id="aid-edge-1",
        event_keys=[100, 101],
        dl_p_class=[[0.5, 0.5], [0.2, 0.8]],
    )
    _ = _publish_pointer(
        published_root,
        target="geometry_location_edge",
        artifact_id="aid-edge-1",
        manifest_path=manifest_path,
    )

    frames = list(aggregate_proposal_manifest_frames("dl_proposal_manifest"))
    assert len(frames) == 1
    df = frames[0]
    assert df["dimension"].unique().to_list() == ["location_edge"]
    assert df.height == 2


def test_credit_manifest_yields_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(tmp_path / "published"))

    frames = list(aggregate_proposal_manifest_frames("dl_credit_proposal_manifest"))
    assert len(frames) == 1
    assert frames[0].height == 0
