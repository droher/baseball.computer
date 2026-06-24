"""dl_embedding_artifact aggregation helper round-trip."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest

from python_models.ml.features import FeatureLayout, Vocabulary
from python_models.statistical import config as cfg
from python_models.statistical.deep import targets as _targets  # noqa: F401
from python_models.statistical.deep.embeddings import (
    aggregate_embedding_frames,
    assemble_embeddings_frame,
)
from python_models.statistical.deep.registry import get_target
from python_models.statistical.manifests import (
    package_versions,
    write_manifest,
    write_published_pointer,
)
from python_models.statistical.schemas import ArtifactManifest, PublishedPointer


def _layout() -> FeatureLayout:
    return FeatureLayout(
        high_card_columns=("batter_id",),
        low_card_columns=(),
        numeric_columns=(),
        grain_column="event_key",
        split_column="primary_fold",
    )


def _matrices() -> dict[str, np.ndarray]:
    return {"batter_id": np.array([[0.0, 0.0], [0.1, 0.2]], dtype=np.float64)}


def _vocabs() -> dict[str, Vocabulary]:
    return {"batter_id": Vocabulary(column="batter_id", values=("alice",))}


def _write_embedding_artifact(
    deep_root: Path,
    *,
    target_name: str,
    artifact_id: str,
) -> Path:
    artifact_dir = deep_root / target_name / artifact_id
    exports = artifact_dir / "exports"
    exports.mkdir(parents=True, exist_ok=True)
    df = assemble_embeddings_frame(
        layout=_layout(),
        embedding_matrices=_matrices(),
        vocabularies=_vocabs(),
    )
    embeddings_path = exports / "embeddings.parquet"
    df.write_parquet(embeddings_path)

    manifest_path = artifact_dir / "manifest.json"
    write_manifest(
        ArtifactManifest(
            artifact_id=artifact_id,
            kind="deep",
            name=target_name,
            version="0.2.0",
            created_at=datetime.now(tz=timezone.utc),
            source_snapshot_id="src-emb-1",
            query_hash="aa1",
            dataset_artifact_id="ds-1",
            output_paths={
                "artifact_dir": artifact_dir,
                "embeddings": embeddings_path,
            },
            package_versions=package_versions(),
        ),
        manifest_path,
    )
    return manifest_path


def test_empty_when_no_embeddings_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path / "deep")
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(tmp_path / "published"))

    frames = list(aggregate_embedding_frames())
    assert len(frames) == 1
    only = frames[0]
    assert only.height == 0
    assert {"entity_type", "entity_id", "embedding_value"} <= set(only.columns)


def test_aggregates_published_embeddings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    deep_root = tmp_path / "deep"
    published_root = tmp_path / "published"
    monkeypatch.setattr(cfg, "DEEP_ROOT", deep_root)
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))

    target = "geometry_trajectory"
    spec = get_target(target)
    artifact_id = "aid-emb-1"
    manifest_path = _write_embedding_artifact(
        deep_root, target_name=target, artifact_id=artifact_id
    )
    _ = write_published_pointer(
        PublishedPointer(
            model_name=spec.published_manifest_name(),
            artifact_id=artifact_id,
            published_at=datetime.now(tz=timezone.utc),
            manifest_path=manifest_path,
        ),
        root=published_root,
    )

    frames = list(aggregate_embedding_frames())
    assert len(frames) == 1
    df = frames[0]
    assert df.height == 2
    assert df["dl_artifact_id"].unique().to_list() == [artifact_id]
    assert df["source_target"].unique().to_list() == [target]
    assert set(df["entity_type"].unique().to_list()) == {"batter_id"}
