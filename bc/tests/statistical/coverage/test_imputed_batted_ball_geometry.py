"""Tests for the bayes geometry manifest-ingest helper (Model E).

The SQLMesh ``@model`` file imports the project dialect (``UINTEGER`` etc.)
which stock sqlglot can't parse without a SQLMesh context, so we exercise
the underlying ``aggregate_geometry_frames`` helper directly, mirroring
``test_imputed_ball_handler_probabilities``.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import datetime as dt
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical import config as cfg
from python_models.statistical.bayes import targets as _targets  # noqa: F401  # pyright: ignore[reportUnusedImport]
from python_models.statistical.bayes.manifest_ingest import (
    GEOMETRY_SCHEMA,
    aggregate_geometry_frames,
    iterate_published_ball_handler_frames,
    iterate_published_credit_frames,
)

MODEL_NAME = "geometry_trajectory"
DIMENSION = "trajectory"
CLASS_LABELS = ("Fly", "GroundBall", "LineDrive", "PopUp", "Bunt")
N_CLASSES = len(CLASS_LABELS)


def _write_geometry_artifact_with_pointer(
    *,
    tmp_path: Path,
    artifact_id: str,
    events: int,
) -> Path:
    from python_models.statistical.bayes.artifacts import bayes_artifact_dir
    from python_models.statistical.manifests import (
        write_manifest,
        write_published_pointer,
    )
    from python_models.statistical.schemas import (
        ArtifactManifest,
        BayesArtifactExtras,
        BayesDiagnosticsSummary,
        BayesPriorConfig,
        BayesSamplerConfig,
        PublishedPointer,
    )

    bayes_root = tmp_path / "bayes"
    published_root = tmp_path / "published"
    artifact_dir = bayes_artifact_dir(MODEL_NAME, artifact_id, root=bayes_root)
    exports_dir = artifact_dir / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)

    event_keys = np.repeat(np.arange(events, dtype=np.int64), N_CLASSES)
    class_indices = np.tile(np.arange(N_CLASSES, dtype=np.int8), events)
    labels = list(CLASS_LABELS) * events
    rng = np.random.default_rng(0)
    raw = rng.uniform(0.0, 1.0, size=(events, N_CLASSES))
    shares = (raw / raw.sum(axis=1, keepdims=True)).reshape(-1).astype(np.float64)
    pl.DataFrame(
        {
            "event_key": event_keys,
            "class_index": class_indices,
            "class_label": labels,
            "expected_share": shares,
        }
    ).write_parquet(exports_dir / "geometry_probabilities.parquet")

    extras = BayesArtifactExtras(
        model_name=MODEL_NAME,
        model_version="0.3.0",
        dimension=DIMENSION,
        prior_config=BayesPriorConfig(),
        sampler_config=BayesSamplerConfig(
            draws=10,
            tune=10,
            chains=1,
            target_accept=0.8,
            random_seed=0,
            backend="numpyro",
        ),
        diagnostics_summary=BayesDiagnosticsSummary(
            rhat_max=1.01,
            ess_bulk_min=500.0,
            ess_tail_min=400.0,
            divergences=0,
            total_draws=10,
        ),
        source_effect_active=True,
        event_row_count=events,
    )
    manifest = ArtifactManifest(
        artifact_id=artifact_id,
        kind="bayes",
        name=MODEL_NAME,
        version="0.3.0",
        created_at=dt.datetime.now(tz=dt.timezone.utc),
        source_snapshot_id="dev-test",
        output_paths={"geometry": exports_dir / "geometry_probabilities.parquet"},
        package_versions={},
        bayes_extras=extras,
    )
    manifest_path = artifact_dir / "manifest.json"
    write_manifest(manifest, manifest_path)
    _ = write_published_pointer(
        PublishedPointer(
            model_name=MODEL_NAME,
            artifact_id=artifact_id,
            published_at=dt.datetime.now(tz=dt.timezone.utc),
            manifest_path=manifest_path,
        ),
        root=published_root,
    )
    return published_root


def test_yields_empty_typed_frame_when_no_targets_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(tmp_path / "empty"))
    frames = list(aggregate_geometry_frames())
    assert len(frames) == 1
    frame = frames[0]
    assert frame.height == 0
    assert dict(frame.schema) == GEOMETRY_SCHEMA


def test_yields_per_target_frame_with_dimension_and_artifact_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = 5
    published_root = _write_geometry_artifact_with_pointer(
        tmp_path=tmp_path, artifact_id="geo-1", events=events
    )
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    frames = list(aggregate_geometry_frames())
    assert len(frames) == 1
    frame = frames[0]
    assert frame.height == events * N_CLASSES
    assert dict(frame.schema) == GEOMETRY_SCHEMA
    assert set(frame.get_column("geometry_dimension").unique().to_list()) == {DIMENSION}
    assert set(frame.get_column("bayes_artifact_id").unique().to_list()) == {"geo-1"}
    assert set(frame.get_column("class_label").unique().to_list()) == set(CLASS_LABELS)
    per_event = frame.group_by("event_key", "geometry_dimension").agg(
        pl.col("expected_share").sum().alias("total")
    )
    np.testing.assert_allclose(per_event.get_column("total").to_numpy(), 1.0, atol=1e-9)


def test_credit_and_ball_handler_iterators_exclude_geometry_spec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    published_root = _write_geometry_artifact_with_pointer(
        tmp_path=tmp_path, artifact_id="geo-2", events=3
    )
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    assert list(iterate_published_credit_frames()) == [], (
        "the credit iterator must not slurp the published geometry export"
    )
    assert list(iterate_published_ball_handler_frames()) == [], (
        "the ball-handler iterator must not slurp the published geometry export"
    )
    geometry_frames = list(aggregate_geometry_frames())
    assert geometry_frames[0].height == 3 * N_CLASSES
