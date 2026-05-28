"""Tests for the bayes responsibility manifest-ingest helper (Model I).

The SQLMesh ``@model`` file imports the project dialect (``UINTEGER`` etc.)
which stock sqlglot can't parse without a SQLMesh context, so we exercise
the underlying ``aggregate_responsibility_frames`` helper directly, mirroring
``test_advancement_manifest_ingest``.
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
    RESPONSIBILITY_SCHEMA,
    aggregate_responsibility_frames,
    empty_responsibility_frame,
)
from python_models.statistical.models._responsibility_data import (
    RESPONSIBILITY_POSITION_LABELS,
)

MODEL_NAME = "responsibility"
DIMENSION = "responsibility"
EVENT_KEYS = (101, 102, 103)


def _write_responsibility_artifact_with_pointer(
    *,
    tmp_path: Path,
    artifact_id: str,
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

    positions = [int(p) for p in RESPONSIBILITY_POSITION_LABELS]
    event_key: list[int] = []
    fielding_position: list[int] = []
    for ek in EVENT_KEYS:
        for pos in positions:
            event_key.append(ek)
            fielding_position.append(pos)
    n_row = len(event_key)
    rng = np.random.default_rng(0)
    shares = rng.uniform(0.05, 0.3, size=n_row).astype(np.float64)
    pl.DataFrame(
        {
            "event_key": pl.Series("event_key", event_key, dtype=pl.Int64),
            "fielding_position": pl.Series(
                "fielding_position", fielding_position, dtype=pl.Int8
            ),
            "expected_share": pl.Series("expected_share", shares, dtype=pl.Float64),
        }
    ).write_parquet(exports_dir / "responsibility_probabilities.parquet")

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
        event_row_count=n_row,
    )
    manifest = ArtifactManifest(
        artifact_id=artifact_id,
        kind="bayes",
        name=MODEL_NAME,
        version="0.3.0",
        created_at=dt.datetime.now(tz=dt.timezone.utc),
        source_snapshot_id="dev-test",
        output_paths={
            "responsibility": exports_dir / "responsibility_probabilities.parquet"
        },
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
    frames = list(aggregate_responsibility_frames())
    assert len(frames) == 1
    frame = frames[0]
    assert frame.height == 0
    assert dict(frame.schema) == dict(empty_responsibility_frame().schema)
    assert dict(frame.schema) == RESPONSIBILITY_SCHEMA


def test_yields_published_frame_with_artifact_id_and_unique_grain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    published_root = _write_responsibility_artifact_with_pointer(
        tmp_path=tmp_path, artifact_id="resp-1"
    )
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    frames = list(aggregate_responsibility_frames())
    assert len(frames) == 1
    frame = frames[0]
    assert frame.height == len(EVENT_KEYS) * len(RESPONSIBILITY_POSITION_LABELS)
    assert dict(frame.schema) == RESPONSIBILITY_SCHEMA
    assert set(frame.get_column("bayes_artifact_id").unique().to_list()) == {"resp-1"}
    grain = frame.select("event_key", "fielding_position")
    assert grain.n_unique() == frame.height
    assert set(frame.get_column("fielding_position").unique().to_list()) == {
        int(p) for p in RESPONSIBILITY_POSITION_LABELS
    }
