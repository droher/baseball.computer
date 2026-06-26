"""Tests for the bayes fielding-credit manifest-ingest helper.

The SQLMesh ``@model`` file imports the project dialect (``UINTEGER`` etc.)
which stock sqlglot can't parse without a SQLMesh context, so we exercise
the underlying ``aggregate_fielding_credit_frames`` helper directly,
mirroring ``test_scorer_observation_propensities``.
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
    CREDIT_SHARE_SCHEMA,
    aggregate_fielding_credit_frames,
)


def _write_credit_artifact_with_pointer(
    *,
    tmp_path: Path,
    artifact_id: str,
    events: int,
    n_positions: int = 9,
    credit_type: str = "putout",
    model_name: str = "putout_credit_allocation",
) -> tuple[Path, Path]:
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
    artifact_dir = bayes_artifact_dir(model_name, artifact_id, root=bayes_root)
    exports_dir = artifact_dir / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)

    n_rows = events * n_positions
    event_keys = np.repeat(np.arange(events, dtype=np.int64), n_positions)
    positions = np.tile(np.arange(1, n_positions + 1, dtype=np.int8), events)
    rng = np.random.default_rng(0)
    raw = rng.uniform(0.0, 1.0, size=(events, n_positions))
    shares = (raw / raw.sum(axis=1, keepdims=True)).reshape(-1).astype(np.float64)

    df = pl.DataFrame(
        {
            "event_key": event_keys,
            "fielding_position": positions,
            "credit_type": [credit_type] * n_rows,
            "expected_share": shares,
        }
    )
    df.write_parquet(exports_dir / "event_credit.parquet")

    extras = BayesArtifactExtras(
        model_name=model_name,
        model_version="0.3.0",
        dimension=credit_type,
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
        name=model_name,
        version="0.3.0",
        created_at=dt.datetime.now(tz=dt.timezone.utc),
        source_snapshot_id="dev-test",
        output_paths={"event_credit": exports_dir / "event_credit.parquet"},
        package_versions={},
        bayes_extras=extras,
    )
    manifest_path = artifact_dir / "manifest.json"
    write_manifest(manifest, manifest_path)
    _ = write_published_pointer(
        PublishedPointer(
            model_name=model_name,
            artifact_id=artifact_id,
            published_at=dt.datetime.now(tz=dt.timezone.utc),
            manifest_path=manifest_path,
        ),
        root=published_root,
    )
    return manifest_path, published_root


def test_credit_yields_empty_typed_frame_when_no_targets_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(tmp_path / "empty"))
    frames = list(aggregate_fielding_credit_frames())
    assert len(frames) == 1
    frame = frames[0]
    assert frame.height == 0
    assert dict(frame.schema) == CREDIT_SHARE_SCHEMA


def test_credit_yields_per_target_frame_with_artifact_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, published_root = _write_credit_artifact_with_pointer(
        tmp_path=tmp_path, artifact_id="credit-po-1", events=4
    )
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    frames = list(aggregate_fielding_credit_frames())
    assert len(frames) == 1
    frame = frames[0]
    assert frame.height == 4 * 9
    assert dict(frame.schema) == CREDIT_SHARE_SCHEMA
    assert set(frame.get_column("artifact_id").unique().to_list()) == {
        "credit-po-1"
    }
    for _contract_col in (
        "model_name",
        "model_version",
        "source_snapshot_id",
        "method",
        "observed_status",
        "confidence_status",
        "weak_identification_flag",
    ):
        assert frame.get_column(_contract_col).null_count() == 0
    assert set(frame.get_column("observed_status").unique().to_list()) == {"estimated"}
    assert set(frame.get_column("credit_type").unique().to_list()) == {"putout"}
