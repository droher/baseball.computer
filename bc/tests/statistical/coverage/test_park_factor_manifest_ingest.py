"""Tests for the bayes park-factor manifest-ingest helper (Model F).

The SQLMesh ``@model`` file imports the project dialect (``UINTEGER`` etc.)
which stock sqlglot can't parse without a SQLMesh context, so we exercise
the underlying ``aggregate_park_factor_frames`` helper directly, mirroring
``test_imputed_batted_ball_geometry``.
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
    PARK_FACTOR_SUMMARY_SCHEMA,
    aggregate_park_factor_frames,
    empty_park_factor_frame,
)

MODEL_NAME = "park_factor_runs"
DIMENSION = "park_factors"
OUTCOME = "runs"
PARK_IDS = ("PARK01", "PARK02", "PARK03")
SEASONS = (2021, 2022)
LEAGUE = "NL"


def _write_park_factor_artifact_with_pointer(
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

    park_ids = [p for p in PARK_IDS for _ in SEASONS]
    seasons = [s for _ in PARK_IDS for s in SEASONS]
    n_cell = len(park_ids)
    rng = np.random.default_rng(0)
    theta_mean = rng.normal(0.0, 0.1, size=n_cell).astype(np.float64)
    theta_sd = rng.uniform(0.01, 0.05, size=n_cell).astype(np.float64)
    pl.DataFrame(
        {
            "park_id": pl.Series("park_id", park_ids, dtype=pl.Utf8),
            "season": pl.Series("season", seasons, dtype=pl.Int16),
            "league": pl.Series("league", [LEAGUE] * n_cell, dtype=pl.Utf8),
            "outcome": pl.Series("outcome", [OUTCOME] * n_cell, dtype=pl.Utf8),
            "theta_mean": pl.Series("theta_mean", theta_mean, dtype=pl.Float64),
            "theta_sd": pl.Series("theta_sd", theta_sd, dtype=pl.Float64),
            "theta_hdi_lower": pl.Series(
                "theta_hdi_lower", theta_mean - 2 * theta_sd, dtype=pl.Float64
            ),
            "theta_hdi_upper": pl.Series(
                "theta_hdi_upper", theta_mean + 2 * theta_sd, dtype=pl.Float64
            ),
            "park_factor_mean": pl.Series(
                "park_factor_mean", np.exp(theta_mean), dtype=pl.Float64
            ),
            "ess_bulk": pl.Series(
                "ess_bulk", rng.uniform(400.0, 900.0, size=n_cell), dtype=pl.Float64
            ),
            "rhat": pl.Series(
                "rhat", rng.uniform(1.0, 1.02, size=n_cell), dtype=pl.Float64
            ),
        }
    ).write_parquet(exports_dir / "park_factor_summary.parquet")

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
        event_row_count=n_cell,
    )
    manifest = ArtifactManifest(
        artifact_id=artifact_id,
        kind="bayes",
        name=MODEL_NAME,
        version="0.3.0",
        created_at=dt.datetime.now(tz=dt.timezone.utc),
        source_snapshot_id="dev-test",
        output_paths={"park_factor": exports_dir / "park_factor_summary.parquet"},
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
    frames = list(aggregate_park_factor_frames())
    assert len(frames) == 1
    frame = frames[0]
    assert frame.height == 0
    assert dict(frame.schema) == dict(empty_park_factor_frame().schema)
    assert dict(frame.schema) == PARK_FACTOR_SUMMARY_SCHEMA


def test_yields_published_frame_with_artifact_id_and_unique_grain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    published_root = _write_park_factor_artifact_with_pointer(
        tmp_path=tmp_path, artifact_id="pf-1"
    )
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    frames = list(aggregate_park_factor_frames())
    assert len(frames) == 1
    frame = frames[0]
    assert frame.height == len(PARK_IDS) * len(SEASONS)
    assert dict(frame.schema) == PARK_FACTOR_SUMMARY_SCHEMA
    assert set(frame.get_column("bayes_artifact_id").unique().to_list()) == {"pf-1"}
    grain = frame.select("park_id", "season", "league", "outcome")
    assert grain.n_unique() == frame.height
