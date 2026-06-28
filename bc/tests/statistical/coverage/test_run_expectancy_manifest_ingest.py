"""Tests for the bayes run-expectancy manifest-ingest helper (Model G).

The SQLMesh ``@model`` file imports the project dialect (``UINTEGER`` etc.)
which stock sqlglot can't parse without a SQLMesh context, so we exercise
the underlying ``aggregate_run_expectancy_frames`` helper directly, mirroring
``test_park_factor_manifest_ingest``.
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
    RUN_EXPECTANCY_SUMMARY_SCHEMA,
    aggregate_run_expectancy_frames,
    empty_run_expectancy_frame,
)

MODEL_NAME = "run_expectancy"
DIMENSION = "run_values"
OUTCOME = "runs_to_end"
SEASONS = (2021, 2022)
LEAGUE = "NL"
STATES = ("0_0", "1_3", "2_7")


def _write_run_expectancy_artifact_with_pointer(
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

    states = [s for s in STATES for _ in SEASONS]
    seasons = [season for _ in STATES for season in SEASONS]
    n_cell = len(states)
    outs = [int(s.split("_")[0]) for s in states]
    base = [int(s.split("_")[1]) for s in states]
    rng = np.random.default_rng(0)
    re_mean = rng.uniform(0.1, 2.0, size=n_cell).astype(np.float64)
    re_sd = rng.uniform(0.01, 0.1, size=n_cell).astype(np.float64)
    pl.DataFrame(
        {
            "state": pl.Series("state", states, dtype=pl.Utf8),
            "base_state": pl.Series("base_state", base, dtype=pl.Int8),
            "outs": pl.Series("outs", outs, dtype=pl.Int8),
            "season": pl.Series("season", seasons, dtype=pl.Int16),
            "league": pl.Series("league", [LEAGUE] * n_cell, dtype=pl.Utf8),
            "outcome": pl.Series("outcome", [OUTCOME] * n_cell, dtype=pl.Utf8),
            "re_value_mean": pl.Series("re_value_mean", re_mean, dtype=pl.Float64),
            "re_value_sd": pl.Series("re_value_sd", re_sd, dtype=pl.Float64),
            "re_value_hdi_lower": pl.Series(
                "re_value_hdi_lower", re_mean - 2 * re_sd, dtype=pl.Float64
            ),
            "re_value_hdi_upper": pl.Series(
                "re_value_hdi_upper", re_mean + 2 * re_sd, dtype=pl.Float64
            ),
            "ess_bulk": pl.Series(
                "ess_bulk", rng.uniform(400.0, 900.0, size=n_cell), dtype=pl.Float64
            ),
            "rhat": pl.Series(
                "rhat", rng.uniform(1.0, 1.02, size=n_cell), dtype=pl.Float64
            ),
        }
    ).write_parquet(exports_dir / "run_expectancy_summary.parquet")

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
        output_paths={"run_expectancy": exports_dir / "run_expectancy_summary.parquet"},
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
    frames = list(aggregate_run_expectancy_frames())
    assert len(frames) == 1
    frame = frames[0]
    assert frame.height == 0
    assert dict(frame.schema) == dict(empty_run_expectancy_frame().schema)
    assert dict(frame.schema) == RUN_EXPECTANCY_SUMMARY_SCHEMA


def test_yields_published_frame_with_artifact_id_and_unique_grain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    published_root = _write_run_expectancy_artifact_with_pointer(
        tmp_path=tmp_path, artifact_id="re-1"
    )
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    frames = list(aggregate_run_expectancy_frames())
    assert len(frames) == 1
    frame = frames[0]
    assert frame.height == len(STATES) * len(SEASONS)
    assert dict(frame.schema) == RUN_EXPECTANCY_SUMMARY_SCHEMA
    assert set(frame.get_column("artifact_id").unique().to_list()) == {"re-1"}
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
    grain = frame.select("state", "season", "league", "outcome")
    assert grain.n_unique() == frame.height
