"""Tests for the bayes pitch-summary manifest-ingest helper (Model J).

The SQLMesh ``@model`` file imports the project dialect (``UINTEGER`` etc.)
which stock sqlglot can't parse without a SQLMesh context, so we exercise
the underlying ``aggregate_pitch_summary_frames`` helper directly, mirroring
``test_run_expectancy_manifest_ingest``.
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
    PITCH_SUMMARY_SUMMARY_SCHEMA,
    aggregate_pitch_summary_frames,
    empty_pitch_summary_frame,
)

MODEL_NAME = "pitch_summary"
DIMENSION = "pitch_summary"
OUTCOME = "final_count"
RESULT_FAMILIES = ("out_in_play", "strikeout")
SEASONS = (2021, 2022)
LEAGUE = "AL"
CLASS_LABELS = tuple(f"b{b}_s{s}" for b in range(4) for s in range(3))


def _write_pitch_summary_artifact_with_pointer(
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

    result_family: list[str] = []
    season: list[int] = []
    final_count_class: list[str] = []
    balls: list[int] = []
    strikes: list[int] = []
    for family in RESULT_FAMILIES:
        for s in SEASONS:
            for label in CLASS_LABELS:
                result_family.append(family)
                season.append(s)
                final_count_class.append(label)
                balls.append(int(label.split("_")[0][1:]))
                strikes.append(int(label.split("_")[1][1:]))
    n_row = len(result_family)
    rng = np.random.default_rng(0)
    prob_mean = rng.uniform(0.01, 0.2, size=n_row).astype(np.float64)
    prob_sd = rng.uniform(0.001, 0.02, size=n_row).astype(np.float64)
    pl.DataFrame(
        {
            "result_family": pl.Series("result_family", result_family, dtype=pl.Utf8),
            "season": pl.Series("season", season, dtype=pl.Int16),
            "league": pl.Series("league", [LEAGUE] * n_row, dtype=pl.Utf8),
            "final_count_class": pl.Series(
                "final_count_class", final_count_class, dtype=pl.Utf8
            ),
            "balls": pl.Series("balls", balls, dtype=pl.Int8),
            "strikes": pl.Series("strikes", strikes, dtype=pl.Int8),
            "outcome": pl.Series("outcome", [OUTCOME] * n_row, dtype=pl.Utf8),
            "prob_mean": pl.Series("prob_mean", prob_mean, dtype=pl.Float64),
            "prob_sd": pl.Series("prob_sd", prob_sd, dtype=pl.Float64),
            "prob_hdi_lower": pl.Series(
                "prob_hdi_lower", prob_mean - 2 * prob_sd, dtype=pl.Float64
            ),
            "prob_hdi_upper": pl.Series(
                "prob_hdi_upper", prob_mean + 2 * prob_sd, dtype=pl.Float64
            ),
            "ess_bulk": pl.Series(
                "ess_bulk", rng.uniform(400.0, 900.0, size=n_row), dtype=pl.Float64
            ),
            "rhat": pl.Series(
                "rhat", rng.uniform(1.0, 1.02, size=n_row), dtype=pl.Float64
            ),
        }
    ).write_parquet(exports_dir / "pitch_summary_summary.parquet")

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
        output_paths={"pitch_summary": exports_dir / "pitch_summary_summary.parquet"},
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
    frames = list(aggregate_pitch_summary_frames())
    assert len(frames) == 1
    frame = frames[0]
    assert frame.height == 0
    assert dict(frame.schema) == dict(empty_pitch_summary_frame().schema)
    assert dict(frame.schema) == PITCH_SUMMARY_SUMMARY_SCHEMA


def test_yields_published_frame_with_artifact_id_and_unique_grain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    published_root = _write_pitch_summary_artifact_with_pointer(
        tmp_path=tmp_path, artifact_id="ps-1"
    )
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    frames = list(aggregate_pitch_summary_frames())
    assert len(frames) == 1
    frame = frames[0]
    assert frame.height == len(RESULT_FAMILIES) * len(SEASONS) * len(CLASS_LABELS)
    assert dict(frame.schema) == PITCH_SUMMARY_SUMMARY_SCHEMA
    assert set(frame.get_column("bayes_artifact_id").unique().to_list()) == {"ps-1"}
    grain = frame.select("result_family", "season", "league", "final_count_class")
    assert grain.n_unique() == frame.height
