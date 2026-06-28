"""Tests for the bayes assist-count manifest-ingest helper.

The SQLMesh ``@model`` file imports the project dialect (``UINTEGER`` etc.)
which stock sqlglot can't parse without a SQLMesh context, so we exercise
the underlying ``aggregate_assist_count_frames`` helper directly, mirroring
``test_state_transition_manifest_ingest``.
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
    ASSIST_COUNT_SUMMARY_SCHEMA,
    aggregate_assist_count_frames,
    empty_assist_count_frame,
)

MODEL_NAME = "assist_count"
DIMENSION = "assist_count"
RESULT_FAMILIES = ("out_in_play", "single")
BASE_STATES = (0, 1, 3)
OUTS = (0, 2)
COUNT_CLASSES = ("1", "2", "3", "4")


def _write_assist_count_artifact_with_pointer(
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
    base_state: list[int] = []
    outs: list[int] = []
    count_class: list[str] = []
    for rf in RESULT_FAMILIES:
        for b in BASE_STATES:
            for o in OUTS:
                for c in COUNT_CLASSES:
                    result_family.append(rf)
                    base_state.append(b)
                    outs.append(o)
                    count_class.append(c)
    n_row = len(result_family)
    rng = np.random.default_rng(0)
    prob_mean = rng.uniform(0.01, 0.4, size=n_row).astype(np.float64)
    prob_sd = rng.uniform(0.001, 0.02, size=n_row).astype(np.float64)
    pl.DataFrame(
        {
            "result_family": pl.Series(
                "result_family", result_family, dtype=pl.Utf8
            ),
            "base_state_start": pl.Series(
                "base_state_start", base_state, dtype=pl.Int8
            ),
            "outs_start": pl.Series("outs_start", outs, dtype=pl.Int8),
            "assist_count_class": pl.Series(
                "assist_count_class", count_class, dtype=pl.Utf8
            ),
            "prob_mean": pl.Series("prob_mean", prob_mean, dtype=pl.Float64),
            "prob_sd": pl.Series("prob_sd", prob_sd, dtype=pl.Float64),
            "prob_hdi_lower": pl.Series(
                "prob_hdi_lower", prob_mean - 2 * prob_sd, dtype=pl.Float64
            ),
            "prob_hdi_upper": pl.Series(
                "prob_hdi_upper", prob_mean + 2 * prob_sd, dtype=pl.Float64
            ),
        }
    ).write_parquet(exports_dir / "assist_count_summary.parquet")

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
            "assist_count": exports_dir / "assist_count_summary.parquet"
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
    frames = list(aggregate_assist_count_frames())
    assert len(frames) == 1
    frame = frames[0]
    assert frame.height == 0
    assert dict(frame.schema) == dict(empty_assist_count_frame().schema)
    assert dict(frame.schema) == ASSIST_COUNT_SUMMARY_SCHEMA


def test_yields_published_frame_with_artifact_id_and_unique_grain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    published_root = _write_assist_count_artifact_with_pointer(
        tmp_path=tmp_path, artifact_id="ac-1"
    )
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    frames = list(aggregate_assist_count_frames())
    assert len(frames) == 1
    frame = frames[0]
    assert frame.height == (
        len(RESULT_FAMILIES) * len(BASE_STATES) * len(OUTS) * len(COUNT_CLASSES)
    )
    assert dict(frame.schema) == ASSIST_COUNT_SUMMARY_SCHEMA
    assert set(frame.get_column("artifact_id").unique().to_list()) == {"ac-1"}
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
    assert set(frame.get_column("method").unique().to_list()) == {
        "hierarchical_bayes_softmax"
    }
    grain = frame.select(
        "result_family", "base_state_start", "outs_start", "assist_count_class"
    )
    assert grain.n_unique() == frame.height
