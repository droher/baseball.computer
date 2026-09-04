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
    METHOD_HIERARCHICAL_BAYES_SOFTMAX,
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
    share_scale: float = 1.0,
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
    raw = rng.uniform(0.01, 0.2, size=(len(RESULT_FAMILIES) * len(SEASONS), len(CLASS_LABELS)))
    prob_mean = (share_scale * raw / raw.sum(axis=1, keepdims=True)).reshape(-1)
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
    assert set(frame.get_column("artifact_id").unique().to_list()) == {"ps-1"}
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
    assert set(frame.get_column("method").unique().to_list()) == {METHOD_HIERARCHICAL_BAYES_SOFTMAX}
    grain = frame.select("result_family", "season", "league", "final_count_class")
    assert grain.n_unique() == frame.height
    per_grain = frame.group_by("result_family", "season", "league").agg(
        pl.col("prob_mean").sum().alias("total")
    )
    np.testing.assert_allclose(per_grain.get_column("total").to_numpy(), 1.0, atol=1e-9)


def test_pointer_with_missing_export_raises_instead_of_emptying(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    published_root = _write_pitch_summary_artifact_with_pointer(
        tmp_path=tmp_path, artifact_id="ps-missing"
    )
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    from python_models.statistical.bayes.artifacts import bayes_artifact_dir

    export_path = (
        bayes_artifact_dir("pitch_summary", "ps-missing", root=tmp_path / "bayes")
        / "exports"
        / "pitch_summary_summary.parquet"
    )
    export_path.unlink()

    with pytest.raises(FileNotFoundError, match=str(export_path)):
        _ = list(aggregate_pitch_summary_frames())


def test_pointer_resolving_to_another_models_manifest_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from python_models.statistical.bayes.artifacts import bayes_artifact_dir
    from python_models.statistical.manifests import read_manifest, write_manifest

    published_root = _write_pitch_summary_artifact_with_pointer(
        tmp_path=tmp_path, artifact_id="ps-wrong-model"
    )
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    manifest_path = (
        bayes_artifact_dir("pitch_summary", "ps-wrong-model", root=tmp_path / "bayes")
        / "manifest.json"
    )
    manifest = read_manifest(manifest_path)
    assert manifest.bayes_extras is not None
    write_manifest(
        manifest.model_copy(
            update={
                "bayes_extras": manifest.bayes_extras.model_copy(
                    update={"model_name": "run_expectancy"}
                )
            }
        ),
        manifest_path,
    )

    with pytest.raises(ValueError, match="run_expectancy"):
        _ = list(aggregate_pitch_summary_frames())


def test_relative_pointer_resolves_through_ingest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from python_models.statistical.bayes.artifacts import bayes_artifact_dir
    from python_models.statistical.manifests import write_published_pointer
    from python_models.statistical.schemas import PublishedPointer

    published_root = _write_pitch_summary_artifact_with_pointer(
        tmp_path=tmp_path, artifact_id="ps-relative"
    )
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    monkeypatch.setenv(cfg.ENV_ARTIFACTS_ROOT, str(tmp_path))
    manifest_path = (
        bayes_artifact_dir("pitch_summary", "ps-relative", root=tmp_path / "bayes")
        / "manifest.json"
    )
    pointer_path = write_published_pointer(
        PublishedPointer(
            model_name=MODEL_NAME,
            artifact_id="ps-relative",
            published_at=dt.datetime.now(tz=dt.timezone.utc),
            manifest_path=manifest_path,
        ),
        root=published_root,
        relative=True,
    )
    raw = PublishedPointer.model_validate_json(pointer_path.read_text(encoding="utf-8"))
    assert not raw.manifest_path.is_absolute()

    frames = list(aggregate_pitch_summary_frames())
    assert len(frames) == 1
    assert frames[0].height == len(RESULT_FAMILIES) * len(SEASONS) * len(CLASS_LABELS)
    assert set(frames[0].get_column("artifact_id").unique().to_list()) == {"ps-relative"}


def test_export_whose_grains_do_not_sum_to_one_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    published_root = _write_pitch_summary_artifact_with_pointer(
        tmp_path=tmp_path, artifact_id="ps-bad", share_scale=1.1
    )
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    with pytest.raises(ValueError, match="do not sum to 1"):
        _ = list(aggregate_pitch_summary_frames())
