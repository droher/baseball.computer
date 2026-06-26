"""Tests for the Phase-5 estimated-metadata contract + publication tiers.

``stamp_estimated_contract`` must derive every contract column from the
manifest (config-driven), and ``tier_for`` must classify each published
model from the registry.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import datetime as dt

import polars as pl
import pytest

from python_models.statistical.bayes.manifest_ingest import (
    ESTIMATED_CONTRACT_COLUMNS,
    ESTIMATED_CONTRACT_SCHEMA,
    stamp_estimated_contract,
)
from python_models.statistical.publication_tiers import (
    PUBLICATION_TIERS,
    PublicationTier,
    estimated_model_names,
    tier_for,
)
from python_models.statistical.schemas import (
    ArtifactManifest,
    BayesArtifactExtras,
    BayesDiagnosticsSummary,
    BayesPriorConfig,
    BayesSamplerConfig,
)


def _synthetic_manifest(
    *,
    artifact_id: str = "stamp-1",
    model_name: str = "park_factor_runs",
    model_version: str = "0.7.0",
    source_snapshot_id: str = "snap-xyz",
    validation_status: str = "passed",
    weak_identification_flag: bool = True,
) -> ArtifactManifest:
    extras = BayesArtifactExtras(
        model_name=model_name,
        model_version=model_version,
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
        weak_identification_flag=weak_identification_flag,
    )
    return ArtifactManifest(
        artifact_id=artifact_id,
        kind="bayes",
        name=model_name,
        version=model_version,
        created_at=dt.datetime.now(tz=dt.timezone.utc),
        source_snapshot_id=source_snapshot_id,
        output_paths={},
        package_versions={},
        validation_status=validation_status,  # type: ignore[arg-type]
        bayes_extras=extras,
    )


def test_stamp_adds_all_eight_contract_columns_from_manifest() -> None:
    base = pl.DataFrame({"event_key": pl.Series("event_key", [1, 2], dtype=pl.UInt32)})
    manifest = _synthetic_manifest()

    out = stamp_estimated_contract(
        base, manifest, method="hierarchical_bayes_nb"
    )

    for column in ESTIMATED_CONTRACT_COLUMNS:
        assert column in out.columns
    assert out.height == base.height

    contract = out.select(list(ESTIMATED_CONTRACT_COLUMNS))
    for name, dtype in ESTIMATED_CONTRACT_SCHEMA.items():
        assert contract.schema[name] == dtype

    assert out.get_column("artifact_id").to_list() == [manifest.artifact_id] * 2
    assert (
        out.get_column("model_name").to_list()
        == [manifest.bayes_extras.model_name] * 2
    )
    assert (
        out.get_column("model_version").to_list()
        == [manifest.bayes_extras.model_version] * 2
    )
    assert (
        out.get_column("source_snapshot_id").to_list()
        == [manifest.source_snapshot_id] * 2
    )
    assert out.get_column("method").to_list() == ["hierarchical_bayes_nb"] * 2
    assert out.get_column("observed_status").to_list() == ["estimated"] * 2
    assert (
        out.get_column("confidence_status").to_list()
        == [str(manifest.validation_status)] * 2
    )
    assert (
        out.get_column("weak_identification_flag").to_list()
        == [manifest.bayes_extras.weak_identification_flag] * 2
    )


def test_stamp_confidence_status_tracks_validation_status() -> None:
    base = pl.DataFrame({"x": pl.Series("x", [1], dtype=pl.UInt32)})
    for status in ("exploratory", "passed", "failed"):
        manifest = _synthetic_manifest(validation_status=status)
        out = stamp_estimated_contract(
            base, manifest, method="hierarchical_logistic"
        )
        assert out.get_column("confidence_status").to_list() == [status]


def test_stamp_requires_bayes_extras() -> None:
    base = pl.DataFrame({"x": pl.Series("x", [1], dtype=pl.UInt32)})
    manifest = ArtifactManifest(
        artifact_id="no-extras",
        kind="dataset",
        name="dataset",
        version="0.1.0",
        created_at=dt.datetime.now(tz=dt.timezone.utc),
        source_snapshot_id="snap",
        output_paths={},
        package_versions={},
    )
    with pytest.raises(ValueError, match="no bayes_extras"):
        _ = stamp_estimated_contract(base, manifest, method="x")


def test_tier_for_coverage_table_is_estimated() -> None:
    for name in estimated_model_names():
        assert tier_for(name) is PublicationTier.ESTIMATED
        assert tier_for(f"main_models.{name}") is PublicationTier.ESTIMATED


def test_tier_for_legacy_surface_is_deterministic() -> None:
    for name in ("linear_weights", "park_factors", "run_expectancy_matrix"):
        assert tier_for(name) is PublicationTier.DETERMINISTIC


def test_estimated_registry_covers_the_nine_coverage_tables() -> None:
    estimated = {
        name
        for name, tier in PUBLICATION_TIERS.items()
        if tier is PublicationTier.ESTIMATED
    }
    assert estimated == set(estimated_model_names())
    assert len(estimated) == 9
