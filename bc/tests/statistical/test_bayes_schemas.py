"""Pydantic round-trip + kind/extras invariant for Bayes manifest schema."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from python_models.statistical.schemas import (
    ArtifactManifest,
    BayesArtifactExtras,
    BayesDiagnosticsSummary,
    BayesPosteriorRow,
    BayesPosteriorSummary,
    BayesPriorConfig,
    BayesSamplerConfig,
)


def _make_extras() -> BayesArtifactExtras:
    return BayesArtifactExtras(
        model_name="trajectory_observedness",
        model_version="0.1.0",
        dimension="trajectory",
        gamma_dl_flavor="gamma_dl_zero",
        prior_config=BayesPriorConfig(),
        sampler_config=BayesSamplerConfig(
            draws=50,
            tune=50,
            chains=2,
            target_accept=0.8,
            random_seed=20260513,
            is_smoke=True,
        ),
        posterior_summary=BayesPosteriorSummary(
            rows=(
                BayesPosteriorRow(
                    variable="alpha",
                    mean=0.1,
                    sd=0.2,
                    hdi_lower=-0.3,
                    hdi_upper=0.5,
                    ess_bulk=120.0,
                    ess_tail=110.0,
                    rhat=1.01,
                ),
            )
        ),
        diagnostics_summary=BayesDiagnosticsSummary(
            rhat_max=1.05,
            ess_bulk_min=120.0,
            ess_tail_min=110.0,
            divergences=0,
            total_draws=100,
            calibration_ece=0.05,
            posterior_predictive_max_bucket_dev=0.02,
        ),
    )


def _make_manifest(*, extras: BayesArtifactExtras | None) -> ArtifactManifest:
    return ArtifactManifest(
        artifact_id="bayes-smoke-1",
        kind="bayes",
        name="trajectory_observedness",
        version="0.1.0",
        created_at=datetime.now(tz=timezone.utc),
        source_snapshot_id="dev",
        output_paths={},
        package_versions={"pymc": "5.0.0"},
        random_seed=20260513,
        ablation_status="gamma_dl_zero",
        bayes_extras=extras,
    )


def test_bayes_manifest_round_trip() -> None:
    manifest = _make_manifest(extras=_make_extras())
    payload = manifest.model_dump_json()
    reloaded = ArtifactManifest.model_validate_json(payload)
    assert reloaded.kind == "bayes"
    assert reloaded.bayes_extras is not None
    assert reloaded.bayes_extras.model_name == "trajectory_observedness"
    assert reloaded.bayes_extras.gamma_dl_flavor == "gamma_dl_zero"
    assert reloaded.bayes_extras.sampler_config.is_smoke is True


def test_bayes_shrunk_round_trip_carries_dl_proposal_inputs() -> None:
    extras = _make_extras().model_copy(
        update={
            "gamma_dl_flavor": "gamma_dl_shrunk",
            "ablation_status": "gamma_dl_shrunk",
            "dl_proposal_inputs": ("phase3-trajectory-v8",),
        }
    )
    manifest = _make_manifest(extras=extras)
    payload = manifest.model_dump_json()
    reloaded = ArtifactManifest.model_validate_json(payload)
    assert reloaded.bayes_extras is not None
    assert reloaded.bayes_extras.gamma_dl_flavor == "gamma_dl_shrunk"
    assert reloaded.bayes_extras.dl_proposal_inputs == ("phase3-trajectory-v8",)


def test_bayes_extras_rejects_flavor_status_mismatch() -> None:
    from python_models.statistical.schemas import BayesArtifactExtras

    payload = _make_extras().model_dump()
    payload["ablation_status"] = "gamma_dl_shrunk"
    with pytest.raises(ValidationError):
        _ = BayesArtifactExtras.model_validate(payload)


def test_bayes_extras_rejects_shrunk_without_dl_inputs() -> None:
    from python_models.statistical.schemas import BayesArtifactExtras

    payload = _make_extras().model_dump()
    payload["gamma_dl_flavor"] = "gamma_dl_shrunk"
    payload["ablation_status"] = "gamma_dl_shrunk"
    payload["dl_proposal_inputs"] = ()
    with pytest.raises(ValidationError):
        _ = BayesArtifactExtras.model_validate(payload)


def test_bayes_kind_requires_extras() -> None:
    with pytest.raises(ValidationError):
        _ = _make_manifest(extras=None)


def test_non_bayes_kind_does_not_require_extras() -> None:
    manifest = ArtifactManifest(
        artifact_id="dataset-1",
        kind="dataset",
        name="some_dataset",
        version="0.1.0",
        created_at=datetime.now(tz=timezone.utc),
        source_snapshot_id="dev",
        output_paths={},
        package_versions={},
    )
    assert manifest.bayes_extras is None
