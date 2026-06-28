"""Tests for the model-config + publication-gate scaffolding."""

from __future__ import annotations

from pathlib import Path

import pytest

from python_models.statistical.model_config import (
    ModelConfig,
    WeakIdentificationPolicy,
    build_policies,
)
from python_models.statistical.publication import (
    PublicationGateResult,
    evaluate_publication_gate,
)
from python_models.statistical.schemas import (
    BlockingFinding,
    EdaReport,
    WeakIdentificationFlag,
)


def _eda(
    *,
    dataset_name: str = "model_input_observation_batted_ball",
    dataset_version: str = "0.1.0",
    flags: tuple[WeakIdentificationFlag, ...] = (),
    blocking: tuple[BlockingFinding, ...] = (),
) -> EdaReport:
    return EdaReport(
        dataset_name=dataset_name,
        dataset_version=dataset_version,
        dataset_artifact_id="ds-1",
        source_snapshot_id="snap-test",
        row_count=10,
        target_population_count=10,
        observed_truth_count=8,
        source_family_block_missing_count=0,
        data_error_excluded_count=0,
        module_paths={"missingness_by_slice": Path("missingness_by_slice.parquet")},
        blocking_findings=blocking,
        weak_identification_flags=flags,
    )


def _config(
    *,
    dataset_name: str = "model_input_observation_batted_ball",
    dataset_version: str = "0.1.0",
    policies: tuple[WeakIdentificationPolicy, ...] = (),
    expected_blocking: tuple[str, ...] = (),
) -> ModelConfig:
    return ModelConfig(
        model_name="scorer_observation_propensities",
        model_version="0.0.1",
        dataset_name=dataset_name,
        dataset_version=dataset_version,
        addressed_weak_identifications=policies,
        expected_blocking_findings=expected_blocking,
    )


def test_publication_gate_passes_when_no_findings() -> None:
    result = evaluate_publication_gate(config=_config(), eda_report=_eda())
    assert isinstance(result, PublicationGateResult)
    assert result.passed
    assert result.blocking_violations == ()
    assert result.warnings == ()


def test_publication_gate_blocks_on_unaddressed_weak_flag() -> None:
    flags = (
        WeakIdentificationFlag(
            effect="scorer_park",
            slice="scorer_x|PARK_A",
            share=0.97,
            reason="collinearity_dominant_share",
        ),
    )
    result = evaluate_publication_gate(
        config=_config(),
        eda_report=_eda(flags=flags),
    )
    assert not result.passed
    assert any(
        v.code == "weak_identification_unaddressed" for v in result.blocking_violations
    )


def test_publication_gate_passes_with_matching_policy() -> None:
    policies = build_policies(
        [
            ("scorer_park", "scorer_x|PARK_A", "partial_pool"),
        ]
    )
    flags = (
        WeakIdentificationFlag(
            effect="scorer_park",
            slice="scorer_x|PARK_A",
            share=0.97,
            reason="collinearity_dominant_share",
        ),
    )
    result = evaluate_publication_gate(
        config=_config(policies=policies),
        eda_report=_eda(flags=flags),
    )
    assert result.passed
    assert len(result.warnings) == 1
    assert result.warnings[0].code == "weak_identification_addressed"


def test_publication_gate_wildcard_slice_matches() -> None:
    policies = build_policies(
        [
            ("scorer_park", "*", "partial_pool"),
        ]
    )
    flags = (
        WeakIdentificationFlag(
            effect="scorer_park",
            slice="any_pair",
            share=0.99,
            reason="collinearity_dominant_share",
        ),
    )
    result = evaluate_publication_gate(
        config=_config(policies=policies),
        eda_report=_eda(flags=flags),
    )
    assert result.passed


def test_publication_gate_exact_slice_wins_over_wildcard() -> None:
    policies = (
        WeakIdentificationPolicy(
            effect="scorer_park", slice="*", treatment="partial_pool"
        ),
        WeakIdentificationPolicy(
            effect="scorer_park",
            slice="exact",
            treatment="accept_unidentified",
            rationale="known historical artifact",
        ),
    )
    flags = (
        WeakIdentificationFlag(
            effect="scorer_park",
            slice="exact",
            share=0.99,
            reason="collinearity_dominant_share",
        ),
    )
    result = evaluate_publication_gate(
        config=_config(policies=policies),
        eda_report=_eda(flags=flags),
    )
    assert result.passed
    assert len(result.warnings) == 1
    assert result.warnings[0].code == "weak_identification_accepted"


def test_publication_gate_blocks_on_unexpected_blocking_finding() -> None:
    blocking = (
        BlockingFinding(
            code="dominant_single_scorer_park_team",
            severity="warn",
            message="dominant scorer/park combo",
        ),
    )
    result = evaluate_publication_gate(
        config=_config(),
        eda_report=_eda(blocking=blocking),
    )
    assert not result.passed
    assert any(
        v.code == "unexpected_blocking_finding" for v in result.blocking_violations
    )


def test_publication_gate_acknowledges_expected_blocking_finding() -> None:
    blocking = (
        BlockingFinding(
            code="dominant_single_scorer_park_team",
            severity="warn",
            message="dominant scorer/park combo",
        ),
    )
    result = evaluate_publication_gate(
        config=_config(
            expected_blocking=("dominant_single_scorer_park_team",),
        ),
        eda_report=_eda(blocking=blocking),
    )
    assert result.passed


def test_publication_gate_blocks_on_dataset_name_mismatch() -> None:
    result = evaluate_publication_gate(
        config=_config(dataset_name="model_input_geometry"),
        eda_report=_eda(),
    )
    assert not result.passed
    assert any(v.code == "dataset_name_mismatch" for v in result.blocking_violations)


def test_publication_gate_blocks_on_dataset_version_mismatch() -> None:
    result = evaluate_publication_gate(
        config=_config(dataset_version="0.0.9"),
        eda_report=_eda(),
    )
    assert not result.passed
    assert any(v.code == "dataset_version_mismatch" for v in result.blocking_violations)


def test_model_config_rejects_duplicate_policies() -> None:
    with pytest.raises(ValueError, match="duplicate weak-identification policy"):
        _ = ModelConfig(
            model_name="m",
            model_version="v",
            dataset_name="d",
            dataset_version="0",
            addressed_weak_identifications=(
                WeakIdentificationPolicy(effect="a", slice="*", treatment="drop"),
                WeakIdentificationPolicy(
                    effect="a", slice="*", treatment="partial_pool"
                ),
            ),
        )


def test_model_config_rejects_accept_unidentified_without_rationale() -> None:
    with pytest.raises(ValueError, match="rationale"):
        _ = ModelConfig(
            model_name="m",
            model_version="v",
            dataset_name="d",
            dataset_version="0",
            addressed_weak_identifications=(
                WeakIdentificationPolicy(
                    effect="a",
                    slice="*",
                    treatment="accept_unidentified",
                    rationale="",
                ),
            ),
        )


def test_model_config_rejects_unknown_treatment() -> None:
    with pytest.raises(ValueError):
        _ = WeakIdentificationPolicy.model_validate(
            {"effect": "a", "slice": "*", "treatment": "fancy_treatment"}
        )


def test_lookup_policy_returns_none_when_no_match() -> None:
    config = _config(policies=build_policies([("scorer_park", "x|y", "partial_pool")]))
    assert config.lookup_policy(effect="other", slice_value="x|y") is None
    assert config.lookup_policy(effect="scorer_park", slice_value="not_x|y") is None


def test_build_policies_handles_empty_iterable() -> None:
    assert build_policies([]) == ()
