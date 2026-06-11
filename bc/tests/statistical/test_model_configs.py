"""Tests for the committed ModelConfig JSONs under model_configs/."""

from __future__ import annotations

from pathlib import Path

import pytest

from python_models.statistical import model_config as model_config_module
from python_models.statistical.dataset_registry import DATASET_SPECS
from python_models.statistical.model_config import ModelConfig
from python_models.statistical.publication import evaluate_publication_gate
from python_models.statistical.schemas import (
    BlockingFinding,
    EdaReport,
    WeakIdentificationFlag,
)

CONFIG_DIR = Path(model_config_module.__file__).parent / "model_configs"

OBSERVEDNESS_SUFFIX = "_observedness"


def _config_paths() -> list[Path]:
    return sorted(CONFIG_DIR.glob("*.json"))


def _load(path: Path) -> ModelConfig:
    return ModelConfig.model_validate_json(path.read_text(encoding="utf-8"))


def _parse_version(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def _fixture_eda_report(
    config: ModelConfig,
    *,
    extra_blocking: tuple[BlockingFinding, ...] = (),
) -> EdaReport:
    blocking = (
        BlockingFinding(
            code="dominant_single_scorer_park_team",
            severity="warn",
            message="pair scorer_park has dominant share 1.000 for (x, PARK01).",
        ),
        BlockingFinding(
            code="no_connected_component_for_effect",
            severity="warn",
            message="edge kinds with single-node components: source_family_season.",
        ),
        BlockingFinding(
            code="category_absent_in_train_present_in_test",
            severity="warn",
            message="column 'park_id' has values in TEST absent from TRAIN.",
        ),
        BlockingFinding(
            code="category_absent_in_train_present_in_test",
            severity="warn",
            message="column 'scorer' has values in TEST absent from TRAIN.",
        ),
        *extra_blocking,
    )
    flags = (
        WeakIdentificationFlag(
            effect="scorer_park",
            slice="x|PARK01",
            share=1.0,
            reason="collinearity_dominant_share",
        ),
        WeakIdentificationFlag(
            effect="scorer_source_family",
            slice="x|retrosheet_event",
            share=1.0,
            reason="collinearity_dominant_share",
        ),
        WeakIdentificationFlag(
            effect="source_family_era",
            slice="retrosheet_event|1910s",
            share=0.99,
            reason="collinearity_dominant_share",
        ),
        WeakIdentificationFlag(
            effect="source_family_season",
            slice="retrosheet_event|1911",
            share=0.99,
            reason="collinearity_dominant_share",
        ),
    )
    return EdaReport(
        dataset_name=config.dataset_name,
        dataset_version=config.dataset_version,
        dataset_artifact_id="ds-fixture",
        source_snapshot_id="snap-fixture",
        row_count=10,
        target_population_count=10,
        observed_truth_count=8,
        source_family_block_missing_count=0,
        data_error_excluded_count=0,
        module_paths={"missingness_by_slice": Path("missingness_by_slice.parquet")},
        blocking_findings=blocking,
        weak_identification_flags=flags,
    )


def test_config_dir_is_nonempty() -> None:
    assert _config_paths()


@pytest.mark.parametrize("path", _config_paths(), ids=lambda p: p.stem)
def test_config_parses_and_matches_filename(path: Path) -> None:
    config = _load(path)
    assert config.model_name == path.stem


@pytest.mark.parametrize("path", _config_paths(), ids=lambda p: p.stem)
def test_config_dataset_is_registered(path: Path) -> None:
    config = _load(path)
    assert config.dataset_name in DATASET_SPECS


@pytest.mark.parametrize("path", _config_paths(), ids=lambda p: p.stem)
def test_config_dataset_version_against_registry(path: Path) -> None:
    config = _load(path)
    registry_version = DATASET_SPECS[config.dataset_name].dataset_version
    assert _parse_version(config.dataset_version) <= _parse_version(registry_version)
    if not config.model_name.endswith(OBSERVEDNESS_SUFFIX):
        assert config.dataset_version == registry_version


@pytest.mark.parametrize("path", _config_paths(), ids=lambda p: p.stem)
def test_publication_gate_passes_against_known_corpus_findings(path: Path) -> None:
    config = _load(path)
    result = evaluate_publication_gate(
        config=config, eda_report=_fixture_eda_report(config)
    )
    assert result.passed, [v.message for v in result.blocking_violations]
    assert {w.effect for w in result.warnings} == {
        "scorer_park",
        "scorer_source_family",
        "source_family_era",
        "source_family_season",
    }


@pytest.mark.parametrize("path", _config_paths(), ids=lambda p: p.stem)
def test_publication_gate_blocks_on_unacknowledged_finding(path: Path) -> None:
    config = _load(path)
    extra = (
        BlockingFinding(
            code="split_leakage_detected",
            severity="block",
            message="unit maps to multiple fold values.",
        ),
    )
    result = evaluate_publication_gate(
        config=config,
        eda_report=_fixture_eda_report(config, extra_blocking=extra),
    )
    assert not result.passed
    assert any(
        v.code == "unexpected_blocking_finding" for v in result.blocking_violations
    )
