"""CLI end-to-end test for ``bc-stats check-publication-gate``."""

from __future__ import annotations

from pathlib import Path

from python_models.statistical.cli import main
from python_models.statistical.model_config import (
    ModelConfig,
    WeakIdentificationPolicy,
)
from python_models.statistical.schemas import (
    BlockingFinding,
    EdaReport,
    WeakIdentificationFlag,
)


def _write_eda(path: Path, *, flags=(), blocking=()) -> None:
    report = EdaReport(
        dataset_name="model_input_observation_batted_ball",
        dataset_version="0.1.0",
        dataset_artifact_id="ds-1",
        source_snapshot_id="snap-test",
        row_count=10,
        target_population_count=10,
        observed_truth_count=8,
        source_family_block_missing_count=0,
        data_error_excluded_count=0,
        module_paths={"missingness_by_slice": Path("missingness_by_slice.parquet")},
        weak_identification_flags=flags,
        blocking_findings=blocking,
    )
    path.write_text(report.model_dump_json(indent=2), encoding="utf-8")


def _write_config(
    path: Path,
    *,
    policies=(),
    expected_blocking=(),
) -> None:
    config = ModelConfig(
        model_name="scorer_observation_propensities",
        model_version="0.0.1",
        dataset_name="model_input_observation_batted_ball",
        dataset_version="0.1.0",
        addressed_weak_identifications=policies,
        expected_blocking_findings=expected_blocking,
    )
    path.write_text(config.model_dump_json(indent=2), encoding="utf-8")


def test_cli_check_publication_gate_passes(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    eda_path = tmp_path / "eda.json"
    _write_config(config_path)
    _write_eda(eda_path)
    rc = main(
        [
            "check-publication-gate",
            "--model-config",
            str(config_path),
            "--eda-report",
            str(eda_path),
        ]
    )
    assert rc == 0


def test_cli_check_publication_gate_blocks(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    eda_path = tmp_path / "eda.json"
    _write_config(config_path)
    _write_eda(
        eda_path,
        flags=(
            WeakIdentificationFlag(
                effect="scorer_park",
                slice="x|y",
                share=0.97,
                reason="collinearity_dominant_share",
            ),
        ),
    )
    rc = main(
        [
            "check-publication-gate",
            "--model-config",
            str(config_path),
            "--eda-report",
            str(eda_path),
        ]
    )
    assert rc == 1


def test_cli_check_publication_gate_with_policy_passes(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    eda_path = tmp_path / "eda.json"
    _write_config(
        config_path,
        policies=(
            WeakIdentificationPolicy(
                effect="scorer_park", slice="*", treatment="partial_pool"
            ),
        ),
    )
    _write_eda(
        eda_path,
        flags=(
            WeakIdentificationFlag(
                effect="scorer_park",
                slice="x|y",
                share=0.97,
                reason="collinearity_dominant_share",
            ),
        ),
    )
    rc = main(
        [
            "check-publication-gate",
            "--model-config",
            str(config_path),
            "--eda-report",
            str(eda_path),
        ]
    )
    assert rc == 0


def test_cli_check_publication_gate_acks_blocking_finding(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    eda_path = tmp_path / "eda.json"
    _write_config(
        config_path,
        expected_blocking=("dominant_single_scorer_park_team",),
    )
    _write_eda(
        eda_path,
        blocking=(
            BlockingFinding(
                code="dominant_single_scorer_park_team",
                severity="warn",
                message="dom",
            ),
        ),
    )
    rc = main(
        [
            "check-publication-gate",
            "--model-config",
            str(config_path),
            "--eda-report",
            str(eda_path),
        ]
    )
    assert rc == 0
