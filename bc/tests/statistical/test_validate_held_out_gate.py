"""Held-out calibration/AUC gate wiring in ``_validate_bayes``."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from python_models.statistical.manifests import package_versions
from python_models.statistical.schemas import (
    ArtifactManifest,
    BayesArtifactExtras,
    BayesDiagnosticsSummary,
    BayesPriorConfig,
    BayesSamplerConfig,
)
from python_models.statistical.validate import _validate_bayes


def _manifest() -> ArtifactManifest:
    extras = BayesArtifactExtras(
        model_name="synthetic_target",
        model_version="0.0.0",
        prior_config=BayesPriorConfig(),
        sampler_config=BayesSamplerConfig(
            draws=10, tune=10, chains=2, target_accept=0.9, random_seed=1
        ),
        diagnostics_summary=BayesDiagnosticsSummary(
            rhat_max=1.0,
            ess_bulk_min=1000.0,
            ess_tail_min=1000.0,
            divergences=0,
            total_draws=20,
        ),
    )
    return ArtifactManifest(
        artifact_id="aid",
        kind="bayes",
        name="synthetic_target",
        version="0.0.0",
        created_at=datetime.now(tz=timezone.utc),
        source_snapshot_id="src-1",
        output_paths={"artifact_dir": Path(".")},
        package_versions=package_versions(),
        bayes_extras=extras,
    )


def _write(artifact_dir: Path, *, diagnostics: dict[str, object], held_out: object | None) -> None:
    validation = artifact_dir / "validation"
    validation.mkdir(parents=True, exist_ok=True)
    (validation / "diagnostics.json").write_text(json.dumps(diagnostics), encoding="utf-8")
    if held_out is not None:
        (validation / "held_out_metrics.json").write_text(
            json.dumps(held_out), encoding="utf-8"
        )


def _healthy_diagnostics(*, is_smoke: bool = False) -> dict[str, object]:
    return {
        "rhat_max": 1.01,
        "ess_bulk_min": 500.0,
        "divergences": 0,
        "total_draws": 4000,
        "is_smoke": is_smoke,
    }


def _codes(artifact_dir: Path, severity: str | None = None) -> set[str]:
    report = _validate_bayes(_manifest(), artifact_dir)
    return {
        f.code
        for f in report.findings
        if severity is None or f.severity == severity
    }


def test_good_bernoulli_metrics_pass(tmp_path: Path) -> None:
    _write(
        tmp_path,
        diagnostics=_healthy_diagnostics(),
        held_out={
            "roc_auc": 0.9,
            "pr_auc": 0.9,
            "baseline_pr_auc": 0.3,
            "ece_held_out": 0.02,
        },
    )
    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "passed"
    assert not [f for f in report.findings if f.code.startswith("bayes_held_out")]


def test_high_ece_warns_but_does_not_fail(tmp_path: Path) -> None:
    _write(
        tmp_path,
        diagnostics=_healthy_diagnostics(),
        held_out={
            "roc_auc": 0.9,
            "pr_auc": 0.9,
            "baseline_pr_auc": 0.3,
            "ece_held_out": 0.2,
        },
    )
    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "passed"
    warns = {f.code for f in report.findings if f.severity == "warn"}
    assert "bayes_held_out_ece" in warns


def test_roc_below_chance_blocks_on_default(tmp_path: Path) -> None:
    _write(
        tmp_path,
        diagnostics=_healthy_diagnostics(),
        held_out={"roc_auc": 0.4, "pr_auc": 0.2, "baseline_pr_auc": 0.1},
    )
    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "failed"
    assert "bayes_held_out_auc_not_beating_baseline" in _codes(tmp_path, "block")


def test_roc_below_chance_only_warns_on_smoke(tmp_path: Path) -> None:
    _write(
        tmp_path,
        diagnostics=_healthy_diagnostics(is_smoke=True),
        held_out={"roc_auc": 0.4, "pr_auc": 0.2, "baseline_pr_auc": 0.1},
    )
    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "passed"
    assert "bayes_held_out_auc_not_beating_baseline" in _codes(tmp_path, "warn")


def test_pr_auc_not_beating_baseline_blocks(tmp_path: Path) -> None:
    _write(
        tmp_path,
        diagnostics=_healthy_diagnostics(),
        held_out={"roc_auc": 0.7, "pr_auc": 0.25, "baseline_pr_auc": 0.30},
    )
    assert "bayes_held_out_pr_auc_not_beating_baseline" in _codes(tmp_path, "block")


def test_multiclass_top1_not_beating_baseline_blocks(tmp_path: Path) -> None:
    _write(
        tmp_path,
        diagnostics=_healthy_diagnostics(),
        held_out={
            "top1_accuracy": 0.20,
            "baseline_top1_accuracy": 0.25,
            "ece_held_out": 0.01,
        },
    )
    assert "bayes_held_out_top1_not_beating_baseline" in _codes(tmp_path, "block")


def test_multiclass_top1_beating_baseline_passes(tmp_path: Path) -> None:
    _write(
        tmp_path,
        diagnostics=_healthy_diagnostics(),
        held_out={
            "top1_accuracy": 0.53,
            "baseline_top1_accuracy": 0.25,
            "ece_held_out": 0.01,
        },
    )
    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "passed"


def test_loglik_lift_non_positive_blocks(tmp_path: Path) -> None:
    _write(
        tmp_path,
        diagnostics=_healthy_diagnostics(),
        held_out={"loglik_lift": -0.01, "tv_improvement": 0.02},
    )
    assert "bayes_held_out_no_loglik_lift" in _codes(tmp_path, "block")


def test_loglik_lift_positive_passes(tmp_path: Path) -> None:
    _write(
        tmp_path,
        diagnostics=_healthy_diagnostics(),
        held_out={"loglik_lift": 0.0124, "tv_improvement": 0.02},
    )
    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "passed"


def test_absent_file_warns_on_full_scale(tmp_path: Path) -> None:
    _write(tmp_path, diagnostics=_healthy_diagnostics(), held_out=None)
    codes = _codes(tmp_path, "warn")
    assert "bayes_held_out_metrics_absent" in codes


def test_absent_file_silent_on_smoke(tmp_path: Path) -> None:
    _write(tmp_path, diagnostics=_healthy_diagnostics(is_smoke=True), held_out=None)
    assert "bayes_held_out_metrics_absent" not in _codes(tmp_path)


def test_malformed_held_out_warns(tmp_path: Path) -> None:
    validation = tmp_path / "validation"
    validation.mkdir(parents=True, exist_ok=True)
    (validation / "diagnostics.json").write_text(
        json.dumps(_healthy_diagnostics()), encoding="utf-8"
    )
    (validation / "held_out_metrics.json").write_text("{not json", encoding="utf-8")
    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "passed"
    assert "bayes_held_out_metrics_malformed" in {
        f.code for f in report.findings if f.severity == "warn"
    }
