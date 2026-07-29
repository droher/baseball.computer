"""Held-out calibration/AUC gate wiring in ``_validate_bayes``."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pytest

from python_models.statistical.manifests import package_versions
from python_models.statistical.schemas import (
    ArtifactManifest,
    BayesArtifactExtras,
    BayesDiagnosticsSummary,
    BayesPriorConfig,
    BayesSamplerConfig,
)
from python_models.statistical.validate import (
    _derive_baseline_log_loss,
    _validate_bayes,
)


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


def _distribution_calibration(shares: list[float]) -> dict[str, object]:
    return {
        "n_evaluated": 1000,
        "per_position": {
            str(i + 1): {
                "predicted_share": share,
                "empirical_share": share,
                "abs_dev": 0.0,
            }
            for i, share in enumerate(shares)
        },
    }


def _entropy(shares: list[float]) -> float:
    return -sum(p * math.log(p) for p in shares if p > 0.0)


def test_multiclass_top1_not_beating_baseline_only_warns(tmp_path: Path) -> None:
    shares = [0.55, 0.25, 0.15, 0.05]
    _write(
        tmp_path,
        diagnostics=_healthy_diagnostics(),
        held_out={
            "top1_accuracy": 0.20,
            "baseline_top1_accuracy": 0.25,
            "log_loss": _entropy(shares) - 0.04,
            "distribution_calibration": _distribution_calibration(shares),
            "ece_held_out": 0.01,
        },
    )
    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "passed"
    assert "bayes_held_out_top1_not_beating_baseline" in _codes(tmp_path, "warn")
    assert "bayes_held_out_top1_not_beating_baseline" not in _codes(tmp_path, "block")


def test_multiclass_top1_beating_baseline_passes(tmp_path: Path) -> None:
    shares = [0.55, 0.25, 0.15, 0.05]
    _write(
        tmp_path,
        diagnostics=_healthy_diagnostics(),
        held_out={
            "top1_accuracy": 0.53,
            "baseline_top1_accuracy": 0.25,
            "log_loss": _entropy(shares) - 0.04,
            "distribution_calibration": _distribution_calibration(shares),
            "ece_held_out": 0.01,
        },
    )
    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "passed"
    assert not [f for f in report.findings if f.code.startswith("bayes_held_out")]


def test_log_loss_beating_baseline_passes(tmp_path: Path) -> None:
    shares = [0.55, 0.25, 0.15, 0.05]
    baseline = _entropy(shares)
    _write(
        tmp_path,
        diagnostics=_healthy_diagnostics(),
        held_out={
            "log_loss": baseline - 0.04,
            "baseline_log_loss": baseline,
            "ece_held_out": 0.01,
        },
    )
    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "passed"
    assert not [f for f in report.findings if f.code.startswith("bayes_held_out")]


def test_log_loss_not_beating_baseline_blocks(tmp_path: Path) -> None:
    shares = [0.55, 0.25, 0.15, 0.05]
    baseline = _entropy(shares)
    _write(
        tmp_path,
        diagnostics=_healthy_diagnostics(),
        held_out={
            "log_loss": baseline + 0.01,
            "baseline_log_loss": baseline,
            "ece_held_out": 0.01,
        },
    )
    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "failed"
    assert "bayes_held_out_log_loss_not_beating_baseline" in _codes(tmp_path, "block")


def test_log_loss_not_beating_baseline_only_warns_on_smoke(tmp_path: Path) -> None:
    shares = [0.5, 0.5]
    baseline = _entropy(shares)
    _write(
        tmp_path,
        diagnostics=_healthy_diagnostics(is_smoke=True),
        held_out={
            "log_loss": baseline + 0.01,
            "baseline_log_loss": baseline,
        },
    )
    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "passed"
    assert "bayes_held_out_log_loss_not_beating_baseline" in _codes(tmp_path, "warn")


def test_absent_baseline_log_loss_is_derived_from_distribution(tmp_path: Path) -> None:
    shares = [0.25, 0.25, 0.25, 0.25]
    _write(
        tmp_path,
        diagnostics=_healthy_diagnostics(),
        held_out={
            "log_loss": _entropy(shares) - 0.05,
            "distribution_calibration": _distribution_calibration(shares),
        },
    )
    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "passed"
    assert report.metrics["held_out_baseline_log_loss"] == pytest.approx(
        math.log(len(shares))
    )
    assert report.metrics["held_out_baseline_log_loss"] == pytest.approx(
        _entropy(shares)
    )


def test_derived_baseline_matches_explicit_key(tmp_path: Path) -> None:
    shares = [0.6, 0.2, 0.15, 0.05]
    distribution = _distribution_calibration(shares)
    log_loss = _entropy(shares) - 0.02

    derived_dir = tmp_path / "derived"
    _write(
        derived_dir,
        diagnostics=_healthy_diagnostics(),
        held_out={"log_loss": log_loss, "distribution_calibration": distribution},
    )
    explicit_dir = tmp_path / "explicit"
    _write(
        explicit_dir,
        diagnostics=_healthy_diagnostics(),
        held_out={
            "log_loss": log_loss,
            "baseline_log_loss": _entropy(shares),
            "distribution_calibration": distribution,
        },
    )

    derived = _validate_bayes(_manifest(), derived_dir)
    explicit = _validate_bayes(_manifest(), explicit_dir)
    assert derived.metrics["held_out_baseline_log_loss"] == pytest.approx(
        explicit.metrics["held_out_baseline_log_loss"]
    )
    assert derived.status == explicit.status


def test_derived_baseline_ignores_degenerate_distribution(tmp_path: Path) -> None:
    _write(
        tmp_path,
        diagnostics=_healthy_diagnostics(),
        held_out={
            "log_loss": 0.5,
            "distribution_calibration": _distribution_calibration([1.0, 0.0]),
        },
    )
    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "passed"
    assert "held_out_baseline_log_loss" not in report.metrics


@pytest.mark.parametrize(
    "distribution",
    [
        None,
        "not-a-dict",
        {"n_evaluated": 10},
        {"per_position": "not-a-dict"},
        {"per_position": {"1": "not-a-dict"}},
        {"per_position": {"1": {"empirical_share": None}}},
        {"per_position": {"1": {"empirical_share": float("nan")}}},
        {"per_position": {"1": {"predicted_share": 0.5}}},
    ],
)
def test_malformed_distribution_skips_log_loss_check(
    tmp_path: Path, distribution: object
) -> None:
    held_out: dict[str, object] = {"log_loss": 99.0}
    if distribution is not None:
        held_out["distribution_calibration"] = distribution
    _write(tmp_path, diagnostics=_healthy_diagnostics(), held_out=held_out)
    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "passed"
    assert "bayes_held_out_log_loss_not_beating_baseline" not in {
        f.code for f in report.findings
    }
    assert "held_out_baseline_log_loss" not in report.metrics


def test_degenerate_baseline_verdict_is_vintage_independent(tmp_path: Path) -> None:
    """A single-class held-out set grades the same whether or not it was emitted.

    The producer writes ``baseline_log_loss`` at zero for a degenerate
    held-out set; the fallback derives nothing from the same data. Both
    must mean "no baseline", or the identical fit would grade differently
    depending only on when it was fit.
    """
    degenerate = _distribution_calibration([1.0, 0.0, 0.0, 0.0])
    perfect: dict[str, object] = {
        "top1_accuracy": 1.0,
        "baseline_top1_accuracy": 1.0,
        "log_loss": 0.0,
        "ece_held_out": 0.01,
        "distribution_calibration": degenerate,
    }

    emitted_dir = tmp_path / "emitted"
    _write(
        emitted_dir,
        diagnostics=_healthy_diagnostics(),
        held_out={**perfect, "baseline_log_loss": 0.0},
    )
    derived_dir = tmp_path / "derived"
    _write(derived_dir, diagnostics=_healthy_diagnostics(), held_out=perfect)

    emitted = _validate_bayes(_manifest(), emitted_dir)
    derived = _validate_bayes(_manifest(), derived_dir)

    assert emitted.status == derived.status
    assert {f.code for f in emitted.findings} == {f.code for f in derived.findings}
    assert "bayes_held_out_log_loss_not_beating_baseline" not in {
        f.code for f in emitted.findings
    }
    assert "held_out_baseline_log_loss" not in emitted.metrics
    assert "held_out_baseline_log_loss" not in derived.metrics


@pytest.mark.parametrize("log_loss", [float("nan"), None])
def test_multinomial_payload_without_gradeable_log_loss_blocks(
    tmp_path: Path, log_loss: float | None
) -> None:
    shares = [0.55, 0.25, 0.15, 0.05]
    held_out: dict[str, object] = {
        "top1_accuracy": 0.53,
        "baseline_top1_accuracy": 0.25,
        "distribution_calibration": _distribution_calibration(shares),
        "ece_held_out": 0.01,
    }
    if log_loss is not None:
        held_out["log_loss"] = log_loss
    _write(tmp_path, diagnostics=_healthy_diagnostics(), held_out=held_out)

    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "failed"
    assert "bayes_held_out_log_loss_ungradeable" in _codes(tmp_path, "block")


@pytest.mark.parametrize("log_loss", [float("nan"), None])
def test_multinomial_ungradeable_log_loss_only_warns_on_smoke(
    tmp_path: Path, log_loss: float | None
) -> None:
    held_out: dict[str, object] = {
        "top1_accuracy": 0.53,
        "baseline_top1_accuracy": 0.25,
    }
    if log_loss is not None:
        held_out["log_loss"] = log_loss
    _write(tmp_path, diagnostics=_healthy_diagnostics(is_smoke=True), held_out=held_out)

    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "passed"
    assert "bayes_held_out_log_loss_ungradeable" in _codes(tmp_path, "warn")


@pytest.mark.parametrize(
    "held_out",
    [
        {"roc_auc": 0.9, "pr_auc": 0.9, "baseline_pr_auc": 0.3, "ece_held_out": 0.02},
        {"loglik_lift": 0.0124, "tv_improvement": 0.02},
    ],
)
def test_payloads_without_top1_are_not_graded_on_log_loss(
    tmp_path: Path, held_out: dict[str, object]
) -> None:
    _write(tmp_path, diagnostics=_healthy_diagnostics(), held_out=held_out)
    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "passed"
    assert "bayes_held_out_log_loss_ungradeable" not in {
        f.code for f in report.findings
    }


@pytest.mark.parametrize(
    "per_position",
    [
        {"1": {"empirical_share": 1.5}, "2": {"empirical_share": 0.5}},
        {"1": {"empirical_share": 1.2}, "2": {"empirical_share": 0.3}},
        {"1": {"empirical_share": 0.55}, "2": {"empirical_share": 0.25}},
        {"1": {"empirical_share": 0.55}},
        {},
    ],
)
def test_malformed_share_sets_derive_no_baseline(
    per_position: dict[str, object],
) -> None:
    payload: dict[str, object] = {
        "distribution_calibration": {"per_position": per_position}
    }
    assert _derive_baseline_log_loss(payload) is None


def test_truncated_shares_do_not_block_a_model_beating_the_true_baseline(
    tmp_path: Path,
) -> None:
    """A dropped class understates the entropy; the gate must not run on it.

    ``log_loss`` sits below the entropy of the full share set but above
    the entropy of the truncated one, so a fallback that accepted the
    truncation would block a model that beats its real baseline.
    """
    full = [0.55, 0.25, 0.15, 0.05]
    truncated = full[:2]
    log_loss = (_entropy(truncated) + _entropy(full)) / 2.0
    assert _entropy(truncated) < log_loss < _entropy(full)

    _write(
        tmp_path,
        diagnostics=_healthy_diagnostics(),
        held_out={
            "log_loss": log_loss,
            "distribution_calibration": _distribution_calibration(truncated),
            "ece_held_out": 0.01,
        },
    )
    report = _validate_bayes(_manifest(), tmp_path)
    assert report.status == "passed"
    assert "bayes_held_out_log_loss_not_beating_baseline" not in {
        f.code for f in report.findings
    }
    assert "held_out_baseline_log_loss" not in report.metrics


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
