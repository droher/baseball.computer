"""Fail-closed convergence gate + weak-identification derivation tests."""

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
    _BAYES_THRESHOLDS_DEFAULT,
    _BAYES_THRESHOLDS_SMOKE,
    _WEAK_IDENTIFICATION_ESS_BULK_MULTIPLIER,
    _WEAK_IDENTIFICATION_RHAT_MARGIN_FRACTION,
    _validate_bayes,
    diagnostics_indicate_weak_identification,
    weak_identification_thresholds,
)


def _bayes_manifest(artifact_id: str) -> ArtifactManifest:
    extras = BayesArtifactExtras(
        model_name="synthetic_target",
        model_version="0.0.0",
        prior_config=BayesPriorConfig(),
        sampler_config=BayesSamplerConfig(
            draws=10,
            tune=10,
            chains=2,
            target_accept=0.9,
            random_seed=1,
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
        artifact_id=artifact_id,
        kind="bayes",
        name="synthetic_target",
        version="0.0.0",
        created_at=datetime.now(tz=timezone.utc),
        source_snapshot_id="src-1",
        output_paths={"artifact_dir": Path(".")},
        package_versions=package_versions(),
        bayes_extras=extras,
    )


def _write_diagnostics(artifact_dir: Path, text: str) -> None:
    validation = artifact_dir / "validation"
    validation.mkdir(parents=True, exist_ok=True)
    (validation / "diagnostics.json").write_text(text, encoding="utf-8")


def _write_healthy_held_out(artifact_dir: Path) -> None:
    validation = artifact_dir / "validation"
    validation.mkdir(parents=True, exist_ok=True)
    (validation / "held_out_metrics.json").write_text(
        json.dumps(
            {"roc_auc": 0.9, "pr_auc": 0.9, "baseline_pr_auc": 0.3, "ece_held_out": 0.02}
        ),
        encoding="utf-8",
    )


def _block_codes(artifact_dir: Path) -> set[str]:
    report = _validate_bayes(_bayes_manifest("aid"), artifact_dir)
    return {f.code for f in report.findings if f.severity == "block"}


def _healthy_payload() -> dict[str, object]:
    return {
        "rhat_max": 1.01,
        "ess_bulk_min": 500.0,
        "divergences": 0,
        "total_draws": 4000,
        "is_smoke": False,
    }


def test_gate_passes_on_healthy_diagnostics(tmp_path: Path) -> None:
    _write_diagnostics(tmp_path, json.dumps(_healthy_payload()))
    _write_healthy_held_out(tmp_path)
    report = _validate_bayes(_bayes_manifest("aid"), tmp_path)
    assert report.status == "passed"
    assert not [f for f in report.findings if f.severity == "block"]


def test_gate_blocks_missing_diagnostics_file(tmp_path: Path) -> None:
    codes = _block_codes(tmp_path)
    assert "bayes_missing_diagnostics" in codes


def test_gate_blocks_non_object_json(tmp_path: Path) -> None:
    _write_diagnostics(tmp_path, json.dumps([1, 2, 3]))
    assert "bayes_malformed_diagnostics" in _block_codes(tmp_path)


def test_gate_blocks_invalid_json(tmp_path: Path) -> None:
    _write_diagnostics(tmp_path, "{not valid json")
    assert "bayes_malformed_diagnostics" in _block_codes(tmp_path)


def test_gate_blocks_empty_object(tmp_path: Path) -> None:
    _write_diagnostics(tmp_path, json.dumps({}))
    codes = _block_codes(tmp_path)
    assert "bayes_missing_rhat" in codes
    assert "bayes_missing_ess" in codes


def test_gate_blocks_missing_rhat(tmp_path: Path) -> None:
    payload = _healthy_payload()
    del payload["rhat_max"]
    _write_diagnostics(tmp_path, json.dumps(payload))
    codes = _block_codes(tmp_path)
    assert "bayes_missing_rhat" in codes
    assert "bayes_missing_ess" not in codes


def test_gate_blocks_null_rhat(tmp_path: Path) -> None:
    payload = _healthy_payload()
    payload["rhat_max"] = None
    _write_diagnostics(tmp_path, json.dumps(payload))
    assert "bayes_missing_rhat" in _block_codes(tmp_path)


def test_gate_blocks_nan_rhat(tmp_path: Path) -> None:
    payload = _healthy_payload()
    payload["rhat_max"] = float("nan")
    _write_diagnostics(tmp_path, json.dumps(payload))
    assert "bayes_missing_rhat" in _block_codes(tmp_path)


def test_gate_blocks_missing_ess(tmp_path: Path) -> None:
    payload = _healthy_payload()
    del payload["ess_bulk_min"]
    _write_diagnostics(tmp_path, json.dumps(payload))
    assert "bayes_missing_ess" in _block_codes(tmp_path)


def test_gate_blocks_nan_ess(tmp_path: Path) -> None:
    payload = _healthy_payload()
    payload["ess_bulk_min"] = float("nan")
    _write_diagnostics(tmp_path, json.dumps(payload))
    assert "bayes_missing_ess" in _block_codes(tmp_path)


def test_weak_thresholds_derive_from_convergence_config() -> None:
    for is_smoke, base in (
        (False, _BAYES_THRESHOLDS_DEFAULT),
        (True, _BAYES_THRESHOLDS_SMOKE),
    ):
        weak = weak_identification_thresholds(is_smoke)
        assert weak["ess_bulk_min"] == (
            base["ess_bulk_min"] * _WEAK_IDENTIFICATION_ESS_BULK_MULTIPLIER
        )
        assert weak["rhat_max"] == (
            1.0 + (base["rhat_max"] - 1.0) * _WEAK_IDENTIFICATION_RHAT_MARGIN_FRACTION
        )


def test_weak_thresholds_sit_inside_convergence_band() -> None:
    for is_smoke, base in (
        (False, _BAYES_THRESHOLDS_DEFAULT),
        (True, _BAYES_THRESHOLDS_SMOKE),
    ):
        weak = weak_identification_thresholds(is_smoke)
        assert weak["ess_bulk_min"] > base["ess_bulk_min"]
        assert 1.0 < weak["rhat_max"] < base["rhat_max"]


def test_flag_unset_on_well_identified_fit() -> None:
    for is_smoke in (False, True):
        weak = weak_identification_thresholds(is_smoke)
        assert not diagnostics_indicate_weak_identification(
            rhat_max=1.0,
            ess_bulk_min=weak["ess_bulk_min"] * 2.0,
            divergences=0,
            is_smoke=is_smoke,
        )


def test_flag_set_when_ess_below_comfort_threshold() -> None:
    for is_smoke in (False, True):
        weak = weak_identification_thresholds(is_smoke)
        below = weak["ess_bulk_min"] * 0.99
        above = weak["ess_bulk_min"] * 1.01
        assert diagnostics_indicate_weak_identification(
            rhat_max=1.0, ess_bulk_min=below, divergences=0, is_smoke=is_smoke
        )
        assert not diagnostics_indicate_weak_identification(
            rhat_max=1.0, ess_bulk_min=above, divergences=0, is_smoke=is_smoke
        )


def test_flag_set_when_rhat_above_comfort_threshold() -> None:
    for is_smoke in (False, True):
        weak = weak_identification_thresholds(is_smoke)
        comfort_rhat = weak["rhat_max"]
        ess_ok = weak["ess_bulk_min"] * 2.0
        assert diagnostics_indicate_weak_identification(
            rhat_max=comfort_rhat + 0.001,
            ess_bulk_min=ess_ok,
            divergences=0,
            is_smoke=is_smoke,
        )
        assert not diagnostics_indicate_weak_identification(
            rhat_max=comfort_rhat - 0.001,
            ess_bulk_min=ess_ok,
            divergences=0,
            is_smoke=is_smoke,
        )


def test_flag_set_on_any_divergence() -> None:
    weak = weak_identification_thresholds(False)
    assert diagnostics_indicate_weak_identification(
        rhat_max=1.0,
        ess_bulk_min=weak["ess_bulk_min"] * 2.0,
        divergences=1,
        is_smoke=False,
    )


def test_flag_set_on_non_finite_diagnostics() -> None:
    weak = weak_identification_thresholds(False)
    ess_ok = weak["ess_bulk_min"] * 2.0
    assert diagnostics_indicate_weak_identification(
        rhat_max=math.nan, ess_bulk_min=ess_ok, divergences=0
    )
    assert diagnostics_indicate_weak_identification(
        rhat_max=1.0, ess_bulk_min=math.nan, divergences=0
    )
    assert diagnostics_indicate_weak_identification(
        rhat_max=math.inf, ess_bulk_min=ess_ok, divergences=0
    )


def _findings_by_code(artifact_dir: Path) -> dict[str, str]:
    report = _validate_bayes(_bayes_manifest("aid"), artifact_dir)
    return {f.code: f.severity for f in report.findings}


def _thresholds(is_smoke: bool) -> dict[str, float]:
    return _BAYES_THRESHOLDS_SMOKE if is_smoke else _BAYES_THRESHOLDS_DEFAULT


def _healthy_payload_for(is_smoke: bool) -> dict[str, object]:
    thresholds = _thresholds(is_smoke)
    return {
        "rhat_max": 1.0,
        "ess_bulk_min": thresholds["ess_bulk_min"] * 10.0,
        "divergences": 0,
        "total_draws": 4000,
        "is_smoke": is_smoke,
    }


def _past(threshold: float, *, direction: float) -> float:
    return math.nextafter(threshold, direction)


def _convergence_cases() -> list[tuple[str, str, bool, dict[str, object], dict[str, object]]]:
    cases: list[tuple[str, str, bool, dict[str, object], dict[str, object]]] = []
    for is_smoke in (False, True):
        thresholds = _thresholds(is_smoke)
        draws = 4000
        cases.append(
            (
                "bayes_high_rhat",
                "block",
                is_smoke,
                {"rhat_max": _past(thresholds["rhat_max"], direction=math.inf)},
                {"rhat_max": thresholds["rhat_max"]},
            )
        )
        cases.append(
            (
                "bayes_low_ess",
                "block",
                is_smoke,
                {"ess_bulk_min": _past(thresholds["ess_bulk_min"], direction=-math.inf)},
                {"ess_bulk_min": thresholds["ess_bulk_min"]},
            )
        )
        limit = int(thresholds["divergence_fraction"] * draws)
        cases.append(
            (
                "bayes_divergences",
                "block",
                is_smoke,
                {"divergences": limit + 1, "total_draws": draws},
                {"divergences": limit, "total_draws": draws},
            )
        )
        cases.append(
            (
                "bayes_divergences",
                "block",
                is_smoke,
                {"divergences": 1, "total_draws": 0},
                {"divergences": 0, "total_draws": 0},
            )
        )
        cases.append(
            (
                "bayes_calibration_ece",
                "warn",
                is_smoke,
                {
                    "calibration_ece": _past(
                        thresholds["calibration_ece_warn"], direction=math.inf
                    )
                },
                {"calibration_ece": thresholds["calibration_ece_warn"]},
            )
        )
        cases.append(
            (
                "bayes_post_pred_bucket_dev",
                "warn",
                is_smoke,
                {
                    "posterior_predictive_max_bucket_dev": _past(
                        thresholds["post_pred_bucket_dev_warn"], direction=math.inf
                    )
                },
                {
                    "posterior_predictive_max_bucket_dev": thresholds[
                        "post_pred_bucket_dev_warn"
                    ]
                },
            )
        )
    return cases


@pytest.mark.parametrize(
    ("code", "severity", "is_smoke", "past", "inside"),
    _convergence_cases(),
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_convergence_gate_fires_just_past_threshold_and_not_inside(
    tmp_path: Path,
    code: str,
    severity: str,
    is_smoke: bool,
    past: dict[str, object],
    inside: dict[str, object],
) -> None:
    past_dir = tmp_path / "past"
    _write_diagnostics(past_dir, json.dumps({**_healthy_payload_for(is_smoke), **past}))
    _write_healthy_held_out(past_dir)
    fired = _findings_by_code(past_dir)
    assert fired.get(code) == severity, fired

    inside_dir = tmp_path / "inside"
    _write_diagnostics(
        inside_dir, json.dumps({**_healthy_payload_for(is_smoke), **inside})
    )
    _write_healthy_held_out(inside_dir)
    quiet = _findings_by_code(inside_dir)
    assert code not in quiet, quiet
    assert _validate_bayes(_bayes_manifest("aid"), inside_dir).status == "passed"


def test_smoke_divergence_limit_scales_with_total_draws(tmp_path: Path) -> None:
    fraction = _BAYES_THRESHOLDS_SMOKE["divergence_fraction"]
    assert fraction > 0.0
    for draws in (100, 4000):
        limit = int(fraction * draws)
        artifact_dir = tmp_path / str(draws)
        _write_diagnostics(
            artifact_dir,
            json.dumps(
                {
                    **_healthy_payload_for(True),
                    "divergences": limit + 1,
                    "total_draws": draws,
                }
            ),
        )
        _write_healthy_held_out(artifact_dir)
        assert _findings_by_code(artifact_dir).get("bayes_divergences") == "block"
