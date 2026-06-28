"""Atomic-write directory helpers for Bayes artifact layout.

A Bayes artifact directory looks like::

    artifacts/statistical/bayes/<model_name>/<artifact_id>/
        manifest.json
        inference/
            prior_predictive.nc
            posterior.nc
            posterior_predictive.nc
        exports/
            posterior_summary.parquet
            calibration_curve.parquet
        validation/
            diagnostics.json
"""

from __future__ import annotations

from pathlib import Path

from python_models.statistical.config import BAYES_ROOT


def bayes_artifact_dir(
    model_name: str, artifact_id: str, *, root: Path = BAYES_ROOT
) -> Path:
    return root / model_name / artifact_id


def bayes_inference_dir(
    model_name: str, artifact_id: str, *, root: Path = BAYES_ROOT
) -> Path:
    return bayes_artifact_dir(model_name, artifact_id, root=root) / "inference"


def bayes_exports_dir(
    model_name: str, artifact_id: str, *, root: Path = BAYES_ROOT
) -> Path:
    return bayes_artifact_dir(model_name, artifact_id, root=root) / "exports"


def bayes_validation_dir(
    model_name: str, artifact_id: str, *, root: Path = BAYES_ROOT
) -> Path:
    return bayes_artifact_dir(model_name, artifact_id, root=root) / "validation"
