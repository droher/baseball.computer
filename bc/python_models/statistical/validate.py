"""Cross-phase artifact validation library.

``validate_artifact(artifact_id)`` discovers the named artifact under any
of the well-known roots (datasets / deep / bayes / eda) and dispatches
on ``manifest.kind``. Each kind has its own check suite. The result is
a ``ValidationReport`` — Pydantic-serializable JSON — written to
``<artifact_dir>/validation/validation_report.json`` by callers (the CLI
handler does this) so downstream gates can pick it up.

Deep validation checks probability normalization, event-key uniqueness,
OOF provenance, and the no-argmax publication contract. Scientific deep
validation additionally requires aligned model and baseline predictions
with explicit held-out truth, and computes log loss, multiclass Brier
score, and classwise calibration from that shared evaluation population.
"""

from __future__ import annotations

import json
import logging
import math
import os
import tempfile
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, cast

import polars as pl
from pydantic import BaseModel

from python_models.statistical import config as _config
from python_models.statistical.manifests import read_manifest
from python_models.statistical.schemas import (
    ArtifactManifest,
    BayesVariableDiagnostics,
    EvidenceStatus,
    FindingSeverity,
    ValidationFinding,
    ValidationEvidence,
    ValidationReport,
)

if TYPE_CHECKING:
    import arviz as az

_log = logging.getLogger(__name__)

VALIDATION_GATE_VERSION: int = 3

DIAGNOSTICS_MAX_ELEMENTS_PER_VARIABLE: int = 100_000
GROUP_LEVEL_MAX_ELEMENTS: int = 512
DIAGNOSTICS_BY_VARIABLE_FILENAME: str = "diagnostics_by_variable.json"
SAMPLE_DIMS: frozenset[str] = frozenset({"chain", "draw"})

_PROHIBITED_DEEP_COLUMNS: frozenset[str] = frozenset(
    {"predicted_class", "argmax_class", "argmax", "dl_argmax_class"}
)


def validate_artifact(
    artifact_id: str,
    *,
    model_name: str | None = None,
    candidate_roots: tuple[Path, ...] | None = None,
    diagnostics_overrides: Mapping[str, object] | None = None,
) -> ValidationReport:
    """Grade one artifact by its ``manifest.kind``.

    ``diagnostics_overrides`` overlays keys onto the Bayes artifact's
    ``validation/diagnostics.json`` payload before grading, so a caller that
    has recomputed the convergence pair from the saved posterior grades those
    numbers without writing them to disk. It is ignored for other kinds.
    """
    roots = candidate_roots or (
        _config.DEEP_ROOT,
        _config.BAYES_ROOT,
        _config.DATASETS_ROOT,
        _config.EDA_ROOT,
    )
    manifest_path = find_manifest(artifact_id, roots, model_name=model_name)
    return validate_manifest(manifest_path, diagnostics_overrides=diagnostics_overrides)


def validate_manifest(
    manifest_path: Path,
    *,
    diagnostics_overrides: Mapping[str, object] | None = None,
) -> ValidationReport:
    """Grade the artifact at an exact manifest path without rediscovery."""
    manifest = read_manifest(manifest_path)
    artifact_dir = manifest_path.parent

    match manifest.kind:
        case "deep":
            return _validate_deep(manifest, artifact_dir)
        case "dataset":
            return _validate_dataset(manifest, artifact_dir)
        case "bayes":
            return _validate_bayes(
                manifest, artifact_dir, diagnostics_overrides=diagnostics_overrides
            )
        case other:
            return ValidationReport(
                artifact_id=manifest.artifact_id,
                kind=manifest.kind,
                name=manifest.name,
                status="exploratory",
                findings=(
                    ValidationFinding(
                        severity="info",
                        code="validate_no_dispatch",
                        message=f"no validator registered for kind {other!r}",
                    ),
                ),
                generated_at=datetime.now(tz=timezone.utc),
            )


def write_json_atomic(path: Path, payload: str) -> None:
    _atomic_write_json(path, payload)


def compute_posterior_diagnostics_from_file(path: Path) -> PosteriorDiagnostics:
    import arviz as az

    idata = az.from_netcdf(path)
    try:
        return compute_posterior_diagnostics(idata)
    finally:
        for group in idata.groups():
            idata[group].close()


def find_manifest(
    artifact_id: str, roots: tuple[Path, ...], *, model_name: str | None = None
) -> Path:
    for root in roots:
        if not root.exists():
            continue
        if model_name is not None:
            candidate = root / model_name / artifact_id / "manifest.json"
            if candidate.exists():
                return candidate
            continue
        matches = list(root.rglob(f"{artifact_id}/manifest.json"))
        if len(matches) > 1:
            models = sorted({p.parent.parent.name for p in matches})
            raise FileNotFoundError(
                f"artifact_id={artifact_id!r} is ambiguous under {root} "
                f"(matches under models {models}); pass --model to disambiguate"
            )
        if matches:
            return matches[0]
    raise FileNotFoundError(
        f"no manifest.json for artifact_id={artifact_id!r}"
        + (f" under model {model_name!r}" if model_name else "")
        + f" in {[str(r) for r in roots]}"
    )


def _validate_deep(manifest: ArtifactManifest, artifact_dir: Path) -> ValidationReport:
    findings: list[ValidationFinding] = []
    metrics: dict[str, float | int] = {}

    probabilities_path = artifact_dir / "exports" / "probabilities.parquet"
    if not probabilities_path.exists():
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_missing_probabilities",
                message=f"probabilities.parquet not found at {probabilities_path}",
            )
        )
        return _finalize(
            manifest,
            findings,
            metrics,
            evidence=ValidationEvidence(numerical="failed"),
        )

    probs = pl.read_parquet(probabilities_path)
    metrics["row_count"] = int(probs.height)
    if probs.is_empty():
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_probabilities_empty",
                message="probabilities.parquet has zero rows",
            )
        )

    schema_findings, schema_metrics = _check_deep_schema(probs)
    findings.extend(schema_findings)
    metrics.update(schema_metrics)

    norm_findings, norm_metrics = _check_probability_normalization(probs)
    findings.extend(norm_findings)
    metrics.update(norm_metrics)

    uniq_findings, uniq_metrics = _check_grain_uniqueness_per_partition(probs)
    findings.extend(uniq_findings)
    metrics.update(uniq_metrics)

    fold_findings, fold_metrics = _check_oof_fold_integrity(probs)
    findings.extend(fold_findings)
    metrics.update(fold_metrics)

    baseline_path = artifact_dir / "exports" / "baseline_predictions.parquet"
    if baseline_path.exists():
        compare_findings, compare_metrics = _compare_against_baseline(
            manifest, probs, baseline_path
        )
        findings.extend(compare_findings)
        metrics.update(compare_metrics)
    else:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_baseline_absent",
                message=(
                    "no exports/baseline_predictions.parquet alongside the artifact; "
                    "predictive and calibration evidence are unsupported"
                ),
            )
        )

    numerical_failed = any(
        finding.severity == "block"
        and finding.code != "deep_baseline_absent"
        and not finding.code.startswith("deep_baseline_")
        and not finding.code.startswith("deep_predictive_")
        and not finding.code.startswith("deep_calibration_")
        for finding in findings
    )
    predictive_failed = any(
        finding.severity == "block"
        and (
            finding.code == "deep_baseline_absent"
            or finding.code.startswith("deep_baseline_")
            or finding.code.startswith("deep_predictive_")
        )
        for finding in findings
    )
    calibration_failed = any(
        finding.severity == "block"
        and (
            finding.code == "deep_baseline_absent"
            or finding.code.startswith("deep_baseline_")
            or finding.code.startswith("deep_calibration_")
        )
        for finding in findings
    )
    evidence = ValidationEvidence(
        numerical="failed" if numerical_failed else "passed",
        predictive="failed" if predictive_failed else "passed",
        calibration="failed" if calibration_failed else "passed",
    )
    return _finalize(manifest, findings, metrics, evidence=evidence)


def _check_deep_schema(
    probs: pl.DataFrame,
) -> tuple[list[ValidationFinding], dict[str, float | int]]:
    findings: list[ValidationFinding] = []
    columns = set(probs.columns)
    metrics: dict[str, float | int] = {"column_count": len(columns)}

    if "dl_p_class" not in columns:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_missing_dl_p_class",
                message="probabilities.parquet is missing the dl_p_class column",
            )
        )
    else:
        dtype = probs.schema["dl_p_class"]
        if not isinstance(dtype, pl.List):
            findings.append(
                ValidationFinding(
                    severity="block",
                    code="deep_dl_p_class_not_list",
                    message=f"dl_p_class must be a LIST type; got {dtype}",
                )
            )

    if "partition" not in columns:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_missing_partition",
                message="probabilities.parquet has no `partition` column",
            )
        )
    elif probs.schema["partition"] != pl.String:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_partition_not_string",
                message=f"partition must be a string type; got {probs.schema['partition']}",
            )
        )
    elif probs.get_column("partition").null_count() > 0:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_partition_null",
                message="partition contains null values",
            )
        )

    prohibited = columns & _PROHIBITED_DEEP_COLUMNS
    if prohibited:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_argmax_column_exposed",
                message=(
                    "probabilities.parquet exposes prohibited argmax-style columns: "
                    f"{sorted(prohibited)}"
                ),
            )
        )

    return findings, metrics


def _check_probability_normalization(
    probs: pl.DataFrame, *, tol: float = 1e-3
) -> tuple[list[ValidationFinding], dict[str, float | int]]:
    if "dl_p_class" not in probs.columns:
        return [], {}
    n_bad = sum(
        not _probability_vector_is_valid(row, tol=tol)
        for row in probs.get_column("dl_p_class").to_list()
    )
    metrics: dict[str, float | int] = {
        "probability_normalization_violations": n_bad,
    }
    if n_bad == 0:
        return [], metrics
    return (
        [
            ValidationFinding(
                severity="block",
                code="deep_probability_not_normalized",
                message=(f"{n_bad} dl_p_class rows do not sum to 1 within {tol}"),
            )
        ],
        metrics,
    )


def _check_grain_uniqueness_per_partition(
    probs: pl.DataFrame,
) -> tuple[list[ValidationFinding], dict[str, float | int]]:
    if "partition" not in probs.columns:
        return [], {}
    if "event_key" not in probs.columns:
        return (
            [
                ValidationFinding(
                    severity="block",
                    code="deep_missing_event_key",
                    message="probabilities.parquet has no event_key column",
                )
            ],
            {},
        )
    grain_cols = ("event_key",)
    dup = (
        probs.group_by([*grain_cols, "partition"])
        .agg(pl.len().alias("_n"))
        .filter(pl.col("_n") > 1)
    )
    n_dup = int(dup.height)
    metrics: dict[str, float | int] = {"grain_uniqueness_violations": n_dup}
    if n_dup == 0:
        return [], metrics
    return (
        [
            ValidationFinding(
                severity="block",
                code="deep_grain_not_unique_per_partition",
                message=(
                    f"{n_dup} grain rows appear more than once within a single partition; "
                    f"grain columns: {grain_cols}"
                ),
            )
        ],
        metrics,
    )


def _check_oof_fold_integrity(
    probs: pl.DataFrame,
) -> tuple[list[ValidationFinding], dict[str, float | int]]:
    if "partition" not in probs.columns:
        return [], {}
    if "fold_id" not in probs.columns:
        return (
            [
                ValidationFinding(
                    severity="warn",
                    code="deep_oof_fold_provenance_unverifiable",
                    message=(
                        "probabilities.parquet has no fold_id column; OOF fold "
                        "provenance is unverifiable (artifact predates fold stamping)"
                    ),
                )
            ],
            {},
        )

    findings: list[ValidationFinding] = []
    metrics: dict[str, float | int] = {}

    oof = probs.filter(pl.col("partition") == "OOF")
    n_oof = int(oof.height)
    metrics["oof_row_count"] = n_oof

    n_null_oof = int(oof.filter(pl.col("fold_id").is_null()).height)
    metrics["oof_null_fold_id_rows"] = n_null_oof
    if n_null_oof > 0:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_oof_fold_id_null",
                message=f"{n_null_oof} OOF rows have a null fold_id",
            )
        )

    n_distinct_folds = int(oof.get_column("fold_id").drop_nulls().n_unique())
    metrics["oof_distinct_fold_count"] = n_distinct_folds
    if n_distinct_folds < 2:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_oof_single_fold",
                message=(
                    f"OOF partition carries {n_distinct_folds} distinct fold_id "
                    "value(s); out-of-fold predictions require at least 2"
                ),
            )
        )

    n_non_oof_fold = int(
        probs.filter(
            (pl.col("partition") != "OOF") & pl.col("fold_id").is_not_null()
        ).height
    )
    metrics["non_oof_fold_id_rows"] = n_non_oof_fold
    if n_non_oof_fold > 0:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_fold_id_outside_oof",
                message=(
                    f"{n_non_oof_fold} non-OOF rows carry a non-null fold_id; "
                    "fold_id must be null outside the OOF partition"
                ),
            )
        )

    grain_cols = ["event_key"] if "event_key" in probs.columns else []
    if grain_cols:
        n_cross = int(
            probs.group_by(grain_cols)
            .agg(pl.col("partition").n_unique().alias("_n_partitions"))
            .filter(pl.col("_n_partitions") > 1)
            .height
        )
        metrics["cross_partition_grain_violations"] = n_cross
        if n_cross > 0:
            findings.append(
                ValidationFinding(
                    severity="block",
                    code="deep_grain_spans_partitions",
                    message=(
                        f"{n_cross} grain values appear in more than one partition; "
                        f"grain columns: {tuple(grain_cols)}"
                    ),
                )
            )

    return findings, metrics


def _compare_against_baseline(
    manifest: ArtifactManifest, probs: pl.DataFrame, baseline_path: Path
) -> tuple[list[ValidationFinding], dict[str, float | int]]:
    try:
        baseline = pl.read_parquet(baseline_path)
    except (OSError, pl.exceptions.PolarsError) as exc:
        return (
            [
                ValidationFinding(
                    severity="block",
                    code="deep_baseline_unreadable",
                    message=f"cannot read baseline predictions at {baseline_path}: {exc}",
                )
            ],
            {},
        )
    metrics: dict[str, float | int] = {
        "baseline_row_count": int(baseline.height),
    }
    findings: list[ValidationFinding] = []
    probability_column = (
        "baseline_p_class"
        if "baseline_p_class" in baseline.columns
        else "dl_p_class"
        if "dl_p_class" in baseline.columns
        else None
    )
    required = {"event_key", "partition", "target_class"}
    missing = sorted(required - set(baseline.columns))
    if probability_column is None:
        missing.append("baseline_p_class")
    missing_model = sorted(
        {"event_key", "partition", "dl_p_class"} - set(probs.columns)
    )
    if missing or missing_model:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_baseline_contract_missing",
                message=(
                    f"baseline comparison requires event_key, partition, target_class, "
                    f"and baseline probabilities; missing baseline={missing}, "
                    f"model={missing_model}"
                ),
            )
        )
        return findings, metrics

    key_columns = ["event_key", "partition"]
    if any(
        frame.get_column(column).null_count() > 0
        for frame in (probs, baseline)
        for column in key_columns
    ):
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_baseline_null_key",
                message="model and baseline comparison keys must be non-null",
            )
        )
        return findings, metrics
    model_duplicates = int(probs.select(key_columns).is_duplicated().sum())
    baseline_duplicates = int(baseline.select(key_columns).is_duplicated().sum())
    metrics["baseline_model_duplicate_keys"] = model_duplicates
    metrics["baseline_duplicate_keys"] = baseline_duplicates
    if model_duplicates or baseline_duplicates:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_baseline_duplicate_keys",
                message=(
                    f"comparison keys must be unique; model duplicates={model_duplicates}, "
                    f"baseline duplicates={baseline_duplicates}"
                ),
            )
        )
        return findings, metrics

    raw_partition = manifest.metadata.get("validation_partition", "TEST")
    evaluation_partition = (
        raw_partition if isinstance(raw_partition, str) and raw_partition else "TEST"
    )
    if any(probs.schema[column] != baseline.schema[column] for column in key_columns):
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_baseline_key_type_mismatch",
                message="model and baseline event_key and partition types must match",
            )
        )
        return findings, metrics
    model_eval = probs.filter(pl.col("partition") == evaluation_partition)
    baseline_eval = baseline.filter(pl.col("partition") == evaluation_partition)
    metrics["baseline_evaluation_row_count"] = int(baseline_eval.height)
    if model_eval.is_empty() or baseline_eval.is_empty():
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_baseline_evaluation_empty",
                message=(
                    f"baseline comparison requires non-empty {evaluation_partition} rows; "
                    f"model={model_eval.height}, baseline={baseline_eval.height}"
                ),
            )
        )
        return findings, metrics

    model_keys = model_eval.select(key_columns)
    baseline_keys = baseline_eval.select(key_columns)
    missing_from_baseline = int(
        model_keys.join(baseline_keys, on=key_columns, how="anti").height
    )
    missing_from_model = int(
        baseline_keys.join(model_keys, on=key_columns, how="anti").height
    )
    metrics["baseline_missing_model_keys"] = missing_from_model
    metrics["baseline_missing_baseline_keys"] = missing_from_baseline
    if missing_from_baseline or missing_from_model:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_baseline_key_mismatch",
                message=(
                    "model and baseline evaluation rows must align exactly on "
                    f"event_key and partition; missing baseline={missing_from_baseline}, "
                    f"missing model={missing_from_model}"
                ),
            )
        )
        return findings, metrics

    joined = model_eval.select("event_key", "partition", "dl_p_class").join(
        baseline_eval.select(
            "event_key",
            "partition",
            "target_class",
            pl.col(cast(str, probability_column)).alias("baseline_p_class"),
        ),
        on=key_columns,
        how="inner",
    )
    labels_path = baseline_path.parent / "class_labels.json"
    labels = _read_deep_class_labels(labels_path)
    if labels is None:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_baseline_class_labels_unavailable",
                message=f"class labels are missing or malformed at {labels_path}",
            )
        )
        return findings, metrics

    model_rows = joined.get_column("dl_p_class").to_list()
    baseline_rows = joined.get_column("baseline_p_class").to_list()
    truth_rows = joined.get_column("target_class").to_list()
    if any(not _probability_vector_is_valid(row) for row in baseline_rows):
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_baseline_probability_invalid",
                message="baseline probabilities contain null, non-finite, negative, or unnormalized rows",
            )
        )
        return findings, metrics
    if any(not _probability_vector_is_valid(row) for row in model_rows):
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_predictive_probability_invalid",
                message="model probabilities contain null, non-finite, negative, or unnormalized rows",
            )
        )
        return findings, metrics
    if any(len(row) != len(labels) for row in [*model_rows, *baseline_rows]):
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_baseline_class_count_mismatch",
                message=(
                    f"probability vectors must match {len(labels)} declared class labels"
                ),
            )
        )
        return findings, metrics

    label_index = {label: index for index, label in enumerate(labels)}
    if any(
        not isinstance(value, str) or value not in label_index for value in truth_rows
    ):
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_baseline_truth_invalid",
                message="target_class contains null or values absent from class_labels.json",
            )
        )
        return findings, metrics
    truth = [label_index[cast(str, value)] for value in truth_rows]
    model_vectors = [cast(list[float], row) for row in model_rows]
    baseline_vectors = [cast(list[float], row) for row in baseline_rows]
    model_log_loss, model_brier, model_ece = _multiclass_metrics(model_vectors, truth)
    baseline_log_loss, baseline_brier, baseline_ece = _multiclass_metrics(
        baseline_vectors, truth
    )
    metrics.update(
        {
            "deep_evaluated_rows": len(truth),
            "deep_model_log_loss": model_log_loss,
            "deep_baseline_log_loss": baseline_log_loss,
            "deep_log_loss_improvement": baseline_log_loss - model_log_loss,
            "deep_model_brier_score": model_brier,
            "deep_baseline_brier_score": baseline_brier,
            "deep_brier_improvement": baseline_brier - model_brier,
            "deep_model_classwise_ece": model_ece,
            "deep_baseline_classwise_ece": baseline_ece,
        }
    )
    if model_log_loss >= baseline_log_loss:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_predictive_log_loss_not_beating_baseline",
                message=(
                    f"model log loss {model_log_loss:.6f} does not beat baseline "
                    f"{baseline_log_loss:.6f} on aligned {evaluation_partition} rows"
                ),
            )
        )
    if model_brier >= baseline_brier:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_predictive_brier_not_beating_baseline",
                message=(
                    f"model Brier score {model_brier:.6f} does not beat baseline "
                    f"{baseline_brier:.6f} on aligned {evaluation_partition} rows"
                ),
            )
        )
    return findings, metrics


def _probability_vector_is_valid(value: object, *, tol: float = 1e-3) -> bool:
    if not isinstance(value, list) or not value:
        return False
    probabilities = [_finite_float(item) for item in value]
    if any(item is None or item < 0.0 or item > 1.0 for item in probabilities):
        return False
    return math.isclose(sum(cast(list[float], probabilities)), 1.0, abs_tol=tol)


def _read_deep_class_labels(path: Path) -> tuple[str, ...] | None:
    if not path.exists():
        return None
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(loaded, dict):
        return None
    labels = cast(dict[str, object], loaded).get("labels")
    if not isinstance(labels, list) or not labels:
        return None
    if any(not isinstance(label, str) or not label for label in labels):
        return None
    string_labels = cast(list[str], labels)
    if len(set(string_labels)) != len(string_labels):
        return None
    return tuple(string_labels)


def _multiclass_metrics(
    probabilities: list[list[float]], truth: list[int], *, n_bins: int = 10
) -> tuple[float, float, float]:
    n_rows = len(truth)
    n_classes = len(probabilities[0])
    log_loss = (
        -sum(
            math.log(max(probabilities[row][target], 1e-15))
            for row, target in enumerate(truth)
        )
        / n_rows
    )
    brier = (
        sum(
            sum(
                (probability - (1.0 if class_index == truth[row] else 0.0)) ** 2
                for class_index, probability in enumerate(vector)
            )
            for row, vector in enumerate(probabilities)
        )
        / n_rows
    )
    classwise_ece = 0.0
    for class_index in range(n_classes):
        class_ece = 0.0
        for bin_index in range(n_bins):
            lower = bin_index / n_bins
            upper = (bin_index + 1) / n_bins
            selected = [
                row
                for row, vector in enumerate(probabilities)
                if vector[class_index] >= lower
                and (
                    vector[class_index] < upper
                    or (bin_index == n_bins - 1 and vector[class_index] <= upper)
                )
            ]
            if not selected:
                continue
            confidence = sum(probabilities[row][class_index] for row in selected) / len(
                selected
            )
            observed = sum(truth[row] == class_index for row in selected) / len(
                selected
            )
            class_ece += len(selected) / n_rows * abs(confidence - observed)
        classwise_ece += class_ece
    return log_loss, brier, classwise_ece / n_classes


def _validate_dataset(
    manifest: ArtifactManifest, artifact_dir: Path
) -> ValidationReport:
    findings: list[ValidationFinding] = []
    metrics: dict[str, float | int] = {}
    dataset_parquet = artifact_dir / "dataset.parquet"
    if not dataset_parquet.exists():
        findings.append(
            ValidationFinding(
                severity="block",
                code="dataset_missing_parquet",
                message=f"dataset.parquet not found at {dataset_parquet}",
            )
        )
    else:
        n = int(pl.read_parquet(dataset_parquet).height)
        metrics["row_count"] = n
        if n == 0:
            findings.append(
                ValidationFinding(
                    severity="block",
                    code="dataset_empty",
                    message="dataset.parquet has zero rows",
                )
            )
    evidence = ValidationEvidence(
        numerical="failed"
        if any(finding.severity == "block" for finding in findings)
        else "passed"
    )
    return _finalize(manifest, findings, metrics, evidence=evidence)


_BAYES_THRESHOLDS_SMOKE: dict[str, float] = {
    "rhat_max": 1.5,
    "ess_bulk_min": 3.0,
    "divergence_fraction": 0.05,
    "calibration_ece_warn": 0.10,
    "post_pred_bucket_dev_warn": 0.10,
    "held_out_ece_warn": 0.10,
}

_BAYES_THRESHOLDS_DEFAULT: dict[str, float] = {
    "rhat_max": 1.05,
    "ess_bulk_min": 100.0,
    "divergence_fraction": 0.0,
    "calibration_ece_warn": 0.10,
    "post_pred_bucket_dev_warn": 0.05,
    "held_out_ece_warn": 0.05,
}

_WEAK_IDENTIFICATION_ESS_BULK_MULTIPLIER: float = 4.0
_WEAK_IDENTIFICATION_RHAT_MARGIN_FRACTION: float = 0.5


def weak_identification_thresholds(is_smoke: bool = False) -> dict[str, float]:
    convergence = _BAYES_THRESHOLDS_SMOKE if is_smoke else _BAYES_THRESHOLDS_DEFAULT
    return {
        "ess_bulk_min": (
            convergence["ess_bulk_min"] * _WEAK_IDENTIFICATION_ESS_BULK_MULTIPLIER
        ),
        "rhat_max": 1.0
        + (convergence["rhat_max"] - 1.0) * _WEAK_IDENTIFICATION_RHAT_MARGIN_FRACTION,
    }


def diagnostics_indicate_weak_identification(
    *,
    rhat_max: float,
    ess_bulk_min: float,
    divergences: int,
    is_smoke: bool = False,
) -> bool:
    thresholds = weak_identification_thresholds(is_smoke)
    if not math.isfinite(rhat_max) or not math.isfinite(ess_bulk_min):
        return True
    if divergences > 0:
        return True
    return (
        ess_bulk_min < thresholds["ess_bulk_min"] or rhat_max > thresholds["rhat_max"]
    )


class PosteriorDiagnostics(BaseModel):
    rhat_max: float
    ess_bulk_min: float
    ess_tail_min: float
    group_level_rhat_max: float
    group_level_ess_bulk_min: float
    divergences: int
    total_draws: int
    by_variable: tuple[BayesVariableDiagnostics, ...]
    group_level_max_elements: int = GROUP_LEVEL_MAX_ELEMENTS

    @property
    def excluded_variables(self) -> tuple[str, ...]:
        return tuple(row.name for row in self.by_variable if not row.diagnosed)

    @property
    def group_level_variables(self) -> tuple[str, ...]:
        return tuple(
            row.name
            for row in self.by_variable
            if row.diagnosed and row.n_elements <= self.group_level_max_elements
        )


def _finite_extreme(values: object, *, largest: bool) -> float | None:
    import numpy as np

    arr = np.asarray(values, dtype=np.float64).ravel()
    finite = arr[np.isfinite(arr)]
    if finite.size == 0:
        return None
    return float(finite.max() if largest else finite.min())


def nan_if_none(value: float | None) -> float:
    return float("nan") if value is None else value


def compute_posterior_diagnostics(
    idata: az.InferenceData,
    *,
    max_elements_per_variable: int = DIAGNOSTICS_MAX_ELEMENTS_PER_VARIABLE,
    group_level_max_elements: int = GROUP_LEVEL_MAX_ELEMENTS,
) -> PosteriorDiagnostics:
    """Convergence diagnostics over every posterior variable.

    ``rhat_max`` / ``ess_*_min`` reduce over every element of every
    variable whose non-sample element count is at most
    ``max_elements_per_variable``; larger variables are skipped (recorded
    with ``diagnosed=False``) and logged. The ``group_level_*`` pair
    reduces over the diagnosed variables with at most
    ``group_level_max_elements`` elements — scalars and small group-level
    effects — and is the pair the weak-identification flag reads.
    """
    import arviz as az
    import numpy as np
    import xarray as xr

    posterior = cast(xr.Dataset, idata["posterior"])
    rows: list[BayesVariableDiagnostics] = []
    rhat_all: list[float] = []
    ess_bulk_all: list[float] = []
    ess_tail_all: list[float] = []
    rhat_group: list[float] = []
    ess_bulk_group: list[float] = []
    for raw_name in posterior.data_vars:
        name = str(raw_name)
        variable = posterior[name]
        n_elements = int(
            np.prod(
                [
                    size
                    for dim, size in variable.sizes.items()
                    if dim not in SAMPLE_DIMS
                ],
                dtype=np.int64,
            )
        )
        if n_elements > max_elements_per_variable:
            _log.warning(
                "posterior diagnostics skipping %s: %d elements exceeds cap %d",
                name,
                n_elements,
                max_elements_per_variable,
            )
            rows.append(
                BayesVariableDiagnostics(
                    name=name, n_elements=n_elements, diagnosed=False
                )
            )
            continue
        rhat_ds = cast(xr.Dataset, az.rhat(idata, var_names=[name]))
        ess_bulk_ds = cast(xr.Dataset, az.ess(idata, var_names=[name], method="bulk"))
        ess_tail_ds = cast(xr.Dataset, az.ess(idata, var_names=[name], method="tail"))
        rhat = _finite_extreme(rhat_ds[name].values, largest=True)
        ess_bulk = _finite_extreme(ess_bulk_ds[name].values, largest=False)
        ess_tail = _finite_extreme(ess_tail_ds[name].values, largest=False)
        rows.append(
            BayesVariableDiagnostics(
                name=name,
                n_elements=n_elements,
                rhat_max=rhat,
                ess_bulk_min=ess_bulk,
                ess_tail_min=ess_tail,
            )
        )
        if rhat is not None:
            rhat_all.append(rhat)
        if ess_bulk is not None:
            ess_bulk_all.append(ess_bulk)
        if ess_tail is not None:
            ess_tail_all.append(ess_tail)
        if n_elements <= group_level_max_elements:
            if rhat is not None:
                rhat_group.append(rhat)
            if ess_bulk is not None:
                ess_bulk_group.append(ess_bulk)

    sample_stats = getattr(idata, "sample_stats", None)
    divergences = 0
    if sample_stats is not None and "diverging" in sample_stats:
        divergences = int(np.asarray(sample_stats["diverging"].values).sum())
    total_draws = int(posterior.sizes["chain"] * posterior.sizes["draw"])

    return PosteriorDiagnostics(
        rhat_max=max(rhat_all) if rhat_all else float("nan"),
        ess_bulk_min=min(ess_bulk_all) if ess_bulk_all else float("nan"),
        ess_tail_min=min(ess_tail_all) if ess_tail_all else float("nan"),
        group_level_rhat_max=max(rhat_group) if rhat_group else float("nan"),
        group_level_ess_bulk_min=min(ess_bulk_group)
        if ess_bulk_group
        else float("nan"),
        divergences=divergences,
        total_draws=total_draws,
        by_variable=tuple(rows),
        group_level_max_elements=group_level_max_elements,
    )


def _atomic_write_json(target: Path, payload: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(target.parent), prefix=f".{target.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            _ = fh.write(payload)
        os.replace(tmp_name, target)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def write_diagnostics_by_variable(
    validation_dir: Path, rows: tuple[BayesVariableDiagnostics, ...]
) -> Path:
    target = validation_dir / DIAGNOSTICS_BY_VARIABLE_FILENAME
    payload = json.dumps([row.model_dump(mode="json") for row in rows], indent=2)
    _atomic_write_json(target, payload)
    return target


def read_diagnostics_by_variable(path: Path) -> tuple[BayesVariableDiagnostics, ...]:
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, list):
        raise ValueError(f"{path} is not a JSON list")
    return tuple(
        BayesVariableDiagnostics.model_validate(entry)
        for entry in cast(list[object], loaded)
    )


def manifest_declares_smoke(manifest: ArtifactManifest) -> bool:
    if bool(manifest.metadata.get("is_smoke", False)):
        return True
    extras = manifest.bayes_extras
    return extras is not None and extras.sampler_config.is_smoke


def diagnostics_file_declares_smoke(artifact_dir: Path) -> bool:
    path = artifact_dir / "validation" / "diagnostics.json"
    if not path.exists():
        return False
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, UnicodeDecodeError):
        return False
    if not isinstance(loaded, dict):
        return False
    return bool(cast(dict[str, object], loaded).get("is_smoke", False))


def manifest_is_smoke(manifest: ArtifactManifest, artifact_dir: Path) -> bool:
    """Whether any of the artifact's smoke markers is set.

    The fit records ``is_smoke`` in three places (manifest metadata, the
    sampler config, and ``validation/diagnostics.json``); any one of them
    marks the artifact as a smoke fit.
    """
    return manifest_declares_smoke(manifest) or diagnostics_file_declares_smoke(
        artifact_dir
    )


def _finite_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    return None


def _positive_float(value: object) -> float | None:
    finite = _finite_float(value)
    return finite if finite is not None and finite > 0.0 else None


def _derive_baseline_log_loss(payload: dict[str, object]) -> float | None:
    """Entropy in nats of the held-out empirical class shares, if derivable.

    Fallback for payloads written before ``baseline_log_loss`` was emitted:
    ``distribution_calibration.per_position`` records each class's
    ``empirical_share``, whose entropy is the same quantity. Returns
    ``None`` when the block is absent, degenerate, or malformed — including
    a share outside ``[0, 1]`` or a share set that does not sum to 1, since
    a partial class set yields a finite but understated entropy that would
    make the gate falsely strict.
    """
    distribution = payload.get("distribution_calibration")
    if not isinstance(distribution, dict):
        return None
    per_position = cast(dict[str, object], distribution).get("per_position")
    if not isinstance(per_position, dict):
        return None
    entropy = 0.0
    total = 0.0
    for entry in cast(dict[str, object], per_position).values():
        if not isinstance(entry, dict):
            return None
        share = _finite_float(cast(dict[str, object], entry).get("empirical_share"))
        if share is None or share < 0.0 or share > 1.0:
            return None
        total += share
        if share > 0.0:
            entropy -= share * math.log(share)
    if not math.isclose(total, 1.0, abs_tol=1e-6):
        return None
    return entropy if entropy > 0.0 else None


def _bayes_metric_family(manifest: ArtifactManifest) -> str | None:
    explicit = manifest.metadata.get("validation_metric_family")
    if explicit in {"bernoulli", "multinomial", "loglik"}:
        return cast(str, explicit)
    name = manifest.bayes_extras.model_name if manifest.bayes_extras else manifest.name
    if name.endswith("_observedness"):
        return "bernoulli"
    if name.startswith("geometry_") or name in {
        "advancement",
        "assist_credit_allocation",
        "ball_handler_imputation",
        "putout_credit_allocation",
    }:
        return "multinomial"
    if name in {
        "assist_count",
        "park_factor_runs",
        "pitch_summary",
        "run_expectancy",
        "state_transition",
    }:
        return "loglik"
    return None


def _positive_count(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    numeric = float(value)
    if not math.isfinite(numeric) or numeric <= 0.0 or not numeric.is_integer():
        return None
    return int(numeric)


def _grade_held_out_metrics(
    manifest: ArtifactManifest,
    artifact_dir: Path,
    *,
    thresholds: dict[str, float],
    is_smoke: bool,
) -> tuple[
    list[ValidationFinding], dict[str, float | int], EvidenceStatus, EvidenceStatus
]:
    findings: list[ValidationFinding] = []
    metrics: dict[str, float | int] = {}
    baseline_severity: FindingSeverity = "warn" if is_smoke else "block"
    predictive_status: EvidenceStatus = "unsupported"
    calibration_status: EvidenceStatus = "unsupported"
    path = artifact_dir / "validation" / "held_out_metrics.json"
    if not path.exists():
        findings.append(
            ValidationFinding(
                severity=baseline_severity,
                code="bayes_held_out_metrics_absent",
                message=(
                    f"no held_out_metrics.json at {path}; the fit ships no "
                    f"out-of-sample evidence (smoke={is_smoke})"
                ),
            )
        )
        return findings, metrics, predictive_status, calibration_status

    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        findings.append(
            ValidationFinding(
                severity=baseline_severity,
                code="bayes_held_out_metrics_malformed",
                message=(
                    f"held_out_metrics.json at {path} is not valid JSON: {exc} "
                    f"(smoke={is_smoke})"
                ),
            )
        )
        return findings, metrics, predictive_status, calibration_status
    if not isinstance(loaded, dict):
        findings.append(
            ValidationFinding(
                severity=baseline_severity,
                code="bayes_held_out_metrics_malformed",
                message=(
                    f"held_out_metrics.json at {path} is not a JSON object "
                    f"(got {type(loaded).__name__}; smoke={is_smoke})"
                ),
            )
        )
        return findings, metrics, predictive_status, calibration_status

    payload: dict[str, object] = cast(dict[str, object], loaded)
    family = _bayes_metric_family(manifest)
    if family is None:
        findings.append(
            ValidationFinding(
                severity=baseline_severity,
                code="bayes_held_out_metric_family_unknown",
                message=(
                    f"model {manifest.name!r} has no known held-out metric family; "
                    "set manifest metadata validation_metric_family explicitly"
                ),
            )
        )
        return findings, metrics, predictive_status, calibration_status
    metrics["held_out_metric_family"] = {"bernoulli": 1, "multinomial": 2, "loglik": 3}[
        family
    ]

    model_name = (
        manifest.bayes_extras.model_name if manifest.bayes_extras else manifest.name
    )
    count_key = "n_evaluated" if family == "multinomial" else "n_events"
    if family == "loglik" and model_name == "park_factor_runs":
        count_key = "n_games"
    elif family == "bernoulli" and "n_events_scored" in payload:
        count_key = "n_events_scored"
    evaluated = _positive_count(payload.get(count_key))
    if evaluated is None:
        findings.append(
            ValidationFinding(
                severity=baseline_severity,
                code="bayes_held_out_evaluated_count_invalid",
                message=(
                    f"{family} held-out payload requires a positive integral {count_key}; "
                    f"got {payload.get(count_key)!r} (smoke={is_smoke})"
                ),
            )
        )
        return findings, metrics, predictive_status, calibration_status
    metrics["held_out_evaluated_count"] = evaluated
    total = _positive_count(payload.get("n_events"))
    if total is not None and evaluated > total:
        findings.append(
            ValidationFinding(
                severity=baseline_severity,
                code="bayes_held_out_evaluated_count_exceeds_total",
                message=f"evaluated count {evaluated} exceeds n_events={total}",
            )
        )
        return findings, metrics, "failed", calibration_status

    if family == "bernoulli":
        roc_auc = _finite_float(payload.get("roc_auc"))
        pr_auc = _finite_float(payload.get("pr_auc"))
        baseline_pr_auc = _finite_float(payload.get("baseline_pr_auc"))
        values = {
            "roc_auc": roc_auc,
            "pr_auc": pr_auc,
            "baseline_pr_auc": baseline_pr_auc,
        }
        if any(
            value is None or value < 0.0 or value > 1.0 for value in values.values()
        ):
            findings.append(
                ValidationFinding(
                    severity=baseline_severity,
                    code="bayes_held_out_bernoulli_metrics_ungradeable",
                    message="Bernoulli held-out evidence requires finite ROC-AUC, PR-AUC, and baseline PR-AUC in [0, 1]",
                )
            )
            predictive_status = "unsupported"
        else:
            valid_roc = cast(float, roc_auc)
            valid_pr = cast(float, pr_auc)
            valid_baseline_pr = cast(float, baseline_pr_auc)
            metrics.update(
                {
                    "held_out_roc_auc": valid_roc,
                    "held_out_pr_auc": valid_pr,
                    "held_out_baseline_pr_auc": valid_baseline_pr,
                }
            )
            predictive_status = "passed"
            if valid_roc <= 0.5:
                predictive_status = "failed"
                findings.append(
                    ValidationFinding(
                        severity=baseline_severity,
                        code="bayes_held_out_auc_not_beating_baseline",
                        message=f"held-out roc_auc={valid_roc:.4f} does not beat 0.5 (smoke={is_smoke})",
                    )
                )
            if valid_pr <= valid_baseline_pr:
                predictive_status = "failed"
                findings.append(
                    ValidationFinding(
                        severity=baseline_severity,
                        code="bayes_held_out_pr_auc_not_beating_baseline",
                        message=(
                            f"held-out pr_auc={valid_pr:.4f} does not beat baseline_pr_auc="
                            f"{valid_baseline_pr:.4f} (smoke={is_smoke})"
                        ),
                    )
                )
    elif family == "multinomial":
        log_loss = _finite_float(payload.get("log_loss"))
        baseline_log_loss = _positive_float(payload.get("baseline_log_loss"))
        if baseline_log_loss is None:
            baseline_log_loss = _derive_baseline_log_loss(payload)
        if log_loss is None or log_loss < 0.0 or baseline_log_loss is None:
            findings.append(
                ValidationFinding(
                    severity=baseline_severity,
                    code="bayes_held_out_log_loss_ungradeable",
                    message=(
                        "multinomial held-out evidence requires non-negative finite log_loss "
                        "and a positive finite baseline_log_loss"
                    ),
                )
            )
            predictive_status = "unsupported"
        else:
            metrics["held_out_log_loss"] = log_loss
            metrics["held_out_baseline_log_loss"] = baseline_log_loss
            predictive_status = "passed"
            if log_loss >= baseline_log_loss:
                predictive_status = "failed"
                findings.append(
                    ValidationFinding(
                        severity=baseline_severity,
                        code="bayes_held_out_log_loss_not_beating_baseline",
                        message=(
                            f"held-out log_loss={log_loss:.4f} does not beat "
                            f"baseline_log_loss={baseline_log_loss:.4f} (smoke={is_smoke})"
                        ),
                    )
                )
        top1 = _finite_float(payload.get("top1_accuracy"))
        baseline_top1 = _finite_float(payload.get("baseline_top1_accuracy"))
        if top1 is not None and baseline_top1 is not None:
            if 0.0 <= top1 <= 1.0 and 0.0 <= baseline_top1 <= 1.0:
                metrics["held_out_top1_accuracy"] = top1
                metrics["held_out_baseline_top1_accuracy"] = baseline_top1
                if top1 <= baseline_top1:
                    findings.append(
                        ValidationFinding(
                            severity="warn",
                            code="bayes_held_out_top1_not_beating_baseline",
                            message=(
                                f"held-out top1_accuracy={top1:.4f} does not beat "
                                f"baseline_top1_accuracy={baseline_top1:.4f}; diagnostic only"
                            ),
                        )
                    )
    else:
        loglik_lift = _finite_float(payload.get("loglik_lift"))
        if loglik_lift is None:
            findings.append(
                ValidationFinding(
                    severity=baseline_severity,
                    code="bayes_held_out_loglik_ungradeable",
                    message="log-likelihood held-out evidence requires a finite loglik_lift",
                )
            )
            predictive_status = "unsupported"
        else:
            metrics["held_out_loglik_lift"] = loglik_lift
            predictive_status = "passed" if loglik_lift > 0.0 else "failed"
            if loglik_lift <= 0.0:
                findings.append(
                    ValidationFinding(
                        severity=baseline_severity,
                        code="bayes_held_out_no_loglik_lift",
                        message=(
                            f"held-out loglik_lift={loglik_lift:.4f} is not positive "
                            f"(smoke={is_smoke})"
                        ),
                    )
                )

    if family in {"bernoulli", "multinomial"}:
        ece = _finite_float(payload.get("ece_held_out"))
        if ece is None or ece < 0.0 or ece > 1.0:
            findings.append(
                ValidationFinding(
                    severity=baseline_severity,
                    code="bayes_held_out_calibration_ungradeable",
                    message="held-out calibration requires finite ece_held_out in [0, 1]",
                )
            )
        else:
            metrics["held_out_ece"] = ece
            calibration_status = (
                "passed" if ece <= thresholds["held_out_ece_warn"] else "failed"
            )
            if calibration_status == "failed":
                findings.append(
                    ValidationFinding(
                        severity="warn",
                        code="bayes_held_out_ece",
                        message=(
                            f"ece_held_out={ece:.4f} exceeds threshold "
                            f"{thresholds['held_out_ece_warn']} (smoke={is_smoke})"
                        ),
                    )
                )

    return findings, metrics, predictive_status, calibration_status


def _validate_bayes(
    manifest: ArtifactManifest,
    artifact_dir: Path,
    *,
    diagnostics_overrides: Mapping[str, object] | None = None,
) -> ValidationReport:
    findings: list[ValidationFinding] = []
    metrics: dict[str, float | int] = {}
    diagnostics_path = artifact_dir / "validation" / "diagnostics.json"
    if not diagnostics_path.exists():
        if diagnostics_overrides is None:
            findings.append(
                ValidationFinding(
                    severity="block",
                    code="bayes_missing_diagnostics",
                    message=f"diagnostics.json not found at {diagnostics_path}",
                )
            )
            return _finalize(
                manifest,
                findings,
                metrics,
                evidence=ValidationEvidence(numerical="failed"),
            )
        return _grade_bayes_payload(
            manifest, artifact_dir, dict(diagnostics_overrides), findings, metrics
        )

    try:
        loaded = json.loads(diagnostics_path.read_text(encoding="utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        findings.append(
            ValidationFinding(
                severity="block",
                code="bayes_malformed_diagnostics",
                message=(
                    f"diagnostics.json at {diagnostics_path} is not valid JSON: {exc}"
                ),
            )
        )
        return _finalize(
            manifest,
            findings,
            metrics,
            evidence=ValidationEvidence(numerical="failed"),
        )
    if not isinstance(loaded, dict):
        findings.append(
            ValidationFinding(
                severity="block",
                code="bayes_malformed_diagnostics",
                message=(
                    f"diagnostics.json at {diagnostics_path} is not a JSON object "
                    f"(got {type(loaded).__name__}); cannot verify convergence"
                ),
            )
        )
        return _finalize(
            manifest,
            findings,
            metrics,
            evidence=ValidationEvidence(numerical="failed"),
        )

    payload: dict[str, object] = dict(cast(dict[str, object], loaded))
    payload.update(diagnostics_overrides or {})
    return _grade_bayes_payload(manifest, artifact_dir, payload, findings, metrics)


def _grade_bayes_payload(
    manifest: ArtifactManifest,
    artifact_dir: Path,
    payload: dict[str, object],
    findings: list[ValidationFinding],
    metrics: dict[str, float | int],
) -> ValidationReport:
    is_smoke = bool(payload.get("is_smoke", False)) or manifest_declares_smoke(manifest)
    thresholds = _BAYES_THRESHOLDS_SMOKE if is_smoke else _BAYES_THRESHOLDS_DEFAULT
    metrics["is_smoke"] = 1 if is_smoke else 0

    rhat_max_raw = payload.get("rhat_max")
    ess_bulk_min_raw = payload.get("ess_bulk_min")
    divergences_raw = payload.get("divergences", 0)
    total_draws_raw = payload.get("total_draws", 0)
    calibration_ece_raw = payload.get("calibration_ece")
    bucket_dev_raw = payload.get("posterior_predictive_max_bucket_dev")

    rhat_max = (
        float(rhat_max_raw) if isinstance(rhat_max_raw, (int, float)) else float("nan")
    )
    ess_bulk_min = (
        float(ess_bulk_min_raw)
        if isinstance(ess_bulk_min_raw, (int, float))
        else float("nan")
    )
    divergences = (
        int(divergences_raw) if isinstance(divergences_raw, (int, float)) else 0
    )
    total_draws = (
        int(total_draws_raw) if isinstance(total_draws_raw, (int, float)) else 0
    )
    metrics["rhat_max"] = rhat_max
    metrics["ess_bulk_min"] = ess_bulk_min
    metrics["divergences"] = divergences
    metrics["total_draws"] = total_draws

    rhat_threshold = thresholds["rhat_max"]
    if not math.isfinite(rhat_max):
        findings.append(
            ValidationFinding(
                severity="block",
                code="bayes_missing_rhat",
                message=(
                    "rhat_max missing or non-finite in diagnostics.json; "
                    "cannot verify convergence"
                ),
            )
        )
    elif rhat_max > rhat_threshold:
        findings.append(
            ValidationFinding(
                severity="block",
                code="bayes_high_rhat",
                message=(
                    f"rhat_max={rhat_max:.4f} exceeds threshold {rhat_threshold} "
                    f"(smoke={is_smoke})"
                ),
            )
        )

    ess_threshold = thresholds["ess_bulk_min"]
    if not math.isfinite(ess_bulk_min):
        findings.append(
            ValidationFinding(
                severity="block",
                code="bayes_missing_ess",
                message=(
                    "ess_bulk_min missing or non-finite in diagnostics.json; "
                    "cannot verify convergence"
                ),
            )
        )
    elif ess_bulk_min < ess_threshold:
        findings.append(
            ValidationFinding(
                severity="block",
                code="bayes_low_ess",
                message=(
                    f"ess_bulk_min={ess_bulk_min:.1f} below threshold {ess_threshold} "
                    f"(smoke={is_smoke})"
                ),
            )
        )

    divergence_fraction = thresholds["divergence_fraction"]
    div_limit = divergence_fraction * total_draws if total_draws > 0 else 0
    if divergences > div_limit:
        findings.append(
            ValidationFinding(
                severity="block",
                code="bayes_divergences",
                message=(
                    f"divergences={divergences} exceeds limit "
                    f"{div_limit:.1f} ({divergence_fraction:.0%} of {total_draws})"
                ),
            )
        )

    if isinstance(calibration_ece_raw, (int, float)):
        metrics["calibration_ece"] = float(calibration_ece_raw)
        if float(calibration_ece_raw) > thresholds["calibration_ece_warn"]:
            findings.append(
                ValidationFinding(
                    severity="warn",
                    code="bayes_calibration_ece",
                    message=(
                        f"calibration_ece={calibration_ece_raw:.4f} exceeds "
                        f"warn threshold {thresholds['calibration_ece_warn']}"
                    ),
                )
            )

    if isinstance(bucket_dev_raw, (int, float)):
        metrics["posterior_predictive_max_bucket_dev"] = float(bucket_dev_raw)
        if float(bucket_dev_raw) > thresholds["post_pred_bucket_dev_warn"]:
            findings.append(
                ValidationFinding(
                    severity="warn",
                    code="bayes_post_pred_bucket_dev",
                    message=(
                        f"posterior_predictive_max_bucket_dev={bucket_dev_raw:.4f} "
                        f"exceeds warn threshold "
                        f"{thresholds['post_pred_bucket_dev_warn']}"
                    ),
                )
            )

    (
        held_out_findings,
        held_out_metrics,
        predictive_status,
        calibration_status,
    ) = _grade_held_out_metrics(
        manifest, artifact_dir, thresholds=thresholds, is_smoke=is_smoke
    )
    findings.extend(held_out_findings)
    metrics.update(held_out_metrics)

    numerical_failed = any(
        finding.severity == "block" and not finding.code.startswith("bayes_held_out_")
        for finding in findings
    )
    evidence = ValidationEvidence(
        numerical="failed" if numerical_failed else "passed",
        predictive=predictive_status,
        calibration=calibration_status,
    )
    return _finalize(manifest, findings, metrics, evidence=evidence)


def _finalize(
    manifest: ArtifactManifest,
    findings: list[ValidationFinding],
    metrics: dict[str, float | int],
    *,
    evidence: ValidationEvidence | None = None,
) -> ValidationReport:
    blocking = [f for f in findings if f.severity == "block"]
    status = "passed" if not blocking else "failed"
    return ValidationReport(
        artifact_id=manifest.artifact_id,
        kind=manifest.kind,
        name=manifest.name,
        status=status,
        findings=tuple(findings),
        metrics=metrics,
        generated_at=datetime.now(tz=timezone.utc),
        evidence=evidence or ValidationEvidence(),
    )
