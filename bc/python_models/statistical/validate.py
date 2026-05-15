"""Cross-phase artifact validation library.

``validate_artifact(artifact_id)`` discovers the named artifact under any
of the well-known roots (datasets / deep / bayes / eda) and dispatches
on ``manifest.kind``. Each kind has its own check suite. The result is
a ``ValidationReport`` — Pydantic-serializable JSON — written to
``<artifact_dir>/validation/validation_report.json`` by callers (the CLI
handler does this) so downstream gates can pick it up.

Phase 3 PR2 ships the structural deep checks: probability normalization
on ``dl_p_class`` LIST sums, grain-key uniqueness per partition (proxy
for OOF group-leakage at write time), and no-argmax-published schema
guard. Slice calibration + baseline comparison hooks are present but
no-op unless the manifest's ``output_paths`` advertises a baseline
parquet — PR3 wires the real Geometry baseline.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from python_models.statistical import config as _config
from python_models.statistical.manifests import read_manifest
from python_models.statistical.schemas import (
    ArtifactManifest,
    ValidationFinding,
    ValidationReport,
)

_log = logging.getLogger(__name__)

_PROHIBITED_DEEP_COLUMNS: frozenset[str] = frozenset(
    {"predicted_class", "argmax_class", "argmax", "dl_argmax_class"}
)


def validate_artifact(
    artifact_id: str,
    *,
    candidate_roots: tuple[Path, ...] | None = None,
) -> ValidationReport:
    roots = candidate_roots or (
        _config.DEEP_ROOT,
        _config.BAYES_ROOT,
        _config.DATASETS_ROOT,
        _config.EDA_ROOT,
    )
    manifest_path = _find_manifest(artifact_id, roots)
    manifest = read_manifest(manifest_path)
    artifact_dir = manifest_path.parent

    match manifest.kind:
        case "deep":
            return _validate_deep(manifest, artifact_dir)
        case "dataset":
            return _validate_dataset(manifest, artifact_dir)
        case "bayes":
            return _validate_bayes_stub(manifest, artifact_dir)
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


def _find_manifest(artifact_id: str, roots: tuple[Path, ...]) -> Path:
    for root in roots:
        if not root.exists():
            continue
        for candidate in root.rglob(f"{artifact_id}/manifest.json"):
            return candidate
    raise FileNotFoundError(
        f"no manifest.json for artifact_id={artifact_id!r} under {[str(r) for r in roots]}"
    )


def _validate_deep(
    manifest: ArtifactManifest, artifact_dir: Path
) -> ValidationReport:
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
        return _finalize(manifest, findings, metrics)

    probs = pl.read_parquet(probabilities_path)
    metrics["row_count"] = int(probs.height)

    schema_findings, schema_metrics = _check_deep_schema(probs)
    findings.extend(schema_findings)
    metrics.update(schema_metrics)

    norm_findings, norm_metrics = _check_probability_normalization(probs)
    findings.extend(norm_findings)
    metrics.update(norm_metrics)

    uniq_findings, uniq_metrics = _check_grain_uniqueness_per_partition(probs)
    findings.extend(uniq_findings)
    metrics.update(uniq_metrics)

    baseline_path = artifact_dir / "exports" / "baseline_predictions.parquet"
    if baseline_path.exists():
        compare_findings, compare_metrics = _compare_against_baseline(
            probs, baseline_path
        )
        findings.extend(compare_findings)
        metrics.update(compare_metrics)
    else:
        findings.append(
            ValidationFinding(
                severity="info",
                code="deep_baseline_absent",
                message=(
                    "no exports/baseline_predictions.parquet alongside the artifact; "
                    "baseline comparison skipped"
                ),
            )
        )

    return _finalize(manifest, findings, metrics)


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
                severity="warn",
                code="deep_missing_partition",
                message="probabilities.parquet has no `partition` column",
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
    sums = probs.with_columns(pl.col("dl_p_class").list.sum().alias("_p_sum"))
    bad = sums.filter(
        (pl.col("_p_sum") < 1.0 - tol) | (pl.col("_p_sum") > 1.0 + tol)
    )
    n_bad = int(bad.height)
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
                message=(
                    f"{n_bad} dl_p_class rows do not sum to 1 within {tol}"
                ),
            )
        ],
        metrics,
    )


def _check_grain_uniqueness_per_partition(
    probs: pl.DataFrame,
) -> tuple[list[ValidationFinding], dict[str, float | int]]:
    if "partition" not in probs.columns:
        return [], {}
    grain_candidates = [
        c for c in probs.columns if c not in {"partition", "dl_p_class"}
    ]
    if not grain_candidates:
        return [], {}
    grain_cols = tuple(grain_candidates)
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


def _compare_against_baseline(
    probs: pl.DataFrame, baseline_path: Path
) -> tuple[list[ValidationFinding], dict[str, float | int]]:
    baseline = pl.read_parquet(baseline_path)
    metrics: dict[str, float | int] = {
        "baseline_row_count": int(baseline.height),
    }
    findings: list[ValidationFinding] = []
    if "dl_p_class" not in baseline.columns:
        findings.append(
            ValidationFinding(
                severity="warn",
                code="deep_baseline_missing_p_class",
                message=f"baseline {baseline_path} has no dl_p_class column",
            )
        )
    return findings, metrics


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
    return _finalize(manifest, findings, metrics)


def _validate_bayes_stub(
    manifest: ArtifactManifest, artifact_dir: Path
) -> ValidationReport:
    del artifact_dir
    return ValidationReport(
        artifact_id=manifest.artifact_id,
        kind=manifest.kind,
        name=manifest.name,
        status="exploratory",
        findings=(
            ValidationFinding(
                severity="info",
                code="bayes_validate_stub",
                message="Bayes validation lands in Phase 4",
            ),
        ),
        generated_at=datetime.now(tz=timezone.utc),
    )


def _finalize(
    manifest: ArtifactManifest,
    findings: list[ValidationFinding],
    metrics: dict[str, float | int],
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
    )
