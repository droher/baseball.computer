"""Sweep published artifact pointers and print the calibration-gate table.

Resolves every published pointer the way ``publish`` / ``validate`` do
(branch root shadows the global root per model), runs ``validate_artifact``
on each, and prints one row per model: validation status and the codes of
the findings that fired. For aggregate surfaces with a coverage hook
(``state_transition`` and ``run_expectancy`` today) it recomputes both a
posterior-predictive 94% coverage (the primary ``hdi_cov`` number, which
folds finite-sample noise into the interval) and the parameter 94%-HDI
coverage (secondary ``hdi_cov_param``) from held-out game folds, and folds
the predictive finding into the row.

In both modes each Bayes artifact's convergence diagnostics are recomputed
from ``inference/posterior.nc`` when that file exists and the gate grades
the recomputed numbers; when it does not exist the gate grades the stored
``validation/diagnostics.json``. Read-only by default: a dry run and a
``--write`` run therefore reach the same verdict. ``--write`` persists the
recomputed diagnostics (rewriting ``validation/diagnostics.json`` and the
per-variable ``validation/diagnostics_by_variable.json``), persists
``validation_report.json`` beside the artifact, and stamps the manifest
with ``validation_status``, ``validation_gate_version``, ``validated_at``,
and the re-derived ``bayes_extras.weak_identification_flag``. Without
``--write`` nothing on disk is mutated.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

from pydantic import BaseModel, Field

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "bc"))

from python_models.statistical import config as cfg  # noqa: E402
from python_models.statistical.hdi_coverage import (  # noqa: E402
    HdiCoverageResult,
    run_expectancy_coverage_pair,
    state_transition_coverage_pair,
)
from python_models.statistical.manifests import (  # noqa: E402
    read_manifest,
    read_published_pointer,
)
from python_models.statistical.schemas import (  # noqa: E402
    ArtifactManifest,
    ValidationFinding,
    ValidationReport,
    ValidationStatus,
    ValidationEvidence,
)
from python_models.statistical.validate import (  # noqa: E402
    VALIDATION_GATE_VERSION,
    PosteriorDiagnostics,
    _atomic_write_json,
    compute_posterior_diagnostics,
    diagnostics_indicate_weak_identification,
    manifest_is_smoke,
    validate_manifest,
    write_diagnostics_by_variable,
)

log = logging.getLogger("validate_gates")

NO_DISPATCH_CODE = "validate_no_dispatch"
STAMP_FAILED_CODE = "gate_write_failed"
DIAGNOSTICS_FAILED_CODE = "diagnostics_recompute_failed"
POSTERIOR_FILENAME = "posterior.nc"

_CANDIDATE_ROOTS: tuple[Path, ...] = (
    cfg.DEEP_ROOT,
    cfg.BAYES_ROOT,
    cfg.DATASETS_ROOT,
    cfg.EDA_ROOT,
)

_DIAGNOSTICS_JSON_KEYS: tuple[str, ...] = (
    "rhat_max",
    "ess_bulk_min",
    "ess_tail_min",
    "divergences",
    "total_draws",
    "group_level_rhat_max",
    "group_level_ess_bulk_min",
)


class _CoverageHook(BaseModel):
    summary_filename: str
    dataset_model_dir: str
    pair_fn: Callable[[Path, Path], tuple[HdiCoverageResult, HdiCoverageResult]]

    model_config = {"arbitrary_types_allowed": True}


_COVERAGE_HOOKS: dict[str, _CoverageHook] = {
    "state_transition": _CoverageHook(
        summary_filename="state_transition_summary.parquet",
        dataset_model_dir="model_input_run_values",
        pair_fn=state_transition_coverage_pair,
    ),
    "run_expectancy": _CoverageHook(
        summary_filename="run_expectancy_summary.parquet",
        dataset_model_dir="model_input_run_values",
        pair_fn=run_expectancy_coverage_pair,
    ),
}


class GateRow(BaseModel):
    model: str
    artifact_id: str
    status: str
    manifest_status: str | None = None
    manifest_gate_version: int | None = None
    hdi_coverage: float | None = None
    hdi_coverage_param: float | None = None
    coverage_kind: str | None = None
    weak_identification_flag: bool | None = None
    findings: tuple[str, ...] = ()
    evidence: ValidationEvidence = Field(default_factory=ValidationEvidence)


def discover_pointers(
    models: tuple[str, ...] | None,
    *,
    roots: tuple[Path, Path] | None = None,
) -> dict[str, Path]:
    """Return ``model_name -> pointer_path`` with branch shadowing global."""
    branch_root, global_root = (
        roots if roots is not None else cfg.resolve_published_roots()
    )
    resolved: dict[str, Path] = {}
    for root in (global_root, branch_root):
        if not root.exists():
            continue
        for pointer_path in sorted(root.glob("*.json")):
            resolved[pointer_path.stem] = pointer_path
    if models:
        resolved = {m: p for m, p in resolved.items() if m in set(models)}
    return resolved


def _coverage_for(
    model: str,
    manifest_dataset_artifact_id: str | None,
    artifact_dir: Path,
    *,
    datasets_root: Path,
) -> tuple[HdiCoverageResult | None, HdiCoverageResult | None]:
    """Return ``(predictive, parameter)`` coverage for a hooked model."""
    hook = _COVERAGE_HOOKS.get(model)
    if hook is None:
        return None, None
    if manifest_dataset_artifact_id is None:
        log.warning("coverage for %s skipped: no dataset_artifact_id", model)
        return None, None
    summary = artifact_dir / "exports" / hook.summary_filename
    dataset = (
        datasets_root
        / hook.dataset_model_dir
        / manifest_dataset_artifact_id
        / "dataset.parquet"
    )
    if not summary.exists() or not dataset.exists():
        log.warning(
            "coverage for %s skipped: summary=%s dataset=%s",
            model,
            summary.exists(),
            dataset.exists(),
        )
        return None, None
    return hook.pair_fn(summary, dataset)


def _write_report(artifact_dir: Path, report: ValidationReport) -> None:
    out = artifact_dir / "validation" / "validation_report.json"
    _atomic_write_json(out, report.model_dump_json(indent=2))


def _read_json_object(path: Path) -> dict[str, object]:
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} is not a JSON object")
    return cast(dict[str, object], loaded)


def compute_diagnostics_from_posterior(
    artifact_dir: Path,
) -> PosteriorDiagnostics | None:
    """Recompute convergence diagnostics from the saved posterior, if any.

    Returns ``None`` when ``inference/posterior.nc`` is absent. Touches
    nothing on disk; ``persist_posterior_diagnostics`` writes the result.
    """
    posterior_path = artifact_dir / "inference" / POSTERIOR_FILENAME
    if not posterior_path.exists():
        log.info(
            "no %s under %s; diagnostics not recomputed",
            POSTERIOR_FILENAME,
            artifact_dir,
        )
        return None
    import arviz as az

    idata = az.from_netcdf(posterior_path)
    diagnostics = compute_posterior_diagnostics(idata)
    log.info(
        "recomputed diagnostics under %s: rhat_max=%.4f ess_bulk_min=%.1f "
        "group_level_rhat_max=%.4f group_level_ess_bulk_min=%.1f (%d variables, "
        "%d excluded)",
        artifact_dir,
        diagnostics.rhat_max,
        diagnostics.ess_bulk_min,
        diagnostics.group_level_rhat_max,
        diagnostics.group_level_ess_bulk_min,
        len(diagnostics.by_variable),
        len(diagnostics.excluded_variables),
    )
    return diagnostics


def diagnostics_overrides(
    diagnostics: PosteriorDiagnostics | None,
) -> dict[str, object] | None:
    """The convergence keys the gate grades in place of the stored file's."""
    if diagnostics is None:
        return None
    return {key: getattr(diagnostics, key) for key in _DIAGNOSTICS_JSON_KEYS}


def persist_posterior_diagnostics(
    artifact_dir: Path, diagnostics: PosteriorDiagnostics
) -> None:
    """Write the per-variable table and the convergence keys of ``diagnostics.json``.

    Rewrites the convergence keys in place; other keys such as ``is_smoke``
    and ``calibration_ece`` survive.
    """
    validation_dir = artifact_dir / "validation"
    _ = write_diagnostics_by_variable(validation_dir, diagnostics.by_variable)
    diagnostics_path = validation_dir / "diagnostics.json"
    payload: dict[str, object] = (
        _read_json_object(diagnostics_path) if diagnostics_path.exists() else {}
    )
    payload.update(diagnostics_overrides(diagnostics) or {})
    _atomic_write_json(diagnostics_path, json.dumps(payload, indent=2))


def derive_weak_identification_flag(
    manifest: ArtifactManifest,
    artifact_dir: Path,
    diagnostics: PosteriorDiagnostics,
) -> bool:
    return diagnostics_indicate_weak_identification(
        rhat_max=diagnostics.group_level_rhat_max,
        ess_bulk_min=diagnostics.group_level_ess_bulk_min,
        divergences=diagnostics.divergences,
        is_smoke=manifest_is_smoke(manifest, artifact_dir),
    )


def _stamp_manifest_gate_result(
    manifest_path: Path,
    *,
    status: ValidationStatus,
    gate_version: int = VALIDATION_GATE_VERSION,
    validated_at: datetime | None = None,
    weak_identification_flag: bool | None = None,
    diagnostics: PosteriorDiagnostics | None = None,
    evidence: ValidationEvidence | None = None,
    artifact_binding: str | None = None,
) -> bool:
    """Stamp the gate verdict and its provenance into the manifest JSON.

    Operates on the raw JSON rather than a parsed ``ArtifactManifest`` so
    on-disk keys the schema does not declare survive the rewrite. Writes
    whenever the status, the gate version, the weak-identification flag,
    or the diagnostics summary would change; ``validated_at`` alone never
    forces a write, so a re-run that changes nothing leaves the file
    untouched. Returns whether a write happened.
    """
    payload = _read_json_object(manifest_path)
    updated: dict[str, object] = dict(payload)
    updated["validation_status"] = status
    updated["validation_gate_version"] = gate_version
    if evidence is not None:
        updated["validation_evidence"] = evidence.model_dump()
    updated["validation_binding"] = artifact_binding
    extras_raw = updated.get("bayes_extras")
    if isinstance(extras_raw, dict):
        extras = dict(cast(dict[str, object], extras_raw))
        if weak_identification_flag is not None:
            extras["weak_identification_flag"] = weak_identification_flag
        summary_raw = extras.get("diagnostics_summary")
        if diagnostics is not None and isinstance(summary_raw, dict):
            summary = dict(cast(dict[str, object], summary_raw))
            for key in _DIAGNOSTICS_JSON_KEYS:
                summary[key] = getattr(diagnostics, key)
            extras["diagnostics_summary"] = summary
        updated["bayes_extras"] = extras

    def _without_timestamp(doc: dict[str, object]) -> dict[str, object]:
        return {k: v for k, v in doc.items() if k != "validated_at"}

    if _without_timestamp(updated) == _without_timestamp(payload):
        return False
    stamp_time = (
        validated_at if validated_at is not None else datetime.now(tz=timezone.utc)
    )
    updated["validated_at"] = stamp_time.isoformat()
    _atomic_write_json(manifest_path, json.dumps(updated, indent=2))
    return True


def _status_from_findings(findings: tuple[ValidationFinding, ...]) -> ValidationStatus:
    return "failed" if any(f.severity == "block" for f in findings) else "passed"


def evaluate_gate(
    model: str,
    pointer_path: Path,
    *,
    write: bool,
    candidate_roots: tuple[Path, ...] = _CANDIDATE_ROOTS,
    datasets_root: Path | None = None,
    bind_evidence: bool = False,
) -> GateRow:
    datasets_root = datasets_root or cfg.DATASETS_ROOT
    pointer = read_published_pointer(pointer_path)
    artifact_id = pointer.artifact_id
    manifest_path = pointer.manifest_path
    if not manifest_path.is_file():
        log.warning("artifact %s for model %s not found on disk", artifact_id, model)
        return GateRow(model=model, artifact_id=artifact_id, status="missing")
    artifact_dir = manifest_path.parent
    manifest = read_manifest(manifest_path)
    if manifest.artifact_id != artifact_id:
        raise ValueError(f"pointer artifact identity does not match {manifest_path}")
    weak_identification_flag: bool | None = (
        manifest.bayes_extras.weak_identification_flag
        if manifest.bayes_extras is not None
        else None
    )

    diagnostics: PosteriorDiagnostics | None = None
    if manifest.kind == "bayes":
        try:
            diagnostics = compute_diagnostics_from_posterior(artifact_dir)
            if write and diagnostics is not None:
                persist_posterior_diagnostics(artifact_dir, diagnostics)
        except Exception:
            log.exception(
                "recomputing diagnostics failed model=%s artifact_id=%s",
                model,
                artifact_id,
            )
            return GateRow(
                model=model,
                artifact_id=artifact_id,
                status="error",
                manifest_status=str(manifest.validation_status),
                weak_identification_flag=weak_identification_flag,
                findings=(f"{DIAGNOSTICS_FAILED_CODE}(block)",),
            )
        if diagnostics is not None:
            weak_identification_flag = derive_weak_identification_flag(
                manifest, artifact_dir, diagnostics
            )

    report = validate_manifest(
        manifest_path,
        diagnostics_overrides=diagnostics_overrides(diagnostics),
    )

    predictive, parameter = _coverage_for(
        model,
        manifest.dataset_artifact_id,
        artifact_dir,
        datasets_root=datasets_root,
    )
    hdi_coverage: float | None = None
    hdi_coverage_param: float | None = None
    if predictive is not None:
        hdi_coverage = predictive.coverage
        if predictive.finding is not None:
            merged = (*report.findings, predictive.finding)
            report = report.model_copy(
                update={
                    "findings": merged,
                    "status": _status_from_findings(merged),
                }
            )
    if parameter is not None:
        hdi_coverage_param = parameter.coverage

    if bind_evidence:
        from python_models.statistical.publication_evidence import (
            bind_validation_report,
        )

        report = bind_validation_report(
            report, manifest_path, candidate_roots=candidate_roots
        )

    manifest_status: str = str(manifest.validation_status)
    manifest_gate_version: int | None = manifest.validation_gate_version
    row_status: str = report.status
    extra_codes: tuple[str, ...] = ()
    if write:
        try:
            _write_report(artifact_dir, report)
            if any(f.code == NO_DISPATCH_CODE for f in report.findings):
                log.info(
                    "stamp skipped model=%s artifact_id=%s: %s (no validator registered)",
                    model,
                    artifact_id,
                    NO_DISPATCH_CODE,
                )
            else:
                stamped = _stamp_manifest_gate_result(
                    manifest_path,
                    status=report.status,
                    weak_identification_flag=(
                        weak_identification_flag if diagnostics is not None else None
                    ),
                    diagnostics=diagnostics,
                    evidence=report.evidence,
                    artifact_binding=report.artifact_binding,
                )
                if stamped:
                    log.info(
                        "stamped manifest model=%s artifact_id=%s status %s -> %s "
                        "gate_version=%d weak_identification_flag=%s",
                        model,
                        artifact_id,
                        manifest.validation_status,
                        report.status,
                        VALIDATION_GATE_VERSION,
                        weak_identification_flag,
                    )
                manifest_status = report.status
                manifest_gate_version = VALIDATION_GATE_VERSION
        except Exception:
            log.exception(
                "persisting gate result failed model=%s artifact_id=%s",
                model,
                artifact_id,
            )
            row_status = "error"
            extra_codes = (f"{STAMP_FAILED_CODE}(block)",)

    findings = [f for f in report.findings if f.severity != "info"]
    codes = tuple(f"{f.code}({f.severity})" for f in findings)
    return GateRow(
        model=model,
        artifact_id=artifact_id,
        status=row_status,
        manifest_status=manifest_status,
        manifest_gate_version=manifest_gate_version,
        hdi_coverage=hdi_coverage,
        coverage_kind=predictive.coverage_kind if predictive is not None else None,
        hdi_coverage_param=hdi_coverage_param,
        weak_identification_flag=weak_identification_flag,
        findings=(*codes, *extra_codes),
        evidence=report.evidence,
    )


def format_table(rows: list[GateRow]) -> str:
    header = (
        "model",
        "artifact_id",
        "status",
        "manifest_status",
        "gate_v",
        "hdi_cov",
        "hdi_cov_param",
        "weak_id",
        "coverage_kind",
        "findings",
    )
    n_fixed = 9
    body: list[tuple[str, ...]] = []
    for r in rows:
        manifest_status = r.manifest_status or "-"
        gate_version = (
            "-" if r.manifest_gate_version is None else str(r.manifest_gate_version)
        )
        cov = "-" if r.hdi_coverage is None else f"{r.hdi_coverage:.4f}"
        cov_param = (
            "-" if r.hdi_coverage_param is None else f"{r.hdi_coverage_param:.4f}"
        )
        weak = (
            "-"
            if r.weak_identification_flag is None
            else str(r.weak_identification_flag)
        )
        fired = ", ".join(r.findings) if r.findings else "-"
        body.append(
            (
                r.model,
                r.artifact_id,
                r.status,
                manifest_status,
                gate_version,
                cov,
                cov_param,
                weak,
                r.coverage_kind or "-",
                fired,
            )
        )
    widths = [len(h) for h in header]
    for row in body:
        for i, cell in enumerate(row[:n_fixed]):
            widths[i] = max(widths[i], len(cell))
    lines = [
        "  ".join(h.ljust(widths[i]) for i, h in enumerate(header[:n_fixed]))
        + "  findings"
    ]
    lines.append("  ".join("-" * widths[i] for i in range(n_fixed)) + "  --------")
    for row in body:
        lines.append(
            "  ".join(row[i].ljust(widths[i]) for i in range(n_fixed))
            + "  "
            + row[n_fixed]
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument("models", nargs="*", help="restrict to these model names")
    _ = parser.add_argument(
        "--write",
        action="store_true",
        help=(
            "persist the Bayes diagnostics recomputed from inference/posterior.nc, "
            "persist validation_report.json, and stamp validation_status, "
            "validation_gate_version, validated_at, and "
            "weak_identification_flag into each manifest, including content-bound "
            "dependency evidence (default off)"
        ),
    )
    _ = parser.add_argument("--log-level", default="INFO")
    _ = parser.add_argument("--json-output", type=Path)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    models: tuple[str, ...] | None = tuple(args.models) or None
    pointers = discover_pointers(models)
    if not pointers:
        log.error("no published pointers resolved under the configured roots")
        return 1

    rows: list[GateRow] = []
    for model, pointer_path in sorted(pointers.items()):
        try:
            rows.append(
                evaluate_gate(
                    model,
                    pointer_path,
                    write=bool(args.write),
                    bind_evidence=bool(args.write),
                    candidate_roots=cfg.resolve_candidate_roots(),
                    datasets_root=cfg.resolve_artifact_root() / "datasets",
                )
            )
        except Exception as exc:
            log.exception("gate evaluation failed for %s: %s", model, exc)
            rows.append(
                GateRow(
                    model=model, artifact_id="?", status="error", findings=(str(exc),)
                )
            )

    print(format_table(rows))
    if args.json_output:
        _atomic_write_json(
            args.json_output,
            json.dumps([r.model_dump(mode="json") for r in rows], indent=2),
        )
    n_failed = sum(1 for r in rows if r.status != "passed")
    log.info("validate-gates swept %d models; %d not passed", len(rows), n_failed)
    return 1 if n_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
