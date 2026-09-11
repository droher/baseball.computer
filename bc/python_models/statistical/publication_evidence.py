from __future__ import annotations

import logging
import json
from pathlib import Path
from typing import cast

from python_models.statistical.evidence_binding import bind_artifact
from python_models.statistical.manifests import read_manifest
from python_models.statistical.schemas import ValidationFinding, ValidationReport
from python_models.statistical.validate import (
    VALIDATION_GATE_VERSION,
    validate_manifest,
    write_json_atomic,
    compute_posterior_diagnostics_from_file,
)

_log = logging.getLogger(__name__)

REQUIRED_PREDICTIVE_EVIDENCE = ("numerical", "predictive", "calibration", "provenance")


def save_publication_evidence(
    report: ValidationReport, manifest_path: Path, *, exploratory: bool
) -> None:
    if (
        report.gate_version != VALIDATION_GATE_VERSION
        or report.artifact_binding is None
    ):
        raise ValueError("publication evidence must be bound under the current gate")
    if not exploratory and (
        report.status != "passed"
        or any(
            getattr(report.evidence, name) != "passed"
            for name in REQUIRED_PREDICTIVE_EVIDENCE
        )
    ):
        raise ValueError("validated publication requires complete predictive evidence")
    raw: object = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"manifest must be an object: {manifest_path}")
    payload = cast(dict[str, object], raw)
    payload.update(
        {
            "publication_mode": "exploratory" if exploratory else "validated",
            "validation_status": report.status
            if report.status == "failed" or not exploratory
            else "exploratory",
            "validation_gate_version": report.gate_version,
            "validated_at": report.generated_at.isoformat(),
            "validation_evidence": report.evidence.model_dump(),
            "validation_binding": report.artifact_binding,
        }
    )
    write_json_atomic(
        manifest_path.parent / "validation" / "validation_report.json",
        report.model_dump_json(indent=2),
    )
    write_json_atomic(manifest_path, json.dumps(payload, indent=2))


def bind_validation_report(
    report: ValidationReport,
    manifest_path: Path,
    *,
    candidate_roots: tuple[Path, ...],
) -> ValidationReport:
    manifest = read_manifest(manifest_path)
    if (report.artifact_id, report.name, report.kind) != (
        manifest.artifact_id,
        manifest.name,
        manifest.kind,
    ):
        raise ValueError("validation report identity differs from artifact")
    binding = bind_artifact(manifest_path, candidate_roots=candidate_roots)
    findings = list(report.findings)
    missing = list(binding.missing_provenance)
    for dependency in binding.manifests:
        if dependency == manifest_path.resolve():
            continue
        manifest = read_manifest(dependency)
        if manifest.kind == "dataset":
            continue
        saved_report_path = dependency.parent / "validation" / "validation_report.json"
        if not saved_report_path.is_file():
            missing.append(
                f"{manifest.name}/{manifest.artifact_id}: no dependency validation report"
            )
            continue
        saved = ValidationReport.model_validate_json(saved_report_path.read_text())
        if (saved.artifact_id, saved.name, saved.kind) != (
            manifest.artifact_id,
            manifest.name,
            manifest.kind,
        ):
            raise ValueError(
                "dependency validation report identity differs from artifact"
            )
        current = bind_artifact(dependency, candidate_roots=candidate_roots)
        if (
            saved.artifact_binding != current.digest
            or saved.gate_version != VALIDATION_GATE_VERSION
        ):
            missing.append(
                f"{manifest.name}/{manifest.artifact_id}: dependency validation is stale"
            )
        if saved.status != "passed" or any(
            getattr(saved.evidence, name) != "passed"
            for name in REQUIRED_PREDICTIVE_EVIDENCE
        ):
            findings.append(
                ValidationFinding(
                    severity="block",
                    code="dependency_validation_not_passed",
                    message=f"{manifest.name}/{manifest.artifact_id} lacks complete predictive evidence; status={saved.status}",
                )
            )
    if missing:
        findings.append(
            ValidationFinding(
                severity="block",
                code="artifact_provenance_incomplete",
                message="; ".join(missing),
            )
        )
    evidence = report.evidence.model_copy(
        update={"provenance": "unsupported" if missing else "passed"}
    )
    status = "failed" if any(f.severity == "block" for f in findings) else report.status
    return report.model_copy(
        update={
            "findings": tuple(findings),
            "evidence": evidence,
            "status": status,
            "artifact_binding": binding.digest,
            "gate_version": VALIDATION_GATE_VERSION,
        }
    )


def assess_publication(
    manifest_path: Path,
    *,
    candidate_roots: tuple[Path, ...],
    exploratory_reason: str | None = None,
) -> ValidationReport:
    manifest = read_manifest(manifest_path)
    overrides: dict[str, object] | None = None
    posterior = manifest_path.parent / "inference" / "posterior.nc"
    if manifest.kind == "bayes" and posterior.is_file():
        overrides = compute_posterior_diagnostics_from_file(posterior).model_dump()
    report = validate_manifest(manifest_path, diagnostics_overrides=overrides)
    if manifest.kind == "bayes" and not posterior.is_file():
        report = report.model_copy(
            update={
                "status": "failed",
                "evidence": report.evidence.model_copy(
                    update={"numerical": "unsupported"}
                ),
                "findings": (
                    *report.findings,
                    ValidationFinding(
                        severity="block",
                        code="publication_missing_posterior",
                        message="Bayes publication requires the saved posterior for fresh diagnostics",
                    ),
                ),
            }
        )
    report = bind_validation_report(
        report,
        manifest_path,
        candidate_roots=candidate_roots,
    )
    if exploratory_reason is not None:
        if not exploratory_reason.strip():
            raise ValueError("exploratory publication requires a nonempty reason")
        _log.warning(
            "exploratory publication %s: %s; evidence=%s",
            manifest_path,
            exploratory_reason,
            report.evidence.model_dump(),
        )
        return report
    unsupported = [
        name
        for name in REQUIRED_PREDICTIVE_EVIDENCE
        if getattr(report.evidence, name) != "passed"
    ]
    if report.status != "passed" or unsupported:
        codes = ", ".join(f.code for f in report.findings if f.severity == "block")
        raise ValueError(
            f"artifact is not validated for predictive publication: status={report.status}; "
            f"evidence={unsupported}; findings={codes}. "
            "Repair the evidence or explicitly request an exploratory publication with a reason."
        )
    return report


def verify_published_evidence(
    manifest_path: Path,
    *,
    expected_binding: str | None,
    candidate_roots: tuple[Path, ...],
    exploratory: bool,
    expected_artifact_id: str | None = None,
) -> None:
    manifest = read_manifest(manifest_path)
    if (
        expected_artifact_id is not None
        and manifest.artifact_id != expected_artifact_id
    ):
        raise ValueError("published pointer identity differs from artifact")
    expected_mode = "exploratory" if exploratory else "validated"
    if manifest.publication_mode != expected_mode:
        raise ValueError("published pointer and artifact publication mode disagree")
    if expected_binding is None:
        raise ValueError(
            f"published artifact has no validation binding: {manifest_path}"
        )
    report_path = manifest_path.parent / "validation" / "validation_report.json"
    report = ValidationReport.model_validate_json(report_path.read_text())
    if (report.artifact_id, report.name, report.kind) != (
        manifest.artifact_id,
        manifest.name,
        manifest.kind,
    ):
        raise ValueError("published report identity differs from artifact")
    binding = bind_artifact(manifest_path, candidate_roots=candidate_roots)
    if (
        report.artifact_binding != expected_binding
        or binding.digest != expected_binding
    ):
        raise ValueError(
            f"published artifact or dependency changed after validation: {manifest_path}"
        )
    if report.gate_version != VALIDATION_GATE_VERSION:
        raise ValueError(f"published validation gate version is stale: {manifest_path}")
    if not exploratory:
        report = bind_validation_report(
            report, manifest_path, candidate_roots=candidate_roots
        )
    if not exploratory and (
        binding.missing_provenance
        or report.status != "passed"
        or any(
            getattr(report.evidence, name) != "passed"
            for name in REQUIRED_PREDICTIVE_EVIDENCE
        )
    ):
        raise ValueError(
            f"published predictive evidence is incomplete: {manifest_path}"
        )
