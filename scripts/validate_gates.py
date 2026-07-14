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

Read-only by default. ``--write`` persists each ``validation_report.json``
beside its artifact; without it, no manifest or report is mutated.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

from pydantic import BaseModel

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
from python_models.statistical.schemas import ValidationReport  # noqa: E402
from python_models.statistical.validate import (  # noqa: E402
    find_manifest,
    validate_artifact,
)

log = logging.getLogger("validate_gates")

_CANDIDATE_ROOTS: tuple[Path, ...] = (
    cfg.DEEP_ROOT,
    cfg.BAYES_ROOT,
    cfg.DATASETS_ROOT,
    cfg.EDA_ROOT,
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
    hdi_coverage: float | None = None
    hdi_coverage_param: float | None = None
    findings: tuple[str, ...] = ()


def discover_pointers(
    models: tuple[str, ...] | None,
    *,
    roots: tuple[Path, Path] | None = None,
) -> dict[str, Path]:
    """Return ``model_name -> pointer_path`` with branch shadowing global."""
    branch_root, global_root = roots if roots is not None else cfg.resolve_published_roots()
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
    out.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(out.parent), prefix=".validation.", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            _ = fh.write(report.model_dump_json(indent=2))
        os.replace(tmp, out)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def evaluate_gate(
    model: str,
    pointer_path: Path,
    *,
    write: bool,
    candidate_roots: tuple[Path, ...] = _CANDIDATE_ROOTS,
    datasets_root: Path | None = None,
) -> GateRow:
    datasets_root = datasets_root or cfg.DATASETS_ROOT
    pointer = read_published_pointer(pointer_path)
    artifact_id = pointer.artifact_id
    try:
        manifest_path = find_manifest(artifact_id, candidate_roots, model_name=model)
    except FileNotFoundError:
        log.warning("artifact %s for model %s not found on disk", artifact_id, model)
        return GateRow(model=model, artifact_id=artifact_id, status="missing")
    artifact_dir = manifest_path.parent
    manifest = read_manifest(manifest_path)

    report = validate_artifact(
        artifact_id, model_name=model, candidate_roots=candidate_roots
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
            report = report.model_copy(
                update={"findings": (*report.findings, predictive.finding)}
            )
    if parameter is not None:
        hdi_coverage_param = parameter.coverage

    if write:
        _write_report(artifact_dir, report)

    findings = [f for f in report.findings if f.severity != "info"]
    codes = tuple(f"{f.code}({f.severity})" for f in findings)
    return GateRow(
        model=model,
        artifact_id=artifact_id,
        status=report.status,
        hdi_coverage=hdi_coverage,
        hdi_coverage_param=hdi_coverage_param,
        findings=codes,
    )


def format_table(rows: list[GateRow]) -> str:
    header = ("model", "artifact_id", "status", "hdi_cov", "hdi_cov_param", "findings")
    n_fixed = 5
    body: list[tuple[str, str, str, str, str, str]] = []
    for r in rows:
        cov = "-" if r.hdi_coverage is None else f"{r.hdi_coverage:.4f}"
        cov_param = "-" if r.hdi_coverage_param is None else f"{r.hdi_coverage_param:.4f}"
        fired = ", ".join(r.findings) if r.findings else "-"
        body.append((r.model, r.artifact_id, r.status, cov, cov_param, fired))
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
        help="persist validation_report.json beside each artifact (default off)",
    )
    _ = parser.add_argument("--log-level", default="INFO")
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
            rows.append(evaluate_gate(model, pointer_path, write=bool(args.write)))
        except Exception as exc:
            log.exception("gate evaluation failed for %s: %s", model, exc)
            rows.append(
                GateRow(model=model, artifact_id="?", status="error", findings=(str(exc),))
            )

    print(format_table(rows))
    n_failed = sum(1 for r in rows if r.status in ("failed", "error", "missing"))
    log.info("validate-gates swept %d models; %d not passed", len(rows), n_failed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
