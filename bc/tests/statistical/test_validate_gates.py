"""Driver: pointer discovery, gate evaluation, and table formatting."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from python_models.statistical.hdi_coverage import HdiCoverageResult
from python_models.statistical.manifests import package_versions
from python_models.statistical.schemas import (
    ArtifactManifest,
    BayesArtifactExtras,
    BayesDiagnosticsSummary,
    BayesPriorConfig,
    BayesSamplerConfig,
    PublishedPointer,
    ValidationFinding,
)

SCRIPTS_DIR = Path(__file__).resolve().parents[3] / "scripts"


def _load() -> Any:
    sys.path.insert(0, str(SCRIPTS_DIR))
    try:
        module_name = f"validate_gates_test_{uuid4().hex}"
        spec = importlib.util.spec_from_file_location(
            module_name, SCRIPTS_DIR / "validate_gates.py"
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise
        return module
    finally:
        sys.path.remove(str(SCRIPTS_DIR))


def _bayes_manifest(artifact_id: str, model: str) -> ArtifactManifest:
    extras = BayesArtifactExtras(
        model_name=model,
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
        artifact_id=artifact_id,
        kind="bayes",
        name=model,
        version="0.0.0",
        created_at=datetime.now(tz=timezone.utc),
        source_snapshot_id="src-1",
        output_paths={"artifact_dir": Path(".")},
        package_versions=package_versions(),
        bayes_extras=extras,
    )


def _build_artifact(
    tmp_path: Path, model: str, artifact_id: str, held_out: dict[str, object]
) -> tuple[Path, Path]:
    bayes_root = tmp_path / "bayes"
    artifact_dir = bayes_root / model / artifact_id
    validation = artifact_dir / "validation"
    validation.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "manifest.json").write_text(
        _bayes_manifest(artifact_id, model).model_dump_json(indent=2),
        encoding="utf-8",
    )
    (validation / "diagnostics.json").write_text(
        json.dumps(
            {
                "rhat_max": 1.01,
                "ess_bulk_min": 500.0,
                "divergences": 0,
                "total_draws": 4000,
                "is_smoke": False,
            }
        ),
        encoding="utf-8",
    )
    (validation / "held_out_metrics.json").write_text(
        json.dumps(held_out), encoding="utf-8"
    )

    published = tmp_path / "published"
    published.mkdir(parents=True, exist_ok=True)
    pointer = PublishedPointer(
        model_name=model,
        artifact_id=artifact_id,
        published_at=datetime.now(tz=timezone.utc),
        manifest_path=artifact_dir / "manifest.json",
    )
    pointer_path = published / f"{model}.json"
    pointer_path.write_text(pointer.model_dump_json(indent=2), encoding="utf-8")
    return bayes_root, pointer_path


def test_evaluate_gate_passing_artifact(tmp_path: Path) -> None:
    module = _load()
    bayes_root, pointer_path = _build_artifact(
        tmp_path,
        "synthetic_good",
        "aid-good",
        {"roc_auc": 0.9, "pr_auc": 0.9, "baseline_pr_auc": 0.3, "ece_held_out": 0.02},
    )
    row = module.evaluate_gate(
        "synthetic_good",
        pointer_path,
        write=False,
        candidate_roots=(bayes_root,),
        datasets_root=tmp_path / "datasets",
    )
    assert row.status == "passed"
    assert row.artifact_id == "aid-good"
    assert row.hdi_coverage is None
    assert not [c for c in row.findings if "block" in c]


def test_evaluate_gate_failing_artifact(tmp_path: Path) -> None:
    module = _load()
    bayes_root, pointer_path = _build_artifact(
        tmp_path,
        "synthetic_bad",
        "aid-bad",
        {"roc_auc": 0.4, "pr_auc": 0.1, "baseline_pr_auc": 0.3, "ece_held_out": 0.02},
    )
    row = module.evaluate_gate(
        "synthetic_bad",
        pointer_path,
        write=False,
        candidate_roots=(bayes_root,),
        datasets_root=tmp_path / "datasets",
    )
    assert row.status == "failed"
    assert any("bayes_held_out_auc_not_beating_baseline" in c for c in row.findings)
    assert not (bayes_root / "synthetic_bad" / "aid-bad" / "validation" / "validation_report.json").exists()


def test_evaluate_gate_write_persists_report(tmp_path: Path) -> None:
    module = _load()
    bayes_root, pointer_path = _build_artifact(
        tmp_path,
        "synthetic_good",
        "aid-good",
        {"roc_auc": 0.9, "pr_auc": 0.9, "baseline_pr_auc": 0.3, "ece_held_out": 0.02},
    )
    _ = module.evaluate_gate(
        "synthetic_good",
        pointer_path,
        write=True,
        candidate_roots=(bayes_root,),
        datasets_root=tmp_path / "datasets",
    )
    report_path = (
        bayes_root / "synthetic_good" / "aid-good" / "validation" / "validation_report.json"
    )
    assert report_path.exists()


def test_evaluate_gate_write_persists_coverage_finding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The persisted report must include the coverage-hook finding, matching
    what the printed table shows — the disk/table disagreement this guards
    against is that ``_coverage_for``'s finding used to be folded into the
    in-memory findings list used for display without ever landing in the
    ``ValidationReport`` object that gets written to disk.
    """
    module = _load()
    bayes_root, pointer_path = _build_artifact(
        tmp_path,
        "synthetic_good",
        "aid-good",
        {"roc_auc": 0.9, "pr_auc": 0.9, "baseline_pr_auc": 0.3, "ece_held_out": 0.02},
    )
    coverage_finding = ValidationFinding(
        severity="warn",
        code="predictive_coverage_out_of_band",
        message="synthetic coverage finding for the disk/table agreement test",
    )
    predictive = HdiCoverageResult(
        model_name="synthetic_good",
        n_cells=10,
        coverage=0.5,
        coverage_band=(0.9, 0.98),
        in_band=False,
        coverage_kind="predictive",
        finding=coverage_finding,
    )
    monkeypatch.setattr(
        module, "_coverage_for", lambda *args, **kwargs: (predictive, None)
    )

    row = module.evaluate_gate(
        "synthetic_good",
        pointer_path,
        write=True,
        candidate_roots=(bayes_root,),
        datasets_root=tmp_path / "datasets",
    )
    assert any(coverage_finding.code in c for c in row.findings)

    report_path = (
        bayes_root / "synthetic_good" / "aid-good" / "validation" / "validation_report.json"
    )
    persisted = json.loads(report_path.read_text())
    persisted_codes = {f["code"] for f in persisted["findings"]}
    assert coverage_finding.code in persisted_codes


def test_format_table_columns_and_dashes() -> None:
    module = _load()
    rows = [
        module.GateRow(
            model="alpha",
            artifact_id="a1",
            status="passed",
            hdi_coverage=None,
            findings=(),
        ),
        module.GateRow(
            model="state_transition",
            artifact_id="stv4",
            status="passed",
            hdi_coverage=0.9123,
            findings=("bayes_held_out_ece(warn)",),
        ),
    ]
    table = module.format_table(rows)
    lines = table.splitlines()
    assert lines[0].split()[:4] == ["model", "artifact_id", "status", "hdi_cov"]
    assert "0.9123" in table
    assert "bayes_held_out_ece(warn)" in table
    assert any(row_line.rstrip().endswith("-") for row_line in lines)


def test_discover_pointers_branch_shadows_global(tmp_path: Path) -> None:
    module = _load()
    global_root = tmp_path / "global"
    branch_root = tmp_path / "branch"
    global_root.mkdir()
    branch_root.mkdir()

    def _pointer(root: Path, model: str, artifact_id: str) -> None:
        PublishedPointer(
            model_name=model,
            artifact_id=artifact_id,
            published_at=datetime.now(tz=timezone.utc),
            manifest_path=root / "m.json",
        ).model_dump_json()
        (root / f"{model}.json").write_text(
            PublishedPointer(
                model_name=model,
                artifact_id=artifact_id,
                published_at=datetime.now(tz=timezone.utc),
                manifest_path=root / "m.json",
            ).model_dump_json(indent=2),
            encoding="utf-8",
        )

    _pointer(global_root, "m1", "g1")
    _pointer(global_root, "m2", "g2")
    _pointer(branch_root, "m1", "b1")

    resolved = module.discover_pointers(None, roots=(branch_root, global_root))
    assert set(resolved) == {"m1", "m2"}
    assert resolved["m1"].parent == branch_root
