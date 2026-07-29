"""Driver: pointer discovery, gate evaluation, and table formatting."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl
import pytest

from python_models.statistical.bayes.manifest_ingest import stamp_estimated_contract
from python_models.statistical.hdi_coverage import HdiCoverageResult
from python_models.statistical.manifests import package_versions, read_manifest
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


def _build_eda_artifact(
    tmp_path: Path, model: str, artifact_id: str
) -> tuple[Path, Path]:
    eda_root = tmp_path / "eda"
    artifact_dir = eda_root / model / artifact_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    manifest = ArtifactManifest(
        artifact_id=artifact_id,
        kind="eda",
        name=model,
        version="0.0.0",
        created_at=datetime.now(tz=timezone.utc),
        source_snapshot_id="src-1",
        output_paths={"artifact_dir": Path(".")},
        package_versions=package_versions(),
        validation_status="passed",
    )
    (artifact_dir / "manifest.json").write_text(
        manifest.model_dump_json(indent=2), encoding="utf-8"
    )

    published = tmp_path / "published"
    published.mkdir(parents=True, exist_ok=True)
    pointer_path = published / f"{model}.json"
    pointer_path.write_text(
        PublishedPointer(
            model_name=model,
            artifact_id=artifact_id,
            published_at=datetime.now(tz=timezone.utc),
            manifest_path=artifact_dir / "manifest.json",
        ).model_dump_json(indent=2),
        encoding="utf-8",
    )
    return eda_root, pointer_path


def _file_identity(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return (stat.st_ino, stat.st_mtime_ns)


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
    assert not (
        bayes_root
        / "synthetic_bad"
        / "aid-bad"
        / "validation"
        / "validation_report.json"
    ).exists()


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
        bayes_root
        / "synthetic_good"
        / "aid-good"
        / "validation"
        / "validation_report.json"
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
        bayes_root
        / "synthetic_good"
        / "aid-good"
        / "validation"
        / "validation_report.json"
    )
    persisted = json.loads(report_path.read_text())
    persisted_codes = {f["code"] for f in persisted["findings"]}
    assert coverage_finding.code in persisted_codes


def test_evaluate_gate_write_false_leaves_manifest_validation_status_untouched(
    tmp_path: Path,
) -> None:
    module = _load()
    bayes_root, pointer_path = _build_artifact(
        tmp_path,
        "synthetic_good",
        "aid-good",
        {"roc_auc": 0.9, "pr_auc": 0.9, "baseline_pr_auc": 0.3, "ece_held_out": 0.02},
    )
    manifest_path = bayes_root / "synthetic_good" / "aid-good" / "manifest.json"
    before = manifest_path.read_text(encoding="utf-8")

    row = module.evaluate_gate(
        "synthetic_good",
        pointer_path,
        write=False,
        candidate_roots=(bayes_root,),
        datasets_root=tmp_path / "datasets",
    )

    assert row.status == "passed"
    assert manifest_path.read_text(encoding="utf-8") == before
    assert read_manifest(manifest_path).validation_status == "exploratory"


@pytest.mark.parametrize(
    ("model", "artifact_id", "held_out", "expected_status"),
    [
        (
            "synthetic_good",
            "aid-good",
            {
                "roc_auc": 0.9,
                "pr_auc": 0.9,
                "baseline_pr_auc": 0.3,
                "ece_held_out": 0.02,
            },
            "passed",
        ),
        (
            "synthetic_bad",
            "aid-bad",
            {
                "roc_auc": 0.4,
                "pr_auc": 0.1,
                "baseline_pr_auc": 0.3,
                "ece_held_out": 0.02,
            },
            "failed",
        ),
    ],
)
def test_evaluate_gate_write_stamps_manifest_validation_status(
    tmp_path: Path,
    model: str,
    artifact_id: str,
    held_out: dict[str, object],
    expected_status: str,
) -> None:
    module = _load()
    bayes_root, pointer_path = _build_artifact(tmp_path, model, artifact_id, held_out)
    manifest_path = bayes_root / model / artifact_id / "manifest.json"
    assert read_manifest(manifest_path).validation_status == "exploratory"

    row = module.evaluate_gate(
        model,
        pointer_path,
        write=True,
        candidate_roots=(bayes_root,),
        datasets_root=tmp_path / "datasets",
    )

    assert row.status == expected_status
    assert row.manifest_status == expected_status
    assert read_manifest(manifest_path).validation_status == row.status


def test_evaluate_gate_write_second_run_with_same_status_does_not_rewrite(
    tmp_path: Path,
) -> None:
    module = _load()
    bayes_root, pointer_path = _build_artifact(
        tmp_path,
        "synthetic_good",
        "aid-good",
        {"roc_auc": 0.9, "pr_auc": 0.9, "baseline_pr_auc": 0.3, "ece_held_out": 0.02},
    )
    manifest_path = bayes_root / "synthetic_good" / "aid-good" / "manifest.json"

    first = module.evaluate_gate(
        "synthetic_good",
        pointer_path,
        write=True,
        candidate_roots=(bayes_root,),
        datasets_root=tmp_path / "datasets",
    )
    assert first.status == "passed"

    before = _file_identity(manifest_path)

    second = module.evaluate_gate(
        "synthetic_good",
        pointer_path,
        write=True,
        candidate_roots=(bayes_root,),
        datasets_root=tmp_path / "datasets",
    )

    assert second.status == "passed"
    assert _file_identity(manifest_path) == before


def test_evaluate_gate_write_restamps_when_a_blessed_artifact_starts_failing(
    tmp_path: Path,
) -> None:
    """A model that degrades after it was already stamped ``passed`` must be
    restamped ``failed`` — the guard is "status changed", not "status is the
    ``exploratory`` default".
    """
    module = _load()
    bayes_root, pointer_path = _build_artifact(
        tmp_path,
        "synthetic_good",
        "aid-good",
        {"roc_auc": 0.9, "pr_auc": 0.9, "baseline_pr_auc": 0.3, "ece_held_out": 0.02},
    )
    artifact_dir = bayes_root / "synthetic_good" / "aid-good"
    manifest_path = artifact_dir / "manifest.json"

    first = module.evaluate_gate(
        "synthetic_good",
        pointer_path,
        write=True,
        candidate_roots=(bayes_root,),
        datasets_root=tmp_path / "datasets",
    )
    assert first.status == "passed"
    assert read_manifest(manifest_path).validation_status == "passed"

    (artifact_dir / "validation" / "held_out_metrics.json").write_text(
        json.dumps(
            {
                "roc_auc": 0.4,
                "pr_auc": 0.1,
                "baseline_pr_auc": 0.3,
                "ece_held_out": 0.02,
            }
        ),
        encoding="utf-8",
    )
    before = _file_identity(manifest_path)

    second = module.evaluate_gate(
        "synthetic_good",
        pointer_path,
        write=True,
        candidate_roots=(bayes_root,),
        datasets_root=tmp_path / "datasets",
    )

    assert second.status == "failed"
    assert second.manifest_status == "failed"
    assert read_manifest(manifest_path).validation_status == "failed"
    assert _file_identity(manifest_path) != before


def test_evaluate_gate_write_block_coverage_finding_fails_report_and_stamp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``block``-severity coverage finding folded in after ``validate_artifact``
    graded the artifact must drive both the persisted report status and the
    stamped ``validation_status`` to ``failed``.
    """
    module = _load()
    bayes_root, pointer_path = _build_artifact(
        tmp_path,
        "synthetic_good",
        "aid-good",
        {"roc_auc": 0.9, "pr_auc": 0.9, "baseline_pr_auc": 0.3, "ece_held_out": 0.02},
    )
    coverage_finding = ValidationFinding(
        severity="block",
        code="predictive_coverage_out_of_band",
        message="synthetic block-severity coverage finding",
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

    assert row.status == "failed"
    persisted = json.loads(
        (
            bayes_root
            / "synthetic_good"
            / "aid-good"
            / "validation"
            / "validation_report.json"
        ).read_text(encoding="utf-8")
    )
    assert persisted["status"] == "failed"
    manifest_path = bayes_root / "synthetic_good" / "aid-good" / "manifest.json"
    assert read_manifest(manifest_path).validation_status == "failed"


def test_evaluate_gate_write_failure_keeps_artifact_id_and_flags_the_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed persist must not cost the operator the artifact id, and must
    land the row in the sweep's not-passed tally.
    """
    module = _load()
    bayes_root, pointer_path = _build_artifact(
        tmp_path,
        "synthetic_good",
        "aid-good",
        {"roc_auc": 0.9, "pr_auc": 0.9, "baseline_pr_auc": 0.3, "ece_held_out": 0.02},
    )

    def _explode(*args: object, **kwargs: object) -> bool:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(module, "_stamp_manifest_validation_status", _explode)

    row = module.evaluate_gate(
        "synthetic_good",
        pointer_path,
        write=True,
        candidate_roots=(bayes_root,),
        datasets_root=tmp_path / "datasets",
    )

    assert row.artifact_id == "aid-good"
    assert row.status == "error"
    assert any(module.STAMP_FAILED_CODE in c for c in row.findings)
    manifest_path = bayes_root / "synthetic_good" / "aid-good" / "manifest.json"
    assert read_manifest(manifest_path).validation_status == "exploratory"


def test_evaluate_gate_write_preserves_unmodeled_manifest_keys(tmp_path: Path) -> None:
    """``ArtifactManifest`` ignores undeclared on-disk keys, so the stamp must
    rewrite the raw JSON — round-tripping through the schema would silently
    drop keys like ``ablation_status`` that real manifests carry.
    """
    module = _load()
    bayes_root, pointer_path = _build_artifact(
        tmp_path,
        "synthetic_good",
        "aid-good",
        {"roc_auc": 0.9, "pr_auc": 0.9, "baseline_pr_auc": 0.3, "ece_held_out": 0.02},
    )
    manifest_path = bayes_root / "synthetic_good" / "aid-good" / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["ablation_status"] = "handler_covariate_ablation_v2"
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    row = module.evaluate_gate(
        "synthetic_good",
        pointer_path,
        write=True,
        candidate_roots=(bayes_root,),
        datasets_root=tmp_path / "datasets",
    )

    stamped = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert stamped["ablation_status"] == "handler_covariate_ablation_v2"
    assert stamped["validation_status"] == row.status
    assert row.status == "passed"


def test_evaluate_gate_write_skips_stamp_for_unregistered_kind(tmp_path: Path) -> None:
    """``validate_artifact`` grades kinds with no registered validator as
    ``exploratory`` on the strength of a single info finding. Stamping that
    would overwrite a previously earned status with a non-verdict.
    """
    module = _load()
    root, pointer_path = _build_eda_artifact(tmp_path, "synthetic_eda", "aid-eda")
    manifest_path = root / "synthetic_eda" / "aid-eda" / "manifest.json"
    before = manifest_path.read_text(encoding="utf-8")

    row = module.evaluate_gate(
        "synthetic_eda",
        pointer_path,
        write=True,
        candidate_roots=(root,),
        datasets_root=tmp_path / "datasets",
    )

    assert row.status == "exploratory"
    assert manifest_path.read_text(encoding="utf-8") == before
    assert read_manifest(manifest_path).validation_status == "passed"
    assert row.manifest_status == "passed"


@pytest.mark.parametrize(
    ("model", "artifact_id", "held_out"),
    [
        (
            "synthetic_good",
            "aid-good",
            {
                "roc_auc": 0.9,
                "pr_auc": 0.9,
                "baseline_pr_auc": 0.3,
                "ece_held_out": 0.02,
            },
        ),
        (
            "synthetic_bad",
            "aid-bad",
            {
                "roc_auc": 0.4,
                "pr_auc": 0.1,
                "baseline_pr_auc": 0.3,
                "ece_held_out": 0.02,
            },
        ),
    ],
)
def test_evaluate_gate_write_report_status_matches_stamped_manifest_status(
    tmp_path: Path,
    model: str,
    artifact_id: str,
    held_out: dict[str, object],
) -> None:
    module = _load()
    bayes_root, pointer_path = _build_artifact(tmp_path, model, artifact_id, held_out)
    artifact_dir = bayes_root / model / artifact_id

    _ = module.evaluate_gate(
        model,
        pointer_path,
        write=True,
        candidate_roots=(bayes_root,),
        datasets_root=tmp_path / "datasets",
    )

    persisted = json.loads(
        (artifact_dir / "validation" / "validation_report.json").read_text(
            encoding="utf-8"
        )
    )
    assert (
        persisted["status"]
        == read_manifest(artifact_dir / "manifest.json").validation_status
    )


def test_write_stamped_status_flows_into_confidence_status_contract(
    tmp_path: Path,
) -> None:
    module = _load()
    bayes_root, pointer_path = _build_artifact(
        tmp_path,
        "synthetic_good",
        "aid-good",
        {"roc_auc": 0.9, "pr_auc": 0.9, "baseline_pr_auc": 0.3, "ece_held_out": 0.02},
    )
    manifest_path = bayes_root / "synthetic_good" / "aid-good" / "manifest.json"

    row = module.evaluate_gate(
        "synthetic_good",
        pointer_path,
        write=True,
        candidate_roots=(bayes_root,),
        datasets_root=tmp_path / "datasets",
    )
    assert row.status == "passed"

    stamped_manifest = read_manifest(manifest_path)
    frame = pl.DataFrame({"event_key": pl.Series("event_key", [1], dtype=pl.UInt32)})
    out = stamp_estimated_contract(
        frame, stamped_manifest, method="hierarchical_logistic"
    )

    assert out.get_column("confidence_status").to_list() == [row.status]


def test_format_table_columns_and_dashes() -> None:
    module = _load()
    rows = [
        module.GateRow(
            model="alpha",
            artifact_id="a1",
            status="passed",
            manifest_status="passed",
            hdi_coverage=None,
            findings=(),
        ),
        module.GateRow(
            model="state_transition",
            artifact_id="stv4",
            status="passed",
            manifest_status=None,
            hdi_coverage=0.9123,
            findings=("bayes_held_out_ece(warn)",),
        ),
    ]
    table = module.format_table(rows)
    lines = table.splitlines()
    assert lines[0].split()[:5] == [
        "model",
        "artifact_id",
        "status",
        "manifest_status",
        "hdi_cov",
    ]
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
