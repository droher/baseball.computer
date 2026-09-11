from __future__ import annotations

import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest
import pyarrow.parquet as pq

from python_models.statistical.evidence_binding import bind_artifact, file_digest
from python_models.statistical.dataset_provenance import hash_schema
from python_models.statistical.manifests import read_manifest, write_manifest
from python_models.statistical.publication_evidence import (
    assess_publication,
    bind_validation_report,
    save_publication_evidence,
    verify_published_evidence,
)
from python_models.statistical.schemas import (
    ArtifactManifest,
    DatasetMetadata,
    ValidationEvidence,
    ValidationReport,
)
from python_models.statistical.validate import VALIDATION_GATE_VERSION


def _dataset(root: Path) -> Path:
    path = root / "datasets" / "input" / "data-1" / "manifest.json"
    path.parent.mkdir(parents=True)
    payload = path.with_name("dataset.parquet")
    pl.DataFrame({"event_key": [1, 2], "label": [0, 1]}).write_parquet(payload)
    write_manifest(
        ArtifactManifest(
            artifact_id="data-1",
            kind="dataset",
            name="input",
            version="1",
            created_at=datetime.now(timezone.utc),
            source_snapshot_id="dev",
            output_paths={"dataset": payload},
            package_versions={},
            content_hash=file_digest(payload),
            schema_hash=hash_schema(pq.read_schema(payload)),
            transformation_hash=hashlib.sha256(b"transformation").hexdigest(),
            dependency_hash=hashlib.sha256(b"dependencies").hexdigest(),
        ),
        path,
    )
    manifest = read_manifest(path)
    metadata = DatasetMetadata(
        dataset_name="input",
        dataset_version="1",
        source_snapshot_id="dev",
        query_hash="query",
        row_count=2,
        target_population_count=2,
        observed_truth_count=2,
        parquet_path=payload,
        columns=(),
        split_policy="fixture",
        category_maps={},
        content_hash=manifest.content_hash,
        schema_hash=manifest.schema_hash,
        transformation_hash=manifest.transformation_hash,
        dependency_hash=manifest.dependency_hash,
    )
    path.with_name("dataset_metadata.json").write_text(
        metadata.model_dump_json(indent=2)
    )
    return path


def _artifact(root: Path, dataset: Path, name: str = "fit") -> Path:
    path = root / "deep" / name / "fit-1" / "manifest.json"
    path.parent.mkdir(parents=True)
    write_manifest(
        ArtifactManifest(
            artifact_id="fit-1",
            kind="deep",
            name=name,
            version="1",
            created_at=datetime.now(timezone.utc),
            source_snapshot_id="dev",
            dataset_artifact_id="data-1",
            input_artifact_ids=("data-1",),
            input_manifests=(dataset,),
            metadata={"pretraining": "none"},
            output_paths={},
            package_versions={},
        ),
        path,
    )
    path.with_name("payload.txt").write_text("frozen predictions")
    return path


def _report(path: Path) -> ValidationReport:
    manifest = read_manifest(path)
    return ValidationReport(
        artifact_id=manifest.artifact_id,
        kind=manifest.kind,
        name=manifest.name,
        generated_at=datetime.now(timezone.utc),
        status="passed",
        evidence=ValidationEvidence(
            numerical="passed",
            predictive="passed",
            calibration="passed",
        ),
    )


def test_binding_survives_saving_evidence_but_detects_payload_change(
    tmp_path: Path,
) -> None:
    dataset = _dataset(tmp_path)
    path = _artifact(tmp_path, dataset)
    report = bind_validation_report(_report(path), path, candidate_roots=())
    assert report.evidence.provenance == "passed"
    save_publication_evidence(report, path, exploratory=False)
    verify_published_evidence(
        path,
        expected_binding=report.artifact_binding,
        candidate_roots=(),
        exploratory=False,
    )
    assert bind_artifact(path).digest == report.artifact_binding
    path.with_name("payload.txt").write_text("changed predictions")
    with pytest.raises(ValueError, match="changed after validation"):
        verify_published_evidence(
            path,
            expected_binding=report.artifact_binding,
            candidate_roots=(),
            exploratory=False,
        )


def test_dataset_content_change_fails_even_with_same_schema(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    path = _artifact(tmp_path, dataset)
    pl.DataFrame({"event_key": [1, 2], "label": [1, 0]}).write_parquet(
        dataset.with_name("dataset.parquet")
    )
    with pytest.raises(ValueError, match="content differs"):
        bind_artifact(path)


def test_missing_lineage_blocks_default_publication(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    path = _artifact(tmp_path, dataset)
    manifest = read_manifest(path).model_copy(update={"input_manifests": ()})
    write_manifest(manifest, path)
    report = bind_validation_report(
        _report(path), path, candidate_roots=(tmp_path / "datasets",)
    )
    assert report.status == "failed"
    assert report.evidence.provenance == "unsupported"
    with pytest.raises(ValueError, match="not validated"):
        assess_publication(path, candidate_roots=(tmp_path / "datasets",))


def test_exploratory_publication_preserves_failed_validation(tmp_path: Path) -> None:
    path = _artifact(tmp_path, _dataset(tmp_path))
    report = assess_publication(
        path,
        candidate_roots=(),
        exploratory_reason="Development artifact; heldout scoring pending",
    )
    assert report.status == "failed"
    save_publication_evidence(report, path, exploratory=True)
    assert read_manifest(path).validation_status == "failed"
    verify_published_evidence(
        path,
        expected_binding=report.artifact_binding,
        candidate_roots=(),
        exploratory=True,
    )
    with pytest.raises(ValueError, match="mode disagree"):
        verify_published_evidence(
            path,
            expected_binding=report.artifact_binding,
            candidate_roots=(),
            exploratory=False,
        )


def test_empty_exploratory_reason_is_rejected(tmp_path: Path) -> None:
    path = _artifact(tmp_path, _dataset(tmp_path))
    with pytest.raises(ValueError, match="nonempty reason"):
        assess_publication(path, candidate_roots=(), exploratory_reason="  ")


def test_incomplete_dependency_evidence_blocks_root(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    dependency = _artifact(tmp_path, dataset, "proposal")
    saved = bind_validation_report(_report(dependency), dependency, candidate_roots=())
    saved.evidence.calibration = "unsupported"
    save_publication_evidence(saved, dependency, exploratory=True)
    path = _artifact(tmp_path, dataset)
    manifest = read_manifest(path).model_copy(
        update={
            "input_manifests": (dataset, dependency),
            "input_artifact_ids": ("data-1", "fit-1"),
        }
    )
    write_manifest(manifest, path)
    report = bind_validation_report(_report(path), path, candidate_roots=())
    assert report.status == "failed"
    assert any(f.code == "dependency_validation_not_passed" for f in report.findings)


def test_dependency_cycle_is_rejected(tmp_path: Path) -> None:
    path = _artifact(tmp_path, _dataset(tmp_path))
    write_manifest(
        read_manifest(path).model_copy(update={"input_manifests": (path,)}), path
    )
    with pytest.raises(ValueError, match="cycle"):
        bind_artifact(path)


@pytest.mark.parametrize("revoke", ["delete", "downgrade"])
def test_revoked_dependency_evidence_invalidates_published_parent(
    tmp_path: Path, revoke: str
) -> None:
    dataset = _dataset(tmp_path)
    dependency = _artifact(tmp_path, dataset, "proposal")
    saved = bind_validation_report(_report(dependency), dependency, candidate_roots=())
    save_publication_evidence(saved, dependency, exploratory=False)
    path = _artifact(tmp_path, dataset)
    write_manifest(
        read_manifest(path).model_copy(
            update={"input_manifests": (dataset, dependency)}
        ),
        path,
    )
    report = bind_validation_report(_report(path), path, candidate_roots=())
    save_publication_evidence(report, path, exploratory=False)
    verify_published_evidence(
        path,
        expected_binding=report.artifact_binding,
        candidate_roots=(),
        exploratory=False,
    )
    report_path = dependency.parent / "validation" / "validation_report.json"
    if revoke == "delete":
        report_path.unlink()
    else:
        saved.evidence.calibration = "failed"
        report_path.write_text(saved.model_dump_json(indent=2))
    assert bind_artifact(path).digest == report.artifact_binding
    with pytest.raises(ValueError, match="incomplete"):
        verify_published_evidence(
            path,
            expected_binding=report.artifact_binding,
            candidate_roots=(),
            exploratory=False,
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_hash", "placeholder"),
        ("schema_hash", "a" * 64),
        ("dependency_hash", "b" * 64),
    ],
)
def test_dataset_provenance_fields_are_verified(
    tmp_path: Path, field: str, value: str
) -> None:
    dataset = _dataset(tmp_path)
    write_manifest(read_manifest(dataset).model_copy(update={field: value}), dataset)
    with pytest.raises(ValueError, match="SHA-256|schema differs|metadata disagree"):
        bind_artifact(dataset)


def test_report_and_pointer_identity_are_checked(tmp_path: Path) -> None:
    path = _artifact(tmp_path, _dataset(tmp_path))
    wrong = _report(path).model_copy(update={"artifact_id": "other"})
    with pytest.raises(ValueError, match="identity"):
        bind_validation_report(wrong, path, candidate_roots=())
    report = bind_validation_report(_report(path), path, candidate_roots=())
    save_publication_evidence(report, path, exploratory=False)
    with pytest.raises(ValueError, match="identity"):
        verify_published_evidence(
            path,
            expected_binding=report.artifact_binding,
            candidate_roots=(),
            exploratory=False,
            expected_artifact_id="other",
        )


def test_changed_manifest_settings_and_stale_gate_invalidate_evidence(
    tmp_path: Path,
) -> None:
    path = _artifact(tmp_path, _dataset(tmp_path))
    report = bind_validation_report(_report(path), path, candidate_roots=())
    save_publication_evidence(report, path, exploratory=False)
    saved_path = path.parent / "validation" / "validation_report.json"
    payload = json.loads(saved_path.read_text())
    payload["gate_version"] = VALIDATION_GATE_VERSION - 1
    saved_path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="gate version is stale"):
        verify_published_evidence(
            path,
            expected_binding=report.artifact_binding,
            candidate_roots=(),
            exploratory=False,
        )
    save_publication_evidence(report, path, exploratory=False)
    write_manifest(read_manifest(path).model_copy(update={"random_seed": 25}), path)
    with pytest.raises(ValueError, match="changed after validation"):
        verify_published_evidence(
            path,
            expected_binding=report.artifact_binding,
            candidate_roots=(),
            exploratory=False,
        )
