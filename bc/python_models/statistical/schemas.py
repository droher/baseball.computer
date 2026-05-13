"""Pydantic v2 schemas for manifests, datasets, diagnostics, validation."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

ArtifactKind = Literal["dataset", "deep", "bayes", "sql_export"]
ValidationStatus = Literal["passed", "failed", "exploratory"]
AblationStatus = Literal["gamma_dl_zero", "gamma_dl_shrunk", "not_applicable"]
DiagnosticStatus = Literal["passed", "warn", "failed"]


class ArtifactManifest(BaseModel):
    artifact_id: str
    kind: ArtifactKind
    name: str
    version: str
    created_at: datetime
    source_snapshot_id: str
    query_hash: str | None = None
    schema_hash: str | None = None
    dataset_artifact_id: str | None = None
    input_artifact_ids: tuple[str, ...] = ()
    output_paths: dict[str, Path]
    package_versions: dict[str, str]
    random_seed: int | None = None
    validation_status: ValidationStatus = "exploratory"
    ablation_status: AblationStatus = "not_applicable"
    blocking_findings: tuple[str, ...] = ()
    metadata: dict[str, str | int | float | bool] = Field(default_factory=dict)


class DatasetColumn(BaseModel):
    name: str
    dtype: str
    role: str


class DatasetMetadata(BaseModel):
    dataset_name: str
    dataset_version: str
    source_snapshot_id: str
    query_hash: str
    row_count: int
    target_population_count: int
    observed_truth_count: int
    parquet_path: Path
    columns: tuple[DatasetColumn, ...]
    split_policy: str
    category_maps: dict[str, dict[str, int]]


class SplitAssignment(BaseModel):
    split_registry_id: str
    unit_type: str
    unit_id: str
    fold_id: int
    split_family: str
    holdout_regime: str


class Diagnostic(BaseModel):
    artifact_id: str
    model_name: str
    diagnostic_name: str
    variable: str
    slice_name: str | None = None
    value: float
    threshold: float
    status: DiagnosticStatus


class ValidationFinding(BaseModel):
    severity: Literal["block", "warn", "info"]
    code: str
    message: str


class PublishedPointer(BaseModel):
    model_name: str
    artifact_id: str
    published_at: datetime
    manifest_path: Path
    notes: str | None = None
