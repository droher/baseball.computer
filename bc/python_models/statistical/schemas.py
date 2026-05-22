"""Pydantic v2 schemas for manifests, datasets, diagnostics, validation."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, model_validator

ArtifactKind = Literal["dataset", "deep", "bayes", "sql_export", "eda", "pretrain"]
ValidationStatus = Literal["passed", "failed", "exploratory"]
DiagnosticStatus = Literal["passed", "warn", "failed"]
FindingSeverity = Literal["block", "warn", "info"]
BlockingCode = Literal[
    "source_family_block_as_event_missing",
    "dominant_single_scorer_park_team",
    "no_connected_component_for_effect",
    "data_error_rows_train_as_truth",
    "split_leakage_detected",
    "category_absent_in_train_present_in_test",
    "constraint_violation_in_dataset",
]


class BayesPriorConfig(BaseModel):
    alpha_loc: float = 0.0
    alpha_scale: float = 1.5
    sigma_season_scale: float = 1.5
    sigma_scorer_scale: float = 1.5
    sigma_park_scale: float = 1.0
    sigma_source_scale: float = 0.7
    fixed_effect_scale: float = 1.0
    continuous_slope_scale: float = 0.5


class BayesSamplerConfig(BaseModel):
    draws: int
    tune: int
    chains: int
    target_accept: float
    random_seed: int
    max_treedepth: int = 10
    is_smoke: bool = False
    backend: str = "pymc"


class BayesPosteriorRow(BaseModel):
    variable: str
    coord_label: str | None = None
    mean: float
    sd: float
    hdi_lower: float
    hdi_upper: float
    ess_bulk: float
    ess_tail: float
    rhat: float


class BayesPosteriorSummary(BaseModel):
    rows: tuple[BayesPosteriorRow, ...] = ()


class BayesDiagnosticsSummary(BaseModel):
    rhat_max: float
    ess_bulk_min: float
    ess_tail_min: float
    divergences: int
    total_draws: int
    calibration_ece: float | None = None
    posterior_predictive_max_bucket_dev: float | None = None
    diagnostics: tuple["Diagnostic", ...] = ()


class BayesArtifactExtras(BaseModel):
    model_name: str
    model_version: str
    dimension: str | None = None
    prior_config: BayesPriorConfig
    sampler_config: BayesSamplerConfig
    posterior_summary: BayesPosteriorSummary = BayesPosteriorSummary()
    diagnostics_summary: BayesDiagnosticsSummary
    inference_files: dict[str, Path] = Field(default_factory=dict)
    source_effect_active: bool = True
    event_row_count: int = 0


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
    blocking_findings: tuple[str, ...] = ()
    metadata: dict[str, str | int | float | bool] = Field(default_factory=dict)
    bayes_extras: BayesArtifactExtras | None = None

    @model_validator(mode="after")
    def _bayes_extras_required_for_bayes_kind(self) -> "ArtifactManifest":
        if self.kind == "bayes" and self.bayes_extras is None:
            raise ValueError("ArtifactManifest with kind='bayes' must set bayes_extras")
        return self


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
    severity: FindingSeverity
    code: str
    message: str


class BlockingFinding(BaseModel):
    code: BlockingCode
    severity: FindingSeverity
    message: str
    evidence_path: Path | None = None


class WeakIdentificationFlag(BaseModel):
    effect: str
    slice: str
    share: float
    reason: str


class EdaReport(BaseModel):
    dataset_name: str
    dataset_version: str
    dataset_artifact_id: str
    source_snapshot_id: str
    row_count: int
    target_population_count: int
    observed_truth_count: int
    source_family_block_missing_count: int
    data_error_excluded_count: int
    module_paths: dict[str, Path]
    blocking_findings: tuple[BlockingFinding, ...] = ()
    weak_identification_flags: tuple[WeakIdentificationFlag, ...] = ()
    recommended_formula_terms: tuple[str, ...] = ()


class PublishedPointer(BaseModel):
    model_name: str
    artifact_id: str
    published_at: datetime
    manifest_path: Path
    notes: str | None = None


class ValidationReport(BaseModel):
    artifact_id: str
    kind: ArtifactKind
    name: str
    status: ValidationStatus
    findings: tuple[ValidationFinding, ...] = ()
    diagnostics: tuple[Diagnostic, ...] = ()
    metrics: dict[str, float | int] = Field(default_factory=dict)
    metadata: dict[str, str | int | float | bool] = Field(default_factory=dict)
    generated_at: datetime


_ = BayesDiagnosticsSummary.model_rebuild()
_ = BayesArtifactExtras.model_rebuild()
_ = ArtifactManifest.model_rebuild()
