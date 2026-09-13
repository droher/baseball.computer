from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping, cast

from python_models.imputation.artifacts import (
    ComponentArtifact,
    FileIdentity,
    file_identity,
    sql_literal,
    verify_component,
)
from python_models.imputation.completion_registry import (
    CompletionRegistryPayload,
    SourcePopulation,
    build_completion_registry_payload,
)
from python_models.imputation.coverage import CoverageReport

CONTRACT_SCHEMA = {
    "artifact_id": "VARCHAR",
    "model_name": "VARCHAR",
    "model_version": "VARCHAR",
    "source_snapshot_id": "VARCHAR",
    "method": "VARCHAR",
    "observed_status": "VARCHAR",
    "confidence_status": "VARCHAR",
    "weak_identification_flag": "BOOLEAN",
}


def completed_schema(columns: Mapping[str, str]) -> dict[str, str]:
    return {**columns, **CONTRACT_SCHEMA}


def build_ingestion_sql(root: str, component: str, columns: Mapping[str, str]) -> str:
    schema = completed_schema(columns)
    if not root:
        return (
            "SELECT "
            + ", ".join(
                f'NULL::{kind} AS "{column}"' for column, kind in schema.items()
            )
            + " WHERE FALSE"
        )
    directory = Path(root)
    if not directory.is_absolute():
        raise ValueError("PBP imputation artifact root must be absolute")
    manifest = cast(
        dict[str, object], json.loads((directory / "manifest.json").read_text())
    )
    if (
        manifest.get("status") != "components_complete"
        or manifest.get("sample_games") is not None
    ):
        raise ValueError("Only completed full-population components can be ingested")
    if manifest.get("scope") != "existing_pbp_games_only":
        raise ValueError("Wrong imputation population")
    population = SourcePopulation.model_validate(manifest.get("source_population"))
    start_season = manifest.get("start_season")
    end_season = manifest.get("end_season")
    if (
        type(start_season) is not int
        or type(end_season) is not int
        or start_season > population.min_season
        or end_season < population.max_season
    ):
        raise ValueError("Artifact season range does not cover the full PBP population")
    validation = manifest.get("validation")
    checks = (
        cast(dict[str, object], validation).get(component)
        if isinstance(validation, dict)
        else None
    )
    if not isinstance(checks, dict):
        raise ValueError(
            "Component coverage and integrity validation is missing or failed"
        )
    checks = cast(dict[str, object], checks)
    if not {"missing_source_keys", "unexpected_keys", "duplicate_keys"}.issubset(
        checks.keys()
    ) or any(type(value) is not int or value != 0 for value in checks.values()):
        raise ValueError(
            "Component coverage and integrity validation is missing or failed"
        )
    source = FileIdentity.model_validate(manifest.get("source_database"))
    artifacts = manifest.get("artifacts")
    coverage_payload = (
        cast(dict[str, object], artifacts).get("coverage")
        if isinstance(artifacts, dict)
        else None
    )
    coverage = FileIdentity.model_validate(coverage_payload)
    registry_identity = FileIdentity.model_validate(manifest.get("completion_registry"))
    for identity, label in (
        (coverage, "Coverage artifact"),
        (registry_identity, "Completion registry"),
    ):
        if Path(identity.path).resolve().parent != directory.resolve():
            raise ValueError(f"{label} must be inside the bound artifact directory")
        if file_identity(Path(identity.path)) != identity:
            raise ValueError(f"{label} content changed")
    registry = CompletionRegistryPayload.model_validate_json(
        Path(registry_identity.path).read_text(encoding="utf-8")
    )
    coverage_report = CoverageReport.model_validate_json(
        Path(coverage.path).read_text(encoding="utf-8")
    )
    expected_registry = build_completion_registry_payload(
        coverage_report, source, coverage
    )
    if registry != expected_registry:
        raise ValueError("Completion registry does not match its bound coverage report")
    artifact_payload = (
        cast(dict[str, object], artifacts).get(component)
        if isinstance(artifacts, dict)
        else None
    )
    artifact = ComponentArtifact.model_validate(artifact_payload)
    if artifact.name != component:
        raise ValueError("Component identity mismatch")
    if Path(artifact.data.path).resolve().parent != directory.resolve():
        raise ValueError("Component file must be inside the bound artifact directory")
    if any(
        not Path(identity.path).resolve().is_relative_to(directory.resolve())
        for identity in (artifact.query, *artifact.dependencies)
    ):
        raise ValueError(
            "Component dependencies must be inside the bound artifact directory"
        )
    verify_component(artifact)
    if artifact.columns != dict(columns):
        raise ValueError("Component schema does not match the consumer")
    payload = [f'"{column}"' for column in columns if column not in CONTRACT_SCHEMA]
    metadata = {
        "artifact_id": artifact.data.sha256,
        "model_name": "pbp_completed_game_context"
        if component == "context"
        else (
            "pbp_completed_fielding_plays"
            if component == "fielding"
            else f"pbp_completed_{component}"
        ),
        "model_version": "1",
        "source_snapshot_id": source.sha256,
        "method": "source_preserving_pbp_completion",
        "observed_status": "estimated",
        "confidence_status": "exploratory",
    }
    payload.extend(
        f'"{column}"'
        if column in {"model_version", "confidence_status"} and column in columns
        else f'{sql_literal(value)} AS "{column}"'
        for column, value in metadata.items()
    )
    payload.append("TRUE AS weak_identification_flag")
    return (
        "SELECT "
        + ", ".join(payload)
        + f" FROM read_parquet({sql_literal(artifact.data.path)})"
    )
