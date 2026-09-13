from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from python_models.imputation.artifacts import (
    FileIdentity,
    export_component,
    file_identity,
    write_json,
)
from python_models.imputation.completion_registry import (
    build_completion_registry_payload,
)
from python_models.imputation.coverage import CoverageReport, EraCoverage, FieldCoverage
from python_models.imputation.ingest import build_ingestion_sql, completed_schema
from python_models.imputation.registry import default_field_registry


def _bind_completion_registry(
    root: Path, manifest: dict[str, object], source_sha256: str
) -> None:
    field = next(
        field
        for field in default_field_registry().fields
        if field.identifier == "main_models.stg_events.count_balls"
    )
    report = CoverageReport(
        season_start=1903,
        season_end=2025,
        sample_game_limit=None,
        sampled_games=2,
        fields=(
            FieldCoverage(
                field=field,
                eras=(
                    EraCoverage(
                        era="all",
                        season_start=1903,
                        season_end=2025,
                        rows=2,
                        applicable=2,
                        observed=1,
                        missing=1,
                        sentinel=0,
                    ),
                ),
            ),
        ),
        excluded_fields=(),
    )
    coverage = root / "coverage.json"
    coverage.write_text(report.model_dump_json(indent=2) + "\n")
    coverage_identity = file_identity(coverage)
    registry = root / "completion_registry.json"
    source_identity = FileIdentity(
        path=str(root / "source.db"), bytes=0, sha256=source_sha256
    )
    registry.write_text(
        build_completion_registry_payload(
            report, source_identity, coverage_identity
        ).model_dump_json(indent=2)
        + "\n"
    )
    artifacts = manifest["artifacts"]
    assert isinstance(artifacts, dict)
    artifacts["coverage"] = coverage_identity.model_dump()
    manifest["completion_registry"] = file_identity(registry).model_dump()


def test_empty_unconfigured_surface_has_explicit_schema() -> None:
    columns = {"event_key": "BIGINT", "value": "DOUBLE"}
    with duckdb.connect() as connection:
        result = connection.execute(build_ingestion_sql("", "example", columns))
        assert result.fetchall() == []
        assert [column[0] for column in result.description] == list(
            completed_schema(columns)
        )


def test_full_bound_artifact_ingests_and_smoke_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "source.txt"
    source.write_text("source identity fixture")
    columns = {"event_key": "INTEGER", "value": "DOUBLE"}
    with duckdb.connect() as connection:
        artifact = export_component(
            connection,
            tmp_path,
            name="example",
            query="SELECT 1 AS event_key, 0.5::DOUBLE AS value",
            expected_columns=columns,
        )
        manifest: dict[str, object] = {
            "status": "components_complete",
            "sample_games": None,
            "scope": "existing_pbp_games_only",
            "start_season": 1903,
            "end_season": 2025,
            "source_population": {
                "games": 2,
                "min_season": 1903,
                "max_season": 2025,
            },
            "source_database": file_identity(source).model_dump(),
            "artifacts": {"example": artifact.model_dump()},
            "validation": {
                "example": {
                    "missing_source_keys": 0,
                    "unexpected_keys": 0,
                    "duplicate_keys": 0,
                }
            },
        }
        source_identity = file_identity(source)
        _bind_completion_registry(tmp_path, manifest, source_identity.sha256)
        write_json(tmp_path / "manifest.json", manifest)
        result = connection.execute(
            build_ingestion_sql(str(tmp_path), "example", columns)
        ).fetchone()
        assert result is not None
        assert result[0:2] == (1, 0.5)
        assert "exploratory" in result
        manifest["validation"] = {
            "example": {
                "missing_source_keys": 1,
                "unexpected_keys": 0,
                "duplicate_keys": 0,
            }
        }
        write_json(tmp_path / "manifest.json", manifest)
        with pytest.raises(ValueError, match="validation"):
            build_ingestion_sql(str(tmp_path), "example", columns)
        manifest["validation"] = {
            "example": {
                "missing_source_keys": 0,
                "unexpected_keys": 0,
                "duplicate_keys": 0,
            }
        }
        manifest["status"] = "smoke_complete"
        write_json(tmp_path / "manifest.json", manifest)
        with pytest.raises(ValueError, match="full-population"):
            build_ingestion_sql(str(tmp_path), "example", columns)
        manifest["status"] = "components_complete"
        write_json(tmp_path / "manifest.json", manifest)
        Path(artifact.data.path).write_bytes(b"changed")
        with pytest.raises(ValueError, match="changed"):
            build_ingestion_sql(str(tmp_path), "example", columns)


def test_partial_history_and_unbound_registry_are_rejected(tmp_path: Path) -> None:
    source = tmp_path / "source.txt"
    source.write_text("source identity fixture")
    columns = {"event_key": "INTEGER", "value": "DOUBLE"}
    with duckdb.connect() as connection:
        artifact = export_component(
            connection,
            tmp_path,
            name="example",
            query="SELECT 1 AS event_key, 0.5::DOUBLE AS value",
            expected_columns=columns,
        )
    source_identity = file_identity(source)
    manifest: dict[str, object] = {
        "status": "components_complete",
        "sample_games": None,
        "scope": "existing_pbp_games_only",
        "start_season": 2025,
        "end_season": 2025,
        "source_population": {
            "games": 2,
            "min_season": 1903,
            "max_season": 2025,
        },
        "source_database": source_identity.model_dump(),
        "artifacts": {"example": artifact.model_dump()},
        "validation": {
            "example": {
                "missing_source_keys": 0,
                "unexpected_keys": 0,
                "duplicate_keys": 0,
            }
        },
    }
    _bind_completion_registry(tmp_path, manifest, source_identity.sha256)
    write_json(tmp_path / "manifest.json", manifest)
    with pytest.raises(ValueError, match="season range"):
        build_ingestion_sql(str(tmp_path), "example", columns)
    manifest["start_season"] = 1903
    registry_identity = manifest["completion_registry"]
    assert isinstance(registry_identity, dict)
    registry_path = Path(str(registry_identity["path"]))
    outside_registry = tmp_path.parent / f"{tmp_path.name}-outside-registry.json"
    outside_registry.write_text(registry_path.read_text())
    manifest["completion_registry"] = file_identity(outside_registry).model_dump()
    write_json(tmp_path / "manifest.json", manifest)
    with pytest.raises(ValueError, match="inside the bound artifact directory"):
        build_ingestion_sql(str(tmp_path), "example", columns)
    manifest["completion_registry"] = registry_identity
    registry_path.write_text("{}")
    write_json(tmp_path / "manifest.json", manifest)
    with pytest.raises(ValueError, match="Completion registry content changed"):
        build_ingestion_sql(str(tmp_path), "example", columns)
