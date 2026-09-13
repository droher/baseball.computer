from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from python_models.imputation.artifacts import file_identity
from python_models.imputation.completion_registry import (
    CompletionRegistryPayload,
    build_completion_registry_payload,
    completion_targets,
    query_source_population,
    validate_completion_targets,
)
from python_models.imputation.coverage import CoverageReport, EraCoverage, FieldCoverage
from python_models.imputation.registry import default_field_registry


def _report(identifier: str, missing: int) -> CoverageReport:
    field = next(
        field
        for field in default_field_registry().fields
        if field.identifier == identifier
    )
    return CoverageReport(
        season_start=1903,
        season_end=1903,
        sample_game_limit=None,
        sampled_games=1,
        fields=(
            FieldCoverage(
                field=field,
                eras=(
                    EraCoverage(
                        era="early",
                        season_start=1903,
                        season_end=1903,
                        rows=1,
                        applicable=1,
                        observed=1 - missing,
                        missing=missing,
                        sentinel=0,
                    ),
                ),
            ),
        ),
        excluded_fields=(),
    )


def test_unmapped_applicable_gap_prevents_closure() -> None:
    with pytest.raises(ValueError, match="Unmapped applicable gap"):
        completion_targets(_report("main_models.personnel_lineup_states.player_id", 1))
    targets = completion_targets(
        _report("main_models.personnel_lineup_states.player_id", 0)
    )
    assert targets[0].disposition == "source_complete_on_applicable_rows"


def test_official_identity_requires_candidates_and_unresolved_evidence() -> None:
    targets = completion_targets(
        _report("main_models.game_start_info.umpire_home_id", 1)
    )
    target = targets[0]
    assert target.row_filter == "role = 'umpire_home_id'"
    assert "unresolved_slot" in target.evidence_fields
    relation, column = target.completed_field.rsplit(".", 1)
    with pytest.raises(ValueError, match="consumer schemas"):
        validate_completion_targets(targets, {relation: {column}})
    validate_completion_targets(targets, {relation: {column, *target.evidence_fields}})


def test_runner_destination_maps_to_completed_child() -> None:
    target = completion_targets(
        _report("main_models.stg_event_baserunners.base_end", 1)
    )[0]
    assert target.completed_field.endswith("pbp_imputed_runners.completed_base_end")
    assert "destination_support" in target.evidence_fields


def test_source_population_uses_every_pbp_game() -> None:
    with duckdb.connect() as connection:
        connection.execute("CREATE SCHEMA main_models")
        connection.execute(
            """
            CREATE TABLE main_models.game_start_info (
                game_id VARCHAR, season INTEGER, source_type VARCHAR
            )
            """
        )
        connection.execute(
            """
            INSERT INTO main_models.game_start_info VALUES
                ('early', 1903, 'PlayByPlay'),
                ('late', 2025, 'PlayByPlay'),
                ('box', 1890, 'BoxScore')
            """
        )
        population = query_source_population(connection)
    assert population.games == 2
    assert population.min_season == 1903
    assert population.max_season == 2025


def test_registry_payload_binds_source_and_coverage(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    coverage = tmp_path / "coverage.json"
    source.write_text("source")
    coverage.write_text("coverage")
    payload = build_completion_registry_payload(
        _report("main_models.stg_events.count_balls", 1),
        file_identity(source),
        file_identity(coverage),
    )
    assert payload.source_snapshot_id == file_identity(source).sha256
    assert payload.coverage_snapshot_id == file_identity(coverage).sha256
    assert payload.targets[0].component == "pitches"


def test_registry_payload_rejects_duplicate_targets_and_unmapped_gaps() -> None:
    mapped = completion_targets(_report("main_models.stg_events.count_balls", 1))[0]
    with pytest.raises(ValueError, match="unique"):
        CompletionRegistryPayload(
            source_snapshot_id="a" * 64,
            coverage_snapshot_id="b" * 64,
            targets=(mapped, mapped),
        )
    with pytest.raises(ValueError, match="lack completion components"):
        CompletionRegistryPayload(
            source_snapshot_id="a" * 64,
            coverage_snapshot_id="b" * 64,
            targets=(mapped.model_copy(update={"component": None}),),
        )
