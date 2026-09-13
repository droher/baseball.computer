from __future__ import annotations

from typing import Mapping, Self

from duckdb import DuckDBPyConnection
from pydantic import BaseModel, ConfigDict, Field, model_validator

from python_models.imputation.artifacts import FileIdentity
from python_models.imputation.context import CONTEXT_FIELDS
from python_models.imputation.coverage import CoverageReport
from python_models.imputation.officials import OFFICIAL_ROLES


class CompletionTarget(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_field: str
    completed_field: str
    component: str | None = None
    evidence_fields: tuple[str, ...] = ()
    row_filter: str | None = None
    disposition: str
    source_missing_or_unspecified: int = Field(ge=0)
    source_missing_blocks: int = Field(ge=0)


class SourcePopulation(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    games: int = Field(gt=0)
    min_season: int = Field(ge=1800, le=2200)
    max_season: int = Field(ge=1800, le=2200)

    @model_validator(mode="after")
    def validate_seasons(self) -> Self:
        if self.min_season > self.max_season:
            raise ValueError("source population season range is reversed")
        return self


class CompletionRegistryPayload(BaseModel):
    model_config = ConfigDict(frozen=True)

    schema_version: str = "1"
    source_snapshot_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    coverage_snapshot_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    targets: tuple[CompletionTarget, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_targets(self) -> Self:
        source_fields = [target.source_field for target in self.targets]
        if len(source_fields) != len(set(source_fields)):
            raise ValueError("completion target source fields must be unique")
        incomplete = [
            target.source_field
            for target in self.targets
            if (
                target.source_missing_or_unspecified > 0
                or target.source_missing_blocks > 0
            )
            and target.component is None
        ]
        if incomplete:
            raise ValueError(
                f"applicable gaps lack completion components: {sorted(incomplete)}"
            )
        return self


def query_source_population(connection: DuckDBPyConnection) -> SourcePopulation:
    row = connection.execute(
        """
        SELECT
            COUNT(*)::BIGINT AS games,
            MIN(season)::INTEGER AS min_season,
            MAX(season)::INTEGER AS max_season
        FROM main_models.game_start_info
        WHERE source_type = 'PlayByPlay'
        """
    ).fetchone()
    if row is None or any(value is None for value in row):
        raise ValueError("PBP source population is empty")
    return SourcePopulation(
        games=int(row[0]), min_season=int(row[1]), max_season=int(row[2])
    )


def build_completion_registry_payload(
    report: CoverageReport,
    source_identity: FileIdentity,
    coverage_identity: FileIdentity,
) -> CompletionRegistryPayload:
    return CompletionRegistryPayload(
        source_snapshot_id=source_identity.sha256,
        coverage_snapshot_id=coverage_identity.sha256,
        targets=completion_targets(report),
    )


def _target(
    relation: str, column: str
) -> tuple[str, str | None, tuple[str, ...], str | None]:
    if relation == "main_models.stg_events":
        component = (
            "pitches"
            if column in {"count_balls", "count_strikes"}
            else "geometry"
            if column.startswith("batted_")
            else None
        )
        method = {
            "batted_trajectory": "trajectory_method",
            "batted_to_fielder": "handler_position_method",
            "batted_location_general": "general_location_method",
            "batted_location_depth": "location_depth_modifier_method",
            "batted_location_angle": "location_angle_method",
            "batted_contact_strength": "contact_strength_method",
            "count_balls": "count_balls_method",
            "count_strikes": "count_strikes_method",
        }.get(column)
        return (
            f"main_models.pbp_completed_events.{column}",
            component,
            (method,) if method else (),
            None,
        )
    if relation == "main_models.game_start_info":
        if column in OFFICIAL_ROLES:
            return (
                "main_models.pbp_completed_officials.candidate_identities",
                "officials",
                ("recorded_identity", "candidate_probabilities", "unresolved_slot"),
                f"role = '{column}'",
            )
        return (
            f"main_models.pbp_completed_games.{column}",
            "context" if column in CONTEXT_FIELDS else None,
            (f"{column}_method",) if column in CONTEXT_FIELDS else (),
            None,
        )
    if relation == "main_models.game_results":
        return (
            f"main_models.pbp_completed_games.{column}",
            "context" if column == "duration_minutes" else None,
            ("duration_minutes_method",) if column == "duration_minutes" else (),
            None,
        )
    if relation == "main_models.stg_event_fielding_plays":
        selected = (
            "completed_fielding_position" if column == "fielding_position" else column
        )
        return (
            f"main_models.pbp_completed_fielding_plays.{selected}",
            "fielding",
            ("completion_method", "constraint_disposition", "candidate_probabilities"),
            None,
        )
    if relation == "main_models.stg_event_baserunners" and column == "base_end":
        return (
            "main_models.pbp_completed_runners.completed_base_end",
            "runners",
            (
                "completed_destination",
                "destination_method",
                "constraint_disposition",
                "destination_support",
            ),
            None,
        )
    if relation == "main_models.stg_event_pitch_sequences":
        selected = {
            "sequence_id": "sequence_index",
            "sequence_item": "completed_sequence_item",
            "runners_going_flag": "completed_runners_going_flag",
            "blocked_by_catcher_flag": "completed_blocked_by_catcher_flag",
            "catcher_pickoff_attempt_at_base": "completed_catcher_pickoff_attempt_at_base",
        }.get(column, column)
        return (
            f"main_models.pbp_completed_pitch_items.{selected}",
            "pitches",
            ("item_method", "source_item_evidence_status", "constraint_disposition"),
            None,
        )
    if relation == "main_models.stg_event_pitch_sequence_status":
        return (
            f"main_models.pbp_completed_pitches.{column}",
            "pitches",
            ("source_resolution_status", "constraint_disposition"),
            None,
        )
    return f"{relation}.{column}", None, (), None


def completion_targets(report: CoverageReport) -> tuple[CompletionTarget, ...]:
    targets: list[CompletionTarget] = []
    for item in report.fields:
        field = item.field
        missing = sum(era.missing + era.sentinel for era in item.eras)
        missing_blocks = sum(era.source_block_missing or 0 for era in item.eras)
        selected, component, evidence, row_filter = _target(
            field.source_relation, field.source_column
        )
        if (missing or missing_blocks) and component is None:
            raise ValueError(f"Unmapped applicable gap: {field.identifier}")
        targets.append(
            CompletionTarget(
                source_field=field.identifier,
                completed_field=selected,
                component=component,
                evidence_fields=evidence,
                row_filter=row_filter,
                disposition="completed_with_explicit_evidence"
                if component
                else "source_complete_on_applicable_rows",
                source_missing_or_unspecified=missing,
                source_missing_blocks=missing_blocks,
            )
        )
    return tuple(targets)


def validate_completion_targets(
    targets: tuple[CompletionTarget, ...],
    columns_by_relation: Mapping[str, set[str]],
) -> None:
    failures: list[str] = []
    for target in targets:
        relation, column = target.completed_field.rsplit(".", 1)
        available = columns_by_relation.get(relation, set())
        if column not in available:
            failures.append(target.completed_field)
        failures.extend(
            f"{relation}.{evidence}"
            for evidence in target.evidence_fields
            if evidence not in available
        )
    if failures:
        raise ValueError(
            f"Completion targets are absent from consumer schemas: {sorted(set(failures))}"
        )
