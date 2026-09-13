from __future__ import annotations

import logging
import re
from typing import Self

from duckdb import DuckDBPyConnection
from pydantic import BaseModel, ConfigDict, Field, model_validator

from python_models.imputation.registry import (
    FieldDefinition,
    FieldRegistry,
    FieldScope,
)


logger = logging.getLogger(__name__)
_ERA_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


class EraDefinition(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    season_start: int = Field(ge=1800, le=2200)
    season_end: int = Field(ge=1800, le=2200)

    @model_validator(mode="after")
    def validate_era(self) -> Self:
        if not _ERA_NAME.fullmatch(self.name):
            raise ValueError("era names must be safe identifiers")
        if self.season_start > self.season_end:
            raise ValueError("era season_start must not exceed season_end")
        return self


def default_eras() -> tuple[EraDefinition, ...]:
    return (
        EraDefinition(name="early", season_start=1903, season_end=1919),
        EraDefinition(name="prewar", season_start=1920, season_end=1946),
        EraDefinition(name="integration", season_start=1947, season_end=1968),
        EraDefinition(name="expansion", season_start=1969, season_end=1987),
        EraDefinition(name="detailed_pbp", season_start=1988, season_end=2007),
        EraDefinition(name="modern", season_start=2008, season_end=2025),
    )


class CoverageOptions(BaseModel):
    model_config = ConfigDict(frozen=True)

    season_start: int = Field(default=1903, ge=1800, le=2200)
    season_end: int = Field(default=2025, ge=1800, le=2200)
    sample_game_limit: int | None = Field(default=None, ge=1)
    eras: tuple[EraDefinition, ...] = Field(default_factory=default_eras)

    @model_validator(mode="after")
    def validate_options(self) -> Self:
        if self.season_start > self.season_end:
            raise ValueError("season_start must not exceed season_end")
        ordered = sorted(self.eras, key=lambda era: era.season_start)
        for previous, current in zip(ordered, ordered[1:], strict=False):
            if previous.season_end >= current.season_start:
                raise ValueError("coverage eras must not overlap")
            if previous.season_end + 1 != current.season_start:
                raise ValueError("coverage eras must not leave season gaps")
        if not ordered:
            raise ValueError("at least one coverage era is required")
        if (
            ordered[0].season_start > self.season_start
            or ordered[-1].season_end < self.season_end
        ):
            raise ValueError("coverage eras must span the requested season range")
        return self


class EraCoverage(BaseModel):
    model_config = ConfigDict(frozen=True)

    era: str
    season_start: int
    season_end: int
    rows: int
    applicable: int
    observed: int
    missing: int
    sentinel: int
    parent_rows: int | None = None
    source_block_missing: int | None = None
    source_unresolved: int | None = None

    @model_validator(mode="after")
    def validate_accounting(self) -> Self:
        if self.observed + self.missing + self.sentinel != self.applicable:
            raise ValueError(
                "observed, missing, and sentinel must partition applicable"
            )
        if self.applicable > self.rows:
            raise ValueError("applicable rows cannot exceed source rows")
        parent_counts = (
            self.parent_rows,
            self.source_block_missing,
            self.source_unresolved,
        )
        if any(value is not None for value in parent_counts) and any(
            value is None for value in parent_counts
        ):
            raise ValueError("parent-grain source counts must be supplied together")
        if (
            self.parent_rows is not None
            and self.source_block_missing is not None
            and self.source_unresolved is not None
            and self.source_block_missing + self.source_unresolved > self.parent_rows
        ):
            raise ValueError("missing and unresolved parent rows cannot exceed parents")
        return self


class FieldCoverage(BaseModel):
    model_config = ConfigDict(frozen=True)

    field: FieldDefinition
    eras: tuple[EraCoverage, ...]


class CoverageReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    season_start: int
    season_end: int
    sample_game_limit: int | None
    sampled_games: int
    fields: tuple[FieldCoverage, ...]
    excluded_fields: tuple[FieldDefinition, ...]


def _quote_identifier(identifier: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", identifier):
        raise ValueError(f"unsafe SQL identifier: {identifier}")
    return f'"{identifier}"'


def _quote_relation(relation: str) -> str:
    parts = relation.split(".")
    if len(parts) != 2:
        raise ValueError(f"unsafe SQL relation: {relation}")
    return ".".join(_quote_identifier(part) for part in parts)


def _render_expression(expression: str, field: FieldDefinition) -> str:
    value = f"src.{_quote_identifier(field.source_column)}"
    rendered = expression.replace("{value}", value)
    if "{" in rendered or "}" in rendered:
        raise ValueError(f"unknown SQL placeholder in {field.identifier}")
    return rendered


def _sampled_games_cte(options: CoverageOptions) -> tuple[str, list[int]]:
    limit = ""
    if options.sample_game_limit is not None:
        limit = f" LIMIT {options.sample_game_limit}"
    return (
        f"""
        sampled_games AS (
            SELECT game.*
            FROM main_models.game_start_info AS game
            WHERE game.season BETWEEN ? AND ?
              AND game.source_type = 'PlayByPlay'
              AND EXISTS (
                  SELECT 1
                  FROM main_models.stg_events AS event
                  WHERE event.game_id = game.game_id
              )
            ORDER BY game.season, game.game_id
            {limit}
        )
        """,
        [options.season_start, options.season_end],
    )


def _coverage_for_field(
    connection: DuckDBPyConnection,
    field: FieldDefinition,
    options: CoverageOptions,
    pitch_absence: dict[str, tuple[int, int, int]],
) -> FieldCoverage:
    games_cte, parameters = _sampled_games_cte(options)
    relation = _quote_relation(field.source_relation)
    missing = _render_expression(field.missing_sql, field)
    sentinel = _render_expression(field.sentinel_sql, field)
    applicable = _render_expression(field.applicability_sql, field)
    needs_event = "event." in applicable or "event." in missing or "event." in sentinel
    event_join = ""
    if needs_event:
        if "event_key" not in field.grain_keys:
            raise ValueError(f"{field.identifier} references event outside event grain")
        event_join = (
            "LEFT JOIN main_models.stg_events AS event "
            "ON event.event_key = src.event_key"
        )
    era_case_parts: list[str] = []
    for era in options.eras:
        era_case_parts.append(
            f"WHEN game.season BETWEEN {era.season_start} AND {era.season_end} "
            f"THEN '{era.name}'"
        )
    era_case = "CASE " + " ".join(era_case_parts) + " END"
    query = f"""
        WITH {games_cte}, classified AS (
            SELECT
                {era_case} AS era,
                ({applicable}) AS is_applicable,
                ({missing}) AS is_missing,
                ({sentinel}) AS is_sentinel
            FROM {relation} AS src
            JOIN sampled_games AS game ON game.game_id = src.game_id
            {event_join}
        )
        SELECT
            era,
            COUNT(*)::BIGINT AS rows,
            COUNT(*) FILTER (WHERE is_applicable)::BIGINT AS applicable,
            COUNT(*) FILTER (
                WHERE is_applicable AND NOT is_missing AND NOT is_sentinel
            )::BIGINT AS observed,
            COUNT(*) FILTER (WHERE is_applicable AND is_missing)::BIGINT AS missing,
            COUNT(*) FILTER (
                WHERE is_applicable AND NOT is_missing AND is_sentinel
            )::BIGINT AS sentinel
        FROM classified
        WHERE era IS NOT NULL
        GROUP BY era
    """
    rows = connection.execute(query, parameters).fetchall()
    by_era = {str(row[0]): row[1:] for row in rows}
    coverage: list[EraCoverage] = []
    for era in options.eras:
        counts = by_era.get(era.name, (0, 0, 0, 0, 0))
        coverage.append(
            EraCoverage(
                era=era.name,
                season_start=era.season_start,
                season_end=era.season_end,
                rows=int(counts[0]),
                applicable=int(counts[1]),
                observed=int(counts[2]),
                missing=int(counts[3]),
                sentinel=int(counts[4]),
                parent_rows=(
                    pitch_absence[era.name][0]
                    if field.source_relation == "main_models.stg_event_pitch_sequences"
                    else None
                ),
                source_block_missing=(
                    pitch_absence[era.name][1]
                    if field.source_relation == "main_models.stg_event_pitch_sequences"
                    else None
                ),
                source_unresolved=(
                    pitch_absence[era.name][2]
                    if field.source_relation == "main_models.stg_event_pitch_sequences"
                    else None
                ),
            )
        )
    return FieldCoverage(field=field, eras=tuple(coverage))


def _pitch_absence_by_era(
    connection: DuckDBPyConnection, options: CoverageOptions
) -> dict[str, tuple[int, int, int]]:
    games_cte, parameters = _sampled_games_cte(options)
    era_case_parts = [
        f"WHEN game.season BETWEEN {era.season_start} AND {era.season_end} "
        f"THEN '{era.name}'"
        for era in options.eras
    ]
    era_case = "CASE " + " ".join(era_case_parts) + " END"
    rows = connection.execute(
        f"""
        WITH {games_cte}, appearances AS (
            SELECT
                {era_case} AS era,
                status.game_id,
                status.appearance_start_event_id,
                ANY_VALUE(status.pitch_sequence_resolution_status) AS resolution_status
            FROM main_models.stg_event_pitch_sequence_status AS status
            JOIN sampled_games AS game ON game.game_id = status.game_id
            GROUP BY era, status.game_id, status.appearance_start_event_id
        )
        SELECT
            era,
            COUNT(*)::BIGINT,
            COUNT(*) FILTER (WHERE resolution_status = 'Unavailable')::BIGINT,
            COUNT(*) FILTER (WHERE resolution_status = 'Unresolved')::BIGINT
        FROM appearances
        WHERE era IS NOT NULL
        GROUP BY era
        """,
        parameters,
    ).fetchall()
    counts = {str(row[0]): (int(row[1]), int(row[2]), int(row[3])) for row in rows}
    return {era.name: counts.get(era.name, (0, 0, 0)) for era in options.eras}


def audit_coverage(
    connection: DuckDBPyConnection,
    registry: FieldRegistry,
    options: CoverageOptions | None = None,
) -> CoverageReport:
    resolved_options = options or CoverageOptions()
    unclassified = registry.unclassified_columns(connection)
    if unclassified:
        formatted = ", ".join(
            f"{relation}.{column}" for relation, column in sorted(unclassified)
        )
        raise ValueError(f"registry has unclassified source columns: {formatted}")
    games_cte, parameters = _sampled_games_cte(resolved_options)
    sampled_game_row = connection.execute(
        f"WITH {games_cte} SELECT COUNT(*) FROM sampled_games", parameters
    ).fetchone()
    if sampled_game_row is None:
        raise RuntimeError("sampled game count query returned no row")
    sampled_games = int(sampled_game_row[0])
    targets = tuple(
        field for field in registry.fields if field.scope is FieldScope.TARGET
    )
    logger.info(
        "Auditing %d fields across %d sampled PBP games",
        len(targets),
        sampled_games,
    )
    has_pitch_sequence_targets = any(
        field.source_relation == "main_models.stg_event_pitch_sequences"
        for field in targets
    )
    pitch_absence = (
        _pitch_absence_by_era(connection, resolved_options)
        if has_pitch_sequence_targets
        else {era.name: (0, 0, 0) for era in resolved_options.eras}
    )
    coverage = tuple(
        _coverage_for_field(connection, field, resolved_options, pitch_absence)
        for field in targets
    )
    excluded = tuple(
        field for field in registry.fields if field.scope is FieldScope.OUT_OF_SCOPE
    )
    return CoverageReport(
        season_start=resolved_options.season_start,
        season_end=resolved_options.season_end,
        sample_game_limit=resolved_options.sample_game_limit,
        sampled_games=sampled_games,
        fields=coverage,
        excluded_fields=excluded,
    )
