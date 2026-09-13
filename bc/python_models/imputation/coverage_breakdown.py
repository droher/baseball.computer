from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Self

import duckdb
from duckdb import DuckDBPyConnection
from pydantic import BaseModel, ConfigDict, Field, model_validator

from python_models.imputation.coverage import CoverageOptions, CoverageReport
from python_models.imputation.registry import (
    FieldDefinition,
    FieldRegistry,
    FieldScope,
    default_field_registry,
)

logger = logging.getLogger(__name__)

EXPECTED_FULL_GAMES = 205_886
EXPECTED_FULL_EVENTS = 18_141_020
GROUPING_DIMENSIONS = ("era", "league", "game_type", "source_type")
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


class BreakdownCounts(BaseModel):
    model_config = ConfigDict(frozen=True)

    grouping_dimension: str
    grouping_value: str
    rows: int = Field(ge=0)
    applicable: int = Field(ge=0)
    observed: int = Field(ge=0)
    missing: int = Field(ge=0)
    sentinel: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_accounting(self) -> Self:
        if self.grouping_dimension not in GROUPING_DIMENSIONS:
            raise ValueError("unknown grouping dimension")
        if self.observed + self.missing + self.sentinel != self.applicable:
            raise ValueError(
                "observed, missing, and sentinel must partition applicable"
            )
        if self.applicable > self.rows:
            raise ValueError("applicable rows cannot exceed rows")
        return self


class FieldBreakdown(BaseModel):
    model_config = ConfigDict(frozen=True)

    field_name: str
    field_identifier: str
    source_relation: str
    source_family: str
    groups: tuple[BreakdownCounts, ...]


class FieldReconciliation(BaseModel):
    model_config = ConfigDict(frozen=True)

    field_identifier: str
    rows: int
    applicable: int
    observed: int
    missing: int
    sentinel: int
    baseline_match: bool | None
    grouping_dimensions_match: bool


class CoverageBreakdownReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    season_start: int
    season_end: int
    sample_game_limit: int | None
    sampled_games: int
    sampled_events: int
    league_definition: str
    source_relation_queries: int
    fields: tuple[FieldBreakdown, ...]
    reconciliation: tuple[FieldReconciliation, ...]
    full_population_match: bool | None

    @model_validator(mode="after")
    def validate_reconciliation(self) -> Self:
        if any(not item.grouping_dimensions_match for item in self.reconciliation):
            raise ValueError("grouped field counts do not reconcile across dimensions")
        if any(item.baseline_match is False for item in self.reconciliation):
            raise ValueError("grouped field counts do not match baseline coverage")
        return self


def _quote_identifier(identifier: str) -> str:
    if not _IDENTIFIER.fullmatch(identifier):
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
    if field.source_relation == "main_models.stg_events":
        rendered = rendered.replace("event.", "src.")
    return rendered


def _era_case(options: CoverageOptions) -> str:
    clauses = " ".join(
        f"WHEN game.season BETWEEN {era.season_start} AND {era.season_end} "
        f"THEN '{era.name}'"
        for era in options.eras
    )
    return f"CASE {clauses} END"


def _sampled_games_cte(options: CoverageOptions) -> str:
    limit = (
        ""
        if options.sample_game_limit is None
        else f"LIMIT {options.sample_game_limit}"
    )
    return f"""
sampled_games AS (
    SELECT
        game.*,
        CASE
            WHEN game.home_league IS NULL AND game.away_league IS NULL THEN 'Unknown'
            WHEN game.home_league IS NULL OR game.away_league IS NULL
              THEN COALESCE(game.home_league, game.away_league)
            WHEN game.home_league = game.away_league THEN game.home_league
            ELSE 'CrossLeague'
        END AS coverage_league
    FROM main_models.game_start_info AS game
    WHERE game.season BETWEEN {options.season_start} AND {options.season_end}
      AND game.source_type = 'PlayByPlay'
      AND EXISTS (
          SELECT 1 FROM main_models.stg_events AS event
          WHERE event.game_id = game.game_id
      )
    ORDER BY game.season, game.game_id
    {limit}
)
"""


def _relation_query(
    relation: str,
    fields: Sequence[FieldDefinition],
    options: CoverageOptions,
) -> str:
    classifications: list[str] = []
    aggregates: list[str] = []
    needs_event = False
    for index, field in enumerate(fields):
        applicable = _render_expression(field.applicability_sql, field)
        missing = _render_expression(field.missing_sql, field)
        sentinel = _render_expression(field.sentinel_sql, field)
        needs_event = needs_event or any(
            "event." in expression
            for expression in (
                field.applicability_sql,
                field.missing_sql,
                field.sentinel_sql,
            )
        )
        classifications.extend(
            (
                f"({applicable}) AS f{index}_applicable",
                f"({missing}) AS f{index}_missing",
                f"({sentinel}) AS f{index}_sentinel",
            )
        )
        aggregates.extend(
            (
                f"COUNT(*)::BIGINT AS f{index}_rows",
                f"COUNT(*) FILTER (WHERE f{index}_applicable)::BIGINT AS f{index}_applicable",
                f"COUNT(*) FILTER (WHERE f{index}_applicable AND NOT f{index}_missing "
                f"AND NOT f{index}_sentinel)::BIGINT AS f{index}_observed",
                f"COUNT(*) FILTER (WHERE f{index}_applicable AND f{index}_missing)::BIGINT "
                f"AS f{index}_missing",
                f"COUNT(*) FILTER (WHERE f{index}_applicable AND NOT f{index}_missing "
                f"AND f{index}_sentinel)::BIGINT AS f{index}_sentinel",
            )
        )
    event_join = ""
    if needs_event and relation != "main_models.stg_events":
        if any("event_key" not in field.grain_keys for field in fields):
            raise ValueError(f"{relation} references event outside event grain")
        event_join = (
            "LEFT JOIN main_models.stg_events AS event "
            "ON event.event_key = src.event_key"
        )
    return f"""
WITH {_sampled_games_cte(options)}, classified AS (
    SELECT
        {_era_case(options)} AS era,
        COALESCE(game.coverage_league, 'Unknown') AS league,
        COALESCE(CAST(game.game_type AS VARCHAR), 'Unknown') AS game_type,
        COALESCE(game.source_type, 'Unknown') AS source_type,
        {", ".join(classifications)}
    FROM {_quote_relation(relation)} AS src
    INNER JOIN sampled_games AS game ON game.game_id = src.game_id
    {event_join}
)
SELECT
    CASE
        WHEN GROUPING(era) = 0 THEN 'era'
        WHEN GROUPING(league) = 0 THEN 'league'
        WHEN GROUPING(game_type) = 0 THEN 'game_type'
        ELSE 'source_type'
    END AS grouping_dimension,
    COALESCE(era, league, game_type, source_type, 'Unknown') AS grouping_value,
    {", ".join(aggregates)}
FROM classified
GROUP BY GROUPING SETS ((era), (league), (game_type), (source_type))
""".strip()


def _counts_from_row(row: Sequence[object], field_index: int) -> BreakdownCounts:
    offset = 2 + field_index * 5

    def integer(value: object) -> int:
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError("coverage count must be an integer")
        return value

    return BreakdownCounts(
        grouping_dimension=str(row[0]),
        grouping_value=str(row[1]),
        rows=integer(row[offset]),
        applicable=integer(row[offset + 1]),
        observed=integer(row[offset + 2]),
        missing=integer(row[offset + 3]),
        sentinel=integer(row[offset + 4]),
    )


def _totals(
    groups: Sequence[BreakdownCounts], dimension: str
) -> tuple[int, int, int, int, int]:
    selected = [group for group in groups if group.grouping_dimension == dimension]
    return (
        sum(group.rows for group in selected),
        sum(group.applicable for group in selected),
        sum(group.observed for group in selected),
        sum(group.missing for group in selected),
        sum(group.sentinel for group in selected),
    )


def _baseline_totals(report: CoverageReport | None) -> Mapping[str, tuple[int, ...]]:
    if report is None:
        return {}
    return {
        coverage.field.identifier: (
            sum(era.rows for era in coverage.eras),
            sum(era.applicable for era in coverage.eras),
            sum(era.observed for era in coverage.eras),
            sum(era.missing for era in coverage.eras),
            sum(era.sentinel for era in coverage.eras),
        )
        for coverage in report.fields
    }


def build_coverage_breakdown(
    connection: DuckDBPyConnection,
    registry: FieldRegistry,
    options: CoverageOptions | None = None,
    baseline: CoverageReport | None = None,
) -> CoverageBreakdownReport:
    resolved = options or CoverageOptions()
    unclassified = registry.unclassified_columns(connection)
    if unclassified:
        formatted = ", ".join(
            f"{relation}.{column}" for relation, column in sorted(unclassified)
        )
        raise ValueError(f"registry has unclassified source columns: {formatted}")
    fields = tuple(
        field for field in registry.fields if field.scope is FieldScope.TARGET
    )
    by_relation: dict[str, list[FieldDefinition]] = defaultdict(list)
    for field in fields:
        by_relation[field.source_relation].append(field)
    groups_by_field: dict[str, list[BreakdownCounts]] = defaultdict(list)
    for relation, relation_fields in by_relation.items():
        logger.info(
            "Coverage breakdown for %d fields from %s",
            len(relation_fields),
            relation,
        )
        rows = connection.execute(
            _relation_query(relation, relation_fields, resolved)
        ).fetchall()
        for row in rows:
            for index, field in enumerate(relation_fields):
                groups_by_field[field.identifier].append(_counts_from_row(row, index))
    games_cte = _sampled_games_cte(resolved)
    population = connection.execute(
        f"""
        WITH {games_cte}
        SELECT
            COUNT(DISTINCT game.game_id)::BIGINT,
            COUNT(event.event_key)::BIGINT
        FROM sampled_games AS game
        LEFT JOIN main_models.stg_events AS event USING (game_id)
        """
    ).fetchone()
    if population is None:
        raise RuntimeError("population query returned no row")
    sampled_games = int(population[0])
    sampled_events = int(population[1])
    baseline_by_field = _baseline_totals(baseline)
    breakdowns: list[FieldBreakdown] = []
    reconciliations: list[FieldReconciliation] = []
    for field in fields:
        groups = tuple(
            sorted(
                groups_by_field[field.identifier],
                key=lambda group: (
                    GROUPING_DIMENSIONS.index(group.grouping_dimension),
                    group.grouping_value,
                ),
            )
        )
        totals_by_dimension = {
            dimension: _totals(groups, dimension) for dimension in GROUPING_DIMENSIONS
        }
        era_totals = totals_by_dimension["era"]
        baseline_totals = baseline_by_field.get(field.identifier)
        breakdowns.append(
            FieldBreakdown(
                field_name=field.name,
                field_identifier=field.identifier,
                source_relation=field.source_relation,
                source_family=str(field.family),
                groups=groups,
            )
        )
        reconciliations.append(
            FieldReconciliation(
                field_identifier=field.identifier,
                rows=era_totals[0],
                applicable=era_totals[1],
                observed=era_totals[2],
                missing=era_totals[3],
                sentinel=era_totals[4],
                baseline_match=(
                    None if baseline_totals is None else era_totals == baseline_totals
                ),
                grouping_dimensions_match=all(
                    total == era_totals for total in totals_by_dimension.values()
                ),
            )
        )
    full_scope = (
        resolved.sample_game_limit is None
        and resolved.season_start == 1903
        and resolved.season_end == 2025
    )
    return CoverageBreakdownReport(
        season_start=resolved.season_start,
        season_end=resolved.season_end,
        sample_game_limit=resolved.sample_game_limit,
        sampled_games=sampled_games,
        sampled_events=sampled_events,
        league_definition=(
            "Shared home and away league when equal; sole recorded league when one is "
            "missing; CrossLeague when both differ; Unknown when both are missing."
        ),
        source_relation_queries=len(by_relation),
        fields=tuple(breakdowns),
        reconciliation=tuple(reconciliations),
        full_population_match=(
            sampled_games == EXPECTED_FULL_GAMES
            and sampled_events == EXPECTED_FULL_EVENTS
            if full_scope
            else None
        ),
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def write_breakdown_artifact(
    output: Path,
    report: CoverageBreakdownReport,
    registry: FieldRegistry,
    database: Path,
    database_sha256: str | None = None,
) -> None:
    if (
        database_sha256 is not None
        and re.fullmatch(r"[0-9a-f]{64}", database_sha256) is None
    ):
        raise ValueError("database_sha256 must be a lowercase SHA-256 digest")
    output.mkdir(parents=True, exist_ok=False)
    report_path = output / "coverage_breakdown.json"
    registry_path = output / "field_registry.json"
    report_path.write_text(report.model_dump_json(indent=2) + "\n")
    registry_path.write_text(registry.model_dump_json(indent=2) + "\n")
    source_stat = database.stat()
    manifest = {
        "created_at": datetime.now(UTC).isoformat(),
        "status": "complete",
        "scope": "existing_pbp_games_only",
        "database": str(database.resolve()),
        "database_bytes": source_stat.st_size,
        "database_mtime_ns": source_stat.st_mtime_ns,
        "database_sha256": database_sha256 or _sha256(database),
        "report": report_path.name,
        "report_sha256": _sha256(report_path),
        "registry": registry_path.name,
        "registry_sha256": _sha256(registry_path),
        "implementation_sha256": _sha256(Path(__file__)),
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report registry field missingness within existing PBP games."
    )
    parser.add_argument("--database", type=Path, default=Path("bc.db"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--start-season", type=int, default=1903)
    parser.add_argument("--end-season", type=int, default=2025)
    parser.add_argument("--sample-games", type=int)
    parser.add_argument("--baseline-coverage", type=Path)
    parser.add_argument("--database-sha256")
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--memory-limit", default="4GB")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    database = args.database.resolve()
    baseline = (
        CoverageReport.model_validate_json(args.baseline_coverage.read_text())
        if args.baseline_coverage is not None
        else None
    )
    registry = default_field_registry()
    with duckdb.connect(
        str(database),
        read_only=True,
        config={"threads": args.threads, "memory_limit": args.memory_limit},
    ) as connection:
        report = build_coverage_breakdown(
            connection,
            registry,
            CoverageOptions(
                season_start=args.start_season,
                season_end=args.end_season,
                sample_game_limit=args.sample_games,
            ),
            baseline,
        )
    write_breakdown_artifact(
        args.output.resolve(), report, registry, database, args.database_sha256
    )
    logger.info(
        "Coverage breakdown complete: %d games, %d events, %d fields",
        report.sampled_games,
        report.sampled_events,
        len(report.fields),
    )


if __name__ == "__main__":
    main()
