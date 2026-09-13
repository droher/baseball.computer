from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

MODEL_NAME = "pbp_imputed_game_context"
MODEL_VERSION = "1"
INPUT_RELATIONS = ("main_models.game_start_info", "main_models.game_results")
CONTEXT_FIELDS = {
    "time_of_day": "VARCHAR",
    "sky": "VARCHAR",
    "field_condition": "VARCHAR",
    "precipitation": "VARCHAR",
    "wind_direction": "VARCHAR",
    "temperature_fahrenheit": "DOUBLE",
    "attendance": "DOUBLE",
    "wind_speed_mph": "DOUBLE",
    "start_time": "TIMESTAMP",
    "duration_minutes": "DOUBLE",
}
OUTPUT_SCHEMA = {
    "game_id": "VARCHAR",
    "season": "SMALLINT",
    **{
        name: sql_type
        for field, value_type in CONTEXT_FIELDS.items()
        for name, sql_type in (
            (field, value_type),
            (f"{field}_raw", value_type),
            (f"{field}_method", "VARCHAR"),
            (f"{field}_donor_count", "BIGINT"),
            (f"{field}_donor_sd", "DOUBLE"),
        )
    },
    "model_version": "VARCHAR",
    "confidence_status": "VARCHAR",
}


class ContextCompletionConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    start_season: int = Field(default=1903, ge=1800, le=3000)
    end_season: int = Field(default=2025, ge=1800, le=3000)
    sample_games: int | None = Field(default=None, ge=1)
    sample_seed: str = "pbp-context-completion-v1"

    @model_validator(mode="after")
    def validate_seasons(self) -> ContextCompletionConfig:
        if self.end_season < self.start_season:
            raise ValueError("end_season precedes start_season")
        return self


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_context_completion_sql(config: ContextCompletionConfig) -> str:
    ctes = [
        """source AS (
            SELECT g.game_id, g.season, date, park_id,
                time_of_day, sky, field_condition, precipitation, wind_direction,
                temperature_fahrenheit, attendance, wind_speed_mph, start_time, duration_minutes,
                floor(g.season / 10)::INTEGER AS decade,
                month(date)::INTEGER AS calendar_month,
                coalesce(nullif(time_of_day::VARCHAR, 'Unknown'), 'Day') AS day_group,
                coalesce(away_league::VARCHAR, home_league::VARCHAR, 'Unknown') AS league_group
            FROM main_models.game_start_info g
            LEFT JOIN main_models.game_results r USING (game_id)
            WHERE source_type = 'PlayByPlay'
        )"""
    ]
    limit = f"LIMIT {config.sample_games}" if config.sample_games else ""
    ctes.append(
        f"""targets AS (
            SELECT * FROM source
            WHERE season BETWEEN {config.start_season} AND {config.end_season}
            ORDER BY hash(game_id, {_literal(config.sample_seed)}), game_id {limit}
        )"""
    )
    joins: list[str] = []
    outputs = ["t.game_id", "t.season"]
    keys = (
        ("park_month_decade", ("park_id", "calendar_month", "decade", "day_group")),
        ("park_month", ("park_id", "calendar_month", "day_group")),
        (
            "league_month_decade",
            ("league_group", "calendar_month", "decade", "day_group"),
        ),
        ("month_decade", ("calendar_month", "decade", "day_group")),
        ("month", ("calendar_month", "day_group")),
        ("historical", ("day_group",)),
        ("global", ()),
    )
    for field, sql_type in CONTEXT_FIELDS.items():
        if field == "start_time":
            value = (
                "date_diff('second', date_trunc('day', start_time), start_time)::DOUBLE"
            )
            present = "start_time IS NOT NULL"
        elif sql_type == "VARCHAR":
            value = f"{field}::VARCHAR"
            present = f"{field} IS NOT NULL AND {field}::VARCHAR NOT IN ('Unknown', '')"
        else:
            value = f"{field}::DOUBLE"
            present = f"{field} IS NOT NULL AND isfinite({field}::DOUBLE)"
        sd = f"stddev_pop({value})" if sql_type != "VARCHAR" else "NULL::DOUBLE"
        estimates: list[str] = []
        methods: list[str] = []
        counts: list[str] = []
        deviations: list[str] = []
        for index, (label, columns) in enumerate(keys):
            alias = f"{field}_{index}"
            group = ", ".join(columns)
            group_select = f"{group}, " if group else ""
            group_by = f"GROUP BY {group}" if group else ""
            ctes.append(
                f"""{alias} AS (
                    SELECT {group_select}count(*) AS donor_count, {sd} AS donor_sd
                    FROM source WHERE {present} {group_by}
                )"""
            )
            partition = f"PARTITION BY {group}" if group else ""
            donor_alias = f"{alias}_values"
            ctes.append(
                f"""{donor_alias} AS (
                    SELECT {group_select}{value} AS donor_value,
                        row_number() OVER ({partition} ORDER BY game_id) AS donor_index
                    FROM source WHERE {present}
                )"""
            )
            on = (
                " AND ".join(
                    f"t.{col} IS NOT DISTINCT FROM {alias}.{col}" for col in columns
                )
                or "TRUE"
            )
            observed = present.replace(field, f"t.{field}")
            joins.append(f"LEFT JOIN {alias} ON {on} AND NOT ({observed})")
            index_sql = f"1 + (hash(t.game_id, {_literal(field)}, {_literal(config.sample_seed)}) % nullif({alias}.donor_count, 0))::BIGINT"
            donor_on = " AND ".join(
                f"t.{col} IS NOT DISTINCT FROM {donor_alias}.{col}" for col in columns
            )
            if donor_on:
                donor_on += " AND "
            joins.append(
                f"LEFT JOIN {donor_alias} ON {donor_on}{donor_alias}.donor_index = {index_sql}"
            )
            estimates.append(f"{donor_alias}.donor_value")
            methods.append(f"WHEN {alias}.donor_count > 0 THEN 'empirical_{label}'")
            counts.append(f"nullif({alias}.donor_count, 0)")
            deviations.append(f"WHEN {alias}.donor_count > 0 THEN {alias}.donor_sd")
        estimate = "coalesce(" + ", ".join(estimates) + ")"
        if field == "start_time":
            estimate = f"t.date::TIMESTAMP + ({estimate}) * INTERVAL '1 second'"
        raw = f"t.{field}::{sql_type}"
        observed = present.replace(field, f"t.{field}")
        outputs.extend(
            (
                f"CASE WHEN {observed} THEN {raw} ELSE {estimate} END AS {field}",
                f"{raw} AS {field}_raw",
                f"CASE WHEN {observed} THEN 'observed' {' '.join(methods)} ELSE 'unsupported_no_donors' END AS {field}_method",
                f"CASE WHEN {observed} THEN 0 ELSE coalesce({', '.join(counts)}, 0) END::BIGINT AS {field}_donor_count",
                f"CASE WHEN {observed} THEN 0.0 {' '.join(deviations)} END AS {field}_donor_sd",
            )
        )
    outputs.extend(
        (f"'{MODEL_VERSION}' AS model_version", "'exploratory' AS confidence_status")
    )
    return (
        "WITH "
        + ",\n".join(ctes)
        + "\nSELECT "
        + ",\n".join(outputs)
        + "\nFROM targets t\n"
        + "\n".join(joins)
    )
