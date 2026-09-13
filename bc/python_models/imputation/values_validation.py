from __future__ import annotations

import logging

import duckdb

from python_models.imputation.artifacts import ComponentArtifact, sql_literal
from python_models.imputation.context import ContextCompletionConfig
from python_models.imputation.values import Config, PARK_METRICS


logger = logging.getLogger(__name__)
VALUE_COMPONENTS = frozenset(
    {
        "event_values",
        "park_factors",
        "run_expectancy",
        "state_transitions",
        "linear_weights",
    }
)


def _eligible_games_sql(config: Config) -> str:
    limit = f"LIMIT {config.sample_games}" if config.sample_games is not None else ""
    seed = sql_literal(config.sample_seed)
    return f"""
eligible_games AS (
    SELECT game.game_id, game.season
    FROM {config.games_relation} AS game
    WHERE game.source_type = 'PlayByPlay'
      AND game.season BETWEEN {config.start_season} AND {config.end_season}
      AND EXISTS (
          SELECT 1 FROM {config.states_relation} AS state
          WHERE state.game_id = game.game_id
      )
    ORDER BY hash(game.game_id, {seed}), game.game_id
    {limit}
)"""


def _expected_source(component: str, config: Config) -> tuple[tuple[str, ...], str]:
    eligible = _eligible_games_sql(config)
    contexts = f"""
contexts AS (
    SELECT DISTINCT state.season,
        coalesce(state.league::VARCHAR, state.league_group, 'Other') AS league
    FROM {config.states_relation} AS state
    JOIN eligible_games USING (game_id)
)"""
    if component == "event_values":
        return (
            ("event_key",),
            f"""
WITH {eligible}
SELECT state.event_key
FROM {config.states_relation} AS state
JOIN eligible_games USING (game_id)
""",
        )
    if component == "park_factors":
        metrics = ", ".join(sql_literal(metric) for metric in PARK_METRICS)
        return (
            ("season", "park_id", "league", "metric"),
            f"""
WITH {eligible},
park_contexts AS (
    SELECT DISTINCT state.season, state.park_id::VARCHAR AS park_id,
        coalesce(state.league::VARCHAR, state.league_group, 'Other') AS league
    FROM {config.states_relation} AS state
    JOIN eligible_games USING (game_id)
)
SELECT park_contexts.*, metric
FROM park_contexts
CROSS JOIN unnest([{metrics}]) AS metrics(metric)
""",
        )
    if component == "run_expectancy":
        return (
            ("season", "league", "base_state", "outs"),
            f"""
WITH {eligible}
SELECT DISTINCT state.season,
    coalesce(state.league::VARCHAR, state.league_group, 'Other') AS league,
    state.base_state_start::TINYINT AS base_state,
    state.outs_start::TINYINT AS outs
FROM {config.states_relation} AS state
JOIN eligible_games USING (game_id)
""",
        )
    if component == "state_transitions":
        return (
            ("season", "league", "start_state", "end_class"),
            f"""
WITH {eligible},
target_contexts AS (
    SELECT DISTINCT state.season,
        coalesce(state.league::VARCHAR, state.league_group, 'Other') AS league,
        concat(state.outs_start, '_', state.base_state_start) AS start_state
    FROM {config.states_relation} AS state
    JOIN eligible_games USING (game_id)
),
available_vectors AS (
    SELECT DISTINCT start_state, end_class
    FROM {config.transition_summary_relation}
    WHERE outcome = 'end_state' AND season BETWEEN 1910 AND 2025
)
SELECT target.season, target.league, target.start_state, vector.end_class
FROM target_contexts AS target
JOIN available_vectors AS vector USING (start_state)
""",
        )
    if component == "linear_weights":
        return (
            ("season", "league", "play"),
            f"""
WITH {eligible},
{contexts},
plays AS (
    SELECT DISTINCT play FROM {config.linear_weights_relation}
)
SELECT contexts.season, contexts.league, plays.play
FROM contexts CROSS JOIN plays
""",
        )
    raise ValueError(f"Unknown values component: {component}")


def _component_checks(component: str, parquet: str, config: Config) -> dict[str, str]:
    zero = "SELECT 0::BIGINT"
    checks = {
        "missing_values": zero,
        "invalid_ranges": zero,
        "invalid_probability_normalization": zero,
        "changed_recorded_values": zero,
    }
    if component == "event_values":
        checks.update(
            missing_values=f"""SELECT count(*) FROM {parquet}
                WHERE run_expectancy_start IS NULL OR run_expectancy_end IS NULL
                   OR home_win_expectancy_start IS NULL OR home_win_expectancy_end IS NULL
                   OR expected_runs_change IS NULL OR expected_home_win_change IS NULL
                   OR expected_batting_win_change IS NULL
                   OR expected_runs_change_method IS NULL OR expected_win_change_method IS NULL""",
            invalid_ranges=f"""SELECT count(*) FROM {parquet}
                WHERE NOT isfinite(run_expectancy_start) OR run_expectancy_start < 0
                   OR NOT isfinite(run_expectancy_end) OR run_expectancy_end < 0
                   OR home_win_expectancy_start NOT BETWEEN 0 AND 1
                   OR home_win_expectancy_end NOT BETWEEN 0 AND 1
                   OR NOT isfinite(expected_runs_change)
                   OR NOT isfinite(expected_home_win_change)
                   OR NOT isfinite(expected_batting_win_change)""",
            changed_recorded_values=f"""SELECT count(*) FROM {parquet}
                WHERE (expected_runs_change_raw IS NOT NULL
                    AND expected_runs_change IS DISTINCT FROM expected_runs_change_raw)
                   OR (expected_home_win_change_raw IS NOT NULL
                    AND expected_home_win_change IS DISTINCT FROM expected_home_win_change_raw)
                   OR (expected_batting_win_change_raw IS NOT NULL
                    AND expected_batting_win_change IS DISTINCT FROM expected_batting_win_change_raw)""",
            invalid_run_value_conservation=f"""SELECT count(*)
                FROM {parquet} AS completed
                JOIN {config.states_relation} AS state USING (event_key)
                WHERE completed.expected_runs_change_raw IS NULL
                  AND abs(completed.expected_runs_change
                    - (state.runs_on_play + completed.run_expectancy_end
                        - completed.run_expectancy_start)) > 1e-10""",
        )
    elif component == "park_factors":
        checks.update(
            missing_values=f"SELECT count(*) FROM {parquet} WHERE completed_value IS NULL OR method IS NULL",
            invalid_ranges=f"SELECT count(*) FROM {parquet} WHERE NOT isfinite(completed_value) OR completed_value < 0 OR donor_count < 0",
            changed_recorded_values=f"SELECT count(*) FROM {parquet} WHERE raw_value IS NOT NULL AND isfinite(raw_value) AND completed_value IS DISTINCT FROM raw_value",
        )
    elif component == "run_expectancy":
        checks.update(
            missing_values=f"SELECT count(*) FROM {parquet} WHERE completed_value IS NULL OR method IS NULL",
            invalid_ranges=f"SELECT count(*) FROM {parquet} WHERE NOT isfinite(completed_value) OR completed_value < 0 OR (completed_sd IS NOT NULL AND (NOT isfinite(completed_sd) OR completed_sd < 0)) OR sample_size < 0",
            changed_recorded_values=f"SELECT count(*) FROM {parquet} WHERE raw_value IS NOT NULL AND completed_value IS DISTINCT FROM raw_value",
        )
    elif component == "state_transitions":
        checks.update(
            missing_values=f"SELECT count(*) FROM {parquet} WHERE completed_probability IS NULL OR method IS NULL OR donor_season IS NULL OR donor_league IS NULL",
            invalid_ranges=f"SELECT count(*) FROM {parquet} WHERE NOT isfinite(completed_probability) OR completed_probability NOT BETWEEN 0 AND 1 OR (completed_sd IS NOT NULL AND (NOT isfinite(completed_sd) OR completed_sd < 0))",
            invalid_probability_normalization=f"""SELECT count(*) FROM (
                SELECT season, league, start_state
                FROM {parquet}
                GROUP BY ALL
                HAVING abs(sum(completed_probability) - 1) > 1e-8
            )""",
            changed_recorded_values=f"SELECT count(*) FROM {parquet} WHERE raw_probability IS NOT NULL AND completed_probability IS DISTINCT FROM raw_probability",
        )
    elif component == "linear_weights":
        checks.update(
            missing_values=f"SELECT count(*) FROM {parquet} WHERE completed_run_value IS NULL OR method IS NULL OR source_is_imputed IS NULL",
            invalid_ranges=f"SELECT count(*) FROM {parquet} WHERE NOT isfinite(completed_run_value) OR (completed_sd IS NOT NULL AND (NOT isfinite(completed_sd) OR completed_sd < 0))",
            changed_recorded_values=f"SELECT count(*) FROM {parquet} WHERE raw_estimated_value IS NOT NULL AND completed_run_value IS DISTINCT FROM raw_estimated_value",
        )
    return checks


def validate_values_component(
    connection: duckdb.DuckDBPyConnection,
    artifact: ComponentArtifact,
    config: ContextCompletionConfig,
) -> dict[str, int]:
    if artifact.name not in VALUE_COMPONENTS:
        raise ValueError(f"Unknown values component: {artifact.name}")
    values_config = Config(
        start_season=config.start_season,
        end_season=config.end_season,
        sample_games=config.sample_games,
    )
    grain, source = _expected_source(artifact.name, values_config)
    parquet = f"read_parquet({sql_literal(artifact.data.path)})"
    grain_sql = ", ".join(grain)
    checks = {
        "missing_source_keys": f"SELECT count(*) FROM (({source}) EXCEPT (SELECT {grain_sql} FROM {parquet}))",
        "unexpected_keys": f"SELECT count(*) FROM ((SELECT {grain_sql} FROM {parquet}) EXCEPT ({source}))",
        "duplicate_keys": f"SELECT coalesce(sum(n - 1), 0)::BIGINT FROM (SELECT count(*) AS n FROM {parquet} GROUP BY {grain_sql} HAVING count(*) > 1)",
        **_component_checks(artifact.name, parquet, values_config),
    }
    results: dict[str, int] = {}
    for name, query in checks.items():
        row = connection.execute(query).fetchone()
        if row is None:
            raise ValueError(f"Validation returned no result: {artifact.name}.{name}")
        results[name] = int(row[0])
    logger.info("Validated %s: %s", artifact.name, results)
    failures = {name: count for name, count in results.items() if count}
    if failures:
        raise ValueError(
            f"Completion validation failed for {artifact.name}: {failures}"
        )
    return results
