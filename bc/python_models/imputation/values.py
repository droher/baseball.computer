from __future__ import annotations

import logging
import re
from typing import ClassVar, Final, Mapping

from pydantic import BaseModel, ConfigDict, Field, model_validator


logger = logging.getLogger(__name__)
MODEL_VERSION: Final = "1.0.0"
INPUT_RELATIONS: Final = (
    "main_models.game_start_info",
    "main_models.event_states_full",
    "main_models.event_transition_values",
    "main_models.run_expectancy_matrix",
    "main_models.win_expectancy_matrix",
    "main_models.run_expectancy_summary",
    "main_models.state_transition_summary",
    "main_models.park_factors",
    "main_models.linear_weights",
    "main_models.linear_weights_estimated",
    "main_models.stg_events",
    "main_models.stg_event_baserunners",
    "main_seeds.seed_plate_appearance_result_types",
    "main_seeds.seed_baserunning_play_types",
)

EVENT_VALUES_OUTPUT_SCHEMA: Final[Mapping[str, str]] = {
    "event_key": "UINTEGER",
    "game_id": "VARCHAR",
    "season": "SMALLINT",
    "league": "VARCHAR",
    "park_id": "VARCHAR",
    "run_expectancy_start": "DOUBLE",
    "run_expectancy_end": "DOUBLE",
    "home_win_expectancy_start": "DOUBLE",
    "home_win_expectancy_end": "DOUBLE",
    "expected_runs_change_raw": "DOUBLE",
    "expected_runs_change": "DOUBLE",
    "expected_runs_change_method": "VARCHAR",
    "expected_home_win_change_raw": "DOUBLE",
    "expected_home_win_change": "DOUBLE",
    "expected_batting_win_change_raw": "DOUBLE",
    "expected_batting_win_change": "DOUBLE",
    "expected_win_change_method": "VARCHAR",
    "model_version": "VARCHAR",
    "confidence_status": "VARCHAR",
}

PARK_FACTORS_OUTPUT_SCHEMA: Final[Mapping[str, str]] = {
    "season": "SMALLINT",
    "park_id": "VARCHAR",
    "league": "VARCHAR",
    "metric": "VARCHAR",
    "raw_value": "DOUBLE",
    "completed_value": "DOUBLE",
    "method": "VARCHAR",
    "donor_count": "BIGINT",
    "model_version": "VARCHAR",
    "confidence_status": "VARCHAR",
}

RUN_EXPECTANCY_OUTPUT_SCHEMA: Final[Mapping[str, str]] = {
    "season": "SMALLINT",
    "league": "VARCHAR",
    "base_state": "TINYINT",
    "outs": "TINYINT",
    "raw_value": "DOUBLE",
    "completed_value": "DOUBLE",
    "completed_sd": "DOUBLE",
    "method": "VARCHAR",
    "donor_season": "SMALLINT",
    "sample_size": "BIGINT",
    "model_version": "VARCHAR",
    "confidence_status": "VARCHAR",
}

STATE_TRANSITIONS_OUTPUT_SCHEMA: Final[Mapping[str, str]] = {
    "season": "SMALLINT",
    "league": "VARCHAR",
    "start_state": "VARCHAR",
    "end_class": "VARCHAR",
    "raw_probability": "DOUBLE",
    "completed_probability": "DOUBLE",
    "completed_sd": "DOUBLE",
    "method": "VARCHAR",
    "donor_season": "SMALLINT",
    "donor_league": "VARCHAR",
    "model_version": "VARCHAR",
    "confidence_status": "VARCHAR",
}

LINEAR_WEIGHTS_OUTPUT_SCHEMA: Final[Mapping[str, str]] = {
    "season": "SMALLINT",
    "league": "VARCHAR",
    "play": "VARCHAR",
    "play_category": "VARCHAR",
    "raw_estimated_value": "DOUBLE",
    "completed_run_value": "DOUBLE",
    "completed_sd": "DOUBLE",
    "method": "VARCHAR",
    "source_is_imputed": "BOOLEAN",
    "model_version": "VARCHAR",
    "confidence_status": "VARCHAR",
}

OUTPUT_SCHEMAS: Final[Mapping[str, Mapping[str, str]]] = {
    "event_values": EVENT_VALUES_OUTPUT_SCHEMA,
    "park_factors": PARK_FACTORS_OUTPUT_SCHEMA,
    "run_expectancy": RUN_EXPECTANCY_OUTPUT_SCHEMA,
    "state_transitions": STATE_TRANSITIONS_OUTPUT_SCHEMA,
    "linear_weights": LINEAR_WEIGHTS_OUTPUT_SCHEMA,
}

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?$")


class Config(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    start_season: int = Field(default=1903, ge=1800, le=3000)
    end_season: int = Field(default=2025, ge=1800, le=3000)
    sample_games: int | None = Field(default=None, ge=1)
    sample_seed: str = "pbp-derived-values-v1"
    games_relation: str = "main_models.game_start_info"
    states_relation: str = "main_models.event_states_full"
    transition_values_relation: str = "main_models.event_transition_values"
    run_matrix_relation: str = "main_models.run_expectancy_matrix"
    win_matrix_relation: str = "main_models.win_expectancy_matrix"
    run_summary_relation: str = "main_models.run_expectancy_summary"
    transition_summary_relation: str = "main_models.state_transition_summary"
    park_factors_relation: str = "main_models.park_factors"
    linear_weights_relation: str = "main_models.linear_weights"
    linear_weights_estimated_relation: str = "main_models.linear_weights_estimated"
    events_relation: str = "main_models.stg_events"
    runners_relation: str = "main_models.stg_event_baserunners"
    plate_result_types_relation: str = "main_seeds.seed_plate_appearance_result_types"
    baserunning_types_relation: str = "main_seeds.seed_baserunning_play_types"

    @model_validator(mode="after")
    def validate_config(self) -> Config:
        if self.start_season > self.end_season:
            raise ValueError("start_season must not exceed end_season")
        if not self.sample_seed:
            raise ValueError("sample_seed must not be empty")
        for relation in self.relations:
            if _IDENTIFIER.fullmatch(relation) is None:
                raise ValueError(f"invalid relation name: {relation}")
        return self

    @property
    def relations(self) -> tuple[str, ...]:
        return (
            self.games_relation,
            self.states_relation,
            self.transition_values_relation,
            self.run_matrix_relation,
            self.win_matrix_relation,
            self.run_summary_relation,
            self.transition_summary_relation,
            self.park_factors_relation,
            self.linear_weights_relation,
            self.linear_weights_estimated_relation,
            self.events_relation,
            self.runners_relation,
            self.plate_result_types_relation,
            self.baserunning_types_relation,
        )


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _eligible_games_sql(config: Config) -> str:
    limit = f"LIMIT {config.sample_games}" if config.sample_games is not None else ""
    return f"""
eligible_games AS (
    SELECT game_id, season
    FROM {config.games_relation}
    WHERE source_type = 'PlayByPlay'
      AND season BETWEEN {config.start_season} AND {config.end_season}
      AND EXISTS (
          SELECT 1 FROM {config.states_relation} AS state
          WHERE state.game_id = {config.games_relation}.game_id
      )
    ORDER BY hash(game_id, {_literal(config.sample_seed)}), game_id
    {limit}
)"""


def build_event_values_sql(config: Config = Config()) -> str:
    logger.info(
        "Building completed event values SQL for seasons %d-%d",
        config.start_season,
        config.end_season,
    )
    return f"""
WITH {_eligible_games_sql(config)},
run_pool AS (
    SELECT outs, base_state,
        sum(avg_runs_scored * sample_size) / nullif(sum(sample_size), 0) AS value
    FROM {config.run_matrix_relation}
    GROUP BY outs, base_state
),
base AS (
    SELECT
        state.*,
        current.expected_runs_change::DOUBLE AS expected_runs_change_raw,
        current.expected_home_win_change::DOUBLE AS expected_home_win_change_raw,
        current.expected_batting_win_change::DOUBLE AS expected_batting_win_change_raw,
        coalesce(run_start.avg_runs_scored::DOUBLE, pool_start.value, 0.0) AS re_start,
        CASE WHEN state.outs_end >= 3 OR state.game_end_flag THEN 0.0
            ELSE coalesce(run_end.avg_runs_scored::DOUBLE, pool_end.value, 0.0)
        END AS re_end,
        coalesce(win_start.home_win_rate::DOUBLE, 0.5) AS we_start,
        CASE
            WHEN state.game_end_flag AND state.truncated_home_margin_end > 0 THEN 1.0
            WHEN state.game_end_flag AND state.truncated_home_margin_end < 0 THEN 0.0
            ELSE coalesce(win_end.home_win_rate::DOUBLE, 0.5)
        END AS we_end,
        run_start.avg_runs_scored IS NOT NULL
            AND (state.outs_end >= 3 OR state.game_end_flag
                OR run_end.avg_runs_scored IS NOT NULL) AS exact_run_cell,
        win_start.home_win_rate IS NOT NULL
            AND ((state.game_end_flag AND state.truncated_home_margin_end <> 0)
                OR win_end.home_win_rate IS NOT NULL) AS exact_win_cell
    FROM {config.states_relation} AS state
    JOIN eligible_games USING (game_id)
    LEFT JOIN {config.transition_values_relation} AS current USING (event_key)
    LEFT JOIN {config.run_matrix_relation} AS run_start
      ON run_start.run_expectancy_key = state.run_expectancy_start_key
    LEFT JOIN {config.run_matrix_relation} AS run_end
      ON run_end.run_expectancy_key = state.run_expectancy_end_key
    LEFT JOIN run_pool AS pool_start
      ON pool_start.outs = state.outs_start AND pool_start.base_state = state.base_state_start
    LEFT JOIN run_pool AS pool_end
      ON pool_end.outs = state.outs_end % 3
     AND pool_end.base_state = coalesce(state.base_state_end, 0)
    LEFT JOIN {config.win_matrix_relation} AS win_start
      ON win_start.win_expectancy_key = state.win_expectancy_start_key
    LEFT JOIN {config.win_matrix_relation} AS win_end
      ON win_end.win_expectancy_key = state.win_expectancy_end_key
)
SELECT
    event_key,
    game_id,
    season,
    league::VARCHAR AS league,
    park_id::VARCHAR AS park_id,
    re_start::DOUBLE AS run_expectancy_start,
    re_end::DOUBLE AS run_expectancy_end,
    we_start::DOUBLE AS home_win_expectancy_start,
    we_end::DOUBLE AS home_win_expectancy_end,
    expected_runs_change_raw,
    coalesce(expected_runs_change_raw, runs_on_play + re_end - re_start)::DOUBLE
        AS expected_runs_change,
    CASE WHEN expected_runs_change_raw IS NOT NULL THEN 'existing_event_transition_value'
        WHEN exact_run_cell THEN 'derived_existing_run_expectancy_cells'
        ELSE 'derived_pooled_base_out_run_expectancy'
    END AS expected_runs_change_method,
    expected_home_win_change_raw,
    coalesce(expected_home_win_change_raw, we_end - we_start)::DOUBLE
        AS expected_home_win_change,
    expected_batting_win_change_raw,
    coalesce(
        expected_batting_win_change_raw,
        CASE WHEN batting_side::VARCHAR = 'Home' THEN we_end - we_start
            ELSE we_start - we_end END
    )::DOUBLE AS expected_batting_win_change,
    CASE WHEN expected_home_win_change_raw IS NOT NULL
            AND expected_batting_win_change_raw IS NOT NULL
        THEN 'existing_event_transition_value'
        WHEN exact_win_cell THEN 'derived_existing_win_expectancy_cells'
        ELSE 'derived_neutral_win_expectancy_prior'
    END AS expected_win_change_method,
    '{MODEL_VERSION}' AS model_version,
    'exploratory' AS confidence_status
FROM base
"""


PARK_METRICS: Final = (
    "basic_park_factor",
    "singles_park_factor",
    "doubles_park_factor",
    "triples_park_factor",
    "home_runs_park_factor",
    "strikeouts_park_factor",
    "walks_park_factor",
    "batting_outs_park_factor",
    "runs_park_factor",
    "balls_in_play_park_factor",
    "trajectory_fly_ball_park_factor",
    "trajectory_ground_ball_park_factor",
    "trajectory_line_drive_park_factor",
    "trajectory_pop_up_park_factor",
    "trajectory_unknown_park_factor",
    "batted_distance_infield_park_factor",
    "batted_distance_outfield_park_factor",
    "batted_distance_unknown_park_factor",
    "batted_angle_left_park_factor",
    "batted_angle_right_park_factor",
    "batted_angle_middle_park_factor",
    "overall_park_factor",
)


def _park_values(alias: str) -> str:
    return ",\n".join(
        f"('{metric}', {alias}.{metric}::DOUBLE)" for metric in PARK_METRICS
    )


def build_park_factors_sql(config: Config = Config()) -> str:
    return f"""
WITH {_eligible_games_sql(config)},
targets AS (
    SELECT DISTINCT state.season, state.park_id::VARCHAR AS park_id,
        coalesce(state.league::VARCHAR, state.league_group, 'Other') AS league
    FROM {config.states_relation} AS state
    JOIN eligible_games USING (game_id)
),
source_long AS (
    SELECT factor.season, factor.park_id::VARCHAR AS park_id,
        coalesce(factor.league, 'Other') AS league, value.metric, value.value
    FROM {config.park_factors_relation} AS factor
    CROSS JOIN LATERAL (VALUES {_park_values("factor")}) AS value(metric, value)
),
donors AS (
    SELECT * FROM source_long
    WHERE season BETWEEN 1910 AND 2025 AND value IS NOT NULL AND isfinite(value)
),
league_pool AS (
    SELECT league, floor(season / 10)::INTEGER AS decade, metric,
        avg(value) AS value, count(*) AS donor_count
    FROM donors GROUP BY ALL
),
global_pool AS (
    SELECT metric, avg(value) AS value, count(*) AS donor_count
    FROM donors GROUP BY metric
),
target_long AS (
    SELECT target.*, metric.metric
    FROM targets AS target
    CROSS JOIN (SELECT DISTINCT metric FROM source_long) AS metric
)
SELECT
    target.season::SMALLINT AS season,
    target.park_id,
    target.league,
    target.metric,
    current.value::DOUBLE AS raw_value,
    CASE WHEN current.value IS NOT NULL AND isfinite(current.value) THEN current.value
        ELSE coalesce(park_donor.value, league_pool.value, global_pool.value, 1.0)
    END::DOUBLE AS completed_value,
    CASE WHEN current.value IS NOT NULL AND isfinite(current.value)
            THEN 'existing_park_factor'
        WHEN park_donor.value IS NOT NULL THEN 'nearest_season_same_park_league'
        WHEN league_pool.value IS NOT NULL THEN 'fixed_donor_league_decade_pool'
        WHEN global_pool.value IS NOT NULL THEN 'fixed_donor_global_metric_pool'
        ELSE 'neutral_park_factor_prior'
    END AS method,
    CASE WHEN current.value IS NOT NULL AND isfinite(current.value) THEN 1
        WHEN park_donor.value IS NOT NULL THEN park_donor.donor_count
        WHEN league_pool.value IS NOT NULL THEN league_pool.donor_count
        WHEN global_pool.value IS NOT NULL THEN global_pool.donor_count
        ELSE 0 END::BIGINT AS donor_count,
    '{MODEL_VERSION}' AS model_version,
    'exploratory' AS confidence_status
FROM target_long AS target
LEFT JOIN source_long AS current USING (season, park_id, league, metric)
LEFT JOIN LATERAL (
    SELECT value, count(*) OVER () AS donor_count
    FROM donors
    WHERE donors.park_id = target.park_id
      AND donors.league = target.league
      AND donors.metric = target.metric
    ORDER BY abs(donors.season - target.season), donors.season
    LIMIT 1
) AS park_donor ON TRUE
LEFT JOIN league_pool
  ON league_pool.league = target.league
 AND league_pool.decade = floor(target.season / 10)
 AND league_pool.metric = target.metric
LEFT JOIN global_pool USING (metric)
"""


def build_run_expectancy_sql(config: Config = Config()) -> str:
    return f"""
WITH {_eligible_games_sql(config)},
targets AS (
    SELECT DISTINCT state.season,
        coalesce(state.league::VARCHAR, state.league_group, 'Other') AS league,
        state.base_state_start::TINYINT AS base_state,
        state.outs_start::TINYINT AS outs,
        state.league_group,
        state.season_group
    FROM {config.states_relation} AS state
    JOIN eligible_games USING (game_id)
)
SELECT
    target.season::SMALLINT AS season,
    target.league,
    target.base_state,
    target.outs,
    current.re_value_mean::DOUBLE AS raw_value,
    coalesce(current.re_value_mean, matrix.avg_runs_scored, donor.re_value_mean, pool.value, 0.0)::DOUBLE
        AS completed_value,
    coalesce(current.re_value_sd, donor.re_value_sd)::DOUBLE AS completed_sd,
    CASE WHEN current.re_value_mean IS NOT NULL THEN 'existing_run_expectancy_summary'
        WHEN matrix.avg_runs_scored IS NOT NULL THEN 'existing_deterministic_run_expectancy_cell'
        WHEN donor.re_value_mean IS NOT NULL
            THEN 'conditional_transport_nearest_season_same_league_state'
        WHEN pool.value IS NOT NULL THEN 'fixed_donor_base_out_pool'
        ELSE 'zero_run_expectancy_prior'
    END AS method,
    donor.season::SMALLINT AS donor_season,
    coalesce(matrix.sample_size, pool.sample_size, 0)::BIGINT AS sample_size,
    '{MODEL_VERSION}' AS model_version,
    'exploratory' AS confidence_status
FROM targets AS target
LEFT JOIN {config.run_summary_relation} AS current
  ON current.season = target.season AND current.league = target.league
 AND current.base_state = target.base_state AND current.outs = target.outs
 AND current.outcome = 'runs_to_end'
LEFT JOIN {config.run_matrix_relation} AS matrix
  ON matrix.season_group = target.season_group
 AND matrix.league_group = target.league_group
 AND matrix.base_state = target.base_state AND matrix.outs = target.outs
LEFT JOIN LATERAL (
    SELECT season, re_value_mean, re_value_sd
    FROM {config.run_summary_relation} AS summary
    WHERE summary.league = target.league
      AND summary.base_state = target.base_state AND summary.outs = target.outs
      AND summary.outcome = 'runs_to_end'
      AND summary.season BETWEEN 1910 AND 2025
    ORDER BY abs(summary.season - target.season), summary.season
    LIMIT 1
) AS donor ON TRUE
LEFT JOIN LATERAL (
    SELECT sum(avg_runs_scored * sample_size) / nullif(sum(sample_size), 0) AS value,
        sum(sample_size) AS sample_size
    FROM {config.run_matrix_relation} AS all_matrix
    WHERE all_matrix.base_state = target.base_state AND all_matrix.outs = target.outs
) AS pool ON TRUE
"""


def build_state_transitions_sql(config: Config = Config()) -> str:
    return f"""
WITH {_eligible_games_sql(config)},
targets AS (
    SELECT DISTINCT state.season,
        coalesce(state.league::VARCHAR, state.league_group, 'Other') AS league,
        concat(state.outs_start, '_', state.base_state_start) AS start_state
    FROM {config.states_relation} AS state
    JOIN eligible_games USING (game_id)
),
donor_context AS (
    SELECT target.*,
        coalesce(same_league.season, any_league.season) AS donor_season,
        coalesce(same_league.league, any_league.league) AS donor_league,
        CASE WHEN same_league.season IS NOT NULL
            THEN 'conditional_transport_nearest_season_same_league_state'
            ELSE 'conditional_transport_nearest_season_any_league_same_state'
        END AS fallback_method
    FROM targets AS target
    LEFT JOIN LATERAL (
        SELECT DISTINCT season, league
        FROM {config.transition_summary_relation} AS summary
        WHERE summary.start_state = target.start_state
          AND summary.league = target.league
          AND summary.season BETWEEN 1910 AND 2025
        ORDER BY abs(summary.season - target.season), summary.season
        LIMIT 1
    ) AS same_league ON TRUE
    LEFT JOIN LATERAL (
        SELECT DISTINCT season, league
        FROM {config.transition_summary_relation} AS summary
        WHERE summary.start_state = target.start_state
          AND summary.season BETWEEN 1910 AND 2025
        ORDER BY abs(summary.season - target.season), summary.season, summary.league
        LIMIT 1
    ) AS any_league ON TRUE
),
expanded AS (
    SELECT donor_context.*, summary.end_class,
        summary.prob_mean, summary.prob_sd
    FROM donor_context
    JOIN {config.transition_summary_relation} AS summary
      ON summary.start_state = donor_context.start_state
     AND summary.season = donor_context.donor_season
     AND summary.league = donor_context.donor_league
     AND summary.outcome = 'end_state'
),
normalized AS (
    SELECT expanded.*,
        prob_mean / nullif(sum(prob_mean) OVER (
            PARTITION BY season, league, start_state
        ), 0) AS normalized_probability
    FROM expanded
)
SELECT
    normalized.season::SMALLINT AS season,
    normalized.league,
    normalized.start_state,
    normalized.end_class,
    current.prob_mean::DOUBLE AS raw_probability,
    CASE WHEN current.prob_mean IS NOT NULL THEN current.prob_mean
        ELSE normalized.normalized_probability END::DOUBLE AS completed_probability,
    coalesce(current.prob_sd, normalized.prob_sd)::DOUBLE AS completed_sd,
    CASE WHEN current.prob_mean IS NOT NULL THEN 'existing_state_transition_summary'
        ELSE normalized.fallback_method END AS method,
    normalized.donor_season::SMALLINT AS donor_season,
    normalized.donor_league,
    '{MODEL_VERSION}' AS model_version,
    'exploratory' AS confidence_status
FROM normalized
LEFT JOIN {config.transition_summary_relation} AS current
  ON current.season = normalized.season AND current.league = normalized.league
 AND current.start_state = normalized.start_state
 AND current.end_class = normalized.end_class AND current.outcome = 'end_state'
"""


def build_linear_weights_sql(config: Config = Config()) -> str:
    return f"""
WITH {_eligible_games_sql(config)},
contexts AS (
    SELECT DISTINCT state.season,
        coalesce(state.league::VARCHAR, state.league_group, 'Other') AS league
    FROM {config.states_relation} AS state
    JOIN eligible_games USING (game_id)
),
plays AS (
    SELECT DISTINCT play, play_category FROM {config.linear_weights_relation}
),
targets AS (
    SELECT contexts.*, plays.* FROM contexts CROSS JOIN plays
)
SELECT
    target.season::SMALLINT AS season,
    target.league,
    target.play,
    target.play_category,
    estimated.run_value_mean::DOUBLE AS raw_estimated_value,
    coalesce(estimated.run_value_mean, deterministic.average_run_value, donor.average_run_value, 0.0)::DOUBLE
        AS completed_run_value,
    coalesce(estimated.run_value_sd, deterministic.std_dev_run_value, donor.std_dev_run_value)::DOUBLE
        AS completed_sd,
    CASE WHEN estimated.run_value_mean IS NOT NULL THEN 'existing_linear_weights_estimated'
        WHEN deterministic.average_run_value IS NOT NULL THEN 'existing_deterministic_linear_weight'
        WHEN donor.average_run_value IS NOT NULL THEN 'nearest_season_deterministic_linear_weight'
        ELSE 'neutral_zero_linear_weight_prior'
    END AS method,
    coalesce(estimated.is_imputed, deterministic.is_imputed, donor.is_imputed, TRUE)::BOOLEAN
        AS source_is_imputed,
    '{MODEL_VERSION}' AS model_version,
    'exploratory' AS confidence_status
FROM targets AS target
LEFT JOIN {config.linear_weights_estimated_relation} AS estimated
  ON estimated.season = target.season AND estimated.league = target.league
 AND estimated.play = target.play
LEFT JOIN {config.linear_weights_relation} AS deterministic
  ON deterministic.season = target.season AND deterministic.league = target.league
 AND deterministic.play = target.play
LEFT JOIN LATERAL (
    SELECT average_run_value, std_dev_run_value, is_imputed
    FROM {config.linear_weights_relation} AS weight
    WHERE weight.play = target.play
      AND (weight.league = target.league OR target.league = 'Other')
      AND weight.season BETWEEN 1910 AND 2025
    ORDER BY CASE WHEN weight.league = target.league THEN 0 ELSE 1 END,
        abs(weight.season - target.season), weight.season
    LIMIT 1
) AS donor ON TRUE
"""


def build_values_component_sql(component: str, config: Config = Config()) -> str:
    builders = {
        "event_values": build_event_values_sql,
        "park_factors": build_park_factors_sql,
        "run_expectancy": build_run_expectancy_sql,
        "state_transitions": build_state_transitions_sql,
        "linear_weights": build_linear_weights_sql,
    }
    try:
        builder = builders[component]
    except KeyError as error:
        choices = ", ".join(sorted(builders))
        raise ValueError(
            f"unknown values component {component!r}; expected {choices}"
        ) from error
    return builder(config)
