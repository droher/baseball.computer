from __future__ import annotations

import logging
import re
from typing import ClassVar, Final, Mapping

from pydantic import BaseModel, ConfigDict, Field, model_validator


logger = logging.getLogger(__name__)
MODEL_NAME: Final = "pbp_completed_runners"
MODEL_VERSION: Final = "1.0.0"
SOURCE_SIGNATURE: Final = "pbp-runners-current-source-v1"
INPUT_RELATIONS: Final = (
    "main_models.stg_event_baserunners",
    "main_models.stg_events",
    "main_models.game_start_info",
    "main_models.event_base_out_states",
)

OUTPUT_SCHEMA: Final[Mapping[str, str]] = {
    "event_key": "UINTEGER",
    "game_id": "VARCHAR",
    "event_id": "UTINYINT",
    "season": "SMALLINT",
    "baserunner": "VARCHAR",
    "runner_id": "VARCHAR",
    "runner_lineup_position": "UTINYINT",
    "attempted_advance_to_base": "VARCHAR",
    "baserunning_play_type": "VARCHAR",
    "is_out": "BOOLEAN",
    "run_scored_flag": "BOOLEAN",
    "raw_base_end": "VARCHAR",
    "completed_base_end": "VARCHAR",
    "completed_destination": "VARCHAR",
    "destination_method": "VARCHAR",
    "destination_status": "VARCHAR",
    "constraint_disposition": "VARCHAR",
    "destination_support": "VARCHAR",
    "p_first": "DOUBLE",
    "p_second": "DOUBLE",
    "p_third": "DOUBLE",
    "p_home": "DOUBLE",
    "p_out": "DOUBLE",
    "raw_explicit_charged_pitcher_id": "VARCHAR",
    "charge_event_key": "UINTEGER",
    "completed_charged_pitcher_id": "VARCHAR",
    "charged_pitcher_method": "VARCHAR",
    "model_name": "VARCHAR",
    "model_version": "VARCHAR",
    "source_signature": "VARCHAR",
    "confidence_status": "VARCHAR",
}

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?$")


class Config(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    start_season: int = Field(default=1903, ge=1800, le=3000)
    end_season: int = Field(default=2025, ge=1800, le=3000)
    sample_games: int | None = Field(default=None, ge=1)
    sample_seed: str = "pbp-runner-completion-v1"
    runners_relation: str = "main_models.stg_event_baserunners"
    events_relation: str = "main_models.stg_events"
    games_relation: str = "main_models.game_start_info"
    base_out_relation: str = "main_models.event_base_out_states"

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
            self.runners_relation,
            self.events_relation,
            self.games_relation,
            self.base_out_relation,
        )


RunnerCompletionConfig = Config


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_runner_completion_sql(config: Config = Config()) -> str:
    logger.info(
        "Building runner completion SQL for seasons %d-%d",
        config.start_season,
        config.end_season,
    )
    limit = f"LIMIT {config.sample_games}" if config.sample_games is not None else ""
    seed = _literal(config.sample_seed)
    return f"""
WITH eligible_games AS (
    SELECT game.game_id, game.season
    FROM {config.games_relation} AS game
    WHERE game.source_type = 'PlayByPlay'
      AND game.season BETWEEN {config.start_season} AND {config.end_season}
      AND EXISTS (
          SELECT 1 FROM {config.events_relation} AS event
          WHERE event.game_id = game.game_id
      )
    ORDER BY HASH(game.game_id, {seed}), game.game_id
    {limit}
),
base AS (
    SELECT
        runner.event_key,
        runner.game_id,
        runner.event_id,
        game.season,
        CAST(runner.baserunner AS VARCHAR) AS baserunner,
        runner.runner_id,
        runner.runner_lineup_position,
        CAST(runner.attempted_advance_to_base AS VARCHAR)
            AS attempted_advance_to_base,
        CAST(runner.baserunning_play_type AS VARCHAR) AS baserunning_play_type,
        runner.is_out,
        runner.run_scored_flag,
        CAST(runner.base_end AS VARCHAR) AS raw_base_end,
        runner.explicit_charged_pitcher_id AS raw_explicit_charged_pitcher_id,
        runner.charge_event_key,
        event.pitcher_id AS event_pitcher_id,
        charge_event.pitcher_id AS charge_event_pitcher_id,
        state.frame_end_flag,
        state.truncated_frame_flag,
        state.game_end_flag,
        state.outs_on_play,
        CASE WHEN CAST(runner.baserunner AS VARCHAR) = 'Batter'
            THEN 'First' ELSE CAST(runner.baserunner AS VARCHAR) END AS fallback_base,
        CASE
            WHEN runner.runner_id = state.runner_first_id_end THEN 'First'
            WHEN runner.runner_id = state.runner_second_id_end THEN 'Second'
            WHEN runner.runner_id = state.runner_third_id_end THEN 'Third'
        END AS next_state_base
    FROM {config.runners_relation} AS runner
    JOIN eligible_games AS game USING (game_id)
    JOIN {config.events_relation} AS event USING (event_key)
    LEFT JOIN {config.events_relation} AS charge_event
      ON charge_event.event_key = runner.charge_event_key
    LEFT JOIN {config.base_out_relation} AS state USING (event_key)
),
classified AS (
    SELECT
        base.*,
        CASE
            WHEN is_out THEN 'Out'
            WHEN run_scored_flag THEN 'Home'
            WHEN raw_base_end IS NOT NULL THEN raw_base_end
            WHEN next_state_base IS NOT NULL THEN next_state_base
            ELSE fallback_base
        END AS completed_destination,
        CASE
            WHEN raw_base_end IS NOT NULL THEN raw_base_end
            WHEN is_out THEN NULL
            WHEN run_scored_flag THEN 'Home'
            WHEN next_state_base IS NOT NULL THEN next_state_base
            ELSE fallback_base
        END AS completed_base_end,
        CASE
            WHEN is_out OR run_scored_flag OR raw_base_end IS NOT NULL THEN 'observed'
            WHEN next_state_base IS NOT NULL THEN 'derived_next_event_runner_identity'
            ELSE 'constrained_conflict_fallback'
        END AS destination_method,
        CASE
            WHEN is_out OR run_scored_flag OR raw_base_end IS NOT NULL THEN 'source_complete'
            WHEN next_state_base IS NOT NULL THEN 'derived_consistent'
            WHEN frame_end_flag THEN 'runner_absent_at_frame_end_conflict'
            ELSE 'runner_absent_from_next_state_conflict'
        END AS destination_status,
        CASE
            WHEN is_out OR run_scored_flag OR raw_base_end IS NOT NULL
                THEN 'source_preserved'
            WHEN next_state_base IS NOT NULL
                THEN 'filled_from_next_event_identity'
            WHEN frame_end_flag AND outs_on_play > 0
                THEN 'probability_support_retains_source_not_out_and_frame_end_out_conflict'
            WHEN frame_end_flag AND (truncated_frame_flag OR game_end_flag)
                THEN 'probability_support_retains_source_not_out_and_terminal_state_conflict'
            ELSE 'probability_support_retains_source_not_out_and_missing_next_identity'
        END AS constraint_disposition,
        CASE
            WHEN is_out THEN 'Out'
            WHEN run_scored_flag THEN 'Home'
            WHEN raw_base_end IS NOT NULL THEN raw_base_end
            WHEN next_state_base IS NOT NULL THEN next_state_base
            WHEN attempted_advance_to_base IS NOT NULL
              AND attempted_advance_to_base <> fallback_base
                THEN fallback_base || '|' || attempted_advance_to_base || '|Out'
            ELSE fallback_base || '|Out'
        END AS destination_support
    FROM base
)
SELECT
    event_key,
    game_id,
    event_id,
    season,
    baserunner,
    runner_id,
    runner_lineup_position,
    attempted_advance_to_base,
    baserunning_play_type,
    is_out,
    run_scored_flag,
    raw_base_end,
    completed_base_end,
    completed_destination,
    destination_method,
    destination_status,
    constraint_disposition,
    destination_support,
    CASE
        WHEN completed_destination = 'First' AND destination_method <> 'constrained_conflict_fallback' THEN 1.0
        WHEN destination_method = 'constrained_conflict_fallback' AND fallback_base = 'First'
            THEN CASE WHEN attempted_advance_to_base IS NOT NULL
                AND attempted_advance_to_base <> fallback_base THEN 0.34 ELSE 0.5 END
        WHEN destination_method = 'constrained_conflict_fallback'
          AND attempted_advance_to_base = 'First' THEN 0.33
        ELSE 0.0
    END::DOUBLE AS p_first,
    CASE
        WHEN completed_destination = 'Second' AND destination_method <> 'constrained_conflict_fallback' THEN 1.0
        WHEN destination_method = 'constrained_conflict_fallback' AND fallback_base = 'Second'
            THEN CASE WHEN attempted_advance_to_base IS NOT NULL
                AND attempted_advance_to_base <> fallback_base THEN 0.34 ELSE 0.5 END
        WHEN destination_method = 'constrained_conflict_fallback'
          AND attempted_advance_to_base = 'Second' THEN 0.33
        ELSE 0.0
    END::DOUBLE AS p_second,
    CASE
        WHEN completed_destination = 'Third' AND destination_method <> 'constrained_conflict_fallback' THEN 1.0
        WHEN destination_method = 'constrained_conflict_fallback' AND fallback_base = 'Third'
            THEN CASE WHEN attempted_advance_to_base IS NOT NULL
                AND attempted_advance_to_base <> fallback_base THEN 0.34 ELSE 0.5 END
        WHEN destination_method = 'constrained_conflict_fallback'
          AND attempted_advance_to_base = 'Third' THEN 0.33
        ELSE 0.0
    END::DOUBLE AS p_third,
    CASE
        WHEN completed_destination = 'Home' AND destination_method <> 'constrained_conflict_fallback' THEN 1.0
        WHEN destination_method = 'constrained_conflict_fallback'
          AND attempted_advance_to_base = 'Home' THEN 0.33
        ELSE 0.0
    END::DOUBLE AS p_home,
    CASE
        WHEN completed_destination = 'Out' AND destination_method <> 'constrained_conflict_fallback' THEN 1.0
        WHEN destination_method = 'constrained_conflict_fallback'
          AND attempted_advance_to_base IS NOT NULL
          AND attempted_advance_to_base <> fallback_base THEN 0.33
        WHEN destination_method = 'constrained_conflict_fallback' THEN 0.5
        ELSE 0.0
    END::DOUBLE AS p_out,
    raw_explicit_charged_pitcher_id,
    charge_event_key,
    COALESCE(
        raw_explicit_charged_pitcher_id,
        charge_event_pitcher_id,
        event_pitcher_id
    ) AS completed_charged_pitcher_id,
    CASE
        WHEN raw_explicit_charged_pitcher_id IS NOT NULL THEN 'observed_explicit_override'
        WHEN charge_event_pitcher_id IS NOT NULL THEN 'derived_charge_event_pitcher'
        WHEN event_pitcher_id IS NOT NULL THEN 'fallback_current_event_pitcher'
        ELSE 'unresolved_missing_pitcher'
    END AS charged_pitcher_method,
    '{MODEL_NAME}' AS model_name,
    '{MODEL_VERSION}' AS model_version,
    '{SOURCE_SIGNATURE}' AS source_signature,
    CASE
        WHEN destination_method = 'constrained_conflict_fallback' THEN 'conflicted'
        WHEN charged_pitcher_method = 'unresolved_missing_pitcher' THEN 'weakly_identified'
        ELSE 'exploratory'
    END AS confidence_status
FROM classified
"""
