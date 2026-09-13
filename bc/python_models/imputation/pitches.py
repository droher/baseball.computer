from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from typing import ClassVar, Final

from pydantic import BaseModel, ConfigDict, Field, model_validator


logger = logging.getLogger(__name__)
MODEL_NAME: Final = "pbp_imputed_pitches"
MODEL_VERSION: Final = "1.0.0"
SOURCE_SIGNATURE: Final = "pbp-pitches-current-source-v1"
INPUT_RELATIONS: Final = (
    "main_models.stg_events",
    "main_models.game_start_info",
    "main_models.stg_event_pitch_sequence_status",
    "main_models.stg_event_pitch_sequences",
    "main_models.event_pitch_sequence_stats",
)

PITCH_COUNTERS: Final = (
    "pitches",
    "swings",
    "swings_with_contact",
    "strikes",
    "strikes_called",
    "strikes_swinging",
    "strikes_foul",
    "strikes_foul_tip",
    "strikes_in_play",
    "strikes_unknown",
    "balls",
    "balls_called",
    "balls_intentional",
    "balls_automatic",
    "unknown_pitches",
    "pitchouts",
    "pitcher_pickoff_attempts",
    "catcher_pickoff_attempts",
    "pitches_blocked_by_catcher",
    "pitches_with_runners_going",
    "passed_balls",
    "wild_pitches",
    "balks",
)

OUTPUT_SCHEMA: Final[Mapping[str, str]] = {
    "event_key": "UINTEGER",
    "game_id": "VARCHAR",
    "event_id": "UTINYINT",
    "season": "SMALLINT",
    "appearance_start_event_id": "UTINYINT",
    "plate_appearance_result": "VARCHAR",
    "count_balls": "UTINYINT",
    "count_strikes": "UTINYINT",
    "completed_count_balls": "UTINYINT",
    "completed_count_strikes": "UTINYINT",
    "count_balls_method": "VARCHAR",
    "count_strikes_method": "VARCHAR",
    "source_resolution_status": "VARCHAR",
    "raw_pitch_sequence": "VARCHAR",
    "source_pitch_sequence": "VARCHAR",
    "completed_pitch_sequence": "VARCHAR",
    "pitch_sequence_method": "VARCHAR",
    "pitch_token_completion_method": "VARCHAR",
    "constraint_status": "VARCHAR",
    "constraint_disposition": "VARCHAR",
    "donor_count": "BIGINT",
    **{f"source_{counter}": "UTINYINT" for counter in PITCH_COUNTERS},
    **{f"completed_{counter}": "USMALLINT" for counter in PITCH_COUNTERS},
    **{f"{counter}_method": "VARCHAR" for counter in PITCH_COUNTERS},
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
    sample_seed: str = "pbp-pitch-completion-v1"
    events_relation: str = "main_models.stg_events"
    games_relation: str = "main_models.game_start_info"
    status_relation: str = "main_models.stg_event_pitch_sequence_status"
    sequences_relation: str = "main_models.stg_event_pitch_sequences"
    stats_relation: str = "main_models.event_pitch_sequence_stats"

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
            self.events_relation,
            self.games_relation,
            self.status_relation,
            self.sequences_relation,
            self.stats_relation,
        )


PitchCompletionConfig = Config


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _counter_source_select(alias: str) -> str:
    return ",\n".join(
        f"{alias}.{counter} AS source_{counter}" for counter in PITCH_COUNTERS
    )


def _appearance_counter_sums(alias: str) -> str:
    return ",\n".join(
        f"SUM(COALESCE({alias}.source_{counter}, 0))::BIGINT AS source_pa_{counter}"
        for counter in PITCH_COUNTERS
    )


def _completed_counter(counter: str) -> str:
    observed = f"w.source_{counter}"
    if counter in {
        "strikes",
        "balls",
        "balls_called",
        "strikes_called",
        "strikes_foul",
        "strikes_in_play",
        "swings",
        "swings_with_contact",
    }:
        observed = f"COALESCE(w.source_{counter}, 0) + COALESCE(w.delta_{counter}, 0)"
    elif counter in {"strikes_unknown", "unknown_pitches"}:
        observed = "0"
    if counter == "pitches":
        estimated = "a.event_balls + a.event_strikes + a.event_fouls"
    elif counter == "balls":
        estimated = "a.event_balls"
    elif counter == "balls_called":
        estimated = "a.event_balls - a.event_hit_batter"
    elif counter == "strikes":
        estimated = "a.event_strikes + a.event_fouls"
    elif counter == "strikes_unknown":
        estimated = "0"
    elif counter == "strikes_called":
        estimated = "a.event_strikes - a.event_in_play"
    elif counter == "strikes_foul":
        estimated = "a.event_fouls"
    elif counter == "strikes_in_play":
        estimated = "a.event_in_play"
    elif counter == "unknown_pitches":
        estimated = "0"
    elif counter in {"swings", "swings_with_contact"}:
        estimated = "a.event_fouls + a.event_in_play"
    elif counter in {"passed_balls", "wild_pitches", "balks"}:
        estimated = f"COALESCE(w.source_{counter}, 0)"
    else:
        estimated = "0"
    return (
        "CASE WHEN w.source_resolution_status = 'Resolved' "
        f"THEN COALESCE({observed}, 0) ELSE {estimated} END::USMALLINT "
        f"AS completed_{counter}"
    )


def _counter_method(counter: str) -> str:
    if counter in {"passed_balls", "wild_pitches", "balks"}:
        estimated = "observed_baserunning_event"
    elif counter in {
        "pitches",
        "balls",
        "balls_called",
        "strikes",
        "strikes_called",
        "strikes_foul",
        "strikes_unknown",
        "unknown_pitches",
        "swings",
        "swings_with_contact",
    }:
        estimated = "estimated_count_boundary_and_empirical_total"
    else:
        estimated = "estimated_from_concrete_completed_sequence"
    estimated_expression = (
        "CASE WHEN a.donor_count > 0 THEN 'estimated_count_boundary_and_empirical_total' "
        "ELSE 'estimated_count_boundary_with_declared_legal_fallback' END"
        if estimated == "estimated_count_boundary_and_empirical_total"
        else _literal(estimated)
    )
    return (
        "CASE WHEN w.source_resolution_status = 'Resolved' "
        "AND COALESCE(w.has_unknown_pitch_token, FALSE) "
        "THEN 'observed_incremental_with_declared_token_fallback' "
        "WHEN w.source_resolution_status = 'Resolved' THEN 'observed_incremental' "
        f"ELSE {estimated_expression} END AS {counter}_method"
    )


def build_pitch_completion_sql(config: Config | None = None) -> str:
    config = config or Config()
    logger.info(
        "Building pitch completion SQL for seasons %d-%d",
        config.start_season,
        config.end_season,
    )
    limit = f"LIMIT {config.sample_games}" if config.sample_games is not None else ""
    seed = _literal(config.sample_seed)
    source_select = _counter_source_select("stats")
    appearance_sums = _appearance_counter_sums("w")
    source_counter_outputs = ",\n".join(
        f"w.source_{counter}" for counter in PITCH_COUNTERS
    )
    completed_counter_outputs = ",\n".join(
        _completed_counter(counter) for counter in PITCH_COUNTERS
    )
    counter_method_outputs = ",\n".join(
        _counter_method(counter) for counter in PITCH_COUNTERS
    )
    delta_outputs = ",\n".join(
        f"SUM(({completed})::INTEGER - ({original})::INTEGER) AS delta_{counter}"
        for counter, completed, original in (
            (
                "strikes",
                "completed_item IN ('Foul', 'CalledStrike', 'InPlay')",
                "sequence_item = 'StrikeUnknownType'",
            ),
            ("balls", "completed_item IN ('Ball', 'HitBatter')", "FALSE"),
            ("balls_called", "completed_item = 'Ball'", "FALSE"),
            (
                "strikes_called",
                "completed_item = 'CalledStrike'",
                "sequence_item = 'StrikeUnknownType'",
            ),
            ("strikes_foul", "completed_item = 'Foul'", "FALSE"),
            ("strikes_in_play", "completed_item = 'InPlay'", "FALSE"),
            ("swings", "completed_item IN ('Foul', 'InPlay')", "FALSE"),
            ("swings_with_contact", "completed_item IN ('Foul', 'InPlay')", "FALSE"),
        )
    )
    delta_select = ",\n".join(
        f"sequence.delta_{counter}"
        for counter in (
            "strikes",
            "balls",
            "balls_called",
            "strikes_called",
            "strikes_foul",
            "strikes_in_play",
            "swings",
            "swings_with_contact",
        )
    )
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
donor_games AS (
    SELECT game.game_id
    FROM {config.games_relation} AS game
    WHERE game.source_type = 'PlayByPlay'
      AND game.season BETWEEN 1903 AND 2025
      AND EXISTS (
          SELECT 1 FROM {config.events_relation} AS event
          WHERE event.game_id = game.game_id
      )
),
source_sequence_context AS (
    SELECT
        sequence.*,
        event.event_id,
        event.count_balls,
        CAST(event.plate_appearance_result AS VARCHAR) AS result,
        COALESCE(status.appearance_start_event_id, event.event_id) AS appearance_start,
        MAX(sequence.sequence_id) FILTER (
            WHERE CAST(sequence.sequence_item AS VARCHAR) NOT IN (
                'NoPitch', 'Unrecognized', 'PickoffAttemptFirst', 'PickoffAttemptSecond',
                'PickoffAttemptThird', 'PlayNotInvolvingBatter'
            )
        ) OVER (PARTITION BY sequence.event_key) AS last_pitch_id
    FROM {config.sequences_relation} AS sequence
    JOIN eligible_games AS game USING (game_id)
    JOIN {config.events_relation} AS event USING (event_key)
    LEFT JOIN {config.status_relation} AS status USING (event_key)
),
source_sequence_items AS (
    SELECT *,
        COUNT(*) FILTER (WHERE CAST(sequence_item AS VARCHAR) IN (
            'Ball', 'IntentionalBall', 'AutomaticBall', 'Pitchout'
        )) OVER (
            PARTITION BY game_id, appearance_start ORDER BY event_id, sequence_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prior_known_balls,
        COUNT(*) FILTER (WHERE CAST(sequence_item AS VARCHAR) = 'Unknown') OVER (
            PARTITION BY event_key ORDER BY sequence_id
        ) AS unknown_ordinal
    FROM source_sequence_context
),
source_boundary_items AS (
    SELECT *, MAX(prior_known_balls) FILTER (WHERE sequence_id = last_pitch_id)
        OVER (PARTITION BY event_key) AS known_balls_at_boundary
    FROM source_sequence_items
),
completed_source_items AS (
    SELECT *,
        CASE
            WHEN CAST(sequence_item AS VARCHAR) = 'Unrecognized' THEN 'NoPitch'
            WHEN CAST(sequence_item AS VARCHAR) IN ('Unknown', 'StrikeUnknownType') THEN
                CASE
                    WHEN sequence_id = last_pitch_id AND result = 'StrikeOut'
                        THEN 'CalledStrike'
                    WHEN CAST(sequence_item AS VARCHAR) = 'Unknown'
                      AND sequence_id = last_pitch_id THEN CASE
                        WHEN result IN ('Walk', 'IntentionalWalk') THEN 'Ball'
                        WHEN result = 'HitByPitch' THEN 'HitBatter'
                        WHEN result IS NOT NULL AND result <> 'Interference' THEN 'InPlay'
                        WHEN unknown_ordinal <= COALESCE(count_balls, 0) - known_balls_at_boundary
                            THEN 'Ball'
                        ELSE 'Foul' END
                    WHEN CAST(sequence_item AS VARCHAR) = 'Unknown'
                      AND unknown_ordinal <= COALESCE(count_balls, 0) - known_balls_at_boundary
                        THEN 'Ball'
                    ELSE 'Foul'
                END
            ELSE CAST(sequence_item AS VARCHAR)
        END AS completed_item
    FROM source_boundary_items
),
source_token_deltas AS (
    SELECT event_key, {delta_outputs}
    FROM completed_source_items
    WHERE CAST(sequence_item AS VARCHAR) IN ('Unknown', 'StrikeUnknownType', 'Unrecognized')
    GROUP BY event_key
),
source_sequences AS (
    SELECT
        sequence.event_key,
        STRING_AGG(CAST(sequence.sequence_item AS VARCHAR), '|' ORDER BY sequence.sequence_id)
            AS source_pitch_sequence,
        STRING_AGG(completed_item, '|' ORDER BY sequence.sequence_id)
            AS completed_source_pitch_sequence,
        BOOL_OR(
            CAST(sequence.sequence_item AS VARCHAR)
                IN ('StrikeUnknownType', 'Unknown', 'Unrecognized')
        ) AS has_unknown_pitch_token
    FROM completed_source_items AS sequence
    GROUP BY sequence.event_key
),
base AS (
    SELECT
        event.event_key,
        event.game_id,
        event.event_id,
        event.season,
        COALESCE(status.appearance_start_event_id, event.event_id)
            AS appearance_start_event_id,
        CAST(event.plate_appearance_result AS VARCHAR) AS plate_appearance_result,
        event.count_balls,
        event.count_strikes,
        COALESCE(status.pitch_sequence_resolution_status, 'MissingStatus')
            AS source_resolution_status,
        status.raw_pitch_sequence,
        sequence.source_pitch_sequence,
        sequence.completed_source_pitch_sequence,
        sequence.has_unknown_pitch_token,
        {delta_select},
        {source_select}
    FROM {config.events_relation} AS event
    JOIN eligible_games AS game USING (game_id)
    LEFT JOIN {config.status_relation} AS status USING (event_key)
    LEFT JOIN (SELECT * FROM source_sequences LEFT JOIN source_token_deltas USING (event_key)) AS sequence USING (event_key)
    LEFT JOIN {config.stats_relation} AS stats USING (event_key)
),
donor_base AS (
    SELECT
        event.event_key,
        event.game_id,
        event.event_id,
        event.season,
        COALESCE(status.appearance_start_event_id, event.event_id)
            AS appearance_start_event_id,
        CAST(event.plate_appearance_result AS VARCHAR) AS plate_appearance_result,
        event.count_balls,
        event.count_strikes,
        COALESCE(status.pitch_sequence_resolution_status, 'MissingStatus')
            AS source_resolution_status,
        stats.pitches AS source_pitches
    FROM {config.events_relation} AS event
    JOIN donor_games AS game USING (game_id)
    LEFT JOIN {config.status_relation} AS status USING (event_key)
    LEFT JOIN {config.stats_relation} AS stats USING (event_key)
),
donor_windowed AS (
    SELECT
        donor_base.*,
        MAX(event_id) OVER appearance AS terminal_event_id
    FROM donor_base
    WINDOW appearance AS (
        PARTITION BY game_id, appearance_start_event_id
    )
),
donor_rollup AS (
    SELECT
        game_id,
        appearance_start_event_id,
        FLOOR(MIN(season) / 10)::INTEGER AS decade,
        FIRST(plate_appearance_result ORDER BY event_id DESC) AS terminal_result,
        FIRST(count_balls ORDER BY event_id DESC) AS terminal_count_balls,
        FIRST(count_strikes ORDER BY event_id DESC) AS terminal_count_strikes,
        BOOL_AND(source_resolution_status = 'Resolved') AS is_resolved,
        SUM(COALESCE(source_pitches, 0))::BIGINT AS source_pa_pitches
    FROM donor_windowed
    GROUP BY game_id, appearance_start_event_id
),
windowed AS (
    SELECT
        base.*,
        LEAD(count_balls) OVER appearance_order AS next_count_balls,
        LEAD(count_strikes) OVER appearance_order AS next_count_strikes,
        MAX(event_id) OVER appearance AS terminal_event_id
    FROM base
    WINDOW
        appearance AS (
            PARTITION BY game_id, appearance_start_event_id
        ),
        appearance_order AS (
            PARTITION BY game_id, appearance_start_event_id ORDER BY event_id
        )
),
appearance_rollup AS (
    SELECT
        game_id,
        appearance_start_event_id,
        MIN(season)::SMALLINT AS season,
        FLOOR(MIN(season) / 10)::INTEGER AS decade,
        FIRST(event_key ORDER BY event_id DESC) AS terminal_event_key,
        FIRST(plate_appearance_result ORDER BY event_id DESC) AS terminal_result,
        FIRST(count_balls ORDER BY event_id DESC) AS terminal_count_balls,
        FIRST(count_strikes ORDER BY event_id DESC) AS terminal_count_strikes,
        ANY_VALUE(source_resolution_status) AS source_resolution_status,
        BOOL_OR(
            (next_count_balls IS NOT NULL AND count_balls IS NOT NULL
                AND next_count_balls < count_balls)
            OR (next_count_strikes IS NOT NULL AND count_strikes IS NOT NULL
                AND next_count_strikes < count_strikes)
        ) AS has_count_regression,
        SUM(
            CASE WHEN event_id < terminal_event_id
                THEN GREATEST(
                    COALESCE(next_count_balls, count_balls, 0)::INTEGER
                    - COALESCE(count_balls, 0)::INTEGER,
                    0
                )
                ELSE 0 END
        )::BIGINT AS prior_ball_progress,
        SUM(
            CASE WHEN event_id < terminal_event_id
                THEN GREATEST(
                    COALESCE(next_count_strikes, count_strikes, 0)::INTEGER
                    - COALESCE(count_strikes, 0)::INTEGER,
                    0
                )
                ELSE 0 END
        )::BIGINT AS prior_strike_progress,
        {appearance_sums}
    FROM windowed AS w
    GROUP BY game_id, appearance_start_event_id
),
exact_priors AS (
    SELECT
        decade,
        terminal_result,
        terminal_count_balls,
        terminal_count_strikes,
        MEDIAN(source_pa_pitches)::BIGINT AS prior_pitches,
        COUNT(*)::BIGINT AS donor_count
    FROM donor_rollup
    WHERE is_resolved
      AND terminal_result IS NOT NULL
    GROUP BY decade, terminal_result, terminal_count_balls, terminal_count_strikes
),
broad_priors AS (
    SELECT
        terminal_result,
        MEDIAN(source_pa_pitches)::BIGINT AS prior_pitches,
        COUNT(*)::BIGINT AS donor_count
    FROM donor_rollup
    WHERE is_resolved
      AND terminal_result IS NOT NULL
    GROUP BY terminal_result
),
appearance_targets AS (
    SELECT
        rollup.*,
        COALESCE(exact.prior_pitches, broad.prior_pitches, 1)::BIGINT AS prior_pitches,
        COALESCE(exact.donor_count, broad.donor_count, 0)::BIGINT AS donor_count,
        CASE
            WHEN rollup.terminal_result = 'IntentionalWalk'
              AND rollup.season >= 2017
              AND COALESCE(rollup.terminal_count_balls, 0) = 0
              AND COALESCE(rollup.terminal_count_strikes, 0) = 0
              AND rollup.prior_ball_progress = 0
              AND rollup.prior_strike_progress = 0
            THEN TRUE ELSE FALSE
        END AS is_automatic_intentional_walk
    FROM appearance_rollup AS rollup
    LEFT JOIN exact_priors AS exact
      ON exact.decade = rollup.decade
      AND exact.terminal_result IS NOT DISTINCT FROM rollup.terminal_result
      AND exact.terminal_count_balls IS NOT DISTINCT FROM rollup.terminal_count_balls
      AND exact.terminal_count_strikes IS NOT DISTINCT FROM rollup.terminal_count_strikes
    LEFT JOIN broad_priors AS broad
      ON broad.terminal_result IS NOT DISTINCT FROM rollup.terminal_result
),
appearance_estimates AS (
    SELECT
        target.*,
        CASE
            WHEN is_automatic_intentional_walk THEN 0
            WHEN terminal_result IN ('Walk', 'IntentionalWalk') THEN 4
            WHEN terminal_result = 'HitByPitch' THEN GREATEST(
                COALESCE(terminal_count_balls, 0), prior_ball_progress
            ) + 1
            ELSE GREATEST(
                COALESCE(terminal_count_balls, 0), prior_ball_progress
            )
        END::BIGINT AS target_balls,
        CASE
            WHEN is_automatic_intentional_walk THEN 0
            WHEN terminal_result = 'StrikeOut' THEN 3
            WHEN terminal_result IS NOT NULL
              AND terminal_result NOT IN (
                  'Walk', 'IntentionalWalk', 'HitByPitch', 'Interference'
              ) THEN GREATEST(
                COALESCE(terminal_count_strikes, 0), prior_strike_progress
              ) + 1
            ELSE GREATEST(
                COALESCE(terminal_count_strikes, 0), prior_strike_progress
            )
        END::BIGINT AS target_strikes
    FROM appearance_targets AS target
),
event_allocations AS (
    SELECT
        w.*,
        estimate.donor_count,
        estimate.target_balls,
        estimate.target_strikes,
        estimate.prior_ball_progress,
        estimate.prior_strike_progress,
        estimate.prior_pitches,
        estimate.is_automatic_intentional_walk,
        estimate.has_count_regression,
        estimate.terminal_result,
        estimate.terminal_count_balls,
        estimate.terminal_count_strikes,
        CASE
            WHEN w.event_id < w.terminal_event_id THEN GREATEST(
                COALESCE(w.next_count_balls, w.count_balls, 0)::INTEGER
                    - COALESCE(w.count_balls, 0)::INTEGER,
                0
            )
            ELSE GREATEST(estimate.target_balls - estimate.prior_ball_progress, 0)
        END::BIGINT AS event_balls,
        CASE
            WHEN w.event_id < w.terminal_event_id THEN GREATEST(
                COALESCE(w.next_count_strikes, w.count_strikes, 0)::INTEGER
                    - COALESCE(w.count_strikes, 0)::INTEGER,
                0
            )
            ELSE GREATEST(estimate.target_strikes - estimate.prior_strike_progress, 0)
        END::BIGINT AS event_strikes
    FROM windowed AS w
    JOIN appearance_estimates AS estimate
      USING (game_id, appearance_start_event_id)
),
allocations_with_completion AS (
    SELECT
        allocation.*,
        CASE
            WHEN is_automatic_intentional_walk THEN 0
            WHEN event_id = terminal_event_id
              AND (
                  terminal_result = 'StrikeOut'
                  OR (
                      terminal_result IS NOT NULL
                      AND terminal_result NOT IN (
                          'Walk', 'IntentionalWalk', 'HitByPitch', 'Interference'
                      )
                      AND target_strikes >= 3
                  )
                  OR (
                      terminal_result IN (
                          'Walk', 'IntentionalWalk', 'HitByPitch', 'Interference'
                      )
                      AND target_strikes >= 2
                  )
              ) THEN GREATEST(prior_pitches - target_balls - target_strikes, 0)
            ELSE 0
        END::BIGINT AS event_fouls
        ,CASE WHEN event_id = terminal_event_id
          AND terminal_result = 'HitByPitch' THEN 1 ELSE 0 END::BIGINT
            AS event_hit_batter
        ,CASE WHEN event_id = terminal_event_id
          AND terminal_result IS NOT NULL
          AND terminal_result NOT IN (
              'StrikeOut', 'Walk', 'IntentionalWalk', 'HitByPitch', 'Interference'
          ) THEN 1 ELSE 0 END::BIGINT AS event_in_play
    FROM event_allocations AS allocation
),
completed_output AS (
SELECT
    w.event_key,
    w.game_id,
    w.event_id,
    w.season,
    w.appearance_start_event_id,
    w.plate_appearance_result,
    w.count_balls,
    w.count_strikes,
    CASE
        WHEN w.event_id = w.terminal_event_id
          AND a.terminal_result IN ('Walk', 'IntentionalWalk')
          AND NOT a.is_automatic_intentional_walk THEN 3
        WHEN w.event_id = w.terminal_event_id
          AND a.terminal_result = 'HitByPitch'
            THEN LEAST(GREATEST(a.target_balls - 1, 0), 3)
        WHEN w.count_balls IS NOT NULL THEN LEAST(w.count_balls, 3)
        WHEN w.event_id = w.terminal_event_id THEN LEAST(a.target_balls, 3)
        ELSE LEAST(COALESCE(SUM(a.event_balls) OVER (
            PARTITION BY w.game_id, w.appearance_start_event_id
            ORDER BY w.event_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0), 3)
    END::UTINYINT AS completed_count_balls,
    CASE
        WHEN w.event_id = w.terminal_event_id
          AND a.terminal_result = 'StrikeOut' THEN 2
        WHEN w.count_strikes IS NOT NULL THEN LEAST(w.count_strikes, 2)
        WHEN w.event_id = w.terminal_event_id
          AND a.terminal_result IS NOT NULL
          AND a.terminal_result NOT IN (
              'Walk', 'IntentionalWalk', 'HitByPitch', 'Interference'
          ) THEN LEAST(GREATEST(a.target_strikes - 1, 0), 2)
        WHEN w.event_id = w.terminal_event_id THEN LEAST(a.target_strikes, 2)
        ELSE LEAST(COALESCE(SUM(a.event_strikes) OVER (
            PARTITION BY w.game_id, w.appearance_start_event_id
            ORDER BY w.event_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0), 2)
    END::UTINYINT AS completed_count_strikes,
    CASE
        WHEN w.event_id = w.terminal_event_id
          AND a.terminal_result IN ('Walk', 'IntentionalWalk')
          AND NOT a.is_automatic_intentional_walk
          AND w.count_balls IS DISTINCT FROM 3
            THEN 'derived_terminal_outcome_constraint'
        WHEN w.count_balls IS NOT NULL THEN 'observed_count'
        WHEN w.event_id = w.terminal_event_id THEN 'derived_terminal_outcome_constraint'
        ELSE 'derived_reconstructed_appearance_progress'
    END AS count_balls_method,
    CASE
        WHEN w.event_id = w.terminal_event_id
          AND a.terminal_result = 'StrikeOut'
          AND w.count_strikes IS DISTINCT FROM 2
            THEN 'derived_terminal_outcome_constraint'
        WHEN w.count_strikes IS NOT NULL THEN 'observed_count'
        WHEN w.event_id = w.terminal_event_id THEN 'derived_terminal_outcome_constraint'
        ELSE 'derived_reconstructed_appearance_progress'
    END AS count_strikes_method,
    w.source_resolution_status,
    w.raw_pitch_sequence,
    w.source_pitch_sequence,
    CASE
        WHEN w.source_resolution_status = 'Resolved'
            THEN COALESCE(w.completed_source_pitch_sequence, '')
        WHEN w.event_id = w.terminal_event_id
          AND a.terminal_result IN ('Walk', 'IntentionalWalk')
            THEN RTRIM(
                REPEAT('CalledStrike|', a.event_strikes)
                || REPEAT('Foul|', a.event_fouls)
                || REPEAT('Ball|', a.event_balls),
                '|'
            )
        WHEN w.event_id = w.terminal_event_id
          AND a.terminal_result = 'HitByPitch'
            THEN RTRIM(
                REPEAT('Ball|', GREATEST(a.event_balls - 1, 0))
                || REPEAT('CalledStrike|', a.event_strikes)
                || REPEAT('Foul|', a.event_fouls)
                || REPEAT('HitBatter|', CASE WHEN a.event_balls > 0 THEN 1 ELSE 0 END),
                '|'
            )
        WHEN w.event_id = w.terminal_event_id
          AND a.terminal_result = 'StrikeOut'
            THEN RTRIM(
                REPEAT('Ball|', a.event_balls)
                || REPEAT('CalledStrike|', GREATEST(a.event_strikes - 1, 0))
                || REPEAT('Foul|', a.event_fouls)
                || REPEAT('CalledStrike|', CASE WHEN a.event_strikes > 0 THEN 1 ELSE 0 END),
                '|'
            )
        WHEN w.event_id = w.terminal_event_id
          AND a.terminal_result IS NOT NULL
          AND a.terminal_result NOT IN (
              'Walk', 'IntentionalWalk', 'HitByPitch', 'Interference'
          )
            THEN RTRIM(
                REPEAT('Ball|', a.event_balls)
                || REPEAT('CalledStrike|', GREATEST(a.event_strikes - 1, 0))
                || REPEAT('Foul|', a.event_fouls)
                || REPEAT('InPlay|', CASE WHEN a.event_strikes > 0 THEN 1 ELSE 0 END),
                '|'
            )
        ELSE RTRIM(
            REPEAT('Ball|', a.event_balls - a.event_hit_batter)
            || REPEAT('HitBatter|', a.event_hit_batter)
            || REPEAT('CalledStrike|', a.event_strikes - a.event_in_play)
            || REPEAT('Foul|', a.event_fouls)
            || REPEAT('InPlay|', a.event_in_play),
            '|'
        )
    END AS completed_pitch_sequence,
    CASE
        WHEN w.source_resolution_status = 'Resolved'
          AND COALESCE(w.has_unknown_pitch_token, FALSE)
            THEN 'observed_incremental_with_declared_token_fallback'
        WHEN w.source_resolution_status = 'Resolved' THEN 'observed_incremental'
        WHEN a.is_automatic_intentional_walk THEN 'structural_automatic_intentional_walk'
        WHEN a.donor_count > 0 THEN 'count_boundary_with_empirical_total'
        ELSE 'count_boundary_with_broad_legal_fallback'
    END AS pitch_sequence_method,
    CASE
        WHEN w.source_resolution_status = 'Resolved'
          AND COALESCE(w.has_unknown_pitch_token, FALSE)
            THEN 'declared_count_and_terminal_constrained_token_fallback'
        WHEN w.source_resolution_status = 'Resolved' THEN 'source_tokens_preserved'
        WHEN a.event_fouls > 0 THEN 'legal_two_strike_foul_residual'
        ELSE 'legal_count_advancement_tokens'
    END AS pitch_token_completion_method,
    CASE
        WHEN w.source_resolution_status = 'Unresolved' THEN 'source_conflict_quarantined'
        WHEN a.has_count_regression THEN 'count_progression_contradiction'
        WHEN a.terminal_result = 'StrikeOut'
          AND a.terminal_count_strikes IS NOT NULL
          AND a.terminal_count_strikes <> 2
            THEN 'terminal_count_contradiction'
        WHEN a.terminal_result IN ('Walk', 'IntentionalWalk')
          AND NOT a.is_automatic_intentional_walk
          AND a.terminal_count_balls IS NOT NULL
          AND a.terminal_count_balls <> 3
            THEN 'terminal_count_contradiction'
        WHEN w.source_resolution_status = 'Resolved'
            THEN 'source_preserved_no_detected_transition_conflict'
        ELSE 'constructed_legal_transition_sequence'
    END AS constraint_status,
    CASE
        WHEN w.source_resolution_status = 'Resolved' THEN 'source_preserved_without_rewrite'
        WHEN w.source_resolution_status = 'Unresolved'
            THEN 'estimate_separate_from_quarantined_source'
        WHEN a.has_count_regression
            THEN 'estimate_separate_from_incompatible_count_progression'
        WHEN (
            a.terminal_result = 'StrikeOut'
            AND a.terminal_count_strikes IS NOT NULL
            AND a.terminal_count_strikes <> 2
        ) OR (
            a.terminal_result IN ('Walk', 'IntentionalWalk')
            AND NOT a.is_automatic_intentional_walk
            AND a.terminal_count_balls IS NOT NULL
            AND a.terminal_count_balls <> 3
        ) THEN 'estimate_separate_from_terminal_count_contradiction'
        ELSE 'estimated_respecting_count_boundaries'
    END AS constraint_disposition,
    a.donor_count,
    {source_counter_outputs},
    {completed_counter_outputs},
    {counter_method_outputs},
    '{MODEL_NAME}' AS model_name,
    '{MODEL_VERSION}' AS model_version,
    '{SOURCE_SIGNATURE}' AS source_signature,
    'exploratory' AS confidence_status
FROM windowed AS w
JOIN allocations_with_completion AS a USING (event_key)
),
completed_tokens AS (
    SELECT
        output.event_key,
        output.game_id,
        output.appearance_start_event_id,
        output.event_id,
        token.ordinality,
        token.item,
        token.item IN ('Ball', 'IntentionalBall', 'AutomaticBall', 'Pitchout')
            AS advances_ball,
        token.item IN (
            'CalledStrike', 'SwingingStrike', 'AutomaticStrike',
            'SwingingOnPitchout', 'FoulTip', 'FoulBunt', 'MissedBunt',
            'FoulTipBunt', 'Foul', 'FoulOnPitchout'
        ) AS advances_strike,
        token.item IN (
            'CalledStrike', 'SwingingStrike', 'AutomaticStrike',
            'SwingingOnPitchout', 'FoulTip', 'FoulBunt', 'MissedBunt', 'FoulTipBunt'
        ) AS can_strike_out,
        token.item IN ('InPlay', 'InPlayOnPitchout') AS in_play,
        token.item IN (
            'Ball', 'IntentionalBall', 'AutomaticBall', 'Pitchout', 'HitBatter',
            'CalledStrike', 'SwingingStrike', 'AutomaticStrike',
            'SwingingOnPitchout', 'FoulTip', 'FoulBunt', 'MissedBunt',
            'FoulTipBunt', 'Foul', 'FoulOnPitchout', 'InPlay', 'InPlayOnPitchout'
        ) AS is_pitch
    FROM completed_output AS output,
    UNNEST(STRING_SPLIT(output.completed_pitch_sequence, '|'))
        WITH ORDINALITY AS token(item, ordinality)
),
transition_states AS (
    SELECT
        *,
        COALESCE(SUM(advances_ball::INTEGER) OVER prior, 0) AS prior_balls,
        LEAST(COALESCE(SUM(advances_strike::INTEGER) OVER prior, 0), 2)
            AS prior_strikes,
        COUNT(*) FILTER (WHERE is_pitch) OVER following AS following_pitches,
        MAX(ordinality) FILTER (WHERE is_pitch) OVER (PARTITION BY event_key)
            AS last_pitch_ordinality
    FROM completed_tokens
    WINDOW
        prior AS (
            PARTITION BY game_id, appearance_start_event_id
            ORDER BY event_id, ordinality
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ),
        following AS (
            PARTITION BY game_id, appearance_start_event_id
            ORDER BY event_id, ordinality
            ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
        )
),
transition_results AS (
    SELECT
        *,
        CASE
            WHEN in_play THEN 'InPlay'
            WHEN item = 'HitBatter' THEN 'HitByPitch'
            WHEN advances_ball AND prior_balls >= 3 THEN 'Walk'
            WHEN can_strike_out AND prior_strikes >= 2 THEN 'StrikeOut'
        END AS terminal_pitch_result
    FROM transition_states
),
appearance_validation AS (
    SELECT
        output.game_id,
        output.appearance_start_event_id,
        BOOL_OR(
            state.terminal_pitch_result IS NOT NULL AND state.following_pitches > 0
        ) AS has_illegal_transition,
        BOOL_OR(
            state.ordinality = state.last_pitch_ordinality
            AND (
                (output.count_balls IS NOT NULL
                    AND output.count_balls <> state.prior_balls)
                OR (output.count_strikes IS NOT NULL
                    AND output.count_strikes <> state.prior_strikes)
            )
        ) AS has_boundary_conflict,
        FIRST(output.plate_appearance_result ORDER BY output.event_id DESC,
            state.ordinality DESC) AS final_result,
        FIRST(state.terminal_pitch_result ORDER BY output.event_id DESC,
            state.is_pitch DESC, state.ordinality DESC) AS final_pitch_result,
        MIN(output.season) AS season,
        SUM(state.is_pitch::INTEGER) AS appearance_pitches
    FROM completed_output AS output
    LEFT JOIN transition_results AS state USING (event_key)
    GROUP BY output.game_id, output.appearance_start_event_id
),
validated_appearances AS (
    SELECT *, final_result IS NOT NULL AND NOT (
            (final_result IN ('Walk', 'IntentionalWalk')
                AND final_pitch_result = 'Walk')
            OR (final_result = 'IntentionalWalk' AND season >= 2017
                AND appearance_pitches = 0)
            OR (final_result IN ('StrikeOut', 'HitByPitch')
                AND final_pitch_result = final_result)
            OR (final_result = 'Interference')
            OR (final_result NOT IN (
                'Walk', 'IntentionalWalk', 'StrikeOut', 'HitByPitch', 'Interference'
            ) AND final_pitch_result = 'InPlay')
        ) IS TRUE AS has_terminal_conflict
    FROM appearance_validation
)
SELECT output.* REPLACE (
    CASE
        WHEN validation.has_illegal_transition
            THEN 'completed_appearance_transition_conflict'
        WHEN validation.has_terminal_conflict THEN 'completed_appearance_terminal_conflict'
        WHEN output.constraint_status IN (
            'source_conflict_quarantined',
            'count_progression_contradiction', 'terminal_count_contradiction'
        ) THEN output.constraint_status
        WHEN validation.has_boundary_conflict THEN 'completed_count_boundary_conflict'
        ELSE output.constraint_status
    END AS constraint_status,
    CASE
        WHEN validation.has_illegal_transition OR validation.has_boundary_conflict
          OR validation.has_terminal_conflict
            THEN 'raw_source_retained_completed_appearance_conflict'
        ELSE output.constraint_disposition
    END AS constraint_disposition
)
FROM completed_output AS output
JOIN validated_appearances AS validation USING (game_id, appearance_start_event_id)
"""
