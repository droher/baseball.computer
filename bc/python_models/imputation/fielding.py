"""Row-preserving completion for unknown play-by-play fielding positions."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import ClassVar, Final

from pydantic import BaseModel, ConfigDict, model_validator

MODEL_NAME: Final = "pbp_completed_fielding_plays"
MODEL_VERSION: Final = "1.0.0"
SOURCE_SIGNATURE: Final = "pbp-fielding-current-source-v1"
INPUT_RELATIONS: Final = (
    "main_models.stg_event_fielding_plays",
    "main_models.stg_events",
    "main_models.event_personnel_lookup",
    "main_models.personnel_fielding_states",
    "main_models.imputed_fielding_credit",
    "main_models.official_aggregate_availability",
)

OUTPUT_SCHEMA: Final[Mapping[str, str]] = {
    "event_key": "UINTEGER",
    "sequence_id": "UTINYINT",
    "game_id": "VARCHAR",
    "event_id": "UTINYINT",
    "season": "SMALLINT",
    "fielding_play": "VARCHAR",
    "credit_type": "VARCHAR",
    "raw_fielding_position": "UTINYINT",
    "completed_fielding_position": "UTINYINT",
    "player_id": "VARCHAR",
    "completion_status": "VARCHAR",
    "completion_method": "VARCHAR",
    "constraint_disposition": "VARCHAR",
    "aggregate_constraint_target": "DOUBLE",
    "aggregate_constraint_assigned": "BIGINT",
    "aggregate_constraint_delta": "DOUBLE",
    "aggregate_constraint_complete": "BOOLEAN",
    "candidate_positions": "UTINYINT[]",
    "candidate_player_ids": "VARCHAR[]",
    "candidate_probabilities": "DOUBLE[]",
    "candidate_aggregate_capacities": "DOUBLE[]",
    "sampled_probability": "DOUBLE",
    "expected_credit": "DOUBLE",
    "distribution_ref": "VARCHAR",
    "model_name": "VARCHAR",
    "model_version": "VARCHAR",
    "source_signature": "VARCHAR",
    "confidence_status": "VARCHAR",
}

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?$")


class FieldingCompletionConfig(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    start_season: int = 1903
    end_season: int = 2025
    sample_games: int | None = None
    sample_seed: str = "pbp-fielding-completion-v1"
    plays_relation: str = "main_models.stg_event_fielding_plays"
    events_relation: str = "main_models.stg_events"
    personnel_lookup_relation: str = "main_models.event_personnel_lookup"
    personnel_states_relation: str = "main_models.personnel_fielding_states"
    imputed_credit_relation: str = "main_models.imputed_fielding_credit"
    aggregate_relation: str = "main_models.official_aggregate_availability"

    @model_validator(mode="after")
    def validate_config(self) -> FieldingCompletionConfig:
        if self.start_season > self.end_season:
            raise ValueError("start_season must not exceed end_season")
        if self.sample_games is not None and self.sample_games < 1:
            raise ValueError("sample_games must be positive")
        if not self.sample_seed:
            raise ValueError("sample_seed must not be empty")
        for relation in self.relations:
            if _IDENTIFIER.fullmatch(relation) is None:
                raise ValueError(f"invalid relation name: {relation}")
        return self

    @property
    def relations(self) -> tuple[str, ...]:
        return (
            self.plays_relation,
            self.events_relation,
            self.personnel_lookup_relation,
            self.personnel_states_relation,
            self.imputed_credit_relation,
            self.aggregate_relation,
        )


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sample_cte(config: FieldingCompletionConfig) -> str:
    if config.sample_games is None:
        return ""
    return f"""
, sampled_games AS (
    SELECT game_id
    FROM (SELECT DISTINCT game_id FROM source_rows
          WHERE season BETWEEN {config.start_season} AND {config.end_season})
    ORDER BY HASH({_literal(config.sample_seed)} || ':' || game_id)
    LIMIT {config.sample_games}
)
"""


def _sample_join(config: FieldingCompletionConfig) -> str:
    if config.sample_games is None:
        return ""
    return "INNER JOIN sampled_games AS sg ON sg.game_id = s.game_id"


def build_fielding_completion_sql(
    config: FieldingCompletionConfig | None = None,
) -> str:
    config = config or FieldingCompletionConfig()
    seed = _literal(config.sample_seed)
    sample_cte = _sample_cte(config)
    sample_join = _sample_join(config)
    return f"""
WITH position_support(fielding_position) AS (
    VALUES (1::UTINYINT), (2::UTINYINT), (3::UTINYINT), (4::UTINYINT), (5::UTINYINT),
           (6::UTINYINT), (7::UTINYINT), (8::UTINYINT), (9::UTINYINT)
),
source_rows AS (
    SELECT
        f.event_key,
        f.sequence_id,
        f.game_id,
        f.event_id,
        e.season,
        CAST(f.fielding_play AS VARCHAR) AS fielding_play,
        f.fielding_position AS raw_fielding_position,
        COALESCE(CAST(e.plate_appearance_result AS VARCHAR), 'missing') AS result,
        COALESCE(CAST(e.batted_trajectory AS VARCHAR), 'missing') AS trajectory,
        CASE WHEN e.season < 1920 THEN 'pre1920'
             ELSE CAST(CAST(FLOOR(e.season / 10) * 10 AS INTEGER) AS VARCHAR) END AS era
    FROM {config.plays_relation} AS f
    INNER JOIN {config.events_relation} AS e USING (event_key)
    WHERE e.season BETWEEN 1903 AND 2025
)
{sample_cte}, target_rows AS (
    SELECT s.*
    FROM source_rows AS s
    {sample_join}
    WHERE s.season BETWEEN {config.start_season} AND {config.end_season}
),
contexts AS (
    SELECT DISTINCT era, fielding_play, result, trajectory FROM source_rows
),
position_exact AS (
    SELECT era, fielding_play, result, trajectory, raw_fielding_position AS fielding_position,
           COUNT(*)::DOUBLE AS n
    FROM source_rows
    WHERE raw_fielding_position BETWEEN 1 AND 9
    GROUP BY ALL
),
position_global AS (
    SELECT p.fielding_position, COALESCE(COUNT(s.raw_fielding_position), 0)::DOUBLE AS n
    FROM position_support AS p
    LEFT JOIN source_rows AS s ON s.raw_fielding_position = p.fielding_position
    GROUP BY p.fielding_position
),
position_weights AS (
    SELECT
        c.era, c.fielding_play, c.result, c.trajectory, p.fielding_position,
        COALESCE(x.n, 0) + 12.0 * (p.n + 0.5)
          / SUM(p.n + 0.5) OVER (
              PARTITION BY c.era, c.fielding_play, c.result, c.trajectory
            ) AS weight
    FROM contexts AS c
    CROSS JOIN position_global AS p
    LEFT JOIN position_exact AS x
      ON x.era = c.era AND x.fielding_play = c.fielding_play
     AND x.result = c.result AND x.trajectory = c.trajectory
     AND x.fielding_position = p.fielding_position
),
event_personnel AS (
    SELECT
        e.event_key,
        p.player_id,
        p.fielding_position
    FROM {config.personnel_lookup_relation} AS e
    INNER JOIN {config.personnel_states_relation} AS p
      ON p.game_id = e.game_id AND p.personnel_fielding_key = e.personnel_fielding_key
    WHERE p.fielding_position BETWEEN 1 AND 9
),
known_credit_rows AS (
    SELECT
        s.game_id,
        s.event_key,
        s.sequence_id,
        s.raw_fielding_position AS fielding_position,
        ep.player_id,
        CASE WHEN s.fielding_play = 'Assist' THEN 'assist' ELSE 'putout' END AS credit_type
    FROM source_rows AS s
    LEFT JOIN event_personnel AS ep
      ON ep.event_key = s.event_key
     AND ep.fielding_position = s.raw_fielding_position
    WHERE s.raw_fielding_position BETWEEN 1 AND 9
      AND s.fielding_play IN ('Putout', 'Assist')
),
known_credit_counts AS (
    SELECT game_id, player_id, fielding_position, credit_type, COUNT(*)::DOUBLE AS n
    FROM known_credit_rows
    WHERE player_id IS NOT NULL
    GROUP BY ALL
),
known_credit_baseline AS (
    SELECT
        game_id,
        credit_type,
        COUNT(*) AS raw_known_credits,
        COUNT(player_id) AS resolved_known_credits
    FROM known_credit_rows
    GROUP BY ALL
),
candidate_base AS (
    SELECT
        t.event_key,
        t.sequence_id,
        t.game_id,
        t.fielding_play,
        p.fielding_position,
        ep.player_id,
        w.weight AS empirical_weight,
        i.expected_share AS artifact_weight,
        a.aggregate_value - COALESCE(k.n, 0.0) AS allocation_capacity,
        a.aggregate_status,
        COALESCE(b.raw_known_credits, 0) = COALESCE(b.resolved_known_credits, 0)
          AS raw_baseline_complete,
        COUNT(ep.player_id) OVER (PARTITION BY t.event_key, t.sequence_id) AS personnel_candidates
    FROM target_rows AS t
    CROSS JOIN position_support AS p
    LEFT JOIN event_personnel AS ep
      ON ep.event_key = t.event_key AND ep.fielding_position = p.fielding_position
    INNER JOIN position_weights AS w
      ON w.era = t.era AND w.fielding_play = t.fielding_play
     AND w.result = t.result AND w.trajectory = t.trajectory
     AND w.fielding_position = p.fielding_position
    LEFT JOIN {config.imputed_credit_relation} AS i
      ON i.event_key = t.event_key AND i.fielding_position = p.fielding_position
     AND i.player_id = ep.player_id
     AND i.credit_type = CASE
         WHEN t.fielding_play = 'Assist' THEN 'assist'
         WHEN t.fielding_play = 'Putout' THEN 'putout'
         ELSE 'putout'
     END
    LEFT JOIN {config.aggregate_relation} AS a
      ON a.game_id = t.game_id AND a.player_id = ep.player_id
     AND a.fielding_position = p.fielding_position
     AND a.stat_name = CASE
         WHEN t.fielding_play = 'Assist' THEN 'assists'
         WHEN t.fielding_play = 'Putout' THEN 'putouts'
         ELSE NULL
     END
    LEFT JOIN known_credit_counts AS k
      ON k.game_id = t.game_id AND k.player_id = ep.player_id
     AND k.fielding_position = p.fielding_position
     AND k.credit_type = CASE WHEN t.fielding_play = 'Assist' THEN 'assist' ELSE 'putout' END
    LEFT JOIN known_credit_baseline AS b
      ON b.game_id = t.game_id
     AND b.credit_type = CASE WHEN t.fielding_play = 'Assist' THEN 'assist' ELSE 'putout' END
    WHERE t.raw_fielding_position = 0
),
candidate_flags AS (
    SELECT
        *,
        BOOL_OR(allocation_capacity > 0 AND aggregate_status IN ('present_clean', 'contradicted'))
          OVER (PARTITION BY event_key, sequence_id) AS has_positive_constraint,
        BOOL_OR(aggregate_status IS NOT NULL)
          OVER (PARTITION BY event_key, sequence_id) AS has_any_constraint,
        BOOL_OR(aggregate_status IS NOT NULL)
          OVER (PARTITION BY event_key, sequence_id) AND BOOL_AND(
            CASE
                WHEN player_id IS NULL THEN TRUE
                ELSE raw_baseline_complete
                     AND aggregate_status IN ('present_clean', 'contradicted')
                     AND allocation_capacity >= 0
                     AND allocation_capacity = FLOOR(allocation_capacity)
            END
        ) OVER (PARTITION BY event_key, sequence_id) AS has_complete_constraint,
        BOOL_OR(
            aggregate_status IN ('negative_residual', 'present_issue_flagged')
            OR allocation_capacity < 0
        )
          OVER (PARTITION BY event_key, sequence_id) AS has_contradicted_constraint,
        BOOL_OR(artifact_weight IS NOT NULL)
          OVER (PARTITION BY event_key, sequence_id) AS has_artifact
    FROM candidate_base
),
candidate_weighted AS (
    SELECT
        *,
        CASE
            WHEN personnel_candidates > 0 AND player_id IS NULL THEN 0.0
            WHEN has_complete_constraint AND has_positive_constraint
              AND NOT (
                  allocation_capacity > 0
                  AND aggregate_status IN ('present_clean', 'contradicted')
              ) THEN 0.0
            ELSE COALESCE(artifact_weight, empirical_weight)
                 * CASE WHEN has_complete_constraint AND has_positive_constraint
                        THEN allocation_capacity ELSE 1.0 END
        END AS constrained_weight
    FROM candidate_flags
),
candidate_probabilities AS (
    SELECT
        *,
        constrained_weight / NULLIF(
            SUM(constrained_weight) OVER (PARTITION BY event_key, sequence_id), 0
        ) AS probability
    FROM candidate_weighted
),
candidate_distribution AS (
    SELECT
        event_key,
        sequence_id,
        game_id,
        fielding_play,
        fielding_position,
        player_id,
        probability,
        personnel_candidates,
        has_positive_constraint,
        has_any_constraint,
        has_complete_constraint,
        has_contradicted_constraint,
        has_artifact,
        allocation_capacity,
        SUM(probability) OVER (
            PARTITION BY event_key, sequence_id ORDER BY fielding_position
        ) AS cumulative_probability,
        LIST(fielding_position ORDER BY fielding_position) FILTER (WHERE probability > 0)
          OVER (PARTITION BY event_key, sequence_id) AS candidate_positions,
        LIST(player_id ORDER BY fielding_position) FILTER (WHERE probability > 0)
          OVER (PARTITION BY event_key, sequence_id) AS candidate_player_ids,
        LIST(probability ORDER BY fielding_position) FILTER (WHERE probability > 0)
          OVER (PARTITION BY event_key, sequence_id) AS candidate_probabilities,
        LIST(allocation_capacity ORDER BY fielding_position) FILTER (WHERE probability > 0)
          OVER (PARTITION BY event_key, sequence_id) AS candidate_aggregate_capacities
    FROM candidate_probabilities
    WHERE probability > 0
),
sampled AS (
    SELECT *
    FROM candidate_distribution
    WHERE cumulative_probability >= (
        HASH({seed} || ':' || CAST(event_key AS VARCHAR) || ':' || CAST(sequence_id AS VARCHAR)) % 1000000
    ) / 1000000.0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event_key, sequence_id ORDER BY cumulative_probability
    ) = 1
),
known_players AS (
    SELECT t.event_key, t.sequence_id, ep.player_id
    FROM target_rows AS t
    LEFT JOIN event_personnel AS ep
      ON ep.event_key = t.event_key AND ep.fielding_position = t.raw_fielding_position
    WHERE t.raw_fielding_position BETWEEN 1 AND 9
)
SELECT
    t.event_key,
    t.sequence_id,
    t.game_id,
    t.event_id,
    t.season,
    t.fielding_play,
    CASE
        WHEN t.fielding_play = 'Assist' THEN 'assist'
        WHEN t.fielding_play = 'Putout' THEN 'putout'
        ELSE 'fielders_choice'
    END AS credit_type,
    t.raw_fielding_position,
    CASE WHEN t.raw_fielding_position BETWEEN 1 AND 9 THEN t.raw_fielding_position
         ELSE s.fielding_position END AS completed_fielding_position,
    CASE WHEN t.raw_fielding_position BETWEEN 1 AND 9 THEN k.player_id ELSE s.player_id END AS player_id,
    CASE WHEN t.raw_fielding_position BETWEEN 1 AND 9 THEN 'observed' ELSE 'estimated' END AS completion_status,
    CASE
        WHEN t.raw_fielding_position BETWEEN 1 AND 9 THEN 'source_raw'
        WHEN s.has_artifact THEN 'current_imputed_credit_distribution'
        ELSE 'partially_pooled_empirical_distribution'
    END AS completion_method,
    CASE
        WHEN t.raw_fielding_position BETWEEN 1 AND 9 THEN 'not_applicable_recorded'
        WHEN s.personnel_candidates = 0 THEN 'no_personnel_candidates_broad_prior'
        WHEN s.has_contradicted_constraint THEN 'aggregate_constraint_contradicted'
        WHEN s.has_complete_constraint THEN 'aggregate_capacity_pending_assignment'
        WHEN s.has_any_constraint THEN 'aggregate_constraint_partial'
        ELSE 'no_compatible_aggregate_constraint'
    END AS constraint_disposition,
    CASE WHEN t.raw_fielding_position = 0 AND s.has_complete_constraint
         THEN s.allocation_capacity ELSE NULL END AS aggregate_constraint_target,
    NULL::BIGINT AS aggregate_constraint_assigned,
    NULL::DOUBLE AS aggregate_constraint_delta,
    CASE WHEN t.raw_fielding_position = 0 THEN s.has_complete_constraint ELSE FALSE END
      AS aggregate_constraint_complete,
    CASE WHEN t.raw_fielding_position = 0 THEN s.candidate_positions ELSE NULL END AS candidate_positions,
    CASE WHEN t.raw_fielding_position = 0 THEN s.candidate_player_ids ELSE NULL END AS candidate_player_ids,
    CASE WHEN t.raw_fielding_position = 0 THEN s.candidate_probabilities ELSE NULL END AS candidate_probabilities,
    CASE WHEN t.raw_fielding_position = 0 THEN s.candidate_aggregate_capacities ELSE NULL END
      AS candidate_aggregate_capacities,
    CASE WHEN t.raw_fielding_position = 0 THEN s.probability ELSE 1.0 END AS sampled_probability,
    CASE WHEN t.raw_fielding_position = 0 THEN s.probability ELSE 1.0 END AS expected_credit,
    CASE WHEN t.raw_fielding_position = 0
         THEN 'fielding:' || t.era || ':' || t.fielding_play || ':' || t.result || ':' || t.trajectory
         ELSE NULL END AS distribution_ref,
    '{MODEL_NAME}' AS model_name,
    '{MODEL_VERSION}' AS model_version,
    '{SOURCE_SIGNATURE}' AS source_signature,
    'exploratory' AS confidence_status
FROM target_rows AS t
LEFT JOIN sampled AS s USING (event_key, sequence_id)
LEFT JOIN known_players AS k USING (event_key, sequence_id)
""".strip()
