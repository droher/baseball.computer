"""Event-wide exploratory completion for play-by-play batted-ball geometry."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import ClassVar, Final

from pydantic import BaseModel, ConfigDict, model_validator

MODEL_NAME: Final = "pbp_completed_geometry"
MODEL_VERSION: Final = "1.0.0"
SOURCE_SIGNATURE: Final = "pbp-geometry-current-source-v1"
INPUT_RELATIONS: Final = (
    "main_models.stg_events",
    "main_models.event_observation_context",
    "main_models.calc_batted_ball_type",
    "main_models.event_offense_stats",
    "main_models.air_trajectory_translation",
    "main_seeds.seed_hit_location_categories",
    "main_seeds.seed_hit_location_distance_buckets",
    "main_seeds.seed_hit_location_angles",
    "main_seeds.seed_hit_to_fielder_categories",
)

OUTPUT_SCHEMA: Final[Mapping[str, str]] = {
    "event_key": "UINTEGER",
    "game_id": "VARCHAR",
    "season": "SMALLINT",
    "result_family": "VARCHAR",
    "batter_hand": "VARCHAR",
    "is_bunt": "BOOLEAN",
    "raw_trajectory": "VARCHAR",
    "trajectory": "VARCHAR",
    "trajectory_status": "VARCHAR",
    "trajectory_method": "VARCHAR",
    "trajectory_distribution_ref": "VARCHAR",
    "trajectory_distribution_classes": "VARCHAR[]",
    "trajectory_distribution_probabilities": "DOUBLE[]",
    "trajectory_sample_probability": "DOUBLE",
    "raw_general_location": "VARCHAR",
    "general_location": "VARCHAR",
    "general_location_status": "VARCHAR",
    "general_location_method": "VARCHAR",
    "location_distribution_ref": "VARCHAR",
    "location_distribution_classes": "VARCHAR[]",
    "location_distribution_probabilities": "DOUBLE[]",
    "location_sample_probability": "DOUBLE",
    "location_side": "VARCHAR",
    "location_depth": "VARCHAR",
    "location_edge": "VARCHAR",
    "location_taxonomy_status": "VARCHAR",
    "location_taxonomy_method": "VARCHAR",
    "raw_location_depth_modifier": "VARCHAR",
    "location_depth_modifier": "VARCHAR",
    "location_depth_modifier_status": "VARCHAR",
    "location_depth_modifier_method": "VARCHAR",
    "raw_location_angle": "VARCHAR",
    "location_angle": "VARCHAR",
    "location_angle_status": "VARCHAR",
    "location_angle_method": "VARCHAR",
    "raw_contact_strength": "VARCHAR",
    "contact_strength": "VARCHAR",
    "contact_strength_status": "VARCHAR",
    "contact_strength_method": "VARCHAR",
    "raw_handler_position": "UTINYINT",
    "handler_position": "UTINYINT",
    "handler_position_status": "VARCHAR",
    "handler_position_method": "VARCHAR",
    "standardized_trajectory": "VARCHAR",
    "standardized_trajectory_status": "VARCHAR",
    "standardized_trajectory_method": "VARCHAR",
    "standardized_trajectory_basis": "VARCHAR",
    "standardized_trajectory_weakly_identified": "BOOLEAN",
    "p_standardized_ground_ball": "DOUBLE",
    "p_standardized_fly": "DOUBLE",
    "p_standardized_line_drive": "DOUBLE",
    "p_standardized_pop_up": "DOUBLE",
    "model_name": "VARCHAR",
    "model_version": "VARCHAR",
    "source_signature": "VARCHAR",
    "confidence_status": "VARCHAR",
}

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?$")


class GeometryCompletionConfig(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    start_season: int = 1903
    end_season: int = 2025
    sample_games: int | None = None
    sample_seed: str = "pbp-geometry-completion-v1"
    events_relation: str = "main_models.stg_events"
    context_relation: str = "main_models.event_observation_context"
    batted_ball_relation: str = "main_models.calc_batted_ball_type"
    offense_relation: str = "main_models.event_offense_stats"
    air_translation_relation: str = "main_models.air_trajectory_translation"
    location_categories_relation: str = "main_seeds.seed_hit_location_categories"
    location_depths_relation: str = "main_seeds.seed_hit_location_distance_buckets"
    location_angles_relation: str = "main_seeds.seed_hit_location_angles"
    handler_categories_relation: str = "main_seeds.seed_hit_to_fielder_categories"

    @model_validator(mode="after")
    def validate_config(self) -> GeometryCompletionConfig:
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
            self.events_relation,
            self.context_relation,
            self.batted_ball_relation,
            self.offense_relation,
            self.air_translation_relation,
            self.location_categories_relation,
            self.location_depths_relation,
            self.location_angles_relation,
            self.handler_categories_relation,
        )


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _game_filter(config: GeometryCompletionConfig) -> str:
    if config.sample_games is None:
        return ""
    seed = _literal(config.sample_seed)
    return f"""
, sampled_games AS (
    SELECT game_id
    FROM (SELECT DISTINCT game_id FROM donor_base
          WHERE season BETWEEN {config.start_season} AND {config.end_season})
    ORDER BY HASH({seed} || ':' || game_id)
    LIMIT {config.sample_games}
)
"""


def _sample_join(config: GeometryCompletionConfig) -> str:
    if config.sample_games is None:
        return ""
    return "INNER JOIN sampled_games AS sg ON sg.game_id = d.game_id"


def build_geometry_completion_sql(
    config: GeometryCompletionConfig | None = None,
) -> str:
    config = config or GeometryCompletionConfig()
    seed = _literal(config.sample_seed)
    game_filter = _game_filter(config)
    sample_join = _sample_join(config)
    return f"""
WITH location_support AS (
    SELECT
        CAST(batted_location_general AS VARCHAR) AS class,
        category_depth,
        category_side,
        category_edge
    FROM {config.location_categories_relation}
),
depth_support AS (
    SELECT DISTINCT general_location, depth AS class
    FROM {config.location_depths_relation}
),
angle_support AS (
    SELECT DISTINCT general_location, angle AS class
    FROM {config.location_angles_relation}
),
handler_support AS (
    SELECT
        CAST(batted_to_fielder AS UTINYINT) AS class,
        category_depth,
        category_side
    FROM {config.handler_categories_relation}
),
trajectory_support(class) AS (
    VALUES ('GroundBall'), ('Fly'), ('LineDrive'), ('PopUp')
),
strength_support(class) AS (
    VALUES ('Hard'), ('Soft')
),
donor_base AS (
    SELECT
        e.event_key,
        e.game_id,
        e.season,
        COALESCE(c.result_family, 'missing') AS result_family,
        COALESCE(CAST(c.batter_hand AS VARCHAR), 'Unknown') AS batter_hand,
        CASE
            WHEN CAST(e.batted_trajectory AS VARCHAR) LIKE '%Bunt' OR COALESCE(o.is_bunt, FALSE) THEN TRUE
            ELSE FALSE
        END AS is_bunt,
        CASE
            WHEN e.season < 1920 THEN 'pre1920'
            ELSE CAST(CAST(FLOOR(e.season / 10) * 10 AS INTEGER) AS VARCHAR)
        END AS era,
        CAST(e.batted_trajectory AS VARCHAR) AS raw_trajectory,
        CAST(b.trajectory AS VARCHAR) AS deduced_trajectory,
        CAST(b.trajectory_broad_classification AS VARCHAR) AS deduced_trajectory_broad,
        CAST(b.location_side AS VARCHAR) AS deduced_location_side,
        CAST(b.location_depth AS VARCHAR) AS deduced_location_depth,
        CAST(e.batted_location_general AS VARCHAR) AS raw_general_location,
        CAST(e.batted_location_depth AS VARCHAR) AS raw_location_depth_modifier,
        CAST(e.batted_location_angle AS VARCHAR) AS raw_location_angle,
        e.batted_contact_strength AS raw_contact_strength,
        e.batted_to_fielder AS raw_handler_position,
        hc.category_side AS raw_handler_side
    FROM {config.events_relation} AS e
    LEFT JOIN {config.context_relation} AS c USING (event_key)
    LEFT JOIN {config.batted_ball_relation} AS b USING (event_key)
    LEFT JOIN (
        SELECT event_key, BOOL_OR(bunts > 0) AS is_bunt
        FROM {config.offense_relation}
        WHERE CAST(baserunner AS VARCHAR) = 'Batter'
        GROUP BY event_key
    ) AS o USING (event_key)
    LEFT JOIN {config.handler_categories_relation} AS hc
      ON hc.batted_to_fielder = e.batted_to_fielder
    WHERE e.season BETWEEN 1903 AND 2025
      AND e.batted_trajectory IS NOT NULL
      AND (c.event_key IS NULL OR (
          c.source_family = 'play_by_play'
          AND c.target_population_status = 'event_level'
      ))
)
{game_filter}, base AS (
    SELECT d.*
    FROM donor_base AS d
    {sample_join}
    WHERE d.season BETWEEN {config.start_season} AND {config.end_season}
),
contexts AS (
    SELECT DISTINCT era, result_family, is_bunt, batter_hand FROM donor_base
),
trajectory_exact AS (
    SELECT era, result_family, is_bunt, batter_hand, raw_trajectory AS class, COUNT(*)::DOUBLE AS n
    FROM donor_base
    WHERE raw_trajectory IN ('GroundBall', 'Fly', 'LineDrive', 'PopUp')
    GROUP BY ALL
),
trajectory_global AS (
    SELECT s.class, COALESCE(COUNT(b.raw_trajectory), 0)::DOUBLE AS n
    FROM trajectory_support AS s
    LEFT JOIN donor_base AS b ON b.raw_trajectory = s.class
    GROUP BY s.class
),
trajectory_weights AS (
    SELECT
        c.era, c.result_family, c.is_bunt, c.batter_hand, g.class,
        COALESCE(x.n, 0) + 8.0 * (g.n + 0.5)
            / SUM(g.n + 0.5) OVER (PARTITION BY c.era, c.result_family, c.is_bunt, c.batter_hand) AS weight
    FROM contexts AS c
    CROSS JOIN trajectory_global AS g
    LEFT JOIN trajectory_exact AS x
      ON x.era = c.era AND x.result_family = c.result_family
     AND x.is_bunt = c.is_bunt AND x.batter_hand = c.batter_hand AND x.class = g.class
),
location_exact AS (
    SELECT era, result_family, is_bunt, batter_hand, raw_general_location AS class, COUNT(*)::DOUBLE AS n
    FROM donor_base
    WHERE raw_general_location IS NOT NULL AND raw_general_location != 'Unknown'
    GROUP BY ALL
),
location_global AS (
    SELECT s.class, COALESCE(COUNT(b.raw_general_location), 0)::DOUBLE AS n
    FROM location_support AS s
    LEFT JOIN donor_base AS b ON b.raw_general_location = s.class
    GROUP BY s.class
),
location_weights AS (
    SELECT
        c.era, c.result_family, c.is_bunt, c.batter_hand, s.class,
        s.category_depth, s.category_side, s.category_edge,
        COALESCE(x.n, 0) + 12.0 * (g.n + 0.5)
            / SUM(g.n + 0.5) OVER (PARTITION BY c.era, c.result_family, c.is_bunt, c.batter_hand) AS weight
    FROM contexts AS c
    CROSS JOIN location_support AS s
    INNER JOIN location_global AS g USING (class)
    LEFT JOIN location_exact AS x
      ON x.era = c.era AND x.result_family = c.result_family
     AND x.is_bunt = c.is_bunt AND x.batter_hand = c.batter_hand AND x.class = s.class
),
depth_exact AS (
    SELECT era, result_family, is_bunt, batter_hand, raw_general_location AS general_location,
           raw_location_depth_modifier AS class, COUNT(*)::DOUBLE AS n
    FROM donor_base
    WHERE raw_general_location IS NOT NULL AND raw_general_location != 'Unknown'
      AND raw_location_depth_modifier IN ('Shallow', 'Deep', 'ExtraDeep')
    GROUP BY ALL
),
depth_global AS (
    SELECT s.general_location, s.class, COALESCE(COUNT(b.raw_location_depth_modifier), 0)::DOUBLE AS n
    FROM depth_support AS s
    LEFT JOIN donor_base AS b ON b.raw_general_location = s.general_location
                       AND b.raw_location_depth_modifier = s.class
                       AND b.raw_location_depth_modifier != 'Default'
    GROUP BY s.general_location, s.class
),
depth_weights AS (
    SELECT
        c.era, c.result_family, c.is_bunt, c.batter_hand, g.general_location, g.class,
        COALESCE(x.n, 0) + 6.0 * (g.n + 0.5)
            / SUM(g.n + 0.5) OVER (
                PARTITION BY c.era, c.result_family, c.is_bunt, c.batter_hand, g.general_location
              ) AS weight
    FROM contexts AS c
    CROSS JOIN depth_global AS g
    LEFT JOIN depth_exact AS x
      ON x.era = c.era AND x.result_family = c.result_family
     AND x.is_bunt = c.is_bunt AND x.batter_hand = c.batter_hand
     AND x.general_location = g.general_location AND x.class = g.class
),
angle_exact AS (
    SELECT era, result_family, is_bunt, batter_hand, raw_general_location AS general_location,
           raw_location_angle AS class, COUNT(*)::DOUBLE AS n
    FROM donor_base
    WHERE raw_general_location IS NOT NULL AND raw_general_location != 'Unknown'
      AND raw_location_angle IN ('Foul', 'FoulLine', 'Left', 'Middle', 'Right')
    GROUP BY ALL
),
angle_global AS (
    SELECT s.general_location, s.class, COALESCE(COUNT(b.raw_location_angle), 0)::DOUBLE AS n
    FROM angle_support AS s
    LEFT JOIN donor_base AS b
      ON b.raw_general_location = s.general_location AND b.raw_location_angle = s.class
     AND b.raw_location_angle != 'Default'
    GROUP BY s.general_location, s.class
),
angle_weights AS (
    SELECT
        c.era, c.result_family, c.is_bunt, c.batter_hand, g.general_location, g.class,
        COALESCE(x.n, 0) + 6.0 * (g.n + 0.5)
            / SUM(g.n + 0.5) OVER (
                PARTITION BY c.era, c.result_family, c.is_bunt, c.batter_hand, g.general_location
              ) AS weight
    FROM contexts AS c
    CROSS JOIN angle_global AS g
    LEFT JOIN angle_exact AS x
      ON x.era = c.era AND x.result_family = c.result_family
     AND x.is_bunt = c.is_bunt AND x.batter_hand = c.batter_hand
     AND x.general_location = g.general_location AND x.class = g.class
),
strength_exact AS (
    SELECT era, result_family, is_bunt, batter_hand, raw_contact_strength AS class, COUNT(*)::DOUBLE AS n
    FROM donor_base
    WHERE raw_contact_strength IN ('Hard', 'Soft')
    GROUP BY ALL
),
strength_global AS (
    SELECT s.class, COALESCE(COUNT(b.raw_contact_strength), 0)::DOUBLE AS n
    FROM strength_support AS s
    LEFT JOIN donor_base AS b ON b.raw_contact_strength = s.class
    GROUP BY s.class
),
strength_weights AS (
    SELECT
        c.era, c.result_family, c.is_bunt, c.batter_hand, g.class,
        COALESCE(x.n, 0) + 5.0 * (g.n + 0.5)
            / SUM(g.n + 0.5) OVER (PARTITION BY c.era, c.result_family, c.is_bunt, c.batter_hand) AS weight
    FROM contexts AS c
    CROSS JOIN strength_global AS g
    LEFT JOIN strength_exact AS x
      ON x.era = c.era AND x.result_family = c.result_family
     AND x.is_bunt = c.is_bunt AND x.batter_hand = c.batter_hand AND x.class = g.class
),
handler_exact AS (
    SELECT era, result_family, is_bunt, batter_hand, raw_handler_position AS class, COUNT(*)::DOUBLE AS n
    FROM donor_base
    WHERE raw_handler_position BETWEEN 1 AND 9
    GROUP BY ALL
),
handler_global AS (
    SELECT s.class, COALESCE(COUNT(b.raw_handler_position), 0)::DOUBLE AS n
    FROM handler_support AS s
    LEFT JOIN donor_base AS b ON b.raw_handler_position = s.class
    GROUP BY s.class
),
handler_weights AS (
    SELECT
        c.era, c.result_family, c.is_bunt, c.batter_hand, s.class,
        s.category_depth, s.category_side,
        COALESCE(x.n, 0) + 8.0 * (g.n + 0.5)
            / SUM(g.n + 0.5) OVER (PARTITION BY c.era, c.result_family, c.is_bunt, c.batter_hand) AS weight
    FROM contexts AS c
    CROSS JOIN handler_support AS s
    INNER JOIN handler_global AS g USING (class)
    LEFT JOIN handler_exact AS x
      ON x.era = c.era AND x.result_family = c.result_family
     AND x.is_bunt = c.is_bunt AND x.batter_hand = c.batter_hand AND x.class = s.class
),
trajectory_completed AS (
    SELECT
        b.*,
        CASE
            WHEN b.raw_trajectory != 'Unknown' THEN b.raw_trajectory
            WHEN b.is_bunt THEN 'UnspecifiedBunt'
            WHEN b.deduced_trajectory IS NOT NULL AND b.deduced_trajectory != 'Unknown' THEN b.deduced_trajectory
            ELSE sampled.class
        END AS trajectory,
        CASE
            WHEN b.raw_trajectory != 'Unknown' THEN 'observed'
            WHEN b.is_bunt THEN 'derived'
            WHEN b.deduced_trajectory IS NOT NULL AND b.deduced_trajectory != 'Unknown' THEN 'derived'
            ELSE 'estimated'
        END AS trajectory_status,
        CASE
            WHEN b.raw_trajectory != 'Unknown' THEN 'source_raw'
            WHEN b.is_bunt THEN 'bunt_indicator_constraint'
            WHEN b.deduced_trajectory IS NOT NULL AND b.deduced_trajectory != 'Unknown' THEN 'deterministic_deduction'
            ELSE 'partially_pooled_empirical_draw'
        END AS trajectory_method,
        CASE WHEN b.raw_trajectory = 'Unknown'
             AND (b.deduced_trajectory IS NULL OR b.deduced_trajectory = 'Unknown')
             THEN 'trajectory:' || b.era || ':' || b.result_family || ':' || CAST(b.is_bunt AS VARCHAR)
                  || ':' || b.batter_hand ELSE NULL END AS trajectory_distribution_ref,
        CASE WHEN b.raw_trajectory = 'Unknown' AND NOT b.is_bunt
             AND (b.deduced_trajectory IS NULL OR b.deduced_trajectory = 'Unknown')
             THEN sampled.classes ELSE NULL END AS trajectory_distribution_classes,
        CASE WHEN b.raw_trajectory = 'Unknown' AND NOT b.is_bunt
             AND (b.deduced_trajectory IS NULL OR b.deduced_trajectory = 'Unknown')
             THEN sampled.probabilities ELSE NULL END AS trajectory_distribution_probabilities,
        CASE WHEN b.raw_trajectory = 'Unknown' AND NOT b.is_bunt
             AND (b.deduced_trajectory IS NULL OR b.deduced_trajectory = 'Unknown')
             THEN sampled.probability ELSE 1.0 END AS trajectory_sample_probability
    FROM base AS b
    LEFT JOIN LATERAL (
        SELECT class, probability, classes, probabilities
        FROM (
            SELECT
                class,
                probability,
                SUM(probability) OVER (ORDER BY class) AS cumulative_probability,
                LIST(class ORDER BY class) OVER () AS classes,
                LIST(probability ORDER BY class) OVER () AS probabilities
            FROM (
                SELECT class, weight / SUM(weight) OVER () AS probability
                FROM trajectory_weights AS w
                WHERE w.era = b.era AND w.result_family = b.result_family
                  AND w.is_bunt = b.is_bunt AND w.batter_hand = b.batter_hand
                  AND (b.deduced_trajectory_broad IS NULL OR b.deduced_trajectory_broad != 'AirBall'
                       OR class != 'GroundBall')
            )
        )
        WHERE cumulative_probability >= (HASH({seed} || ':' || CAST(b.event_key AS VARCHAR) || ':trajectory') % 1000000) / 1000000.0
        ORDER BY cumulative_probability
        LIMIT 1
    ) AS sampled ON TRUE
),
location_completed AS (
    SELECT
        t.*,
        COALESCE(NULLIF(t.raw_general_location, 'Unknown'), sampled.class, fallback.class) AS general_location,
        CASE WHEN t.raw_general_location IS NOT NULL AND t.raw_general_location != 'Unknown'
             THEN 'observed' ELSE 'estimated' END AS general_location_status,
        CASE WHEN t.raw_general_location IS NOT NULL AND t.raw_general_location != 'Unknown'
             THEN 'source_raw'
             WHEN sampled.class IS NOT NULL THEN 'joint_partially_pooled_empirical_draw'
             ELSE 'constraint_conflict_empirical_draw' END AS general_location_method,
        CASE WHEN t.raw_general_location IS NULL OR t.raw_general_location = 'Unknown'
             THEN 'joint_location:' || t.era || ':' || t.result_family || ':' || CAST(t.is_bunt AS VARCHAR)
                  || ':' || t.batter_hand ELSE NULL END AS location_distribution_ref,
        CASE WHEN t.raw_general_location IS NULL OR t.raw_general_location = 'Unknown'
             THEN COALESCE(sampled.classes, fallback.classes) ELSE NULL END AS location_distribution_classes,
        CASE WHEN t.raw_general_location IS NULL OR t.raw_general_location = 'Unknown'
             THEN COALESCE(sampled.probabilities, fallback.probabilities) ELSE NULL END AS location_distribution_probabilities,
        CASE WHEN t.raw_general_location IS NULL OR t.raw_general_location = 'Unknown'
             THEN COALESCE(sampled.probability, fallback.probability) ELSE 1.0 END AS location_sample_probability
    FROM trajectory_completed AS t
    LEFT JOIN LATERAL (
        SELECT class, probability, classes, probabilities
        FROM (
            SELECT
                class, probability,
                SUM(probability) OVER (ORDER BY class) AS cumulative_probability,
                LIST(class ORDER BY class) OVER () AS classes,
                LIST(probability ORDER BY class) OVER () AS probabilities
            FROM (
                SELECT w.class, w.weight / SUM(w.weight) OVER () AS probability
                FROM location_weights AS w
                WHERE w.era = t.era AND w.result_family = t.result_family
                  AND w.is_bunt = t.is_bunt AND w.batter_hand = t.batter_hand
                  AND (COALESCE(t.raw_handler_position, 0) NOT BETWEEN 1 AND 9
                       OR w.category_side = t.raw_handler_side OR t.raw_handler_side = 'All')
                  AND (t.deduced_location_side IS NULL OR t.deduced_location_side = 'Unknown'
                       OR w.category_side = t.deduced_location_side)
                  AND (t.deduced_location_depth IS NULL OR t.deduced_location_depth = 'Unknown'
                       OR w.category_depth = t.deduced_location_depth)
            )
        )
        WHERE cumulative_probability >= (HASH({seed} || ':' || CAST(t.event_key AS VARCHAR) || ':location') % 1000000) / 1000000.0
        ORDER BY cumulative_probability
        LIMIT 1
    ) AS sampled ON TRUE
    LEFT JOIN LATERAL (
        SELECT class, probability, classes, probabilities
        FROM (
            SELECT
                class, probability,
                SUM(probability) OVER (ORDER BY class) AS cumulative_probability,
                LIST(class ORDER BY class) OVER () AS classes,
                LIST(probability ORDER BY class) OVER () AS probabilities
            FROM (
                SELECT w.class, w.weight / SUM(w.weight) OVER () AS probability
                FROM location_weights AS w
                WHERE w.era = t.era AND w.result_family = t.result_family
                  AND w.is_bunt = t.is_bunt AND w.batter_hand = t.batter_hand
            )
        )
        WHERE cumulative_probability >= (HASH({seed} || ':' || CAST(t.event_key AS VARCHAR) || ':location') % 1000000) / 1000000.0
        ORDER BY cumulative_probability
        LIMIT 1
    ) AS fallback ON TRUE
),
taxonomy_completed AS (
    SELECT
        l.*,
        s.category_side AS location_side,
        s.category_depth AS location_depth,
        s.category_edge AS location_edge,
        CASE WHEN s.class IS NULL THEN 'unmapped_source_value'
             WHEN l.general_location_status = 'observed' THEN 'derived' ELSE 'estimated' END
            AS location_taxonomy_status,
        'seed_taxonomy_from_general_location' AS location_taxonomy_method
    FROM location_completed AS l
    LEFT JOIN location_support AS s ON s.class = l.general_location
),
companions_completed AS (
    SELECT
        l.*,
        CASE WHEN l.raw_location_depth_modifier IN ('Shallow', 'Deep', 'ExtraDeep')
             THEN l.raw_location_depth_modifier ELSE depth_sample.class END AS location_depth_modifier,
        CASE WHEN l.raw_location_depth_modifier IN ('Shallow', 'Deep', 'ExtraDeep') THEN 'observed'
             WHEN l.raw_location_depth_modifier = 'Default' THEN 'estimated_default_code'
             ELSE 'estimated' END AS location_depth_modifier_status,
        CASE WHEN l.raw_location_depth_modifier IN ('Shallow', 'Deep', 'ExtraDeep') THEN 'source_raw'
             WHEN l.raw_location_depth_modifier = 'Default' THEN 'default_sentinel_compatible_empirical_draw'
             ELSE 'compatible_partially_pooled_empirical_draw' END AS location_depth_modifier_method,
        CASE WHEN l.raw_location_angle IN ('Foul', 'FoulLine', 'Left', 'Middle', 'Right')
             THEN l.raw_location_angle ELSE angle_sample.class END AS location_angle,
        CASE WHEN l.raw_location_angle IN ('Foul', 'FoulLine', 'Left', 'Middle', 'Right') THEN 'observed'
             WHEN l.raw_location_angle = 'Default' THEN 'estimated_default_code'
             ELSE 'estimated' END AS location_angle_status,
        CASE WHEN l.raw_location_angle IN ('Foul', 'FoulLine', 'Left', 'Middle', 'Right') THEN 'source_raw'
             WHEN l.raw_location_angle = 'Default' THEN 'default_sentinel_compatible_empirical_draw'
             ELSE 'compatible_partially_pooled_empirical_draw' END AS location_angle_method,
        CASE WHEN l.raw_contact_strength IN ('Hard', 'Soft', 'Default') THEN l.raw_contact_strength
             ELSE 'Default' END AS contact_strength,
        CASE
            WHEN l.raw_contact_strength IN ('Hard', 'Soft') THEN 'observed'
            WHEN l.raw_contact_strength = 'Default' THEN 'default_code'
            ELSE 'assumed'
        END AS contact_strength_status,
        CASE WHEN l.raw_contact_strength IN ('Hard', 'Soft') THEN 'source_raw'
             WHEN l.raw_contact_strength = 'Default' THEN 'source_default_unspecified_or_neutral'
             ELSE 'neutral_unspecified_fallback' END AS contact_strength_method,
        CASE WHEN l.raw_handler_position BETWEEN 1 AND 9 THEN l.raw_handler_position
             ELSE handler_sample.class END AS handler_position,
        CASE WHEN l.raw_handler_position BETWEEN 1 AND 9 THEN 'observed' ELSE 'estimated' END AS handler_position_status,
        CASE WHEN l.raw_handler_position BETWEEN 1 AND 9 THEN 'source_raw'
             ELSE 'compatible_partially_pooled_empirical_draw' END AS handler_position_method
    FROM taxonomy_completed AS l
    LEFT JOIN LATERAL (
        SELECT class
        FROM (
            SELECT
                w.class,
                SUM(w.weight) OVER (ORDER BY w.class) / SUM(w.weight) OVER () AS cumulative_probability
            FROM depth_weights AS w
            WHERE w.general_location = l.general_location AND w.era = l.era
              AND w.result_family = l.result_family AND w.is_bunt = l.is_bunt
              AND w.batter_hand = l.batter_hand
        )
        WHERE cumulative_probability >= (HASH({seed} || ':' || CAST(l.event_key AS VARCHAR) || ':depth') % 1000000) / 1000000.0
        ORDER BY cumulative_probability LIMIT 1
    ) AS depth_sample ON TRUE
    LEFT JOIN LATERAL (
        SELECT class
        FROM (
            SELECT
                w.class,
                SUM(w.weight) OVER (ORDER BY w.class) / SUM(w.weight) OVER () AS cumulative_probability
            FROM angle_weights AS w
            WHERE w.general_location = l.general_location AND w.era = l.era
              AND w.result_family = l.result_family AND w.is_bunt = l.is_bunt
              AND w.batter_hand = l.batter_hand
        )
        WHERE cumulative_probability >= (HASH({seed} || ':' || CAST(l.event_key AS VARCHAR) || ':angle') % 1000000) / 1000000.0
        ORDER BY cumulative_probability LIMIT 1
    ) AS angle_sample ON TRUE
    LEFT JOIN LATERAL (
        SELECT class
        FROM (
            SELECT
                w.class,
                SUM(w.weight) OVER (ORDER BY w.class) / SUM(w.weight) OVER () AS cumulative_probability
            FROM strength_weights AS w
            WHERE w.era = l.era AND w.result_family = l.result_family
              AND w.is_bunt = l.is_bunt AND w.batter_hand = l.batter_hand
        )
        WHERE cumulative_probability >= (HASH({seed} || ':' || CAST(l.event_key AS VARCHAR) || ':strength') % 1000000) / 1000000.0
        ORDER BY cumulative_probability LIMIT 1
    ) AS strength_sample ON TRUE
    LEFT JOIN LATERAL (
        SELECT class
        FROM (
            SELECT
                w.class,
                SUM(w.weight) OVER (ORDER BY w.class) / SUM(w.weight) OVER () AS cumulative_probability
            FROM handler_weights AS w
            WHERE w.era = l.era AND w.result_family = l.result_family
              AND w.is_bunt = l.is_bunt AND w.batter_hand = l.batter_hand
              AND (w.category_side = l.location_side OR l.location_side = 'All')
        )
        WHERE cumulative_probability >= (HASH({seed} || ':' || CAST(l.event_key AS VARCHAR) || ':handler') % 1000000) / 1000000.0
        ORDER BY cumulative_probability LIMIT 1
    ) AS handler_sample ON TRUE
),
air_probabilities AS (
    SELECT
        c.*,
        CAST(CASE
            WHEN c.trajectory = 'GroundBall' THEN 1.0
            WHEN c.is_bunt THEN 0.0
            ELSE 0.0
        END AS DOUBLE) AS p_standardized_ground_ball,
        CASE WHEN c.trajectory IN ('Fly', 'LineDrive', 'PopUp') THEN COALESCE(a.p_fly, 0.0) ELSE 0.0 END AS p_standardized_fly,
        CASE WHEN c.trajectory IN ('Fly', 'LineDrive', 'PopUp') THEN COALESCE(a.p_line_drive, 0.0) ELSE 0.0 END AS p_standardized_line_drive,
        CASE WHEN c.trajectory IN ('Fly', 'LineDrive', 'PopUp') THEN COALESCE(a.p_pop_up, 0.0) ELSE 0.0 END AS p_standardized_pop_up,
        CASE
            WHEN c.is_bunt THEN 'not_applicable_bunt'
            WHEN c.trajectory = 'GroundBall' THEN 'ground_preserved'
            WHEN c.trajectory IN ('Fly', 'LineDrive', 'PopUp') THEN 'estimated'
            ELSE 'unresolved'
        END AS standardized_trajectory_status,
        CASE
            WHEN c.is_bunt THEN 'bunt_excluded'
            WHEN c.trajectory = 'GroundBall' THEN 'deterministic_ground_constraint'
            WHEN c.season < 1989 THEN 'air_translation_assumed_transport'
            ELSE 'air_translation'
        END AS standardized_trajectory_method,
        CASE
            WHEN c.is_bunt OR c.trajectory = 'GroundBall' THEN 'not_applicable'
            WHEN c.season < 1989 THEN 'earliest_compatible_seed_assumed_transport'
            ELSE COALESCE(a.basis, 'translation_missing')
        END AS standardized_trajectory_basis,
        CASE WHEN c.trajectory IN ('Fly', 'LineDrive', 'PopUp')
             THEN c.season < 1989 OR COALESCE(a.partially_identified, TRUE)
             ELSE FALSE END AS standardized_trajectory_weakly_identified
    FROM companions_completed AS c
    LEFT JOIN LATERAL (
        SELECT
            MAX(CASE WHEN standardized_air_subtype = 'Fly' THEN probability_mean END) AS p_fly,
            MAX(CASE WHEN standardized_air_subtype = 'LineDrive' THEN probability_mean END) AS p_line_drive,
            MAX(CASE WHEN standardized_air_subtype = 'PopUp' THEN probability_mean END) AS p_pop_up,
            MIN(basis) AS basis,
            BOOL_OR(partially_identified) AS partially_identified
        FROM {config.air_translation_relation} AS a
        WHERE a.season = CASE WHEN c.season < 1989 THEN 1989 ELSE c.season END
          AND a.recorded_air_subtype = c.trajectory
          AND a.result_family = c.result_family
    ) AS a ON TRUE
),
standardized AS (
    SELECT
        a.*,
        CASE
            WHEN a.is_bunt THEN NULL
            WHEN a.trajectory = 'GroundBall' THEN 'GroundBall'
            WHEN u <= a.p_standardized_fly THEN 'Fly'
            WHEN u <= a.p_standardized_fly + a.p_standardized_line_drive THEN 'LineDrive'
            WHEN u <= a.p_standardized_fly + a.p_standardized_line_drive + a.p_standardized_pop_up THEN 'PopUp'
            ELSE NULL
        END AS standardized_trajectory
    FROM air_probabilities AS a
    CROSS JOIN LATERAL (
        SELECT (HASH({seed} || ':' || CAST(a.event_key AS VARCHAR) || ':standardized') % 1000000) / 1000000.0 AS u
    )
)
SELECT
    event_key, game_id, season, result_family, batter_hand, is_bunt,
    raw_trajectory, trajectory, trajectory_status, trajectory_method, trajectory_distribution_ref,
    trajectory_distribution_classes, trajectory_distribution_probabilities, trajectory_sample_probability,
    raw_general_location, general_location, general_location_status, general_location_method, location_distribution_ref,
    location_distribution_classes, location_distribution_probabilities, location_sample_probability,
    location_side, location_depth, location_edge, location_taxonomy_status, location_taxonomy_method,
    raw_location_depth_modifier, location_depth_modifier, location_depth_modifier_status, location_depth_modifier_method,
    raw_location_angle, location_angle, location_angle_status, location_angle_method,
    raw_contact_strength, contact_strength, contact_strength_status, contact_strength_method,
    raw_handler_position, handler_position, handler_position_status, handler_position_method,
    standardized_trajectory, standardized_trajectory_status, standardized_trajectory_method,
    standardized_trajectory_basis, standardized_trajectory_weakly_identified,
    p_standardized_ground_ball, p_standardized_fly, p_standardized_line_drive, p_standardized_pop_up,
    '{MODEL_NAME}' AS model_name, '{MODEL_VERSION}' AS model_version,
    '{SOURCE_SIGNATURE}' AS source_signature, 'exploratory' AS confidence_status
FROM standardized
""".strip()
