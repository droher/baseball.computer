"""Repair invalid standardized trajectory distributions in completed geometry."""

from __future__ import annotations

import re
from typing import ClassVar, Final

from pydantic import BaseModel, ConfigDict, model_validator

from python_models.imputation.geometry_fast import (
    OUTPUT_SCHEMA as GEOMETRY_OUTPUT_SCHEMA,
)

MODEL_NAME: Final = "pbp_geometry_standardization_repair"
MODEL_VERSION: Final = "1.0.0"
INPUT_RELATIONS: Final = ("main_models.air_trajectory_translation",)
OUTPUT_SCHEMA: Final = GEOMETRY_OUTPUT_SCHEMA

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?$")


class GeometryStandardizationConfig(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    sample_seed: str = "pbp-geometry-completion-v1"
    air_translation_relation: str = "main_models.air_trajectory_translation"
    probability_tolerance: float = 1e-9

    @model_validator(mode="after")
    def validate_config(self) -> GeometryStandardizationConfig:
        if not self.sample_seed:
            raise ValueError("sample_seed must not be empty")
        if _IDENTIFIER.fullmatch(self.air_translation_relation) is None:
            raise ValueError("invalid air translation relation")
        if not 0 < self.probability_tolerance < 1:
            raise ValueError("probability_tolerance must be between zero and one")
        return self


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_geometry_standardization_query(
    completed_geometry_query: str,
    config: GeometryStandardizationConfig | None = None,
) -> str:
    resolved = config or GeometryStandardizationConfig()
    seed = _literal(resolved.sample_seed)
    tolerance = resolved.probability_tolerance
    translation = resolved.air_translation_relation
    return f"""
WITH completed_geometry AS (
    {completed_geometry_query}
),
translation_long AS (
    SELECT
        season,
        recorded_air_subtype,
        standardized_air_subtype,
        probability_mean
    FROM {translation}
    WHERE recorded_air_subtype IN ('Fly', 'LineDrive', 'PopUp')
      AND standardized_air_subtype IN ('Fly', 'LineDrive', 'PopUp')
      AND ISFINITE(probability_mean)
      AND probability_mean >= 0.0
),
season_recorded_pooled AS (
    SELECT
        season,
        recorded_air_subtype,
        AVG(probability_mean) FILTER (WHERE standardized_air_subtype = 'Fly') AS p_fly,
        AVG(probability_mean) FILTER (WHERE standardized_air_subtype = 'LineDrive') AS p_line_drive,
        AVG(probability_mean) FILTER (WHERE standardized_air_subtype = 'PopUp') AS p_pop_up
    FROM translation_long
    GROUP BY season, recorded_air_subtype
),
global_recorded_pooled AS (
    SELECT
        recorded_air_subtype,
        AVG(probability_mean) FILTER (WHERE standardized_air_subtype = 'Fly') AS p_fly,
        AVG(probability_mean) FILTER (WHERE standardized_air_subtype = 'LineDrive') AS p_line_drive,
        AVG(probability_mean) FILTER (WHERE standardized_air_subtype = 'PopUp') AS p_pop_up
    FROM translation_long
    GROUP BY recorded_air_subtype
),
classified AS (
    SELECT
        g.*,
        CASE
            WHEN g.is_bunt THEN FALSE
            WHEN g.trajectory = 'GroundBall' THEN
                g.standardized_trajectory IS DISTINCT FROM 'GroundBall'
                OR g.p_standardized_ground_ball IS NULL
                OR g.p_standardized_fly IS NULL
                OR g.p_standardized_line_drive IS NULL
                OR g.p_standardized_pop_up IS NULL
                OR NOT ISFINITE(g.p_standardized_ground_ball)
                OR NOT ISFINITE(g.p_standardized_fly)
                OR NOT ISFINITE(g.p_standardized_line_drive)
                OR NOT ISFINITE(g.p_standardized_pop_up)
                OR ABS(g.p_standardized_ground_ball - 1.0) > {tolerance}
                OR ABS(g.p_standardized_fly) > {tolerance}
                OR ABS(g.p_standardized_line_drive) > {tolerance}
                OR ABS(g.p_standardized_pop_up) > {tolerance}
            WHEN g.trajectory IN ('Fly', 'LineDrive', 'PopUp') THEN
                g.p_standardized_ground_ball IS NULL
                OR g.p_standardized_fly IS NULL
                OR g.p_standardized_line_drive IS NULL
                OR g.p_standardized_pop_up IS NULL
                OR NOT ISFINITE(g.p_standardized_ground_ball)
                OR NOT ISFINITE(g.p_standardized_fly)
                OR NOT ISFINITE(g.p_standardized_line_drive)
                OR NOT ISFINITE(g.p_standardized_pop_up)
                OR g.p_standardized_ground_ball < 0.0
                OR g.p_standardized_fly < 0.0
                OR g.p_standardized_line_drive < 0.0
                OR g.p_standardized_pop_up < 0.0
                OR ABS(
                    g.p_standardized_ground_ball + g.p_standardized_fly
                    + g.p_standardized_line_drive + g.p_standardized_pop_up - 1.0
                ) > {tolerance}
                OR CASE g.standardized_trajectory
                    WHEN 'Fly' THEN g.p_standardized_fly <= 0.0
                    WHEN 'LineDrive' THEN g.p_standardized_line_drive <= 0.0
                    WHEN 'PopUp' THEN g.p_standardized_pop_up <= 0.0
                    ELSE TRUE
                END
            ELSE FALSE
        END AS standardization_requires_repair
    FROM completed_geometry AS g
),
fallback AS (
    SELECT
        c.*,
        CASE
            WHEN s.recorded_air_subtype IS NOT NULL THEN 'season_recorded_subtype_pooled_over_result_family'
            WHEN p.recorded_air_subtype IS NOT NULL THEN 'global_recorded_subtype_pooled'
            ELSE 'recorded_subtype_identity'
        END AS fallback_basis,
        COALESCE(s.p_fly, p.p_fly, CASE WHEN c.trajectory = 'Fly' THEN 1.0 ELSE 0.0 END) AS fallback_p_fly,
        COALESCE(s.p_line_drive, p.p_line_drive, CASE WHEN c.trajectory = 'LineDrive' THEN 1.0 ELSE 0.0 END) AS fallback_p_line_drive,
        COALESCE(s.p_pop_up, p.p_pop_up, CASE WHEN c.trajectory = 'PopUp' THEN 1.0 ELSE 0.0 END) AS fallback_p_pop_up
    FROM classified AS c
    LEFT JOIN season_recorded_pooled AS s
      ON c.standardization_requires_repair
     AND s.season = CASE WHEN c.season < 1989 THEN 1989 ELSE c.season END
     AND s.recorded_air_subtype = c.trajectory
    LEFT JOIN global_recorded_pooled AS p
      ON c.standardization_requires_repair
     AND p.recorded_air_subtype = c.trajectory
),
normalized AS (
    SELECT
        f.*,
        f.fallback_p_fly + f.fallback_p_line_drive + f.fallback_p_pop_up AS fallback_total,
        (HASH({seed} || ':' || CAST(f.event_key AS VARCHAR) || ':standardized') % 1000000)
            / 1000000.0 AS standardization_draw
    FROM fallback AS f
),
repaired AS (
    SELECT
        n.*,
        n.fallback_p_fly / n.fallback_total AS repaired_p_fly,
        n.fallback_p_line_drive / n.fallback_total AS repaired_p_line_drive,
        n.fallback_p_pop_up / n.fallback_total AS repaired_p_pop_up
    FROM normalized AS n
)
SELECT
    r.* EXCLUDE (
        standardization_requires_repair, fallback_basis, fallback_p_fly,
        fallback_p_line_drive, fallback_p_pop_up, fallback_total,
        standardization_draw, repaired_p_fly, repaired_p_line_drive, repaired_p_pop_up
    ) REPLACE (
        CASE
            WHEN NOT r.standardization_requires_repair THEN r.standardized_trajectory
            WHEN r.trajectory = 'GroundBall' THEN 'GroundBall'
            WHEN r.repaired_p_fly > 0.0
             AND r.standardization_draw < r.repaired_p_fly THEN 'Fly'
            WHEN r.repaired_p_line_drive > 0.0
             AND r.standardization_draw < r.repaired_p_fly + r.repaired_p_line_drive THEN 'LineDrive'
            WHEN r.repaired_p_pop_up > 0.0 THEN 'PopUp'
            WHEN r.repaired_p_line_drive > 0.0 THEN 'LineDrive'
            ELSE 'Fly'
        END AS standardized_trajectory,
        CASE
            WHEN NOT r.standardization_requires_repair THEN r.standardized_trajectory_status
            WHEN r.trajectory = 'GroundBall' THEN 'ground_preserved'
            ELSE 'estimated_fallback'
        END AS standardized_trajectory_status,
        CASE
            WHEN NOT r.standardization_requires_repair THEN r.standardized_trajectory_method
            WHEN r.trajectory = 'GroundBall' THEN 'standardization_repair_deterministic_ground_constraint'
            WHEN r.season < 1989 THEN 'standardization_repair_assumed_transport'
            ELSE 'standardization_repair_translation_fallback'
        END AS standardized_trajectory_method,
        CASE
            WHEN NOT r.standardization_requires_repair THEN r.standardized_trajectory_basis
            WHEN r.trajectory = 'GroundBall' THEN 'not_applicable'
            WHEN r.season < 1989 THEN '1989_' || r.fallback_basis || '_assumed_transport'
            ELSE CAST(r.season AS VARCHAR) || '_' || r.fallback_basis
        END AS standardized_trajectory_basis,
        CASE
            WHEN NOT r.standardization_requires_repair THEN r.standardized_trajectory_weakly_identified
            WHEN r.trajectory = 'GroundBall' THEN FALSE
            ELSE TRUE
        END AS standardized_trajectory_weakly_identified,
        CASE
            WHEN NOT r.standardization_requires_repair THEN r.p_standardized_ground_ball
            WHEN r.trajectory = 'GroundBall' THEN CAST(1.0 AS DOUBLE)
            ELSE CAST(0.0 AS DOUBLE)
        END AS p_standardized_ground_ball,
        CASE WHEN r.standardization_requires_repair AND r.trajectory = 'GroundBall' THEN CAST(0.0 AS DOUBLE)
             WHEN r.standardization_requires_repair THEN r.repaired_p_fly
             ELSE r.p_standardized_fly END AS p_standardized_fly,
        CASE WHEN r.standardization_requires_repair AND r.trajectory = 'GroundBall' THEN CAST(0.0 AS DOUBLE)
             WHEN r.standardization_requires_repair THEN r.repaired_p_line_drive
             ELSE r.p_standardized_line_drive END AS p_standardized_line_drive,
        CASE WHEN r.standardization_requires_repair AND r.trajectory = 'GroundBall' THEN CAST(0.0 AS DOUBLE)
             WHEN r.standardization_requires_repair THEN r.repaired_p_pop_up
             ELSE r.p_standardized_pop_up END AS p_standardized_pop_up
    )
FROM repaired AS r
""".strip()


def build_geometry_standardization_parquet_query(
    parquet_path: str,
    config: GeometryStandardizationConfig | None = None,
) -> str:
    source = f"SELECT * FROM READ_PARQUET({_literal(parquet_path)})"
    return build_geometry_standardization_query(source, config)
