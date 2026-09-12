from __future__ import annotations

ESTIMATE_ID = "geometry-air-translation-estimate-v1"
RUN_MANIFEST_SHA256 = "8b9fc41c5cda5882014f20195f1a51b0b68bb67a5a8d3bef5788536c7b926550"
MODEL_VERSION = "1"
TRANSLATION_MODEL = "air_trajectory_translation"
STANDARDIZED_MODEL = "standardized_air_trajectory"
SEED_TABLE = "main_seeds.seed_air_trajectory_translation"
RECORDED_AIR_SUBTYPES = ("Fly", "LineDrive", "PopUp")
STANDARDIZED_AIR_SUBTYPES = RECORDED_AIR_SUBTYPES
RESULT_FAMILIES = (
    "hit",
    "out_in_play",
    "sacrifice",
    "reached_on_error",
    "fielders_choice",
)
PARTIALLY_IDENTIFIED_BASIS = "raked_from_pipeline_c_with_modern_mix"
BASES = (
    "referenced_season_posterior",
    "pipeline_new_season_predictive",
    PARTIALLY_IDENTIFIED_BASIS,
)
CELL_STATUSES = ("posterior_cell", "prior_only_cell")
SEED_COLUMNS = (
    "season",
    "recorded_air_subtype",
    "result_family",
    "standardized_air_subtype",
    "probability_mean",
    "probability_lower_95",
    "probability_upper_95",
    "basis",
    "cell_status",
    "partially_identified",
)


def _sql_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def build_translation_sql(seed_table: str = SEED_TABLE) -> str:
    return f"""
SELECT
    CAST(season AS SMALLINT) AS season,
    CAST(recorded_air_subtype AS VARCHAR) AS recorded_air_subtype,
    CAST(result_family AS VARCHAR) AS result_family,
    CAST(standardized_air_subtype AS VARCHAR) AS standardized_air_subtype,
    CAST(probability_mean AS DOUBLE) AS probability_mean,
    CAST(probability_lower_95 AS DOUBLE) AS probability_lower_95,
    CAST(probability_upper_95 AS DOUBLE) AS probability_upper_95,
    CAST(basis AS VARCHAR) AS basis,
    CAST(cell_status AS VARCHAR) AS cell_status,
    CAST(partially_identified AS BOOLEAN) AS partially_identified,
    '{ESTIMATE_ID}' AS artifact_id,
    '{TRANSLATION_MODEL}' AS model_name,
    '{MODEL_VERSION}' AS model_version,
    '{RUN_MANIFEST_SHA256}' AS source_snapshot_id,
    CAST(basis AS VARCHAR) AS method,
    'estimated' AS observed_status,
    'exploratory' AS confidence_status,
    CAST(partially_identified AS BOOLEAN) AS weak_identification_flag
FROM {seed_table}
"""


def build_standardized_sql(
    geometry_table: str = "main_models.event_observation_geometry",
    context_table: str = "main_models.event_observation_context",
    translation_table: str = f"main_models.{TRANSLATION_MODEL}",
) -> str:
    return f"""
SELECT
    g.event_key,
    c.season,
    g.raw_value AS recorded_air_subtype,
    c.result_family,
    t.standardized_air_subtype,
    t.probability_mean AS expected_share,
    t.probability_lower_95 AS share_lower_95,
    t.probability_upper_95 AS share_upper_95,
    t.basis,
    t.cell_status,
    t.partially_identified,
    t.artifact_id,
    '{STANDARDIZED_MODEL}' AS model_name,
    t.model_version,
    t.source_snapshot_id,
    t.method,
    t.observed_status,
    t.confidence_status,
    t.weak_identification_flag
FROM {geometry_table} AS g
INNER JOIN {context_table} AS c USING (event_key)
INNER JOIN {translation_table} AS t
    ON t.season = c.season
    AND t.recorded_air_subtype = g.raw_value
    AND t.result_family = c.result_family
WHERE g.dimension = 'trajectory'
  AND g.observed_status = 'observed'
  AND g.raw_value IN ({_sql_list(RECORDED_AIR_SUBTYPES)})
"""
