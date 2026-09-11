MODEL (
  name main_models.game_context_observation_ledger,
  kind FULL,
  description 'Per (game_id, context_dimension) observation ledger for game-level context covariates. Atomic dimensions: park_id, sky, field_condition, precipitation, temperature, wind_direction, wind_speed, time_of_day, attendance, dh_rule, extra_inning_runner_rule, game_type, scorer, official_scorer, source_scorer, inputter, translator, umpire_home, umpire_first, umpire_second, umpire_third, umpire_left, umpire_right, batter_hand, pitcher_hand. scorer is the legacy ambiguous compatibility field. official_scorer and source_scorer preserve the distinct raw info,oscorer and info,scorer keys. observed_status drawn from seed_observed_status: observed (source value present and not a sentinel), unknown_code (a source sentinel such as Unknown or ?), derived (bio-derived hand availability, deterministic rule_era flags), missing (NULL or blank with no sentinel), not_applicable (structurally inapplicable for era/game_type). contradicted and data_error_prone reserved for future cross-source disagreement enrichment. source_family identifies the originating registry: retrosheet_pbp / retrosheet_box for stg_games columns (split by source_type), rule_era for deterministic extra-inning runner rule by season/game_type, retrosheet_bio for hand dimensions (game-grain row stamping bio-derived availability; per-roster bio coverage refinement is a follow-up). umpire_third treats NULL as not_applicable pre-1933 (3-man crews common); umpire_left/umpire_right treat NULL as not_applicable except in postseason eras where 6-man crews are standard (WorldSeries from 1947, LeagueChampionshipSeries from 1969, DivisionSeries from 1995). extra_inning_runner_rule emits observed only for RegularSeason 2020+; doc-01 lists "umpires", "weather", and "wind" as single dimensions — split into per-slot/per-sub-field atoms here so missingness rates aggregate cleanly. Consumed by context-dependent fitted models (park, geometry, advancement, run-value) for downweighting or imputation.',
  grain (game_id, context_dimension),
  columns (
    game_id VARCHAR,
    context_dimension VARCHAR,
    raw_value VARCHAR,
    normalized_value VARCHAR,
    observed_status VARCHAR,
    source_family VARCHAR,
    context_confidence VARCHAR
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    context_dimension = 'Atomic context-field key. See model description for full list.',
    raw_value = 'Source value serialized as text (cast from native type). NULL when source is structurally absent (e.g., bio-derived dimensions that have no per-game scalar).',
    normalized_value = 'Canonical/deterministic value when distinct from raw_value (currently same as raw_value for stg_games columns; NULL when raw is NULL or when no normalization applies).',
    observed_status = 'FK to seed_observed_status. observed = directly recorded; derived = deterministically deduced (e.g., bio-derived hand availability, rule_era flags); missing = source absent or null; not_applicable = structurally inapplicable for the era/game_type.',
    source_family = 'Originating registry: retrosheet_pbp (stg_games row from PlayByPlay source), retrosheet_box (stg_games row from BoxScore source), retrosheet_bio (bio-derived hand availability), rule_era (deterministic by season/game_type).',
    context_confidence = 'Qualitative confidence: high (observed/derived/not_applicable); low (missing).'
  ),
  audits (
    not_null(columns := (game_id, context_dimension, observed_status, source_family, context_confidence)),
    unique_grain(columns := (game_id, context_dimension)),
    accepted_values(column := context_dimension, is_in := (
      'park_id', 'sky', 'field_condition', 'precipitation', 'temperature',
      'wind_direction', 'wind_speed', 'time_of_day', 'attendance', 'dh_rule',
      'extra_inning_runner_rule', 'game_type', 'scorer', 'official_scorer',
      'source_scorer', 'inputter', 'translator',
      'umpire_home', 'umpire_first', 'umpire_second', 'umpire_third',
      'umpire_left', 'umpire_right', 'batter_hand', 'pitcher_hand'
    )),
    accepted_values(column := observed_status, is_in := (
      'observed', 'derived', 'aggregate_only', 'missing', 'unknown_code',
      'default_code', 'not_applicable', 'contradicted', 'data_error_prone'
    )),
    accepted_values(column := source_family, is_in := (
      'retrosheet_pbp', 'retrosheet_box', 'retrosheet_bio', 'rule_era'
    )),
    accepted_values(column := context_confidence, is_in := ('high', 'medium', 'low')),
    relationships(
      column := observed_status,
      to_model := main_seeds.seed_observed_status,
      to_column := observed_status
    )
  )
);

WITH games_in_scope AS (
    SELECT
        game_id,
        season,
        game_type,
        source_type,
        park_id,
        sky,
        field_condition,
        precipitation,
        temperature_fahrenheit,
        wind_direction,
        wind_speed_mph,
        time_of_day,
        attendance,
        use_dh,
        scorer,
        official_scorer,
        source_scorer,
        inputter,
        translator,
        umpire_home_id,
        umpire_first_id,
        umpire_second_id,
        umpire_third_id,
        umpire_left_id,
        umpire_right_id
    FROM main_models.stg_games
    WHERE season BETWEEN @start_season AND @end_season
),

stg_source_family AS (
    SELECT
        game_id,
        CASE source_type
            WHEN 'PlayByPlay' THEN 'retrosheet_pbp'
            ELSE 'retrosheet_box'
        END AS source_family
    FROM games_in_scope
),

park AS (
    SELECT
        g.game_id,
        'park_id' AS context_dimension,
        CAST(g.park_id AS VARCHAR) AS raw_value,
        CAST(g.park_id AS VARCHAR) AS normalized_value,
        CASE WHEN g.park_id IS NULL THEN 'missing' ELSE 'observed' END AS observed_status,
        sf.source_family,
        CASE WHEN g.park_id IS NULL THEN 'low' ELSE 'high' END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

sky AS (
    SELECT
        g.game_id,
        'sky' AS context_dimension,
        CAST(g.sky AS VARCHAR) AS raw_value,
        CAST(g.sky AS VARCHAR) AS normalized_value,
        CASE
            WHEN g.sky IS NULL THEN 'missing'
            WHEN g.sky = 'Unknown' THEN 'unknown_code'
            ELSE 'observed'
        END AS observed_status,
        sf.source_family,
        CASE
            WHEN g.sky IS NULL THEN 'low'
            WHEN g.sky = 'Unknown' THEN 'low'
            ELSE 'high'
        END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

field_condition AS (
    SELECT
        g.game_id,
        'field_condition' AS context_dimension,
        CAST(g.field_condition AS VARCHAR) AS raw_value,
        CAST(g.field_condition AS VARCHAR) AS normalized_value,
        CASE
            WHEN g.field_condition IS NULL THEN 'missing'
            WHEN g.field_condition = 'Unknown' THEN 'unknown_code'
            ELSE 'observed'
        END AS observed_status,
        sf.source_family,
        CASE
            WHEN g.field_condition IS NULL THEN 'low'
            WHEN g.field_condition = 'Unknown' THEN 'low'
            ELSE 'high'
        END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

precipitation AS (
    SELECT
        g.game_id,
        'precipitation' AS context_dimension,
        CAST(g.precipitation AS VARCHAR) AS raw_value,
        CAST(g.precipitation AS VARCHAR) AS normalized_value,
        CASE
            WHEN g.precipitation IS NULL THEN 'missing'
            WHEN g.precipitation = 'Unknown' THEN 'unknown_code'
            ELSE 'observed'
        END AS observed_status,
        sf.source_family,
        CASE
            WHEN g.precipitation IS NULL THEN 'low'
            WHEN g.precipitation = 'Unknown' THEN 'low'
            ELSE 'high'
        END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

temperature AS (
    SELECT
        g.game_id,
        'temperature' AS context_dimension,
        CAST(g.temperature_fahrenheit AS VARCHAR) AS raw_value,
        CAST(g.temperature_fahrenheit AS VARCHAR) AS normalized_value,
        CASE WHEN g.temperature_fahrenheit IS NULL THEN 'missing' ELSE 'observed' END AS observed_status,
        sf.source_family,
        CASE WHEN g.temperature_fahrenheit IS NULL THEN 'low' ELSE 'high' END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

wind_direction AS (
    SELECT
        g.game_id,
        'wind_direction' AS context_dimension,
        CAST(g.wind_direction AS VARCHAR) AS raw_value,
        CAST(g.wind_direction AS VARCHAR) AS normalized_value,
        CASE
            WHEN g.wind_direction IS NULL THEN 'missing'
            WHEN g.wind_direction = 'Unknown' THEN 'unknown_code'
            ELSE 'observed'
        END AS observed_status,
        sf.source_family,
        CASE
            WHEN g.wind_direction IS NULL THEN 'low'
            WHEN g.wind_direction = 'Unknown' THEN 'low'
            ELSE 'high'
        END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

wind_speed AS (
    SELECT
        g.game_id,
        'wind_speed' AS context_dimension,
        CAST(g.wind_speed_mph AS VARCHAR) AS raw_value,
        CAST(g.wind_speed_mph AS VARCHAR) AS normalized_value,
        CASE WHEN g.wind_speed_mph IS NULL THEN 'missing' ELSE 'observed' END AS observed_status,
        sf.source_family,
        CASE WHEN g.wind_speed_mph IS NULL THEN 'low' ELSE 'high' END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

time_of_day AS (
    SELECT
        g.game_id,
        'time_of_day' AS context_dimension,
        CAST(g.time_of_day AS VARCHAR) AS raw_value,
        CAST(g.time_of_day AS VARCHAR) AS normalized_value,
        CASE
            WHEN g.time_of_day IS NULL THEN 'missing'
            WHEN g.time_of_day = 'Unknown' THEN 'unknown_code'
            ELSE 'observed'
        END AS observed_status,
        sf.source_family,
        CASE
            WHEN g.time_of_day IS NULL THEN 'low'
            WHEN g.time_of_day = 'Unknown' THEN 'low'
            ELSE 'high'
        END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

attendance AS (
    SELECT
        g.game_id,
        'attendance' AS context_dimension,
        CAST(g.attendance AS VARCHAR) AS raw_value,
        CAST(g.attendance AS VARCHAR) AS normalized_value,
        CASE WHEN g.attendance IS NULL THEN 'missing' ELSE 'observed' END AS observed_status,
        sf.source_family,
        CASE WHEN g.attendance IS NULL THEN 'low' ELSE 'high' END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

dh_rule AS (
    SELECT
        g.game_id,
        'dh_rule' AS context_dimension,
        CAST(g.use_dh AS VARCHAR) AS raw_value,
        CAST(g.use_dh AS VARCHAR) AS normalized_value,
        CASE WHEN g.use_dh IS NULL THEN 'missing' ELSE 'observed' END AS observed_status,
        sf.source_family,
        CASE WHEN g.use_dh IS NULL THEN 'low' ELSE 'high' END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

extra_inning_runner_rule AS (
    SELECT
        g.game_id,
        'extra_inning_runner_rule' AS context_dimension,
        CASE
            WHEN g.season >= 2020 AND g.game_type = 'RegularSeason' THEN 'on'
            ELSE NULL
        END AS raw_value,
        CASE
            WHEN g.season >= 2020 AND g.game_type = 'RegularSeason' THEN 'on'
            ELSE NULL
        END AS normalized_value,
        CASE
            WHEN g.season >= 2020 AND g.game_type = 'RegularSeason' THEN 'observed'
            ELSE 'not_applicable'
        END AS observed_status,
        'rule_era' AS source_family,
        'high' AS context_confidence
    FROM games_in_scope AS g
),

game_type AS (
    SELECT
        g.game_id,
        'game_type' AS context_dimension,
        CAST(g.game_type AS VARCHAR) AS raw_value,
        CAST(g.game_type AS VARCHAR) AS normalized_value,
        CASE WHEN g.game_type IS NULL THEN 'missing' ELSE 'observed' END AS observed_status,
        sf.source_family,
        CASE WHEN g.game_type IS NULL THEN 'low' ELSE 'high' END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

scorer AS (
    SELECT
        g.game_id,
        'scorer' AS context_dimension,
        g.scorer AS raw_value,
        g.scorer AS normalized_value,
        CASE WHEN g.scorer IS NULL THEN 'missing' ELSE 'observed' END AS observed_status,
        sf.source_family,
        CASE WHEN g.scorer IS NULL THEN 'low' ELSE 'high' END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

official_scorer AS (
    SELECT
        g.game_id,
        'official_scorer' AS context_dimension,
        g.official_scorer AS raw_value,
        g.official_scorer AS normalized_value,
        CASE
            WHEN g.official_scorer IS NULL OR TRIM(g.official_scorer) = '' THEN 'missing'
            WHEN LOWER(TRIM(g.official_scorer)) IN ('unknown', '?') THEN 'unknown_code'
            ELSE 'observed'
        END AS observed_status,
        sf.source_family,
        CASE
            WHEN g.official_scorer IS NULL OR TRIM(g.official_scorer) = '' THEN 'low'
            WHEN LOWER(TRIM(g.official_scorer)) IN ('unknown', '?') THEN 'low'
            ELSE 'high'
        END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

source_scorer AS (
    SELECT
        g.game_id,
        'source_scorer' AS context_dimension,
        g.source_scorer AS raw_value,
        g.source_scorer AS normalized_value,
        CASE
            WHEN g.source_scorer IS NULL OR TRIM(g.source_scorer) = '' THEN 'missing'
            WHEN LOWER(TRIM(g.source_scorer)) IN ('unknown', '?') THEN 'unknown_code'
            ELSE 'observed'
        END AS observed_status,
        sf.source_family,
        CASE
            WHEN g.source_scorer IS NULL OR TRIM(g.source_scorer) = '' THEN 'low'
            WHEN LOWER(TRIM(g.source_scorer)) IN ('unknown', '?') THEN 'low'
            ELSE 'high'
        END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

inputter AS (
    SELECT
        g.game_id,
        'inputter' AS context_dimension,
        g.inputter AS raw_value,
        g.inputter AS normalized_value,
        CASE WHEN g.inputter IS NULL THEN 'missing' ELSE 'observed' END AS observed_status,
        sf.source_family,
        CASE WHEN g.inputter IS NULL THEN 'low' ELSE 'high' END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

translator AS (
    SELECT
        g.game_id,
        'translator' AS context_dimension,
        g.translator AS raw_value,
        g.translator AS normalized_value,
        CASE WHEN g.translator IS NULL THEN 'missing' ELSE 'observed' END AS observed_status,
        sf.source_family,
        CASE WHEN g.translator IS NULL THEN 'low' ELSE 'high' END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

umpire_home AS (
    SELECT
        g.game_id,
        'umpire_home' AS context_dimension,
        g.umpire_home_id AS raw_value,
        g.umpire_home_id AS normalized_value,
        CASE WHEN g.umpire_home_id IS NULL THEN 'missing' ELSE 'observed' END AS observed_status,
        sf.source_family,
        CASE WHEN g.umpire_home_id IS NULL THEN 'low' ELSE 'high' END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

umpire_first AS (
    SELECT
        g.game_id,
        'umpire_first' AS context_dimension,
        g.umpire_first_id AS raw_value,
        g.umpire_first_id AS normalized_value,
        CASE WHEN g.umpire_first_id IS NULL THEN 'missing' ELSE 'observed' END AS observed_status,
        sf.source_family,
        CASE WHEN g.umpire_first_id IS NULL THEN 'low' ELSE 'high' END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

umpire_second AS (
    SELECT
        g.game_id,
        'umpire_second' AS context_dimension,
        g.umpire_second_id AS raw_value,
        g.umpire_second_id AS normalized_value,
        CASE WHEN g.umpire_second_id IS NULL THEN 'missing' ELSE 'observed' END AS observed_status,
        sf.source_family,
        CASE WHEN g.umpire_second_id IS NULL THEN 'low' ELSE 'high' END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

umpire_third AS (
    SELECT
        g.game_id,
        'umpire_third' AS context_dimension,
        g.umpire_third_id AS raw_value,
        g.umpire_third_id AS normalized_value,
        CASE
            WHEN g.umpire_third_id IS NOT NULL THEN 'observed'
            WHEN g.season < 1933 THEN 'not_applicable'
            ELSE 'missing'
        END AS observed_status,
        sf.source_family,
        CASE
            WHEN g.umpire_third_id IS NOT NULL THEN 'high'
            WHEN g.season < 1933 THEN 'high'
            ELSE 'low'
        END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

umpire_left AS (
    SELECT
        g.game_id,
        'umpire_left' AS context_dimension,
        g.umpire_left_id AS raw_value,
        g.umpire_left_id AS normalized_value,
        CASE
            WHEN g.umpire_left_id IS NOT NULL THEN 'observed'
            WHEN g.game_type = 'WorldSeries' AND g.season >= 1947 THEN 'missing'
            WHEN g.game_type = 'LeagueChampionshipSeries' AND g.season >= 1969 THEN 'missing'
            WHEN g.game_type = 'DivisionSeries' AND g.season >= 1995 THEN 'missing'
            ELSE 'not_applicable'
        END AS observed_status,
        sf.source_family,
        CASE
            WHEN g.umpire_left_id IS NOT NULL THEN 'high'
            WHEN g.game_type = 'WorldSeries' AND g.season >= 1947 THEN 'low'
            WHEN g.game_type = 'LeagueChampionshipSeries' AND g.season >= 1969 THEN 'low'
            WHEN g.game_type = 'DivisionSeries' AND g.season >= 1995 THEN 'low'
            ELSE 'high'
        END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

umpire_right AS (
    SELECT
        g.game_id,
        'umpire_right' AS context_dimension,
        g.umpire_right_id AS raw_value,
        g.umpire_right_id AS normalized_value,
        CASE
            WHEN g.umpire_right_id IS NOT NULL THEN 'observed'
            WHEN g.game_type = 'WorldSeries' AND g.season >= 1947 THEN 'missing'
            WHEN g.game_type = 'LeagueChampionshipSeries' AND g.season >= 1969 THEN 'missing'
            WHEN g.game_type = 'DivisionSeries' AND g.season >= 1995 THEN 'missing'
            ELSE 'not_applicable'
        END AS observed_status,
        sf.source_family,
        CASE
            WHEN g.umpire_right_id IS NOT NULL THEN 'high'
            WHEN g.game_type = 'WorldSeries' AND g.season >= 1947 THEN 'low'
            WHEN g.game_type = 'LeagueChampionshipSeries' AND g.season >= 1969 THEN 'low'
            WHEN g.game_type = 'DivisionSeries' AND g.season >= 1995 THEN 'low'
            ELSE 'high'
        END AS context_confidence
    FROM games_in_scope AS g
    JOIN stg_source_family AS sf USING (game_id)
),

batter_hand AS (
    SELECT
        game_id,
        'batter_hand' AS context_dimension,
        CAST(NULL AS VARCHAR) AS raw_value,
        CAST(NULL AS VARCHAR) AS normalized_value,
        'derived' AS observed_status,
        'retrosheet_bio' AS source_family,
        'high' AS context_confidence
    FROM games_in_scope
),

pitcher_hand AS (
    SELECT
        game_id,
        'pitcher_hand' AS context_dimension,
        CAST(NULL AS VARCHAR) AS raw_value,
        CAST(NULL AS VARCHAR) AS normalized_value,
        'derived' AS observed_status,
        'retrosheet_bio' AS source_family,
        'high' AS context_confidence
    FROM games_in_scope
)

SELECT * FROM park
UNION ALL BY NAME SELECT * FROM sky
UNION ALL BY NAME SELECT * FROM field_condition
UNION ALL BY NAME SELECT * FROM precipitation
UNION ALL BY NAME SELECT * FROM temperature
UNION ALL BY NAME SELECT * FROM wind_direction
UNION ALL BY NAME SELECT * FROM wind_speed
UNION ALL BY NAME SELECT * FROM time_of_day
UNION ALL BY NAME SELECT * FROM attendance
UNION ALL BY NAME SELECT * FROM dh_rule
UNION ALL BY NAME SELECT * FROM extra_inning_runner_rule
UNION ALL BY NAME SELECT * FROM game_type
UNION ALL BY NAME SELECT * FROM scorer
UNION ALL BY NAME SELECT * FROM official_scorer
UNION ALL BY NAME SELECT * FROM source_scorer
UNION ALL BY NAME SELECT * FROM inputter
UNION ALL BY NAME SELECT * FROM translator
UNION ALL BY NAME SELECT * FROM umpire_home
UNION ALL BY NAME SELECT * FROM umpire_first
UNION ALL BY NAME SELECT * FROM umpire_second
UNION ALL BY NAME SELECT * FROM umpire_third
UNION ALL BY NAME SELECT * FROM umpire_left
UNION ALL BY NAME SELECT * FROM umpire_right
UNION ALL BY NAME SELECT * FROM batter_hand
UNION ALL BY NAME SELECT * FROM pitcher_hand
