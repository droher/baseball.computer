---- STANDARD
-- Games
-- PA
-- AB
-- R
-- H
-- 2B
-- 3B
-- HR
-- RBI
-- SB
-- CS
-- BB
-- SO
-- BA
-- OBP
-- SLG
-- OPS
-- OPS+
-- TB
-- GDP
-- HBP
-- SH
-- SF
-- IBB

---- Advanced
-- rOBA
-- Rbat+
-- BAbip
-- ISO
-- HR%
-- SO%
-- BB%
-- LD%
-- FB%
-- GB%
-- Pull%
-- Cent%
-- Oppo%
-- WPA
-- cWPA
-- RE24
-- RS%
-- SB%
-- XBT%

---- Sabermetric
-- RC
-- RC/G

---- Value
-- Rbat
-- Rbaser
-- Rdp
-- Rfield
-- Rpos
-- RAA
-- WAA
-- Rrep
-- RAR
-- WAR
-- oWAR

---- Ratios (just ratios)

---- 
WITH plate_appearances AS (
    SELECT *
    FROM {{ ref('event_plate_appearances') }}
),

result_types AS (
    SELECT *
    FROM {{ ref('plate_appearance_result_types') }}
),

flags AS (
    SELECT *
    FROM {{ ref('event_flags') }}
),

dp_flag_types AS (
    SELECT *
    FROM {{ ref('double_play_flag_types') }}
),

double_plays AS (
    SELECT
        flags.game_id,
        flags.event_id,
        BOOL_OR(dp_flag_types.is_double_play) AS is_double_play,
        BOOL_OR(dp_flag_types.is_triple_play) AS is_triple_play,
        BOOL_OR(
            dp_flag_types.is_ground_ball_double_play
        ) AS is_ground_ball_double_play
    FROM flags
    INNER JOIN dp_flag_types ON (flags.flag = dp_flag_types.name)
    GROUP BY 1, 2
),

pre_agg AS (
    SELECT
        plate_appearances.game_id,
        plate_appearances.event_id,

        1 AS plate_appearances,
        result_types.is_at_bat::INT AS at_bats,
        result_types.is_hit::INT AS hits,
        (result_types.total_bases = 1)::INT AS singles,
        (result_types.total_bases = 2)::INT AS doubles,
        (result_types.total_bases = 3)::INT AS triples,
        (result_types.total_bases = 4)::INT AS home_runs,
        result_types.total_bases,

        (result_types.name = 'Strikeout') AS strikeouts,
        (result_types.name IN ('Walk', 'IntentionalWalk'))::INT AS walks,
        (result_types.name = 'IntentionalWalk')::INT AS intentional_walks,
        (result_types.name = 'HitByPitch')::INT AS hit_by_pitches,
        (result_types.name = 'SacrificeFly')::INT AS sacrifice_flies,
        (result_types.name = 'SacrificeHit')::INT AS sacrifice_hits,
        (result_types.name = 'FieldersChoice')::INT AS fielders_choices,
        (result_types.name = 'ReachedOnErrors')::INT AS reached_on_errors,

        double_plays.is_ground_ball_double_play::INT AS ground_ball_double_plays,
        double_plays.is_double_play::INT AS double_plays,
        double_plays.is_triple_play::INT AS triple_plays,

        result_types.is_on_base_opportunity::INT AS on_base_opportunities,
        result_types.is_on_base_success::INT AS on_base_successes

    FROM plate_appearances
    LEFT JOIN result_types
        ON result_types.name = plate_appearances.plate_appearance_result
    LEFT JOIN double_plays USING (event_key)
)

SELECT * FROM pre_agg
