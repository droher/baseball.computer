{{
  config(
    materialized = 'table',
    )
}}
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
    FROM {{ ref('stg_event_plate_appearances') }}
),

result_types AS (
    SELECT *
    FROM {{ ref('plate_appearance_result_types') }}
),

event_base AS (
    SELECT *
    FROM {{ ref('stg_events') }}
),

lineups AS (
    SELECT *
    FROM {{ ref('event_lineup_states') }}
),

defenses AS (
    SELECT *
    FROM {{ ref('event_fielding_states') }}
),

double_plays AS (
    SELECT *
    FROM {{ ref('event_double_plays') }}
),

advances AS (
    SELECT *
    FROM {{ ref('stg_event_baserunning_advance_attempts') }}
),

rbi AS (
    SELECT
        event_key,
        COUNT(*) AS runs_batted_in
    FROM advances
    WHERE rbi_flag
    GROUP BY 1
),

add_ids AS (
    SELECT
        plate_appearances.*,
        lineups.team_id AS batting_team_id,
        lineups.player_id AS batter_id,
        defenses.team_id AS pitching_team_id,
        defenses.player_id AS pitcher_id,
        event_base.game_id
    FROM plate_appearances
    INNER JOIN lineups USING (event_key)
    INNER JOIN defenses USING (event_key)
    INNER JOIN event_base USING (event_key)
    WHERE lineups.is_at_bat
        AND defenses.fielding_position = 1
),

final AS (
    SELECT
        add_ids.game_id,
        add_ids.event_key,
        add_ids.batter_id,
        add_ids.batting_team_id,
        add_ids.pitcher_id,
        add_ids.pitching_team_id,

        1 AS plate_appearances,
        result_types.is_at_bat::INT AS at_bats,
        result_types.is_hit::INT AS hits,
        (result_types.total_bases = 1)::INT AS singles,
        (result_types.total_bases = 2)::INT AS doubles,
        (result_types.total_bases = 3)::INT AS triples,
        (result_types.total_bases = 4)::INT AS home_runs,
        result_types.total_bases,

        (result_types.plate_appearance_result = 'StrikeOut')::INT AS strikeouts,
        (result_types.plate_appearance_result IN ('Walk', 'IntentionalWalk'))::INT AS walks,
        (result_types.plate_appearance_resuzlt = 'IntentionalWalk')::INT AS intentional_walks,
        (result_types.plate_appearance_result = 'HitByPitch')::INT AS hit_by_pitches,
        (result_types.plate_appearance_result = 'SacrificeFly')::INT AS sacrifice_flies,
        (result_types.plate_appearance_result = 'SacrificeHit')::INT AS sacrifice_hits,
        (result_types.plate_appearance_result = 'FieldersChoice')::INT AS fielders_choices,
        (result_types.plate_appearance_result = 'ReachedOnError')::INT AS reached_on_errors,
        (result_types.plate_appearance_result = 'Interference')::INT AS reached_on_interferences,

        result_types.is_on_base_opportunity::INT AS on_base_opportunities,

        result_types.is_on_base_success::INT AS on_base_successes,
        COALESCE(rbi.runs_batted_in, 0) AS runs_batted_in,
        COALESCE(double_plays.is_ground_ball_double_play::INT, 0) AS ground_ball_double_plays,

        COALESCE(double_plays.is_double_play::INT, 0) AS double_plays,
        COALESCE(double_plays.is_triple_play::INT, 0) AS triple_plays,
        -- The extra out from GIDPs is attributed to the batter,
        -- but for other types of double plays, the other out
        -- is considered to be a baserunning out.
        result_types.is_batting_out::INT + ground_ball_double_plays AS batting_outs

    FROM add_ids
    INNER JOIN result_types USING (plate_appearance_result)
    LEFT JOIN double_plays USING (event_key)
    LEFT JOIN rbi USING (event_key)
)

SELECT * FROM final
