WITH source AS (
    SELECT * FROM {{ source('baseballdatabank', 'appearances') }}
),

renamed AS (
    SELECT
        yearid AS year_id,
        teamid AS team_id,
        lgid AS league_id,
        playerid AS databank_player_id,
        g_all AS games_all,
        gs AS games_started,
        g_batting AS games_batting,
        g_defense AS games_defense,
        g_p AS games_pitcher,
        g_c AS games_catcher,
        g_1b AS games_first_base,
        g_2b AS games_second_base,
        g_3b AS games_third_base,
        g_ss AS games_shortstop,
        g_lf AS games_left_field,
        g_cf AS games_center_field,
        g_rf AS games_right_field,
        g_of AS games_outfield,
        g_dh AS games_designated_hitter,
        g_ph AS games_pinch_hitter,
        g_pr AS games_pinch_runner
    FROM source
)

SELECT * FROM renamed
