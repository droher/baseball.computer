WITH source AS (
    SELECT * FROM {{ source('baseballdatabank', 'pitching') }}
),

renamed AS (
    SELECT
        playerid AS databank_player_id,
        yearid AS year_id,
        stint,
        teamid AS team_id,
        lgid AS league_id,
        w AS wins,
        l AS losses,
        g AS games,
        gs AS games_started,
        cg AS complete_games,
        sho AS shutouts,
        sv AS saves,
        ipouts AS outs_pitched,
        h AS hits,
        er AS earned_runs,
        hr AS home_runs,
        bb AS walks,
        so AS strikeouts,
        baopp AS opponent_batting_average,
        era AS earned_run_average,
        ibb AS intentional_walks,
        wp AS wild_pitches,
        hbp AS hit_by_pitches,
        bk AS balks,
        bfp AS batters_faced,
        gf AS games_finished,
        r AS runs,
        sh AS sacrifice_hits,
        sf AS sacrifice_flies,
        gidp AS ground_into_double_plays
    FROM source
)

SELECT * FROM renamed