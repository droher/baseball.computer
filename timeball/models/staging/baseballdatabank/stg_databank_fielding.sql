WITH source AS (
    SELECT * FROM {{ source('baseballdatabank', 'fielding') }}
),

renamed AS (
    SELECT
        playerid AS databank_player_id,
        yearid AS year_id,
        stint,
        teamid AS team_id,
        lgid AS league_id,
        pos AS fielding_position,
        g AS games,
        gs AS games_started,
        innouts AS outs_played,
        po AS putouts,
        a AS assists,
        e AS errors,
        dp AS double_plays,
        pb AS passed_balls,
        wp AS wild_pitches,
        sb AS stolen_bases,
        cs AS caught_stealing,
    FROM source
)

SELECT * FROM renamed
