WITH source AS (
    SELECT * FROM {{ source('baseballdatabank', 'batting') }}
),

renamed AS (

    SELECT
        playerid AS databank_player_id,
        yearid AS year_id,
        stint,
        teamid AS team_id,
        lgid AS league_id,
        g AS games,
        ab AS at_bats,
        r AS runs,
        h AS hits,
        "2B" AS doubles, -- noqa: RF06
        "3B" AS triples, -- noqa: RF06
        hr AS home_runs,
        rbi,
        sb AS stolen_bases,
        cs AS caught_stealing,
        bb AS walks,
        so AS strikeouts,
        ibb AS intentional_walks,
        hbp AS hit_by_pitches,
        sh AS sacrifice_hits,
        sf AS sacrifice_flies,
        gidp AS grounded_into_double_plays
    FROM source

)

SELECT * FROM renamed
