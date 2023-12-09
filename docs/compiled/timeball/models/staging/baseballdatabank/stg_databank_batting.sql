WITH source AS (
    SELECT * FROM "timeball"."baseballdatabank"."batting"
),

renamed AS (

    SELECT
        player_id AS databank_player_id,
        year_id AS season,
        stint,
        team_id AS team_id,
        lg_id AS league_id,
        g AS games,
        ab AS at_bats,
        r AS runs,
        h AS hits,
        _2b AS doubles, -- noqa: RF06
        _3b AS triples, -- noqa: RF06
        hr AS home_runs,
        rbi AS runs_batted_in,
        sb AS stolen_bases,
        cs AS caught_stealing,
        bb AS walks,
        so AS strikeouts,
        ibb AS intentional_walks,
        hbp AS hit_by_pitches,
        sh AS sacrifice_hits,
        sf AS sacrifice_flies,
        gidp AS grounded_into_double_plays,
        hits - home_runs - triples - doubles AS singles,
        singles + doubles * 2 + triples * 3 + home_runs * 4 AS total_bases,
        at_bats + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0) + COALESCE(sacrifice_flies, 0)
        + COALESCE(sacrifice_hits, 0)
        AS plate_appearances,
        at_bats + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0) + COALESCE(sacrifice_flies, 0)
        AS on_base_opportunities,
        hits + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0) AS on_base_successes,
    FROM source

)

SELECT * FROM renamed