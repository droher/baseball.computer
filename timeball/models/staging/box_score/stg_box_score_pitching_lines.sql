WITH source AS (
    SELECT * FROM {{ source('box_score', 'box_score_pitching_lines') }}
),

renamed AS (
    SELECT
        game_id,
        pitcher_id,
        side,
        nth_pitcher,
        outs_recorded,
        no_out_batters,
        batters_faced,
        hits,
        doubles,
        triples,
        home_runs,
        runs,
        earned_runs,
        walks,
        intentional_walks,
        strikeouts,
        -- TODO: Change in original
        hit_batsmen AS hit_by_pitches,
        wild_pitches,
        balks,
        sacrifice_hits,
        sacrifice_flies,
        hits - doubles - triples - home_runs AS singles,
        singles + doubles * 2 + triples * 3 + home_runs * 4 AS total_bases,
        -- This is a different formula vs batting lines because we don't have at bats.
        -- The one missing piece is catcher's interferfence, which is extremely rare 
        -- if not non-existent in box-score only games.
        batters_faced - COALESCE(sacrifice_hits, 0) AS on_base_opportunities,
        hits + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0) AS on_base_successes,
    FROM source
)

SELECT * FROM renamed