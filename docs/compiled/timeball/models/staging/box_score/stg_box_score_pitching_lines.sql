WITH source AS (
    SELECT * FROM "timeball"."box_score"."box_score_pitching_lines"
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
        -- TODO: Fix rows where XBH > H
        CASE WHEN hits::INT - (home_runs + triples + doubles) < 0 THEN NULL
            ELSE hits - (home_runs + triples + doubles)
        END AS singles,
        singles + doubles * 2 + triples * 3 + home_runs * 4 AS total_bases,
        -- This is a different formula vs batting lines because we don't have at bats.
        -- The one missing piece is catcher's interferfence, which is extremely rare 
        -- if not non-existent in box-score only games.
        batters_faced - COALESCE(sacrifice_hits, 0) AS on_base_opportunities,
        hits + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0) AS on_base_successes,
        CASE WHEN nth_pitcher = 1 THEN 1 ELSE 0 END AS games_started,
        CASE WHEN nth_pitcher != 1 THEN 1 ELSE 0 END AS games_relieved,
        CASE WHEN nth_pitcher = MAX(nth_pitcher) OVER (PARTITION BY game_id, side)
                THEN 1
            ELSE 0
        END AS games_finished,
    FROM source
)

SELECT * FROM renamed