WITH source AS (
    SELECT * FROM "timeball"."box_score"."box_score_batting_lines"
),

renamed AS (
    SELECT
        game_id,
        batter_id,
        side,
        lineup_position,
        nth_player_at_position,
        at_bats,
        runs,
        hits,
        doubles,
        triples,
        home_runs,
        -- TODO: Change in source
        rbi AS runs_batted_in,
        sacrifice_hits,
        sacrifice_flies,
        -- TODO: Change in source
        hit_by_pitch AS hit_by_pitches,
        walks,
        intentional_walks,
        strikeouts,
        stolen_bases,
        caught_stealing,
        grounded_into_double_plays,
        -- TODO: Change in source
        reached_on_interference AS reached_on_interferences,
        -- TODO: Fix rows with 0 hits but 1+ XBH and where hits > AB
        CASE WHEN hits = 0 AND doubles + triples + home_runs > 0 THEN NULL 
            ELSE hits - home_runs - triples - doubles
        END::UTINYINT AS singles,
        (singles + doubles * 2 + triples * 3 + home_runs * 4)::UTINYINT AS total_bases,
        (at_bats + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0) + COALESCE(sacrifice_flies, 0)
        + COALESCE(sacrifice_hits, 0) + COALESCE(reached_on_interferences, 0))::UTINYINT
        AS plate_appearances,
        (at_bats + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0) + COALESCE(sacrifice_flies, 0))::UTINYINT
        AS on_base_opportunities,
        (hits + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0))::UTINYINT AS on_base_successes,
        CASE WHEN hits > at_bats THEN NULL
            ELSE at_bats - hits + COALESCE(grounded_into_double_plays, 0)
        END::UTINYINT AS batting_outs,
    FROM source
)

SELECT * FROM renamed