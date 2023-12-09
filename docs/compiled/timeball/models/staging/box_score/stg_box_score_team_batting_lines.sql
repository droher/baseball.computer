WITH source AS (
    SELECT * FROM "timeball"."box_score"."box_score_team_batting_lines"
),

renamed AS (
    SELECT
        game_id,
        side,
        at_bats,
        runs,
        hits,
        doubles,
        triples,
        home_runs,
        rbi,
        sacrifice_hits,
        sacrifice_flies,
        hit_by_pitch AS hit_by_pitches,
        walks,
        intentional_walks,
        strikeouts,
        stolen_bases,
        caught_stealing,
        grounded_into_double_plays,
        reached_on_interference
    FROM source
)

SELECT * FROM renamed