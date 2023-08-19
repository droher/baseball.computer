WITH source AS (
    SELECT * FROM {{ source('box_score', 'box_score_pinch_hitting_lines') }}
),

renamed AS (
    SELECT
        game_id,
        pinch_hitter_id,
        inning,
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
-- TODO: Eliminate 20 dupes in source
QUALIFY ROW_NUMBER() OVER (PARTITION BY game_id, side, pinch_hitter_id ORDER BY inning) = 1
