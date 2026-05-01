WITH source AS (
    SELECT * FROM {{ source('box_score', 'box_score_team_batting_lines') }}
),

renamed AS (
    SELECT
        game_id,
        side,
        at_bats::BIGINT AS at_bats,
        runs::BIGINT AS runs,
        hits::BIGINT AS hits,
        doubles::BIGINT AS doubles,
        triples::BIGINT AS triples,
        home_runs::BIGINT AS home_runs,
        rbi::BIGINT AS rbi,
        sacrifice_hits::BIGINT AS sacrifice_hits,
        sacrifice_flies::BIGINT AS sacrifice_flies,
        hit_by_pitch::BIGINT AS hit_by_pitches,
        walks::BIGINT AS walks,
        intentional_walks::BIGINT AS intentional_walks,
        strikeouts::BIGINT AS strikeouts,
        stolen_bases::BIGINT AS stolen_bases,
        caught_stealing::BIGINT AS caught_stealing,
        grounded_into_double_plays::BIGINT AS grounded_into_double_plays,
        reached_on_interference::BIGINT AS reached_on_interference
    FROM source
)

SELECT * FROM renamed
