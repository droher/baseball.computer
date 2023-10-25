WITH roster_files AS (
    SELECT
        player_id,
        MODE(bats) AS bats,
        MODE(throws) AS throws,
    FROM {{ ref('stg_rosters') }}
    GROUP BY 1
),

box_files AS (
      SELECT DISTINCT batter_id AS player_id FROM box_score.box_score_batting_lines
      UNION
      SELECT DISTINCT fielder_id AS player_id FROM box_score.box_score_fielding_lines
),

final AS (
    SELECT
        COALESCE(
            retro.player_id,
            databank.retrosheet_player_id,
            roster_files.player_id,
            box_files.player_id
        ) AS player_id,
        COALESCE(retro.first_name, databank.first_name) AS first_name,
        COALESCE(retro.last_name, databank.last_name) AS last_name,
        COALESCE(retro.bats, databank.bats, roster_files.bats) AS bats,
        COALESCE(retro.throws, databank.throws, roster_files.throws) AS throws,
        retro.official_name,
        retro.birth_date,
        retro.birth_city,
        retro.birth_state,
        retro.birth_country,
        COALESCE(retro.height_inches, databank.height_inches) AS height_inches,
        COALESCE(retro.weight_pounds, databank.weight_pounds) AS weight_pounds
    FROM {{ ref('stg_bio') }} AS retro
    FULL OUTER JOIN roster_files USING (player_id)
    FULL OUTER JOIN box_files USING (player_id)
    FULL OUTER JOIN {{ ref('stg_people') }} AS databank
        ON databank.retrosheet_player_id = COALESCE(retro.player_id, roster_files.player_id, box_files.player_id)
)

SELECT * FROM final
