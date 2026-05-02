MODEL (
  name main_models.people,
  kind FULL,
  description 'Table containing biographical data on each player, coach, umpire, or manager for whom we have data.',
  grain (person_id),
  columns (
    person_id VARCHAR,
    player_id VARCHAR,
    baseball_reference_player_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    bats VARCHAR,
    throws VARCHAR,
    birth_year SMALLINT,
    official_name VARCHAR,
    birth_date VARCHAR,
    birth_city VARCHAR,
    birth_state VARCHAR,
    birth_country VARCHAR,
    height_inches DOUBLE,
    weight_pounds INTEGER
  ),
  column_descriptions (
    person_id = @doc('person_id'),
    player_id = @doc('player_id')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_people.parquet'
  ),
);







WITH roster_files AS (
    SELECT
        player_id,
        MODE(bats) AS bats,
        MODE(throws) AS throws,
    FROM main_models.stg_rosters
    GROUP BY 1
),

box_files AS (
      SELECT DISTINCT batter_id AS player_id FROM main_models.stg_box_score_batting_lines
      UNION
      SELECT DISTINCT fielder_id AS player_id FROM main_models.stg_box_score_fielding_lines
),

joined AS (
    SELECT
        COALESCE(
            retro.player_id,
            databank.retrosheet_player_id,
            roster_files.player_id,
            box_files.player_id
        ) AS person_id,
        databank.baseball_reference_player_id,
        COALESCE(retro.first_name, databank.first_name) AS first_name,
        COALESCE(retro.last_name, databank.last_name) AS last_name,
        COALESCE(retro.bats, databank.bats, roster_files.bats) AS bats,
        COALESCE(retro.throws, databank.throws, roster_files.throws) AS throws,
        databank.birth_year,
        retro.official_name,
        retro.birth_date,
        retro.birth_city,
        retro.birth_state,
        retro.birth_country,
        COALESCE(retro.height_inches, databank.height_inches) AS height_inches,
        COALESCE(retro.weight_pounds, databank.weight_pounds) AS weight_pounds
    FROM main_models.stg_bio AS retro
    FULL OUTER JOIN roster_files USING (player_id)
    FULL OUTER JOIN box_files USING (player_id)
    FULL OUTER JOIN main_models.stg_people AS databank
        ON databank.retrosheet_player_id = COALESCE(retro.player_id, roster_files.player_id, box_files.player_id)
    WHERE COALESCE(
            retro.player_id,
            databank.retrosheet_player_id,
            roster_files.player_id,
            box_files.player_id
        ) IS NOT NULL
),

final AS (
    SELECT
        CASE WHEN person_id SIMILAR TO '[a-z]{5}[01][0-9]{2}' THEN person_id ELSE NULL END AS player_id,
        *
    FROM joined
)

SELECT * FROM final
