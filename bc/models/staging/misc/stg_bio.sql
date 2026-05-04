MODEL (
  name main_models.stg_bio,
  kind FULL,
  description 'Staging table for Retrosheet''s `bio.csv` file, which contains basic demographic information about players, managers, coaches, and umpires.',
  grain (player_id),
  columns (
    player_id VARCHAR,
    last_name VARCHAR,
    official_name VARCHAR,
    first_name VARCHAR,
    birth_date VARCHAR,
    birth_city VARCHAR,
    birth_state VARCHAR,
    birth_country VARCHAR,
    player_debut_date VARCHAR,
    player_last_game_date VARCHAR,
    manager_debut_date VARCHAR,
    manager_last_game_date VARCHAR,
    coach_debut_date VARCHAR,
    coach_last_game_date VARCHAR,
    umpire_debut_date VARCHAR,
    umpire_last_game_date VARCHAR,
    death_date VARCHAR,
    death_city VARCHAR,
    death_state VARCHAR,
    death_country VARCHAR,
    bats VARCHAR,
    throws VARCHAR,
    height_inches INTEGER,
    weight_pounds INTEGER,
    cemetery_name VARCHAR,
    cemetery_city VARCHAR,
    cemetery_state VARCHAR,
    cemetery_country VARCHAR,
    cemetery_note VARCHAR,
    birth_name VARCHAR,
    name_change_notes VARCHAR,
    batting_hand_change_notes VARCHAR,
    hall_of_fame_status VARCHAR
  ),
  column_descriptions (
    player_id = @doc('player_id')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_bio.parquet'
  ),
);







WITH source AS (
    SELECT * FROM misc.bio
),

renamed AS (
    SELECT
        player_id,
        last AS last_name,
        first AS official_name,
        nickname AS first_name,
        birthdate AS birth_date,
        birth_city,
        birth_state,
        birth_country,
        play_debut AS player_debut_date,
        play_lastgame AS player_last_game_date,
        mgr_debut AS manager_debut_date,
        mgr_lastgame AS manager_last_game_date,
        coach_debut AS coach_debut_date,
        coach_lastgame AS coach_last_game_date,
        ump_debut AS umpire_debut_date,
        ump_lastgame AS umpire_last_game_date,
        deathdate AS death_date,
        death_city,
        death_state,
        death_country,
        bats,
        throws,
        CASE WHEN height LIKE '%-%'
            THEN SPLIT_PART(height, '-', 1)::INT * 12 + SPLIT_PART(height, '-', 2)::INT
        END AS height_inches,
        weight::INT AS weight_pounds,
        cemetary AS cemetery_name,
        ceme_city AS cemetery_city,
        ceme_state AS cemetery_state,
        ceme_country AS cemetery_country,
        ceme_note AS cemetery_note,
        birth_name,
        name_chg AS name_change_notes,
        bat_chg AS batting_hand_change_notes,
        hof AS hall_of_fame_status

    FROM source
)

SELECT * FROM renamed
