MODEL (
  name main_models.stg_schedule,
  kind FULL,
  columns (
    date TIMESTAMP,
    double_header SMALLINT,
    day_of_week VARCHAR,
    visiting_team VARCHAR,
    visiting_team_league VARCHAR,
    visiting_team_game_number SMALLINT,
    home_team VARCHAR,
    home_team_league VARCHAR,
    home_team_game_number INTEGER,
    day_night VARCHAR,
    postponement_indicator VARCHAR,
    makeup_dates VARCHAR,
    park_id PARK_ID
  ),
  column_descriptions (
    date = @doc('date'),
    park_id = @doc('park_id')
  ),
);






WITH source AS (
    SELECT * FROM misc.schedule
),

renamed AS (
    SELECT
        date,
        double_header,
        day_of_week,
        visiting_team,
        visiting_team_league,
        visiting_team_game_number,
        home_team,
        home_team_league,
        home_team_game_number,
        day_night,
        postponement_indicator,
        makeup_dates,
        park_id

    FROM source
)

SELECT * FROM renamed
