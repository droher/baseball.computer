MODEL (
  name main_models.event_fielders_flat,
  kind FULL,
  grain (event_key),
  columns (
    game_id VARCHAR,
    event_id UTINYINT,
    event_key UINTEGER,
    personnel_fielding_key INTEGER,
    pitcher_id VARCHAR,
    catcher_id VARCHAR,
    first_base_id VARCHAR,
    second_base_id VARCHAR,
    third_base_id VARCHAR,
    shortstop_id VARCHAR,
    left_field_id VARCHAR,
    center_field_id VARCHAR,
    right_field_id VARCHAR
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    event_id = @doc('event_id'),
    event_key = @doc('event_key'),
    pitcher_id = @doc('pitcher_id')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_event_fielders_flat.parquet'
  ),
);







WITH fielders AS (
    SELECT
        personnel_fielding_key,
        ANY_VALUE(player_id) FILTER (WHERE fielding_position = 1)::PLAYER_ID AS pitcher_id,
        ANY_VALUE(player_id) FILTER (WHERE fielding_position = 2)::PLAYER_ID AS catcher_id,
        ANY_VALUE(player_id) FILTER (WHERE fielding_position = 3)::PLAYER_ID AS first_base_id,
        ANY_VALUE(player_id) FILTER (WHERE fielding_position = 4)::PLAYER_ID AS second_base_id,
        ANY_VALUE(player_id) FILTER (WHERE fielding_position = 5)::PLAYER_ID AS third_base_id,
        ANY_VALUE(player_id) FILTER (WHERE fielding_position = 6)::PLAYER_ID AS shortstop_id,
        ANY_VALUE(player_id) FILTER (WHERE fielding_position = 7)::PLAYER_ID AS left_field_id,
        ANY_VALUE(player_id) FILTER (WHERE fielding_position = 8)::PLAYER_ID AS center_field_id,
        ANY_VALUE(player_id) FILTER (WHERE fielding_position = 9)::PLAYER_ID AS right_field_id
    FROM main_models.personnel_fielding_states
    GROUP BY 1
),

final AS (
    SELECT
        epl.game_id,
        epl.event_id,
        epl.event_key,
        fielders.*
    FROM main_models.event_personnel_lookup AS epl
    INNER JOIN fielders USING (personnel_fielding_key)
)

SELECT * FROM final
