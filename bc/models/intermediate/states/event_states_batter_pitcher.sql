MODEL (
  name main_models.event_states_batter_pitcher,
  kind FULL,
  grain (event_key),
  columns (
    game_id VARCHAR,
    event_key UINTEGER,
    batting_side SIDE,
    fielding_side SIDE,
    batting_team_id TEAM_ID,
    fielding_team_id TEAM_ID,
    batter_id VARCHAR,
    batter_lineup_position UTINYINT,
    batter_fielding_position UTINYINT,
    pitcher_id VARCHAR,
    batter_hand HAND,
    pitcher_hand HAND,
    strikeout_responsible_batter_id VARCHAR,
    walk_responsible_pitcher_id VARCHAR
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    event_key = @doc('event_key'),
    batting_side = @doc('batting_side'),
    batter_id = @doc('batter_id'),
    pitcher_id = @doc('pitcher_id')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_event_states_batter_pitcher.parquet'
  ),
);







WITH joined AS (
    SELECT
        events.game_id,
        events.event_key,
        events.batting_side,
        CASE WHEN events.batting_side = 'Home' THEN 'Away' ELSE 'Home' END::SIDE AS fielding_side,
        events.batting_team_id,
        events.fielding_team_id,
        events.batter_id,
        events.batter_lineup_position,
        batter_field.fielding_position AS batter_fielding_position,
        events.pitcher_id,
        CASE
            WHEN events.specified_batter_hand IS NOT NULL THEN events.specified_batter_hand
            WHEN batters.bats = 'B' AND pitchers.throws = 'L' THEN 'R'
            WHEN batters.bats = 'B' AND pitchers.throws = 'R' THEN 'L'
            ELSE NULLIF(batters.bats, 'B')
        END::HAND AS batter_hand,
        CASE
            WHEN events.specified_pitcher_hand IS NOT NULL THEN events.specified_pitcher_hand
            WHEN pitchers.throws = 'B' AND batter_hand = 'L' THEN 'R'
            WHEN pitchers.throws = 'B' AND batter_hand = 'R' THEN 'L'
            ELSE NULLIF(pitchers.throws, 'B')
        END::HAND AS pitcher_hand,
        events.strikeout_responsible_batter_id,
        events.walk_responsible_pitcher_id,
        {# fielders.catcher_id,
        fielders.first_base_id,
        fielders.second_base_id,
        fielders.third_base_id,
        fielders.shortstop_id,
        fielders.left_field_id,
        fielders.center_field_id,
        fielders.right_field_id #}
    FROM main_models.stg_events AS events
    {# INNER JOIN main_models.event_fielders_flat AS fielders USING (event_key) #}
    LEFT JOIN main_models.people AS batters
        ON events.batter_id = batters.player_id
    LEFT JOIN main_models.people AS pitchers
        ON events.pitcher_id = pitchers.player_id
    LEFT JOIN main_models.stg_game_fielding_appearances AS batter_field
        ON events.game_id = batter_field.game_id
            AND events.batter_id = batter_field.player_id
            AND events.event_id BETWEEN batter_field.start_event_id AND batter_field.end_event_id
),

the_singular_exception_of_shohhei_ohtani AS (
    SELECT *
    FROM joined
    -- Choose DH when he's both, just because
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_key ORDER BY batter_fielding_position DESC) = 1
)


SELECT * FROM the_singular_exception_of_shohhei_ohtani
