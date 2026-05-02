MODEL (
  name main_models.calc_fielding_play_agg,
  kind FULL,
  description 'Table containing one row for each fielder who made at least one fielding play on a given event. It serves as the canonical way to calculate fielding data for each fielder on each event. For the most part, the definitions involve conditions based on each individual fielding play, but some definitions require aggregate knowledge of the event as a whole.',
  grain (event_key, fielding_position),
  columns (
    event_key UINTEGER,
    fielding_position UTINYINT,
    game_id VARCHAR,
    putouts UTINYINT,
    assists UTINYINT,
    errors UTINYINT,
    fielders_choices UTINYINT,
    plays_started UTINYINT,
    assisted_putouts UTINYINT,
    first_errors BIGINT,
    unknown_putouts BIGINT,
    incomplete_events UTINYINT
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    fielding_position = @doc('fielding_position'),
    game_id = @doc('game_id'),
    putouts = @doc('putouts'),
    assists = @doc('assists'),
    errors = @doc('errors'),
    fielders_choices = @doc('fielders_choices'),
    plays_started = @doc('plays_started'),
    assisted_putouts = @doc('assisted_putouts'),
    first_errors = @doc('first_errors'),
    unknown_putouts = @doc('unknown_putouts'),
    incomplete_events = @doc('incomplete_events')
  ),
  audits (
    not_null(columns := (event_key, fielding_position)),
    unique_grain(columns := (event_key, fielding_position)),
    relationships(column := event_key, to_model := main_models.stg_events, to_column := event_key),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_calc_fielding_play_agg.parquet'
  ),
);







WITH grouper_init AS (
    SELECT
        *,
        COUNT(CASE WHEN fielding_play IN ('Putout', 'Error') THEN 1 END) OVER w AS group_id_init
    FROM main_models.stg_event_fielding_plays
    WINDOW w AS (PARTITION BY event_key ORDER BY sequence_id)
),

grouper AS (
    SELECT
        *,
        -- need to do this to properly put the putout/error in with its assists
        LAG(group_id_init, 1, 0) OVER w AS group_id
    FROM grouper_init
    WINDOW w AS (PARTITION BY event_key ORDER BY sequence_id)
),

assist_tracker AS (
    SELECT
        *,
        COUNT(CASE WHEN fielding_play = 'Assist' THEN 1 END) OVER w AS assists_in_group
    FROM grouper
    WINDOW w AS (PARTITION BY event_key, group_id)
),

add_batted_ball AS (
    SELECT
        assist_tracker.*,
        e.batted_location_general IS NOT NULL as is_batted_ball,
    FROM assist_tracker
    INNER JOIN main_models.stg_events AS e USING (event_key)
),

final AS (
    SELECT
        event_key,
        fielding_position,
        ANY_VALUE(game_id) AS game_id,
        COUNT(*) FILTER (WHERE fielding_play = 'Putout')::UTINYINT AS putouts,
        -- A fielder can appear multiple times in one segment (on a rundown)
        -- but is only credited with one assist per putout
        COUNT(DISTINCT CASE WHEN fielding_play = 'Assist' THEN group_id END)::UTINYINT AS assists,
        COUNT(*) FILTER (WHERE fielding_play = 'Error')::UTINYINT AS errors,
        COUNT(*) FILTER (WHERE fielding_play = 'FieldersChoice')::UTINYINT AS fielders_choices,
        COUNT(*) FILTER (WHERE sequence_id = 1 AND fielding_play != 'Error' AND is_batted_ball)::UTINYINT AS plays_started,
        -- An "unassisted putout" often refers specifically to ground balls, which we don't always know about.
        -- We also don't know if a putout by an unknown fielder was assisted or not (see below).
        -- So we'll just track when putouts are explicitly assisted
        COUNT(*) FILTER (WHERE assists_in_group > 0 AND fielding_play = 'Putout')::UTINYINT AS assisted_putouts,
        COUNT(*) FILTER (WHERE group_id = 0 AND fielding_play = 'Error') AS first_errors,
        -- We always know how many putouts occur on a play even when we don't know who made them...
        COUNT(*) FILTER (WHERE fielding_position = 0 AND fielding_play = 'Putout') AS unknown_putouts,
        -- ...But the same is not true for assists.
        -- Explicitly unknown assists are extremely rare in the data. An unknown putout also implies
        -- unknown assists (0 or more). So it just makes sense to count the total number of events
        -- where an unknown assist may have occurred.
        BOOL_OR(fielding_position = 0)::UTINYINT AS incomplete_events,
    FROM add_batted_ball
    GROUP BY 1, 2
)

SELECT * FROM final
