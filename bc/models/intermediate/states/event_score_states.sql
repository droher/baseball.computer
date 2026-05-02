MODEL (
  name main_models.event_score_states,
  kind FULL,
  grain (event_key),
  columns (
    event_key UINTEGER,
    score_home_start UTINYINT,
    score_away_start UTINYINT,
    score_home_end UTINYINT,
    score_away_end UTINYINT,
    runs_on_play UTINYINT
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    score_home_start = @doc('score_home_start'),
    score_away_start = @doc('score_away_start'),
    score_home_end = @doc('score_home_end'),
    score_away_end = @doc('score_away_end'),
    runs_on_play = @doc('runs_on_play')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_event_score_states.parquet'
  ),
);







WITH windowed AS (
    SELECT
        e.event_key,
        SUM(e.runs_on_play) FILTER (WHERE e.batting_side = 'Home') OVER start_event AS score_home_start,
        SUM(e.runs_on_play) FILTER (WHERE e.batting_side = 'Away') OVER start_event AS score_away_start,
        SUM(e.runs_on_play) FILTER (WHERE e.batting_side = 'Home') OVER end_event AS score_home_end,
        SUM(e.runs_on_play) FILTER (WHERE e.batting_side = 'Away') OVER end_event AS score_away_end,
        e.runs_on_play,
    FROM main_models.stg_events AS e
    WINDOW
        start_event AS (
            PARTITION BY e.game_id
            ORDER BY e.event_key
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ),
        end_event AS (
            PARTITION BY e.game_id
            ORDER BY e.event_key
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
),

final AS (
    SELECT
        event_key,
        COALESCE(score_home_start, 0)::UTINYINT AS score_home_start,
        COALESCE(score_away_end, 0)::UTINYINT AS score_away_start,
        COALESCE(score_home_end, 0)::UTINYINT AS score_home_end,
        COALESCE(score_away_end, 0)::UTINYINT AS score_away_end,
        runs_on_play
    FROM windowed
)

SELECT * FROM final
