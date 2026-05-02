MODEL (
  name main_models.event_double_plays,
  kind FULL,
  description 'A small helper table designed to flag double and triple plays.',
  grain (event_key),
  columns (
    event_key UINTEGER,
    is_double_play BOOLEAN,
    is_triple_play BOOLEAN,
    is_ground_ball_double_play BOOLEAN
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    is_double_play = 'Whether the event is a double play.',
    is_triple_play = 'Whether the event is a triple play.',
    is_ground_ball_double_play = 'Whether the event is a ground ball double play.'
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_event_double_plays.parquet'
  ),
);







WITH final AS (
    SELECT
        flags.event_key,
        BOOL_OR(dp_flag_types.is_double_play) AS is_double_play,
        BOOL_OR(dp_flag_types.is_triple_play) AS is_triple_play,
        BOOL_OR(
            dp_flag_types.is_ground_ball_double_play
        ) AS is_ground_ball_double_play
    FROM main_models.stg_event_flags AS flags
    INNER JOIN main_seeds.seed_double_play_flag_types AS dp_flag_types USING (flag)
    GROUP BY 1
)

SELECT * FROM final
