MODEL (
  name main_models.personnel_fielding_states,
  kind FULL,
  grain (personnel_fielding_key, fielding_position),
  columns (
    game_id VARCHAR,
    fielding_team_id TEAM_ID,
    fielding_side SIDE,
    personnel_fielding_key INTEGER,
    start_event_id UTINYINT,
    end_event_id UINTEGER,
    player_id VARCHAR,
    fielding_position UTINYINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    player_id = @doc('player_id'),
    fielding_position = @doc('fielding_position')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_personnel_fielding_states.parquet'
  ),
);







-- We create the concept of a personnel state as a range of events
-- where none of the players in the lineup change. This allows us
-- to store personnel info at a lower cardinality than one row per event-player.
WITH ranges AS (
    SELECT DISTINCT
        game_id,
        side,
        start_event_id,
        COALESCE(
            MIN(start_event_id - 1) OVER w,
            end_event_id
        )::UINTEGER AS end_event_id
    FROM main_models.stg_game_fielding_appearances
    -- The MIN over this window is the next-largest value
    WINDOW w AS (
        PARTITION BY game_id, side
        ORDER BY start_event_id
        RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
    )
),

final AS (
    SELECT
        appearances.game_id,
        CASE WHEN appearances.side = 'Home' THEN games.home_team_id ELSE games.away_team_id END AS fielding_team_id,
        appearances.side AS fielding_side,
        -- INTEGER (not BIGINT) so downstream joins can hash on the raw column.
        -- Range: ±(max(game_key) + 255). game_key is a row id over stg_games
        -- (a few million max), well within INT range.
        ((games.game_key + ranges.start_event_id) * CASE WHEN appearances.side = 'Home' THEN 1 ELSE -1 END)::INTEGER
        AS personnel_fielding_key,
        ranges.start_event_id,
        ranges.end_event_id,
        appearances.player_id,
        appearances.fielding_position,
    FROM main_models.stg_game_fielding_appearances AS appearances
    INNER JOIN main_models.stg_games AS games USING (game_id)
    INNER JOIN ranges
        ON ranges.game_id = appearances.game_id
            AND ranges.side = appearances.side
            AND ranges.start_event_id <= appearances.end_event_id
            AND ranges.end_event_id >= appearances.start_event_id
    -- We need this to dedupe positions on multi-sub events. There's definitely
    -- a better way to to this that fails louder on real dupes.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY personnel_fielding_key, appearances.fielding_position
        ORDER BY appearances.end_event_id DESC, appearances.player_id, appearances.start_event_id
    ) = 1
)

SELECT * FROM final
