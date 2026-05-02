MODEL (
  name main_models.stg_event_baserunners,
  kind FULL,
  description 'Event-level info for each baserunner present at an event. This includes all baserunners present at the start of the event, as well as the batter if they end up active on the bases during the play.',
  grain (event_key, baserunner),
  columns (
    game_id VARCHAR,
    event_id UTINYINT,
    event_key UINTEGER,
    baserunner BASERUNNER,
    runner_lineup_position UTINYINT,
    runner_id VARCHAR,
    charge_event_id UINTEGER,
    reached_on_event_id UINTEGER,
    explicit_charged_pitcher_id VARCHAR,
    attempted_advance_to_base BASE,
    baserunning_play_type BASERUNNING_PLAY,
    is_out BOOLEAN,
    base_end BASE,
    advanced_on_error_flag BOOLEAN,
    explicit_out_flag BOOLEAN,
    run_scored_flag BOOLEAN,
    rbi_flag BOOLEAN,
    reached_on_event_key UINTEGER,
    charge_event_key UINTEGER,
    baserunner_bit INTEGER,
    is_advance_attempt BOOLEAN
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    event_id = @doc('event_id'),
    event_key = @doc('event_key'),
    baserunner = @doc('baserunner'),
    runner_lineup_position = @doc('lineup_position'),
    runner_id = @doc('player_id'),
    charge_event_id = @doc('charge_event_id'),
    reached_on_event_id = @doc('reached_on_event_id'),
    explicit_charged_pitcher_id = @doc('explicit_charged_pitcher_id'),
    attempted_advance_to_base = 'Populated for events where the runner attempts to advance to a base. In specific cases, this can be identical the initial base, such as when a runner is picked off or otherwise put out after trying to get back to the bag.',
    baserunning_play_type = 'Populated for events that specifically have a baserunning play, as opposed to an advance on a batting play. Some plays apply to all runners present, while others apply to a specific runner. See `seed_baserunning_play_types` for more info.',
    is_out = 'Whether or not the runner is out at the end of the play',
    base_end = 'The runner''s base at the end of the play, if applicable',
    advanced_on_error_flag = 'True if the runner advanced on an error specifically associated with a play on this runner, as opposed to a play on the batter.',
    explicit_out_flag = 'True if the raw data specifically described the runner as out on an advance.',
    run_scored_flag = 'True if the runner scored on the play',
    rbi_flag = 'True if the runner scored and the batter was credited with the RBI',
    reached_on_event_key = @doc('reached_on_event_key'),
    charge_event_key = @doc('charge_event_key'),
    baserunner_bit = 'The bitwise representation of the runner''s position: - 1st: 1 (001) - 2nd: 2 (010) - 3rd: 4 (100) This is a useful way to represent the runner''s position because you can do a bitwise OR to get the full base state for the event.',
    is_advance_attempt = 'True if the runner attempted to advance on the play. Includes plays where the runner is subject to a force play.'
  ),
  audits (
    relationships(column := game_id, to_column := game_id, to_model := main_models.stg_games),
    relationships(column := event_key, to_column := event_key, to_model := main_models.stg_events)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_event_baserunners.parquet'
  ),
);







WITH source AS (
    SELECT * FROM event.event_baserunners
),

renamed AS (
    SELECT
        game_id,
        event_id,
        event_key,
        baserunner,
        runner_lineup_position,
        runner_id,
        charge_event_id,
        reached_on_event_id,
        explicit_charged_pitcher_id,
        attempted_advance_to_base,
        baserunning_play_type,
        is_out,
        base_end,
        advanced_on_error_flag,
        explicit_out_flag,
        run_scored_flag,
        rbi_flag,
        @event_id_to_key(reached_on_event_id, event_key) AS reached_on_event_key,
        @event_id_to_key(charge_event_id, event_key) AS charge_event_key,
        -- Bitwise agg of this gives us the full base_state
        CASE baserunner
            WHEN 'First' THEN 1
            WHEN 'Second' THEN 2
            WHEN 'Third' THEN 4
        END AS baserunner_bit,
        attempted_advance_to_base IS NOT NULL AS is_advance_attempt,

    FROM source
)

SELECT * FROM renamed
