MODEL (
  name main_models.stg_events,
  kind FULL,
  description 'The single most important staging table in the database, `stg_events` has one row for each event in the Retrosheet play-by-play data. An "event" has no inherent meaning in baseball, but is instead a Retrosheet-specific concept that represents discrete sets of plays. An event can be categorized into one of three types: - Plate appearance result - Baserunning-only play(s) - No play A plate appearance result can also include baserunning plays, but an event never contains more than one plate appearance. No-plays serve as markers in the data to place substitutions. In cases where multiple baserunning plays or substitutions occur over a single plate apperance, the PA is split across those events, but is considered to have occurred on the final event. In the case of a baserunning play ending an inning, it is also possible that a plate appearance to not occur at all. `stg_events` has the following information: - The game state (inning, frame, outs, runners on, batter up, etc.) - The plate appearance result (if applicable) - The batted ball information (if applicable) - Basic results of the event (runs scored, outs recorded) - Other rare miscellaneous information (e.g. the batter batting from an unusual side) See the Retrosheet event file spec: https://www.retrosheet.org/eventfile.htm As well as the batted ball location diagram: https://www.retrosheet.org/location.htm',
  grain (event_key),
  columns (
    game_id VARCHAR,
    event_id UTINYINT,
    event_key UINTEGER,
    batting_side SIDE,
    inning UTINYINT,
    frame FRAME,
    batter_lineup_position UTINYINT,
    batter_id VARCHAR,
    pitcher_id VARCHAR,
    batting_team_id TEAM_ID,
    fielding_team_id TEAM_ID,
    outs UTINYINT,
    count_balls UTINYINT,
    count_strikes UTINYINT,
    base_state UTINYINT,
    specified_batter_hand HAND,
    specified_pitcher_hand HAND,
    strikeout_responsible_batter_id VARCHAR,
    walk_responsible_pitcher_id VARCHAR,
    plate_appearance_result PLATE_APPEARANCE_RESULT,
    batted_trajectory TRAJECTORY,
    batted_to_fielder UTINYINT,
    batted_location_general LOCATION_GENERAL,
    batted_location_depth LOCATION_DEPTH,
    batted_location_angle LOCATION_ANGLE,
    batted_contact_strength VARCHAR,
    outs_on_play UTINYINT,
    runs_on_play UTINYINT,
    runs_batted_in UTINYINT,
    team_unearned_runs UTINYINT,
    no_play_flag BOOLEAN,
    date DATE,
    season SMALLINT,
    runners_count TINYINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    event_id = @doc('event_id'),
    event_key = @doc('event_key'),
    batting_side = @doc('batting_side'),
    inning = @doc('inning'),
    frame = @doc('frame'),
    batter_lineup_position = @doc('lineup_position'),
    batter_id = @doc('batter_id'),
    pitcher_id = @doc('pitcher_id'),
    batting_team_id = @doc('team_id'),
    fielding_team_id = @doc('team_id'),
    outs = 'The number of outs at the start of the event.',
    count_balls = 'The number of balls at the time that the event occurred.',
    count_strikes = 'The number of strikes at the time that the event occurred.',
    base_state = 'A binary representation of the baserunners present at the start of the event, represented as a 0-7 base 10 integer: ``` 0 - Bases empty (000) 1 - Runner on 1st (001) 2 - Runner on 2nd (010) 3 - Runner on 1st and 2nd (011) 4 - Runner on 3rd (100) 5 - Runner on 1st and 3rd (101) 6 - Runner on 2nd and 3rd (110) 7 - Bases loaded (111) ``` Representing the base state this way might seem counterintuitive at first, but it ends up being very useful and easier to work with in many cases. It''s also a common convention. See `seed_base_state` for more info.',
    specified_batter_hand = 'A rare data point that indicates the batter batted from a different side than he would normally. In all other cases, roster/bio data is more authoritative.',
    specified_pitcher_hand = 'A rare data point that indicates the pitcher threw from a different side than he would normally. In all other cases, roster/bio data is more authoritative. This is mostly just Pat Venditte against switch hitters.',
    strikeout_responsible_batter_id = 'This is the batter who, if the plate appearance ends in a strikeout, is charged with the strikeout. This is only applicable for mid-plate-appearance substitutions. There is a specific set of cases in which the original batter is charged with the strikeout. For all other statistics, the final batter is the one who is credited.',
    walk_responsible_pitcher_id = 'This is the pitcher who, if the plate appearance ends in a walk, is charged with the walk. This is only applicable for mid-plate-appearance substitutions. There is a specific set of cases in which the original pitcher is charged with the walk. For all other statistics, the final pitcher is the one who is credited.',
    plate_appearance_result = @doc('plate_appearance_result'),
    batted_trajectory = 'An enum indicating the trajectory of the batted ball. This field is *always* populated for plate appearances that end in a batted ball, regardless of whether we know the trajectory. It is filled with `Unknown` in those cases where no trajectory was recorded. This data is sparse prior to 1988, but many of the missing values can be deduced based on other details.',
    batted_to_fielder = @doc('batted_to_fielder'),
    batted_location_general = @doc('batted_location_general'),
    batted_location_depth = @doc('batted_location_depth'),
    batted_location_angle = @doc('batted_location_angle'),
    batted_contact_strength = @doc('batted_contact_strength'),
    outs_on_play = @doc('outs_on_play'),
    runs_on_play = @doc('runs_on_play'),
    runs_batted_in = @doc('runs_batted_in'),
    team_unearned_runs = @doc('team_unearned_runs'),
    no_play_flag = 'True if nothing happened on the play. This will be false in the specific case of an error on a foul ball, which is marked as a no-play in the Retrosheet data but is not treated that way for our purposes. This field is a useful way to filter out unneeded events, especially in the context of determining the personnel in a game.',
    date = @doc('date'),
    season = @doc('season'),
    runners_count = 'The total number of runners on base at the start of the event. Redundant with `base_state`, but provided for convenience.'
  ),
  audits (
    relationships(column := game_id, to_column := game_id, to_model := main_models.stg_games)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_events.parquet'
  ),
);







WITH source AS (
    SELECT * FROM event.events
),

renamed AS (
    SELECT
        game_id,
        event_id,
        event_key,
        -- TODO: Add plate appearance id
        batting_side,
        inning,
        frame,
        batter_lineup_position,
        batter_id,
        pitcher_id,
        batting_team_id,
        fielding_team_id,
        outs,
        count_balls,
        count_strikes,
        base_state,
        LEFT(specified_batter_hand, 1)::HAND AS specified_batter_hand,
        LEFT(specified_pitcher_hand, 1)::HAND AS specified_pitcher_hand,
        strikeout_responsible_batter_id,
        walk_responsible_pitcher_id,
        plate_appearance_result,
        batted_trajectory,
        batted_to_fielder,
        batted_location_general,
        batted_location_depth,
        batted_location_angle,
        batted_contact_strength,
        outs_on_play,
        runs_on_play,
        runs_batted_in,
        team_unearned_runs,
        no_play_flag,
        STRPTIME(SUBSTRING(game_id, 4, 8), '%Y%m%d')::DATE AS date,
        SUBSTRING(game_id, 4, 4)::INT2 AS season,
        BIT_COUNT(base_state) AS runners_count,
    from source
)

SELECT * from renamed
