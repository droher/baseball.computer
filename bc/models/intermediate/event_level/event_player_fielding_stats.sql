MODEL (
  name main_models.event_player_fielding_stats,
  kind FULL,
  description 'Fielding statistics for each player in each event, regardless of whether the fielder was involved in the play. This is the largest table in the database by number of rows, so exercise caution when querying it.',
  grain (event_key, fielding_position),
  columns (
    event_key UINTEGER,
    fielding_position UTINYINT,
    game_id VARCHAR,
    player_id VARCHAR,
    team_id TEAM_ID,
    outs_played UTINYINT,
    plate_appearances_in_field UTINYINT,
    plate_appearances_in_field_with_ball_in_play UTINYINT,
    unknown_putouts_while_fielding UTINYINT,
    balls_hit_to UTINYINT,
    putouts UTINYINT,
    assists UTINYINT,
    errors UTINYINT,
    fielders_choices UTINYINT,
    assisted_putouts UTINYINT,
    in_play_putouts UTINYINT,
    in_play_assists UTINYINT,
    reaching_errors UTINYINT,
    stolen_bases UTINYINT,
    caught_stealing UTINYINT,
    pickoffs UTINYINT,
    passed_balls UTINYINT,
    double_plays UTINYINT,
    triple_plays UTINYINT,
    ground_ball_double_plays UTINYINT,
    double_plays_started UTINYINT,
    ground_ball_double_plays_started UTINYINT
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    fielding_position = @doc('fielding_position'),
    game_id = @doc('game_id'),
    player_id = @doc('player_id'),
    team_id = @doc('team_id'),
    outs_played = @doc('outs_played'),
    plate_appearances_in_field = @doc('plate_appearances_in_field'),
    plate_appearances_in_field_with_ball_in_play = @doc('plate_appearances_in_field_with_ball_in_play'),
    unknown_putouts_while_fielding = @doc('unknown_putouts_while_fielding'),
    balls_hit_to = @doc('balls_hit_to'),
    putouts = @doc('putouts'),
    assists = @doc('assists'),
    errors = @doc('errors'),
    fielders_choices = @doc('fielders_choices'),
    assisted_putouts = @doc('assisted_putouts'),
    in_play_putouts = @doc('in_play_putouts'),
    in_play_assists = @doc('in_play_assists'),
    reaching_errors = @doc('reaching_errors'),
    stolen_bases = @doc('stolen_bases'),
    caught_stealing = @doc('caught_stealing'),
    pickoffs = @doc('pickoffs'),
    passed_balls = @doc('passed_balls'),
    double_plays = @doc('double_plays'),
    triple_plays = @doc('triple_plays'),
    ground_ball_double_plays = @doc('ground_ball_double_plays'),
    double_plays_started = @doc('double_plays_started'),
    ground_ball_double_plays_started = @doc('ground_ball_double_plays_started')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_event_player_fielding_stats.parquet'
  ),
);







-- TODO: Unclear whether this should exist as such.
-- Per-event-fielding stats for positions not involved in the play
-- are only there for innings/PA played, which can be tabulated
-- in more efficient ways.
WITH final AS (
    SELECT
        e.event_key,
        p.fielding_position,
        p.game_id,
        p.player_id,
        p.fielding_team_id AS team_id,
        -- DHs are in this table, which makes the nomenclature for the 3 cols below
        -- a little ambigious, but keeping for now because it's useful to keep track of
        -- for them.
        e.outs_played,
        e.plate_appearances_in_field,
        e.plate_appearances_in_field_with_ball_in_play,
        e.unknown_putouts AS unknown_putouts_while_fielding,
        CASE WHEN e.batted_to_fielder = p.fielding_position THEN 1 ELSE 0 END::UTINYINT AS balls_hit_to,
        COALESCE(fp.putouts, 0)::UTINYINT AS putouts,
        COALESCE(fp.assists, 0)::UTINYINT AS assists,
        COALESCE(fp.errors, 0)::UTINYINT AS errors,
        COALESCE(fp.fielders_choices, 0)::UTINYINT AS fielders_choices,
        COALESCE(fp.assisted_putouts)::UTINYINT AS assisted_putouts,
        CASE WHEN e.plate_appearances_in_field_with_ball_in_play > 0
                THEN COALESCE(fp.putouts, 0)
            ELSE 0
        END::UTINYINT AS in_play_putouts,
        CASE WHEN e.plate_appearances_in_field_with_ball_in_play > 0
                THEN COALESCE(fp.assists, 0)
            ELSE 0
        END::UTINYINT AS in_play_assists,
        CASE WHEN fp.first_errors = 1 THEN e.reaching_errors ELSE 0 END::UTINYINT AS reaching_errors,
        CASE WHEN p.fielding_position IN (1, 2) THEN e.stolen_bases ELSE 0 END::UTINYINT AS stolen_bases,
        CASE WHEN p.fielding_position IN (1, 2) THEN e.caught_stealing ELSE 0 END::UTINYINT AS caught_stealing,
        CASE WHEN p.fielding_position IN (1, 2) AND fp.assists > 0 THEN e.pickoffs ELSE 0 END::UTINYINT AS pickoffs,
        CASE WHEN p.fielding_position = 2 THEN e.passed_balls ELSE 0 END::UTINYINT AS passed_balls,
        -- Only count double plays for the fielder who made a putout
        -- or assist on the play
        CASE WHEN fp.putouts + fp.assists > 0
                THEN e.double_plays
            ELSE 0
        END::UTINYINT AS double_plays,
        CASE WHEN fp.putouts + fp.assists > 0
                THEN e.triple_plays
            ELSE 0
        END::UTINYINT AS triple_plays,
        CASE WHEN fp.putouts + fp.assists > 0
                THEN e.ground_ball_double_plays
            ELSE 0
        END::UTINYINT AS ground_ball_double_plays,
        CASE WHEN fp.plays_started > 0
                THEN e.double_plays
            ELSE 0
        END::UTINYINT AS double_plays_started,
        CASE WHEN fp.plays_started > 0
                THEN e.ground_ball_double_plays
            ELSE 0
        END::UTINYINT AS ground_ball_double_plays_started,
    FROM main_models.event_fielding_stats AS e
    INNER JOIN main_models.personnel_fielding_states AS p USING (personnel_fielding_key)
    LEFT JOIN main_models.calc_fielding_play_agg AS fp USING (event_key, fielding_position)
)

SELECT * FROM final
