MODEL (
  name main_models.player_position_game_fielding_stats,
  kind FULL,
  grain (game_id, player_id, fielding_position),
  columns (
    game_id VARCHAR,
    player_id VARCHAR,
    fielding_position UTINYINT,
    team_id TEAM_ID,
    games_started UTINYINT,
    outs_played UTINYINT,
    putouts UTINYINT,
    assists UTINYINT,
    errors UTINYINT,
    double_plays UTINYINT,
    triple_plays UTINYINT,
    plate_appearances_in_field UTINYINT,
    plate_appearances_in_field_with_ball_in_play UTINYINT,
    reaching_errors UTINYINT,
    fielders_choices UTINYINT,
    assisted_putouts UTINYINT,
    in_play_putouts UTINYINT,
    in_play_assists UTINYINT,
    balls_hit_to UTINYINT,
    ground_ball_double_plays UTINYINT,
    passed_balls UTINYINT,
    stolen_bases UTINYINT,
    caught_stealing UTINYINT,
    surplus_box_putouts TINYINT,
    surplus_box_assists TINYINT,
    surplus_box_errors TINYINT,
    unknown_putouts_while_fielding UTINYINT,
    pickoffs UTINYINT,
    double_plays_started UTINYINT,
    ground_ball_double_plays_started UTINYINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    player_id = @doc('player_id'),
    fielding_position = @doc('fielding_position'),
    team_id = @doc('team_id'),
    games_started = @doc('games_started'),
    outs_played = @doc('outs_played'),
    putouts = @doc('putouts'),
    assists = @doc('assists'),
    errors = @doc('errors'),
    double_plays = @doc('double_plays'),
    triple_plays = @doc('triple_plays'),
    plate_appearances_in_field = @doc('plate_appearances_in_field'),
    plate_appearances_in_field_with_ball_in_play = @doc('plate_appearances_in_field_with_ball_in_play'),
    reaching_errors = @doc('reaching_errors'),
    fielders_choices = @doc('fielders_choices'),
    assisted_putouts = @doc('assisted_putouts'),
    in_play_putouts = @doc('in_play_putouts'),
    in_play_assists = @doc('in_play_assists'),
    balls_hit_to = @doc('balls_hit_to'),
    ground_ball_double_plays = @doc('ground_ball_double_plays'),
    passed_balls = @doc('passed_balls'),
    stolen_bases = @doc('stolen_bases'),
    caught_stealing = @doc('caught_stealing'),
    unknown_putouts_while_fielding = @doc('unknown_putouts_while_fielding'),
    pickoffs = @doc('pickoffs'),
    double_plays_started = @doc('double_plays_started'),
    ground_ball_double_plays_started = @doc('ground_ball_double_plays_started')
  ),
  audits (
    not_null(columns := (game_id, player_id, fielding_position)),
    unique_grain(columns := (game_id, player_id, fielding_position)),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id),
    relationships(column := player_id, to_model := main_models.people, to_column := player_id),
    relationships(column := team_id, to_model := main_seeds.seed_franchises, to_column := team_id),
    not_null_proportion(column := outs_played, threshold := 0.995),
    not_null_proportion(column := putouts, threshold := 0.995),
    not_null_proportion(column := assists, threshold := 0.995),
    not_null_proportion(column := errors, threshold := 0.995),
    not_null_proportion(column := double_plays, threshold := 0.995),
    not_null_proportion(column := triple_plays, threshold := 0.995),
    not_null_proportion(column := stolen_bases, threshold := 0.93),
    not_null_proportion(column := caught_stealing, threshold := 0.93)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_player_position_game_fielding_stats.parquet'
  ),
);







WITH box_agg AS (
    SELECT
        game_id,
        stats.fielder_id AS player_id,
        stats.fielding_position,
        MIN(CASE WHEN stats.side = 'Home' THEN games.home_team_id ELSE games.away_team_id END) AS team_id,
        SUM(stats.outs_played) AS outs_played,
        CASE WHEN BOOL_OR(stats.putouts IS NULL) THEN NULL ELSE SUM(stats.putouts) END AS putouts,
        CASE WHEN BOOL_OR(stats.assists IS NULL) THEN NULL ELSE SUM(stats.assists) END AS assists,
        CASE WHEN BOOL_OR(stats.errors IS NULL) THEN NULL ELSE SUM(stats.errors) END AS errors,
        CASE WHEN BOOL_OR(stats.double_plays IS NULL) THEN NULL ELSE SUM(stats.double_plays) END AS double_plays,
        CASE WHEN BOOL_OR(stats.triple_plays IS NULL) THEN NULL ELSE SUM(stats.triple_plays) END AS triple_plays,
        CASE WHEN BOOL_OR(stats.passed_balls IS NULL) THEN NULL ELSE SUM(stats.passed_balls) END AS passed_balls
    FROM main_models.stg_box_score_fielding_lines AS stats
    INNER JOIN main_models.stg_games AS games USING (game_id)
    GROUP BY 1, 2, 3
),

event_agg AS (
    SELECT
        game_id,
        player_id,
        fielding_position,
        MIN(team_id) AS team_id,
        SUM(outs_played)::UTINYINT AS outs_played,
        SUM(plate_appearances_in_field)::UTINYINT AS plate_appearances_in_field,
        SUM(plate_appearances_in_field_with_ball_in_play)::UTINYINT AS plate_appearances_in_field_with_ball_in_play,
        SUM(putouts)::UTINYINT AS putouts,
        SUM(assists)::UTINYINT AS assists,
        SUM(errors)::UTINYINT AS errors,
        SUM(fielders_choices)::UTINYINT AS fielders_choices,
        SUM(assisted_putouts)::UTINYINT AS assisted_putouts,
        SUM(in_play_putouts)::UTINYINT AS in_play_putouts,
        SUM(in_play_assists)::UTINYINT AS in_play_assists,
        SUM(double_plays)::UTINYINT AS double_plays,
        SUM(triple_plays)::UTINYINT AS triple_plays,
        SUM(ground_ball_double_plays)::UTINYINT AS ground_ball_double_plays,
        SUM(stolen_bases)::UTINYINT AS stolen_bases,
        SUM(caught_stealing)::UTINYINT AS caught_stealing,
        SUM(passed_balls)::UTINYINT AS passed_balls,
        SUM(balls_hit_to)::UTINYINT AS balls_hit_to,
        SUM(reaching_errors)::UTINYINT AS reaching_errors,
        SUM(pickoffs)::UTINYINT AS pickoffs,
        SUM(double_plays_started)::UTINYINT AS double_plays_started,
        SUM(ground_ball_double_plays_started)::UTINYINT AS ground_ball_double_plays_started,
        SUM(unknown_putouts_while_fielding)::UTINYINT AS unknown_putouts_while_fielding
    FROM main_models.event_player_fielding_stats
    GROUP BY 1, 2, 3
),

-- Unlike batting/fielding, we join the data instead of unioning
-- because box scores are more reliable for fielding plays
final AS (
    SELECT
        game_id,
        player_id,
        fielding_position,
        COALESCE(event_agg.team_id, box_agg.team_id) AS team_id,
        CASE
            WHEN appearances.first_fielding_position = fielding_position
                OR appearances.games_ohtani_rule = 1 AND fielding_position IN (1, 10)
                THEN appearances.games_started
            ELSE 0
        END::UTINYINT AS games_started,
        -- Outs played data is always more authoritative from events, unlike other fielding data
        COALESCE(event_agg.outs_played, box_agg.outs_played)::UTINYINT AS outs_played,
        -- The rules for combining box and event fielding data are:
        -- If there is an event account with no unknown plays while the fielder was playing, use it
        -- Otherwise, if there is there is a box score account with data for that field, use it
        -- If the box score account exists but is missing data for that particular field, leave it empty
        -- If there is an event account but no box score account, use events
        CASE WHEN box_agg.game_id IS NULL OR event_agg.unknown_putouts_while_fielding = 0
                THEN event_agg.putouts
            ELSE box_agg.putouts
        END::UTINYINT AS putouts,
        CASE WHEN box_agg.game_id IS NULL OR event_agg.unknown_putouts_while_fielding = 0
                THEN event_agg.assists
            ELSE box_agg.assists
        END::UTINYINT AS assists,
        CASE WHEN box_agg.game_id IS NULL OR event_agg.unknown_putouts_while_fielding = 0
                THEN event_agg.errors
            ELSE box_agg.errors
        END::UTINYINT AS errors,
        CASE WHEN box_agg.game_id IS NULL OR event_agg.unknown_putouts_while_fielding = 0
                THEN event_agg.double_plays
            ELSE box_agg.double_plays
        END::UTINYINT AS double_plays,
        CASE WHEN box_agg.game_id IS NULL OR event_agg.unknown_putouts_while_fielding = 0
                THEN event_agg.triple_plays
            ELSE box_agg.triple_plays
        END::UTINYINT AS triple_plays,
        event_agg.plate_appearances_in_field,
        event_agg.plate_appearances_in_field_with_ball_in_play,
        event_agg.reaching_errors,
        event_agg.fielders_choices,
        event_agg.assisted_putouts,
        event_agg.in_play_putouts,
        event_agg.in_play_assists,
        event_agg.balls_hit_to,
        event_agg.ground_ball_double_plays,
        COALESCE(box_agg.passed_balls, event_agg.passed_balls)::UTINYINT AS passed_balls,
        event_agg.stolen_bases,
        event_agg.caught_stealing,
        event_agg.unknown_putouts_while_fielding,
        event_agg.pickoffs,
        event_agg.double_plays_started,
        event_agg.ground_ball_double_plays_started,
        (box_agg.putouts - event_agg.putouts)::TINYINT AS surplus_box_putouts,
        (box_agg.assists - event_agg.assists)::TINYINT AS surplus_box_assists,
        (box_agg.errors - event_agg.errors)::TINYINT AS surplus_box_errors,
    FROM box_agg
    FULL OUTER JOIN event_agg USING (game_id, player_id, fielding_position)
    LEFT JOIN main_models.player_game_appearances AS appearances USING (game_id, player_id)
)

SELECT * FROM final
