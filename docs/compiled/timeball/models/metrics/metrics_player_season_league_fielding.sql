-- Add extra context columns to get potential grouiping keys
    WITH season AS (SELECT 
            s.*,
                f.franchise_id,
                f.league,
                f.division,
                f.location,
                f.nickname,
                f.alternative_nicknames,
                f.date_start,
                f.date_end,
                f.city,
                f.state,
        FROM "timeball"."main_models"."player_position_team_season_fielding_stats" AS s
        LEFT JOIN "timeball"."main_seeds"."seed_franchises" AS f
            ON s.team_id = f.team_id
            AND s.season BETWEEN EXTRACT(YEAR FROM f.date_start) AND COALESCE(EXTRACT(YEAR FROM f.date_end), 9999)
    ),
    event AS (SELECT 
            e.*,
                g.opponent_id,
                g.league,
                g.opponent_league,
                g.division,
                g.opponent_division,
                g.team_name,
                g.opponent_name,
                g.starting_pitcher_id,
                g.opponent_starting_pitcher_id,
                g.team_side,
                g.date,
                g.start_time,
                g.season,
                g.doubleheader_status,
                g.time_of_day,
                g.game_type,
                g.bat_first_side,
                g.sky,
                g.field_condition,
                g.precipitation,
                g.wind_direction,
                g.park_id,
                g.temperature_fahrenheit,
                g.attendance,
                g.wind_speed_mph,
                g.use_dh,
                g.scorer,
                g.scoring_method,
                g.source_type,
                g.umpire_home_id,
                g.umpire_first_id,
                g.umpire_second_id,
                g.umpire_third_id,
                g.umpire_left_id,
                g.umpire_right_id,
                g.filename,
                g.is_regular_season,
                g.is_postseason,
                g.is_integrated,
                g.is_negro_leagues,
                g.is_segregated_white,
                g.away_franchise_id,
                g.home_franchise_id,
                g.is_interleague,
                g.lineup_map_away,
                g.lineup_map_home,
                g.fielding_map_away,
                g.fielding_map_home,
                g.series_id,
                g.season_game_number,
                g.series_game_number,
                g.days_since_last_game,
        FROM "timeball"."main_models"."event_player_fielding_stats" AS e
        LEFT JOIN "timeball"."main_models"."team_game_start_info" AS g USING (team_id, game_id)
    ),
    -- Need to use the season table for basic stats/metrics to ensure full coverage...
    basic_stats AS (
        SELECT
            player_id,
            season,
            league,
            SUM(games) AS games,
            SUM(outs_played) AS outs_played,
            SUM(plate_appearances_in_field) AS plate_appearances_in_field,
            SUM(plate_appearances_in_field_with_ball_in_play) AS plate_appearances_in_field_with_ball_in_play,
            SUM(unknown_putouts_while_fielding) AS unknown_putouts_while_fielding,
            SUM(balls_hit_to) AS balls_hit_to,
            SUM(putouts) AS putouts,
            SUM(assists) AS assists,
            SUM(errors) AS errors,
            SUM(fielders_choices) AS fielders_choices,
            SUM(assisted_putouts) AS assisted_putouts,
            SUM(in_play_putouts) AS in_play_putouts,
            SUM(in_play_assists) AS in_play_assists,
            SUM(reaching_errors) AS reaching_errors,
            SUM(stolen_bases) AS stolen_bases,
            SUM(caught_stealing) AS caught_stealing,
            SUM(pickoffs) AS pickoffs,
            SUM(passed_balls) AS passed_balls,
            SUM(double_plays) AS double_plays,
            SUM(triple_plays) AS triple_plays,
            SUM(ground_ball_double_plays) AS ground_ball_double_plays,
            SUM(double_plays_started) AS double_plays_started,
            SUM(ground_ball_double_plays_started) AS ground_ball_double_plays_started,
            SUM(putouts + assists) / SUM(putouts + assists + errors) AS fielding_percentage,
            (SUM(putouts) + SUM(assists)) * 9 / SUM(outs_played * 3) AS range_factor,
            ROUND(SUM(outs_played) / 3, 2) AS innings_played,FROM season
        
        WHERE game_type IN (SELECT game_type FROM "timeball"."main_seeds"."seed_game_types" WHERE is_regular_season)
        
        GROUP BY player_id, season, league
    ),

    --- ...but we need to use the event table for event-based metrics,
    event_agg AS (
        SELECT
            player_id,
            season,
            league,
            COUNT(DISTINCT game_id) AS games,FROM event
        
        WHERE game_id IN (SELECT game_id FROM "timeball"."main_models"."game_start_info" WHERE is_regular_season)
        
        GROUP BY player_id, season, league
    ),

    final AS (
        SELECT
            player_id,
            season,
            league,
            basic_stats.outs_played::INT AS outs_played,
            basic_stats.plate_appearances_in_field::INT AS plate_appearances_in_field,
            basic_stats.plate_appearances_in_field_with_ball_in_play::INT AS plate_appearances_in_field_with_ball_in_play,
            basic_stats.unknown_putouts_while_fielding::INT AS unknown_putouts_while_fielding,
            basic_stats.balls_hit_to::INT AS balls_hit_to,
            basic_stats.putouts::INT AS putouts,
            basic_stats.assists::INT AS assists,
            basic_stats.errors::INT AS errors,
            basic_stats.fielders_choices::INT AS fielders_choices,
            basic_stats.assisted_putouts::INT AS assisted_putouts,
            basic_stats.in_play_putouts::INT AS in_play_putouts,
            basic_stats.in_play_assists::INT AS in_play_assists,
            basic_stats.reaching_errors::INT AS reaching_errors,
            basic_stats.stolen_bases::INT AS stolen_bases,
            basic_stats.caught_stealing::INT AS caught_stealing,
            basic_stats.pickoffs::INT AS pickoffs,
            basic_stats.passed_balls::INT AS passed_balls,
            basic_stats.double_plays::INT AS double_plays,
            basic_stats.triple_plays::INT AS triple_plays,
            basic_stats.ground_ball_double_plays::INT AS ground_ball_double_plays,
            basic_stats.double_plays_started::INT AS double_plays_started,
            basic_stats.ground_ball_double_plays_started::INT AS ground_ball_double_plays_started,
            basic_stats.fielding_percentage,
            basic_stats.range_factor,
            basic_stats.innings_played,COALESCE(event_agg.games / basic_stats.games, 0) AS event_coverage_rate,FROM basic_stats
        LEFT JOIN event_agg USING (player_id, season, league)
    )
    
    SELECT * FROM final
