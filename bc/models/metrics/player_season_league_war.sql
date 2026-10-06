MODEL (
  name main_models.player_season_league_war,
  kind FULL,
  description 'Quick, generic wins above replacement for each regular-season player-season-league, built only from baseball.computer season totals. Position-player value follows the FanGraphs framework (batting, baserunning, fielding, positional, league, and replacement runs); pitching value is park-adjusted runs allowed per out against league average, so it overlaps with team fielding. League-season run values come from `linear_weights`, and 1,000 replacement wins per 2,430 games are split 57/43 between hitters and pitchers.',
  grain (player_id, season, league),
  column_descriptions (
    player_id = @doc('player_id'),
    season = @doc('season'),
    league = @doc('league'),
    plate_appearances = @doc('plate_appearances'),
    outs_recorded = @doc('outs_recorded'),
    is_primary_pitcher = 'Whether the player was a primary pitcher (`player_team_season_roles.is_primary_pitcher`) for at least one of their teams in the league-season. Batting for a team where the player was a primary pitcher is compared with other primary pitchers and earns no replacement or league-adjustment runs.',
    batting_runs = 'Park-adjusted linear-weights batting runs above league average: singles, doubles, triples, home runs, unintentional walks, intentional walks, and hit by pitches valued with the league-season `linear_weights.relative_run_value`.',
    baserunning_runs = 'Stolen-base runs above league average (wSB): stolen bases and times caught stealing at league-season run values, less the league rate per time on first base.',
    fielding_runs = 'Range runs: plays made above the league-season rate for the position per ball in play while on the field, at 0.4 runs per play. Plays are assists for infielders and putouts plus assists for outfielders. Balls in play are allotted from the team''s pitching totals by outs played; league-seasons where any team lacks ball-in-play counts use non-strikeout outs instead. Catchers are credited instead for times caught stealing above the league rate. Pitchers and designated hitters get none.',
    positional_runs = 'Position adjustment per out played (FanGraphs scale per 1,458 innings: C +12.5, SS +7.5, 2B/3B/CF +2.5, LF/RF -7.5, 1B -12.5, DH -17.5), scaled by league-season runs per win / 10 so the adjustment is worth the same wins in every run environment. Primary pitchers instead offset the league-season average pitcher batting runs per plate appearance.',
    league_runs = 'League adjustment that zeroes the league-season sum of batting, baserunning, fielding, and positional runs per non-pitcher plate appearance.',
    replacement_runs = 'Position-player replacement runs, distributed per non-pitcher plate appearance.',
    position_player_war = 'Sum of batting, baserunning, fielding, positional, league, and replacement runs, divided by runs per win.',
    pitching_runs = 'Park-adjusted runs allowed below the league-season average per out recorded, recentered so the league-season sum is zero.',
    pitching_replacement_runs = 'Pitcher replacement runs, distributed per out recorded.',
    pitching_war = 'Pitching runs plus pitching replacement runs, divided by runs per win.',
    runs_per_win = 'League-season runs per win: 1.5 * runs allowed per nine innings + 3. Runs per team game stands in for runs per nine innings when the league-season has no pitching outs.',
    war = 'Position-player WAR plus pitching WAR.'
  ),
  audits (
    not_null(columns := (player_id, season, league, war, runs_per_win)),
    unique_grain(columns := (player_id, season, league)),
    valid_baseball_season(column := season),
    zero_sum_by_group(
      group_by := (season, league),
      measure := batting_runs + baserunning_runs + fielding_runs + positional_runs + league_runs
    ),
    zero_sum_by_group(group_by := (season, league), measure := pitching_runs)
  ),
);

WITH weights AS (
    SELECT
        season,
        league,
        MAX(relative_run_value) FILTER (WHERE play = 'Single') AS w_single,
        MAX(relative_run_value) FILTER (WHERE play = 'Double') AS w_double,
        MAX(relative_run_value) FILTER (WHERE play = 'Triple') AS w_triple,
        MAX(relative_run_value) FILTER (WHERE play = 'HomeRun') AS w_home_run,
        MAX(relative_run_value) FILTER (WHERE play = 'Walk') AS w_walk,
        MAX(relative_run_value) FILTER (WHERE play = 'IntentionalWalk') AS w_intentional_walk,
        MAX(relative_run_value) FILTER (WHERE play = 'HitByPitch') AS w_hit_by_pitch,
        MAX(average_run_value) FILTER (WHERE play = 'StolenBase') AS w_stolen_base,
        MAX(average_run_value) FILTER (WHERE play = 'CaughtStealing') AS w_caught_stealing,
    FROM main_models.linear_weights
    GROUP BY 1, 2
),

generic_weights AS (
    SELECT
        AVG(w_single) AS w_single,
        AVG(w_double) AS w_double,
        AVG(w_triple) AS w_triple,
        AVG(w_home_run) AS w_home_run,
        AVG(w_walk) AS w_walk,
        AVG(w_intentional_walk) AS w_intentional_walk,
        AVG(w_hit_by_pitch) AS w_hit_by_pitch,
        AVG(w_stolen_base) AS w_stolen_base,
        AVG(w_caught_stealing) AS w_caught_stealing,
    FROM weights
),

league_weights AS (
    SELECT
        ctx.season,
        ctx.league,
        COALESCE(w.w_single, g.w_single) AS w_single,
        COALESCE(w.w_double, g.w_double) AS w_double,
        COALESCE(w.w_triple, g.w_triple) AS w_triple,
        COALESCE(w.w_home_run, g.w_home_run) AS w_home_run,
        COALESCE(w.w_walk, g.w_walk) AS w_walk,
        COALESCE(w.w_intentional_walk, g.w_intentional_walk) AS w_intentional_walk,
        COALESCE(w.w_hit_by_pitch, g.w_hit_by_pitch) AS w_hit_by_pitch,
        COALESCE(w.w_stolen_base, g.w_stolen_base) AS w_stolen_base,
        COALESCE(w.w_caught_stealing, g.w_caught_stealing) AS w_caught_stealing,
    FROM (SELECT DISTINCT season, league FROM main_models.team_season_context) AS ctx
    CROSS JOIN generic_weights AS g
    LEFT JOIN weights AS w USING (season, league)
),

fielding AS (
    SELECT
        f.season,
        f.team_id,
        f.player_id,
        f.fielding_position,
        COALESCE(f.outs_played, 0) AS outs_played,
        COALESCE(f.putouts, 0) AS putouts,
        COALESCE(f.assists, 0) AS assists,
        COALESCE(f.stolen_bases, 0) AS stolen_bases_allowed,
        COALESCE(f.caught_stealing, 0) AS caught_stealing,
    FROM main_models.player_position_team_season_fielding_stats AS f
    WHERE f.game_type = 'RegularSeason'
),

pitching AS (
    SELECT
        p.season,
        p.team_id,
        p.player_id,
        COALESCE(p.outs_recorded, 0) AS outs_recorded,
        COALESCE(p.runs, 0) AS runs_allowed,
        COALESCE(p.strikeouts, 0) AS strikeouts,
        p.balls_in_play,
    FROM main_models.player_team_season_pitching_stats AS p
    WHERE p.game_type = 'RegularSeason'
),

team_ball_in_play_rates AS (
    SELECT
        p.season,
        p.team_id,
        ctx.league,
        SUM(p.balls_in_play) / NULLIF(SUM(p.outs_recorded) FILTER (WHERE p.balls_in_play IS NOT NULL), 0)
            AS balls_in_play_per_out,
        1 - SUM(p.strikeouts) / NULLIF(SUM(p.outs_recorded), 0) AS non_strikeout_share_of_outs,
    FROM pitching AS p
    INNER JOIN main_models.team_season_context AS ctx USING (season, team_id)
    GROUP BY 1, 2, 3
),

team_balls_in_play AS (
    SELECT
        season,
        team_id,
        CASE
            WHEN BOOL_AND(balls_in_play_per_out IS NOT NULL) OVER (PARTITION BY season, league)
                THEN balls_in_play_per_out
            ELSE non_strikeout_share_of_outs
        END AS balls_in_play_per_out,
    FROM team_ball_in_play_rates
),

batting AS (
    SELECT
        o.season,
        o.team_id,
        o.player_id,
        ctx.league,
        ctx.batting_park_factor,
        COALESCE(roles.is_primary_pitcher, FALSE) AS is_primary_pitcher,
        COALESCE(o.plate_appearances, 0) AS plate_appearances,
        COALESCE(o.singles, 0) AS singles,
        COALESCE(o.doubles, 0) AS doubles,
        COALESCE(o.triples, 0) AS triples,
        COALESCE(o.home_runs, 0) AS home_runs,
        COALESCE(o.walks, 0) - COALESCE(o.intentional_walks, 0) AS unintentional_walks,
        COALESCE(o.intentional_walks, 0) AS intentional_walks,
        COALESCE(o.hit_by_pitches, 0) AS hit_by_pitches,
        COALESCE(o.stolen_bases, 0) AS stolen_bases,
        COALESCE(o.caught_stealing, 0) AS caught_stealing,
    FROM main_models.player_team_season_offense_stats AS o
    INNER JOIN main_models.team_season_context AS ctx USING (season, team_id)
    LEFT JOIN main_models.player_team_season_roles AS roles USING (season, team_id, player_id)
    WHERE o.game_type = 'RegularSeason'
),

batting_values AS (
    SELECT
        b.*,
        b.singles * w.w_single
        + b.doubles * w.w_double
        + b.triples * w.w_triple
        + b.home_runs * w.w_home_run
        + b.unintentional_walks * w.w_walk
        + b.intentional_walks * w.w_intentional_walk
        + b.hit_by_pitches * w.w_hit_by_pitch AS raw_batting_runs,
        b.stolen_bases * w.w_stolen_base + b.caught_stealing * w.w_caught_stealing AS raw_stolen_base_runs,
        b.singles + b.unintentional_walks + b.hit_by_pitches AS times_on_first,
    FROM batting AS b
    INNER JOIN league_weights AS w USING (season, league)
),

league_totals AS (
    SELECT
        ctx.season,
        ctx.league,
        SUM(ctx.games) AS team_games,
        SUM(ctx.runs_allowed) AS runs,
        ANY_VALUE(lb.plate_appearances) AS plate_appearances,
        ANY_VALUE(lb.non_pitcher_plate_appearances) AS non_pitcher_plate_appearances,
        ANY_VALUE(lb.raw_batting_runs) AS raw_batting_runs,
        ANY_VALUE(lb.raw_stolen_base_runs) AS raw_stolen_base_runs,
        ANY_VALUE(lb.times_on_first) AS times_on_first,
        ANY_VALUE(lp.outs_recorded) AS outs_recorded,
    FROM main_models.team_season_context AS ctx
    LEFT JOIN (
        SELECT
            season,
            league,
            SUM(plate_appearances) AS plate_appearances,
            SUM(plate_appearances) FILTER (WHERE NOT is_primary_pitcher) AS non_pitcher_plate_appearances,
            SUM(raw_batting_runs) AS raw_batting_runs,
            SUM(raw_stolen_base_runs) AS raw_stolen_base_runs,
            SUM(times_on_first) AS times_on_first,
        FROM batting_values
        GROUP BY 1, 2
    ) AS lb USING (season, league)
    LEFT JOIN (
        SELECT
            pitching.season,
            ctx.league,
            SUM(pitching.outs_recorded) AS outs_recorded,
        FROM pitching
        INNER JOIN main_models.team_season_context AS ctx USING (season, team_id)
        GROUP BY 1, 2
    ) AS lp USING (season, league)
    GROUP BY 1, 2
),

league_rates AS (
    SELECT
        season,
        league,
        team_games,
        runs / NULLIF(plate_appearances, 0) AS runs_per_plate_appearance,
        COALESCE(runs / NULLIF(outs_recorded, 0), runs / (27.0 * team_games)) AS runs_per_out,
        1.5 * 27 * COALESCE(runs / NULLIF(outs_recorded, 0), runs / (27.0 * team_games)) + 3 AS runs_per_win,
        raw_batting_runs / NULLIF(plate_appearances, 0) AS raw_batting_runs_per_plate_appearance,
        raw_stolen_base_runs / NULLIF(times_on_first, 0) AS stolen_base_runs_per_time_on_first,
        non_pitcher_plate_appearances,
        outs_recorded,
    FROM league_totals
),

player_batting AS (
    SELECT
        b.season,
        b.team_id,
        b.player_id,
        b.league,
        b.is_primary_pitcher,
        b.plate_appearances,
        b.raw_batting_runs
        - b.plate_appearances * lr.raw_batting_runs_per_plate_appearance
        + (1 - b.batting_park_factor) * lr.runs_per_plate_appearance * b.plate_appearances AS batting_runs,
        b.raw_stolen_base_runs - b.times_on_first * lr.stolen_base_runs_per_time_on_first AS baserunning_runs,
    FROM batting_values AS b
    INNER JOIN league_rates AS lr USING (season, league)
),

pitcher_batting_rates AS (
    SELECT
        season,
        league,
        SUM(batting_runs) / NULLIF(SUM(plate_appearances), 0) AS batting_runs_per_plate_appearance,
    FROM player_batting
    WHERE is_primary_pitcher
    GROUP BY 1, 2
),

fielding_context AS (
    SELECT
        f.*,
        ctx.league,
        CASE WHEN f.fielding_position IN (3, 4, 5, 6) THEN f.assists ELSE f.putouts + f.assists END AS plays,
        f.outs_played * COALESCE(tbip.balls_in_play_per_out, 1) AS balls_in_play_while_fielding,
    FROM fielding AS f
    INNER JOIN main_models.team_season_context AS ctx USING (season, team_id)
    LEFT JOIN team_balls_in_play AS tbip USING (season, team_id)
),

position_rates AS (
    SELECT
        season,
        league,
        fielding_position,
        SUM(plays) / NULLIF(SUM(balls_in_play_while_fielding), 0) AS plays_per_ball_in_play,
        SUM(caught_stealing) / NULLIF(SUM(caught_stealing + stolen_bases_allowed), 0) AS caught_stealing_rate,
    FROM fielding_context
    GROUP BY 1, 2, 3
),

player_fielding AS (
    SELECT
        f.season,
        f.team_id,
        f.player_id,
        f.league,
        CASE
            WHEN f.fielding_position = 2
                THEN (f.caught_stealing - (f.caught_stealing + f.stolen_bases_allowed) * COALESCE(pr.caught_stealing_rate, 0))
                    * (w.w_stolen_base - w.w_caught_stealing)
            WHEN f.fielding_position IN (0, 3, 4, 5, 6, 7, 8, 9)
                THEN 0.4 * (f.plays - f.balls_in_play_while_fielding * COALESCE(pr.plays_per_ball_in_play, 0))
            ELSE 0
        END AS fielding_runs,
        CASE
            WHEN f.fielding_position = 1 THEN 0
            ELSE f.outs_played / 4374.0 * lr.runs_per_win / 10 * CASE f.fielding_position
                WHEN 2 THEN 12.5
                WHEN 3 THEN -12.5
                WHEN 4 THEN 2.5
                WHEN 5 THEN 2.5
                WHEN 6 THEN 7.5
                WHEN 7 THEN -7.5
                WHEN 8 THEN 2.5
                WHEN 9 THEN -7.5
                WHEN 0 THEN -12.5 / 3
                WHEN 10 THEN -17.5
                ELSE 0
            END
        END AS positional_runs,
    FROM fielding_context AS f
    INNER JOIN league_weights AS w USING (season, league)
    INNER JOIN league_rates AS lr USING (season, league)
    LEFT JOIN position_rates AS pr USING (season, league, fielding_position)
),

player_pitching AS (
    SELECT
        p.season,
        p.team_id,
        p.player_id,
        ctx.league,
        p.outs_recorded,
        p.outs_recorded * lr.runs_per_out * ctx.batting_park_factor - p.runs_allowed AS unadjusted_pitching_runs,
        0.43 * 1000 / 4860.0 * lr.team_games * lr.runs_per_win / NULLIF(lr.outs_recorded, 0) * p.outs_recorded
            AS pitching_replacement_runs,
    FROM pitching AS p
    INNER JOIN main_models.team_season_context AS ctx USING (season, team_id)
    INNER JOIN league_rates AS lr ON ctx.season = lr.season AND ctx.league = lr.league
),

player_pitching_adjusted AS (
    SELECT
        * EXCLUDE (unadjusted_pitching_runs),
        unadjusted_pitching_runs
        - outs_recorded * SUM(unadjusted_pitching_runs) OVER league / NULLIF(SUM(outs_recorded) OVER league, 0)
            AS pitching_runs,
    FROM player_pitching
    WINDOW league AS (PARTITION BY season, league)
),

player_team_rows AS (
    SELECT
        season,
        team_id,
        player_id,
        league,
        plate_appearances,
        batting_runs,
        baserunning_runs,
        CASE WHEN is_primary_pitcher THEN plate_appearances ELSE 0 END AS primary_pitcher_plate_appearances,
    FROM player_batting
    UNION ALL BY NAME
    SELECT
        season,
        team_id,
        player_id,
        league,
        fielding_runs,
        positional_runs,
    FROM player_fielding
    UNION ALL BY NAME
    SELECT
        season,
        team_id,
        player_id,
        league,
        outs_recorded,
        pitching_runs,
        pitching_replacement_runs,
    FROM player_pitching_adjusted
),

player_totals AS (
    SELECT
        r.player_id,
        r.season,
        r.league,
        BOOL_OR(COALESCE(roles.is_primary_pitcher, FALSE)) AS is_primary_pitcher,
        COALESCE(SUM(r.plate_appearances), 0) AS plate_appearances,
        COALESCE(SUM(r.primary_pitcher_plate_appearances), 0) AS primary_pitcher_plate_appearances,
        COALESCE(SUM(r.outs_recorded), 0) AS outs_recorded,
        COALESCE(SUM(r.batting_runs), 0) AS batting_runs,
        COALESCE(SUM(r.baserunning_runs), 0) AS baserunning_runs,
        COALESCE(SUM(r.fielding_runs), 0) AS fielding_runs,
        COALESCE(SUM(r.positional_runs), 0) AS positional_runs,
        COALESCE(SUM(r.pitching_runs), 0) AS pitching_runs,
        COALESCE(SUM(r.pitching_replacement_runs), 0) AS pitching_replacement_runs,
    FROM player_team_rows AS r
    LEFT JOIN main_models.player_team_season_roles AS roles USING (season, team_id, player_id)
    GROUP BY 1, 2, 3
),

with_pitcher_offset AS (
    SELECT
        t.* REPLACE (
            t.positional_runs
            - t.primary_pitcher_plate_appearances * COALESCE(pbr.batting_runs_per_plate_appearance, 0) AS positional_runs
        ),
        t.plate_appearances - t.primary_pitcher_plate_appearances AS non_pitcher_plate_appearances,
    FROM player_totals AS t
    LEFT JOIN pitcher_batting_rates AS pbr USING (season, league)
),

league_adjustment AS (
    SELECT
        season,
        league,
        -SUM(batting_runs + baserunning_runs + fielding_runs + positional_runs)
        / NULLIF(SUM(non_pitcher_plate_appearances), 0) AS league_runs_per_plate_appearance,
    FROM with_pitcher_offset
    GROUP BY 1, 2
),

with_runs AS (
    SELECT
        t.player_id,
        t.season,
        t.league,
        t.is_primary_pitcher,
        t.plate_appearances,
        t.outs_recorded,
        t.batting_runs,
        t.baserunning_runs,
        t.fielding_runs,
        t.positional_runs,
        t.non_pitcher_plate_appearances * COALESCE(la.league_runs_per_plate_appearance, 0) AS league_runs,
        COALESCE(
            0.57 * 1000 / 4860.0 * lr.team_games * lr.runs_per_win
            / NULLIF(lr.non_pitcher_plate_appearances, 0) * t.non_pitcher_plate_appearances,
            0
        ) AS replacement_runs,
        t.pitching_runs,
        t.pitching_replacement_runs,
        lr.runs_per_win,
    FROM with_pitcher_offset AS t
    INNER JOIN league_rates AS lr USING (season, league)
    LEFT JOIN league_adjustment AS la USING (season, league)
),

final AS (
    SELECT
        player_id,
        season,
        league,
        is_primary_pitcher,
        plate_appearances,
        outs_recorded,
        batting_runs,
        baserunning_runs,
        fielding_runs,
        positional_runs,
        league_runs,
        replacement_runs,
        (batting_runs + baserunning_runs + fielding_runs + positional_runs + league_runs + replacement_runs)
        / runs_per_win AS position_player_war,
        pitching_runs,
        pitching_replacement_runs,
        (pitching_runs + pitching_replacement_runs) / runs_per_win AS pitching_war,
        runs_per_win,
        (
            batting_runs + baserunning_runs + fielding_runs + positional_runs + league_runs + replacement_runs
            + pitching_runs + pitching_replacement_runs
        ) / runs_per_win AS war,
    FROM with_runs
)

SELECT * FROM final
