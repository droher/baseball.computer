MODEL (
  name main_models.player_season_league_ops_plus,
  kind FULL,
  description 'Park- and league-adjusted OPS (OPS+) for each regular-season player-season-league. 100 is league average for non-pitchers; the league baseline excludes batting by primary pitchers (`player_team_season_roles`).',
  grain (player_id, season, league),
  column_descriptions (
    player_id = @doc('player_id'),
    season = @doc('season'),
    league = @doc('league'),
    plate_appearances = @doc('plate_appearances'),
    on_base_percentage = @doc('on_base_percentage'),
    slugging_percentage = @doc('slugging_percentage'),
    on_base_plus_slugging = @doc('on_base_plus_slugging'),
    league_on_base_percentage = 'On-base percentage of all non-pitchers in the league-season.',
    league_slugging_percentage = 'Slugging percentage of all non-pitchers in the league-season.',
    batting_park_factor = 'Plate-appearance-weighted `team_season_context.batting_park_factor` across the player''s teams.',
    ops_plus = '100 * (OBP / lgOBP + SLG / lgSLG - 1) / batting park factor.'
  ),
  audits (
    not_null(columns := (player_id, season, league)),
    unique_grain(columns := (player_id, season, league)),
    valid_baseball_season(column := season)
  ),
);

WITH team_lines AS (
    SELECT
        o.player_id,
        o.season,
        ctx.league,
        ctx.batting_park_factor,
        COALESCE(roles.is_primary_pitcher, FALSE) AS is_primary_pitcher,
        COALESCE(o.plate_appearances, 0) AS plate_appearances,
        COALESCE(o.at_bats, 0) AS at_bats,
        COALESCE(o.hits, 0) AS hits,
        COALESCE(o.walks, 0) AS walks,
        COALESCE(o.hit_by_pitches, 0) AS hit_by_pitches,
        COALESCE(o.sacrifice_flies, 0) AS sacrifice_flies,
        COALESCE(o.total_bases, 0) AS total_bases,
    FROM main_models.player_team_season_offense_stats AS o
    INNER JOIN main_models.team_season_context AS ctx USING (season, team_id)
    LEFT JOIN main_models.player_team_season_roles AS roles USING (season, team_id, player_id)
    WHERE o.game_type = 'RegularSeason'
),

league_baseline AS (
    SELECT
        season,
        league,
        SUM(hits + walks + hit_by_pitches)
            / NULLIF(SUM(at_bats + walks + hit_by_pitches + sacrifice_flies), 0) AS league_on_base_percentage,
        SUM(total_bases) / NULLIF(SUM(at_bats), 0) AS league_slugging_percentage,
    FROM team_lines
    WHERE NOT is_primary_pitcher
    GROUP BY 1, 2
),

player_lines AS (
    SELECT
        player_id,
        season,
        league,
        SUM(plate_appearances) AS plate_appearances,
        SUM(hits + walks + hit_by_pitches)
            / NULLIF(SUM(at_bats + walks + hit_by_pitches + sacrifice_flies), 0) AS on_base_percentage,
        SUM(total_bases) / NULLIF(SUM(at_bats), 0) AS slugging_percentage,
        SUM(batting_park_factor * plate_appearances) / NULLIF(SUM(plate_appearances), 0) AS batting_park_factor,
    FROM team_lines
    GROUP BY 1, 2, 3
),

final AS (
    SELECT
        p.player_id,
        p.season,
        p.league,
        p.plate_appearances,
        p.on_base_percentage,
        p.slugging_percentage,
        p.on_base_percentage + p.slugging_percentage AS on_base_plus_slugging,
        lg.league_on_base_percentage,
        lg.league_slugging_percentage,
        p.batting_park_factor,
        ROUND(
            100 * (
                p.on_base_percentage / lg.league_on_base_percentage
                + p.slugging_percentage / lg.league_slugging_percentage
                - 1
            ) / p.batting_park_factor
        )::INTEGER AS ops_plus,
    FROM player_lines AS p
    INNER JOIN league_baseline AS lg USING (season, league)
)

SELECT * FROM final
