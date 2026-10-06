MODEL (
  name main_models.team_season_context,
  kind FULL,
  description 'Regular-season league, games played, and batting park factor for each team-season. Inputs for OPS+ and WAR.',
  grain (season, team_id),
  column_descriptions (
    season = @doc('season'),
    team_id = @doc('team_id'),
    league = @doc('league'),
    games = 'Regular-season games played by the team.',
    runs_scored = 'Regular-season runs scored by the team.',
    runs_allowed = 'Regular-season runs allowed by the team.',
    home_park_factor = 'Game-weighted average of `park_factors.overall_park_factor` (falling back to `basic_park_factor` where the advanced factor is missing) across the team''s home games. 1.0 is neutral.',
    batting_park_factor = 'Park factor applied to a full season of the team''s games, assuming half are played at home and the road parks average out to neutral: (1 + home_park_factor) / 2.'
  ),
  audits (
    not_null(columns := (season, team_id, league, games, batting_park_factor)),
    unique_grain(columns := (season, team_id)),
    valid_baseball_season(column := season),
    bounded_range(column := batting_park_factor, min_v := 0.5, max_v := 2)
  ),
);

WITH team_totals AS (
    SELECT
        season,
        team_id,
        COALESCE(ANY_VALUE(league) FILTER (WHERE league IS NOT NULL), 'N/A') AS league,
        COUNT(*) AS games,
        SUM(runs_scored) AS runs_scored,
        SUM(runs_allowed) AS runs_allowed,
    FROM main_models.team_game_results
    WHERE game_type = 'RegularSeason'
    GROUP BY 1, 2
),

home_parks AS (
    SELECT
        info.season,
        info.team_id,
        AVG(
            CASE
                WHEN NOT ISNAN(pf.overall_park_factor) THEN pf.overall_park_factor
                WHEN NOT ISNAN(pf.basic_park_factor) THEN pf.basic_park_factor
            END
        ) AS home_park_factor,
    FROM main_models.team_game_start_info AS info
    LEFT JOIN main_models.park_factors AS pf
        ON info.season = pf.season
            AND info.park_id = pf.park_id
            AND info.league = pf.league
    WHERE info.game_type = 'RegularSeason'
        AND info.team_side = 'Home'
    GROUP BY 1, 2
),

final AS (
    SELECT
        t.season,
        t.team_id,
        t.league,
        t.games,
        t.runs_scored,
        t.runs_allowed,
        COALESCE(h.home_park_factor, 1) AS home_park_factor,
        (1 + COALESCE(h.home_park_factor, 1)) / 2 AS batting_park_factor,
    FROM team_totals AS t
    LEFT JOIN home_parks AS h USING (season, team_id)
)

SELECT * FROM final
