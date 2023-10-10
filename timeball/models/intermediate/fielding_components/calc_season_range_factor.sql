WITH season_pitching AS (
    SELECT season,
        SUM(hits - home_runs) AS hits_in_play,
        SUM(reached_on_errors) AS reached_on_errors,
        SUM(strikeouts) AS strikeouts,
        SUM(at_bats - strikeouts - home_runs) AS balls_in_play,
        SUM(outs_recorded) AS outs_recorded,
    FROM {{ ref('player_team_season_pitching_lines') }}
    WHERE game_type = 'RegularSeason'
    GROUP BY 1
),

season_fielding AS (
    SELECT
        season,
        SUM(putouts) AS putouts,
        SUM(assists) AS assists,
        SUM(errors) AS errors,
    FROM {{ ref('team_season_fielding_lines') }}
    WHERE game_type = 'RegularSeason'
    GROUP BY 1
),

final AS (
    SELECT
        season,
        hits_in_play,
        strikeouts,
        reached_on_errors,
        outs_recorded,
        balls_in_play,
        putouts,
        assists,
        errors,
        ROUND(reached_on_errors/errors * 100)
    FROM season_pitching
    FULL OUTER JOIN season_fielding USING (season)
)

SELECT * FROM final
