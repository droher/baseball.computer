{{
  config(
    materialized = 'table',
    )
}}
WITH databank AS (
    SELECT
        pitch.season,
        pitch.team_id,
        people.retrosheet_player_id AS player_id,
        'RegularSeason' AS game_type,
        SUM(pitch.wins) AS wins,
        SUM(pitch.losses) AS losses,
        SUM(pitch.games) AS games,
        SUM(pitch.games_started) AS games_started,
        SUM(pitch.complete_games) AS complete_games,
        SUM(pitch.shutouts) AS shutouts,
        SUM(pitch.saves) AS saves,
        SUM(pitch.outs_recorded) AS outs_recorded,
        SUM(pitch.hits) AS hits,
        SUM(pitch.earned_runs) AS earned_runs,
        SUM(pitch.home_runs) AS home_runs,
        SUM(pitch.walks) AS walks,
        SUM(pitch.strikeouts) AS strikeouts,
        SUM(pitch.intentional_walks) AS intentional_walks,
        SUM(pitch.wild_pitches) AS wild_pitches,
        SUM(pitch.hit_by_pitches) AS hit_by_pitches,
        SUM(pitch.balks) AS balks,
        SUM(pitch.batters_faced) AS batters_faced,
        SUM(pitch.games_finished) AS games_finished,
        SUM(pitch.runs) AS runs,
        SUM(pitch.sacrifice_hits) AS sacrifice_hits,
        SUM(pitch.sacrifice_flies) AS sacrifice_flies,
        SUM(pitch.grounded_into_double_plays) AS grounded_into_double_plays,
    FROM {{ ref('stg_databank_pitching') }} AS pitch
    INNER JOIN {{ ref('stg_people') }} AS people USING (databank_player_id)
    -- We'd need to do something different for partial coverage seasons but
    -- currently box scores are all or nothing for a given year
    WHERE pitch.season NOT IN (SELECT DISTINCT season FROM {{ ref('stg_games') }})
    GROUP BY 1, 2, 3
),

retrosheet AS (
    SELECT
        games.season,
        stats.team_id,
        stats.player_id,
        games.game_type,
        COUNT(*) AS games,
        {% for stat in event_level_pitching_stats() + game_level_pitching_stats() -%}
            SUM({{ stat }}) AS {{ stat }},
        {% endfor %}
    FROM {{ ref('stg_games') }} AS games
    INNER JOIN {{ ref('player_game_pitching_lines') }} AS stats USING (game_id)
    GROUP BY 1, 2, 3, 4
),

reround_ip AS (
    SELECT * REPLACE (
        ROUND(outs_recorded / 3, 2) AS innings_pitched
    )
    FROM retrosheet
)

SELECT * FROM reround_ip
UNION ALL BY NAME
SELECT * FROM databank
