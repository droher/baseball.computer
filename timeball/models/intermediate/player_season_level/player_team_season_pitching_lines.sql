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
        SUM(pitch.wins)::INT AS wins,
        SUM(pitch.losses)::INT AS losses,
        SUM(pitch.games)::INT AS games,
        SUM(pitch.games_started)::INT AS games_started,
        SUM(pitch.complete_games)::INT AS complete_games,
        SUM(pitch.shutouts)::INT AS shutouts,
        SUM(pitch.saves)::INT AS saves,
        SUM(pitch.outs_recorded)::INT AS outs_recorded,
        SUM(pitch.hits)::INT AS hits,
        SUM(pitch.earned_runs)::INT AS earned_runs,
        SUM(pitch.home_runs)::INT AS home_runs,
        SUM(pitch.walks)::INT AS walks,
        SUM(pitch.strikeouts)::INT AS strikeouts,
        SUM(pitch.intentional_walks)::INT AS intentional_walks,
        SUM(pitch.wild_pitches)::INT AS wild_pitches,
        SUM(pitch.hit_by_pitches)::INT AS hit_by_pitches,
        SUM(pitch.balks)::INT AS balks,
        SUM(pitch.batters_faced)::INT AS batters_faced,
        SUM(pitch.games_finished)::INT AS games_finished,
        SUM(pitch.runs)::INT AS runs,
        SUM(pitch.sacrifice_hits)::INT AS sacrifice_hits,
        SUM(pitch.sacrifice_flies)::INT AS sacrifice_flies,
        SUM(pitch.grounded_into_double_plays)::INT AS grounded_into_double_plays,
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
            SUM({{ stat }})::INT AS {{ stat }},
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
