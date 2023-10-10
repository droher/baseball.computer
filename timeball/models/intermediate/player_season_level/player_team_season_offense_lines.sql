{{
  config(
    materialized = 'table',
    )
}}
WITH databank AS (
    SELECT
        bat.season,
        bat.team_id,
        people.retrosheet_player_id AS player_id,
        'RegularSeason' AS game_type,
        SUM(bat.games) AS games,
        SUM(bat.at_bats) AS at_bats,
        SUM(bat.runs) AS runs,
        SUM(bat.hits) AS hits,
        SUM(bat.doubles) AS doubles,
        SUM(bat.triples) AS triples,
        SUM(bat.home_runs) AS home_runs,
        SUM(bat.runs_batted_in) AS runs_batted_in,
        SUM(bat.stolen_bases) AS stolen_bases,
        SUM(bat.caught_stealing) AS caught_stealing,
        SUM(bat.walks) AS walks,
        SUM(bat.strikeouts) AS strikeouts,
        SUM(bat.intentional_walks) AS intentional_walks,
        SUM(bat.hit_by_pitches) AS hit_by_pitches,
        SUM(bat.sacrifice_hits) AS sacrifice_hits,
        SUM(bat.sacrifice_flies) AS sacrifice_flies,
        SUM(bat.grounded_into_double_plays) AS grounded_into_double_plays,
        SUM(bat.singles) AS singles,
        SUM(bat.total_bases) AS total_bases,
        SUM(bat.plate_appearances) AS plate_appearances,
        SUM(bat.on_base_opportunities) AS on_base_opportunities,
        SUM(bat.on_base_successes) AS on_base_successes,
    FROM {{ ref('stg_databank_batting') }} AS bat
    INNER JOIN {{ ref('stg_people') }} AS people USING (databank_player_id)
    -- We'd need to do something different for partial coverage seasons but
    -- currently box scores are all or nothing for a given year
    WHERE bat.season NOT IN (SELECT DISTINCT season FROM {{ ref('stg_games') }})
    GROUP BY 1, 2, 3
),

retrosheet AS (
    SELECT
        games.season,
        stats.team_id,
        stats.player_id,
        games.game_type,
        COUNT(*) AS games,
        {% for stat in event_level_offense_stats() -%}
            SUM({{ stat }}) AS {{ stat }},
        {% endfor %}
    FROM {{ ref('stg_games') }} AS games
    INNER JOIN {{ ref('player_game_offense_lines') }} AS stats USING (game_id)
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM retrosheet
UNION ALL BY NAME
SELECT * FROM databank
