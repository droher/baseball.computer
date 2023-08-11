{{
  config(
    materialized = 'table',
    )
}}
WITH box_score AS (
    SELECT
        teams.team_id,
        lines.*
    FROM {{ ref('stg_box_score_batting_lines') }} AS lines
    INNER JOIN {{ ref('stg_game_teams') }} AS teams USING (game_id, side)
    WHERE teams.source_type = 'BoxScore'
)

SELECT
    game_id,
    batting_team_id AS team_id,
    player_id,
    {% for stat in offense_stats() -%}
        SUM({{ stat }}) AS {{ stat }},
    {% endfor %}
FROM {{ ref('event_offense_stats') }}
GROUP BY 1, 2, 3
UNION ALL BY NAME
SELECT
    game_id,
    team_id,
    batter_id AS player_id,
    at_bats,
    runs,
    hits,
    doubles,
    triples,
    home_runs,
    runs_batted_in,
    strikeouts,
    walks,
    intentional_walks,
    hit_by_pitches,
    sacrifice_hits,
    sacrifice_flies,
    NULL AS reached_on_errors,
    NULL AS reached_on_interferences,
    grounded_into_double_plays,
    NULL AS double_plays,
    NULL AS triple_plays,
    singles,
    total_bases,
    plate_appearances,
    on_base_opportunities,
    on_base_successes,
    batting_outs,
    stolen_bases,
    caught_stealing,
FROM box_score
