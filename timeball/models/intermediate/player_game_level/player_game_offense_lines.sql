{{
  config(
    materialized = 'table',
    )
}}
WITH box_score AS (
    SELECT
        CASE WHEN lines.side = 'Home' THEN games.home_team_id ELSE games.away_team_id END AS team_id,
        lines.*
    FROM {{ ref('stg_box_score_batting_lines') }} AS lines
    -- This join ensures that we only get the box score lines for games that
    -- do not have an event file.
    INNER JOIN {{ ref('stg_games') }} AS games USING (game_id)
    WHERE games.source_type = 'BoxScore'
),

final AS (
    SELECT
        game_id,
        team_id,
        player_id,
        {% for stat in event_level_offense_stats() -%}
            {% set dtype = "INT1" if stat.startswith("surplus") else "UTINYINT" %}
            SUM({{ stat }})::{{ dtype }} AS {{ stat }},
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
)

SELECT * FROM final
