{{
  config(
    materialized = 'table',
    )
}}
WITH box_score AS (
    SELECT
        CASE WHEN bat.side = 'Home' THEN games.home_team_id ELSE games.away_team_id END AS team_id,
        bat.*
    FROM {{ ref('stg_box_score_batting_lines') }} AS bat
    -- This join ensures that we only get the box score lines for games that
    -- do not have an event file.
    INNER JOIN {{ ref('game_start_info') }} AS games USING (game_id)
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
        SUM(at_bats)::UTINYINT AS at_bats,
        SUM(runs)::UTINYINT AS runs,
        SUM(hits)::UTINYINT AS hits,
        SUM(doubles)::UTINYINT AS doubles,
        SUM(triples)::UTINYINT AS triples,
        SUM(home_runs)::UTINYINT AS home_runs,
        SUM(runs_batted_in)::UTINYINT AS runs_batted_in,
        SUM(strikeouts)::UTINYINT AS strikeouts,
        SUM(walks)::UTINYINT AS walks,
        SUM(intentional_walks)::UTINYINT AS intentional_walks,
        SUM(hit_by_pitches)::UTINYINT AS hit_by_pitches,
        SUM(sacrifice_hits)::UTINYINT AS sacrifice_hits,
        SUM(sacrifice_flies)::UTINYINT AS sacrifice_flies,
        NULL AS reached_on_errors,
        NULL AS reached_on_interferences,
        SUM(grounded_into_double_plays)::UTINYINT AS grounded_into_double_plays,
        NULL AS double_plays,
        NULL AS triple_plays,
        SUM(singles)::UTINYINT AS singles,
        SUM(total_bases)::UTINYINT AS total_bases,
        SUM(plate_appearances)::UTINYINT AS plate_appearances,
        SUM(on_base_opportunities)::UTINYINT AS on_base_opportunities,
        SUM(on_base_successes)::UTINYINT AS on_base_successes,
        SUM(batting_outs)::UTINYINT AS batting_outs,
        SUM(stolen_bases)::UTINYINT AS stolen_bases,
        SUM(caught_stealing)::UTINYINT AS caught_stealing,
    FROM box_score
    GROUP BY 1, 2, 3
)

SELECT * FROM final
