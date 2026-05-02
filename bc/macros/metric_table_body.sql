{# Phase 1 SQLMesh port of metrics_table_generator body (lines 43-124).

   Parameterized on `int_cols` and `game_cols` because SQLMesh's data-type
   parser blows up on DuckDB ENUM columns when introspecting via
   `adapter.get_columns_in_relation()` (the original macro's approach).
   The blueprint at bc/models/metrics/_metrics_blueprint.sql passes both
   lists explicitly per variant.

   `kind` selects which event/game/season tables and which basic-rate
   formulas to use; `grouping_keys` controls the GROUP BY.
   `regular_season_only` is hardcoded TRUE — every wrapper passed it as TRUE.
#}
{% macro metric_table_body(kind, grouping_keys, int_cols, game_cols) %}
    {%- if kind == "offense" -%}
        {%- set event_model = "event_offense_stats" -%}
        {%- set season_model = "player_team_season_offense_stats" -%}
        {%- set basic_metric_dict = basic_rate_stats_offense() -%}
    {%- elif kind == "pitching" -%}
        {%- set event_model = "event_pitching_stats" -%}
        {%- set season_model = "player_team_season_pitching_stats" -%}
        {%- set basic_metric_dict = basic_rate_stats_pitching() -%}
    {%- elif kind == "fielding" -%}
        {%- set event_model = "event_player_fielding_stats" -%}
        {%- set season_model = "player_position_team_season_fielding_stats" -%}
        {%- set basic_metric_dict = basic_rate_stats_fielding() -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error("Invalid kind - must be one of offense, pitching, fielding. Got " ~ kind) }}
    {%- endif -%}

    {%- set event_based_metrics = {} -%}
    {%- if kind != "fielding"  -%}
        {%- do event_based_metrics.update(batted_ball_stats()) -%}
        {%- do event_based_metrics.update(baserunning_stats()) -%}
        {%- do event_based_metrics.update(pitch_sequence_stats()) -%}
    {%- endif -%}

    -- Add extra context columns to get potential grouping keys
    WITH season AS (
        SELECT
            s.*,
            COALESCE(f.league, 'N/A') AS league
        FROM {{ ref(season_model) }} AS s
        LEFT JOIN {{ ref('seed_franchises') }} AS f
            ON s.team_id = f.team_id
            AND s.season BETWEEN EXTRACT(YEAR FROM f.date_start) AND COALESCE(EXTRACT(YEAR FROM f.date_end), 9999)
    ),
    event AS (
        SELECT
            e.*,
            {%- for g_col in game_cols %}
                g.{{ g_col }}{% if not loop.last %},{% endif %}
            {%- endfor %}
        FROM {{ ref(event_model) }} AS e
        LEFT JOIN {{ ref('team_game_start_info') }} AS g USING (team_id, game_id)
    ),
    -- Need to use the season table for basic stats/metrics to ensure full coverage and more efficient agg...
    basic_stats AS (
        SELECT
        {%- for key in grouping_keys %}
            {{ key }},
        {%- endfor %}
            SUM(games) AS games,
        {%- for col in int_cols %}
            SUM({{ col }}) AS {{ col }},
        {%- endfor -%}
        {%- for col_name, formula in basic_metric_dict.items() %}
            {{ formula }} AS {{ col_name }}{% if not loop.last %},{% endif %}
        {%- endfor %}
        FROM season
        WHERE game_type IN (SELECT game_type FROM {{ ref('seed_game_types') }} WHERE is_regular_season)
        GROUP BY {{ grouping_keys|join(', ') }}
    ),

    --- ...but we need to use the event table for event-based metrics,
    event_agg AS (
        SELECT
        {%- for key in grouping_keys %}
            {{ key }},
        {%- endfor %}
            COUNT(DISTINCT game_id) AS games,
        {%- for col_name, formula in event_based_metrics.items() %}
            {{ formula }} AS {{ col_name }}{% if not loop.last %},{% endif %}
        {%- endfor %}
        FROM event
        WHERE game_id IN (SELECT game_id FROM {{ ref('game_start_info') }} WHERE is_regular_season)
        GROUP BY {{ grouping_keys|join(', ') }}
    ),

    final AS (
        SELECT
        {%- for key in grouping_keys %}
            {{ key }},
        {%- endfor -%}
        {%- for col in int_cols %}
            basic_stats.{{ col }}::INT AS {{ col }},
        {%- endfor -%}
        {%- for col in basic_metric_dict %}
            basic_stats.{{ col }},
        {%- endfor %}
            COALESCE(event_agg.games / basic_stats.games, 0) AS event_coverage_rate
        {%- for col in event_based_metrics %},
            event_agg.{{ col }}
        {%- endfor %}
        FROM basic_stats
        LEFT JOIN event_agg USING ({{ grouping_keys|join(', ') }})
    )

    SELECT * FROM final
{% endmacro %}
