{% macro metric_table_generator(kind, grouping_keys, player_agg=True, regular_season_only=True) %}
    {%- if kind == "offense" -%}
        {%- set event_model = "event_offense_stats" -%}
        {%- set game_model = "player_game_offense_lines" if player_agg else "team_game_offense_stats" -%}
        {%- set season_model = "player_team_season_offense_lines" if player_agg else "team_season_offense_stats" -%}
        {%- set basic_metric_dict = basic_rate_stats_offense() -%}
    {%- elif kind == "pitching" -%}
        {%- set event_model = "event_pitching_stats" -%}
        {%- set game_model = "player_game_pitching_lines" if player_agg else "team_game_pitching_stats" -%}
        {%- set season_model = "player_team_season_pitching_lines" if player_agg else "team_season_pitching_stats" -%}
        {%- set basic_metric_dict = basic_rate_stats_pitching() -%}
    {%- elif kind == "fielding" -%}
        {%- set event_model = "event_player_fielding_lines" if player_agg else "event_fielding_stats"  -%}
        {%- set game_model = "player_game_fielding_lines" if player_agg else "team_game_fielding_stats" -%}
        {%- set season_model = "player_team_season_fielding_lines" if player_agg else "team_season_fielding_stats" -%}
        {%- set basic_metric_dict = basic_rate_stats_fielding() -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error("Invalid kind - must be one of offense, pitching, fielding. Got " ~ kind ) }}
    {%- endif -%}

    {%- set event_based_metrics = {} -%}
    {%- if kind != "fielding"  -%}
        {%- do event_based_metrics.update(batted_ball_stats()) -%}
        {%- do event_based_metrics.update(baserunning_stats()) -%}
        {%- do event_based_metrics.update(pitch_sequence_stats()) -%}
    {%- endif -%}

    {%- set cols = adapter.get_columns_in_relation(ref(event_model)) -%}
    {%- set filtered_cols = [] -%}
    {%- for col in cols if 'INT' in col.data_type 
            and not (col.name.endswith('_id') or col.name.endswith('_key') or col.name.endswith('_position')) 
            and col.name != "season" -%}
        {%- do filtered_cols.append(col) -%}
    {%- endfor -%}

    -- Need to use the season table for basic stats/metrics to ensure full coverage...
    WITH basic_stats AS (
        SELECT
        {%- for key in grouping_keys %}
            {{ key }},
        {%- endfor %}
            SUM(games) AS games,
        {%- for col in filtered_cols %}
            SUM({{ col.name }}) AS {{ col.name }},
        {%- endfor -%}
        {%- for col_name, formula in basic_metric_dict.items() %}
            {{ formula }} AS {{ col_name }},
        {%- endfor -%}
        FROM {{ ref(season_model) }}
        {% if regular_season_only %}
        WHERE game_type IN (SELECT game_type FROM {{ ref('seed_game_types') }} WHERE is_regular_season)
        {% endif %}
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
            {{ formula }} AS {{ col_name }},
        {%- endfor -%}
        FROM {{ ref(event_model) }}
        {% if regular_season_only %}
        WHERE game_id IN (SELECT game_id FROM {{ ref('game_start_info') }} WHERE is_regular_season)
        {% endif %}
        GROUP BY {{ grouping_keys|join(', ') }}
    ),

    final AS (
        SELECT
        {%- for key in grouping_keys %}
            {{ key }},
        {%- endfor -%}
        {%- for col in filtered_cols %}
            basic_stats.{{ col.name }},
        {%- endfor -%}
        {%- for col in basic_metric_dict %}
            basic_stats.{{ col }},
        {%- endfor -%}
            COALESCE(event_agg.games / basic_stats.games, 0) AS event_coverage_rate,
        {%- for col in event_based_metrics %}
            event_agg.{{ col }},
        {%- endfor -%}
        FROM basic_stats
        LEFT JOIN event_agg USING ({{ grouping_keys|join(', ') }})
    )
    
    SELECT * FROM final
{% endmacro %}