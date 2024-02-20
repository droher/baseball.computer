{% macro metric_table_generator(kind, grouping_keys, agg_type, regular_season_only=True) %}
    {%- if agg_type not in ("player", "team", "league") -%}
        {{ exceptions.raise_compiler_error("Invalid agg_type - must be one of player, team, league. Got " ~ agg_type ) }}
    {%- endif -%}
    {%- set player_agg = agg_type == "player" -%}

    {%- if kind == "offense" -%}
        {%- set event_model = "event_offense_stats" -%}
        {%- set game_model = "player_game_offense_stats" if player_agg else "team_game_offense_stats" -%}
        {%- set season_model = "player_team_season_offense_stats" if player_agg else "team_season_offense_stats" -%}
        {%- set basic_metric_dict = basic_rate_stats_offense() -%}
    {%- elif kind == "pitching" -%}
        {%- set event_model = "event_pitching_stats" -%}
        {%- set game_model = "player_game_pitching_stats" if player_agg else "team_game_pitching_stats" -%}
        {%- set season_model = "player_team_season_pitching_stats" if player_agg else "team_season_pitching_stats" -%}
        {%- set basic_metric_dict = basic_rate_stats_pitching() -%}
    {%- elif kind == "fielding" -%}
        {%- set event_model = "event_player_fielding_stats" if player_agg else "event_fielding_stats"  -%}
        {%- set game_model = "player_position_game_fielding_stats" if player_agg else "team_game_fielding_stats" -%}
        {%- set season_model = "player_position_team_season_fielding_stats" if player_agg else "team_season_fielding_stats" -%}
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

    -- Add extra context columns to get potential grouping keys
    WITH season AS (
        {%- set franchise_cols = dbt_utils.get_filtered_columns_in_relation(ref('seed_franchises'),
            except=dbt_utils.get_filtered_columns_in_relation(ref(season_model))) -%}
        SELECT 
            s.*,
            {%- for f_col in franchise_cols %}
                f.{{ f_col }},
            {%- endfor %}
        FROM {{ ref(season_model) }} AS s
        LEFT JOIN {{ ref('seed_franchises') }} AS f
            ON s.team_id = f.team_id
            AND s.season BETWEEN EXTRACT(YEAR FROM f.date_start) AND COALESCE(EXTRACT(YEAR FROM f.date_end), 9999)
    ),
    event AS (
        {%- set game_cols = dbt_utils.get_filtered_columns_in_relation(ref('team_game_start_info'),
            except=dbt_utils.get_filtered_columns_in_relation(ref(game_model))) -%}
        SELECT 
            e.*,
            {%- for g_col in game_cols %}
                g.{{ g_col }},
            {%- endfor %}
        FROM {{ ref(event_model) }} AS e
        LEFT JOIN {{ ref('team_game_start_info') }} AS g USING (team_id, game_id)
    ),
    -- Need to use the season table for basic stats/metrics to ensure full coverage...
    basic_stats AS (
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
        FROM season
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
        FROM event
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
            basic_stats.{{ col.name }}::INT AS {{ col.name }},
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
