{{
  config(
    materialized = 'table',
    )
}}
WITH lineups AS (
    SELECT * FROM {{ ref('event_lineup_states') }}
),

defenses AS (
    SELECT * FROM {{ ref('event_fielding_states') }}
),

lineups_flat AS (
    SELECT
        event_key,
        {%- for i in range(1, 10) %}
        FIRST(player_id) FILTER (WHERE lineup_position = {{ i }}) AS lineup_{{ i }}_id,
        {%- endfor %}
        FIRST(lineup_position) FILTER (WHERE is_at_bat) AS batter_lineup_position,
        FIRST(player_id) FILTER (WHERE is_at_bat) AS batter_id,
        FIRST(player_id) FILTER (WHERE nth_next_batter_up = 1) AS on_deck_batter_id
    FROM lineups
    GROUP BY 1
),

defenses_flat AS (
    SELECT
        event_key,
        {%- for i in range(1, 11) %}
        FIRST(player_id) FILTER (WHERE fielding_position = {{ i }}) AS defense_{{ i }}_id,
        {%- endfor %}
    FROM defenses
    GROUP BY 1
),

final AS (
    SELECT
        event_key,
        lineups_flat.* EXCLUDE (event_key),
        defenses_flat.* EXCLUDE (event_key)
    FROM lineups_flat
    INNER JOIN defenses_flat USING (event_key)
)

SELECT * FROM final
