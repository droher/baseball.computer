WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

lineups AS (
    SELECT * FROM {{ ref('event_lineup_states') }}
),

{% for pos in range(1, 10) -%}
lineup_{{ pos }} AS (
    SELECT
        event_key,
        player_id AS lineup_{{ pos }}_id,
    FROM lineups
    WHERE lineup_position = {{ pos }}
),

{% endfor %}
final AS (
    SELECT
        event_key,
        {% for pos in range(1, 10) -%}
            lineup_{{ pos }}_id,
        {% endfor %}
    FROM events
    {%- for pos in range(1, 10) %}
    INNER JOIN lineup_{{ pos }} USING (event_key)
    {%- endfor %}
)

SELECT * FROM final
