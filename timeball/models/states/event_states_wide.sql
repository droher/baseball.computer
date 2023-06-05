WITH lineups_flat AS (
    SELECT
        event_key,
        FIRST(game_id) AS game_id,
        FIRST(team_id) AS batting_team_id,
        FIRST(batting_side) AS batting_side,
        {%- for i in range(1, 10) %}
        FIRST(player_id) FILTER (WHERE lineup_position = {{ i }}) AS lineup_{{ i }}_id,
        {%- endfor %}
        FIRST(lineup_position) FILTER (WHERE is_at_bat) AS batter_lineup_position,
        FIRST(player_id) FILTER (WHERE is_at_bat) AS batter_id,
        FIRST(player_id) FILTER (WHERE nth_next_batter_up = 1) AS on_deck_batter_id
    FROM {{ ref('event_lineup_states') }}
    GROUP BY 1
),

defenses_flat AS (
    SELECT
        event_key,
        FIRST(team_id) AS fielding_team_id,
        FIRST(fielding_side) AS fielding_side,
        {%- for i in range(1, 11) %}
        FIRST(player_id) FILTER (WHERE fielding_position = {{ i }}) AS defense_{{ i }}_id,
        {%- endfor %}
    FROM {{ ref('event_fielding_states') }}
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
