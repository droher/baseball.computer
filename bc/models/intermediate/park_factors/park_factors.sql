{% set advanced_cols = dbt_utils.get_filtered_columns_in_relation(from=ref('calc_park_factors_advanced')) %}

WITH final AS (
    SELECT
        b.season,
        b.park_id,
        b.league,
        b.basic_park_factor,
        {% for c in advanced_cols if c.endswith("_park_factor") %}
            a.{{ c }},
        {% endfor %}
        COALESCE(a.runs_park_factor, b.basic_park_factor) AS overall_park_factor
    FROM {{ ref('calc_park_factors_basic') }} AS b
    LEFT JOIN {{ ref('calc_park_factors_advanced') }} AS a USING (season, park_id, league)
)

SELECT * FROM final
