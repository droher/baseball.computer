{% macro batting_rate_stats() %}
    {% set sql %}
        ROUND(hits / at_bats, 3) AS batting_average,
        ROUND(total_bases / at_bats, 3) AS slugging_percentage,
        ROUND(on_base_successes / on_base_opportunities, 3) AS on_base_percentage,
        ROUND(total_bases / at_bats + on_base_successes / on_base_opportunities, 3) AS on_base_plus_slugging,
    {% endset %}
{% endmacro %}