{% macro summarize_tables(args) %}
    {% set relations = dbt_utils.get_relations_by_pattern('%', '%') %}
    {% set sql %}
    SET schema = 'main';
    CREATE OR REPLACE TABLE main.summaries AS (
    {% for rel in relations %}
        SELECT '{{ rel.schema }}' AS schema_name, '{{ rel.identifier }}' AS table_name, * FROM (SUMMARIZE {{ rel }})
        {{ "UNION ALL" if not loop.last else "" }}
    {% endfor %}
    );
    {% endset %}
    {% do log(sql, info=True) %}
    {% do run_query(sql) %}
{% endmacro %}